use std::time::Instant as StdInstant;

use cucumber::then;
use tokio::time::{Instant as TokioInstant, timeout_at};

use super::*;

const SAMPLE_INTERVAL: Duration = Duration::from_secs(2);
const MATCHED_ATTACK_REPORT_ENV: &str = "SECURITY_AUDIT_MATCHED_ATTACK_REPORT";
const ARTIFACT_DIRECTORY: &str = "security-audit-sustained-evidence";

#[derive(Clone)]
struct NodeTip {
    id: HeaderId,
    id_text: String,
    height: u64,
    slot: u64,
    observed_at_unix_ms: u64,
}

#[derive(Clone, Copy)]
struct WorkloadCriteria {
    budget: usize,
    initial_pending: usize,
    interval: Duration,
    readiness_timeout: Duration,
    max_tip_height_skew: u64,
}

#[derive(Clone, Copy)]
struct ExposureCriteria {
    min_opportunities: usize,
    min_nonempty: usize,
    min_delivered: usize,
    cooldown_blocks: u64,
    min_duration: Duration,
    timeout: Duration,
}

#[derive(Default)]
struct ActiveLogObservations {
    nodes: BTreeMap<String, Vec<ObservedLogLine>>,
    opportunities: BTreeMap<String, ProducerOpportunity>,
    control_timing: BTreeMap<String, Value>,
    attack_candidates: BTreeSet<HeaderId>,
    read_failures: Vec<Value>,
}

struct PostStopLogObservation {
    tail: Option<AuditLogTail>,
    setup_error: Option<String>,
    read_error: Option<String>,
    equivocation_events: Vec<Value>,
}

impl PostStopLogObservation {
    fn new(world: &CucumberWorld) -> Self {
        let (tail, setup_error) = match AuditLogTail::after_readiness(world) {
            Ok(tail) => (Some(tail), None),
            Err(error) => (None, Some(error)),
        };
        Self {
            tail,
            read_error: setup_error.clone(),
            setup_error,
            equivocation_events: Vec::new(),
        }
    }

    fn sample(&mut self, producer: &str) {
        let Some(tail) = &mut self.tail else {
            return;
        };
        match tail.read_new_lines(producer) {
            Ok(lines) => {
                for line in lines {
                    let fields = parse_log_fields(&line.text);
                    if matches!(
                        fields.get("event").map(String::as_str),
                        Some(
                            "security_audit_ordinary_submitted"
                                | "security_audit_sibling_submitted"
                        )
                    ) {
                        self.equivocation_events.push(json!({"event": fields.get("event"), "line": line.text, "path": line.path, "offset": line.offset}));
                    }
                }
            }
            Err(error) => self.read_error = Some(error),
        }
    }

    const fn quiet(&self) -> bool {
        self.tail.is_some() && self.read_error.is_none() && self.equivocation_events.is_empty()
    }
}

impl ActiveLogObservations {
    fn read_nodes(
        &mut self,
        tail: &mut AuditLogTail,
        participants: &SecurityAuditParticipants,
        final_read: bool,
    ) -> usize {
        let mut failures = 0;
        for name in node_names(participants) {
            match tail.read_new_lines(name) {
                Ok(lines) => {
                    self.nodes.entry(name.to_owned()).or_default().extend(
                        lines.into_iter().filter(|line| {
                            line.text.contains("security_audit_")
                                || line
                                    .text
                                    .contains("logos_blockchain::chain::leader: proposed block ")
                        }),
                    );
                }
                Err(error) => {
                    failures += 1;
                    let mut record =
                        json!({"node": name, "observed_at_unix_ms": unix_now_ms(), "error": error});
                    if final_read {
                        record["final_read"] = json!(true);
                    }
                    self.read_failures.push(record);
                }
            }
        }
        failures
    }

    fn update_opportunities(
        &mut self,
        producer: &str,
        attack_enabled: bool,
    ) -> (Option<AttackEvidence>, usize) {
        let lines = self
            .nodes
            .get(producer)
            .map(Vec::as_slice)
            .unwrap_or_default();
        if attack_enabled {
            for opportunity in find_producer_opportunities(lines) {
                if let Some(id) = opportunity.ordinary.get("ordinary_block_id") {
                    self.opportunities.entry(id.clone()).or_insert(opportunity);
                }
            }
        } else {
            for timing in find_control_timing_events(lines) {
                if let Some(key) = timing["opportunity_key"].as_str() {
                    self.control_timing.entry(key.to_owned()).or_insert(timing);
                }
            }
        }
        let mut latest = None;
        let mut failures = 0;
        for opportunity in self.opportunities.values() {
            match attack_evidence_from_log(opportunity) {
                Ok(evidence) => {
                    self.attack_candidates.insert(evidence.ordinary_id);
                    self.attack_candidates
                        .extend(evidence.siblings.iter().map(|(_, sibling)| sibling.id));
                    latest = Some(evidence);
                }
                Err(_) => failures += 1,
            }
        }
        if attack_enabled {
            latest = self
                .opportunities
                .values()
                .max_by_key(|opportunity| opportunity.ordinary_line)
                .and_then(|opportunity| attack_evidence_from_log(opportunity).ok());
        }
        (latest, failures)
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
struct ExperimentalNode {
    role: String,
    canonical_reference: bool,
    blend_core_provider: bool,
    initial_peers: Vec<usize>,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
struct ExperimentalConfiguration {
    nodes: Vec<ExperimentalNode>,
    readiness_timeout_ms: u128,
    active_timeout_ms: u128,
    sampling_interval_ms: u128,
    maximum_tip_height_skew: u64,
}

fn experimental_configuration(
    world: &CucumberWorld,
    workload: WorkloadCriteria,
    active_timeout: Duration,
) -> Result<ExperimentalConfiguration, String> {
    let participants = audit_participants(world)?;
    let names = node_names(participants);
    let configs = names
        .iter()
        .map(|name| {
            let node = world
                .nodes_info
                .get(*name)
                .ok_or_else(|| format!("missing node {name}"))?;
            let bytes = fs::read(node.runtime_dir.join(USER_CONFIG_FILE))
                .map_err(|error| error.to_string())?;
            serde_yaml::from_slice::<serde_yaml::Value>(&bytes).map_err(|error| error.to_string())
        })
        .collect::<Result<Vec<_>, _>>()?;
    let ports = configs
        .iter()
        .map(|config| {
            config["network"]["backend"]["swarm"]["port"]
                .as_u64()
                .ok_or_else(|| "network listener port missing".to_owned())
        })
        .collect::<Result<Vec<_>, _>>()?;
    if ports.iter().collect::<BTreeSet<_>>().len() != ports.len() {
        return Err("duplicate listener ports".to_owned());
    }
    let nodes = configs
        .iter()
        .enumerate()
        .map(|(index, config)| {
            let peers = config["network"]["backend"]["initial_peers"]
                .as_sequence()
                .ok_or_else(|| "initial peers unavailable".to_owned())?;
            let mut initial_peers = peers
                .iter()
                .map(|peer| {
                    let address = peer
                        .as_str()
                        .ok_or_else(|| "invalid initial peer address".to_owned())?;
                    let port = address
                        .split("/udp/")
                        .nth(1)
                        .and_then(|part| part.split('/').next())
                        .and_then(|part| part.parse::<u64>().ok())
                        .ok_or_else(|| format!("unrecognized initial peer {address}"))?;
                    ports
                        .iter()
                        .position(|candidate| *candidate == port)
                        .ok_or_else(|| {
                            format!("initial peer {address} is not a declared participant")
                        })
                })
                .collect::<Result<Vec<_>, String>>()?;
            initial_peers.sort_unstable();
            initial_peers.dedup();
            let name = names[index];
            Ok(ExperimentalNode {
                role: if name == participants.producer {
                    "producer"
                } else {
                    "honest"
                }
                .to_owned(),
                canonical_reference: name == participants.canonical_reference,
                blend_core_provider: config["blend"]["core"].is_mapping(),
                initial_peers,
            })
        })
        .collect::<Result<Vec<_>, String>>()?;
    let provider_count = world
        .cluster
        .blend_core_nodes
        .ok_or_else(|| "Blend provider count was not declared".to_owned())?;
    if nodes.iter().filter(|node| node.blend_core_provider).count() != provider_count {
        return Err(
            "generated Blend provider configuration disagrees with the declared provider count"
                .to_owned(),
        );
    }
    Ok(ExperimentalConfiguration {
        nodes,
        readiness_timeout_ms: workload.readiness_timeout.as_millis(),
        active_timeout_ms: active_timeout.as_millis(),
        sampling_interval_ms: SAMPLE_INTERVAL.as_millis(),
        maximum_tip_height_skew: workload.max_tip_height_skew,
    })
}

fn equivalent_topology(a: &[ExperimentalNode], b: &[ExperimentalNode]) -> bool {
    fn search(a: &[ExperimentalNode], b: &[ExperimentalNode], mapping: &mut Vec<usize>) -> bool {
        let i = mapping.len();
        if i == a.len() {
            return true;
        }
        for (j, node) in b.iter().enumerate() {
            if mapping.contains(&j)
                || a[i].role != node.role
                || a[i].canonical_reference != node.canonical_reference
                || a[i].blend_core_provider != node.blend_core_provider
                || a[i].initial_peers.len() != node.initial_peers.len()
            {
                continue;
            }
            if mapping.iter().enumerate().any(|(old_i, old_j)| {
                a[i].initial_peers.contains(&old_i) != node.initial_peers.contains(old_j)
                    || a[old_i].initial_peers.contains(&i) != b[*old_j].initial_peers.contains(&j)
            }) {
                continue;
            }
            if a[i].initial_peers.contains(&i) != node.initial_peers.contains(&j) {
                continue;
            }
            mapping.push(j);
            if search(a, b, mapping) {
                return true;
            }
            mapping.pop();
        }
        false
    }
    a.len() == b.len() && search(a, b, &mut Vec::new())
}

fn exposure_is_verified(active: &Value, attack_enabled: bool, attack_present: bool) -> bool {
    if !attack_enabled {
        return true;
    }
    let exposure = &active["attack_exposure"];
    let meets = |observed: &str, required: &str| {
        exposure[observed]
            .as_u64()
            .zip(exposure[required].as_u64())
            .is_some_and(|(count, threshold)| threshold > 0 && count >= threshold)
    };
    attack_present
        && meets(
            "complete_genuine_opportunities",
            "required_complete_opportunities",
        )
        && meets(
            "nonempty_ordinary_bodies",
            "required_nonempty_opportunities",
        )
        && meets(
            "nonempty_bodies_containing_submitted_transactions",
            "required_nonempty_opportunities",
        )
        && meets(
            "distinct_opportunities_with_ordered_blend_to_acceptance",
            "required_delivered_opportunities",
        )
}

#[then("the sustained equivocation meets these exposure and cooldown criteria")]
#[expect(
    clippy::allow_attributes,
    reason = "The Cucumber step signature mandates a mutable world"
)]
#[allow(
    clippy::needless_pass_by_ref_mut,
    reason = "Cucumber step functions require the world as the first `&mut` argument"
)]
async fn step_observe_sustained_attack(world: &mut CucumberWorld, step: &Step) -> StepResult {
    let values = criteria_table(step)?;
    require_criteria(
        &values,
        &[
            "complete_opportunities",
            "nonempty_submitted_opportunities",
            "delivered_opportunities",
            "cooldown_canonical_blocks",
            "minimum_active_seconds",
            "active_timeout_seconds",
            "transaction_budget",
            "initial_pending_transactions",
            "submission_interval_seconds",
            "readiness_seconds",
            "maximum_tip_height_skew",
        ],
    )?;
    let workload = workload_criteria(&values)?;
    let exposure = ExposureCriteria {
        min_opportunities: positive_criterion(&values, "complete_opportunities")?,
        min_nonempty: positive_criterion(&values, "nonempty_submitted_opportunities")?,
        min_delivered: positive_criterion(&values, "delivered_opportunities")?,
        cooldown_blocks: positive_criterion(&values, "cooldown_canonical_blocks")?,
        min_duration: Duration::from_secs(positive_criterion(&values, "minimum_active_seconds")?),
        timeout: Duration::from_secs(positive_criterion(&values, "active_timeout_seconds")?),
    };
    if exposure.min_duration > exposure.timeout {
        return Err(StepError::InvalidArgument {
            message: "minimum active duration exceeds timeout".to_owned(),
        });
    }
    run_active_window(world, Some(exposure), workload, None).await
}

#[then("the control matches the attack's canonical block count with this workload")]
#[expect(
    clippy::allow_attributes,
    reason = "The Cucumber step signature mandates a mutable world"
)]
#[allow(
    clippy::needless_pass_by_ref_mut,
    reason = "Cucumber step functions require the world as the first `&mut` argument"
)]
async fn step_observe_sustained_control(world: &mut CucumberWorld, step: &Step) -> StepResult {
    let values = criteria_table(step)?;
    require_criteria(
        &values,
        &[
            "active_timeout_seconds",
            "transaction_budget",
            "initial_pending_transactions",
            "submission_interval_seconds",
            "readiness_seconds",
            "maximum_tip_height_skew",
        ],
    )?;
    let timeout = Duration::from_secs(positive_criterion(&values, "active_timeout_seconds")?);
    run_active_window(world, None, workload_criteria(&values)?, Some(timeout)).await
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RecoveryBoundary {
    LastAttackSlot,
    ControlBaseline,
}

impl RecoveryBoundary {
    fn parse(value: &str) -> Result<Self, StepError> {
        match value {
            "last attack slot" => Ok(Self::LastAttackSlot),
            "control baseline" => Ok(Self::ControlBaseline),
            _ => Err(StepError::InvalidArgument {
                message: format!("unknown recovery boundary: {value}"),
            }),
        }
    }
}

#[then(
    expr = "the honest nodes maintain common finality beyond {string} for {int} seconds within {int} seconds"
)]
#[expect(
    clippy::allow_attributes,
    reason = "The Cucumber signature mandates a mutable world for read-only recovery"
)]
#[allow(
    clippy::needless_pass_by_ref_mut,
    reason = "Cucumber step functions require the world as the first `&mut` argument"
)]
async fn step_recover_after_sustained_experiment(
    world: &mut CucumberWorld,
    step: &Step,
    boundary: String,
    stability: u64,
    timeout: u64,
) -> StepResult {
    let max_tip_height_skew = validate_recovery_criteria(step)?;
    run_recovery_window(
        world,
        RecoveryBoundary::parse(&boundary)?,
        Duration::from_secs(stability),
        Duration::from_secs(timeout),
        max_tip_height_skew,
    )
    .await
}

fn validate_recovery_criteria(step: &Step) -> Result<u64, StepError> {
    let table = step.table.as_ref().ok_or(StepError::MissingTable)?;
    parse_recovery_criteria(&table.rows)
}

fn parse_recovery_criteria(rows: &[Vec<String>]) -> Result<u64, StepError> {
    let expected = [
        ("criterion", "requirement"),
        ("honest_nodes_responsive", "all"),
        ("honest_nodes_make_progress", "all"),
        ("common_lib", "same ID and slot"),
        ("tip_and_finalized_ancestry", "verified"),
        ("lib_continuity", "verified"),
        ("finalized_history_conflicts", "none"),
        ("producer_stopped_and_quiet", "yes"),
    ];
    let mut values = BTreeMap::new();
    for row in rows {
        let [key, value] = row.as_slice() else {
            return Err(StepError::InvalidArgument {
                message: "each recovery criterion needs a name and requirement".to_owned(),
            });
        };
        if values.insert(key.as_str(), value.as_str()).is_some() {
            return Err(StepError::InvalidArgument {
                message: format!("duplicate recovery criterion: {key}"),
            });
        }
    }
    if values.len() != expected.len() + 1
        || expected
            .iter()
            .any(|(key, value)| values.get(*key) != Some(value))
    {
        return Err(StepError::InvalidArgument {
            message: "recovery criteria table does not match the enforced safety checks".to_owned(),
        });
    }
    values
        .get("maximum_tip_height_skew")
        .and_then(|value| value.parse().ok())
        .ok_or_else(|| StepError::InvalidArgument {
            message: "maximum_tip_height_skew must be a nonnegative integer".to_owned(),
        })
}

fn criteria_table(step: &Step) -> Result<BTreeMap<String, u64>, StepError> {
    let table = step.table.as_ref().ok_or(StepError::MissingTable)?;
    parse_criteria_rows(&table.rows)
}

fn parse_criteria_rows(rows: &[Vec<String>]) -> Result<BTreeMap<String, u64>, StepError> {
    if rows.first().map(Vec::as_slice) != Some(&["criterion".to_owned(), "value".to_owned()]) {
        return Err(StepError::InvalidArgument {
            message: "criteria table requires criterion and value columns".to_owned(),
        });
    }
    let mut values = BTreeMap::new();
    for row in rows.iter().skip(1) {
        let [key, value] = row.as_slice() else {
            return Err(StepError::InvalidArgument {
                message: "each criterion needs a name and value".to_owned(),
            });
        };
        let parsed: u64 = value.parse().map_err(|_| StepError::InvalidArgument {
            message: format!("invalid value for criterion {key}: {value}"),
        })?;
        if values.insert(key.clone(), parsed).is_some() {
            return Err(StepError::InvalidArgument {
                message: format!("criterion {key} must be unique"),
            });
        }
    }
    Ok(values)
}

fn require_criteria(values: &BTreeMap<String, u64>, required: &[&str]) -> StepResult {
    if values.len() != required.len()
        || values.keys().any(|name| !required.contains(&name.as_str()))
    {
        return Err(StepError::InvalidArgument {
            message: format!("criteria must be exactly: {}", required.join(", ")),
        });
    }
    Ok(())
}

fn criterion<T: TryFrom<u64>>(values: &BTreeMap<String, u64>, name: &str) -> Result<T, StepError> {
    values
        .get(name)
        .copied()
        .and_then(|value| T::try_from(value).ok())
        .ok_or_else(|| StepError::InvalidArgument {
            message: format!("missing or invalid criterion {name}"),
        })
}

fn positive_criterion<T: TryFrom<u64>>(
    values: &BTreeMap<String, u64>,
    name: &str,
) -> Result<T, StepError> {
    if values.get(name) == Some(&0) {
        return Err(StepError::InvalidArgument {
            message: format!("criterion {name} must be positive"),
        });
    }
    criterion(values, name)
}

fn workload_criteria(values: &BTreeMap<String, u64>) -> Result<WorkloadCriteria, StepError> {
    let budget = positive_criterion(values, "transaction_budget")?;
    let initial_pending = criterion(values, "initial_pending_transactions")?;
    if initial_pending > budget {
        return Err(StepError::InvalidArgument {
            message: "initial transactions exceed the budget".to_owned(),
        });
    }
    Ok(WorkloadCriteria {
        budget,
        initial_pending,
        interval: Duration::from_secs(positive_criterion(values, "submission_interval_seconds")?),
        readiness_timeout: Duration::from_secs(positive_criterion(values, "readiness_seconds")?),
        max_tip_height_skew: criterion(values, "maximum_tip_height_skew")?,
    })
}

#[expect(
    clippy::too_many_lines,
    reason = "Preserves setup evidence for each validation failure"
)]
#[expect(
    clippy::too_many_arguments,
    reason = "Validates the explicit scenario configuration before observation"
)]
async fn prepare_active_environment(
    world: &mut CucumberWorld,
    scenario: &str,
    expected_siblings: usize,
    artifact_path: &Path,
    matched_attack: Option<&(PathBuf, u64, String, Value)>,
    workload: WorkloadCriteria,
    active_timeout: Duration,
    declared_workload: &SecurityAuditWorkload,
) -> Result<
    (
        Value,
        Value,
        Value,
        AuditLogTail,
        ExperimentalConfiguration,
        Vec<String>,
    ),
    StepError,
> {
    world.reset_wallet_scanner_after_current_iteration().await;
    let provenance = match verify_runtime_configuration_with_siblings(
        world,
        scenario,
        expected_siblings,
    )
    .await
    {
        Ok(provenance) => provenance,
        Err(error) => {
            write_json(
                artifact_path,
                &json!({
                    "status": "workload_or_leadership_setup_failure",
                    "failure_classification": "effective_runtime_configuration_invalid",
                    "failure": error,
                    "scenario": scenario,
                    "expected_producer_siblings": expected_siblings,
                    "observed_at_unix_ms": unix_now_ms(),
                }),
            )?;
            return Err(StepError::StepFail {
                message: format!("effective runtime configuration is invalid: {error}"),
            });
        }
    };
    let configuration = consensus_configuration(world);
    if configuration["status"].as_str() != Some("read") {
        write_json(
            artifact_path,
            &json!({
                "status": "workload_or_leadership_setup_failure",
            "failure_classification": "consensus_configuration_unverified",
                "consensus_configuration": configuration,
            }),
        )?;
        return Err(StepError::StepFail {
            message: "node deployment configuration is unverified".to_owned(),
        });
    }
    let experiment = match experimental_configuration(world, workload, active_timeout) {
        Ok(experiment) => experiment,
        Err(error) => {
            write_json(
                artifact_path,
                &json!({"status": "workload_or_leadership_setup_failure", "failure_classification": "experimental_configuration_unverified", "failure": error, "provenance": provenance, "consensus_configuration": configuration}),
            )?;
            return Err(StepError::StepFail {
                message: format!(
                    "experimental configuration is unverified: {error}; evidence: {}",
                    artifact_path.display()
                ),
            });
        }
    };
    let mismatches = matched_attack.map_or_else(Vec::new, |(_, _, _, report)| {
        matched_attack_compatibility(
            report,
            &configuration,
            &provenance,
            workload,
            declared_workload,
            &experiment,
        )
    });
    if let Some((path, _, _, _)) = matched_attack
        && !mismatches.is_empty()
    {
        let failure = format!(
            "attack report {} is incompatible or unverified: {}",
            path.display(),
            mismatches.join(", ")
        );
        write_json(
            artifact_path,
            &json!({
                "status": "workload_or_leadership_setup_failure",
                "failure_classification": "matched_attack_configuration_mismatch_or_unverified",
                "failure": failure,
                "consensus_configuration": configuration,
                "experimental_configuration": experiment,
                "compatibility_mismatches": mismatches,
            }),
        )?;
        return Err(StepError::StepFail { message: failure });
    }
    let (readiness, log_tail) = match wait_for_blend_readiness(world, workload.readiness_timeout)
        .await
    {
        Ok(readiness) => readiness,
        Err(error) => {
            write_json(
                artifact_path,
                &json!({
                    "status": "workload_or_leadership_setup_failure",
                    "failure_classification": "blend_readiness_failed",
                    "failure": error,
                    "scenario": scenario,
                    "provenance": provenance,
                    "observed_at_unix_ms": unix_now_ms(),
                }),
            )?;
            return Err(StepError::StepFail {
                message: format!("Blend readiness failed before sustained observation: {error}"),
            });
        }
    };
    Ok((
        provenance,
        configuration,
        readiness,
        log_tail,
        experiment,
        mismatches,
    ))
}

#[expect(
    clippy::too_many_lines,
    reason = "Collects one bounded active run and preserves all observations in one report"
)]
#[expect(
    clippy::cognitive_complexity,
    reason = "The active sample loop keeps related safety and liveness evidence synchronized"
)]
async fn run_active_window(
    world: &mut CucumberWorld,
    exposure: Option<ExposureCriteria>,
    workload: WorkloadCriteria,
    control_timeout: Option<Duration>,
) -> StepResult {
    let declared_workload = audit_workload(world)
        .cloned()
        .map_err(|message| StepError::InvalidArgument { message })?;
    let participants = audit_participants(world)
        .map_err(|message| StepError::InvalidArgument { message })?
        .clone();
    let producer = participants.producer.as_str();
    let honest_nodes = participants
        .honest_nodes
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();
    let canonical_reference = participants.canonical_reference.as_str();
    let attack_enabled = exposure.is_some();
    let exposure_criteria = exposure.unwrap_or(ExposureCriteria {
        min_opportunities: 0,
        min_nonempty: 0,
        min_delivered: 0,
        cooldown_blocks: 0,
        min_duration: Duration::ZERO,
        timeout: Duration::ZERO,
    });
    let scenario = if attack_enabled {
        "Honest peers recover after sustained nonempty sibling equivocation"
    } else {
        "Honest peers under equivalent workload without sibling equivocation"
    };
    let artifact_dir = world.lifecycle.scenario_base_dir.join(ARTIFACT_DIRECTORY);
    fs::create_dir_all(&artifact_dir).map_err(|error| StepError::StepFail {
        message: format!("cannot create {}: {error}", artifact_dir.display()),
    })?;
    let artifact_path = artifact_dir.join("active-observations.json");
    let matched_attack = if attack_enabled {
        None
    } else {
        Some(read_matched_attack_report()?)
    };
    let window_limit = exposure.map_or_else(
        || control_timeout.expect("control timeout"),
        |criteria| criteria.timeout,
    );
    let expected_siblings =
        audit_sibling_count(world).map_err(|message| StepError::InvalidArgument { message })?;
    if attack_enabled != (expected_siblings > 0) {
        return Err(StepError::InvalidArgument {
            message: "producer sibling configuration contradicts experiment mode".to_owned(),
        });
    }
    let (
        mut provenance,
        configuration,
        readiness,
        mut log_tail,
        experiment,
        compatibility_mismatches,
    ) = prepare_active_environment(
        world,
        scenario,
        expected_siblings,
        &artifact_path,
        matched_attack.as_ref(),
        workload,
        window_limit,
        &declared_workload,
    )
    .await?;
    let mut header_caches = node_names(&participants)
        .into_iter()
        .map(|name| (name.to_owned(), HeaderCache::default()))
        .collect::<BTreeMap<_, _>>();
    let mut log_observations = ActiveLogObservations::default();
    let mut last_verified_libs = BTreeMap::new();
    let mut last_tips = BTreeMap::new();
    let pre_window_observations = sample_all_nodes(
        world,
        &participants,
        None,
        &mut last_verified_libs,
        &mut header_caches,
    )
    .await;
    let pre_window_snapshots = pre_window_observations
        .iter()
        .map(|observation| observation.snapshot.clone())
        .collect::<Vec<_>>();
    let baseline_history_checks = check_finalized_history_agreement(
        world,
        &honest_observations(&pre_window_observations, &participants),
        &mut header_caches,
        0,
    )
    .await;
    let baseline_safety_conflict = baseline_history_checks
        .iter()
        .any(|check| check["status"].as_str() == Some("conflicting_finalized_histories"));
    let mut baseline_ancestry_unverified = baseline_history_checks
        .iter()
        .any(|check| check["status"].as_str() == Some("ancestry_unverified"));
    for observation in &pre_window_observations {
        if observation.consensus.is_some()
            && (!observation.tip_path_complete
                || observation
                    .snapshot
                    .pointer("/tip_descends_from_lib")
                    .and_then(Value::as_bool)
                    != Some(true))
        {
            baseline_ancestry_unverified = true;
        }
    }
    for observation in &pre_window_observations {
        if let Some(state) = observation.consensus.as_ref() {
            let name = observation.snapshot["node"].as_str().unwrap_or_default();
            last_verified_libs.insert(name.to_owned(), (state.lib_id, state.lib_slot));
            last_tips.insert(name.to_owned(), tip_from_observation(observation));
        }
    }
    if let Err(error) = world.ensure_wallet_scanner_started().await {
        let report = json!({
            "status": "workload_setup_failed",
            "failure_classification": "workload_or_wallet_setup",
            "failure": error.to_string(),
            "scenario": scenario,
            "provenance": provenance,
            "blend_readiness": readiness,
            "pre_window_consensus_snapshots": pre_window_snapshots,
        });
        write_json(&artifact_path, &report)?;
        return Err(StepError::StepFail {
            message: format!(
                "wallet scanner failed; evidence: {}",
                artifact_path.display()
            ),
        });
    }

    let mut submitted_transactions = Vec::new();
    let mut failed_preparations = Vec::new();
    let mut failed_submissions = Vec::new();
    let mut stop_replenishing = false;
    for index in 1..=workload.initial_pending {
        if submit_workload_transaction(
            world,
            index,
            &mut submitted_transactions,
            &mut failed_preparations,
            &mut failed_submissions,
        )
        .await
        .is_err()
        {
            stop_replenishing = true;
            break;
        }
    }

    let active_started_at = unix_timestamp_ms();
    let active_started = StdInstant::now();
    let active_deadline = active_started + window_limit;
    let active_deadline_tokio = TokioInstant::now() + window_limit;
    let mut next_submission = active_started + workload.interval;
    let mut snapshots = Vec::new();
    let mut mempool_observations = Vec::new();
    let mut finalized_history_checks = vec![json!({
        "phase": "pre_attack_baseline",
        "observed_at_unix_ms": unix_now_ms(),
        "pairs": baseline_history_checks,
    })];
    let mut safety_conflict_observed = baseline_safety_conflict;
    let mut ancestry_unverified = baseline_ancestry_unverified;
    let mut continuity_regressions = Vec::new();
    let mut continuity_unverified = Vec::new();
    let mut ancestry_gaps = pre_window_observations
        .iter()
        .filter(|observation| {
            observation.consensus.is_some()
                && (!observation.tip_path_complete
                    || observation
                        .snapshot
                        .pointer("/tip_descends_from_lib")
                        .and_then(Value::as_bool)
                        != Some(true))
        })
        .map(|observation| observation.snapshot.clone())
        .collect::<Vec<_>>();
    let mut tip_transitions = Vec::new();
    let mut tip_reorganizations = Vec::new();
    let mut tip_reorganizations_unverified = Vec::new();
    let mut alignment_transitions = Vec::new();
    let mut previous_honest_alignment = None;
    let mut last_sample_at = StdInstant::now();
    let mut last_tip_progress = node_names(&participants)
        .into_iter()
        .map(|name| (name.to_owned(), active_started_at))
        .collect::<BTreeMap<_, _>>();
    let mut last_lib_progress = last_tip_progress.clone();
    let mut tip_stall_intervals = Vec::new();
    let mut lib_stall_intervals = Vec::new();
    let mut per_node_tip_advance_counts = BTreeMap::<String, usize>::new();
    let mut per_node_lib_advance_counts = BTreeMap::<String, usize>::new();
    let mut unique_tip_blocks = BTreeSet::new();
    let mut sample_error_count = 0usize;
    let mut cooldown_start: Option<NodeTip> = None;
    let mut cooldown_verified_blocks = 0u64;
    let mut cooldown_events = Vec::new();
    let mut exposure_observations = Vec::new();
    let mut observed_ordinary_bodies = BTreeMap::<String, Value>::new();
    let mut exposure_requirements_met = false;
    let mut next_exposure_check = active_started;
    let mut canonical_start: Option<NodeTip> = None;
    let mut canonical_end: Option<NodeTip> = None;
    let mut canonical_blocks = 0u64;
    let mut canonical_status = "unverified";
    let mut canonical_error: Option<String> = None;
    let target_blocks = matched_attack.as_ref().map(|(_, blocks, _, _)| *blocks);

    while StdInstant::now() < active_deadline {
        if !stop_replenishing
            && submitted_transactions.len() + failed_preparations.len() + failed_submissions.len()
                < workload.budget
            && StdInstant::now() >= next_submission
        {
            let index = submitted_transactions.len()
                + failed_preparations.len()
                + failed_submissions.len()
                + 1;
            if submit_workload_transaction(
                world,
                index,
                &mut submitted_transactions,
                &mut failed_preparations,
                &mut failed_submissions,
            )
            .await
            .is_err()
            {
                stop_replenishing = true;
            }
            next_submission += workload.interval;
        }

        sample_error_count += log_observations.read_nodes(&mut log_tail, &participants, false);
        let (latest_attack, evidence_errors) =
            log_observations.update_opportunities(producer, attack_enabled);
        sample_error_count += evidence_errors;

        let observations = timeout_at(
            active_deadline_tokio,
            sample_all_nodes(
                world,
                &participants,
                latest_attack.as_ref(),
                &mut last_verified_libs,
                &mut header_caches,
            ),
        )
        .await
        .unwrap_or_default();
        if observations.is_empty() {
            break;
        }
        let observed_at_unix_ms = unix_timestamp_ms();
        let elapsed_at_ms = active_started.elapsed().as_millis();
        let mut node_snapshot_values = Vec::new();
        for observation in &observations {
            let node_name = observation.snapshot["node"].as_str().unwrap_or_default();
            if observation.consensus.is_some() {
                let new_tip = tip_from_observation(observation);
                let previous_tip = last_tips.get(node_name).cloned();
                if let Some(previous) = previous_tip
                    && previous.id != new_tip.id
                    && let Some(node) = world.nodes_info.get(node_name)
                {
                    let transition = observe_tip_transition(
                        node_name,
                        &node.client,
                        previous,
                        new_tip.clone(),
                        &mut header_caches,
                        &log_observations.attack_candidates,
                    )
                    .await;
                    if transition["status"].as_str() == Some("reorganization") {
                        if transition["last_common_ancestor"]["status"].as_str() == Some("verified")
                        {
                            tip_reorganizations.push(transition.clone());
                        } else {
                            tip_reorganizations_unverified.push(transition.clone());
                        }
                    } else if transition["status"].as_str() == Some("unverified") {
                        tip_reorganizations_unverified.push(transition.clone());
                    }
                    tip_transitions.push(transition);
                }
                if last_tips
                    .get(node_name)
                    .is_none_or(|old| old.id != new_tip.id)
                {
                    let previous_at = last_tip_progress
                        .insert(node_name.to_owned(), observed_at_unix_ms)
                        .unwrap_or(active_started_at);
                    if observed_at_unix_ms.saturating_sub(previous_at) > 0 {
                        tip_stall_intervals.push(json!({
                            "node": node_name,
                            "started_at_unix_ms": previous_at,
                            "ended_at_unix_ms": observed_at_unix_ms,
                            "duration_ms": observed_at_unix_ms.saturating_sub(previous_at),
                        }));
                    }
                    *per_node_tip_advance_counts
                        .entry(node_name.to_owned())
                        .or_default() += 1;
                    unique_tip_blocks.insert(new_tip.id_text.clone());
                    last_tips.insert(node_name.to_owned(), new_tip);
                }
                if observation.continuity == "verified" {
                    let previous_at = last_lib_progress
                        .insert(node_name.to_owned(), observed_at_unix_ms)
                        .unwrap_or(active_started_at);
                    if observed_at_unix_ms.saturating_sub(previous_at) > 0 {
                        lib_stall_intervals.push(json!({
                            "node": node_name,
                            "started_at_unix_ms": previous_at,
                            "ended_at_unix_ms": observed_at_unix_ms,
                            "duration_ms": observed_at_unix_ms.saturating_sub(previous_at),
                        }));
                    }
                    *per_node_lib_advance_counts
                        .entry(node_name.to_owned())
                        .or_default() += 1;
                }
            } else {
                sample_error_count += 1;
            }
            if observation.continuity == "regression_or_unrelated" {
                continuity_regressions.push(observation.snapshot.clone());
            } else if observation.continuity == "unverified" {
                continuity_unverified.push(observation.snapshot.clone());
            }
            if !observation.tip_path_complete
                || observation
                    .snapshot
                    .pointer("/tip_descends_from_lib")
                    .and_then(Value::as_bool)
                    != Some(true)
            {
                ancestry_unverified = true;
                ancestry_gaps.push(observation.snapshot.clone());
            }
            node_snapshot_values.push(observation.snapshot.clone());
        }
        let honest_observations = honest_observations(&observations, &participants);
        let history_checks = check_finalized_history_agreement(
            world,
            &honest_observations,
            &mut header_caches,
            latest_attack.as_ref().map_or(0, |attack| attack.slot),
        )
        .await;
        safety_conflict_observed |= history_checks
            .iter()
            .any(|check| check["status"].as_str() == Some("conflicting_finalized_histories"));
        ancestry_unverified |= history_checks
            .iter()
            .any(|check| check["status"].as_str() == Some("ancestry_unverified"));
        finalized_history_checks.push(json!({
            "observed_at_unix_ms": observed_at_unix_ms,
            "pairs": history_checks,
        }));
        let alignment = align_observations(
            world,
            &observations,
            &mut header_caches,
            &honest_nodes,
            workload.max_tip_height_skew,
        )
        .await;
        let aligned = alignment["aligned"].as_bool() == Some(true);
        if previous_honest_alignment.is_some_and(|previous| previous != aligned) {
            alignment_transitions.push(json!({
                "observed_at_unix_ms": observed_at_unix_ms,
                "aligned": aligned,
                "details": alignment,
            }));
        }
        previous_honest_alignment = Some(aligned);
        let states = observations
            .iter()
            .filter_map(|observation| {
                Some((
                    observation.snapshot["node"].as_str()?.to_owned(),
                    observation.consensus.clone()?,
                ))
            })
            .collect::<Vec<_>>();
        let common_lib = common_finalized_lib(&states, node_names(&participants).len());
        let round = json!({
            "observed_at_unix_ms": observed_at_unix_ms,
            "elapsed_ms": elapsed_at_ms,
            "nodes": node_snapshot_values,
            "honest_tip_alignment": alignment,
            "all_node_common_lib": common_lib.map(|(id, slot)| json!({
                "lib_id": id.encode_hex::<String>(),
                "lib_slot": slot,
            })),
            "finalized_history_pairs": finalized_history_checks.last(),
        });
        snapshots.push(round);
        let mempool_round = sample_mempools(world, &participants, &submitted_transactions).await;
        mempool_observations.push(json!({
            "observed_at_unix_ms": observed_at_unix_ms,
            "elapsed_ms": elapsed_at_ms,
            "nodes": mempool_round,
        }));
        last_sample_at = StdInstant::now();

        if let Some(observation) = observations.iter().find(|item| {
            item.snapshot["node"].as_str() == Some(canonical_reference) && item.consensus.is_some()
        }) {
            let tip = tip_from_observation(observation);
            let start = canonical_start.get_or_insert_with(|| tip.clone());
            canonical_end = Some(tip.clone());
            if tip.height < start.height {
                canonical_status = "unverified";
                canonical_error = Some("canonical tip is below the start height".to_owned());
            } else if let (Some(node), Some(cache)) = (
                world.nodes_info.get(canonical_reference),
                header_caches.get_mut(canonical_reference),
            ) {
                match timeout_at(
                    active_deadline_tokio,
                    block_id_at_height(&node.client, tip.id, tip.height, start.height, cache),
                )
                .await
                {
                    Ok(Ok(ancestor)) if ancestor == start.id => {
                        canonical_status = "verified";
                        canonical_blocks = tip.height - start.height;
                        canonical_error = None;
                    }
                    Ok(Ok(_)) => {
                        canonical_status = "unverified";
                        canonical_error =
                            Some("active start is no longer on the canonical chain".to_owned());
                    }
                    Ok(Err(error)) => {
                        canonical_status = "unverified";
                        canonical_error = Some(error);
                    }
                    Err(_) => break,
                }
            }
        } else {
            canonical_status = "unverified";
            canonical_error = Some(format!("{canonical_reference} did not respond"));
        }
        if !attack_enabled
            && canonical_status == "verified"
            && target_blocks.is_some_and(|target| canonical_blocks >= target)
        {
            break;
        }

        if attack_enabled {
            if cooldown_start.is_none() && StdInstant::now() >= next_exposure_check {
                let exposure = timeout_at(
                    active_deadline_tokio,
                    observe_active_exposure(
                        world,
                        &participants,
                        &log_observations.opportunities,
                        &log_observations.nodes,
                        &submitted_transactions,
                        &mut observed_ordinary_bodies,
                        exposure_criteria,
                    ),
                )
                .await;
                match exposure {
                    Ok(exposure) => {
                        exposure_requirements_met =
                            exposure["requirements_met"].as_bool() == Some(true);
                        exposure_observations.push(exposure);
                    }
                    Err(_) => break,
                }
                next_exposure_check = StdInstant::now() + Duration::from_secs(15);
            }
            if cooldown_start.is_none()
                && exposure_requirements_met
                && active_started.elapsed() >= exposure_criteria.min_duration
                && let Some(tip) = last_tips.get(canonical_reference).cloned()
            {
                cooldown_events.push(json!({"event": "cooldown_started", "tip_id": tip.id_text, "height": tip.height, "observed_at_unix_ms": observed_at_unix_ms}));
                cooldown_start = Some(tip);
            }
            if let (Some(start), Some(tip), Some(node), Some(cache)) = (
                cooldown_start.as_ref(),
                last_tips.get(canonical_reference),
                world.nodes_info.get(canonical_reference),
                header_caches.get_mut(canonical_reference),
            ) {
                if tip.height < start.height {
                    cooldown_events.push(json!({"event": "cooldown_reset", "reason": "tip_height_regressed", "previous_start": start.id_text, "new_start": tip.id_text, "observed_at_unix_ms": observed_at_unix_ms}));
                    cooldown_start = Some(tip.clone());
                    cooldown_verified_blocks = 0;
                } else {
                    match timeout_at(
                        active_deadline_tokio,
                        block_id_at_height(&node.client, tip.id, tip.height, start.height, cache),
                    )
                    .await
                    {
                        Ok(Ok(ancestor)) if ancestor == start.id => {
                            cooldown_verified_blocks = tip.height - start.height;
                        }
                        Ok(Ok(_)) => {
                            cooldown_events.push(json!({"event": "cooldown_reset", "reason": "start_not_on_canonical_chain", "previous_start": start.id_text, "new_start": tip.id_text, "observed_at_unix_ms": observed_at_unix_ms}));
                            cooldown_start = Some(tip.clone());
                            cooldown_verified_blocks = 0;
                        }
                        Ok(Err(error)) => {
                            cooldown_events.push(json!({"event": "cooldown_ancestry_unverified", "error": error, "observed_at_unix_ms": observed_at_unix_ms}));
                            cooldown_verified_blocks = 0;
                        }
                        Err(_) => break,
                    }
                }
                if cooldown_verified_blocks >= exposure_criteria.cooldown_blocks {
                    cooldown_events.push(json!({"event": "cooldown_completed", "tip_id": tip.id_text, "height": tip.height, "verified_blocks": cooldown_verified_blocks, "observed_at_unix_ms": observed_at_unix_ms}));
                    break;
                }
            }
        }

        let next_tick = (active_started
            + Duration::from_millis(
                u64::try_from(snapshots.len())
                    .unwrap_or(u64::MAX)
                    .saturating_mul(2_000),
            ))
        .min(active_deadline);
        if next_tick > StdInstant::now() {
            sleep(next_tick.saturating_duration_since(StdInstant::now())).await;
        }
    }

    let active_ended_at = unix_timestamp_ms();
    let active_duration_ms = active_started.elapsed().as_millis();
    sample_error_count += log_observations.read_nodes(&mut log_tail, &participants, true);
    let ActiveLogObservations {
        nodes: node_logs,
        opportunities,
        control_timing: control_timing_events,
        attack_candidates: mut attack_branch_candidates,
        read_failures: log_read_failures,
    } = log_observations;
    let OpportunitySummary {
        records: opportunity_records,
        indexed: attack_opportunity_records,
        nonempty: nonempty_opportunities,
        nonempty_submitted: nonempty_submitted_body_opportunities,
        delivered: delivered_opportunities,
    } = summarize_opportunities(OpportunitySummaryContext {
        world,
        participants: &participants,
        opportunities: &opportunities,
        node_logs: &node_logs,
        observed_ordinary_bodies: &observed_ordinary_bodies,
        submitted_transactions: &submitted_transactions,
        last_tips: &last_tips,
        last_verified_libs: &last_verified_libs,
        header_caches: &mut header_caches,
        attack_branch_candidates: &mut attack_branch_candidates,
    })
    .await;
    let control_opportunity_records = control_timing_events.into_values().collect::<Vec<_>>();
    if !opportunity_records.is_empty() {
        record_observed_audit_opportunity(&mut provenance, opportunity_records.len());
    } else if !control_opportunity_records.is_empty() {
        provenance["observed_audit_functionality"] = json!({
            "status": "control_timing_events_observed",
            "control_timing_events": control_opportunity_records.len(),
            "evidence_source": "producer security_audit_leadership_timing log events",
            "executable_feature_set_independently_verified": false,
        });
    }
    for node in node_names(&participants) {
        if let Some(started_at) = last_tip_progress.get(node) {
            let duration_ms = active_ended_at.saturating_sub(*started_at);
            if duration_ms > 0 {
                tip_stall_intervals.push(json!({
                    "node": node,
                    "started_at_unix_ms": started_at,
                    "ended_at_unix_ms": active_ended_at,
                    "duration_ms": duration_ms,
                    "right_censored_at_active_window_end": true,
                }));
            }
        }
        if let Some(started_at) = last_lib_progress.get(node) {
            let duration_ms = active_ended_at.saturating_sub(*started_at);
            if duration_ms > 0 {
                lib_stall_intervals.push(json!({
                    "node": node,
                    "started_at_unix_ms": started_at,
                    "ended_at_unix_ms": active_ended_at,
                    "duration_ms": duration_ms,
                    "right_censored_at_active_window_end": true,
                }));
            }
        }
    }
    let attacked_opportunity_count = opportunity_records.len();
    let workload_setup_failure =
        submitted_transactions.is_empty() && !failed_preparations.is_empty();
    let insufficient_exposure = attack_enabled
        && (attacked_opportunity_count < exposure_criteria.min_opportunities
            || nonempty_opportunities < exposure_criteria.min_nonempty
            || nonempty_submitted_body_opportunities < exposure_criteria.min_nonempty
            || delivered_opportunities.len() < exposure_criteria.min_delivered);
    let control_blocks_incomplete = !attack_enabled
        && (canonical_status != "verified"
            || target_blocks.is_none_or(|target| canonical_blocks < target));
    let cooldown_incomplete =
        attack_enabled && cooldown_verified_blocks < exposure_criteria.cooldown_blocks;
    let active_window = json!({
        "started_at_unix_ms": active_started_at,
        "ended_at_unix_ms": active_ended_at,
        "duration_ms": active_duration_ms,
        "minimum_duration_ms": attack_enabled.then_some(exposure_criteria.min_duration.as_millis()),
        "maximum_duration_ms": window_limit.as_millis(),
        "matched_attack_report": matched_attack.as_ref().map(|(path, _, hash, _)| json!({"path": path, "sha256": hash})),
        "canonical_blocks": {
            "node": canonical_reference, "status": canonical_status,
            "start_tip_id": canonical_start.as_ref().map(|tip| &tip.id_text),
            "start_height": canonical_start.as_ref().map(|tip| tip.height),
            "end_tip_id": canonical_end.as_ref().map(|tip| &tip.id_text),
            "end_height": canonical_end.as_ref().map(|tip| tip.height),
            "verified_blocks": (canonical_status == "verified").then_some(canonical_blocks),
            "matched_target": target_blocks,
            "error": canonical_error,
        },
        "exposure_observations": exposure_observations,
        "cooldown": {"required_blocks": exposure_criteria.cooldown_blocks, "verified_blocks": cooldown_verified_blocks, "verified": attack_enabled.then_some(cooldown_verified_blocks >= exposure_criteria.cooldown_blocks), "events": cooldown_events},
        "sample_interval_ms": SAMPLE_INTERVAL.as_millis(),
        "sample_count": snapshots.len(),
    });
    let workload = json!({
        "budget": workload.budget,
        "transfer_amount": declared_workload.amount,
        "sender_wallet": declared_workload.sender,
        "receiver_wallet": declared_workload.receiver,
        "initial_pending_transactions": workload.initial_pending,
        "submission_interval_ms": workload.interval.as_millis(),
        "submitted_count": submitted_transactions.len(),
        "submitted_transactions": submitted_transactions,
        "failed_preparations": failed_preparations,
        "failed_submissions": failed_submissions,
        "funding_exhausted_or_replenishment_stopped": stop_replenishing,
    });
    let attack_exposure = json!({
        "complete_genuine_opportunities": attacked_opportunity_count,
        "required_complete_opportunities": exposure_criteria.min_opportunities,
        "nonempty_ordinary_bodies": nonempty_opportunities,
        "required_nonempty_opportunities": exposure_criteria.min_nonempty,
        "nonempty_bodies_containing_submitted_transactions": nonempty_submitted_body_opportunities,
        "required_submitted_transaction_body_opportunities": exposure_criteria.min_nonempty,
        "distinct_opportunities_with_ordered_blend_to_acceptance": delivered_opportunities.len(),
        "required_delivered_opportunities": exposure_criteria.min_delivered,
        "insufficient_exposure": insufficient_exposure,
    });
    let liveness_metrics = json!({
        "per_node_tip_advance_counts": per_node_tip_advance_counts,
        "per_node_lib_advance_counts": per_node_lib_advance_counts,
        "unique_observed_tip_blocks": unique_tip_blocks.len(),
        "tip_stall_intervals": tip_stall_intervals,
        "lib_stall_intervals": lib_stall_intervals,
        "tip_alignment_transitions": alignment_transitions,
    });
    let safety_metrics = json!({
        "safety_conflict_observed": safety_conflict_observed,
        "ancestry_unverified": ancestry_unverified,
        "lib_continuity_regressions": continuity_regressions,
        "lib_continuity_unverified_observations": continuity_unverified,
        "ancestry_gaps": ancestry_gaps,
        "pre_attack_finalized_history_checks": finalized_history_checks.first(),
        "finalized_history_checks": finalized_history_checks,
    });
    let reorganization_metrics = json!({
        "tip_transition_observations": tip_transitions,
        "tip_reorganizations": tip_reorganizations,
        "tip_reorganizations_unverified": tip_reorganizations_unverified,
        "tip_reorganization_observation_is_lower_bound": true,
    });
    let baseline_and_final = json!({
        "pre_window_consensus_snapshots": pre_window_snapshots,
        "last_active_tips": last_tips.iter().map(|(node, tip)| json!({
            "node": node,
            "tip_id": tip.id_text,
            "tip_height": tip.height,
            "tip_slot": tip.slot,
            "observed_at_unix_ms": tip.observed_at_unix_ms,
        })).collect::<Vec<_>>(),
        "last_verified_libs": last_verified_libs.iter().map(|(node, (id, slot))| json!({
            "node": node,
            "lib_id": id.encode_hex::<String>(),
            "lib_slot": slot,
        })).collect::<Vec<_>>(),
    });
    let cache_summary = header_caches
        .iter()
        .map(|(node, cache)| {
            json!({
                "node": node,
                "cached_headers": cache.links.len(),
                "lookup_failures": cache.failures,
            })
        })
        .collect::<Vec<_>>();
    let report = json!({
        "status": if workload_setup_failure { "workload_setup_failed" } else if insufficient_exposure { "insufficient_exposure" } else if cooldown_incomplete { "cooldown_incomplete" } else if control_blocks_incomplete { "control_block_target_incomplete" } else { "active_window_observed" },
        "workload_setup_failure": workload_setup_failure,
        "workload_setup_failure_classification": workload_setup_failure.then_some("sender_wallet_unavailable_or_unfunded"),
        "failure_classification": if insufficient_exposure { Some("insufficient_exposure") } else if cooldown_incomplete { Some("canonical_block_cooldown_not_verified") } else if control_blocks_incomplete { Some("control_block_target_incomplete") } else { None },
        "scenario": scenario,
        "experiment_mode": if attack_enabled { "sustained_equivocation" } else { "matched_control" },
        "source_commit": provenance["commit_sha"],
        "provenance": provenance,
        "consensus_configuration": configuration,
        "experimental_configuration": experiment,
        "matched_control_compatibility": if attack_enabled { Value::Null } else { json!({"status": "verified", "mismatches": compatibility_mismatches}) },
        "blend_readiness": readiness,
        "active_window": active_window,
        "baseline_and_final": baseline_and_final,
        "pre_attack_consensus_snapshots": pre_window_snapshots,
        "snapshots": snapshots,
        "mempool_observations": mempool_observations,
        "transaction_workload": workload,
        "attack_exposure": attack_exposure,
        "opportunities": opportunity_records,
        "control_leadership_timing_events": control_opportunity_records,
        "liveness_metrics": liveness_metrics,
        "reorganization_metrics": reorganization_metrics,
        "safety_metrics": safety_metrics,
        "sample_error_count": sample_error_count,
        "log_read_failures": log_read_failures,
        "last_sample_at_unix_ms": unix_timestamp_ms().saturating_sub(
            u64::try_from(last_sample_at.elapsed().as_millis()).unwrap_or(u64::MAX),
        ),
        "producer_opportunity_count_at_window_end": attacked_opportunity_count,
        "header_cache_summary": cache_summary,
        "artifact_path": artifact_path,
    });
    write_json(&artifact_path, &report)?;
    let opportunities_path = artifact_dir.join("producer-opportunities.json");
    write_json(
        &opportunities_path,
        &json!({
            "attack_opportunities": attack_opportunity_records.into_values().collect::<Vec<_>>(),
            "control_leadership_timing_events": control_opportunity_records,
            "producer_log_source": world.nodes_info.get(producer).map(|node| node.runtime_dir.display().to_string()),
        }),
    )?;
    Ok(())
}

struct OpportunitySummary {
    records: Vec<Value>,
    indexed: BTreeMap<String, Value>,
    nonempty: usize,
    nonempty_submitted: usize,
    delivered: BTreeSet<String>,
}

struct OpportunitySummaryContext<'a> {
    world: &'a CucumberWorld,
    participants: &'a SecurityAuditParticipants,
    opportunities: &'a BTreeMap<String, ProducerOpportunity>,
    node_logs: &'a BTreeMap<String, Vec<ObservedLogLine>>,
    observed_ordinary_bodies: &'a BTreeMap<String, Value>,
    submitted_transactions: &'a [Value],
    last_tips: &'a BTreeMap<String, NodeTip>,
    last_verified_libs: &'a BTreeMap<String, (HeaderId, u64)>,
    header_caches: &'a mut BTreeMap<String, HeaderCache>,
    attack_branch_candidates: &'a mut BTreeSet<HeaderId>,
}

async fn summarize_opportunities(context: OpportunitySummaryContext<'_>) -> OpportunitySummary {
    let OpportunitySummaryContext {
        world,
        participants,
        opportunities,
        node_logs,
        observed_ordinary_bodies,
        submitted_transactions,
        last_tips,
        last_verified_libs,
        header_caches,
        attack_branch_candidates,
    } = context;
    let mut opportunity_records = Vec::new();
    let mut attack_opportunity_records = BTreeMap::<String, Value>::new();
    let mut nonempty_opportunities = 0usize;
    let mut nonempty_submitted_body_opportunities = 0usize;
    let mut delivered_opportunities = BTreeSet::new();
    let submitted_hashes = submitted_transactions
        .iter()
        .filter_map(|transaction| transaction["transaction_hash"].as_str().map(str::to_owned))
        .collect::<BTreeSet<_>>();
    for (ordinary_id, opportunity) in opportunities {
        let (mut record, mut evidence) =
            summarize_attack_opportunity(world, participants, opportunity, node_logs).await;
        if record["ordinary_body"]["available"].as_bool() != Some(true)
            && let Some(body) = observed_ordinary_bodies.get(ordinary_id)
        {
            record["ordinary_body"] = body.clone();
            record["ordinary_body"]["retrieved_during_active_window"] = json!(true);
            evidence.body_root = body["body_root"].as_str().unwrap_or_default().to_owned();
            evidence.transaction_hashes = body["transaction_hashes"]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(Value::as_str)
                .map(str::to_owned)
                .collect();
            record["attack_evidence"]["body_root"] = json!(evidence.body_root);
            record["attack_evidence"]["ordered_transaction_hashes"] =
                json!(evidence.transaction_hashes);
            record["body_integrity"] = compare_sibling_bodies(
                &record["ordinary_body"],
                record["sibling_blocks"]
                    .as_array()
                    .map_or(&[], Vec::as_slice),
            );
        }
        record["canonical_branch_by_node_at_active_window_end"] = canonical_membership_by_node(
            world,
            participants,
            &evidence,
            last_tips,
            last_verified_libs,
            header_caches,
        )
        .await;
        let body_hashes = record["ordinary_body"]["transaction_hashes"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(Value::as_str)
            .map(str::to_owned)
            .collect::<Vec<_>>();
        let contains_submitted_transaction = body_hashes
            .iter()
            .any(|hash| submitted_hashes.contains(hash));
        record["body_integrity"]["contains_submitted_transaction"] =
            json!(contains_submitted_transaction);
        if record["ordinary_body"]["transaction_count"]
            .as_u64()
            .is_some_and(|count| count > 0)
        {
            nonempty_opportunities += 1;
            if contains_submitted_transaction {
                nonempty_submitted_body_opportunities += 1;
            }
        }
        if record["ordered_acceptance_by_distinct_peer"].as_bool() == Some(true) {
            delivered_opportunities.insert(ordinary_id.clone());
        }
        attack_branch_candidates.insert(evidence.ordinary_id);
        for (_, sibling) in &evidence.siblings {
            attack_branch_candidates.insert(sibling.id);
        }
        opportunity_records.push(record.clone());
        attack_opportunity_records.insert(ordinary_id.clone(), record);
    }
    OpportunitySummary {
        records: opportunity_records,
        indexed: attack_opportunity_records,
        nonempty: nonempty_opportunities,
        nonempty_submitted: nonempty_submitted_body_opportunities,
        delivered: delivered_opportunities,
    }
}

fn node_names(participants: &SecurityAuditParticipants) -> Vec<&str> {
    std::iter::once(participants.producer.as_str())
        .chain(participants.honest_nodes.iter().map(String::as_str))
        .collect()
}

fn unix_now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| {
            u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
        })
}

fn observed_log_lines(log: &str, source: &str) -> Vec<ObservedLogLine> {
    let path = PathBuf::from(source);
    let mut offset = 0u64;
    log.lines()
        .map(|line| {
            let observed = ObservedLogLine {
                text: line.to_owned(),
                path: path.clone(),
                offset,
            };
            offset = offset
                .saturating_add(u64::try_from(line.len()).unwrap_or(u64::MAX))
                .saturating_add(1);
            observed
        })
        .collect()
}

fn read_matched_attack_report() -> Result<(PathBuf, u64, String, Value), StepError> {
    let path = std::env::var_os(MATCHED_ATTACK_REPORT_ENV)
        .map(PathBuf::from)
        .ok_or_else(|| StepError::StepFail {
            message: format!(
                "set {MATCHED_ATTACK_REPORT_ENV} to the attack active-observations.json"
            ),
        })?;
    let bytes = fs::read(&path).map_err(|error| StepError::StepFail {
        message: format!("cannot read attack report {}: {error}", path.display()),
    })?;
    let report: Value = serde_json::from_slice(&bytes).map_err(|error| StepError::StepFail {
        message: format!("cannot parse attack report {}: {error}", path.display()),
    })?;
    let blocks = report
        .pointer("/active_window/canonical_blocks/verified_blocks")
        .and_then(Value::as_u64)
        .filter(|blocks| *blocks > 0)
        .filter(|_| {
            report
                .pointer("/active_window/canonical_blocks/status")
                .and_then(Value::as_str)
                == Some("verified")
        })
        .filter(|_| report["experiment_mode"].as_str() == Some("sustained_equivocation"))
        .ok_or_else(|| StepError::StepFail {
            message: format!(
                "attack report {} lacks verified canonical-reference block growth",
                path.display()
            ),
        })?;
    let hash = sha256_file(&path).map_err(|error| StepError::StepFail { message: error })?;
    Ok((path, blocks, hash, report))
}

#[expect(
    clippy::too_many_lines,
    reason = "Reports each independently verified matching condition"
)]
fn matched_attack_compatibility(
    attack: &Value,
    configuration: &Value,
    provenance: &Value,
    workload: WorkloadCriteria,
    declared_workload: &SecurityAuditWorkload,
    experiment: &ExperimentalConfiguration,
) -> Vec<String> {
    let attack_configuration = &attack["consensus_configuration"];
    let fields = [
        "security_parameter_k",
        "slot_duration_secs",
        "uncle_reference_window_in_block",
        "slot_activation_coeff_numerator",
        "slot_activation_coeff_denominator",
        "epoch_stake_distribution_stabilization",
        "epoch_period_nonce_buffer",
        "epoch_period_nonce_stabilization",
    ];
    let mut mismatches = Vec::new();
    if configuration["status"].as_str() != Some("read")
        || attack_configuration["status"].as_str() != Some("read")
    {
        mismatches.push("consensus_configuration_unverified".to_owned());
    }
    if provenance["binary_sha256"].as_str().is_none()
        || provenance["binary_sha256"] != attack["provenance"]["binary_sha256"]
    {
        mismatches.push("binary_sha256_mismatch_or_unverified".to_owned());
    }
    for field in fields {
        if configuration[field].is_null() || configuration[field] != attack_configuration[field] {
            mismatches.push(format!("consensus_{field}_mismatch_or_unverified"));
        }
    }
    let workload_matches = attack
        .pointer("/transaction_workload/budget")
        .and_then(Value::as_u64)
        == u64::try_from(workload.budget).ok()
        && attack
            .pointer("/transaction_workload/initial_pending_transactions")
            .and_then(Value::as_u64)
            == u64::try_from(workload.initial_pending).ok()
        && attack
            .pointer("/transaction_workload/submission_interval_ms")
            .and_then(Value::as_u64)
            == u64::try_from(workload.interval.as_millis()).ok()
        && attack["transaction_workload"]["transfer_amount"].as_u64()
            == Some(declared_workload.amount)
        && attack["transaction_workload"]["sender_wallet"].as_str()
            == Some(declared_workload.sender.as_str())
        && attack["transaction_workload"]["receiver_wallet"].as_str()
            == Some(declared_workload.receiver.as_str());
    if !workload_matches {
        mismatches.push("workload_mismatch_or_unverified".to_owned());
    }
    match serde_json::from_value::<ExperimentalConfiguration>(
        attack["experimental_configuration"].clone(),
    ) {
        Ok(previous) => {
            if previous.nodes.len() != experiment.nodes.len() {
                mismatches.push("participant_count_mismatch".to_owned());
            }
            let normalise = |nodes: &[ExperimentalNode], keep_reference: bool, keep_blend: bool| {
                nodes
                    .iter()
                    .cloned()
                    .map(|mut node| {
                        if !keep_reference {
                            node.canonical_reference = false;
                        }
                        if !keep_blend {
                            node.blend_core_provider = false;
                        }
                        node
                    })
                    .collect::<Vec<_>>()
            };
            let graph_matches = equivalent_topology(
                &normalise(&previous.nodes, false, false),
                &normalise(&experiment.nodes, false, false),
            );
            let reference_matches = equivalent_topology(
                &normalise(&previous.nodes, true, false),
                &normalise(&experiment.nodes, true, false),
            );
            let blend_matches = equivalent_topology(
                &normalise(&previous.nodes, false, true),
                &normalise(&experiment.nodes, false, true),
            );
            if !graph_matches {
                mismatches.push("participant_roles_or_topology_mismatch".to_owned());
            }
            if graph_matches && !reference_matches {
                mismatches.push("canonical_reference_mismatch".to_owned());
            }
            if graph_matches && !blend_matches {
                mismatches.push("blend_provider_configuration_mismatch".to_owned());
            }
            if reference_matches
                && blend_matches
                && !equivalent_topology(&previous.nodes, &experiment.nodes)
            {
                mismatches.push("reference_and_blend_assignment_mismatch".to_owned());
            }
            if previous.readiness_timeout_ms != experiment.readiness_timeout_ms {
                mismatches.push("readiness_timeout_mismatch".to_owned());
            }
            if previous.active_timeout_ms != experiment.active_timeout_ms {
                mismatches.push("active_timeout_mismatch".to_owned());
            }
            if previous.sampling_interval_ms != experiment.sampling_interval_ms {
                mismatches.push("sampling_interval_mismatch".to_owned());
            }
            if previous.maximum_tip_height_skew != experiment.maximum_tip_height_skew {
                mismatches.push("maximum_tip_height_skew_mismatch".to_owned());
            }
        }
        Err(_) => mismatches.push("experimental_configuration_unverified".to_owned()),
    }
    mismatches
}

async fn observe_active_exposure(
    world: &CucumberWorld,
    participants: &SecurityAuditParticipants,
    opportunities: &BTreeMap<String, ProducerOpportunity>,
    node_logs: &BTreeMap<String, Vec<ObservedLogLine>>,
    submitted: &[Value],
    observed_bodies: &mut BTreeMap<String, Value>,
    criteria: ExposureCriteria,
) -> Value {
    let submitted_hashes = submitted
        .iter()
        .filter_map(|transaction| transaction["transaction_hash"].as_str())
        .collect::<BTreeSet<_>>();
    let mut complete = 0;
    let mut nonempty_submitted = 0;
    let mut delivered = 0;
    for (ordinary_id, opportunity) in opportunities {
        if attack_evidence_from_log(opportunity).is_err() {
            continue;
        }
        complete += 1;
        if !observed_bodies.contains_key(ordinary_id) {
            let mut body = fetch_body_evidence(
                world,
                &participants.producer,
                opportunity.ordinary.get("ordinary_block_id"),
            )
            .await;
            if body["available"].as_bool() == Some(true) {
                body["observed_at_unix_ms"] = json!(unix_now_ms());
                observed_bodies.insert(ordinary_id.clone(), body);
            }
        }
        let body = observed_bodies.get(ordinary_id).unwrap_or(&Value::Null);
        if body["transaction_count"]
            .as_u64()
            .is_some_and(|count| count > 0)
            && body["transaction_hashes"].as_array().is_some_and(|hashes| {
                hashes
                    .iter()
                    .filter_map(Value::as_str)
                    .any(|hash| submitted_hashes.contains(hash))
            })
        {
            nonempty_submitted += 1;
        }
        let siblings = opportunity
            .siblings
            .iter()
            .filter_map(|(_, sibling)| sibling.get("sibling_block_id").cloned())
            .collect::<BTreeSet<_>>();
        let honest_nodes = participants
            .honest_nodes
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>();
        let stages = receiver_stages(node_logs, &siblings, &opportunity.ordinary, &honest_nodes);
        if stages.iter().any(|((node, _), stage)| {
            honest_nodes.contains(&node.as_str()) && stage.has_ordered_blend_to_acceptance()
        }) {
            delivered += 1;
        }
    }
    json!({
        "observed_at_unix_ms": unix_now_ms(),
        "complete_genuine_opportunities": complete,
        "nonempty_submitted_opportunities": nonempty_submitted,
        "delivered_opportunities": delivered,
        "requirements_met": complete >= criteria.min_opportunities
            && nonempty_submitted >= criteria.min_nonempty
            && delivered >= criteria.min_delivered,
    })
}

fn write_json(path: &Path, value: &Value) -> StepResult {
    fs::write(
        path,
        serde_json::to_vec_pretty(value).map_err(|error| StepError::StepFail {
            message: format!("cannot encode {}: {error}", path.display()),
        })?,
    )
    .map_err(|error| StepError::StepFail {
        message: format!("cannot write {}: {error}", path.display()),
    })
}

async fn submit_workload_transaction(
    world: &mut CucumberWorld,
    index: usize,
    submitted: &mut Vec<Value>,
    preparation_failures: &mut Vec<Value>,
    submission_failures: &mut Vec<Value>,
) -> Result<(), ()> {
    let producer = audit_participants(world)
        .map_err(|error| {
            preparation_failures.push(json!({"error": error}));
        })?
        .producer
        .clone();
    let workload = audit_workload(world).cloned().map_err(|error| {
        preparation_failures.push(json!({"error": error}));
    })?;
    let alias = format!("SECURITY_AUDIT_SUSTAINED_{index:03}");
    if let Err(error) = prepare_transfer_transaction(
        world,
        "sustained security audit workload",
        alias.clone(),
        workload.amount,
        workload.sender.clone(),
        workload.receiver.clone(),
    )
    .await
    {
        preparation_failures.push(json!({
            "alias": alias,
            "attempted_at_unix_ms": unix_now_ms(),
            "error": error.to_string(),
        }));
        return Err(());
    }
    let signed = match world.resolve_prepared_transaction(&alias) {
        Ok(signed) => signed,
        Err(error) => {
            preparation_failures.push(json!({
                "alias": alias,
                "attempted_at_unix_ms": unix_now_ms(),
                "error": error.to_string(),
            }));
            return Err(());
        }
    };
    let transaction_hash = tx_hash_to_hex(&signed.hash());
    let target_nodes = vec![producer];
    match submit_prepared_transaction_to_nodes(
        world,
        "sustained security audit workload",
        alias.clone(),
        target_nodes.clone(),
    )
    .await
    {
        Ok(()) => {
            submitted.push(json!({
                "alias": alias,
                "transaction_hash": transaction_hash,
                "submitted_at_unix_ms": unix_now_ms(),
                "amount": workload.amount,
                "target_nodes": target_nodes,
            }));
            Ok(())
        }
        Err(error) => {
            submission_failures.push(json!({
                "alias": alias,
                "transaction_hash": transaction_hash,
                "attempted_at_unix_ms": unix_now_ms(),
                "error": error.to_string(),
                "submission_to_all_nodes_confirmed": false,
            }));
            Err(())
        }
    }
}

fn find_control_timing_events(lines: &[ObservedLogLine]) -> Vec<Value> {
    let proposal_logs = lines
        .iter()
        .filter_map(|line| {
            let proposal = parse_ordinary_proposal_log(&line.text)?;
            let key = format!("{}:{}", proposal.parent_block_id, proposal.slot);
            Some((
                key,
                json!({
                    "ordinary_block_id": proposal.block_id,
                    "ordinary_transaction_count": proposal.transaction_count,
                    "proposal_log_path": line.path.display().to_string(),
                    "proposal_log_offset": line.offset,
                }),
            ))
        })
        .collect::<BTreeMap<_, _>>();
    let mut events = BTreeMap::new();
    for (index, line) in lines.iter().enumerate() {
        let fields = parse_log_fields(&line.text);
        if fields.get("event").map(String::as_str) != Some("security_audit_leadership_timing")
            || parse_usize(&fields, "configured_siblings") != Some(0)
            || !identity_bool(&fields, "ordinary_self_applied")
            || !identity_bool(&fields, "ordinary_relay_enqueued")
        {
            continue;
        }
        let Some(parent) = fields.get("parent_block_id") else {
            continue;
        };
        let Some(slot) = fields.get("slot") else {
            continue;
        };
        let key = format!("{parent}:{slot}");
        let proposal = proposal_logs.get(&key).cloned();
        let proposal_found = proposal.is_some();
        let ordinary_block_id = proposal
            .as_ref()
            .and_then(|proposal| proposal["ordinary_block_id"].as_str())
            .map(str::to_owned);
        let ordinary_transaction_count = proposal
            .as_ref()
            .and_then(|proposal| proposal["ordinary_transaction_count"].as_u64());
        events.entry(key.clone()).or_insert_with(|| {
            json!({
                "opportunity_key": key,
                "parent_block_id": parent,
                "slot": slot,
                "timing_event_line_index": index,
                "timestamp": parse_log_timestamp(&line.text),
                "configured_siblings": 0,
                "ordinary_self_applied": true,
                "ordinary_relay_enqueued": true,
                "ordinary_block_id": ordinary_block_id,
                "ordinary_transaction_count": ordinary_transaction_count,
                "ordinary_proposal_evidence_status": if proposal_found { "matched" } else { "unavailable" },
                "ordinary_proposal_log": proposal,
                "sibling_events": [],
                "note": "real producer leadership timing event; no sibling event is fabricated for control",
            })
        });
    }
    events.into_values().collect()
}

#[derive(Debug, Eq, PartialEq)]
struct OrdinaryProposalLog {
    parent_block_id: String,
    slot: String,
    block_id: String,
    transaction_count: usize,
}

fn parse_ordinary_proposal_log(line: &str) -> Option<OrdinaryProposalLog> {
    if !line.contains("logos_blockchain::chain::leader: proposed block ") {
        return None;
    }
    let parent = line
        .split_once("parent=HeaderId(")?
        .1
        .split_once(')')?
        .0
        .trim_start_matches("0x");
    let slot = line.split_once("slot=Slot(")?.1.split_once(')')?.0;
    let block_id = line
        .split_once("proposed block HeaderId(")?
        .1
        .split_once(')')?
        .0
        .trim_start_matches("0x");
    let transaction_count = line
        .split_once(" with ")?
        .1
        .split_once(" transactions")?
        .0
        .parse::<usize>()
        .ok()?;
    Some(OrdinaryProposalLog {
        parent_block_id: format!("0x{parent}"),
        slot: slot.to_owned(),
        block_id: format!("0x{block_id}"),
        transaction_count,
    })
}

fn attack_evidence_from_log(opportunity: &ProducerOpportunity) -> Result<AttackEvidence, String> {
    let ordinary_id_text = opportunity
        .ordinary
        .get("ordinary_block_id")
        .ok_or_else(|| "ordinary block ID is missing".to_owned())?
        .clone();
    let parent_id_text = opportunity
        .ordinary
        .get("parent_block_id")
        .ok_or_else(|| "ordinary parent block ID is missing".to_owned())?
        .clone();
    let slot = opportunity
        .ordinary
        .get("slot")
        .ok_or_else(|| "ordinary slot is missing".to_owned())?
        .parse::<u64>()
        .map_err(|error| format!("ordinary slot is invalid: {error}"))?;
    let siblings = opportunity
        .siblings
        .iter()
        .map(|(index, sibling)| {
            let id_text = sibling
                .get("sibling_block_id")
                .ok_or_else(|| format!("sibling {index} ID is missing"))?;
            Ok((
                *index,
                AttackCandidate {
                    id: parse_header_id(id_text)?,
                },
            ))
        })
        .collect::<Result<Vec<_>, String>>()?;
    Ok(AttackEvidence {
        ordinary_id: parse_header_id(&ordinary_id_text)?,
        ordinary_id_text,
        parent_id: parse_header_id(&parent_id_text)?,
        parent_id_text,
        slot,
        body_root: String::new(),
        transaction_hashes: Vec::new(),
        siblings,
    })
}

async fn sample_all_nodes(
    world: &CucumberWorld,
    participants: &SecurityAuditParticipants,
    attack: Option<&AttackEvidence>,
    last_verified_libs: &mut BTreeMap<String, (HeaderId, u64)>,
    caches: &mut BTreeMap<String, HeaderCache>,
) -> Vec<FinalityNodeObservation> {
    let mut observations = Vec::with_capacity(participants.honest_nodes.len() + 1);
    for node_name in node_names(participants) {
        let Some(node) = world.nodes_info.get(node_name) else {
            observations.push(unresponsive_observation(node_name, "node was not started"));
            continue;
        };
        let Some(cache) = caches.get_mut(node_name) else {
            observations.push(unresponsive_observation(
                node_name,
                "header cache was not allocated",
            ));
            continue;
        };
        let previous_lib = last_verified_libs.get(node_name).copied();
        let mut observation =
            observe_sustained_node(node_name, &node.client, attack, cache, previous_lib).await;
        if let Some(state) = observation.consensus.as_ref() {
            if previous_lib.is_some_and(|(previous_id, _)| previous_id == state.lib_id) {
                "unchanged".clone_into(&mut observation.continuity);
                observation.snapshot["lib_continuity"] = json!("unchanged");
            }
            if observation.continuity == "verified" || previous_lib.is_none() {
                last_verified_libs.insert(node_name.to_owned(), (state.lib_id, state.lib_slot));
                if previous_lib.is_none() {
                    "baseline".clone_into(&mut observation.continuity);
                    observation.snapshot["lib_continuity"] = json!("baseline");
                }
            }
        }
        observations.push(observation);
    }
    observations
}

fn unresponsive_observation(node_name: &str, error: &str) -> FinalityNodeObservation {
    FinalityNodeObservation {
        snapshot: json!({
            "node": node_name,
            "observed_at_unix_ms": unix_now_ms(),
            "responsive": false,
            "error": error,
        }),
        consensus: None,
        tip_path_complete: false,
        finalized_path_complete: false,
        attack_branch: AttackBranch::Unverified,
        finalized_attack_branch: AttackBranch::Unverified,
        continuity: "unverified".to_owned(),
    }
}

#[expect(
    clippy::too_many_lines,
    reason = "Builds one consistent ancestry and continuity observation for a node"
)]
async fn observe_sustained_node(
    node_name: &str,
    client: &NodeHttpClient,
    attack: Option<&AttackEvidence>,
    cache: &mut HeaderCache,
    previous_lib: Option<(HeaderId, u64)>,
) -> FinalityNodeObservation {
    let observed_at_unix_ms = unix_now_ms();
    let consensus = match client.consensus_info().await {
        Ok(consensus) => consensus,
        Err(error) => {
            return unresponsive_observation(node_name, &error.to_string());
        }
    };
    let info = &consensus.cryptarchia_info;
    let tip_id = info.tip;
    let lib_id = info.lib;
    let tip_height = info.height;
    let tip_slot = u64::from(info.slot);
    let lib_slot = u64::from(info.lib_slot);
    let tip_to_lib = walk_to_target(client, tip_id, lib_id, lib_slot, cache).await;
    let tip_descends_from_lib = ancestry_presence(&tip_to_lib, lib_id);
    let (tip_path, finalized_path, attack_branch, finalized_branch) = if let Some(attack) = attack {
        let tip_path = walk_to_target(client, tip_id, attack.parent_id, attack.slot, cache).await;
        let finalized_path = match proven_below_target_slot_path(lib_slot, attack.slot) {
            Some(path) => path,
            None => walk_to_target(client, lib_id, attack.parent_id, attack.slot, cache).await,
        };
        let tip_branch =
            classify_attack_branch(&tip_path.links, tip_path.complete_below_slot, attack);
        let finalized_branch = classify_attack_branch(
            &finalized_path.links,
            finalized_path.complete_below_slot,
            attack,
        );
        (tip_path, finalized_path, tip_branch, finalized_branch)
    } else {
        (
            AncestryPath {
                links: tip_to_lib.links.clone(),
                target_reached: tip_to_lib.target_reached,
                complete_below_slot: tip_to_lib.complete_below_slot,
                error: tip_to_lib.error.clone(),
            },
            AncestryPath {
                links: Vec::new(),
                target_reached: true,
                complete_below_slot: true,
                error: None,
            },
            AttackBranch::None,
            AttackBranch::None,
        )
    };
    let continuity = if let Some((previous_id, previous_slot)) = previous_lib {
        let path = if previous_id == lib_id {
            AncestryPath {
                links: Vec::new(),
                target_reached: true,
                complete_below_slot: true,
                error: None,
            }
        } else {
            walk_between_finalized_blocks(
                client,
                lib_id,
                lib_slot,
                previous_id,
                previous_slot,
                cache,
            )
            .await
        };
        if previous_id == lib_id {
            "unchanged"
        } else {
            match ancestry_presence(&path, previous_id) {
                Some(true) => "verified",
                Some(false) => "regression_or_unrelated",
                None => "unverified",
            }
        }
    } else {
        "baseline"
    };
    let consensus_state = ConsensusState {
        tip_id,
        tip_id_text: tip_id.encode_hex::<String>(),
        tip_height,
        tip_slot,
        lib_id,
        lib_id_text: lib_id.encode_hex::<String>(),
        lib_slot,
        tip_descends_from_lib,
    };
    let mut snapshot = json!({
        "node": node_name,
        "observed_at_unix_ms": observed_at_unix_ms,
        "responsive": true,
        "phase": format!("{:?}", consensus.phase),
        "tip_id": consensus_state.tip_id_text,
        "tip_height": tip_height,
        "tip_slot": tip_slot,
        "lib_id": consensus_state.lib_id_text,
        "lib_slot": lib_slot,
        "tip_descends_from_lib": tip_descends_from_lib,
        "tip_to_lib_ancestry": ancestry_path_json(&tip_to_lib),
        "lib_continuity": continuity,
        "lib_continuity_ancestry": previous_lib.map(|(id, slot)| json!({
            "previous_lib_id": id.encode_hex::<String>(),
            "previous_lib_slot": slot,
            "new_lib_id": consensus_state.lib_id_text,
            "new_lib_slot": lib_slot,
            "status": continuity,
        })),
        "header_lookup_failures": header_cache_failures_json(cache),
    });
    if let Some(attack) = attack {
        snapshot["attack_slot"] = json!(attack.slot);
        snapshot["canonical_attack_branch"] = attack_branch_json(&attack_branch, attack);
        snapshot["finalized_attack_branch"] = attack_branch_json(&finalized_branch, attack);
        snapshot["tip_path_to_attack_parent"] = json!(ancestry_path_json(&tip_path));
        snapshot["finalized_path_to_attack_parent"] = json!(ancestry_path_json(&finalized_path));
    }
    FinalityNodeObservation {
        snapshot,
        consensus: Some(consensus_state),
        tip_path_complete: tip_path.complete_below_slot || tip_path.target_reached,
        finalized_path_complete: finalized_path.complete_below_slot
            || finalized_path.target_reached,
        attack_branch,
        finalized_attack_branch: finalized_branch,
        continuity: continuity.to_owned(),
    }
}

fn tip_from_observation(observation: &FinalityNodeObservation) -> NodeTip {
    let state = observation
        .consensus
        .as_ref()
        .expect("tip requested from a responsive observation");
    NodeTip {
        id: state.tip_id,
        id_text: state.tip_id_text.clone(),
        height: state.tip_height,
        slot: state.tip_slot,
        observed_at_unix_ms: observation.snapshot["observed_at_unix_ms"]
            .as_u64()
            .unwrap_or_default(),
    }
}

fn honest_observations(
    observations: &[FinalityNodeObservation],
    participants: &SecurityAuditParticipants,
) -> Vec<FinalityNodeObservation> {
    participants
        .honest_nodes
        .iter()
        .filter_map(|name| {
            observations
                .iter()
                .find(|observation| observation.snapshot["node"].as_str() == Some(name.as_str()))
        })
        .map(|observation| FinalityNodeObservation {
            snapshot: observation.snapshot.clone(),
            consensus: observation.consensus.clone(),
            tip_path_complete: observation.tip_path_complete,
            finalized_path_complete: observation.finalized_path_complete,
            attack_branch: observation.attack_branch.clone(),
            finalized_attack_branch: observation.finalized_attack_branch.clone(),
            continuity: observation.continuity.clone(),
        })
        .collect()
}

async fn sample_mempools(
    world: &CucumberWorld,
    participants: &SecurityAuditParticipants,
    submitted: &[Value],
) -> Vec<Value> {
    let submitted_hashes = submitted
        .iter()
        .filter_map(|transaction| transaction["transaction_hash"].as_str().map(str::to_owned))
        .collect::<BTreeSet<_>>();
    let mut samples = Vec::with_capacity(participants.honest_nodes.len() + 1);
    for name in node_names(participants) {
        let Some(node) = world.nodes_info.get(name) else {
            samples
                .push(json!({"node": name, "available": false, "error": "node was not started"}));
            continue;
        };
        match node.client.test_mempool_view().await {
            Ok(hashes) => {
                let pending = hashes
                    .iter()
                    .map(tx_hash_to_hex)
                    .filter(|hash| submitted_hashes.contains(hash))
                    .collect::<Vec<_>>();
                samples.push(json!({
                    "node": name,
                    "available": true,
                    "pending_submitted_transaction_count": pending.len(),
                    "pending_submitted_transaction_hashes": pending,
                }));
            }
            Err(error) => samples.push(json!({
                "node": name,
                "available": false,
                "error": error.to_string(),
            })),
        }
    }
    samples
}

#[expect(
    clippy::too_many_lines,
    reason = "Preserves all construction, body, storage, and per-peer acceptance evidence"
)]
async fn summarize_attack_opportunity(
    world: &CucumberWorld,
    participants: &SecurityAuditParticipants,
    opportunity: &ProducerOpportunity,
    node_logs: &BTreeMap<String, Vec<ObservedLogLine>>,
) -> (Value, AttackEvidence) {
    let evidence = attack_evidence_from_log(opportunity).unwrap_or_else(|_| AttackEvidence {
        ordinary_id: HeaderId::from([0; 32]),
        ordinary_id_text: String::new(),
        parent_id: HeaderId::from([0; 32]),
        parent_id_text: String::new(),
        slot: 0,
        body_root: String::new(),
        transaction_hashes: Vec::new(),
        siblings: Vec::new(),
    });
    let ordinary_body = fetch_body_evidence(
        world,
        &participants.producer,
        opportunity.ordinary.get("ordinary_block_id"),
    )
    .await;
    let mut ordinary_storage_by_node = Vec::new();
    for node_name in node_names(participants) {
        ordinary_storage_by_node.push(json!({
            "node": node_name,
            "block_api": fetch_body_evidence(
                world,
                node_name,
                opportunity.ordinary.get("ordinary_block_id"),
            ).await,
        }));
    }
    let sibling_ids = opportunity
        .siblings
        .iter()
        .filter_map(|(_, sibling)| sibling.get("sibling_block_id").cloned())
        .collect::<BTreeSet<_>>();
    let stages = receiver_stages(
        node_logs,
        &sibling_ids,
        &opportunity.ordinary,
        &participants
            .honest_nodes
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>(),
    );
    let accepted_bodies = query_accepted_blocks(world, opportunity, &stages).await;
    let mut sibling_records = Vec::new();
    let mut ordered_acceptance_nodes = BTreeSet::new();
    for (index, sibling) in &opportunity.siblings {
        let sibling_id = sibling.get("sibling_block_id").cloned().unwrap_or_default();
        let body = fetch_body_evidence(world, &participants.producer, Some(&sibling_id)).await;
        let mut block_storage_by_node = Vec::new();
        for node_name in node_names(participants) {
            block_storage_by_node.push(json!({
                "node": node_name,
                "block_api": fetch_body_evidence(world, node_name, Some(&sibling_id)).await,
            }));
        }
        let mut peer_stages = Vec::new();
        for node_name in participants.honest_nodes.iter().map(String::as_str) {
            let item = stages.get(&(node_name.to_owned(), sibling_id.clone()));
            if let Some(item) = item {
                if item.has_ordered_blend_to_acceptance() {
                    ordered_acceptance_nodes.insert(node_name.to_owned());
                }
                peer_stages.push(json!({
                    "node": node_name,
                    "ordered_blend_to_acceptance": item.has_ordered_blend_to_acceptance(),
                    "matching_events": item.matching_events.iter().map(|event| json!({
                        "stage": event.stage,
                        "parent_block_id": event.parent_block_id,
                        "slot": event.slot,
                        "timestamp": event.timestamp,
                        "reason": event.reason,
                        "log_path": event.path,
                        "log_offset": event.offset,
                    })).collect::<Vec<_>>(),
                    "mismatched_events": item.mismatched_events.iter().map(|event| json!({
                        "stage": event.stage,
                        "parent_block_id": event.parent_block_id,
                        "slot": event.slot,
                        "reason": event.reason,
                        "log_path": event.path,
                        "log_offset": event.offset,
                    })).collect::<Vec<_>>(),
                }));
            } else {
                peer_stages.push(json!({
                    "node": node_name,
                    "ordered_blend_to_acceptance": false,
                    "matching_events": [],
                    "mismatched_events": [],
                }));
            }
        }
        let enqueue_event = sibling.clone();
        let construction_failures =
            matching_construction_failures(node_logs, &participants.producer, &sibling_id);
        sibling_records.push(json!({
            "sibling_index": index,
            "sibling_block_id": sibling_id,
            "relay_enqueue_event": enqueue_event,
            "producer_block_api": body,
            "block_storage_by_node": block_storage_by_node,
            "honest_peer_validation": peer_stages,
            "construction_failures": construction_failures,
        }));
    }
    let ordinary_body_root = ordinary_body["body_root"].as_str().unwrap_or_default();
    let mut evidence = evidence;
    ordinary_body_root.clone_into(&mut evidence.body_root);
    evidence.transaction_hashes = ordinary_body["transaction_hashes"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .map(str::to_owned)
        .collect();
    let body_integrity = compare_sibling_bodies(&ordinary_body, &sibling_records);
    let record = json!({
        "ordinary_block_id": opportunity.ordinary.get("ordinary_block_id"),
        "parent_block_id": opportunity.ordinary.get("parent_block_id"),
        "slot": opportunity.ordinary.get("slot"),
        "ordinary_submission": opportunity.ordinary,
        "ordinary_submitted_line": opportunity.ordinary_line,
        "ordinary_constructed": true,
        "ordinary_relay_enqueued": identity_bool(&opportunity.ordinary, "ordinary_relay_enqueued"),
        "ordinary_before_sibling_construction": true,
        "ordinary_body": ordinary_body,
        "sibling_construction_and_enqueue": opportunity.siblings.iter().map(|(line, event)| json!({
            "line_index": line,
            "sibling_index": event.get("sibling_index"),
            "sibling_block_id": event.get("sibling_block_id"),
            "relay_enqueued": identity_bool(event, "relay_enqueued"),
            "event": event,
        })).collect::<Vec<_>>(),
        "timing": opportunity.timing,
        "timing_line": opportunity.timing_line,
        "sibling_blocks": sibling_records,
        "ordinary_block_storage_by_node": ordinary_storage_by_node,
        "accepted_block_api_observations": accepted_bodies,
        "body_integrity": body_integrity,
        "ordered_acceptance_peer_count": ordered_acceptance_nodes.len(),
        "ordered_acceptance_peer_names": ordered_acceptance_nodes,
        "ordered_acceptance_by_distinct_peer": !ordered_acceptance_nodes.is_empty(),
        "attack_evidence": {
            "ordinary_block_id": evidence.ordinary_id_text,
            "parent_block_id": evidence.parent_id_text,
            "attack_slot": evidence.slot,
            "body_root": evidence.body_root,
            "ordered_transaction_hashes": evidence.transaction_hashes,
            "siblings": evidence.siblings.iter().map(|(index, sibling)| json!({
                "sibling_index": index,
                "sibling_block_id": sibling.id.encode_hex::<String>(),
            })).collect::<Vec<_>>(),
        },
    });
    (record, evidence)
}

fn compare_sibling_bodies(ordinary: &Value, siblings: &[Value]) -> Value {
    let ordinary_available = ordinary["available"].as_bool() == Some(true);
    let ordinary_root = ordinary_available
        .then(|| ordinary["body_root"].as_str())
        .flatten();
    let ordinary_hashes = ordinary_available
        .then(|| ordinary["transaction_hashes"].as_array())
        .flatten()
        .filter(|hashes| hashes.iter().all(Value::is_string));
    let mut retrieved = 0;
    let mut roots = (0, 0);
    let mut hashes = (0, 0);
    for sibling in siblings {
        let body = &sibling["producer_block_api"];
        if body["available"].as_bool() != Some(true) {
            continue;
        }
        retrieved += 1;
        if let (Some(expected), Some(actual)) = (ordinary_root, body["body_root"].as_str()) {
            roots.0 += 1;
            roots.1 += usize::from(expected == actual);
        }
        if let (Some(expected), Some(actual)) = (
            ordinary_hashes,
            body["transaction_hashes"]
                .as_array()
                .filter(|values| values.iter().all(Value::is_string)),
        ) {
            hashes.0 += 1;
            hashes.1 += usize::from(expected == actual);
        }
    }
    json!({
        "retrieved_sibling_bodies": retrieved,
        "sibling_records": siblings.len(),
        "body_root": comparison_outcome(roots.0, roots.1, siblings.len()),
        "ordered_transaction_hashes": comparison_outcome(hashes.0, hashes.1, siblings.len()),
    })
}

fn comparison_outcome(compared: usize, matched: usize, total: usize) -> Value {
    json!({
        "status": if matched < compared { "mismatched" } else if compared == 0 { "unverified" } else { "matched" },
        "comparisons": compared,
        "successful_comparisons": matched,
        "all_siblings_compared_and_matched": total > 0 && compared == total && matched == total,
    })
}

async fn canonical_membership_by_node(
    world: &CucumberWorld,
    participants: &SecurityAuditParticipants,
    attack: &AttackEvidence,
    tips: &BTreeMap<String, NodeTip>,
    libs: &BTreeMap<String, (HeaderId, u64)>,
    caches: &mut BTreeMap<String, HeaderCache>,
) -> Value {
    let mut memberships = Vec::new();
    for node_name in participants.honest_nodes.iter().map(String::as_str) {
        let Some(node) = world.nodes_info.get(node_name) else {
            memberships.push(
                json!({"node": node_name, "status": "unverified", "error": "node was not started"}),
            );
            continue;
        };
        let (Some(tip), Some((lib_id, lib_slot)), Some(cache)) = (
            tips.get(node_name),
            libs.get(node_name),
            caches.get_mut(node_name),
        ) else {
            memberships.push(json!({"node": node_name, "status": "unverified", "error": "tip, LIB, or cache baseline was missing"}));
            continue;
        };
        let canonical_path =
            walk_to_target(&node.client, tip.id, attack.parent_id, attack.slot, cache).await;
        let canonical = classify_attack_branch(
            &canonical_path.links,
            canonical_path.complete_below_slot,
            attack,
        );
        let finalized_path = if *lib_slot < attack.slot {
            proven_below_target_slot_path(*lib_slot, attack.slot)
                .expect("LIB below attack slot has an empty proven prefix")
        } else {
            walk_to_target(&node.client, *lib_id, attack.parent_id, attack.slot, cache).await
        };
        let finalized = classify_attack_branch(
            &finalized_path.links,
            finalized_path.complete_below_slot,
            attack,
        );
        memberships.push(json!({
            "node": node_name,
            "observed_at_unix_ms": tip.observed_at_unix_ms,
            "tip_id": tip.id_text,
            "tip_height": tip.height,
            "lib_id": lib_id.encode_hex::<String>(),
            "lib_slot": lib_slot,
            "canonical_attack_branch": attack_branch_json(&canonical, attack),
            "canonical_ancestry": ancestry_path_json(&canonical_path),
            "finalized_attack_branch": attack_branch_json(&finalized, attack),
            "finalized_ancestry": ancestry_path_json(&finalized_path),
        }));
    }
    Value::Array(memberships)
}

async fn fetch_body_evidence(
    world: &CucumberWorld,
    node_name: &str,
    block_id: Option<&String>,
) -> Value {
    let Some(block_id) = block_id else {
        return json!({"available": false, "error": "block ID was missing"});
    };
    let id = match parse_header_id(block_id) {
        Ok(id) => id,
        Err(error) => return json!({"available": false, "error": error}),
    };
    let Some(node) = world.nodes_info.get(node_name) else {
        return json!({"available": false, "error": format!("{node_name} was not started")});
    };
    match node.client.block(&id).await {
        Ok(Some(block)) => {
            let body = summarize_api_block(&block);
            json!({
                "available": true,
                "block_id": body.block_id,
                "parent_block_id": body.parent_block_id,
                "slot": body.slot,
                "body_root": body.body_root,
                "transaction_count": body.transaction_hashes.len(),
                "transaction_hashes": body.transaction_hashes,
                "evidence_source": format!("NodeHttpClient::block at {node_name}"),
            })
        }
        Ok(None) => json!({"available": false, "error": "block API returned no block"}),
        Err(error) => json!({"available": false, "error": error.to_string()}),
    }
}

fn matching_construction_failures(
    node_logs: &BTreeMap<String, Vec<ObservedLogLine>>,
    producer: &str,
    ordinary_id: &str,
) -> Vec<Value> {
    node_logs
        .get(producer)
        .into_iter()
        .flatten()
        .filter_map(|line| {
            let fields = parse_log_fields(&line.text);
            (fields.get("event").map(String::as_str)
                == Some("security_audit_sibling_construction_failed")
                && fields.get("ordinary_block_id").map(String::as_str) == Some(ordinary_id))
            .then(|| {
                json!({
                    "log_path": line.path,
                    "log_offset": line.offset,
                    "fields": fields,
                    "raw": line.text,
                })
            })
        })
        .collect()
}

async fn align_observations(
    world: &CucumberWorld,
    observations: &[FinalityNodeObservation],
    caches: &mut BTreeMap<String, HeaderCache>,
    names: &[&str],
    max_tip_height_skew: u64,
) -> Value {
    let states = names
        .iter()
        .filter_map(|name| {
            observations
                .iter()
                .find(|observation| observation.snapshot["node"].as_str() == Some(*name))
                .and_then(|observation| observation.consensus.as_ref())
                .map(|state| ((*name).to_owned(), state.clone()))
        })
        .collect::<Vec<_>>();
    if states.len() != names.len() {
        return json!({
            "aligned": false,
            "reason": "one or more compared nodes were unresponsive",
            "responsive_node_count": states.len(),
            "expected_node_count": names.len(),
        });
    }
    let min_height = states
        .iter()
        .map(|(_, state)| state.tip_height)
        .min()
        .unwrap_or_default();
    let max_height = states
        .iter()
        .map(|(_, state)| state.tip_height)
        .max()
        .unwrap_or_default();
    let height_skew = max_height.saturating_sub(min_height);
    if !tip_height_skew_allowed(height_skew, max_tip_height_skew) {
        return json!({
            "aligned": false,
            "reason": "tip heights exceed the configured tolerance",
            "common_height": min_height,
            "height_skew": height_skew,
            "max_allowed_height_skew": max_tip_height_skew,
        });
    }
    let mut ids = Vec::new();
    let mut errors = Vec::new();
    for (name, state) in &states {
        let (Some(node), Some(cache)) = (world.nodes_info.get(name), caches.get_mut(name)) else {
            errors.push(json!({"node": name, "error": "node or ancestry cache was missing"}));
            continue;
        };
        match block_id_at_height(
            &node.client,
            state.tip_id,
            state.tip_height,
            min_height,
            cache,
        )
        .await
        {
            Ok(id) => ids.push((name, id)),
            Err(error) => errors.push(json!({"node": name, "error": error})),
        }
    }
    if !errors.is_empty() || ids.len() != names.len() {
        return json!({
            "aligned": false,
            "reason": "canonical ancestry at the common height was unavailable",
            "common_height": min_height,
            "height_skew": height_skew,
            "block_ids_at_common_height": ids.iter().map(|(node, id)| json!({
                "node": node,
                "block_id": id.encode_hex::<String>(),
            })).collect::<Vec<_>>(),
            "errors": errors,
        });
    }
    let aligned = canonical_tips_aligned(
        height_skew,
        &ids.iter().map(|(_, id)| *id).collect::<Vec<_>>(),
        max_tip_height_skew,
    );
    json!({
        "aligned": aligned,
        "common_height": min_height,
        "height_skew": height_skew,
        "block_ids_at_common_height": ids.iter().map(|(node, id)| json!({
            "node": node,
            "block_id": id.encode_hex::<String>(),
        })).collect::<Vec<_>>(),
        "errors": errors,
    })
}

const fn tip_height_skew_allowed(actual: u64, maximum: u64) -> bool {
    actual <= maximum
}

async fn observe_tip_transition(
    node_name: &str,
    client: &NodeHttpClient,
    previous: NodeTip,
    current: NodeTip,
    caches: &mut BTreeMap<String, HeaderCache>,
    attack_candidates: &BTreeSet<HeaderId>,
) -> Value {
    let Some(cache) = caches.get_mut(node_name) else {
        return json!({
            "node": node_name,
            "status": "unverified",
            "previous_tip_id": previous.id_text,
            "previous_tip_height": previous.height,
            "new_tip_id": current.id_text,
            "new_tip_height": current.height,
            "error": "header cache was not allocated",
        });
    };
    let path = walk_to_target(client, current.id, previous.id, previous.slot, cache).await;
    let relation = ancestry_presence(&path, previous.id);
    let transition = classify_tip_transition(previous.id, current.id, relation);
    let status = match transition {
        TipTransition::Descendant => "descendant_of_previous_tip",
        TipTransition::Reorganization => "reorganization",
        TipTransition::Unverified => "unverified",
        TipTransition::Unchanged => "unchanged",
    };
    if transition != TipTransition::Reorganization {
        return json!({
            "node": node_name,
            "status": status,
            "previous_observed_at_unix_ms": previous.observed_at_unix_ms,
            "observed_at_unix_ms": current.observed_at_unix_ms,
            "previous_tip_id": previous.id_text,
            "previous_tip_height": previous.height,
            "new_tip_id": current.id_text,
            "new_tip_height": current.height,
            "new_tip_descends_from_previous_tip": relation,
            "supporting_ancestry": ancestry_path_json(&path),
        });
    }
    let lca = find_last_common_ancestor(
        client,
        previous.clone(),
        current.clone(),
        cache,
        attack_candidates,
    )
    .await;
    let lca_evidence = match lca {
        Ok((ancestor, ancestor_height, old_branch, new_branch, old_attacks, new_attacks)) => {
            match reorganization_depths(previous.height, current.height, ancestor_height) {
                Some((rolled_back, new_count)) => json!({
                    "status": "verified",
                    "block_id": ancestor.encode_hex::<String>(),
                    "height": ancestor_height,
                    "rolled_back_block_count": rolled_back,
                    "new_branch_block_count": new_count,
                    "old_branch_block_ids": old_branch.iter().map(hex::ToHex::encode_hex::<String>).collect::<Vec<_>>(),
                    "new_branch_block_ids": new_branch.iter().map(hex::ToHex::encode_hex::<String>).collect::<Vec<_>>(),
                    "attack_candidate_ids_on_previous_branch": old_attacks,
                    "attack_candidate_ids_on_new_branch": new_attacks,
                }),
                None => json!({
                    "status": "unverified",
                    "error": "common-ancestor height exceeds one or both observed tip heights",
                }),
            }
        }
        Err(error) => json!({
            "status": "unverified",
            "error": error,
        }),
    };
    json!({
        "node": node_name,
        "status": status,
        "previous_observed_at_unix_ms": previous.observed_at_unix_ms,
        "observed_at_unix_ms": current.observed_at_unix_ms,
        "previous_tip_id": previous.id_text,
        "previous_tip_height": previous.height,
        "previous_tip_slot": previous.slot,
        "new_tip_id": current.id_text,
        "new_tip_height": current.height,
        "new_tip_slot": current.slot,
        "new_tip_descends_from_previous_tip": false,
        "supporting_ancestry": ancestry_path_json(&path),
        "last_common_ancestor": lca_evidence,
        "reorganization_depth_is_lower_bound": true,
    })
}

async fn find_last_common_ancestor(
    client: &NodeHttpClient,
    previous: NodeTip,
    current: NodeTip,
    cache: &mut HeaderCache,
    attack_candidates: &BTreeSet<HeaderId>,
) -> Result<
    (
        HeaderId,
        u64,
        Vec<HeaderId>,
        Vec<HeaderId>,
        Vec<String>,
        Vec<String>,
    ),
    String,
> {
    let mut old_id = previous.id;
    let mut new_id = current.id;
    let mut old_height = previous.height;
    let mut new_height = current.height;
    let mut old_branch = Vec::new();
    let mut new_branch = Vec::new();
    for _ in 0..FINALITY_MAX_ANCESTRY_DEPTH {
        if old_id == new_id {
            let old_candidates = attack_candidate_members(&old_branch, old_id, attack_candidates);
            let new_candidates = attack_candidate_members(&new_branch, new_id, attack_candidates);
            return Ok((
                old_id,
                old_height,
                old_branch,
                new_branch,
                old_candidates,
                new_candidates,
            ));
        }
        if old_height >= new_height {
            let link = fetch_header_link(client, old_id, cache).await?;
            if link.parent == old_id {
                return Err(format!(
                    "self-parented header {} reached before a common ancestor",
                    old_id.encode_hex::<String>()
                ));
            }
            old_branch.push(old_id);
            old_id = link.parent;
            old_height = old_height.saturating_sub(1);
        }
        if new_height >= old_height && old_id != new_id {
            let link = fetch_header_link(client, new_id, cache).await?;
            if link.parent == new_id {
                return Err(format!(
                    "self-parented header {} reached before a common ancestor",
                    new_id.encode_hex::<String>()
                ));
            }
            new_branch.push(new_id);
            new_id = link.parent;
            new_height = new_height.saturating_sub(1);
        }
    }
    Err(format!(
        "common ancestor search exceeded {FINALITY_MAX_ANCESTRY_DEPTH} headers"
    ))
}

const fn reorganization_depths(
    previous_tip_height: u64,
    new_tip_height: u64,
    ancestor_height: u64,
) -> Option<(u64, u64)> {
    if ancestor_height > previous_tip_height || ancestor_height > new_tip_height {
        return None;
    }
    Some((
        previous_tip_height - ancestor_height,
        new_tip_height - ancestor_height,
    ))
}

fn attack_candidate_members(
    branch: &[HeaderId],
    ancestor: HeaderId,
    candidates: &BTreeSet<HeaderId>,
) -> Vec<String> {
    branch
        .iter()
        .copied()
        .chain(std::iter::once(ancestor))
        .filter(|id| candidates.contains(id))
        .map(|id| id.encode_hex::<String>())
        .collect()
}

struct RecoveryNodeContext<'a> {
    world: &'a CucumberWorld,
    honest_nodes: &'a [&'a str],
    attack: Option<&'a AttackEvidence>,
    header_caches: &'a mut BTreeMap<String, HeaderCache>,
    last_verified_libs: &'a mut BTreeMap<String, (HeaderId, u64)>,
    last_tips: &'a mut BTreeMap<String, NodeTip>,
    attack_candidates: &'a BTreeSet<HeaderId>,
    initial_tips: &'a BTreeMap<String, NodeTip>,
    progress_by_node: &'a mut BTreeMap<String, bool>,
    recovery_tip_advance_counts: &'a mut BTreeMap<String, usize>,
    recovery_lib_advance_counts: &'a mut BTreeMap<String, usize>,
    last_tip_progress_at: &'a mut BTreeMap<String, u64>,
    last_lib_progress_at: &'a mut BTreeMap<String, u64>,
    tip_stall_intervals: &'a mut Vec<Value>,
    lib_stall_intervals: &'a mut Vec<Value>,
    tip_transitions: &'a mut Vec<Value>,
    tip_reorganizations: &'a mut Vec<Value>,
    tip_reorganizations_unverified: &'a mut Vec<Value>,
    responsive_failure_observations: &'a mut Vec<Value>,
    continuity_regressions: &'a mut Vec<Value>,
    continuity_unverified: &'a mut Vec<Value>,
    ancestry_gaps: &'a mut Vec<Value>,
    any_unverified_ancestry: &'a mut bool,
}

#[expect(
    clippy::too_many_lines,
    reason = "Tracks each sampled node's liveness and safety evidence together"
)]
async fn observe_recovery_nodes(context: RecoveryNodeContext<'_>) -> Vec<FinalityNodeObservation> {
    let RecoveryNodeContext {
        world,
        honest_nodes,
        attack,
        header_caches,
        last_verified_libs,
        last_tips,
        attack_candidates,
        initial_tips,
        progress_by_node,
        recovery_tip_advance_counts,
        recovery_lib_advance_counts,
        last_tip_progress_at,
        last_lib_progress_at,
        tip_stall_intervals,
        lib_stall_intervals,
        tip_transitions,
        tip_reorganizations,
        tip_reorganizations_unverified,
        responsive_failure_observations,
        continuity_regressions,
        continuity_unverified,
        ancestry_gaps,
        any_unverified_ancestry,
    } = context;
    let mut observations = Vec::with_capacity(honest_nodes.len());
    for &node_name in honest_nodes {
        let Some(node) = world.nodes_info.get(node_name) else {
            observations.push(unresponsive_observation(node_name, "node was not started"));
            continue;
        };
        let Some(cache) = header_caches.get_mut(node_name) else {
            observations.push(unresponsive_observation(
                node_name,
                "header cache was not allocated",
            ));
            continue;
        };
        let previous_lib = last_verified_libs.get(node_name).copied();
        let mut observation =
            observe_sustained_node(node_name, &node.client, attack, cache, previous_lib).await;
        if let Some(state) = observation.consensus.as_ref() {
            if previous_lib.is_some_and(|(previous_id, _)| previous_id == state.lib_id) {
                "unchanged".clone_into(&mut observation.continuity);
                observation.snapshot["lib_continuity"] = json!("unchanged");
            }
            if observation.continuity == "verified" || previous_lib.is_none() {
                last_verified_libs.insert(node_name.to_owned(), (state.lib_id, state.lib_slot));
                if previous_lib.is_none() {
                    "baseline".clone_into(&mut observation.continuity);
                }
            }
            if observation.continuity == "verified" {
                let observed_at = observation.snapshot["observed_at_unix_ms"]
                    .as_u64()
                    .unwrap_or_else(unix_now_ms);
                if let Some(started_at) =
                    last_lib_progress_at.insert(node_name.to_owned(), observed_at)
                {
                    lib_stall_intervals.push(json!({
                        "node": node_name,
                        "started_at_unix_ms": started_at,
                        "ended_at_unix_ms": observed_at,
                        "duration_ms": observed_at.saturating_sub(started_at),
                    }));
                }
                *recovery_lib_advance_counts
                    .entry(node_name.to_owned())
                    .or_default() += 1;
            }
            if let Some(previous) = last_tips.get(node_name).cloned() {
                let current = tip_from_observation(&observation);
                let tip_changed = previous.id != current.id;
                if tip_changed {
                    let transition = observe_tip_transition(
                        node_name,
                        &node.client,
                        previous,
                        current.clone(),
                        header_caches,
                        attack_candidates,
                    )
                    .await;
                    if transition["status"].as_str() == Some("reorganization") {
                        if transition["last_common_ancestor"]["status"].as_str() == Some("verified")
                        {
                            tip_reorganizations.push(transition.clone());
                        } else {
                            tip_reorganizations_unverified.push(transition.clone());
                        }
                    } else if transition["status"].as_str() == Some("unverified") {
                        tip_reorganizations_unverified.push(transition.clone());
                    }
                    tip_transitions.push(transition);
                    if let Some(started_at) = last_tip_progress_at
                        .insert(node_name.to_owned(), current.observed_at_unix_ms)
                    {
                        tip_stall_intervals.push(json!({
                            "node": node_name,
                            "started_at_unix_ms": started_at,
                            "ended_at_unix_ms": current.observed_at_unix_ms,
                            "duration_ms": current.observed_at_unix_ms.saturating_sub(started_at),
                        }));
                    }
                    *recovery_tip_advance_counts
                        .entry(node_name.to_owned())
                        .or_default() += 1;
                }
                if initial_tips
                    .get(node_name)
                    .is_some_and(|baseline| current.height > baseline.height)
                {
                    progress_by_node.insert(node_name.to_owned(), true);
                }
                last_tips.insert(node_name.to_owned(), current);
            }
        } else {
            responsive_failure_observations.push(observation.snapshot.clone());
        }
        if observation.continuity == "regression_or_unrelated" {
            continuity_regressions.push(observation.snapshot.clone());
        } else if observation.continuity == "unverified" {
            continuity_unverified.push(observation.snapshot.clone());
        }
        if !observation.tip_path_complete
            || observation
                .snapshot
                .pointer("/tip_descends_from_lib")
                .and_then(Value::as_bool)
                != Some(true)
            || !observation.finalized_path_complete
        {
            ancestry_gaps.push(observation.snapshot.clone());
            *any_unverified_ancestry = true;
        }
        observations.push(observation);
    }
    observations
}

#[expect(
    clippy::too_many_lines,
    reason = "The recovery verdict and evidence share sampled observations"
)]
#[expect(
    clippy::cognitive_complexity,
    reason = "The recovery verdict binds finality, ancestry, liveness, and shutdown evidence"
)]
async fn run_recovery_window(
    world: &CucumberWorld,
    boundary: RecoveryBoundary,
    stability: Duration,
    timeout: Duration,
    max_tip_height_skew: u64,
) -> StepResult {
    let participants =
        audit_participants(world).map_err(|message| StepError::InvalidArgument { message })?;
    let producer_name = participants.producer.as_str();
    let honest_nodes = participants
        .honest_nodes
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();
    let attack_enabled = boundary == RecoveryBoundary::LastAttackSlot;
    if stability.is_zero() || stability > timeout {
        return Err(StepError::InvalidArgument {
            message: "recovery stability must be positive and no longer than its timeout"
                .to_owned(),
        });
    }
    let recovery_entry = StdInstant::now();
    let recovery_entry_unix_ms = unix_now_ms();
    let recorded_producer_stop = world.lifecycle.node_stopped_at.get(producer_name).copied();
    let recovery_started = recorded_producer_stop.unwrap_or(recovery_entry);
    let recovery_started_at =
        recovery_entry_unix_ms.saturating_sub(recorded_producer_stop.map_or(0, |stopped_at| {
            u64::try_from(recovery_entry.duration_since(stopped_at).as_millis()).unwrap_or(u64::MAX)
        }));
    let recovery_deadline = recovery_started + timeout;
    let artifact_dir = world.lifecycle.scenario_base_dir.join(ARTIFACT_DIRECTORY);
    let active_path = artifact_dir.join("active-observations.json");
    let active_bytes = fs::read(&active_path).map_err(|error| StepError::StepFail {
        message: format!(
            "cannot read active observations {}: {error}",
            active_path.display()
        ),
    })?;
    let active: Value =
        serde_json::from_slice(&active_bytes).map_err(|error| StepError::StepFail {
            message: format!(
                "cannot parse active observations {}: {error}",
                active_path.display()
            ),
        })?;
    let opportunity_records = active["opportunities"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    let attack = attack_enabled
        .then(|| {
            opportunity_records
                .iter()
                .filter_map(attack_evidence_from_record)
                .max_by_key(|evidence| evidence.slot)
        })
        .flatten();
    let mut last_attacked_slot = attack.as_ref().map(|attack| attack.slot);
    let mut boundary_slot = if attack_enabled {
        last_attacked_slot.unwrap_or_default()
    } else if let Some(slot) = active_control_boundary_slot(&active, &honest_nodes) {
        slot
    } else {
        let path = artifact_dir.join("recovery-observations.json");
        write_json(
            &path,
            &json!({ "status": "unverified", "failure": "missing an honest node's active-window LIB", "active_observations": active_path }),
        )?;
        return Err(StepError::StepFail {
            message: format!(
                "control baseline is unverified; evidence: {}",
                path.display()
            ),
        });
    };
    let producer = world
        .nodes_info
        .get(producer_name)
        .ok_or_else(|| StepError::StepFail {
            message: format!("{producer_name} was not started before recovery"),
        })?;
    let stop_step_seen = world.lifecycle.node_stopped_at.contains_key(producer_name);
    let producer_probe = match producer.client.consensus_info().await {
        Ok(consensus) => json!({
            "responsive_after_stop_step": true,
            "tip_id": consensus.cryptarchia_info.tip.encode_hex::<String>(),
            "tip_height": consensus.cryptarchia_info.height,
        }),
        Err(error) => json!({
            "responsive_after_stop_step": false,
            "error": error.to_string(),
        }),
    };
    let producer_confirmed_stopped =
        stop_step_seen && producer_probe["responsive_after_stop_step"].as_bool() == Some(false);
    let mut post_stop_observation = PostStopLogObservation::new(world);
    post_stop_observation.sample(producer_name);
    let mut post_stop_logs_quiet = post_stop_observation.quiet();
    let (producer_log, producer_log_read_error) =
        match read_runtime_node_logs(&producer.runtime_dir, &producer.runtime_name) {
            Ok(log) => (log, None),
            Err(error) => (String::new(), Some(error)),
        };
    let final_producer_opportunities = producer_equivocation_evidence(&producer_log);
    let final_complete_opportunities =
        find_producer_opportunities(&observed_log_lines(&producer_log, "complete-producer-log"));
    let active_opportunity_ids = opportunity_records
        .iter()
        .filter_map(|record| record["ordinary_block_id"].as_str().map(str::to_owned))
        .collect::<BTreeSet<_>>();
    let final_logged_ids = final_complete_opportunities
        .iter()
        .filter_map(|opportunity| opportunity.ordinary.get("ordinary_block_id").cloned())
        .collect::<BTreeSet<_>>();
    let opportunities_logged_after_active_cutoff = final_logged_ids
        .difference(&active_opportunity_ids)
        .cloned()
        .collect::<Vec<_>>();
    let active_opportunities_missing_from_final_log = active_opportunity_ids
        .difference(&final_logged_ids)
        .cloned()
        .collect::<Vec<_>>();
    let final_ordinary_ids = final_producer_opportunities
        .iter()
        .filter_map(|opportunity| opportunity["ordinary_block_id"].as_str().map(str::to_owned))
        .collect::<BTreeSet<_>>();
    let incomplete_producer_opportunity_ids = final_ordinary_ids
        .difference(&final_logged_ids)
        .cloned()
        .collect::<Vec<_>>();
    let producer_opportunity_log_verified = producer_log_read_error.is_none()
        && active_opportunities_missing_from_final_log.is_empty()
        && incomplete_producer_opportunity_ids.is_empty();
    if let Some(latest_slot) = final_complete_opportunities
        .iter()
        .filter_map(|opportunity| opportunity.ordinary.get("slot")?.parse::<u64>().ok())
        .max()
        && attack_enabled
    {
        boundary_slot = latest_slot.max(boundary_slot);
        last_attacked_slot = Some(latest_slot);
    }
    let mut last_verified_libs = parse_last_verified_libs(&active);
    let mut last_tips = parse_last_active_tips(&active);
    let initial_tips = last_tips.clone();
    let mut header_caches = node_names(participants)
        .into_iter()
        .map(|name| (name.to_owned(), HeaderCache::default()))
        .collect::<BTreeMap<_, _>>();
    let mut attack_candidates = opportunity_records
        .iter()
        .flat_map(attack_candidate_ids_from_record)
        .collect::<BTreeSet<_>>();
    attack_candidates.extend(attack_candidate_ids_from_opportunities(
        &final_complete_opportunities,
    ));
    let mut snapshots = Vec::new();
    let mut finalized_history_checks = Vec::new();
    let mut continuity_regressions = Vec::new();
    let mut continuity_unverified = Vec::new();
    let mut ancestry_gaps = active["safety_metrics"]["ancestry_gaps"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    let mut tip_transitions = Vec::new();
    let mut tip_reorganizations = Vec::new();
    let mut tip_reorganizations_unverified = Vec::new();
    let mut safety_conflict_observed =
        active["safety_metrics"]["safety_conflict_observed"].as_bool() == Some(true);
    let mut any_unverified_ancestry =
        active["safety_metrics"]["ancestry_unverified"].as_bool() != Some(false);
    let mut progress_by_node = BTreeMap::<String, bool>::new();
    let mut recovery_tip_advance_counts = honest_nodes
        .iter()
        .map(|name| ((*name).to_owned(), 0usize))
        .collect::<BTreeMap<_, _>>();
    let mut recovery_lib_advance_counts = recovery_tip_advance_counts.clone();
    let mut last_tip_progress_at = honest_nodes
        .iter()
        .map(|name| ((*name).to_owned(), recovery_started_at))
        .collect::<BTreeMap<_, _>>();
    let mut last_lib_progress_at = last_tip_progress_at.clone();
    let mut tip_stall_intervals = Vec::new();
    let mut lib_stall_intervals = Vec::new();
    let mut recovery_alignment_transitions = Vec::new();
    let mut previous_recovery_alignment = None;
    let mut responsive_failure_observations = Vec::new();
    let mut stable_since_elapsed_ms = None;
    let mut stable_completion = None;
    let mut final_alignment = json!({"aligned": false, "reason": "no recovery sample yet"});
    while StdInstant::now() < recovery_deadline {
        post_stop_observation.sample(producer_name);
        post_stop_logs_quiet = post_stop_observation.quiet();
        let observations = observe_recovery_nodes(RecoveryNodeContext {
            world,
            honest_nodes: &honest_nodes,
            attack: attack.as_ref(),
            header_caches: &mut header_caches,
            last_verified_libs: &mut last_verified_libs,
            last_tips: &mut last_tips,
            attack_candidates: &attack_candidates,
            initial_tips: &initial_tips,
            progress_by_node: &mut progress_by_node,
            recovery_tip_advance_counts: &mut recovery_tip_advance_counts,
            recovery_lib_advance_counts: &mut recovery_lib_advance_counts,
            last_tip_progress_at: &mut last_tip_progress_at,
            last_lib_progress_at: &mut last_lib_progress_at,
            tip_stall_intervals: &mut tip_stall_intervals,
            lib_stall_intervals: &mut lib_stall_intervals,
            tip_transitions: &mut tip_transitions,
            tip_reorganizations: &mut tip_reorganizations,
            tip_reorganizations_unverified: &mut tip_reorganizations_unverified,
            responsive_failure_observations: &mut responsive_failure_observations,
            continuity_regressions: &mut continuity_regressions,
            continuity_unverified: &mut continuity_unverified,
            ancestry_gaps: &mut ancestry_gaps,
            any_unverified_ancestry: &mut any_unverified_ancestry,
        })
        .await;
        let mut caches_for_pairs = header_caches;
        let history_checks = check_finalized_history_agreement(
            world,
            &observations,
            &mut caches_for_pairs,
            boundary_slot,
        )
        .await;
        header_caches = caches_for_pairs;
        safety_conflict_observed |= history_checks
            .iter()
            .any(|check| check["status"].as_str() == Some("conflicting_finalized_histories"));
        any_unverified_ancestry |= history_checks
            .iter()
            .any(|check| check["status"].as_str() == Some("ancestry_unverified"));
        finalized_history_checks.push(json!({
            "observed_at_unix_ms": unix_now_ms(),
            "pairs": history_checks,
        }));
        final_alignment = align_observations(
            world,
            &observations,
            &mut header_caches,
            &honest_nodes,
            max_tip_height_skew,
        )
        .await;
        let aligned = final_alignment["aligned"].as_bool() == Some(true);
        if previous_recovery_alignment.is_some_and(|previous| previous != aligned) {
            recovery_alignment_transitions.push(json!({
                "observed_at_unix_ms": unix_now_ms(),
                "aligned": aligned,
                "details": final_alignment,
            }));
        }
        previous_recovery_alignment = Some(aligned);
        let states = observations
            .iter()
            .filter_map(|observation| {
                Some((
                    observation.snapshot["node"].as_str()?.to_owned(),
                    observation.consensus.clone()?,
                ))
            })
            .collect::<Vec<_>>();
        let common_lib = common_finalized_lib(&states, honest_nodes.len());
        let all_responsive = states.len() == honest_nodes.len();
        let all_progressed = honest_nodes
            .iter()
            .all(|node| progress_by_node.get(*node).copied().unwrap_or(false));
        let all_ancestry_complete = observations.iter().all(|observation| {
            observation.consensus.is_some()
                && observation.tip_path_complete
                && observation.finalized_path_complete
                && observation
                    .snapshot
                    .pointer("/tip_descends_from_lib")
                    .and_then(Value::as_bool)
                    == Some(true)
        });
        let continuity_safe = continuity_regressions.is_empty()
            && continuity_unverified.is_empty()
            && observations.iter().all(|observation| {
                matches!(
                    observation.continuity.as_str(),
                    "verified" | "unchanged" | "baseline"
                )
            });
        let common_beyond_boundary = common_lib.is_some_and(|(_, slot)| slot > boundary_slot);
        let stable_predicate = producer_confirmed_stopped
            && post_stop_logs_quiet
            && all_responsive
            && all_progressed
            && final_alignment["aligned"].as_bool() == Some(true)
            && common_beyond_boundary
            && all_ancestry_complete
            && continuity_safe
            && !safety_conflict_observed
            && !any_unverified_ancestry;
        let elapsed_ms = recovery_started.elapsed().as_millis();
        let (next_stable_since, completion) = update_stable_finality_window(
            stable_predicate,
            elapsed_ms,
            stable_since_elapsed_ms,
            stability.as_millis(),
        );
        stable_since_elapsed_ms = next_stable_since;
        let snapshot = json!({
            "observed_at_unix_ms": unix_now_ms(),
            "elapsed_ms": elapsed_ms,
            "nodes": observations.iter().map(|observation| &observation.snapshot).collect::<Vec<_>>(),
            "aligned": final_alignment,
            "common_lib": common_lib.map(|(id, slot)| json!({
                "lib_id": id.encode_hex::<String>(),
                "lib_slot": slot,
            })),
            "stable_predicate": stable_predicate,
            "producer_confirmed_stopped": producer_confirmed_stopped,
            "post_stop_logs_quiet": post_stop_logs_quiet,
        });
        snapshots.push(snapshot);
        if completion.is_some() {
            stable_completion = completion;
            break;
        }
        if StdInstant::now() >= recovery_deadline {
            break;
        }
        sleep(SAMPLE_INTERVAL).await;
    }

    let PostStopLogObservation {
        setup_error: stopped_log_tail_setup_error,
        read_error: post_stop_log_read_error,
        equivocation_events: post_stop_equivocation_events,
        ..
    } = post_stop_observation;
    let recovery_ended_at = unix_now_ms();
    for &node_name in &honest_nodes {
        if let Some(started_at) = last_tip_progress_at.get(node_name) {
            let duration_ms = recovery_ended_at.saturating_sub(*started_at);
            if duration_ms > 0 {
                tip_stall_intervals.push(json!({
                    "node": node_name,
                    "started_at_unix_ms": started_at,
                    "ended_at_unix_ms": recovery_ended_at,
                    "duration_ms": duration_ms,
                    "right_censored_at_recovery_end": true,
                }));
            }
        }
        if let Some(started_at) = last_lib_progress_at.get(node_name) {
            let duration_ms = recovery_ended_at.saturating_sub(*started_at);
            if duration_ms > 0 {
                lib_stall_intervals.push(json!({
                    "node": node_name,
                    "started_at_unix_ms": started_at,
                    "ended_at_unix_ms": recovery_ended_at,
                    "duration_ms": duration_ms,
                    "right_censored_at_recovery_end": true,
                }));
            }
        }
    }
    let stable_completion_verified = stable_completion.is_some();
    let final_states = snapshots.last().map_or_else(Vec::new, |snapshot| {
        snapshot["nodes"]
            .as_array()
            .into_iter()
            .flatten()
            .filter(|node| node["responsive"].as_bool() == Some(true))
            .filter_map(|node| {
                Some((
                    node["node"].as_str()?.to_owned(),
                    ConsensusState {
                        tip_id: parse_header_id(node["tip_id"].as_str()?).ok()?,
                        tip_id_text: node["tip_id"].as_str()?.to_owned(),
                        tip_height: node["tip_height"].as_u64()?,
                        tip_slot: node["tip_slot"].as_u64()?,
                        lib_id: parse_header_id(node["lib_id"].as_str()?).ok()?,
                        lib_id_text: node["lib_id"].as_str()?.to_owned(),
                        lib_slot: node["lib_slot"].as_u64()?,
                        tip_descends_from_lib: node["tip_descends_from_lib"].as_bool(),
                    },
                ))
            })
            .collect::<Vec<_>>()
    });
    let final_common = common_finalized_lib(&final_states, honest_nodes.len());
    let final_common_beyond_boundary = final_common.is_some_and(|(_, slot)| slot > boundary_slot);
    let exposure_verified = exposure_is_verified(&active, attack_enabled, attack.is_some());
    let required_complete = active["attack_exposure"]["required_complete_opportunities"]
        .as_u64()
        .filter(|value| *value > 0)
        .unwrap_or(u64::MAX);
    let control_sibling_event_count = producer_log
        .lines()
        .filter(|line| {
            parse_log_fields(line).get("event").map(String::as_str)
                == Some("security_audit_sibling_submitted")
        })
        .count();
    let no_control_sibling_events =
        attack_enabled || (producer_log_read_error.is_none() && control_sibling_event_count == 0);
    let continuity_verified = continuity_regressions.is_empty()
        && continuity_unverified.is_empty()
        && active["safety_metrics"]["lib_continuity_regressions"]
            .as_array()
            .is_some_and(Vec::is_empty)
        && active["safety_metrics"]["lib_continuity_unverified_observations"]
            .as_array()
            .is_some_and(Vec::is_empty);
    let ancestry_verified = !any_unverified_ancestry
        && active["safety_metrics"]["ancestry_unverified"].as_bool() == Some(false)
        && active["safety_metrics"]["ancestry_gaps"]
            .as_array()
            .is_some_and(Vec::is_empty)
        && active["reorganization_metrics"]["tip_reorganizations_unverified"]
            .as_array()
            .is_some_and(Vec::is_empty)
        && ancestry_gaps.is_empty()
        && tip_reorganizations_unverified.is_empty();
    let liveness_recovered = honest_nodes
        .iter()
        .all(|node| progress_by_node.get(*node).copied().unwrap_or(false));
    let cooldown_verified = !attack_enabled
        || active
            .pointer("/active_window/cooldown/verified")
            .and_then(Value::as_bool)
            == Some(true);
    let control_blocks_verified = attack_enabled
        || (active
            .pointer("/active_window/canonical_blocks/status")
            .and_then(Value::as_str)
            == Some("verified")
            && active
                .pointer("/active_window/canonical_blocks/verified_blocks")
                .and_then(Value::as_u64)
                .zip(
                    active
                        .pointer("/active_window/canonical_blocks/matched_target")
                        .and_then(Value::as_u64),
                )
                .is_some_and(|(blocks, target)| target > 0 && blocks >= target));
    let recovery_status = producer_confirmed_stopped
        && post_stop_logs_quiet
        && producer_opportunity_log_verified
        && exposure_verified
        && cooldown_verified
        && control_blocks_verified
        && no_control_sibling_events
        && liveness_recovered
        && final_alignment["aligned"].as_bool() == Some(true)
        && final_common_beyond_boundary
        && stable_completion_verified
        && continuity_verified
        && ancestry_verified
        && !safety_conflict_observed;
    let mut failure_classifications = Vec::new();
    if attack_enabled && !exposure_verified {
        failure_classifications.push("insufficient_exposure");
    }
    if !cooldown_verified {
        failure_classifications.push("canonical_block_cooldown_not_verified");
    }
    if !control_blocks_verified {
        failure_classifications.push("control_block_target_not_verified");
    }
    if active["workload_setup_failure"].as_bool() == Some(true) {
        failure_classifications.push("workload_or_leadership_setup_failure");
    }
    if !producer_confirmed_stopped || !post_stop_logs_quiet {
        failure_classifications.push("producer_stop_or_post_stop_opportunity_unverified");
    }
    if !producer_opportunity_log_verified {
        failure_classifications.push("producer_opportunity_log_incomplete_or_mismatched");
    }
    if !no_control_sibling_events {
        failure_classifications.push("control_observed_sibling_production");
    }
    if !liveness_recovered {
        failure_classifications.push("honest_node_recovery_progress_incomplete");
    }
    if final_alignment["aligned"].as_bool() != Some(true) {
        failure_classifications.push("honest_tip_alignment_not_reached");
    }
    if !final_common_beyond_boundary {
        failure_classifications.push("common_lib_did_not_advance_beyond_recovery_boundary");
    }
    if !stable_completion_verified {
        failure_classifications.push("stable_common_finality_not_verified_before_timeout");
    }
    if safety_conflict_observed {
        failure_classifications.push("conflicting_finalized_histories");
    }
    if !continuity_verified {
        failure_classifications.push("lib_continuity_regressed_or_unverified");
    }
    if !ancestry_verified {
        failure_classifications.push("missing_ancestry_or_tip_reorganization_lca");
    }
    if attack_enabled
        && u64::try_from(final_complete_opportunities.len()).unwrap_or(u64::MAX) < required_complete
    {
        failure_classifications
            .push("producer_log_exposure_did_not_confirm_required_opportunities");
    }
    let recovery_duration_ms = recovery_started.elapsed().as_millis();
    let finality_report_path = artifact_dir.join("recovery-observations.json");
    let mut report = json!({
        "status": if recovery_status { "passed" } else if attack_enabled && !exposure_verified { "insufficient_exposure" } else { "failed" },
        "failure_classifications": failure_classifications,
        "scenario": if attack_enabled { "Honest peers recover after sustained nonempty sibling equivocation" } else { "Honest peers under equivalent workload without sibling equivocation" },
        "experiment_mode": if attack_enabled { "sustained_equivocation" } else { "matched_control" },
        "source_commit": active["source_commit"],
        "provenance": active["provenance"],
        "consensus_configuration": consensus_configuration(world),
        "active_observations_path": active_path,
        "active_window": active["active_window"],
        "attack_exposure": active["attack_exposure"],
        "transaction_workload": active["transaction_workload"],
        "opportunities": active["opportunities"],
        "control_leadership_timing_events": active["control_leadership_timing_events"],
        "producer_shutdown": {
            "stop_step_was_invoked": stop_step_seen,
            "process_confirmed_stopped_by_consensus_api": producer_confirmed_stopped,
            "estimated_stopped_at_unix_ms": recovery_started_at,
            "active_window_end_to_stop_delay_ms": active["active_window"]["ended_at_unix_ms"]
                .as_u64()
                .map(|ended| recovery_started_at.saturating_sub(ended)),
            "consensus_probe": producer_probe,
            "checked_quiet_after_stop_for_ms": 2000,
            "post_stop_equivocation_events": post_stop_equivocation_events,
            "post_stop_logs_quiet": post_stop_logs_quiet,
            "post_stop_log_read_error": post_stop_log_read_error,
            "log_tail_setup_error": stopped_log_tail_setup_error,
            "full_producer_log_read_error": producer_log_read_error,
            "complete_producer_opportunity_log_verified": producer_opportunity_log_verified,
            "complete_logged_opportunity_count_after_stop": final_complete_opportunities.len(),
            "raw_ordinary_submission_event_count_after_stop": final_producer_opportunities.len(),
            "opportunities_logged_after_active_cutoff_before_shutdown": opportunities_logged_after_active_cutoff,
            "active_opportunities_missing_from_final_producer_log": active_opportunities_missing_from_final_log,
        },
        "recovery_window": {
            "started_at_unix_ms": recovery_started_at,
            "ended_at_unix_ms": recovery_ended_at,
            "duration_ms": recovery_duration_ms,
            "timeout_ms": timeout.as_millis(),
            "sample_interval_ms": SAMPLE_INTERVAL.as_millis(),
            "sample_count": snapshots.len(),
            "recovery_boundary_kind": if attack_enabled { "last_attacked_slot" } else { "highest_honest_lib_slot_at_active_window_end" },
            "recovery_boundary_slot": boundary_slot,
            "last_attacked_slot": last_attacked_slot,
            "earliest_common_lib_beyond_boundary": first_common_beyond(&snapshots, boundary_slot, &honest_nodes),
            "stable_completion_verified": stable_completion_verified,
            "stable_completion": stable_completion.map(|completion| json!({
                "started_elapsed_ms": completion.started_elapsed_ms,
                "completed_elapsed_ms": completion.completed_elapsed_ms,
                "started_at_unix_ms": recovery_started_at.saturating_add(u64::try_from(completion.started_elapsed_ms).unwrap_or(u64::MAX)),
                "completed_at_unix_ms": recovery_started_at.saturating_add(u64::try_from(completion.completed_elapsed_ms).unwrap_or(u64::MAX)),
                "observed_stability_duration_ms": completion.completed_elapsed_ms.saturating_sub(completion.started_elapsed_ms),
                "required_stability_duration_ms": stability.as_millis(),
            })),
            "final_common_lib": final_common.map(|(id, slot)| json!({
                "lib_id": id.encode_hex::<String>(),
                "lib_slot": slot,
                "beyond_boundary": slot > boundary_slot,
                "node_libs": final_states.iter().map(|(node, state)| json!({
                    "node": node,
                    "lib_id": state.lib_id_text,
                    "lib_slot": state.lib_slot,
                })).collect::<Vec<_>>(),
            })),
            "final_tip_alignment": final_alignment,
            "honest_nodes_progressed_after_shutdown": progress_by_node,
            "responsive_failure_observations": responsive_failure_observations,
            "liveness_metrics": {
                "per_node_tip_advance_counts": recovery_tip_advance_counts,
                "per_node_lib_advance_counts": recovery_lib_advance_counts,
                "tip_stall_intervals": tip_stall_intervals,
                "lib_stall_intervals": lib_stall_intervals,
                "tip_alignment_transitions": recovery_alignment_transitions,
            },
        },
        "snapshots": snapshots,
        "finalized_history_checks": finalized_history_checks,
        "safety_conflict_observed": safety_conflict_observed,
        "lib_continuity_verified": continuity_verified,
        "lib_continuity_regressions": continuity_regressions,
        "lib_continuity_unverified_observations": continuity_unverified,
        "ancestry_verified": ancestry_verified,
        "ancestry_gaps": ancestry_gaps,
        "tip_transition_observations": tip_transitions,
        "tip_reorganizations": tip_reorganizations,
        "tip_reorganizations_unverified": tip_reorganizations_unverified,
        "tip_reorganization_observation_is_lower_bound": true,
        "finalized_history_conflict_is_sticky": safety_conflict_observed,
        "final_producer_equivocation_opportunities": final_producer_opportunities,
        "incomplete_producer_opportunity_ids": incomplete_producer_opportunity_ids,
        "final_complete_producer_opportunity_count": final_complete_opportunities.len(),
        "final_complete_producer_opportunities": final_complete_opportunities
            .iter()
            .map(|opportunity| json!({
                "ordinary_submission_line": opportunity.ordinary_line,
                "ordinary_submission": opportunity.ordinary,
                "sibling_submission_events": opportunity.siblings.iter().map(|(line, event)| json!({
                    "line_index": line,
                    "event": event,
                })).collect::<Vec<_>>(),
                "leadership_timing_line": opportunity.timing_line,
                "leadership_timing_event": opportunity.timing,
            }))
            .collect::<Vec<_>>(),
        "control_sibling_event_count": control_sibling_event_count,
        "active_source_sha256": sha256_file(&active_path).ok(),
        "artifact_path": finality_report_path,
    });
    let node_logs = copy_node_logs(world, &artifact_dir);
    match node_logs {
        Ok(paths) => {
            report["node_logs"] = json!(
                paths
                    .iter()
                    .map(|path| json!({
                        "path": path,
                        "sha256": sha256_file(Path::new(path)).ok(),
                    }))
                    .collect::<Vec<_>>()
            );
        }
        Err(error) => report["node_log_capture_error"] = json!(error),
    }
    write_json(&finality_report_path, &report)?;
    let summary_path = artifact_dir.join("experiment-summary.json");
    write_json(
        &summary_path,
        &json!({
            "mode": report["experiment_mode"],
            "active": report["active_window"],
            "transaction_workload": report["transaction_workload"],
            "attack_exposure": report["attack_exposure"],
            "recovery": report["recovery_window"],
            "reorganizations": {
                "active": active["reorganization_metrics"]["tip_reorganizations"],
                "recovery": report["tip_reorganizations"],
                "active_observation_is_lower_bound": true,
                "recovery_observation_is_lower_bound": true,
            },
            "safety_conflict_observed": report["safety_conflict_observed"],
            "status": report["status"],
        }),
    )?;
    tracing::info!(
        target: TARGET,
        event = "security_audit_sustained_experiment_evidence",
        evidence_path = %finality_report_path.display(),
        mode = if attack_enabled { "sustained_equivocation" } else { "matched_control" },
        status = report["status"].as_str().unwrap_or("failed"),
        "Sustained security audit experiment observations were preserved"
    );
    if report["status"].as_str() == Some("passed") {
        Ok(())
    } else {
        Err(StepError::StepFail {
            message: format!(
                "sustained security audit did not satisfy its criteria: {}; evidence: {}",
                report["failure_classifications"],
                finality_report_path.display()
            ),
        })
    }
}

fn parse_last_verified_libs(active: &Value) -> BTreeMap<String, (HeaderId, u64)> {
    active["baseline_and_final"]["last_verified_libs"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|item| {
            Some((
                item["node"].as_str()?.to_owned(),
                (
                    parse_header_id(item["lib_id"].as_str()?).ok()?,
                    item["lib_slot"].as_u64()?,
                ),
            ))
        })
        .collect()
}

fn parse_last_active_tips(active: &Value) -> BTreeMap<String, NodeTip> {
    active["baseline_and_final"]["last_active_tips"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|item| {
            let id_text = item["tip_id"].as_str()?.to_owned();
            Some((
                item["node"].as_str()?.to_owned(),
                NodeTip {
                    id: parse_header_id(&id_text).ok()?,
                    id_text,
                    height: item["tip_height"].as_u64()?,
                    slot: item["tip_slot"].as_u64()?,
                    observed_at_unix_ms: item["observed_at_unix_ms"].as_u64()?,
                },
            ))
        })
        .collect()
}

fn active_control_boundary_slot(active: &Value, honest_nodes: &[&str]) -> Option<u64> {
    let nodes = active["snapshots"].as_array()?.last()?["nodes"].as_array()?;
    honest_nodes
        .iter()
        .map(|name| {
            let node = nodes
                .iter()
                .find(|node| node["node"].as_str() == Some(*name))?;
            (node["responsive"].as_bool() == Some(true))
                .then(|| node["lib_slot"].as_u64())
                .flatten()
        })
        .collect::<Option<Vec<_>>>()?
        .into_iter()
        .max()
}

fn attack_evidence_from_record(record: &Value) -> Option<AttackEvidence> {
    let raw = &record["attack_evidence"];
    Some(AttackEvidence {
        ordinary_id: parse_header_id(raw["ordinary_block_id"].as_str()?).ok()?,
        ordinary_id_text: raw["ordinary_block_id"].as_str()?.to_owned(),
        parent_id: parse_header_id(raw["parent_block_id"].as_str()?).ok()?,
        parent_id_text: raw["parent_block_id"].as_str()?.to_owned(),
        slot: raw["attack_slot"].as_u64()?,
        body_root: raw["body_root"].as_str().unwrap_or_default().to_owned(),
        transaction_hashes: raw["ordered_transaction_hashes"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(Value::as_str)
            .map(str::to_owned)
            .collect(),
        siblings: raw["siblings"]
            .as_array()?
            .iter()
            .map(|sibling| {
                Some((
                    usize::try_from(sibling["sibling_index"].as_u64()?).ok()?,
                    AttackCandidate {
                        id: parse_header_id(sibling["sibling_block_id"].as_str()?).ok()?,
                    },
                ))
            })
            .collect::<Option<Vec<_>>>()?,
    })
}

fn attack_candidate_ids_from_record(record: &Value) -> Vec<HeaderId> {
    let mut ids = Vec::new();
    if let Some(id) = record["attack_evidence"]["ordinary_block_id"].as_str()
        && let Ok(id) = parse_header_id(id)
    {
        ids.push(id);
    }
    for sibling in record["attack_evidence"]["siblings"]
        .as_array()
        .into_iter()
        .flatten()
    {
        if let Some(id) = sibling["sibling_block_id"].as_str()
            && let Ok(id) = parse_header_id(id)
        {
            ids.push(id);
        }
    }
    ids
}

fn attack_candidate_ids_from_opportunities(
    opportunities: &[ProducerOpportunity],
) -> BTreeSet<HeaderId> {
    let mut ids = BTreeSet::new();
    for opportunity in opportunities {
        for id in std::iter::once(opportunity.ordinary.get("ordinary_block_id"))
            .chain(
                opportunity
                    .siblings
                    .iter()
                    .map(|(_, sibling)| sibling.get("sibling_block_id")),
            )
            .flatten()
        {
            if let Ok(id) = parse_header_id(id) {
                ids.insert(id);
            }
        }
    }
    ids
}

fn first_common_beyond(
    snapshots: &[Value],
    boundary_slot: u64,
    honest_nodes: &[&str],
) -> Option<Value> {
    snapshots.iter().find_map(|snapshot| {
        let nodes = snapshot["nodes"].as_array()?;
        let states = honest_nodes
            .iter()
            .map(|name| {
                let node = nodes
                    .iter()
                    .find(|node| node["node"].as_str() == Some(*name))?;
                Some((
                    node["node"].as_str()?.to_owned(),
                    ConsensusState {
                        tip_id: parse_header_id(node["tip_id"].as_str()?).ok()?,
                        tip_id_text: node["tip_id"].as_str()?.to_owned(),
                        tip_height: node["tip_height"].as_u64()?,
                        tip_slot: node["tip_slot"].as_u64()?,
                        lib_id: parse_header_id(node["lib_id"].as_str()?).ok()?,
                        lib_id_text: node["lib_id"].as_str()?.to_owned(),
                        lib_slot: node["lib_slot"].as_u64()?,
                        tip_descends_from_lib: node["tip_descends_from_lib"].as_bool(),
                    },
                ))
            })
            .collect::<Option<Vec<_>>>()?;
        let (id, slot) = common_finalized_lib(&states, honest_nodes.len())?;
        (slot > boundary_slot).then(|| {
            json!({
                "observed_at_unix_ms": snapshot["observed_at_unix_ms"],
                "lib_id": id.encode_hex::<String>(),
                "lib_slot": slot,
            })
        })
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_active_log_tracker_does_not_invent_opportunities() {
        let mut tracker = ActiveLogObservations::default();
        let (latest, errors) = tracker.update_opportunities("producer", true);
        assert!(latest.is_none());
        assert_eq!(errors, 0);
        assert_eq!(tracker.opportunities.len(), 0);
        assert_eq!(tracker.attack_candidates.len(), 0);
        let (latest, errors) = tracker.update_opportunities("producer", false);
        assert!(latest.is_none());
        assert_eq!(errors, 0);
        assert_eq!(tracker.control_timing.len(), 0);
    }

    #[test]
    fn unavailable_post_stop_log_is_not_quiet() {
        let observation = PostStopLogObservation {
            tail: None,
            setup_error: Some("missing".to_owned()),
            read_error: Some("missing".to_owned()),
            equivocation_events: Vec::new(),
        };
        assert!(!observation.quiet());
    }

    #[test]
    fn zero_workload_and_skew_are_valid_but_positive_fields_remain_required() {
        assert!(tip_height_skew_allowed(0, 0));
        assert!(!tip_height_skew_allowed(1, 0));
        let mut values = BTreeMap::from([
            ("transaction_budget".to_owned(), 4),
            ("initial_pending_transactions".to_owned(), 0),
            ("submission_interval_seconds".to_owned(), 1),
            ("readiness_seconds".to_owned(), 1),
            ("maximum_tip_height_skew".to_owned(), 0),
        ]);
        let result = workload_criteria(&values).unwrap();
        assert_eq!((result.initial_pending, result.max_tip_height_skew), (0, 0));
        values.insert("readiness_seconds".to_owned(), 0);
        assert!(workload_criteria(&values).is_err());
        values.insert("readiness_seconds".to_owned(), 1);
        values.insert("initial_pending_transactions".to_owned(), 5);
        assert!(workload_criteria(&values).is_err());
        values.remove("transaction_budget");
        assert!(workload_criteria(&values).is_err());
    }

    #[test]
    fn criteria_rows_reject_invalid_duplicate_and_out_of_range_values() {
        let rows = |entries: &[(&str, &str)]| {
            std::iter::once(("criterion", "value"))
                .chain(entries.iter().copied())
                .map(|(key, value)| vec![key.to_owned(), value.to_owned()])
                .collect::<Vec<_>>()
        };
        assert_eq!(
            parse_criteria_rows(&rows(&[("skew", "0"), ("budget", "2")])).unwrap()["skew"],
            0
        );
        assert!(parse_criteria_rows(&rows(&[("skew", "-1")])).is_err());
        assert!(parse_criteria_rows(&rows(&[("skew", "18446744073709551616")])).is_err());
        assert!(parse_criteria_rows(&rows(&[("skew", "1"), ("skew", "2")])).is_err());
        let values = parse_criteria_rows(&rows(&[("skew", "1")])).unwrap();
        assert!(require_criteria(&values, &["skew", "budget"]).is_err());
    }

    #[test]
    fn absent_exposure_evidence_cannot_be_verified() {
        assert!(!exposure_is_verified(&json!({}), true, true));
        assert!(!exposure_is_verified(
            &json!({"attack_exposure": {"complete_genuine_opportunities": 0, "required_complete_opportunities": 0}}),
            true,
            true
        ));
        assert!(exposure_is_verified(&json!({}), false, false));
    }

    #[test]
    fn topology_matching_preserves_roles_reference_and_edges_across_renaming() {
        let node = |role: &str, reference, peers| ExperimentalNode {
            role: role.to_owned(),
            canonical_reference: reference,
            blend_core_provider: true,
            initial_peers: peers,
        };
        let attack = vec![
            node("producer", false, vec![1]),
            node("honest", true, vec![]),
            node("honest", false, vec![1]),
        ];
        let mut control = vec![
            node("honest", false, vec![2]),
            node("producer", false, vec![2]),
            node("honest", true, vec![]),
        ];
        assert!(equivalent_topology(&attack, &control));
        control[0].initial_peers = vec![1];
        assert!(!equivalent_topology(&attack, &control));
        control[0].initial_peers = vec![2];
        control[2].canonical_reference = false;
        control[0].canonical_reference = true;
        assert!(!equivalent_topology(&attack, &control));
        control.pop();
        assert!(!equivalent_topology(&attack, &control));
    }

    #[test]
    fn matching_reports_specific_skew_readiness_and_missing_evidence() {
        let config = json!({"status":"read", "security_parameter_k":5, "slot_duration_secs":1, "uncle_reference_window_in_block":4, "slot_activation_coeff_numerator":1, "slot_activation_coeff_denominator":30, "epoch_stake_distribution_stabilization":3, "epoch_period_nonce_buffer":3, "epoch_period_nonce_stabilization":4});
        let provenance = json!({"binary_sha256":"hash"});
        let workload = WorkloadCriteria {
            budget: 4,
            initial_pending: 0,
            interval: Duration::from_secs(2),
            readiness_timeout: Duration::from_secs(60),
            max_tip_height_skew: 0,
        };
        let declared = SecurityAuditWorkload {
            sender: "sender".to_owned(),
            receiver: "receiver".to_owned(),
            amount: 10,
        };
        let experiment = ExperimentalConfiguration {
            nodes: vec![
                ExperimentalNode {
                    role: "producer".to_owned(),
                    canonical_reference: false,
                    blend_core_provider: true,
                    initial_peers: vec![],
                },
                ExperimentalNode {
                    role: "honest".to_owned(),
                    canonical_reference: true,
                    blend_core_provider: true,
                    initial_peers: vec![0],
                },
            ],
            readiness_timeout_ms: 60000,
            active_timeout_ms: 1_800_000,
            sampling_interval_ms: 2000,
            maximum_tip_height_skew: 0,
        };
        let mut attack = json!({"consensus_configuration":config, "provenance":provenance, "experimental_configuration":experiment, "transaction_workload":{"budget":4,"initial_pending_transactions":0,"submission_interval_ms":2000,"transfer_amount":10,"sender_wallet":"sender","receiver_wallet":"receiver"}});
        assert_eq!(
            matched_attack_compatibility(
                &attack,
                &config,
                &provenance,
                workload,
                &declared,
                &experiment
            ),
            Vec::<String>::new()
        );
        attack["experimental_configuration"]["maximum_tip_height_skew"] = json!(1);
        attack["experimental_configuration"]["readiness_timeout_ms"] = json!(30000);
        attack["experimental_configuration"]["active_timeout_ms"] = json!(900_000);
        attack["experimental_configuration"]["sampling_interval_ms"] = json!(5000);
        let mismatches = matched_attack_compatibility(
            &attack,
            &config,
            &provenance,
            workload,
            &declared,
            &experiment,
        );
        assert!(mismatches.contains(&"maximum_tip_height_skew_mismatch".to_owned()));
        assert!(mismatches.contains(&"readiness_timeout_mismatch".to_owned()));
        assert!(mismatches.contains(&"active_timeout_mismatch".to_owned()));
        assert!(mismatches.contains(&"sampling_interval_mismatch".to_owned()));
        attack["experimental_configuration"] = Value::Null;
        assert!(
            matched_attack_compatibility(
                &attack,
                &config,
                &provenance,
                workload,
                &declared,
                &experiment
            )
            .contains(&"experimental_configuration_unverified".to_owned())
        );
    }

    #[test]
    fn control_boundary_requires_each_declared_honest_lib() {
        let report = json!({"snapshots": [{"nodes": [
            {"node": "alice", "responsive": true, "lib_slot": 7},
            {"node": "bob", "responsive": true, "lib_slot": 12}
        ]}]});
        assert_eq!(
            active_control_boundary_slot(&report, &["alice", "bob"]),
            Some(12)
        );
        assert_eq!(
            active_control_boundary_slot(&report, &["alice", "missing"]),
            None
        );
        assert_eq!(active_control_boundary_slot(&report, &[]), None);
    }

    fn recovery_rows(skew: &str) -> Vec<Vec<String>> {
        [
            ("criterion", "requirement"),
            ("honest_nodes_responsive", "all"),
            ("honest_nodes_make_progress", "all"),
            ("maximum_tip_height_skew", skew),
            ("common_lib", "same ID and slot"),
            ("tip_and_finalized_ancestry", "verified"),
            ("lib_continuity", "verified"),
            ("finalized_history_conflicts", "none"),
            ("producer_stopped_and_quiet", "yes"),
        ]
        .into_iter()
        .map(|(key, value)| vec![key.to_owned(), value.to_owned()])
        .collect()
    }

    #[test]
    fn recovery_boundary_is_explicit_and_rejects_unknown_values() {
        assert_eq!(
            RecoveryBoundary::parse("last attack slot").unwrap(),
            RecoveryBoundary::LastAttackSlot
        );
        assert_eq!(
            RecoveryBoundary::parse("control baseline").unwrap(),
            RecoveryBoundary::ControlBaseline
        );
        assert!(RecoveryBoundary::parse("other").is_err());
    }

    #[test]
    fn recovery_criteria_use_the_scenario_skew_and_require_safety_checks() {
        assert_eq!(parse_recovery_criteria(&recovery_rows("0")).unwrap(), 0);
        assert_eq!(parse_recovery_criteria(&recovery_rows("2")).unwrap(), 2);
        assert!(parse_recovery_criteria(&recovery_rows("invalid")).is_err());
        let mut missing = recovery_rows("1");
        missing.pop();
        assert!(parse_recovery_criteria(&missing).is_err());
        let mut duplicate = recovery_rows("1");
        duplicate.push(duplicate[1].clone());
        assert!(parse_recovery_criteria(&duplicate).is_err());
        let mut weakened = recovery_rows("1");
        weakened[6][1] = "unverified".to_owned();
        assert!(parse_recovery_criteria(&weakened).is_err());
    }

    fn sibling_body(root: &str, hashes: &[&str]) -> Value {
        json!({"producer_block_api": {
            "available": true, "body_root": root, "transaction_hashes": hashes,
        }})
    }

    #[test]
    fn body_comparison_without_retrieved_siblings_is_unverified() {
        let ordinary = json!({"available": true, "body_root": "root", "transaction_hashes": ["a"]});
        let siblings = vec![json!({"producer_block_api": {"available": false}}); 4];
        let result = compare_sibling_bodies(&ordinary, &siblings);
        assert_eq!(result["retrieved_sibling_bodies"], 0);
        assert_eq!(result["body_root"]["status"], "unverified");
        assert_eq!(result["ordered_transaction_hashes"]["status"], "unverified");
        assert_eq!(result["body_root"]["successful_comparisons"], 0);
    }

    #[test]
    fn body_comparison_reports_matches_and_partial_coverage() {
        let ordinary =
            json!({"available": true, "body_root": "root", "transaction_hashes": ["a", "b"]});
        let siblings = vec![
            sibling_body("root", &["a", "b"]),
            json!({"producer_block_api": {"available": false}}),
        ];
        let result = compare_sibling_bodies(&ordinary, &siblings);
        assert_eq!(result["retrieved_sibling_bodies"], 1);
        assert_eq!(result["body_root"]["status"], "matched");
        assert_eq!(
            result["ordered_transaction_hashes"]["successful_comparisons"],
            1
        );
        assert_eq!(
            result["body_root"]["all_siblings_compared_and_matched"],
            false
        );
        let all = compare_sibling_bodies(&ordinary, &[sibling_body("root", &["a", "b"])]);
        assert_eq!(all["body_root"]["all_siblings_compared_and_matched"], true);
        assert_eq!(
            all["ordered_transaction_hashes"]["all_siblings_compared_and_matched"],
            true
        );
    }

    #[test]
    fn body_comparison_reports_mismatches_and_missing_ordinary() {
        let ordinary =
            json!({"available": true, "body_root": "root", "transaction_hashes": ["a", "b"]});
        let siblings = vec![
            sibling_body("root", &["a", "b"]),
            sibling_body("other", &["b", "a"]),
        ];
        let result = compare_sibling_bodies(&ordinary, &siblings);
        assert_eq!(result["body_root"]["status"], "mismatched");
        assert_eq!(result["ordered_transaction_hashes"]["status"], "mismatched");
        assert_eq!(result["body_root"]["comparisons"], 2);
        assert_eq!(result["body_root"]["successful_comparisons"], 1);
        let missing_ordinary = compare_sibling_bodies(&json!({"available": false}), &siblings);
        assert_eq!(missing_ordinary["retrieved_sibling_bodies"], 2);
        assert_eq!(missing_ordinary["body_root"]["status"], "unverified");
        assert_eq!(
            missing_ordinary["ordered_transaction_hashes"]["status"],
            "unverified"
        );
    }

    fn header(byte: u8) -> HeaderId {
        HeaderId::try_from([byte; 32].as_slice()).expect("32-byte test header ID")
    }

    #[test]
    fn control_proposal_log_records_block_id_and_transaction_count() {
        let proposal = parse_ordinary_proposal_log(
            "2026-10-20T13:19:26.467542Z DEBUG ChainLeader:propose_block{parent=HeaderId(7b0f9446a860c046df75ab1892730a030b40651d5954c61a04c0b8003ad4ec32) slot=Slot(64)}: logos_blockchain::chain::leader: proposed block HeaderId(a954addc564624602d8cbca2fa2ab160a6ee847befc71755fadcf9c622c9c60a) with 6 transactions (0 removed)",
        )
        .expect("normal producer proposal log parses");

        assert_eq!(
            proposal,
            OrdinaryProposalLog {
                parent_block_id:
                    "0x7b0f9446a860c046df75ab1892730a030b40651d5954c61a04c0b8003ad4ec32".to_owned(),
                slot: "64".to_owned(),
                block_id: "0xa954addc564624602d8cbca2fa2ab160a6ee847befc71755fadcf9c622c9c60a"
                    .to_owned(),
                transaction_count: 6,
            }
        );
    }

    #[test]
    fn reorganization_depths_measure_both_branches_from_the_lca() {
        assert_eq!(reorganization_depths(12, 14, 9), Some((3, 5)));
    }

    #[test]
    fn impossible_lca_height_remains_unverified() {
        assert_eq!(reorganization_depths(12, 14, 15), None);
        assert_eq!(reorganization_depths(12, 14, 13), None);
    }

    #[test]
    fn reorganization_is_visible_above_an_unchanged_attack_sibling() {
        let selected_before = AttackBranch::Sibling(2);
        let selected_after = AttackBranch::Sibling(2);
        assert_eq!(selected_before, selected_after);
        assert_eq!(
            classify_tip_transition(header(1), header(2), Some(false)),
            TipTransition::Reorganization
        );
        assert_eq!(reorganization_depths(10, 11, 8), Some((2, 3)));
    }

    #[test]
    fn lca_branch_membership_keeps_attack_candidates_on_either_side() {
        let old_candidate = header(3);
        let new_candidate = header(4);
        let ancestor = header(5);
        let candidates = BTreeSet::from([old_candidate, new_candidate]);

        assert_eq!(
            attack_candidate_members(&[old_candidate], ancestor, &candidates),
            vec![old_candidate.encode_hex::<String>()]
        );
        assert_eq!(
            attack_candidate_members(&[new_candidate], ancestor, &candidates),
            vec![new_candidate.encode_hex::<String>()]
        );
    }
}
