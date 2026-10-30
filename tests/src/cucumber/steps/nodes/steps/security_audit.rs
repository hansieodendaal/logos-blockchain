use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    fs::{self, File},
    io::{BufRead as _, BufReader, Seek as _, SeekFrom},
    path::{Path, PathBuf},
    process::Command,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use cucumber::{gherkin::Step, given, then, when};
use futures_util::future::join_all;
use hex::ToHex as _;
use lb_common_http_client::ApiBlock;
use lb_core::{
    header::HeaderId, mantle::traits::Hashable as _, proofs::leader_proof::LeaderProof as _,
};
use lb_node::config::DeploymentSettings;
use lb_testing_framework::{
    NodeHttpClient, USER_CONFIG_FILE, configs::deployment::NodeBinaryProfile,
    ensure_node_binary_built,
};
use serde_json::{Value, json};
use tokio::time::sleep;

use crate::{
    common::manual_cluster::read_manual_node_logs,
    cucumber::{
        error::{StepError, StepResult},
        steps::{
            TARGET,
            mempool::actions::{
                prepare_transfer_transaction, submit_prepared_transaction_to_nodes,
            },
        },
        utils::tx_hash_to_hex,
        world::{
            CucumberWorld, SecurityAuditNonemptyWorkload, SecurityAuditParticipants,
            SecurityAuditWorkload,
        },
    },
};

#[cfg(test)]
const SIBLING_COUNT: usize = 4;
#[cfg(test)]
const FINALITY_OBSERVATION_TIMEOUT: Duration = Duration::from_secs(600);
const FINALITY_OBSERVATION_INTERVAL: Duration = Duration::from_secs(2);
const FINALITY_MAX_ANCESTRY_DEPTH: usize = 100_000;

#[given("the security audit participants are:")]
fn step_declare_audit_participants(world: &mut CucumberWorld, step: &Step) -> StepResult {
    if world.security_audit_participants.is_some() {
        return Err(StepError::InvalidArgument {
            message: "security audit participants are already declared".to_owned(),
        });
    }
    let table = step.table.as_ref().ok_or(StepError::MissingTable)?;
    let nodes = world.nodes_info.keys().cloned().collect::<HashSet<_>>();
    let participants = parse_audit_participants(&table.rows, &nodes)
        .map_err(|message| StepError::InvalidArgument { message })?;
    world.security_audit_participants = Some(participants);
    Ok(())
}

fn parse_audit_participants(
    rows: &[Vec<String>],
    available_nodes: &HashSet<String>,
) -> Result<SecurityAuditParticipants, String> {
    if rows.first().map(Vec::as_slice)
        != Some(&[
            "node".to_owned(),
            "role".to_owned(),
            "canonical_reference".to_owned(),
        ])
    {
        return Err(
            "participant table requires node, role and canonical_reference columns".to_owned(),
        );
    }
    let mut seen = HashSet::new();
    let mut producer = None;
    let mut honest_nodes = Vec::new();
    let mut canonical_reference = None;
    for row in &rows[1..] {
        let [name, role, reference] = row.as_slice() else {
            return Err(
                "each participant requires a node, role and canonical reference".to_owned(),
            );
        };
        if !available_nodes.contains(name) || !seen.insert(name.clone()) {
            return Err(format!(
                "participant {name} is missing from the cluster or duplicated"
            ));
        }
        match (role.as_str(), reference.as_str()) {
            ("producer", "no") if producer.is_none() => producer = Some(name.clone()),
            ("honest", "no") => honest_nodes.push(name.clone()),
            ("honest", "yes") if canonical_reference.is_none() => {
                honest_nodes.push(name.clone());
                canonical_reference = Some(name.clone());
            }
            _ => {
                return Err(format!(
                    "invalid or conflicting participant role for {name}"
                ));
            }
        }
    }
    if seen.len() != available_nodes.len() {
        return Err("every started cluster node must have a declared audit role".to_owned());
    }
    if honest_nodes.is_empty() {
        return Err("at least one honest node is required".to_owned());
    }
    Ok(SecurityAuditParticipants {
        producer: producer.ok_or_else(|| "exactly one producer is required".to_owned())?,
        honest_nodes,
        canonical_reference: canonical_reference
            .ok_or_else(|| "one honest canonical reference is required".to_owned())?,
    })
}

fn audit_participants(world: &CucumberWorld) -> Result<&SecurityAuditParticipants, String> {
    world
        .security_audit_participants
        .as_ref()
        .ok_or_else(|| "security audit participants were not declared".to_owned())
}

#[given(expr = "the audit producer is configured to produce {int} siblings")]
fn step_audit_sibling_configuration(world: &mut CucumberWorld, count: usize) -> StepResult {
    if world.security_audit_siblings.is_some() {
        return Err(StepError::InvalidArgument {
            message: "audit sibling configuration was already declared".to_owned(),
        });
    }
    world.security_audit_siblings = Some(count);
    Ok(())
}

#[given(expr = "the audit workload sends {int} from {string} to {string}")]
fn step_audit_workload_configuration(
    world: &mut CucumberWorld,
    amount: u64,
    sender: String,
    receiver: String,
) -> StepResult {
    if amount == 0 || sender == receiver {
        return Err(StepError::InvalidArgument {
            message: "audit workload requires distinct wallets and a positive amount".to_owned(),
        });
    }
    if world.security_audit_workload.is_some() {
        return Err(StepError::InvalidArgument {
            message: "audit workload was already declared".to_owned(),
        });
    }
    world.security_audit_workload = Some(SecurityAuditWorkload {
        sender,
        receiver,
        amount,
    });
    Ok(())
}

#[given(
    expr = "the nonempty selection submits at most {int} transactions within {int} seconds and samples each pending transaction for {int} seconds"
)]
fn step_nonempty_workload_configuration(
    world: &mut CucumberWorld,
    budget: usize,
    window_seconds: u64,
    pending_seconds: u64,
) -> StepResult {
    if budget == 0
        || window_seconds == 0
        || pending_seconds == 0
        || world.security_audit_nonempty_workload.is_some()
    {
        return Err(StepError::InvalidArgument {
            message: "nonempty selection requires positive settings and a single declaration"
                .to_owned(),
        });
    }
    world.security_audit_nonempty_workload = Some(SecurityAuditNonemptyWorkload {
        budget,
        window_seconds,
        pending_seconds,
    });
    Ok(())
}

fn audit_sibling_count(world: &CucumberWorld) -> Result<usize, String> {
    world
        .security_audit_siblings
        .ok_or_else(|| "audit sibling configuration was not declared".to_owned())
}

const fn valid_sibling_minimum(configured: usize, minimum: usize) -> bool {
    minimum > 0 && minimum <= configured
}

fn audit_workload(world: &CucumberWorld) -> Result<&SecurityAuditWorkload, String> {
    world
        .security_audit_workload
        .as_ref()
        .ok_or_else(|| "audit workload was not declared".to_owned())
}

#[derive(Clone)]
struct ProducerOpportunity {
    ordinary: HashMap<String, String>,
    ordinary_line: usize,
    siblings: Vec<(usize, HashMap<String, String>)>,
    timing: HashMap<String, String>,
    timing_line: usize,
}

#[derive(Clone, Debug, serde::Serialize)]
struct BlockBodyEvidence {
    block_id: String,
    parent_block_id: String,
    slot: u64,
    body_root: String,
    voucher_commitment: String,
    transaction_hashes: Vec<String>,
}

struct AuditFailure {
    message: String,
    details: Value,
}

struct AuditLogTail {
    root: PathBuf,
    participants: SecurityAuditParticipants,
    runtime_names: BTreeMap<String, String>,
    paths: BTreeMap<String, Vec<PathBuf>>,
    offsets: HashMap<PathBuf, u64>,
}

/// State belonging to one Cucumber scenario, including its selected
/// opportunity.
pub struct AuditScenarioState {
    provenance: Value,
    readiness: Value,
    tails: AuditLogTail,
    logs: BTreeMap<String, Vec<ObservedLogLine>>,
    opportunity: Option<ProducerOpportunity>,
    nonempty: Option<NonemptySelection>,
}

struct NonemptySelection {
    ordinary_body: BlockBodyEvidence,
    pre_attack_consensus: Value,
    submitted_hashes: BTreeSet<String>,
    submitted_transactions: Vec<Value>,
    transaction_observations: Vec<Value>,
    skipped_opportunities: Vec<Value>,
    examined_count: usize,
    workload: SecurityAuditNonemptyWorkload,
    selected_at_unix_ms: u64,
    selected_at: Instant,
}

/// Scheduling another transaction and observing leadership have separate
/// bounds: running out of transactions must not end the observation window.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum NonemptyWorkloadAction {
    SubmitNext,
    Observe,
    Finished,
}

#[derive(Default)]
struct NonemptyOpportunityState {
    logs: Vec<ObservedLogLine>,
    examined_ids: BTreeSet<String>,
    incomplete_body_ids: BTreeSet<String>,
    skipped: Vec<Value>,
    last_body_error: Option<String>,
}

impl NonemptyOpportunityState {
    async fn select(
        &mut self,
        world: &CucumberWorld,
        producer: &str,
        configured_siblings: usize,
        submitted_hashes: &BTreeSet<String>,
    ) -> Result<Option<(ProducerOpportunity, ApiBlock)>, AuditFailure> {
        let node = world.nodes_info.get(producer).ok_or_else(|| AuditFailure {
            message: format!("{producer} was not started"),
            details: json!({}),
        })?;
        for opportunity in find_producer_opportunities(&self.logs) {
            if parse_usize(&opportunity.ordinary, "configured_siblings")
                != Some(configured_siblings)
            {
                return Err(AuditFailure {
                    message: "producer opportunity contradicts declared sibling configuration"
                        .to_owned(),
                    details: json!({"ordinary_submission": opportunity.ordinary}),
                });
            }
            let Some(ordinary_id) = opportunity.ordinary.get("ordinary_block_id").cloned() else {
                continue;
            };
            if self.examined_ids.contains(&ordinary_id) {
                continue;
            }
            let header_id = match parse_header_id(&ordinary_id) {
                Ok(id) => id,
                Err(error) => {
                    self.incomplete_body_ids.insert(ordinary_id.clone());
                    self.skipped.push(json!({"ordinary_block_id": ordinary_id, "reason": "invalid_ordinary_block_id", "detail": error}));
                    self.examined_ids.insert(ordinary_id);
                    continue;
                }
            };
            self.incomplete_body_ids.insert(ordinary_id.clone());
            let block = match node.client.block(&header_id).await {
                Ok(Some(block)) => block,
                Ok(None) => {
                    self.incomplete_body_ids.insert(ordinary_id);
                    continue;
                }
                Err(error) => {
                    self.incomplete_body_ids.insert(ordinary_id);
                    self.last_body_error = Some(error.to_string());
                    continue;
                }
            };
            self.incomplete_body_ids.remove(&ordinary_id);
            let body = summarize_api_block(&block);
            self.examined_ids.insert(ordinary_id.clone());
            if body.block_id != ordinary_id
                || Some(&body.parent_block_id) != opportunity.ordinary.get("parent_block_id")
                || Some(&body.slot.to_string()) != opportunity.ordinary.get("slot")
            {
                return Err(AuditFailure {
                    message: format!(
                        "{producer} block API identity disagrees with the ordinary submission event"
                    ),
                    details: json!({"ordinary_submission": opportunity.ordinary, "ordinary_block_api": body}),
                });
            }
            if has_nonempty_submitted_body(&body.transaction_hashes, submitted_hashes) {
                return Ok(Some((opportunity, block)));
            }
            self.skipped.push(json!({
                "ordinary_block_id": body.block_id, "parent_block_id": body.parent_block_id,
                "slot": body.slot, "transaction_count": body.transaction_hashes.len(),
                "transaction_hashes": body.transaction_hashes, "body_root": body.body_root,
                "reason": if body.transaction_hashes.is_empty() { "empty_ordinary_body" } else { "no_submitted_transaction_in_ordinary_body" },
            }));
        }
        Ok(None)
    }
}

fn nonempty_workload_action(
    now: Instant,
    deadline: Instant,
    pending_deadline: Option<Instant>,
    pending_disappeared: bool,
    submitted: usize,
    budget: usize,
) -> NonemptyWorkloadAction {
    if now >= deadline {
        NonemptyWorkloadAction::Finished
    } else if submitted < budget
        && (pending_deadline.is_none_or(|end| now >= end) || pending_disappeared)
    {
        NonemptyWorkloadAction::SubmitNext
    } else {
        NonemptyWorkloadAction::Observe
    }
}

#[derive(Clone)]
struct ObservedLogLine {
    text: String,
    path: PathBuf,
    offset: u64,
}

#[derive(Clone)]
struct ReceiverEvent {
    stage: &'static str,
    parent_block_id: Option<String>,
    slot: Option<String>,
    timestamp: Option<[u32; 7]>,
    reason: Option<String>,
    path: PathBuf,
    offset: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct HeaderLink {
    id: HeaderId,
    parent: HeaderId,
    slot: u64,
}

#[derive(Default)]
struct HeaderCache {
    links: HashMap<HeaderId, HeaderLink>,
    failures: BTreeMap<String, String>,
}

struct AncestryPath {
    links: Vec<HeaderLink>,
    target_reached: bool,
    complete_below_slot: bool,
    error: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum AttackBranch {
    Ordinary,
    Sibling(usize),
    OtherCompeting(HeaderId),
    None,
    Unverified,
}

#[derive(Clone)]
struct AttackCandidate {
    id: HeaderId,
}

#[derive(Clone)]
struct AttackEvidence {
    ordinary_id: HeaderId,
    ordinary_id_text: String,
    parent_id: HeaderId,
    parent_id_text: String,
    slot: u64,
    body_root: String,
    transaction_hashes: Vec<String>,
    siblings: Vec<(usize, AttackCandidate)>,
}

#[derive(Clone)]
struct ConsensusState {
    tip_id: HeaderId,
    tip_id_text: String,
    tip_height: u64,
    tip_slot: u64,
    lib_id: HeaderId,
    lib_id_text: String,
    lib_slot: u64,
    tip_descends_from_lib: Option<bool>,
}

#[derive(Clone)]
struct CanonicalTipState {
    id: HeaderId,
    id_text: String,
    height: u64,
    slot: u64,
    observed_at_unix_ms: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum FinalizedHistoryRelation {
    Shared,
    Ancestral,
    Conflicting,
    Unverified,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TipTransition {
    Unchanged,
    Descendant,
    Reorganization,
    Unverified,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct StableFinalityCompletion {
    started_elapsed_ms: u128,
    completed_elapsed_ms: u128,
}

mod sustained;

struct FinalityNodeObservation {
    node_name: String,
    observed_at_unix_ms: u64,
    snapshot: Value,
    consensus: Option<ConsensusState>,
    tip_path_complete: bool,
    finalized_path_complete: bool,
    attack_branch: AttackBranch,
    finalized_attack_branch: AttackBranch,
    continuity: LibContinuity,
}

impl FinalityNodeObservation {
    fn tip_descends_from_lib(&self) -> bool {
        self.consensus
            .as_ref()
            .is_some_and(|state| state.tip_descends_from_lib == Some(true))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
enum LibContinuity {
    Verified,
    Unchanged,
    Baseline,
    RegressionOrUnrelated,
    Unverified,
    MissingPreviousLibBaseline,
}

impl LibContinuity {
    const fn is_verified(self) -> bool {
        matches!(self, Self::Verified | Self::Unchanged | Self::Baseline)
    }
}

#[derive(Clone, Default)]
struct ReceiverEvidence {
    matching_events: Vec<ReceiverEvent>,
    mismatched_events: Vec<ReceiverEvent>,
}

impl ReceiverEvidence {
    fn has_stage(&self, stage: &str) -> bool {
        self.matching_events
            .iter()
            .any(|event| event.stage == stage)
    }

    fn has_ordered_blend_to_acceptance(&self) -> bool {
        self.matching_events
            .iter()
            .filter(|blend| blend.stage == "blend_received")
            .any(|blend| {
                self.matching_events
                    .iter()
                    .filter(|chain| chain.stage == "chain_proposal_received")
                    .filter(|chain| receiver_event_precedes(blend, chain))
                    .any(|chain| {
                        self.matching_events
                            .iter()
                            .filter(|reconstructed| reconstructed.stage == "reconstructed")
                            .filter(|reconstructed| receiver_event_precedes(chain, reconstructed))
                            .any(|reconstructed| {
                                self.matching_events.iter().any(|accepted| {
                                    accepted.stage == "accepted"
                                        && receiver_event_precedes(reconstructed, accepted)
                                })
                            })
                    })
            })
    }

    fn has_terminal_stage(&self) -> bool {
        [
            "accepted",
            "rejected",
            "apply_failed",
            "not_processed",
            "reconstruction_failed",
        ]
        .iter()
        .any(|stage| self.has_stage(stage))
    }

    fn stage_names(&self) -> Vec<&'static str> {
        self.ordered_events()
            .into_iter()
            .map(|event| event.stage)
            .collect()
    }

    fn ordered_events(&self) -> Vec<&ReceiverEvent> {
        let mut events = self.matching_events.iter().collect::<Vec<_>>();
        events.sort_by(|first, second| match (first.timestamp, second.timestamp) {
            (Some(first_time), Some(second_time)) => match first_time.cmp(&second_time) {
                std::cmp::Ordering::Equal if first.path == second.path => {
                    first.offset.cmp(&second.offset)
                }
                ordering => ordering,
            },
            (Some(_), None) => std::cmp::Ordering::Less,
            (None, Some(_)) => std::cmp::Ordering::Greater,
            (None, None) => std::cmp::Ordering::Equal,
        });
        events
    }
}

fn receiver_event_precedes(first: &ReceiverEvent, second: &ReceiverEvent) -> bool {
    let (Some(first_time), Some(second_time)) = (first.timestamp, second.timestamp) else {
        return false;
    };
    match first_time.cmp(&second_time) {
        std::cmp::Ordering::Less => true,
        std::cmp::Ordering::Equal => first.path == second.path && first.offset < second.offset,
        std::cmp::Ordering::Greater => false,
    }
}

#[then(expr = "all declared audit nodes establish Blend readiness within {int} seconds")]
async fn step_audit_blend_readiness(world: &mut CucumberWorld, seconds: u64) -> StepResult {
    if seconds == 0 || world.security_audit_state.is_some() {
        return Err(StepError::InvalidArgument {
            message: "readiness duration must be positive and declared only once".to_owned(),
        });
    }
    let count =
        audit_sibling_count(world).map_err(|message| StepError::InvalidArgument { message })?;
    let provenance = verify_runtime_configuration_with_siblings(world, "security audit", count)
        .await
        .map_err(|message| StepError::StepFail { message })?;
    let (readiness, tails) = wait_for_blend_readiness(world, Duration::from_secs(seconds))
        .await
        .map_err(|message| StepError::StepFail { message })?;
    world.security_audit_state = Some(AuditScenarioState {
        provenance,
        readiness,
        tails,
        logs: BTreeMap::new(),
        opportunity: None,
        nonempty: None,
    });
    Ok(())
}

#[when(
    expr = "the audit producer completes a genuine leadership opportunity with at least {int} valid siblings within {int} seconds"
)]
async fn step_audit_opportunity(
    world: &mut CucumberWorld,
    minimum: usize,
    seconds: u64,
) -> StepResult {
    let count =
        audit_sibling_count(world).map_err(|message| StepError::InvalidArgument { message })?;
    if !valid_sibling_minimum(count, minimum) || seconds == 0 {
        return Err(StepError::InvalidArgument {
            message: "invalid sibling minimum or leadership duration".to_owned(),
        });
    }
    let state = world
        .security_audit_state
        .as_mut()
        .ok_or_else(|| StepError::StepFail {
            message: "Blend readiness has not been established".to_owned(),
        })?;
    if state.opportunity.is_some() {
        return Err(StepError::InvalidArgument {
            message: "an audit opportunity is already selected".to_owned(),
        });
    }
    let producer = state.tails.participants.producer.clone();
    let opportunity = wait_for_producer_opportunity(
        &mut state.tails,
        state.logs.entry(producer).or_default(),
        minimum,
        Duration::from_secs(seconds),
    )
    .await
    .map_err(|message| StepError::StepFail { message })?;
    if parse_usize(&opportunity.ordinary, "configured_siblings") != Some(count) {
        return Err(StepError::StepFail {
            message: "producer opportunity contradicts declared sibling configuration".to_owned(),
        });
    }
    record_observed_audit_opportunity(&mut state.provenance, 1);
    state.opportunity = Some(opportunity);
    Ok(())
}

#[then(
    expr = "at least {int} sibling proposals are accepted by declared honest nodes through ordered Blend-to-consensus paths within {int} seconds"
)]
#[expect(
    clippy::too_many_lines,
    reason = "Preserves the full receiver evidence report"
)]
async fn step_verify_sibling_propagation(
    world: &mut CucumberWorld,
    minimum_accepted: usize,
    seconds: u64,
) -> StepResult {
    if minimum_accepted == 0 || seconds == 0 {
        return Err(StepError::InvalidArgument {
            message: "sibling count must be positive".to_owned(),
        });
    }
    if world
        .security_audit_state
        .as_ref()
        .is_some_and(|state| state.nonempty.is_some())
    {
        return record_nonempty_sibling_propagation(
            world,
            minimum_accepted,
            Duration::from_secs(seconds),
        )
        .await;
    }
    let artifact_dir = world
        .lifecycle
        .scenario_base_dir
        .join("security-audit-evidence");
    fs::create_dir_all(&artifact_dir).map_err(|error| StepError::StepFail {
        message: format!("cannot create audit evidence directory: {error}"),
    })?;

    let mut state = world
        .security_audit_state
        .take()
        .ok_or_else(|| StepError::StepFail {
            message: "audit opportunity state is unavailable".to_owned(),
        })?;
    let result = observe_sibling_propagation(
        world,
        &mut state,
        minimum_accepted,
        Duration::from_secs(seconds),
    )
    .await;
    world.security_audit_state = Some(state);
    let log_capture = copy_node_logs(world, &artifact_dir);
    let mut report = match result {
        Ok(report) => report,
        Err(error) => {
            let mut details = error.details;
            details["status"] = json!("failed");
            details["failure"] = json!(error.message);
            details
        }
    };
    report["artifact_dir"] = json!(artifact_dir);
    match &log_capture {
        Ok(paths) => report["node_logs"] = json!(paths),
        Err(error) => report["node_log_capture_error"] = json!(error),
    }
    let storage_events = report["storage_observations"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|event| serde_json::to_string(event).ok())
        .collect::<Vec<_>>();
    let mut storage_evidence = storage_events.join("\n");
    if !storage_events.is_empty() {
        storage_evidence.push('\n');
    }
    fs::write(
        artifact_dir.join("receiver-observations.jsonl"),
        storage_evidence,
    )
    .map_err(|error| StepError::StepFail {
        message: format!("cannot write receiver storage evidence: {error}"),
    })?;
    let report_path = artifact_dir.join("opportunity.json");
    fs::write(
        &report_path,
        serde_json::to_vec_pretty(&report).map_err(|error| StepError::StepFail {
            message: format!("cannot encode audit evidence: {error}"),
        })?,
    )
    .map_err(|error| StepError::StepFail {
        message: format!("cannot write {}: {error}", report_path.display()),
    })?;

    if let Err(error) = log_capture {
        return Err(StepError::StepFail {
            message: format!(
                "audit evidence was collected, but node logs could not be preserved: {error}; evidence: {}",
                report_path.display()
            ),
        });
    }

    if report["honest_peer_validation"].as_bool() == Some(true) {
        tracing::info!(
            target: TARGET,
            event = "security_audit_scenario_evidence",
            evidence_path = %report_path.display(),
            "An honest node received, reconstructed, and applied a same-slot sibling"
        );
        Ok(())
    } else {
        let failure = report["failure"]
            .as_str()
            .unwrap_or("no honest node validation evidence was observed");
        Err(StepError::StepFail {
            message: format!(
                "security-audit sibling assertion failed: {failure}; evidence: {}",
                report_path.display()
            ),
        })
    }
}

#[when(
    expr = "the audit producer completes a genuine nonempty opportunity with at least {int} valid siblings"
)]
async fn step_select_nonempty_opportunity(world: &mut CucumberWorld, minimum: usize) -> StepResult {
    let count =
        audit_sibling_count(world).map_err(|message| StepError::InvalidArgument { message })?;
    if !valid_sibling_minimum(count, minimum) {
        return Err(StepError::InvalidArgument {
            message: "invalid minimum sibling count".to_owned(),
        });
    }
    world.reset_wallet_scanner_after_current_iteration().await;
    let mut state = world
        .security_audit_state
        .take()
        .ok_or_else(|| StepError::StepFail {
            message: "Blend readiness has not been established".to_owned(),
        })?;
    let result = select_nonempty_sibling_opportunity(world, &mut state).await;
    world.security_audit_state = Some(state);
    if let Err(failure) = result {
        let artifact_dir = world
            .lifecycle
            .scenario_base_dir
            .join("security-audit-nonempty-evidence");
        fs::create_dir_all(&artifact_dir).map_err(|error| StepError::StepFail {
            message: format!("cannot create evidence directory: {error}"),
        })?;
        let path = artifact_dir.join("nonempty-opportunity.json");
        let logs = copy_node_logs(world, &artifact_dir);
        let report = json!({ "status": "failed", "failure": failure.message, "details": failure.details, "node_logs": logs.as_ref().ok(), "node_log_capture_error": logs.err() });
        fs::write(
            &path,
            serde_json::to_vec_pretty(&report).map_err(|error| StepError::StepFail {
                message: error.to_string(),
            })?,
        )
        .map_err(|error| StepError::StepFail {
            message: error.to_string(),
        })?;
        return Err(StepError::StepFail {
            message: format!("{}; evidence: {}", report["failure"], path.display()),
        });
    }
    Ok(())
}

#[when(
    expr = "the audit producer is stopped upon the first selected sibling's Blend reception by a declared honest node within {int} seconds"
)]
async fn step_stop_on_selected_sibling_reception(
    world: &mut CucumberWorld,
    seconds: u64,
) -> StepResult {
    if seconds == 0 {
        return Err(StepError::InvalidArgument {
            message: "reception deadline must be positive".to_owned(),
        });
    }
    let mut state = world
        .security_audit_state
        .take()
        .ok_or_else(|| StepError::StepFail {
            message: "no selected opportunity".to_owned(),
        })?;
    let selection = state.nonempty.as_ref().ok_or_else(|| StepError::StepFail {
        message: "no selected nonempty opportunity".to_owned(),
    })?;
    let selected_at = selection.selected_at;
    let selected_at_unix_ms = selection.selected_at_unix_ms;
    let opportunity = state
        .opportunity
        .as_ref()
        .ok_or_else(|| StepError::StepFail {
            message: "no selected opportunity".to_owned(),
        })?
        .clone();
    let participants = state.tails.participants.clone();
    let deadline = selected_at + Duration::from_secs(seconds);
    let result = wait_for_selected_reception(
        &mut state,
        &opportunity,
        &participants.honest_nodes,
        deadline,
    )
    .await;
    let observed_at = Instant::now();
    let observed_at_unix_ms = unix_timestamp_ms();
    // Always stop the producer, including on a missing or unreadable trigger.
    let stop_result = crate::cucumber::steps::nodes::stop_node(
        world,
        "stop on selected sibling reception",
        &participants.producer,
    )
    .await;
    let stopped_at = world
        .lifecycle
        .node_stopped_at
        .get(&participants.producer)
        .copied();
    let prompt = stopped_at.is_some_and(|stop| stop <= deadline);
    let report = json!({
        "status": if result.is_ok() && stop_result.is_ok() && prompt { "observed" } else { "unverified" },
        "selected_at_unix_ms": selected_at_unix_ms,
        "trigger_observed_at_unix_ms": result.as_ref().ok().map(|_| observed_at_unix_ms),
        "selection_to_reception_ms": result.as_ref().ok().map(|_| observed_at.duration_since(selected_at).as_millis()),
        "reception_to_stop_ms": result.as_ref().ok().and_then(|_| stopped_at.map(|stop| stop.duration_since(observed_at).as_millis())),
        "selection_to_stop_ms": stopped_at.map(|stop| stop.duration_since(selected_at).as_millis()),
        "stop_completed_unix_ms": stopped_at.map(|stop| unix_timestamp_ms().saturating_sub(u64::try_from(stop.elapsed().as_millis()).unwrap_or(u64::MAX))),
        "deadline_seconds": seconds,
        "trigger": result.as_ref().ok().map(|(node, sibling, event)| json!({ "node": node, "sibling_id": sibling, "event_timestamp": format_log_timestamp(event.timestamp), "source_log": event.path, "source_offset": event.offset, "parent_block_id": event.parent_block_id, "slot": event.slot })),
        "error": result.as_ref().err(),
        "stop_error": stop_result.as_ref().err().map(ToString::to_string),
    });
    let path = world
        .lifecycle
        .scenario_base_dir
        .join("security-audit-nonempty-evidence/shutdown-trigger.json");
    fs::create_dir_all(path.parent().expect("artifact path has a parent")).map_err(|error| {
        StepError::StepFail {
            message: error.to_string(),
        }
    })?;
    fs::write(
        &path,
        serde_json::to_vec_pretty(&report).map_err(|error| StepError::StepFail {
            message: error.to_string(),
        })?,
    )
    .map_err(|error| StepError::StepFail {
        message: error.to_string(),
    })?;
    world.security_audit_state = Some(state);
    stop_result?;
    if let Err(error) = result {
        return Err(StepError::StepFail {
            message: format!(
                "shutdown trigger unverified: {error}; evidence: {}",
                path.display()
            ),
        });
    }
    if !prompt {
        return Err(StepError::StepFail {
            message: format!(
                "producer shutdown exceeded the selection deadline; evidence: {}",
                path.display()
            ),
        });
    }
    Ok(())
}

async fn wait_for_selected_reception(
    state: &mut AuditScenarioState,
    opportunity: &ProducerOpportunity,
    honest_nodes: &[String],
    deadline: Instant,
) -> Result<(String, String, ReceiverEvent), String> {
    loop {
        let mut error = None;
        for node in honest_nodes {
            match state.tails.read_new_lines(node) {
                Ok(lines) => state.logs.entry(node.clone()).or_default().extend(lines),
                Err(message) => {
                    error = Some(message);
                    break;
                }
            }
        }
        if let Some(error) = error {
            break Err(error);
        }
        if Instant::now() <= deadline
            && let Some(trigger) =
                selected_sibling_reception(&state.logs, opportunity, honest_nodes)
        {
            break Ok(trigger);
        }
        if Instant::now() >= deadline {
            break Err(
                "selected sibling Blend reception was not observed before the deadline".to_owned(),
            );
        }
        sleep(Duration::from_millis(250).min(deadline.saturating_duration_since(Instant::now())))
            .await;
    }
}

fn selected_sibling_reception(
    logs: &BTreeMap<String, Vec<ObservedLogLine>>,
    opportunity: &ProducerOpportunity,
    honest_nodes: &[String],
) -> Option<(String, String, ReceiverEvent)> {
    let ids = opportunity
        .siblings
        .iter()
        .filter_map(|(_, sibling)| sibling.get("sibling_block_id").cloned())
        .collect::<BTreeSet<_>>();
    let nodes = honest_nodes.iter().map(String::as_str).collect::<Vec<_>>();
    receiver_stages(logs, &ids, &opportunity.ordinary, &nodes)
        .into_iter()
        .flat_map(|((node, sibling), evidence)| {
            evidence
                .matching_events
                .into_iter()
                .filter(|event| event.stage == "blend_received" && event.timestamp.is_some())
                .map(move |event| (node.clone(), sibling.clone(), event))
        })
        .min_by(|a, b| {
            a.2.timestamp
                .cmp(&b.2.timestamp)
                .then_with(|| a.0.cmp(&b.0))
                .then_with(|| a.2.offset.cmp(&b.2.offset))
        })
}

#[then(
    "the selected ordinary block contains a submitted transaction and its siblings preserve its identity"
)]
#[expect(
    clippy::needless_pass_by_ref_mut,
    reason = "Cucumber step entrypoints require a mutable world"
)]
fn step_verify_selected_nonempty_body(world: &mut CucumberWorld) -> StepResult {
    let state = world
        .security_audit_state
        .as_ref()
        .ok_or_else(|| StepError::StepFail {
            message: "audit opportunity state unavailable".to_owned(),
        })?;
    let selection = state.nonempty.as_ref().ok_or_else(|| StepError::StepFail {
        message: "nonempty opportunity not selected".to_owned(),
    })?;
    let opportunity = state
        .opportunity
        .as_ref()
        .ok_or_else(|| StepError::StepFail {
            message: "opportunity not selected".to_owned(),
        })?;
    if !has_nonempty_submitted_body(
        &selection.ordinary_body.transaction_hashes,
        &selection.submitted_hashes,
    ) || !sibling_body_integrity(&selection.ordinary_body, opportunity)
    {
        return Err(StepError::StepFail {
            message: "selected opportunity failed transaction or sibling identity checks"
                .to_owned(),
        });
    }
    Ok(())
}

#[then("accepted siblings have the ordinary block's body root and ordered transactions")]
#[expect(
    clippy::needless_pass_by_ref_mut,
    reason = "Cucumber step entrypoints require a mutable world"
)]
fn step_verify_accepted_sibling_bodies(world: &mut CucumberWorld) -> StepResult {
    let path = world
        .lifecycle
        .scenario_base_dir
        .join("security-audit-nonempty-evidence/nonempty-opportunity.json");
    let bytes = fs::read(&path).map_err(|error| StepError::StepFail {
        message: format!("cannot read {}: {error}", path.display()),
    })?;
    let report: Value = serde_json::from_slice(&bytes).map_err(|error| StepError::StepFail {
        message: format!("cannot parse {}: {error}", path.display()),
    })?;
    if report["honest_peer_validation"] != true
        || report["accepted_bodies_all_match_ordinary"] != true
        || report["sibling_body_integrity"] != true
    {
        return Err(StepError::StepFail {
            message: format!(
                "accepted sibling body integrity is not verified; evidence: {}",
                path.display()
            ),
        });
    }
    Ok(())
}

async fn record_nonempty_sibling_propagation(
    world: &mut CucumberWorld,
    minimum_accepted: usize,
    timeout: Duration,
) -> StepResult {
    let artifact_dir = world
        .lifecycle
        .scenario_base_dir
        .join("security-audit-nonempty-evidence");
    fs::create_dir_all(&artifact_dir).map_err(|error| StepError::StepFail {
        message: format!("cannot create nonempty audit evidence directory: {error}"),
    })?;

    let mut state = world
        .security_audit_state
        .take()
        .ok_or_else(|| StepError::StepFail {
            message: "audit opportunity state unavailable".to_owned(),
        })?;
    let result =
        observe_nonempty_sibling_propagation(world, &mut state, timeout, minimum_accepted).await;
    world.security_audit_state = Some(state);
    let log_capture = copy_node_logs(world, &artifact_dir);
    let mut report = match result {
        Ok(report) => report,
        Err(error) => {
            let mut details = error.details;
            details["status"] = json!("failed");
            details["failure"] = json!(error.message);
            details
        }
    };
    report["artifact_dir"] = json!(artifact_dir);
    match &log_capture {
        Ok(paths) => report["node_logs"] = json!(paths),
        Err(error) => report["node_log_capture_error"] = json!(error),
    }
    let report_path = artifact_dir.join("nonempty-opportunity.json");
    fs::write(
        &report_path,
        serde_json::to_vec_pretty(&report).map_err(|error| StepError::StepFail {
            message: format!("cannot encode nonempty audit evidence: {error}"),
        })?,
    )
    .map_err(|error| StepError::StepFail {
        message: format!("cannot write {}: {error}", report_path.display()),
    })?;

    if let Err(error) = log_capture {
        return Err(StepError::StepFail {
            message: format!(
                "nonempty audit evidence was collected, but node logs could not be preserved: {error}; evidence: {}",
                report_path.display()
            ),
        });
    }

    if report["nonempty_honest_peer_validation"].as_bool() == Some(true) {
        tracing::info!(
            target: TARGET,
            event = "security_audit_nonempty_scenario_evidence",
            evidence_path = %report_path.display(),
            "An honest node reconstructed and applied a same-slot sibling with the ordinary block transaction body"
        );
        Ok(())
    } else {
        let failure = report["failure"]
            .as_str()
            .unwrap_or("no honest node validated a sibling with the matching nonempty body");
        Err(StepError::StepFail {
            message: format!(
                "nonempty security-audit sibling assertion failed: {failure}; evidence: {}",
                report_path.display()
            ),
        })
    }
}

fn unix_timestamp_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| {
            u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
        })
}

async fn capture_pre_attack_consensus(world: &CucumberWorld) -> Value {
    let Ok(participants) = audit_participants(world) else {
        return json!([]);
    };
    let observations = join_all(participants.honest_nodes.iter().map(async |node_name| {
        let Some(node) = world.nodes_info.get(node_name) else {
            return json!({
                "node": node_name,
                "observed_at_unix_ms": unix_timestamp_ms(),
                "responsive": false,
                "error": "node was not started",
            });
        };

        let observed_at_unix_ms = unix_timestamp_ms();
        let consensus = match node.client.consensus_info().await {
            Ok(consensus) => consensus,
            Err(error) => {
                return json!({
                    "node": node_name,
                    "observed_at_unix_ms": observed_at_unix_ms,
                    "responsive": false,
                    "error": error.to_string(),
                });
            }
        };
        let info = &consensus.cryptarchia_info;
        let mut cache = HeaderCache::default();
        let path = walk_to_target(
            &node.client,
            info.tip,
            info.lib,
            u64::from(info.lib_slot),
            &mut cache,
        )
        .await;
        let descends_from_lib = ancestry_presence(&path, info.lib);

        json!({
            "node": node_name,
            "observed_at_unix_ms": observed_at_unix_ms,
            "responsive": true,
            "phase": format!("{:?}", consensus.phase),
            "tip_id": info.tip.encode_hex::<String>(),
            "tip_height": info.height,
            "tip_slot": u64::from(info.slot),
            "lib_id": info.lib.encode_hex::<String>(),
            "lib_slot": u64::from(info.lib_slot),
            "tip_descends_from_lib": descends_from_lib,
            "tip_to_lib_ancestry": ancestry_path_json(&path),
            "header_lookup_failures": header_cache_failures_json(&cache),
        })
    }))
    .await;
    Value::Array(observations)
}

async fn walk_to_target(
    client: &NodeHttpClient,
    start: HeaderId,
    target: HeaderId,
    target_slot: u64,
    cache: &mut HeaderCache,
) -> AncestryPath {
    let mut links = Vec::new();
    let mut current = start;
    let mut seen = HashSet::new();
    for _ in 0..FINALITY_MAX_ANCESTRY_DEPTH {
        if current == target {
            return AncestryPath {
                links,
                target_reached: true,
                complete_below_slot: true,
                error: None,
            };
        }
        if !seen.insert(current) {
            return AncestryPath {
                links,
                target_reached: false,
                complete_below_slot: false,
                error: Some(format!(
                    "ancestry cycle at {}",
                    current.encode_hex::<String>()
                )),
            };
        }
        let link = match fetch_header_link(client, current, cache).await {
            Ok(link) => link,
            Err(error) => {
                return AncestryPath {
                    links,
                    target_reached: false,
                    complete_below_slot: false,
                    error: Some(error),
                };
            }
        };
        links.push(link);
        if link.parent == target {
            return AncestryPath {
                links,
                target_reached: true,
                complete_below_slot: true,
                error: None,
            };
        }
        if link.parent == current {
            return self_parented_ancestry_result(links, current);
        }
        if link.slot < target_slot {
            return AncestryPath {
                links,
                target_reached: false,
                complete_below_slot: true,
                error: None,
            };
        }
        current = link.parent;
    }
    AncestryPath {
        links,
        target_reached: false,
        complete_below_slot: false,
        error: Some(format!(
            "ancestry depth exceeded {FINALITY_MAX_ANCESTRY_DEPTH} blocks"
        )),
    }
}

fn self_parented_ancestry_result(links: Vec<HeaderLink>, block_id: HeaderId) -> AncestryPath {
    AncestryPath {
        links,
        target_reached: false,
        complete_below_slot: false,
        error: Some(format!(
            "self-parented header {} cannot prove target ancestry or absence",
            block_id.encode_hex::<String>()
        )),
    }
}

fn proven_below_target_slot_path(current_slot: u64, target_slot: u64) -> Option<AncestryPath> {
    (current_slot < target_slot).then_some(AncestryPath {
        links: Vec::new(),
        target_reached: false,
        complete_below_slot: true,
        error: None,
    })
}

async fn fetch_header_link(
    client: &NodeHttpClient,
    id: HeaderId,
    cache: &mut HeaderCache,
) -> Result<HeaderLink, String> {
    if let Some(link) = cache.links.get(&id) {
        return Ok(*link);
    }
    let id_text = id.encode_hex::<String>();
    let block = match client.block(&id).await {
        Ok(Some(block)) => block,
        Ok(None) => {
            let error = format!("block API returned no block for {id_text}");
            cache.failures.insert(id_text, error.clone());
            return Err(error);
        }
        Err(error) => {
            let error = format!("block API failed for {id_text}: {error}");
            cache.failures.insert(id_text, error.clone());
            return Err(error);
        }
    };
    if block.header.id != id {
        return Err(format!(
            "block API returned {} while resolving {id_text}",
            block.header.id.encode_hex::<String>()
        ));
    }
    let link = HeaderLink {
        id,
        parent: block.header.parent_block,
        slot: u64::from(block.header.slot),
    };
    cache.links.insert(id, link);
    Ok(link)
}

fn ancestry_presence(path: &AncestryPath, target: HeaderId) -> Option<bool> {
    if path.target_reached
        || path
            .links
            .iter()
            .any(|link| link.id == target || link.parent == target)
    {
        Some(true)
    } else if path.complete_below_slot {
        Some(false)
    } else {
        None
    }
}

fn ancestry_path_json(path: &AncestryPath) -> Value {
    json!({
        "block_ids": path.links.iter().map(|link| link.id.encode_hex::<String>()).collect::<Vec<_>>(),
        "header_links": path.links.iter().map(|link| json!({
            "block_id": link.id.encode_hex::<String>(),
            "parent_block_id": link.parent.encode_hex::<String>(),
            "slot": link.slot,
        })).collect::<Vec<_>>(),
        "target_reached": path.target_reached,
        "complete_below_target": path.complete_below_slot,
        "error": path.error,
    })
}

fn header_cache_failures_json(cache: &HeaderCache) -> Value {
    Value::Object(
        cache
            .failures
            .iter()
            .map(|(id, error)| (id.clone(), json!(error)))
            .collect(),
    )
}

#[then(
    expr = "the honest nodes converge with tip height skew at most {int} and finalize beyond the nonempty equivocation for {int} seconds within {int} seconds, with producer stopped within {int} seconds"
)]
#[expect(
    clippy::too_many_lines,
    reason = "Reads and preserves the complete finality evidence for this scenario"
)]
#[expect(
    clippy::needless_pass_by_ref_mut,
    reason = "Cucumber step functions require the world as the first `&mut` argument"
)]
async fn step_verify_finality_after_nonempty_equivocation(
    world: &mut CucumberWorld,
    max_tip_height_skew: u64,
    stability: u64,
    timeout: u64,
    prompt_stop: u64,
) -> StepResult {
    if stability == 0 || stability > timeout || prompt_stop == 0 {
        return Err(StepError::InvalidArgument {
            message: "finality stability must be positive and no longer than its timeout"
                .to_owned(),
        });
    }
    let artifact_dir = world
        .lifecycle
        .scenario_base_dir
        .join("security-audit-nonempty-evidence");
    let opportunity_path = artifact_dir.join("nonempty-opportunity.json");
    let opportunity_bytes = fs::read(&opportunity_path).map_err(|error| StepError::StepFail {
        message: format!(
            "cannot read selected nonempty opportunity {}: {error}",
            opportunity_path.display()
        ),
    })?;
    let opportunity: Value =
        serde_json::from_slice(&opportunity_bytes).map_err(|error| StepError::StepFail {
            message: format!(
                "cannot parse selected nonempty opportunity {}: {error}",
                opportunity_path.display()
            ),
        })?;
    let evidence = parse_attack_evidence(&opportunity).map_err(|error| StepError::StepFail {
        message: format!(
            "selected nonempty opportunity is incomplete: {error}; evidence: {}",
            opportunity_path.display()
        ),
    })?;

    let report = observe_finality_after_equivocation(
        world,
        &opportunity,
        &evidence,
        Duration::from_secs(stability),
        Duration::from_secs(timeout),
        max_tip_height_skew,
        Duration::from_secs(prompt_stop),
    )
    .await;
    let node_logs = copy_node_logs(world, &artifact_dir);
    let node_log_hashes = node_logs.as_ref().ok().map(|paths| {
        paths
            .iter()
            .map(|path| {
                json!({
                    "path": path,
                    "sha256": sha256_file(Path::new(path)).ok(),
                })
            })
            .collect::<Vec<_>>()
    });
    let mut report = match report {
        Ok(report) => report,
        Err(error) => json!({
            "status": "failed",
            "failure": error,
            "failure_classification": "incomplete_finality_observation",
        }),
    };
    report["selected_opportunity_path"] = json!(opportunity_path.display().to_string());
    report["selected_opportunity_sha256"] = json!(sha256_file(&opportunity_path).ok());
    report["finality_artifact_path"] = json!(
        artifact_dir
            .join("finality-observations.json")
            .display()
            .to_string()
    );
    match &node_logs {
        Ok(paths) => {
            report["node_logs_after_producer_stop"] = json!(paths);
            report["node_log_sha256"] = json!(node_log_hashes);
        }
        Err(error) => report["node_log_capture_error"] = json!(error),
    }
    let report_path = artifact_dir.join("finality-observations.json");
    fs::write(
        &report_path,
        serde_json::to_vec_pretty(&report).map_err(|error| StepError::StepFail {
            message: format!("cannot encode finality observations: {error}"),
        })?,
    )
    .map_err(|error| StepError::StepFail {
        message: format!("cannot write {}: {error}", report_path.display()),
    })?;

    if let Err(error) = node_logs {
        return Err(StepError::StepFail {
            message: format!(
                "finality observations were collected, but node logs could not be preserved: {error}; evidence: {}",
                report_path.display()
            ),
        });
    }
    if report["status"].as_str() == Some("passed") {
        tracing::info!(
            target: TARGET,
            event = "security_audit_finality_evidence",
            evidence_path = %report_path.display(),
            "Honest nodes converged and advanced a common LIB beyond the observed equivocations"
        );
        Ok(())
    } else {
        let failure = report["failure"]
            .as_str()
            .unwrap_or("honest nodes did not satisfy convergence and finality checks");
        Err(StepError::StepFail {
            message: format!(
                "fork-choice/finality experiment failed: {failure}; evidence: {}",
                report_path.display()
            ),
        })
    }
}

fn parse_attack_evidence(report: &Value) -> Result<AttackEvidence, String> {
    if report["nonempty_honest_peer_validation"].as_bool() != Some(true) {
        return Err("the nonempty sibling acceptance assertion did not pass".to_owned());
    }
    let ordinary = &report["selected_ordinary_block"];
    let ordinary_id_text = ordinary["block_id"]
        .as_str()
        .ok_or_else(|| "ordinary block ID is missing".to_owned())?
        .to_owned();
    let ordinary_id = parse_header_id(&ordinary_id_text)?;
    let parent_id_text = ordinary["parent_block_id"]
        .as_str()
        .ok_or_else(|| "original parent block ID is missing".to_owned())?
        .to_owned();
    let parent_id = parse_header_id(&parent_id_text)?;
    let slot = ordinary["slot"]
        .as_u64()
        .ok_or_else(|| "attack slot is missing or invalid".to_owned())?;
    let body_root = ordinary["body_root"]
        .as_str()
        .ok_or_else(|| "ordinary block body root is missing".to_owned())?
        .to_owned();
    let transaction_hashes = ordinary["transaction_hashes"]
        .as_array()
        .ok_or_else(|| "ordered ordinary transaction hashes are missing".to_owned())?
        .iter()
        .map(|hash| {
            hash.as_str()
                .map(str::to_owned)
                .ok_or_else(|| "ordinary transaction hash is not a string".to_owned())
        })
        .collect::<Result<Vec<_>, _>>()?;
    if transaction_hashes.is_empty() {
        return Err("the selected ordinary block has an empty transaction body".to_owned());
    }
    let siblings = report["siblings"]
        .as_array()
        .ok_or_else(|| "sibling evidence is missing".to_owned())?
        .iter()
        .map(|sibling| {
            let index = json_usize(&sibling["sibling_index"])
                .ok_or_else(|| "sibling index is missing or invalid".to_owned())?;
            let id_text = sibling["sibling_block_id"]
                .as_str()
                .ok_or_else(|| format!("sibling {index} block ID is missing"))?
                .to_owned();
            Ok((
                index,
                AttackCandidate {
                    id: parse_header_id(&id_text)?,
                },
            ))
        })
        .collect::<Result<Vec<_>, String>>()?;
    let sibling_indexes = siblings
        .iter()
        .map(|(index, _)| *index)
        .collect::<BTreeSet<_>>();
    if sibling_indexes != BTreeSet::from([1, 2, 3, 4]) {
        return Err(format!(
            "expected sibling indexes 1–4, got {sibling_indexes:?}"
        ));
    }
    Ok(AttackEvidence {
        ordinary_id,
        ordinary_id_text,
        parent_id,
        parent_id_text,
        slot,
        body_root,
        transaction_hashes,
        siblings,
    })
}

fn json_usize(value: &Value) -> Option<usize> {
    value
        .as_u64()
        .and_then(|value| usize::try_from(value).ok())
        .or_else(|| value.as_str()?.parse().ok())
}

fn classify_attack_branch(
    path: &[HeaderLink],
    complete_below_attack: bool,
    attack: &AttackEvidence,
) -> AttackBranch {
    if let Some(link) = path
        .iter()
        .find(|link| (link.slot, link.parent) == (attack.slot, attack.parent_id))
    {
        if link.id == attack.ordinary_id {
            return AttackBranch::Ordinary;
        }
        if let Some((index, _)) = attack
            .siblings
            .iter()
            .find(|(_, candidate)| candidate.id == link.id)
        {
            return AttackBranch::Sibling(*index);
        }
        return AttackBranch::OtherCompeting(link.id);
    }
    if complete_below_attack {
        AttackBranch::None
    } else {
        AttackBranch::Unverified
    }
}

fn attack_branch_json(branch: &AttackBranch, attack: &AttackEvidence) -> Value {
    match branch {
        AttackBranch::Ordinary => json!({
            "kind": "ordinary",
            "block_id": attack.ordinary_id_text,
        }),
        AttackBranch::Sibling(index) => json!({"kind": "sibling", "sibling_index": index}),
        AttackBranch::OtherCompeting(id) => json!({
            "kind": "other_competing_block",
            "block_id": id.encode_hex::<String>(),
        }),
        AttackBranch::None => json!({"kind": "none_of_selected_equivocation_blocks"}),
        AttackBranch::Unverified => json!({"kind": "unverified"}),
    }
}

async fn observe_finality_node(
    node_name: &str,
    client: &NodeHttpClient,
    attack: &AttackEvidence,
    cache: &mut HeaderCache,
    previous_lib: Option<(HeaderId, u64)>,
) -> FinalityNodeObservation {
    let observed_at_unix_ms = unix_timestamp_ms();
    let consensus = match client.consensus_info().await {
        Ok(consensus) => consensus,
        Err(error) => {
            return FinalityNodeObservation {
                node_name: node_name.to_owned(),
                observed_at_unix_ms,
                snapshot: json!({
                    "node": node_name,
                    "observed_at_unix_ms": observed_at_unix_ms,
                    "responsive": false,
                    "error": error.to_string(),
                    "header_lookup_failures": header_cache_failures_json(cache),
                }),
                consensus: None,
                tip_path_complete: false,
                finalized_path_complete: false,
                attack_branch: AttackBranch::Unverified,
                finalized_attack_branch: AttackBranch::Unverified,
                continuity: LibContinuity::Unverified,
            };
        }
    };
    let info = &consensus.cryptarchia_info;
    let tip_id = info.tip;
    let lib_id = info.lib;
    let tip_slot = u64::from(info.slot);
    let lib_slot = u64::from(info.lib_slot);
    let tip_path = walk_to_target(client, tip_id, attack.parent_id, attack.slot, cache).await;
    let attack_branch =
        classify_attack_branch(&tip_path.links, tip_path.complete_below_slot, attack);
    let tip_to_lib = walk_to_target(client, tip_id, lib_id, lib_slot, cache).await;
    let tip_descends_from_lib = ancestry_presence(&tip_to_lib, lib_id);
    let finalized_path = match proven_below_target_slot_path(lib_slot, attack.slot) {
        Some(path) => path,
        None => walk_to_target(client, lib_id, attack.parent_id, attack.slot, cache).await,
    };
    let finalized_attack_branch = classify_attack_branch(
        &finalized_path.links,
        finalized_path.complete_below_slot,
        attack,
    );
    let (continuity, continuity_path) = if let Some((previous_id, previous_slot)) = previous_lib {
        if previous_id == lib_id {
            (LibContinuity::Verified, None)
        } else {
            let path = walk_to_target(client, lib_id, previous_id, previous_slot, cache).await;
            match ancestry_presence(&path, previous_id) {
                Some(true) => (LibContinuity::Verified, Some(path)),
                Some(false) => (LibContinuity::RegressionOrUnrelated, Some(path)),
                None => (LibContinuity::Unverified, Some(path)),
            }
        }
    } else {
        (LibContinuity::MissingPreviousLibBaseline, None)
    };
    let state = ConsensusState {
        tip_id,
        tip_id_text: tip_id.encode_hex::<String>(),
        tip_height: info.height,
        tip_slot,
        lib_id,
        lib_id_text: lib_id.encode_hex::<String>(),
        lib_slot,
        tip_descends_from_lib,
    };
    let snapshot = json!({
        "node": node_name,
        "observed_at_unix_ms": observed_at_unix_ms,
        "responsive": true,
        "phase": format!("{:?}", consensus.phase),
        "tip_id": state.tip_id_text,
        "tip_height": state.tip_height,
        "tip_slot": state.tip_slot,
        "lib_id": state.lib_id_text,
        "lib_slot": state.lib_slot,
        "tip_descends_from_lib": state.tip_descends_from_lib,
        "tip_to_attack_parent_ancestry": ancestry_path_json(&tip_path),
        "canonical_attack_branch": attack_branch_json(&attack_branch, attack),
        "tip_to_lib_ancestry": ancestry_path_json(&tip_to_lib),
        "lib_to_attack_parent_ancestry": ancestry_path_json(&finalized_path),
        "finalized_attack_branch": attack_branch_json(&finalized_attack_branch, attack),
        "lib_continuity_from_previous_observation": continuity,
        "lib_continuity_ancestry": continuity_path.as_ref().map(ancestry_path_json),
        "header_lookup_failures": header_cache_failures_json(cache),
    });
    FinalityNodeObservation {
        node_name: node_name.to_owned(),
        observed_at_unix_ms,
        snapshot,
        consensus: Some(state),
        tip_path_complete: tip_path.complete_below_slot,
        finalized_path_complete: finalized_path.complete_below_slot,
        attack_branch,
        finalized_attack_branch,
        continuity,
    }
}

async fn block_id_at_height(
    client: &NodeHttpClient,
    tip: HeaderId,
    tip_height: u64,
    target_height: u64,
    cache: &mut HeaderCache,
) -> Result<HeaderId, String> {
    if target_height > tip_height {
        return Err(format!(
            "target height {target_height} exceeds tip height {tip_height}"
        ));
    }
    let mut current = tip;
    let mut height = tip_height;
    while height > target_height {
        let link = fetch_header_link(client, current, cache).await?;
        if link.parent == current {
            return Err(format!("self-parented block at height {height}"));
        }
        current = link.parent;
        height -= 1;
    }
    Ok(current)
}

fn canonical_tips_aligned(
    height_skew: u64,
    block_ids_at_common_height: &[HeaderId],
    max_tip_height_skew: u64,
) -> bool {
    height_skew <= max_tip_height_skew
        && block_ids_at_common_height
            .first()
            .is_some_and(|first| block_ids_at_common_height.iter().all(|id| id == first))
}

fn producer_stop_is_prompt(producer_stopped: bool, delay_ms: u64, limit: Duration) -> bool {
    producer_stopped && delay_ms <= u64::try_from(limit.as_millis()).unwrap_or(u64::MAX)
}

fn common_finalized_lib(
    states: &[(String, ConsensusState)],
    expected_nodes: usize,
) -> Option<(HeaderId, u64)> {
    let (_, first) = states.first()?;
    (states.len() == expected_nodes
        && states
            .iter()
            .all(|(_, state)| state.lib_id == first.lib_id && state.lib_slot == first.lib_slot))
    .then_some((first.lib_id, first.lib_slot))
}

fn classify_finalized_history_relation(
    first_id: HeaderId,
    first_slot: u64,
    second_id: HeaderId,
    second_slot: u64,
    first_descends_from_second: Option<bool>,
    second_descends_from_first: Option<bool>,
) -> FinalizedHistoryRelation {
    if first_id == second_id {
        return if first_slot == second_slot {
            FinalizedHistoryRelation::Shared
        } else {
            FinalizedHistoryRelation::Unverified
        };
    }
    if first_descends_from_second == Some(true) || second_descends_from_first == Some(true) {
        FinalizedHistoryRelation::Ancestral
    } else if first_descends_from_second == Some(false) && second_descends_from_first == Some(false)
    {
        FinalizedHistoryRelation::Conflicting
    } else {
        FinalizedHistoryRelation::Unverified
    }
}

fn classify_tip_transition(
    previous_tip_id: HeaderId,
    new_tip_id: HeaderId,
    new_tip_descends_from_previous: Option<bool>,
) -> TipTransition {
    if previous_tip_id == new_tip_id {
        TipTransition::Unchanged
    } else {
        match new_tip_descends_from_previous {
            Some(true) => TipTransition::Descendant,
            Some(false) => TipTransition::Reorganization,
            None => TipTransition::Unverified,
        }
    }
}

/// One sampled finality verdict. Collection and ancestry lookups happen before
/// this evaluator; absent or unverified evidence must be supplied as false.
#[derive(Clone, Copy, Debug)]
#[expect(
    clippy::struct_excessive_bools,
    reason = "Each independent observation is required for the finality verdict"
)]
struct FinalityConditions {
    producer_ready: bool,
    all_responsive: bool,
    all_progressed: bool,
    aligned: bool,
    common_beyond_boundary: bool,
    ancestry_verified: bool,
    continuity_verified: bool,
    no_finalized_conflict: bool,
}

impl FinalityConditions {
    const fn satisfied(self, elapsed_ms: u128, timeout_ms: u128) -> bool {
        self.producer_ready
            && self.all_responsive
            && self.all_progressed
            && self.aligned
            && self.common_beyond_boundary
            && self.ancestry_verified
            && self.continuity_verified
            && self.no_finalized_conflict
            && elapsed_ms <= timeout_ms
    }
}

/// Accumulates safety evidence; later agreement cannot erase an earlier
/// conflict or an incomplete ancestry lookup.
#[derive(Default, Debug)]
struct FinalizedSafety {
    conflict_observed: bool,
    ancestry_unverified: bool,
}

impl FinalizedSafety {
    const fn record_relation(&mut self, relation: FinalizedHistoryRelation) {
        match relation {
            FinalizedHistoryRelation::Conflicting => self.conflict_observed = true,
            FinalizedHistoryRelation::Unverified => self.ancestry_unverified = true,
            FinalizedHistoryRelation::Shared | FinalizedHistoryRelation::Ancestral => {}
        }
    }

    fn record_pair_reports(&mut self, reports: &[Value]) {
        for report in reports {
            let relation = match report["status"].as_str() {
                Some("shared_finalized_history") => FinalizedHistoryRelation::Shared,
                Some("ancestral_finalized_histories") => FinalizedHistoryRelation::Ancestral,
                Some("conflicting_finalized_histories") => FinalizedHistoryRelation::Conflicting,
                _ => FinalizedHistoryRelation::Unverified,
            };
            self.record_relation(relation);
        }
    }
}

fn update_stable_finality_window(
    conditions_hold: bool,
    elapsed_ms: u128,
    previous_started_elapsed_ms: Option<u128>,
    required_window_ms: u128,
) -> (Option<u128>, Option<StableFinalityCompletion>) {
    if !conditions_hold {
        return (None, None);
    }
    let started_elapsed_ms = previous_started_elapsed_ms.unwrap_or(elapsed_ms);
    let completion = (elapsed_ms.saturating_sub(started_elapsed_ms) >= required_window_ms)
        .then_some(StableFinalityCompletion {
            started_elapsed_ms,
            completed_elapsed_ms: elapsed_ms,
        });
    (Some(started_elapsed_ms), completion)
}

#[derive(Default)]
struct StableFinalityState {
    started_elapsed_ms: Option<u128>,
    completion: Option<StableFinalityCompletion>,
}

impl StableFinalityState {
    fn observe(
        &mut self,
        conditions_hold: bool,
        elapsed_ms: u128,
        required_window_ms: u128,
    ) -> bool {
        let (started, completion) = update_stable_finality_window(
            conditions_hold,
            elapsed_ms,
            self.started_elapsed_ms,
            required_window_ms,
        );
        self.started_elapsed_ms = started;
        self.completion = completion;
        self.completion.is_some()
    }
}

fn unix_timestamp_at_elapsed(start_unix_ms: u64, elapsed_ms: u128) -> u64 {
    start_unix_ms.saturating_add(u64::try_from(elapsed_ms).unwrap_or(u64::MAX))
}

fn parse_initial_lib_baselines(report: &Value) -> BTreeMap<String, (HeaderId, u64)> {
    report["pre_attack_consensus_snapshots"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|snapshot| snapshot["responsive"].as_bool() == Some(true))
        .filter_map(|snapshot| {
            let node = snapshot["node"].as_str()?.to_owned();
            let id = parse_header_id(snapshot["lib_id"].as_str()?).ok()?;
            let slot = snapshot["lib_slot"].as_u64()?;
            Some((node, (id, slot)))
        })
        .collect()
}

fn parse_initial_tip_baselines(report: &Value) -> BTreeMap<String, CanonicalTipState> {
    report["pre_attack_consensus_snapshots"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|snapshot| snapshot["responsive"].as_bool() == Some(true))
        .filter_map(|snapshot| {
            let node = snapshot["node"].as_str()?.to_owned();
            let id_text = snapshot["tip_id"].as_str()?.to_owned();
            Some((
                node,
                CanonicalTipState {
                    id: parse_header_id(&id_text).ok()?,
                    id_text,
                    height: snapshot["tip_height"].as_u64()?,
                    slot: snapshot["tip_slot"].as_u64()?,
                    observed_at_unix_ms: snapshot["observed_at_unix_ms"].as_u64()?,
                },
            ))
        })
        .collect()
}

fn producer_audit_events(log: &str) -> Vec<&str> {
    log.lines()
        .filter(|line| {
            line.contains("security_audit_ordinary_submitted")
                || line.contains("security_audit_sibling_submitted")
        })
        .collect()
}

fn producer_equivocation_evidence(log: &str) -> Vec<Value> {
    let parsed = log
        .lines()
        .enumerate()
        .filter(|(_, text)| text.contains("security_audit_"))
        .map(|(line, text)| (line + 1, text, parse_log_fields(text)))
        .collect::<Vec<_>>();
    parsed
        .iter()
        .filter_map(|(line, _, fields)| {
            if fields.get("event")? != "security_audit_ordinary_submitted" {
                return None;
            }
            let configured_siblings =
                parse_usize(fields, "configured_siblings").filter(|count| *count > 0)?;
            let ordinary_id = fields.get("ordinary_block_id")?;
            let parent_id = fields.get("parent_block_id")?;
            let slot = fields.get("slot")?;
            let sibling_ids = parsed
                .iter()
                .filter(|(_, _, sibling)| {
                    sibling
                        .get("event")
                        .is_some_and(|event| event == "security_audit_sibling_submitted")
                        && sibling.get("ordinary_block_id") == Some(ordinary_id)
                        && sibling.get("parent_block_id") == Some(parent_id)
                        && sibling.get("slot") == Some(slot)
                })
                .map(|(_, _, sibling)| {
                    json!({
                        "sibling_index": parse_usize(sibling, "sibling_index"),
                        "sibling_block_id": sibling.get("sibling_block_id"),
                    })
                })
                .collect::<Vec<_>>();
            Some(json!({
                "ordinary_log_line": line,
                "ordinary_block_id": ordinary_id,
                "parent_block_id": parent_id,
                "slot": slot,
                "configured_siblings": configured_siblings,
                "observed_sibling_events": sibling_ids,
            }))
        })
        .collect()
}

fn last_observed_attack_slot(opportunities: &[Value], selected_slot: u64) -> Result<u64, String> {
    opportunities
        .iter()
        .try_fold(selected_slot, |latest, opportunity| {
            let slot = opportunity["slot"]
                .as_str()
                .and_then(|value| value.parse::<u64>().ok())
                .ok_or_else(|| "producer opportunity has a missing or invalid slot".to_owned())?;
            Ok(latest.max(slot))
        })
}

fn read_runtime_node_logs(runtime_dir: &Path, runtime_node_name: &str) -> Result<String, String> {
    let log_prefix = format!("__logs-{runtime_node_name}");
    let entries = fs::read_dir(runtime_dir)
        .map_err(|error| format!("cannot read {}: {error}", runtime_dir.display()))?;
    let mut log_paths = entries
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.starts_with(&log_prefix))
        })
        .collect::<Vec<_>>();
    log_paths.sort();
    log_paths
        .into_iter()
        .map(|path| {
            fs::read_to_string(&path)
                .map_err(|error| format!("cannot read producer log {}: {error}", path.display()))
        })
        .collect::<Result<Vec<_>, _>>()
        .map(|logs| logs.join("\n"))
}

struct FinalityProducerEvidence<'a> {
    initial_events: Vec<&'a str>,
    opportunities: Vec<Value>,
    boundary_slot: u64,
    selected_is_logged: bool,
}

fn finality_producer_evidence<'a>(
    producer_log: &'a str,
    attack: &AttackEvidence,
) -> Result<FinalityProducerEvidence<'a>, String> {
    let initial_producer_events = producer_audit_events(producer_log);
    let mut opportunities_by_id = BTreeMap::new();
    for opportunity in producer_equivocation_evidence(producer_log) {
        if let Some(id) = opportunity["ordinary_block_id"].as_str() {
            opportunities_by_id.insert(id.to_owned(), opportunity);
        }
    }
    let producer_opportunities = opportunities_by_id.into_values().collect::<Vec<_>>();
    let finality_boundary_slot = last_observed_attack_slot(&producer_opportunities, attack.slot)?;
    let expected_siblings = attack
        .siblings
        .iter()
        .map(|(index, candidate)| (*index, candidate.id.encode_hex::<String>()))
        .collect::<BTreeSet<_>>();
    let selected_is_logged = producer_opportunities.iter().any(|opportunity| {
        let observed_siblings = opportunity["observed_sibling_events"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|sibling| {
                Some((
                    usize::try_from(sibling["sibling_index"].as_u64()?).ok()?,
                    parse_header_id(sibling["sibling_block_id"].as_str()?)
                        .ok()?
                        .encode_hex::<String>(),
                ))
            })
            .collect::<BTreeSet<_>>();
        let ordinary_id = opportunity["ordinary_block_id"]
            .as_str()
            .and_then(|id| parse_header_id(id).ok());
        let parent_id = opportunity["parent_block_id"]
            .as_str()
            .and_then(|id| parse_header_id(id).ok());
        ordinary_id == Some(attack.ordinary_id)
            && parent_id == Some(attack.parent_id)
            && opportunity["slot"]
                .as_str()
                .and_then(|slot| slot.parse::<u64>().ok())
                == Some(attack.slot)
            && opportunity["observed_sibling_events"]
                .as_array()
                .is_some_and(|siblings| siblings.len() == expected_siblings.len())
            && observed_siblings == expected_siblings
    });
    Ok(FinalityProducerEvidence {
        initial_events: initial_producer_events,
        opportunities: producer_opportunities,
        boundary_slot: finality_boundary_slot,
        selected_is_logged,
    })
}

#[derive(Default)]
struct BriefFinalityNodeState {
    header_caches: BTreeMap<String, HeaderCache>,
    last_verified_libs: BTreeMap<String, (HeaderId, u64)>,
    previous_tips: BTreeMap<String, CanonicalTipState>,
    previous_branches: BTreeMap<String, AttackBranch>,
    attack_branch_changes: Vec<Value>,
    tip_transition_observations: Vec<Value>,
    tip_level_reorganizations: Vec<Value>,
    tip_reorganizations_unverified: Vec<Value>,
    continuity_regressions: Vec<Value>,
    continuity_unverified: Vec<Value>,
    ancestry_gaps: Vec<Value>,
}

#[expect(
    clippy::too_many_lines,
    reason = "Records each node's finality and canonical tip evidence together"
)]
async fn observe_brief_finality_nodes(
    world: &CucumberWorld,
    honest_nodes: &[&str],
    attack: &AttackEvidence,
    state: &mut BriefFinalityNodeState,
) -> Result<Vec<FinalityNodeObservation>, String> {
    let BriefFinalityNodeState {
        header_caches,
        last_verified_libs,
        previous_tips,
        previous_branches,
        attack_branch_changes,
        tip_transition_observations,
        tip_level_reorganizations,
        tip_reorganizations_unverified,
        continuity_regressions,
        continuity_unverified,
        ancestry_gaps,
    } = state;
    let mut observations = Vec::with_capacity(honest_nodes.len());
    for &node_name in honest_nodes {
        let node = world
            .nodes_info
            .get(node_name)
            .ok_or_else(|| format!("{node_name} was not started"))?;
        let previous_lib = last_verified_libs.get(node_name).copied();
        let cache = header_caches
            .get_mut(node_name)
            .ok_or_else(|| format!("no header cache allocated for {node_name}"))?;
        let observation =
            observe_finality_node(node_name, &node.client, attack, cache, previous_lib).await;
        if let Some(consensus) = &observation.consensus {
            match observation.continuity {
                LibContinuity::Verified => {
                    last_verified_libs.insert(
                        node_name.to_owned(),
                        (consensus.lib_id, consensus.lib_slot),
                    );
                }
                LibContinuity::RegressionOrUnrelated => continuity_regressions.push(json!({
                    "node": node_name,
                    "observed_at_unix_ms": observation.snapshot["observed_at_unix_ms"],
                    "current_lib_id": consensus.lib_id_text,
                    "current_lib_slot": consensus.lib_slot,
                    "previous_verified_lib_id": previous_lib.map(|(id, _)| id.encode_hex::<String>()),
                    "ancestry": observation.snapshot["lib_continuity_ancestry"],
                })),
                LibContinuity::Unverified | LibContinuity::MissingPreviousLibBaseline => {
                    continuity_unverified.push(json!({
                        "node": node_name,
                        "observed_at_unix_ms": observation.snapshot["observed_at_unix_ms"],
                        "status": observation.continuity,
                        "current_lib_id": consensus.lib_id_text,
                        "current_lib_slot": consensus.lib_slot,
                        "previous_verified_lib_id": previous_lib.map(|(id, _)| id.encode_hex::<String>()),
                        "ancestry": observation.snapshot["lib_continuity_ancestry"],
                    }));
                }
                _ => {}
            }
            if !observation.tip_path_complete
                || !observation.tip_descends_from_lib()
                || !observation.finalized_path_complete
            {
                ancestry_gaps.push(json!({
                    "node": node_name,
                    "observed_at_unix_ms": observation.snapshot["observed_at_unix_ms"],
                    "tip_path_complete_below_attack": observation.tip_path_complete,
                    "tip_descends_from_reported_lib": observation.snapshot["tip_descends_from_lib"],
                    "finalized_path_complete_below_attack": observation.finalized_path_complete,
                    "canonical_attack_branch": observation.snapshot["canonical_attack_branch"],
                    "finalized_attack_branch": observation.snapshot["finalized_attack_branch"],
                }));
            }
            if observation.attack_branch != AttackBranch::Unverified {
                if let Some(previous) = previous_branches.get(node_name)
                    && previous != &observation.attack_branch
                {
                    attack_branch_changes.push(json!({
                        "node": node_name,
                        "observed_at_unix_ms": observation.snapshot["observed_at_unix_ms"],
                        "from": attack_branch_json(previous, attack),
                        "to": attack_branch_json(&observation.attack_branch, attack),
                        "attack_branch_changed": true,
                    }));
                }
                previous_branches.insert(node_name.to_owned(), observation.attack_branch.clone());
            }

            let new_tip = CanonicalTipState {
                id: consensus.tip_id,
                id_text: consensus.tip_id_text.clone(),
                height: consensus.tip_height,
                slot: consensus.tip_slot,
                observed_at_unix_ms: observation.observed_at_unix_ms,
            };
            let previous_tip = previous_tips.get(node_name).cloned();
            let (transition, ancestry, unverified_reason) =
                if let Some(previous_tip) = previous_tip.as_ref() {
                    if previous_tip.id == new_tip.id {
                        (TipTransition::Unchanged, None, None)
                    } else {
                        let path = walk_to_target(
                            &node.client,
                            new_tip.id,
                            previous_tip.id,
                            previous_tip.slot,
                            cache,
                        )
                        .await;
                        let relation = ancestry_presence(&path, previous_tip.id);
                        (
                            classify_tip_transition(previous_tip.id, new_tip.id, relation),
                            Some(path),
                            None,
                        )
                    }
                } else {
                    (
                        TipTransition::Unverified,
                        None,
                        Some("no previous responsive tip baseline"),
                    )
                };
            let transition_name = match transition {
                TipTransition::Unchanged => "unchanged",
                TipTransition::Descendant => "descendant_of_previous_tip",
                TipTransition::Reorganization => "reorganization",
                TipTransition::Unverified => "unverified",
            };
            let transition_record = json!({
                "node": node_name,
                "status": transition_name,
                "previous_observed_at_unix_ms": previous_tip.as_ref().map(|tip| tip.observed_at_unix_ms),
                "observed_at_unix_ms": new_tip.observed_at_unix_ms,
                "previous_tip_id": previous_tip.as_ref().map(|tip| &tip.id_text),
                "previous_tip_height": previous_tip.as_ref().map(|tip| tip.height),
                "previous_tip_slot": previous_tip.as_ref().map(|tip| tip.slot),
                "new_tip_id": new_tip.id_text,
                "new_tip_height": new_tip.height,
                "new_tip_slot": new_tip.slot,
                "new_tip_descends_from_previous_tip": match transition {
                    TipTransition::Unchanged | TipTransition::Descendant => Some(true),
                    TipTransition::Reorganization => Some(false),
                    TipTransition::Unverified => None,
                },
                "supporting_ancestry": ancestry.as_ref().map(ancestry_path_json),
                "unverified_reason": unverified_reason,
            });
            match transition {
                TipTransition::Reorganization => {
                    tip_level_reorganizations.push(transition_record.clone());
                }
                TipTransition::Unverified => {
                    tip_reorganizations_unverified.push(transition_record.clone());
                }
                TipTransition::Unchanged | TipTransition::Descendant => {}
            }
            tip_transition_observations.push(transition_record);
            previous_tips.insert(node_name.to_owned(), new_tip);
        }
        observations.push(observation);
    }

    Ok(observations)
}

#[expect(
    clippy::too_many_lines,
    reason = "Coordinates bounded finality observation"
)]
#[expect(
    clippy::cognitive_complexity,
    reason = "Keeps safety observations synchronized"
)]
async fn observe_finality_after_equivocation(
    world: &CucumberWorld,
    opportunity_report: &Value,
    attack: &AttackEvidence,
    stability: Duration,
    timeout: Duration,
    max_tip_height_skew: u64,
    prompt_stop_limit: Duration,
) -> Result<Value, String> {
    let participants = audit_participants(world)?;
    let honest_nodes = participants
        .honest_nodes
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();
    let producer = world
        .nodes_info
        .get(&participants.producer)
        .ok_or_else(|| format!("{} was not started", participants.producer))?;
    let producer_log = read_runtime_node_logs(&producer.runtime_dir, &producer.runtime_name)?;
    let evidence = finality_producer_evidence(&producer_log, attack)?;
    let initial_producer_events = evidence.initial_events;
    let producer_opportunities = evidence.opportunities;
    let finality_boundary_slot = evidence.boundary_slot;
    let selected_is_logged = evidence.selected_is_logged;
    let observation_completed_unix_ms = opportunity_report["observation_completed_unix_ms"]
        .as_u64()
        .unwrap_or_default();
    let stop_observed_unix_ms = world
        .lifecycle
        .node_stopped_at
        .get(&participants.producer)
        .map_or_else(unix_timestamp_ms, |stopped| {
            unix_timestamp_ms()
                .saturating_sub(u64::try_from(stopped.elapsed().as_millis()).unwrap_or(u64::MAX))
        });
    let stop_upper_bound_delay_ms =
        stop_observed_unix_ms.saturating_sub(observation_completed_unix_ms);
    let producer_stop_confirmation = match producer.client.consensus_info().await {
        Ok(info) => json!({
            "responsive_after_stop_step": true,
            "tip_id": info.cryptarchia_info.tip.encode_hex::<String>(),
            "tip_height": info.cryptarchia_info.height,
            "note": "producer still answered consensus_info after the stop step",
        }),
        Err(error) => json!({
            "responsive_after_stop_step": false,
            "error": error.to_string(),
            "evidence": "NodeHttpClient::consensus_info failed after the producer stop step",
        }),
    };
    let producer_stopped = world
        .lifecycle
        .node_stopped_at
        .contains_key(&participants.producer)
        && producer_stop_confirmation["responsive_after_stop_step"].as_bool() == Some(false);
    let baselines = parse_initial_lib_baselines(opportunity_report);
    let previous_tips = parse_initial_tip_baselines(opportunity_report);
    let initial_tips = opportunity_report["pre_attack_consensus_snapshots"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|snapshot| snapshot["responsive"].as_bool() == Some(true))
        .filter_map(|snapshot| {
            Some((
                snapshot["node"].as_str()?.to_owned(),
                snapshot["tip_id"].as_str()?.to_owned(),
            ))
        })
        .collect::<BTreeMap<_, _>>();

    let start_unix_ms = stop_observed_unix_ms;
    let start = world
        .lifecycle
        .node_stopped_at
        .get(&participants.producer)
        .copied()
        .unwrap_or_else(Instant::now);
    let deadline = start + timeout;
    let mut node_state = BriefFinalityNodeState {
        header_caches: honest_nodes
            .iter()
            .map(|node| ((*node).to_owned(), HeaderCache::default()))
            .collect(),
        last_verified_libs: baselines.clone(),
        previous_tips,
        ..BriefFinalityNodeState::default()
    };
    let mut snapshots = Vec::new();
    let mut finalized_history_checks = Vec::new();
    let mut safety = FinalizedSafety::default();
    let mut earliest_common_finalized_history: Option<(u64, HeaderId, u64)> = None;
    let mut stability_state = StableFinalityState::default();
    let (final_nodes, final_alignment, final_progress) = loop {
        let observations =
            observe_brief_finality_nodes(world, &honest_nodes, attack, &mut node_state).await?;

        let live_states = honest_nodes
            .iter()
            .zip(&observations)
            .filter_map(|(node, observation)| {
                observation
                    .consensus
                    .as_ref()
                    .map(|state| ((*node).to_owned(), state.clone()))
            })
            .collect::<Vec<_>>();
        let mut alignment_ids = Vec::new();
        let alignment = if live_states.len() == honest_nodes.len() {
            let min_height = live_states
                .iter()
                .map(|(_, state)| state.tip_height)
                .min()
                .unwrap_or_default();
            let max_height = live_states
                .iter()
                .map(|(_, state)| state.tip_height)
                .max()
                .unwrap_or_default();
            let height_skew = max_height.saturating_sub(min_height);
            if height_skew > max_tip_height_skew {
                json!({
                    "aligned": false,
                    "reason": "honest tip heights exceed the configured alignment tolerance",
                    "common_height": min_height,
                    "height_skew": height_skew,
                    "max_allowed_height_skew": max_tip_height_skew,
                })
            } else {
                let mut errors = Vec::new();
                for (node_name, state) in &live_states {
                    let node = &world.nodes_info[node_name];
                    let cache = node_state
                        .header_caches
                        .get_mut(node_name)
                        .expect("each honest node has a header cache");
                    match block_id_at_height(
                        &node.client,
                        state.tip_id,
                        state.tip_height,
                        min_height,
                        cache,
                    )
                    .await
                    {
                        Ok(id) => alignment_ids.push((node_name.clone(), id)),
                        Err(error) => errors.push(json!({"node": node_name, "error": error})),
                    }
                }
                let common_height_ids = alignment_ids.iter().map(|(_, id)| *id).collect::<Vec<_>>();
                json!({
                    "aligned": errors.is_empty()
                        && canonical_tips_aligned(height_skew, &common_height_ids, max_tip_height_skew),
                    "common_height": min_height,
                    "height_skew": height_skew,
                    "max_allowed_height_skew": max_tip_height_skew,
                    "block_ids_at_common_height": alignment_ids.iter().map(|(node, id)| json!({
                        "node": node,
                        "block_id": id.encode_hex::<String>(),
                    })).collect::<Vec<_>>(),
                    "errors": errors,
                })
            }
        } else {
            json!({
                "aligned": false,
                "reason": "one or more honest consensus APIs were unresponsive",
                "responsive_honest_nodes": live_states.iter().map(|(node, _)| node).collect::<Vec<_>>(),
            })
        };
        let aligned = alignment["aligned"].as_bool() == Some(true);
        let common_lib = common_finalized_lib(&live_states, honest_nodes.len());
        let common_lib_id = common_lib.map(|(id, _)| id);
        let common_lib_slot = common_lib.map(|(_, slot)| slot);
        let common_lib_beyond_attack =
            common_lib.is_some_and(|(_, slot)| slot > finality_boundary_slot);
        if common_lib_beyond_attack && earliest_common_finalized_history.is_none() {
            earliest_common_finalized_history = live_states
                .first()
                .map(|(_, state)| (unix_timestamp_ms(), state.lib_id, state.lib_slot));
        }

        let mut progress_nodes = Vec::new();
        let progress = live_states.len() == honest_nodes.len()
            && honest_nodes.iter().all(|node| {
                let changed = live_states
                    .iter()
                    .find(|(name, _)| name == node)
                    .and_then(|(_, state)| {
                        initial_tips.get(*node).map(|tip| state.tip_id_text != *tip)
                    })
                    .unwrap_or(false);
                progress_nodes.push(json!({"node": node, "tip_changed_since_pre_attack": changed}));
                changed
            });

        let pair_checks = check_finalized_history_agreement(
            world,
            &observations,
            &mut node_state.header_caches,
            finality_boundary_slot,
        )
        .await;
        safety.record_pair_reports(&pair_checks);
        finalized_history_checks.push(json!({
            "observed_at_unix_ms": unix_timestamp_ms(),
            "pairs": pair_checks.clone(),
        }));

        let continuity_ok = node_state.continuity_regressions.is_empty()
            && node_state.continuity_unverified.is_empty();
        let ancestry_ok = observations.len() == honest_nodes.len()
            && observations.iter().all(|observation| {
                observation.tip_path_complete
                    && observation.finalized_path_complete
                    && observation.tip_descends_from_lib()
            });
        let snapshot = json!({
            "elapsed_ms": start.elapsed().as_millis(),
            "observed_at_unix_ms": unix_timestamp_ms(),
            "nodes": observations.iter().map(|observation| &observation.snapshot).collect::<Vec<_>>(),
            "honest_tips_aligned": alignment.clone(),
            "progress_since_pre_attack": {
                "all_honest_nodes_progressed": progress,
                "nodes": progress_nodes,
            },
            "common_lib": {
                "same_lib_id": common_lib_id.map(|id| id.encode_hex::<String>()),
                "same_lib_slot": common_lib_slot,
                "beyond_attack_slot": common_lib_beyond_attack,
                "attack_slot": attack.slot,
                "finality_boundary_slot": finality_boundary_slot,
            },
            "finalized_history_pair_checks": pair_checks,
            "ancestry_sufficient_for_this_snapshot": ancestry_ok,
            "lib_continuity_sufficient_for_this_snapshot": continuity_ok,
        });
        snapshots.push(snapshot);

        let elapsed_ms = start.elapsed().as_millis();
        let stable_predicate = FinalityConditions {
            producer_ready: producer_stop_is_prompt(
                producer_stopped,
                stop_upper_bound_delay_ms,
                prompt_stop_limit,
            ),
            all_responsive: live_states.len() == honest_nodes.len(),
            all_progressed: progress,
            aligned,
            common_beyond_boundary: common_lib_beyond_attack,
            ancestry_verified: ancestry_ok && !safety.ancestry_unverified,
            continuity_verified: continuity_ok,
            no_finalized_conflict: !safety.conflict_observed,
        }
        .satisfied(elapsed_ms, timeout.as_millis());
        if stability_state.observe(stable_predicate, elapsed_ms, stability.as_millis()) {
            break (observations, alignment, progress);
        }
        if Instant::now() >= deadline {
            break (observations, alignment, progress);
        }
        sleep(FINALITY_OBSERVATION_INTERVAL).await;
    };

    let BriefFinalityNodeState {
        header_caches,
        attack_branch_changes,
        tip_transition_observations,
        tip_level_reorganizations,
        tip_reorganizations_unverified,
        continuity_regressions,
        continuity_unverified,
        ancestry_gaps,
        ..
    } = node_state;
    let duration_ms = start.elapsed().as_millis();
    let attack_record_count = producer_opportunities.len();
    let acceptance_path = opportunity_report["nonempty_honest_peer_validation"].as_bool()
        == Some(true)
        && opportunity_report["sibling_body_integrity"].as_bool() == Some(true);
    let single_isolated_opportunity = attack_record_count == 1 && selected_is_logged;
    let final_live_states = final_nodes
        .iter()
        .filter_map(|observation| {
            Some((
                observation.node_name.clone(),
                observation.consensus.clone()?,
            ))
        })
        .collect::<Vec<_>>();
    let final_common_lib = common_finalized_lib(&final_live_states, honest_nodes.len());
    let final_common_lib_beyond_attack =
        final_common_lib.is_some_and(|(_, slot)| slot > finality_boundary_slot);
    let final_common_lib_evidence = json!({
        "all_honest_nodes_responsive": final_live_states.len() == honest_nodes.len(),
        "same_lib_id": final_common_lib.map(|(id, _)| id.encode_hex::<String>()),
        "same_lib_slot": final_common_lib.map(|(_, slot)| slot),
        "beyond_attack_slot": final_common_lib_beyond_attack,
        "attack_slot": attack.slot,
        "finality_boundary_slot": finality_boundary_slot,
        "node_libs": final_nodes.iter().map(|observation| json!({
            "node": observation.snapshot["node"],
            "responsive": observation.snapshot["responsive"],
            "lib_id": observation.snapshot["lib_id"],
            "lib_slot": observation.snapshot["lib_slot"],
        })).collect::<Vec<_>>(),
    });
    let stable_completion_verified = stability_state.completion.is_some();
    let stable_completion_evidence = stability_state.completion.map(|completion| {
        json!({
            "started_at_unix_ms": unix_timestamp_at_elapsed(
                start_unix_ms,
                completion.started_elapsed_ms,
            ),
            "completed_at_unix_ms": unix_timestamp_at_elapsed(
                start_unix_ms,
                completion.completed_elapsed_ms,
            ),
            "started_elapsed_ms": completion.started_elapsed_ms,
            "completed_elapsed_ms": completion.completed_elapsed_ms,
            "observed_stability_duration_ms": completion
                .completed_elapsed_ms
                .saturating_sub(completion.started_elapsed_ms),
            "required_stability_duration_ms": stability.as_millis(),
        })
    });
    let prompt_stop_verified = producer_stop_is_prompt(
        producer_stopped,
        stop_upper_bound_delay_ms,
        prompt_stop_limit,
    );
    let post_recovery_log = read_runtime_node_logs(&producer.runtime_dir, &producer.runtime_name);
    let producer_quiet_after_stop = post_recovery_log
        .as_ref()
        .is_ok_and(|log| producer_audit_events(log) == initial_producer_events);
    let ancestry_verified = !ancestry_gaps.iter().any(|gap| {
        gap["tip_path_complete_below_attack"].as_bool() != Some(true)
            || gap["tip_descends_from_reported_lib"].as_bool() != Some(true)
            || gap["finalized_path_complete_below_attack"].as_bool() != Some(true)
    }) && !safety.ancestry_unverified;
    let continuity_verified = continuity_regressions.is_empty() && continuity_unverified.is_empty();
    let status = if acceptance_path
        && selected_is_logged
        && prompt_stop_verified
        && producer_quiet_after_stop
        && final_progress
        && final_alignment["aligned"].as_bool() == Some(true)
        && final_common_lib_beyond_attack
        && stable_completion_verified
        && ancestry_verified
        && continuity_verified
        && !safety.conflict_observed
        && !snapshots.is_empty()
    {
        "passed"
    } else {
        "failed"
    };
    let mut failure_classifications = Vec::new();
    if !acceptance_path {
        failure_classifications.push("workload_or_leadership_setup");
    }
    if !selected_is_logged {
        failure_classifications.push("selected_equivocation_not_confirmed_in_stopped_producer_log");
    }
    if !prompt_stop_verified {
        failure_classifications.push("producer_stop_not_confirmed_or_not_prompt");
    }
    if !producer_quiet_after_stop {
        failure_classifications.push("producer_post_stop_log_changed_or_unavailable");
    }
    if !final_progress {
        failure_classifications.push("node_or_network_liveness_failure");
    }
    if final_alignment["aligned"].as_bool() != Some(true) {
        failure_classifications
            .push("canonical_chain_divergence_persisted_through_observation_end");
    }
    if earliest_common_finalized_history.is_none() {
        failure_classifications
            .push("lib_failed_to_advance_past_attack_slot_or_common_lib_not_reached");
    }
    if !final_common_lib_beyond_attack {
        failure_classifications.push("no_current_common_lib_beyond_attack_slot");
    }
    if !stable_completion_verified {
        failure_classifications.push("stable_finality_completion_not_verified_before_timeout");
    }
    if safety.conflict_observed {
        failure_classifications.push("conflicting_finalized_histories");
    }
    if !continuity_verified {
        failure_classifications.push("lib_continuity_unverified_or_regressed");
    }
    if !ancestry_verified {
        failure_classifications.push("missing_ancestry_evidence");
    }
    if let Some(branch_change) = attack_branch_changes.first() {
        tracing::info!(
            target: TARGET,
            event = "security_audit_attack_branch_change_observed",
            observed_at_unix_ms = branch_change["observed_at_unix_ms"].as_u64().unwrap_or_default(),
            "Canonical attack-slot branch changed during the finality observation"
        );
    }
    let final_canonical_branches = final_nodes
        .iter()
        .filter_map(|observation| {
            Some(json!({
                "node": observation.snapshot["node"],
                "tip_id": observation.consensus.as_ref()?.tip_id_text,
                "tip_height": observation.consensus.as_ref()?.tip_height,
                "canonical_attack_branch": attack_branch_json(&observation.attack_branch, attack),
                "finalized_attack_branch": attack_branch_json(&observation.finalized_attack_branch, attack),
            }))
        })
        .collect::<Vec<_>>();
    let elapsed_to_common_lib_ms = earliest_common_finalized_history
        .as_ref()
        .map(|(timestamp, _, _)| timestamp.saturating_sub(start_unix_ms));
    let tip_reorganization_status = if !tip_level_reorganizations.is_empty() {
        "reorganization_observed"
    } else if !tip_reorganizations_unverified.is_empty() {
        "unverified"
    } else {
        "no_reorganization_observed_with_complete_transition_evidence"
    };
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let repository_root = manifest_dir.parent().unwrap_or(manifest_dir);
    let consensus_configuration = consensus_configuration(world);
    let attack_evidence = json!({
        "producer": participants.producer,
        "honest_nodes": honest_nodes,
        "ordinary_block_id": attack.ordinary_id_text,
        "parent_block_id": attack.parent_id_text,
        "attack_slot": attack.slot,
        "finality_boundary_slot": finality_boundary_slot,
        "sibling_ids": attack.siblings.iter().map(|(index, sibling)| json!({
            "sibling_index": index,
            "block_id": sibling.id.encode_hex::<String>(),
        })).collect::<Vec<_>>(),
        "ordered_transaction_hashes": attack.transaction_hashes,
        "body_root": attack.body_root,
        "nonempty_acceptance_and_body_integrity_passed": acceptance_path,
    });
    let producer_stop_evidence = json!({
        "selected_opportunity_observation_completed_unix_ms": observation_completed_unix_ms,
        "stop_step_observed_by_finality_step_unix_ms": stop_observed_unix_ms,
        "stop_upper_bound_delay_ms": stop_upper_bound_delay_ms,
        "prompt_stop_limit_ms": prompt_stop_limit.as_millis(),
        "prompt_stop_verified": prompt_stop_verified,
        "producer_audit_events_unchanged_after_recovery": producer_quiet_after_stop,
        "post_recovery_log_error": post_recovery_log.err(),
        "producer_unresponsive_after_stop_step": producer_stopped,
        "producer_consensus_probe": producer_stop_confirmation,
    });
    let observation_window = json!({
        "started_unix_ms": start_unix_ms,
        "duration_ms": duration_ms,
        "timeout_ms": timeout.as_millis(),
        "sample_interval_ms": FINALITY_OBSERVATION_INTERVAL.as_millis(),
        "stability_window_ms": stability.as_millis(),
        "sample_count": snapshots.len(),
        "earliest_common_lib_beyond_attack": earliest_common_finalized_history
            .as_ref()
            .map(|(observed_at_unix_ms, lib_id, lib_slot)| json!({
                "observed_at_unix_ms": observed_at_unix_ms,
                "lib_id": lib_id.encode_hex::<String>(),
                "lib_slot": lib_slot,
            })),
        "elapsed_to_common_lib_ms": elapsed_to_common_lib_ms,
    });
    let complete_observation_provenance = json!({
        "selected_opportunity_artifact": "security-audit-nonempty-evidence/nonempty-opportunity.json",
        "producer_log_source": producer.runtime_dir,
        "node_log_capture_root": world.lifecycle.scenario_base_dir,
        "finality_artifact": "security-audit-nonempty-evidence/finality-observations.json",
        "source_files": [
            {
                "path": "tests/cucumber_tests/features/security_audit_v1.feature",
                "sha256": sha256_file(&repository_root.join("tests/cucumber_tests/features/security_audit_v1.feature")).ok(),
            },
            {
                "path": "tests/src/cucumber/steps/nodes/steps/security_audit.rs",
                "sha256": sha256_file(&repository_root.join("tests/src/cucumber/steps/nodes/steps/security_audit.rs")).ok(),
            },
        ],
        "chain_observation_api": "NodeHttpClient::consensus_info and NodeHttpClient::block",
        "block_header_cache_entries": header_caches.iter().map(|(node, cache)| json!({
            "node": node,
            "cached_header_count": cache.links.len(),
            "lookup_failures": cache.failures,
        })).collect::<Vec<_>>(),
    });
    let failure = (status != "passed").then(|| {
        format!(
            "finality experiment did not satisfy: {}",
            failure_classifications.join(", ")
        )
    });
    Ok(json!({
        "status": status,
        "failure": failure,
        "failure_classifications": failure_classifications,
        "scenario": "Honest nodes converge and finalize after nonempty sibling equivocation",
        "source_commit": opportunity_report["provenance"]["commit_sha"],
        "provenance": opportunity_report["provenance"],
        "consensus_configuration": consensus_configuration,
        "attack": attack_evidence,
        "producer_stop": producer_stop_evidence,
        "producer_equivocation_opportunities_until_stop": producer_opportunities,
        "attack_isolated": single_isolated_opportunity,
        "observation_window": observation_window,
        "continued_progress": final_progress,
        "eventual_honest_tip_alignment": final_alignment,
        "common_lib_beyond_attack": final_common_lib_beyond_attack,
        "final_common_lib": final_common_lib_evidence,
        "eventually_common_lib_beyond_attack": earliest_common_finalized_history.is_some(),
        "stable_completion_verified": stable_completion_verified,
        "stable_completion": stable_completion_evidence,
        "canonical_branch_changes": attack_branch_changes,
        "observed_attack_branch_change": !attack_branch_changes.is_empty(),
        "tip_transition_observations": tip_transition_observations,
        "tip_level_reorganizations": tip_level_reorganizations,
        "tip_reorganizations_unverified": tip_reorganizations_unverified,
        "tip_reorganization_status": tip_reorganization_status,
        "observed_reorganization": !tip_level_reorganizations.is_empty(),
        "final_canonical_and_finalized_branches": final_canonical_branches,
        "finalized_history_agreement_checks": finalized_history_checks,
        "lib_continuity_verified": continuity_verified,
        "lib_continuity_regressions": continuity_regressions,
        "lib_continuity_unverified_observations": continuity_unverified,
        "ancestry_verified": ancestry_verified,
        "ancestry_gaps": ancestry_gaps,
        "snapshots": snapshots,
        "complete_observation_provenance": complete_observation_provenance,
    }))
}

#[expect(
    clippy::too_many_lines,
    reason = "Each LIB pair is classified with symmetric ancestry lookups and evidence"
)]
async fn check_finalized_history_agreement(
    world: &CucumberWorld,
    observations: &[FinalityNodeObservation],
    caches: &mut BTreeMap<String, HeaderCache>,
    attack_slot: u64,
) -> Vec<Value> {
    let mut checks = Vec::new();
    for first_index in 0..observations.len() {
        for second_index in first_index + 1..observations.len() {
            let (Some(first), Some(second)) = (
                observations[first_index].consensus.as_ref(),
                observations[second_index].consensus.as_ref(),
            ) else {
                continue;
            };
            let first_name = observations[first_index].node_name.as_str();
            let second_name = observations[second_index].node_name.as_str();
            let (first_to_second, second_to_first) = if first.lib_id == second.lib_id {
                (None, None)
            } else {
                let (Some(first_node), Some(second_node)) = (
                    world.nodes_info.get(first_name),
                    world.nodes_info.get(second_name),
                ) else {
                    checks.push(json!({
                        "first_node": first_name,
                        "first_lib_id": first.lib_id_text,
                        "first_lib_slot": first.lib_slot,
                        "second_node": second_name,
                        "second_lib_id": second.lib_id_text,
                        "second_lib_slot": second.lib_slot,
                        "status": "ancestry_unverified",
                        "attack_slot": attack_slot,
                        "error": "node client was unavailable for finalized-history lookup",
                    }));
                    continue;
                };
                let first_to_second = match caches.get_mut(first_name) {
                    Some(cache) => {
                        walk_between_finalized_blocks(
                            &first_node.client,
                            first.lib_id,
                            first.lib_slot,
                            second.lib_id,
                            second.lib_slot,
                            cache,
                        )
                        .await
                    }
                    None => AncestryPath {
                        links: Vec::new(),
                        target_reached: false,
                        complete_below_slot: false,
                        error: Some(format!("no ancestry cache allocated for {first_name}")),
                    },
                };
                let second_to_first = match caches.get_mut(second_name) {
                    Some(cache) => {
                        walk_between_finalized_blocks(
                            &second_node.client,
                            second.lib_id,
                            second.lib_slot,
                            first.lib_id,
                            first.lib_slot,
                            cache,
                        )
                        .await
                    }
                    None => AncestryPath {
                        links: Vec::new(),
                        target_reached: false,
                        complete_below_slot: false,
                        error: Some(format!("no ancestry cache allocated for {second_name}")),
                    },
                };
                (Some(first_to_second), Some(second_to_first))
            };
            let first_descends_from_second = first_to_second.as_ref().map_or_else(
                || (first.lib_id == second.lib_id).then_some(true),
                |path| ancestry_presence(path, second.lib_id),
            );
            let second_descends_from_first = second_to_first.as_ref().map_or_else(
                || (first.lib_id == second.lib_id).then_some(true),
                |path| ancestry_presence(path, first.lib_id),
            );
            let relation = classify_finalized_history_relation(
                first.lib_id,
                first.lib_slot,
                second.lib_id,
                second.lib_slot,
                first_descends_from_second,
                second_descends_from_first,
            );
            let status = match relation {
                FinalizedHistoryRelation::Shared => "shared_finalized_history",
                FinalizedHistoryRelation::Ancestral => "ancestral_finalized_histories",
                FinalizedHistoryRelation::Conflicting => "conflicting_finalized_histories",
                FinalizedHistoryRelation::Unverified => "ancestry_unverified",
            };
            checks.push(json!({
                "first_node": first_name,
                "first_lib_id": first.lib_id_text,
                "first_lib_slot": first.lib_slot,
                "second_node": second_name,
                "second_lib_id": second.lib_id_text,
                "second_lib_slot": second.lib_slot,
                "first_lib_descends_from_second": first_descends_from_second,
                "second_lib_descends_from_first": second_descends_from_first,
                "status": status,
                "attack_slot": attack_slot,
                "first_to_second_ancestry": first_to_second.as_ref().map(ancestry_path_json),
                "second_to_first_ancestry": second_to_first.as_ref().map(ancestry_path_json),
            }));
        }
    }
    checks
}

async fn walk_between_finalized_blocks(
    client: &NodeHttpClient,
    start: HeaderId,
    start_slot: u64,
    target: HeaderId,
    target_slot: u64,
    cache: &mut HeaderCache,
) -> AncestryPath {
    if let Some(path) = proven_below_target_slot_path(start_slot, target_slot) {
        return path;
    }
    walk_to_target(client, start, target, target_slot, cache).await
}

#[expect(
    clippy::too_many_lines,
    reason = "Collects all correlated producer and receiver evidence for one selected opportunity"
)]
async fn observe_sibling_propagation(
    world: &CucumberWorld,
    state: &mut AuditScenarioState,
    minimum_accepted: usize,
    receiver_timeout: Duration,
) -> Result<Value, AuditFailure> {
    let participants = audit_participants(world).map_err(|message| AuditFailure {
        message,
        details: json!({}),
    })?;
    let opportunity = state.opportunity.as_ref().ok_or_else(|| AuditFailure {
        message: "no selected opportunity".to_owned(),
        details: json!({}),
    })?;
    if minimum_accepted > opportunity.siblings.len() {
        return Err(AuditFailure {
            message: "required accepted siblings exceeds selected sibling count".to_owned(),
            details: json!({}),
        });
    }
    let provenance = &state.provenance;
    let readiness = &state.readiness;
    let (receiver_stages, no_subscriber_errors) = observe_honest_receivers(
        &mut state.tails,
        &mut state.logs,
        opportunity,
        receiver_timeout,
        minimum_accepted,
    )
    .await
    .map_err(|message| AuditFailure {
        message,
        details: json!({"provenance": provenance, "blend_readiness": readiness}),
    })?;
    let storage_events = query_accepted_blocks(world, opportunity, &receiver_stages).await;

    let mut sibling_outcomes = Vec::new();
    let mut accepted_count = 0usize;
    for (_, identity) in &opportunity.siblings {
        let block_id = identity
            .get("sibling_block_id")
            .cloned()
            .unwrap_or_default();
        let mut receivers = Vec::new();
        let mut delivered = false;
        let mut reconstructed = false;
        let mut accepted = false;
        let mut rejected = false;
        let mut reconstruction_failed = false;
        let mut not_processed = false;
        let mut sibling_accepted_via_blend = false;

        for node in participants.honest_nodes.iter().map(String::as_str) {
            let evidence = receiver_stages
                .get(&(node.to_owned(), block_id.clone()))
                .cloned()
                .unwrap_or_default();
            let node_delivered = evidence.has_stage("blend_received");
            let node_reconstructed = evidence.has_stage("reconstructed");
            let node_accepted = evidence.has_stage("accepted");
            delivered |= node_delivered;
            reconstructed |= node_reconstructed;
            accepted |= node_accepted;
            rejected |= evidence.has_stage("rejected") || evidence.has_stage("apply_failed");
            reconstruction_failed |= evidence.has_stage("reconstruction_failed");
            not_processed |= evidence.has_stage("not_processed");
            let ordered_blend_to_acceptance = evidence.has_ordered_blend_to_acceptance();
            sibling_accepted_via_blend |= ordered_blend_to_acceptance;
            receivers.push(json!({
                "node": node,
                "node_identity": world.cluster.node_peer_ids.get(node).map(ToString::to_string),
                "blend_received": node_delivered,
                "chain_proposal_received": evidence.has_stage("chain_proposal_received"),
                "reconstructed": node_reconstructed,
                "reconstruction_failed": evidence.has_stage("reconstruction_failed"),
                "accepted": node_accepted,
                "rejected": evidence.has_stage("rejected"),
                "apply_failed": evidence.has_stage("apply_failed"),
                "not_processed": evidence.has_stage("not_processed"),
                "ordered_blend_to_acceptance": ordered_blend_to_acceptance,
                "storage_observed": storage_events.iter().any(|event| {
                    event["node"].as_str() == Some(node)
                        && event["block_id"].as_str() == Some(block_id.as_str())
                }),
                "canonical_status": "unknown",
                "observed_stages": evidence.stage_names(),
                "evidence_events": evidence.ordered_events().iter().map(|event| json!({
                    "stage": event.stage,
                    "timestamp": format_log_timestamp(event.timestamp),
                    "reason": event.reason,
                    "source_file": event.path,
                    "source_offset": event.offset,
                })).collect::<Vec<_>>(),
                "mismatched_identity_events": evidence.mismatched_events.iter().map(|event| json!({
                    "stage": event.stage,
                    "parent_block_id": event.parent_block_id,
                    "slot": event.slot,
                    "timestamp": format_log_timestamp(event.timestamp),
                    "reason": event.reason,
                    "source_file": event.path,
                    "source_offset": event.offset,
                })).collect::<Vec<_>>(),
            }));
        }
        accepted_count += usize::from(sibling_accepted_via_blend);

        let outcome = if sibling_accepted_via_blend {
            "accepted_after_blend_reception"
        } else if rejected {
            "reconstructed_or_received_but_rejected"
        } else if reconstruction_failed {
            "received_but_reconstruction_failed"
        } else if not_processed {
            "received_but_not_processed"
        } else if delivered && !reconstructed {
            "blend_received_reconstruction_not_observed"
        } else if no_subscriber_errors && !delivered {
            "delivery_not_observed_with_no_subscribers_error_in_producer_log"
        } else if identity_bool(identity, "relay_enqueued") {
            "local_enqueue_succeeded_peer_delivery_not_observed"
        } else {
            "local_relay_enqueue_failed"
        };

        sibling_outcomes.push(json!({
            "sibling_index": identity.get("sibling_index"),
            "ordinary_block_id": identity.get("ordinary_block_id"),
            "sibling_block_id": block_id,
            "parent_block_id": identity.get("parent_block_id"),
            "slot": identity.get("slot"),
            "relay_enqueued": identity_bool(identity, "relay_enqueued"),
            "outcome": outcome,
            "blend_received_by_any_honest_node": delivered,
            "reconstructed_by_any_honest_node": reconstructed,
            "accepted_by_any_honest_node": accepted,
            "receivers": receivers,
        }));
    }

    let accepted_via_blend = accepted_count >= minimum_accepted;
    let timing = &opportunity.timing;
    let summary = json!({
        "configured_siblings": parse_usize(timing, "configured_siblings"),
        "constructed_siblings": parse_usize(timing, "constructed_siblings"),
        "failed_siblings": parse_usize(timing, "failed_siblings"),
        "relay_enqueued_siblings": parse_usize(timing, "relay_enqueued_siblings"),
        "ordinary_self_applied": identity_bool(timing, "ordinary_self_applied"),
        "ordinary_relay_enqueued": identity_bool(timing, "ordinary_relay_enqueued"),
        "slot_to_ordinary_submission_ms": timing.get("slot_to_ordinary_submission_ms"),
        "ordinary_proposal_ms": timing.get("ordinary_proposal_ms"),
        "sibling_construction_ms": timing.get("sibling_construction_ms"),
        "sibling_blend_enqueue_ms": timing.get("sibling_blend_enqueue_ms"),
        "additional_processing_ms": timing.get("additional_processing_ms"),
        "total_leadership_processing_ms": timing.get("total_leadership_processing_ms"),
    });

    Ok(json!({
        "status": if accepted_via_blend { "passed" } else { "failed" },
        "failure": if accepted_via_blend { Value::Null } else {
            json!("no single honest node showed Blend reception, reconstruction, and successful consensus application for the same sibling")
        },
        "honest_peer_validation": accepted_via_blend,
        "cluster_nodes": participants.honest_nodes.len() + 1,
        "blend_provider_count": world.cluster.blend_core_nodes,
        "producer": participants.producer,
        "producer_runtime_name": world.nodes_info.get(&participants.producer).map(|node| node.runtime_name.as_str()),
        "blend_readiness": readiness,
        "provenance": provenance,
        "ordinary_block_id": opportunity.ordinary.get("ordinary_block_id"),
        "parent_block_id": opportunity.ordinary.get("parent_block_id"),
        "slot": opportunity.ordinary.get("slot"),
        "ordinary_submitted_log_line": opportunity.ordinary_line,
        "first_sibling_log_line": opportunity.siblings.first().map(|(line, _)| line),
        "timing_log_line": opportunity.timing_line,
        "ordinary_submission_preceded_sibling_construction": opportunity.ordinary_line
            < opportunity.siblings.first().map_or(usize::MAX, |(line, _)| *line),
        "timing": summary,
        "producer_no_peers_subscribed_to_topic_errors_after_readiness": no_subscriber_errors,
        "siblings": sibling_outcomes,
        "storage_observations": storage_events,
        "receiver_observability": {
            "blend_topic_subscription_api": "not exposed; matching decapsulated sibling event is direct per-payload reception evidence",
            "canonical_selection": "not asserted; ordinary acceptance may be non-canonical"
        },
    }))
}

#[expect(
    clippy::too_many_lines,
    reason = "Runs the bounded nonempty transaction workload and correlates the selected block bodies"
)]
async fn select_nonempty_sibling_opportunity(
    world: &mut CucumberWorld,
    state: &mut AuditScenarioState,
) -> Result<(), AuditFailure> {
    let participants = audit_participants(world)
        .cloned()
        .map_err(|message| AuditFailure {
            message,
            details: json!({}),
        })?;
    let configured_siblings = audit_sibling_count(world).map_err(|message| AuditFailure {
        message,
        details: json!({}),
    })?;
    let transfer = audit_workload(world)
        .cloned()
        .map_err(|message| AuditFailure {
            message,
            details: json!({}),
        })?;
    let workload = world
        .security_audit_nonempty_workload
        .clone()
        .ok_or_else(|| AuditFailure {
            message: "nonempty selection workload was not declared".to_owned(),
            details: json!({}),
        })?;
    let provenance = &mut state.provenance;
    let readiness = &state.readiness;
    let tails = &mut state.tails;
    let pre_attack_consensus = capture_pre_attack_consensus(world).await;

    world
        .ensure_wallet_scanner_started()
        .await
        .map_err(|error| AuditFailure {
            message: format!("could not start the wallet scanner for the workload: {error}"),
            details: json!({
                "provenance": provenance,
                "blend_readiness": readiness,
            }),
        })?;

    // `wait_for_blend_readiness` snapshots all current log offsets before this
    // function prepares or submits any workload transactions.
    let mut opportunity_state = NonemptyOpportunityState::default();
    let mut submitted_hashes = BTreeSet::new();
    let mut submitted_transactions = Vec::new();
    let mut transaction_observations = Vec::new();
    let global_deadline = Instant::now() + Duration::from_secs(workload.window_seconds);
    let mut selected: Option<(ProducerOpportunity, ApiBlock)> = None;
    let mut last_mempool_status = "not_observed".to_owned();
    let mut mempool_observation_timed_out = false;

    'transactions: for transaction_index in 1..=workload.budget {
        if Instant::now() >= global_deadline {
            break;
        }
        let alias = format!("SECURITY_AUDIT_NONEMPTY_{transaction_index}");
        tokio::time::timeout_at(tokio::time::Instant::from_std(global_deadline), prepare_transfer_transaction(
            world,
            "security audit nonempty sibling workload",
            alias.clone(),
            transfer.amount,
            transfer.sender.clone(),
            transfer.receiver.clone(),
        ))
        .await
        .map_err(|_| AuditFailure {
            message: format!("workload transaction preparation exceeded the observation deadline: {alias}"),
            details: json!({"provenance": provenance, "blend_readiness": readiness, "submitted_transactions": submitted_transactions, "failure_classification": "workload_preparation_timeout_unverified"}),
        })?
        .map_err(|error| AuditFailure {
            message: format!("could not prepare independent workload transaction {alias}: {error}"),
            details: json!({
                "provenance": provenance,
                "blend_readiness": readiness,
                "transactions_prepared": submitted_transactions,
                "failure_classification": "workload_preparation_failed",
            }),
        })?;
        let signed = world
            .resolve_prepared_transaction(&alias)
            .map_err(|error| AuditFailure {
                message: format!("prepared workload transaction {alias} disappeared: {error}"),
                details: json!({ "provenance": provenance, "failure_classification": "workload_preparation_failed" }),
            })?;
        let transaction_hash = tx_hash_to_hex(&signed.hash());
        tokio::time::timeout_at(tokio::time::Instant::from_std(global_deadline), submit_prepared_transaction_to_nodes(
            world,
            "security audit nonempty sibling workload",
            alias.clone(),
            std::iter::once(&participants.producer)
                .chain(&participants.honest_nodes)
                .cloned()
                .collect(),
        ))
        .await
        .map_err(|_| AuditFailure {
            message: format!("workload transaction submission exceeded the observation deadline: {alias}"),
            details: json!({"provenance": provenance, "blend_readiness": readiness, "submitted_transactions": submitted_transactions, "in_flight_transaction_hash": transaction_hash, "failure_classification": "workload_submission_timeout_unverified"}),
        })?
        .map_err(|error| AuditFailure {
            message: format!(
                "could not submit workload transaction {alias} to the declared nodes: {error}"
            ),
            details: json!({
                "provenance": provenance,
                "blend_readiness": readiness,
                "pre_attack_consensus_snapshots": pre_attack_consensus,
                "submitted_transactions": submitted_transactions,
                "failure_classification": "workload_submission_failed",
            }),
        })?;
        submitted_hashes.insert(transaction_hash.clone());
        submitted_transactions.push(json!({
            "alias": alias,
            "transaction_hash": transaction_hash,
        }));

        let submitted_hash = signed.hash();
        let transaction_deadline =
            (Instant::now() + Duration::from_secs(workload.pending_seconds)).min(global_deadline);
        let mut saw_transaction_pending = false;
        let mut mempool_samples = Vec::new();

        loop {
            opportunity_state
                .logs
                .extend(
                    tails
                        .read_new_lines(&participants.producer)
                        .map_err(|message| AuditFailure {
                            message,
                            details: json!({
                                "provenance": provenance,
                                "blend_readiness": readiness,
                                "submitted_transactions": submitted_transactions,
                                "skipped_opportunities": opportunity_state.skipped,
                            }),
                        })?,
                );

            let selection = tokio::time::timeout_at(
                tokio::time::Instant::from_std(global_deadline),
                opportunity_state.select(
                    world,
                    &participants.producer,
                    configured_siblings,
                    &submitted_hashes,
                ),
            )
            .await;
            let selection = match selection {
                Ok(Ok(selection)) => selection,
                Ok(Err(mut error)) => {
                    error.details["provenance"] = provenance.clone();
                    error.details["blend_readiness"] = readiness.clone();
                    error.details["submitted_transactions"] = json!(submitted_transactions);
                    return Err(error);
                }
                Err(_) => {
                    opportunity_state.last_body_error =
                        Some("opportunity lookup exceeded observation deadline".to_owned());
                    transaction_observations.push(json!({"alias": alias, "transaction_hash": transaction_hash, "observed_pending_before_deadline": saw_transaction_pending, "mempool_observations": mempool_samples, "last_mempool_status": last_mempool_status}));
                    break 'transactions;
                }
            };
            if let Some(candidate) = selection {
                transaction_observations.push(json!({
                    "alias": alias,
                    "transaction_hash": transaction_hash,
                    "observed_pending_before_candidate": saw_transaction_pending,
                    "mempool_observations": mempool_samples,
                    "last_mempool_status": last_mempool_status,
                }));
                selected = Some(candidate);
                break 'transactions;
            }

            if nonempty_workload_action(
                Instant::now(),
                global_deadline,
                Some(transaction_deadline),
                false,
                submitted_transactions.len(),
                workload.budget,
            ) != NonemptyWorkloadAction::Observe
            {
                break;
            }

            // The final transaction's bounded mempool observation is complete;
            // continue polling producer events without growing its sample list.
            if submitted_transactions.len() >= workload.budget
                && Instant::now() >= transaction_deadline
            {
                sleep(Duration::from_millis(250)).await;
                continue;
            }

            let views = tokio::time::timeout_at(
                tokio::time::Instant::from_std(global_deadline),
                join_all(
                    std::iter::once(&participants.producer)
                        .chain(&participants.honest_nodes)
                        .map(async |name| {
                            (
                                name.as_str(),
                                world.nodes_info[name].client.test_mempool_view().await,
                            )
                        }),
                ),
            )
            .await;
            let Ok(views) = views else {
                mempool_observation_timed_out = true;
                "mempool observation exceeded the observation deadline"
                    .clone_into(&mut last_mempool_status);
                break;
            };
            let all_views_succeeded = views.iter().all(|(_, view)| view.is_ok());
            let pending_nodes = views
                .iter()
                .filter_map(|(name, view)| {
                    view.as_ref()
                        .ok()
                        .filter(|hashes| hashes.contains(&submitted_hash))
                        .map(|_| *name)
                })
                .collect::<Vec<_>>();
            saw_transaction_pending |= !pending_nodes.is_empty();
            last_mempool_status = views
                .iter()
                .map(|(name, view)| match view {
                    Ok(hashes) => format!("{name}:pending={}", hashes.contains(&submitted_hash)),
                    Err(error) => format!("{name}:error={error}"),
                })
                .collect::<Vec<_>>()
                .join(", ");
            mempool_samples.push(json!({
                "observed_at_utc": SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .map_or(0.0, |duration| duration.as_secs_f64()),
                "pending_nodes": pending_nodes,
                "all_node_views_succeeded": all_views_succeeded,
                "observations": last_mempool_status,
            }));
            if nonempty_workload_action(
                Instant::now(),
                global_deadline,
                Some(transaction_deadline),
                saw_transaction_pending && all_views_succeeded && pending_nodes.is_empty(),
                submitted_transactions.len(),
                workload.budget,
            ) != NonemptyWorkloadAction::Observe
            {
                break;
            }
            sleep(Duration::from_millis(250)).await;
        }
        transaction_observations.push(json!({
            "alias": alias,
            "transaction_hash": transaction_hash,
            "observed_pending_before_disappearing": saw_transaction_pending,
            "mempool_observations": mempool_samples,
            "last_mempool_status": last_mempool_status,
        }));
    }

    let Some((opportunity, ordinary_block)) = selected else {
        let ordinary_event_count = opportunity_state
            .logs
            .iter()
            .filter(|line| {
                parse_log_fields(&line.text)
                    .get("event")
                    .map(String::as_str)
                    == Some("security_audit_ordinary_submitted")
            })
            .count();
        let complete_opportunity_count = find_producer_opportunities(&opportunity_state.logs).len();
        let leadership_observed = opportunity_state.logs.iter().any(|line| {
            matches!(
                parse_log_fields(&line.text)
                    .get("event")
                    .map(String::as_str),
                Some("security_audit_ordinary_submitted" | "security_audit_leadership_timing")
            )
        });
        let failure_classification = if mempool_observation_timed_out
            || !opportunity_state.incomplete_body_ids.is_empty()
            || ordinary_event_count > complete_opportunity_count
            || (leadership_observed && opportunity_state.examined_ids.is_empty())
        {
            "opportunity_evidence_unverified"
        } else if leadership_observed {
            "leadership_without_qualifying_nonempty_body"
        } else {
            "no_producer_leadership_observed"
        };
        return Err(AuditFailure {
            message: format!(
                "{} did not produce a verified qualifying sibling opportunity containing one of the submitted transactions after {} bounded submissions ({failure_classification})",
                participants.producer,
                submitted_transactions.len()
            ),
            details: json!({
                "provenance": provenance,
                "blend_readiness": readiness,
                "submitted_transactions": submitted_transactions,
                "transaction_observations": transaction_observations,
                "producer_opportunities_examined": opportunity_state.examined_ids.len(),
                "producer_ordinary_event_count": ordinary_event_count,
                "producer_complete_opportunity_count": complete_opportunity_count,
                "empty_or_nonmatching_opportunities_skipped": opportunity_state.skipped.len(),
                "skipped_opportunities": opportunity_state.skipped,
                "unavailable_ordinary_body_ids": opportunity_state.incomplete_body_ids,
                "producer_logs": opportunity_state.logs.iter().map(|line| json!({"line": line.text, "path": line.path, "offset": line.offset})).collect::<Vec<_>>(),
                "last_mempool_status": last_mempool_status,
                "last_body_error": opportunity_state.last_body_error,
                "failure_classification": failure_classification,
            }),
        });
    };
    record_observed_audit_opportunity(provenance, 1);

    let ordinary_body = summarize_api_block(&ordinary_block);
    state
        .logs
        .insert(participants.producer.clone(), opportunity_state.logs);
    state.opportunity = Some(opportunity);
    state.nonempty = Some(NonemptySelection {
        ordinary_body,
        pre_attack_consensus,
        submitted_hashes,
        submitted_transactions,
        transaction_observations,
        skipped_opportunities: opportunity_state.skipped,
        examined_count: opportunity_state.examined_ids.len(),
        workload,
        selected_at_unix_ms: unix_timestamp_ms(),
        selected_at: Instant::now(),
    });
    Ok(())
}

#[expect(
    clippy::too_many_lines,
    reason = "Records per-sibling body and receiver evidence together"
)]
async fn observe_nonempty_sibling_propagation(
    world: &CucumberWorld,
    state: &mut AuditScenarioState,
    receiver_timeout: Duration,
    minimum_accepted: usize,
) -> Result<Value, AuditFailure> {
    let participants = audit_participants(world).map_err(|message| AuditFailure {
        message,
        details: json!({}),
    })?;
    let selection = state.nonempty.as_ref().ok_or_else(|| AuditFailure {
        message: "nonempty opportunity not selected".to_owned(),
        details: json!({}),
    })?;
    let opportunity = state.opportunity.as_ref().ok_or_else(|| AuditFailure {
        message: "opportunity not selected".to_owned(),
        details: json!({}),
    })?;
    let NonemptySelection {
        ordinary_body,
        pre_attack_consensus,
        submitted_hashes,
        submitted_transactions,
        transaction_observations,
        skipped_opportunities,
        examined_count,
        workload,
        selected_at_unix_ms,
        ..
    } = selection;
    let provenance = &state.provenance;
    let readiness = &state.readiness;
    let tails = &mut state.tails;
    let producer_log_map = &mut state.logs;
    let (receiver_stages, no_subscriber_errors) = observe_honest_receivers(
        tails,
        producer_log_map,
        opportunity,
        receiver_timeout,
        minimum_accepted,
    )
    .await
    .map_err(|message| AuditFailure {
        message,
        details: json!({
            "provenance": provenance,
            "blend_readiness": readiness,
            "ordinary_block": ordinary_body,
            "siblings": opportunity.siblings.iter().map(|(_, event)| event).collect::<Vec<_>>(),
        }),
    })?;
    let storage_events = query_accepted_blocks(world, opportunity, &receiver_stages).await;
    let ordinary_hashes = &ordinary_body.transaction_hashes;
    let mut sibling_outcomes = Vec::new();
    let mut accepted_body_path = false;
    let mut accepted_bodies_all_match = true;
    let mut accepted_sibling_count = 0usize;
    let mut voucher_commitments = BTreeMap::<String, String>::new();
    let mut voucher_evidence_valid = true;
    for (_, identity) in &opportunity.siblings {
        let sibling_id = identity
            .get("sibling_block_id")
            .cloned()
            .unwrap_or_default();
        let mut receiver_reports = Vec::new();
        let mut accepted = false;
        let mut matching_ordered_path = false;
        for node in participants.honest_nodes.iter().map(String::as_str) {
            let evidence = receiver_stages
                .get(&(node.to_owned(), sibling_id.clone()))
                .cloned()
                .unwrap_or_default();
            let storage = storage_events.iter().find(|event| {
                event["node"].as_str() == Some(node)
                    && event["block_id"].as_str() == Some(sibling_id.as_str())
            });
            let has_stored_block =
                storage.and_then(|event| event["stored"].as_bool()) == Some(true);
            if has_stored_block {
                let voucher = storage.and_then(|event| event["voucher_commitment"].as_str());
                voucher_evidence_valid &= record_voucher_commitment(
                    &mut voucher_commitments,
                    &ordinary_body.voucher_commitment,
                    &sibling_id,
                    voucher,
                );
            }
            let root_matches = storage
                .and_then(|event| event["body_root"].as_str())
                .is_some_and(|root| root == ordinary_body.body_root);
            let stored_hashes = storage
                .and_then(|event| event["transaction_hashes"].as_array())
                .map(|hashes| {
                    hashes
                        .iter()
                        .filter_map(Value::as_str)
                        .map(str::to_owned)
                        .collect::<Vec<_>>()
                });
            let hashes_match = stored_hashes
                .as_ref()
                .is_some_and(|hashes| hashes == ordinary_hashes);
            let body_matches = storage
                .and_then(|event| event["body_root"].as_str())
                .zip(stored_hashes.as_ref())
                .is_some_and(|(root, hashes)| same_transaction_body(ordinary_body, root, hashes));
            let ordered_path = evidence.has_ordered_blend_to_acceptance();
            accepted |= evidence.has_stage("accepted");
            if evidence.has_stage("accepted") && (!has_stored_block || !body_matches) {
                accepted_bodies_all_match = false;
            }
            accepted_body_path |= ordered_path && has_stored_block && body_matches;
            matching_ordered_path |= ordered_path && has_stored_block && body_matches;
            let reasons = evidence
                .ordered_events()
                .into_iter()
                .filter_map(|event| {
                    event.reason.as_ref().map(|reason| json!({
                    "stage": event.stage,
                    "reason": reason,
                    "transaction_references_unavailable": reason.contains("UnresolvedReference"),
                }))
                })
                .collect::<Vec<_>>();
            receiver_reports.push(json!({
                "node": node,
                "node_identity": world.cluster.node_peer_ids.get(node).map(ToString::to_string),
                "blend_received": evidence.has_stage("blend_received"),
                "chain_proposal_received": evidence.has_stage("chain_proposal_received"),
                "reconstructed": evidence.has_stage("reconstructed"),
                "reconstruction_failed": evidence.has_stage("reconstruction_failed"),
                "accepted": evidence.has_stage("accepted"),
                "rejected": evidence.has_stage("rejected"),
                "apply_failed": evidence.has_stage("apply_failed"),
                "not_processed": evidence.has_stage("not_processed"),
                "ordered_blend_to_acceptance": ordered_path,
                "explicit_reasons": reasons,
                "transaction_references_unavailable": reasons.iter().any(|reason| {
                    reason["transaction_references_unavailable"] == true
                }),
                "full_block_available": has_stored_block,
                "body_root_matches_ordinary": root_matches,
                "ordered_transaction_hashes_match_ordinary": hashes_match,
                "body_matches_ordinary": body_matches,
                "transaction_count": storage.and_then(|event| event["transaction_count"].as_u64()),
                "body_root": storage.and_then(|event| event["body_root"].as_str()),
                "voucher_commitment": storage.and_then(|event| event["voucher_commitment"].as_str()),
                "transaction_hashes": storage.and_then(|event| event["transaction_hashes"].as_array()),
                "storage_error": storage.and_then(|event| event["error"].as_str()),
                "observed_stages": evidence.stage_names(),
                "evidence_events": evidence.ordered_events().iter().map(|event| json!({
                    "stage": event.stage,
                    "timestamp": format_log_timestamp(event.timestamp),
                    "reason": event.reason,
                    "source_file": event.path,
                    "source_offset": event.offset,
                })).collect::<Vec<_>>(),
                "mismatched_identity_events": evidence.mismatched_events.iter().map(|event| json!({
                    "stage": event.stage,
                    "parent_block_id": event.parent_block_id,
                    "slot": event.slot,
                    "timestamp": format_log_timestamp(event.timestamp),
                    "reason": event.reason,
                    "source_file": event.path,
                    "source_offset": event.offset,
                })).collect::<Vec<_>>(),
            }));
        }
        accepted_sibling_count += usize::from(matching_ordered_path);
        sibling_outcomes.push(json!({
            "sibling_index": identity.get("sibling_index"),
            "ordinary_block_id": identity.get("ordinary_block_id"),
            "sibling_block_id": sibling_id,
            "parent_block_id": identity.get("parent_block_id"),
            "slot": identity.get("slot"),
            "relay_enqueued": identity_bool(identity, "relay_enqueued"),
            "accepted_by_any_honest_node": accepted,
            "receivers": receiver_reports,
        }));
    }

    let submitted_hashes_in_body = ordinary_body
        .transaction_hashes
        .iter()
        .filter(|hash| submitted_hashes.contains(*hash))
        .cloned()
        .collect::<Vec<_>>();
    let integrity = sibling_body_integrity(ordinary_body, opportunity);
    let passed = accepted_body_path
        && accepted_sibling_count >= minimum_accepted
        && accepted_bodies_all_match
        && voucher_evidence_valid
        && !submitted_hashes_in_body.is_empty()
        && integrity;
    let timing = &opportunity.timing;
    Ok(json!({
        "status": if passed { "passed" } else { "failed" },
        "failure": if passed { Value::Null } else {
            json!("no ordered honest-peer acceptance was paired with an available sibling block whose body root and ordered transaction hashes exactly match the nonempty ordinary block")
        },
        "failure_classification": if passed { Value::Null } else { json!("sibling_validation_or_body_integrity") },
        "nonempty_honest_peer_validation": passed,
        "honest_peer_validation": accepted_body_path,
        "cluster_nodes": participants.honest_nodes.len() + 1,
        "blend_provider_count": world.cluster.blend_core_nodes,
        "pre_attack_consensus_snapshots": pre_attack_consensus,
        "producer": participants.producer,
        "producer_runtime_name": world.nodes_info.get(&participants.producer).map(|node| node.runtime_name.as_str()),
        "blend_readiness": readiness,
        "provenance": provenance,
        "transaction_budget": workload.budget,
        "transactions_submitted": submitted_transactions.len(),
        "submitted_transactions": submitted_transactions,
        "transaction_observations": transaction_observations,
        "producer_opportunities_examined": examined_count,
        "observation_completed_unix_ms": selected_at_unix_ms,
        "empty_or_nonmatching_opportunities_skipped": skipped_opportunities.len(),
        "skipped_opportunities": skipped_opportunities,
        "selected_ordinary_block": {
            "block_id": ordinary_body.block_id,
            "parent_block_id": ordinary_body.parent_block_id,
            "slot": ordinary_body.slot,
            "body_root": ordinary_body.body_root,
            "voucher_commitment": ordinary_body.voucher_commitment,
            "transaction_count": ordinary_body.transaction_hashes.len(),
            "transaction_hashes": ordinary_body.transaction_hashes,
            "submitted_transaction_hashes_in_body": submitted_hashes_in_body,
        },
        "ordinary_submission_preceded_sibling_construction": opportunity.ordinary_line
            < opportunity.siblings.first().map_or(usize::MAX, |(line, _)| *line),
        "timing": {
            "configured_siblings": parse_usize(timing, "configured_siblings"),
            "constructed_siblings": parse_usize(timing, "constructed_siblings"),
            "failed_siblings": parse_usize(timing, "failed_siblings"),
            "relay_enqueued_siblings": parse_usize(timing, "relay_enqueued_siblings"),
            "ordinary_self_applied": identity_bool(timing, "ordinary_self_applied"),
            "ordinary_relay_enqueued": identity_bool(timing, "ordinary_relay_enqueued"),
            "slot_to_ordinary_submission_ms": timing.get("slot_to_ordinary_submission_ms"),
            "ordinary_proposal_ms": timing.get("ordinary_proposal_ms"),
            "sibling_construction_ms": timing.get("sibling_construction_ms"),
            "sibling_blend_enqueue_ms": timing.get("sibling_blend_enqueue_ms"),
            "additional_processing_ms": timing.get("additional_processing_ms"),
            "total_leadership_processing_ms": timing.get("total_leadership_processing_ms"),
        },
        "producer_no_peers_subscribed_to_topic_errors_after_readiness": no_subscriber_errors,
        "sibling_body_integrity": integrity,
        "retrieved_voucher_commitments_valid": (!voucher_commitments.is_empty()).then_some(voucher_evidence_valid),
        "accepted_bodies_all_match_ordinary": storage_events.iter().any(|event| event["stored"] == true).then_some(accepted_bodies_all_match),
        "siblings": sibling_outcomes,
        "storage_observations": storage_events,
        "receiver_observability": {
            "blend_topic_subscription_api": "not exposed; matching decapsulated sibling event is direct per-payload reception evidence",
            "canonical_selection": "not asserted; accepted sibling may be non-canonical",
            "producer_enqueue_is_not_peer_delivery": true,
        },
    }))
}

async fn wait_for_blend_readiness(
    world: &CucumberWorld,
    timeout: Duration,
) -> Result<(Value, AuditLogTail), String> {
    let participants = audit_participants(world)?;
    let deadline = Instant::now() + timeout;
    let mut last_status;
    loop {
        let mut all_ready = true;
        let mut samples = Vec::new();
        for node_name in std::iter::once(&participants.producer).chain(&participants.honest_nodes) {
            let node = world
                .nodes_info
                .get(node_name)
                .ok_or_else(|| format!("node {node_name} was not started"))?;
            let (network, blend) =
                tokio::join!(node.client.network_info(), node.client.blend_info());
            let network = network.map_err(|error| error.to_string());
            let blend = blend.map_err(|error| error.to_string());
            let network_peers = network.as_ref().ok().map_or(0, |value| value.n_peers);
            let blend_peers = blend
                .as_ref()
                .ok()
                .and_then(Option::as_ref)
                .and_then(|value| value.core_info.as_ref())
                .map_or(0, |core| {
                    core.current_epoch_peers
                        .iter()
                        .filter(|(_, healthy)| *healthy)
                        .count()
                });
            let ready = network_peers > 0 && blend_peers > 0;
            all_ready &= ready;
            samples.push(json!({
                "node": node_name,
                "node_identity": world.cluster.node_peer_ids.get(node_name).map(ToString::to_string),
                "network_peer_count": network_peers,
                "healthy_blend_core_peer_count": blend_peers,
                "network_info_error": network.err(),
                "blend_info_error": blend.err(),
                "ready": ready,
            }));
        }
        last_status = serde_json::to_string(&samples).unwrap_or_else(|_| "unavailable".to_owned());
        if all_ready {
            let tails = AuditLogTail::after_readiness(world)?;
            let mut baseline = Vec::new();
            for name in std::iter::once(&participants.producer).chain(&participants.honest_nodes) {
                baseline.push(json!({
                    "node": name,
                    "new_log_events_observed_after_readiness_only": true,
                    "log_file_count": tails.paths.get(name).map_or(0, Vec::len),
                }));
            }
            return Ok((
                json!({
                    "established": true,
                    "observed_at_utc": SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .map_err(|error| error.to_string())?
                        .as_secs_f64(),
                    "nodes": samples,
                    "log_offsets_captured_after_readiness": baseline,
                    "topic_subscription_api": "not exposed; receiver decapsulation events will establish per-sibling delivery",
                }),
                tails,
            ));
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "Blend network readiness did not establish within {}s; last peer samples: {last_status}",
                timeout.as_secs()
            ));
        }
        sleep(Duration::from_secs(1)).await;
    }
}

async fn wait_for_producer_opportunity(
    tails: &mut AuditLogTail,
    logs: &mut Vec<ObservedLogLine>,
    minimum_siblings: usize,
    timeout: Duration,
) -> Result<ProducerOpportunity, String> {
    let producer = tails.participants.producer.clone();
    let deadline = Instant::now() + timeout;
    loop {
        logs.extend(tails.read_new_lines(&producer)?);
        if let Some(opportunity) = find_producer_opportunities(logs)
            .into_iter()
            .find(|opportunity| opportunity.siblings.len() >= minimum_siblings)
        {
            return Ok(opportunity);
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "{producer} did not complete a genuine leadership opportunity within {}s after Blend readiness",
                timeout.as_secs(),
            ));
        }
        sleep(Duration::from_millis(250)).await;
    }
}

fn find_producer_opportunities(lines: &[ObservedLogLine]) -> Vec<ProducerOpportunity> {
    let parsed = lines
        .iter()
        .enumerate()
        .filter(|(_, raw)| raw.text.contains("security_audit_"))
        .map(|(line, raw)| (line, raw, parse_log_fields(&raw.text)))
        .collect::<Vec<_>>();
    let mut opportunities = Vec::new();
    for (ordinary_line, ordinary_record, ordinary) in &parsed {
        if ordinary
            .get("event")
            .is_none_or(|event| event != "security_audit_ordinary_submitted")
        {
            continue;
        }
        let Some(configured_siblings) =
            parse_usize(ordinary, "configured_siblings").filter(|count| *count > 0)
        else {
            continue;
        };
        let Some(ordinary_id) = ordinary.get("ordinary_block_id") else {
            continue;
        };
        let Some(parent) = ordinary.get("parent_block_id") else {
            continue;
        };
        let Some(slot) = ordinary.get("slot") else {
            continue;
        };
        let mut siblings = parsed
            .iter()
            .filter(|(_, record, fields)| {
                log_line_precedes(ordinary_record, record)
                    && fields
                        .get("event")
                        .is_some_and(|event| event == "security_audit_sibling_submitted")
                    && fields.get("ordinary_block_id") == Some(ordinary_id)
                    && fields.get("parent_block_id") == Some(parent)
                    && fields.get("slot") == Some(slot)
            })
            .map(|(line, record, fields)| (*line, record, fields.clone()))
            .collect::<Vec<_>>();
        siblings.sort_by_key(|(_, _, fields)| {
            parse_usize(fields, "sibling_index").unwrap_or(usize::MAX)
        });
        if siblings.len() != configured_siblings {
            continue;
        }
        let indexes = siblings
            .iter()
            .filter_map(|(_, _, fields)| parse_usize(fields, "sibling_index"))
            .collect::<BTreeSet<_>>();
        let ids = siblings
            .iter()
            .filter_map(|(_, _, fields)| fields.get("sibling_block_id"))
            .collect::<BTreeSet<_>>();
        if indexes.len() != configured_siblings
            || !indexes.iter().copied().eq(1..=configured_siblings)
            || ids.len() != configured_siblings
            || ids.contains(ordinary_id)
        {
            continue;
        }
        let Some((timing_line, _, timing)) = parsed.iter().find(|(_, record, fields)| {
            siblings
                .iter()
                .all(|(_, sibling_record, _)| log_line_precedes(sibling_record, record))
                && fields
                    .get("event")
                    .is_some_and(|event| event == "security_audit_leadership_timing")
                && fields.get("parent_block_id") == Some(parent)
                && fields.get("slot") == Some(slot)
        }) else {
            continue;
        };
        if parse_usize(timing, "configured_siblings") != Some(configured_siblings)
            || parse_usize(timing, "constructed_siblings") != Some(configured_siblings)
            || !identity_bool(timing, "ordinary_self_applied")
            || !identity_bool(timing, "ordinary_relay_enqueued")
            || !identity_bool(ordinary, "ordinary_self_applied")
            || !identity_bool(ordinary, "ordinary_relay_enqueued")
        {
            continue;
        }
        opportunities.push(ProducerOpportunity {
            ordinary: ordinary.clone(),
            ordinary_line: *ordinary_line,
            siblings: siblings
                .into_iter()
                .map(|(line, _, fields)| (line, fields))
                .collect(),
            timing: timing.clone(),
            timing_line: *timing_line,
        });
    }
    opportunities
}

fn log_line_precedes(first: &ObservedLogLine, second: &ObservedLogLine) -> bool {
    let (Some(first_time), Some(second_time)) = (
        parse_log_timestamp(&first.text),
        parse_log_timestamp(&second.text),
    ) else {
        return false;
    };
    match first_time.cmp(&second_time) {
        std::cmp::Ordering::Less => true,
        std::cmp::Ordering::Equal => first.path == second.path && first.offset < second.offset,
        std::cmp::Ordering::Greater => false,
    }
}

fn summarize_api_block(block: &ApiBlock) -> BlockBodyEvidence {
    BlockBodyEvidence {
        block_id: block.header.id.to_string(),
        parent_block_id: block.header.parent_block.to_string(),
        slot: u64::from(block.header.slot),
        body_root: block.header.body_root.to_string(),
        voucher_commitment: format!("{:?}", block.header.proof_of_leadership.voucher_cm()),
        transaction_hashes: block
            .transactions
            .iter()
            .map(|transaction| tx_hash_to_hex(&transaction.hash()))
            .collect(),
    }
}

fn parse_header_id(value: &str) -> Result<HeaderId, String> {
    let bytes = value
        .strip_prefix("0x")
        .map_or_else(|| hex::decode(value), hex::decode)
        .map_err(|error| format!("invalid block ID {value}: {error}"))?;
    HeaderId::try_from(bytes.as_slice())
        .map_err(|error| format!("invalid block ID {value}: {error}"))
}

fn has_nonempty_submitted_body(
    transaction_hashes: &[String],
    submitted_hashes: &BTreeSet<String>,
) -> bool {
    !transaction_hashes.is_empty()
        && transaction_hashes
            .iter()
            .any(|transaction_hash| submitted_hashes.contains(transaction_hash))
}

fn same_transaction_body(
    ordinary: &BlockBodyEvidence,
    body_root: &str,
    transaction_hashes: &[String],
) -> bool {
    ordinary.body_root == body_root && ordinary.transaction_hashes == transaction_hashes
}

fn record_voucher_commitment(
    seen: &mut BTreeMap<String, String>,
    ordinary: &str,
    sibling_id: &str,
    voucher: Option<&str>,
) -> bool {
    let Some(voucher) = voucher else {
        return false;
    };
    let valid = voucher != ordinary
        && seen
            .get(sibling_id)
            .is_none_or(|previous| previous == voucher)
        && seen
            .iter()
            .all(|(id, previous)| id == sibling_id || previous != voucher);
    seen.insert(sibling_id.to_owned(), voucher.to_owned());
    valid
}

fn sibling_body_integrity(ordinary: &BlockBodyEvidence, opportunity: &ProducerOpportunity) -> bool {
    let sibling_ids = opportunity
        .siblings
        .iter()
        .filter_map(|(_, sibling)| sibling.get("sibling_block_id"))
        .collect::<BTreeSet<_>>();
    let configured = parse_usize(&opportunity.ordinary, "configured_siblings");
    configured.is_some_and(|count| {
        count > 0 && sibling_ids.len() == count && opportunity.siblings.len() == count
    }) && !sibling_ids.contains(&ordinary.block_id)
        && {
            let expected_slot = ordinary.slot.to_string();
            opportunity.siblings.iter().all(|(_, sibling)| {
                sibling.get("parent_block_id") == Some(&ordinary.parent_block_id)
                    && sibling.get("slot") == Some(&expected_slot)
            })
        }
}

async fn observe_honest_receivers(
    tails: &mut AuditLogTail,
    logs: &mut BTreeMap<String, Vec<ObservedLogLine>>,
    opportunity: &ProducerOpportunity,
    timeout: Duration,
    minimum_accepted: usize,
) -> Result<(HashMap<(String, String), ReceiverEvidence>, bool), String> {
    let participants = tails.participants.clone();
    let ids = opportunity
        .siblings
        .iter()
        .filter_map(|(_, event)| event.get("sibling_block_id").cloned())
        .collect::<BTreeSet<_>>();
    let deadline = Instant::now() + timeout;
    loop {
        for node in &participants.honest_nodes {
            logs.entry(node.clone())
                .or_default()
                .extend(tails.read_new_lines(node)?);
        }
        logs.entry(participants.producer.clone())
            .or_default()
            .extend(tails.read_new_lines(&participants.producer)?);
        let honest_nodes = participants
            .honest_nodes
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>();
        let stages = receiver_stages(logs, &ids, &opportunity.ordinary, &honest_nodes);
        let has_valid_acceptance = ids
            .iter()
            .filter(|id| {
                stages.iter().any(|((_, candidate), evidence)| {
                    candidate == *id && evidence.has_ordered_blend_to_acceptance()
                })
            })
            .count()
            >= minimum_accepted;
        let all_terminal = ids.iter().all(|id| {
            stages.iter().any(|((_, observed_id), observed)| {
                observed_id == id && observed.has_terminal_stage()
            })
        });
        if (has_valid_acceptance && all_terminal) || Instant::now() >= deadline {
            let no_subscriber_errors = logs.get(&participants.producer).is_some_and(|producer| {
                producer
                    .iter()
                    .any(|line| line.text.contains("NoPeersSubscribedToTopic"))
            });
            return Ok((stages, no_subscriber_errors));
        }
        sleep(Duration::from_millis(250)).await;
    }
}

fn receiver_stages(
    logs: &BTreeMap<String, Vec<ObservedLogLine>>,
    ids: &BTreeSet<String>,
    ordinary: &HashMap<String, String>,
    honest_nodes: &[&str],
) -> HashMap<(String, String), ReceiverEvidence> {
    let mut stages: HashMap<(String, String), ReceiverEvidence> = HashMap::new();
    for &node in honest_nodes {
        for line in logs.get(node).into_iter().flatten() {
            let fields = parse_log_fields(&line.text);
            let Some(stage) = receiver_stage(fields.get("event").map(String::as_str)) else {
                continue;
            };
            let Some(block_id) = fields
                .get("sibling_block_id")
                .or_else(|| fields.get("block_id"))
            else {
                continue;
            };
            if !ids.contains(block_id) {
                continue;
            }
            let event = ReceiverEvent {
                stage,
                parent_block_id: fields.get("parent_block_id").cloned(),
                slot: fields.get("slot").cloned(),
                timestamp: parse_log_timestamp(&line.text),
                reason: fields.get("reason").cloned(),
                path: line.path.clone(),
                offset: line.offset,
            };
            let evidence = stages
                .entry((node.to_owned(), block_id.clone()))
                .or_default();
            if event.parent_block_id.as_ref() == ordinary.get("parent_block_id")
                && event.slot.as_ref() == ordinary.get("slot")
            {
                evidence.matching_events.push(event);
            } else {
                evidence.mismatched_events.push(event);
            }
        }
    }
    stages
}

fn receiver_stage(event: Option<&str>) -> Option<&'static str> {
    match event? {
        "security_audit_blend_payload_decapsulated" => Some("blend_received"),
        "security_audit_chain_proposal_received" => Some("chain_proposal_received"),
        "security_audit_proposal_reconstructed" => Some("reconstructed"),
        "security_audit_proposal_reconstruction_failed" => Some("reconstruction_failed"),
        "security_audit_proposal_accepted" => Some("accepted"),
        "security_audit_proposal_rejected" => Some("rejected"),
        "security_audit_proposal_apply_failed" => Some("apply_failed"),
        "security_audit_proposal_not_processed" => Some("not_processed"),
        _ => None,
    }
}

async fn query_accepted_blocks(
    world: &CucumberWorld,
    opportunity: &ProducerOpportunity,
    stages: &HashMap<(String, String), ReceiverEvidence>,
) -> Vec<Value> {
    let mut observations = Vec::new();
    for ((node_name, block_id), receiver_stages) in stages {
        if !receiver_stages.has_stage("accepted") {
            continue;
        }
        let Some(node) = world.nodes_info.get(node_name) else {
            continue;
        };
        let Ok(header_id) = parse_header_id(block_id) else {
            continue;
        };
        let block_result = node.client.block(&header_id).await;
        let block_error = block_result.as_ref().err().map(ToString::to_string);
        let block = block_result.as_ref().ok().and_then(Option::as_ref);
        let identity = opportunity
            .siblings
            .iter()
            .find(|(_, event)| event.get("sibling_block_id") == Some(block_id))
            .map(|(_, event)| event);
        observations.push(json!({
            "timestamp": SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_or(0.0, |duration| duration.as_secs_f64()),
            "event": "security_audit_block_storage_observed",
            "node": node_name,
            "block_id": block_id,
            "parent_block_id": identity.and_then(|event| event.get("parent_block_id")),
            "slot": identity.and_then(|event| event.get("slot")),
            "stored": block.is_some(),
            "body_root": block.map(|block| block.header.body_root.to_string()),
            "voucher_commitment": block.map(|block| format!("{:?}", block.header.proof_of_leadership.voucher_cm())),
            "transaction_count": block.map(|block| block.transactions.len()),
            "transaction_hashes": block.map(|block| {
                block.transactions.iter()
                    .map(|transaction| tx_hash_to_hex(&transaction.hash()))
                    .collect::<Vec<_>>()
            }),
            "error": block_error,
            "evidence_source": "NodeHttpClient::block",
        }));
    }
    observations
}

fn consensus_configuration(world: &CucumberWorld) -> Value {
    let result = (|| -> Result<Value, String> {
        let producer = &audit_participants(world)?.producer;
        let node = world
            .nodes_info
            .get(producer)
            .ok_or_else(|| format!("node information for {producer} is unavailable"))?;
        let path = node.runtime_dir.join("deployment.yaml");
        let contents =
            fs::read(&path).map_err(|error| format!("cannot read {}: {error}", path.display()))?;
        let deployment: DeploymentSettings = serde_yaml::from_slice(&contents)
            .map_err(|error| format!("cannot parse {}: {error}", path.display()))?;
        let parameters = deployment.genesis_era_parameters();
        let cryptarchia = &parameters.cryptarchia;
        let epoch = cryptarchia.epoch_config;
        Ok(json!({
            "status": "read",
            "source_node": producer,
            "deployment_config_path": path,
            "deployment_config_sha256": sha256_file(&path).ok(),
            "security_parameter_k": cryptarchia.security_param.get(),
            "slot_duration_secs": parameters.time.slot_duration().as_secs(),
            "uncle_reference_window_in_block": cryptarchia.uncle_reference_window_in_block.get(),
            "slot_activation_coeff_numerator": cryptarchia.slot_activation_coeff.numerator,
            "slot_activation_coeff_denominator": cryptarchia.slot_activation_coeff.denominator.get(),
            "epoch_stake_distribution_stabilization": epoch.epoch_stake_distribution_stabilization.get(),
            "epoch_period_nonce_buffer": epoch.epoch_period_nonce_buffer.get(),
            "epoch_period_nonce_stabilization": epoch.epoch_period_nonce_stabilization.get(),
            "source": "generated node deployment YAML",
            "interpretation": "functional experiment; not a production security threshold",
        }))
    })();
    result.unwrap_or_else(
        |error| json!({ "status": "unverified", "security_parameter_k": null, "error": error }),
    )
}

#[expect(
    clippy::too_many_lines,
    reason = "Preserves per-node configuration and binary provenance in one report"
)]
async fn verify_runtime_configuration_with_siblings(
    world: &CucumberWorld,
    scenario: &str,
    expected_producer_siblings: usize,
) -> Result<Value, String> {
    let participants = audit_participants(world)?;
    let profile =
        NodeBinaryProfile::from_string(&std::env::var("NODE_BINARY_PROFILE").unwrap_or_default());
    let binary = ensure_node_binary_built(&profile)
        .await
        .map_err(|error| format!("cannot resolve framework-selected node binary: {error}"))?
        .canonicalize()
        .map_err(|error| format!("cannot resolve selected node binary: {error}"))?;
    let binary_sha256 = sha256_file(&binary)?;
    let configured_binary = std::env::var_os("LOGOS_BLOCKCHAIN_NODE_BIN")
        .filter(|value| !value.is_empty())
        .map(PathBuf::from);
    let binary_selection_mode = binary_selection_mode(configured_binary.as_deref(), &binary);
    let commit = command_output("git", &["rev-parse", "HEAD"])?;
    let mut nodes = BTreeMap::new();

    for (node_name, expected) in
        std::iter::once((participants.producer.as_str(), expected_producer_siblings)).chain(
            participants
                .honest_nodes
                .iter()
                .map(|node| (node.as_str(), 0)),
        )
    {
        let node = world
            .nodes_info
            .get(node_name)
            .ok_or_else(|| format!("node {node_name} was not started"))?;
        let config_path = node.runtime_dir.join(USER_CONFIG_FILE);
        let config_bytes = fs::read(&config_path).map_err(|error| {
            format!(
                "cannot read generated YAML {}: {error}",
                config_path.display()
            )
        })?;
        let config: serde_yaml::Value = serde_yaml::from_slice(&config_bytes).map_err(|error| {
            format!(
                "cannot parse generated YAML {}: {error}",
                config_path.display()
            )
        })?;
        let setting =
            &config["cryptarchia"]["leader"]["security_audit"]["sibling_blocks_per_leadership"];
        let actual = setting
            .as_u64()
            .and_then(|value| usize::try_from(value).ok())
            .unwrap_or(0);
        if actual != expected {
            return Err(format!(
                "generated YAML for {node_name} has sibling count {actual}, expected {expected}: {}",
                config_path.display()
            ));
        }
        let identity = world
            .cluster
            .node_peer_ids
            .get(node_name)
            .map(ToString::to_string)
            .ok_or_else(|| format!("node identity was unavailable for {node_name}"))?;
        nodes.insert(
            node_name,
            json!({
                "runtime_name": node.runtime_name,
                "node_identity": identity,
                "sibling_count": actual,
                "sibling_count_field_present_in_yaml": !setting.is_null(),
                "runtime_audit_configuration": {
                    "sibling_blocks_per_leadership": actual,
                    "source": "generated user YAML; absent setting uses the documented default of zero",
                    "independently_verified_in_running_process": false,
                },
                "user_config_path": config_path,
                "user_config_sha256": sha256_file(&config_path)?,
                "executable_path": binary,
                "executable_sha256": binary_sha256,
            }),
        );
    }

    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let repository_root = manifest_dir.parent().unwrap_or(manifest_dir);
    let source_files = [
        "tests/cucumber_tests/features/security_audit_v1.feature",
        "tests/src/cucumber/steps/nodes/steps/security_audit.rs",
        "tests/src/cucumber/steps/nodes/steps/security_audit/sustained.rs",
        "tests/tf_integration/logos/src/local/provisioning.rs",
    ]
    .into_iter()
    .map(|relative_path| {
        json!({
            "path": relative_path,
            "sha256": sha256_file(&repository_root.join(relative_path)).ok(),
        })
    })
    .collect::<Vec<_>>();

    Ok(json!({
        "commit_sha": commit,
        "binary_path": binary,
        "binary_sha256": binary_sha256,
        "binary_resolution": "test framework binary provider",
        "binary_selection_mode": binary_selection_mode,
        "configured_binary_override": configured_binary,
        "node_binary_profile": profile.to_string(),
        "feature_set": Value::Null,
        "feature_set_note": "Legacy field is null: executable features are not independently verified.",
        "requested_cucumber_features": Value::Null,
        "requested_cucumber_features_note": "Cargo invocation flags are not available to the running test.",
        "compiled_test_features_known_enabled": ["cucumber", "security-audit"],
        "expected_node_build_features": expected_node_build_features(&profile),
        "expected_node_build_features_note": "Expected for a framework auto-build; selected executable features have not been independently verified.",
        "binary_features_independently_verified": false,
        "observed_audit_functionality": {
            "status": "not_observed_yet",
            "complete_producer_opportunities": 0,
        },
        "scenario": scenario,
        "source_files": source_files,
        "node_configs": nodes,
    }))
}

fn binary_selection_mode(configured: Option<&Path>, selected: &Path) -> &'static str {
    match configured {
        Some(path) if path.canonicalize().ok().as_deref() == Some(selected) => {
            "environment_override_matches_selected_path"
        }
        Some(_) => "environment_override_configured_selection_unverified",
        None => "framework_provider_unspecified",
    }
}

fn expected_node_build_features(profile: &NodeBinaryProfile) -> Vec<&'static str> {
    let mut features = vec!["testing", "security-audit"];
    if *profile == NodeBinaryProfile::TokioConsole {
        features.push("tokio-console");
    }
    features
}

fn record_observed_audit_opportunity(provenance: &mut Value, count: usize) {
    provenance["observed_audit_functionality"] = json!({
        "status": "producer_opportunity_events_observed",
        "complete_producer_opportunities": count,
        "evidence_source": "correlated producer ordinary, sibling and timing log events",
        "executable_feature_set_independently_verified": false,
    });
}

impl AuditLogTail {
    fn after_readiness(world: &CucumberWorld) -> Result<Self, String> {
        let participants = audit_participants(world)?;
        let root = &world.lifecycle.scenario_base_dir;
        let mut runtime_names = BTreeMap::new();
        let mut paths = BTreeMap::new();
        let mut offsets = HashMap::new();
        for node_name in std::iter::once(&participants.producer).chain(&participants.honest_nodes) {
            let node = world
                .nodes_info
                .get(node_name)
                .ok_or_else(|| format!("node {node_name} was not started"))?;
            let runtime_prefix = format!("{}_", node.runtime_name);
            let log_prefix = format!("__logs-{}", node.runtime_name);
            runtime_names.insert(node_name.to_owned(), node.runtime_name.clone());
            let mut files = Vec::new();
            for entry in fs::read_dir(root).map_err(|error| {
                format!(
                    "cannot inspect scenario log root {}: {error}",
                    root.display()
                )
            })? {
                let entry = entry.map_err(|error| error.to_string())?;
                if !entry
                    .file_type()
                    .map_err(|error| error.to_string())?
                    .is_dir()
                    || !entry
                        .file_name()
                        .to_string_lossy()
                        .starts_with(&runtime_prefix)
                {
                    continue;
                }
                for log in fs::read_dir(entry.path()).map_err(|error| error.to_string())? {
                    let log = log.map_err(|error| error.to_string())?;
                    if !log.file_type().map_err(|error| error.to_string())?.is_dir()
                        && log.file_name().to_string_lossy().starts_with(&log_prefix)
                    {
                        files.push(log.path());
                    }
                }
            }
            files.sort();
            if files.is_empty() {
                return Err(format!(
                    "no live log file found for {node_name} ({})",
                    node.runtime_name
                ));
            }
            for path in &files {
                let len = fs::metadata(path).map_err(|error| error.to_string())?.len();
                offsets.insert(path.clone(), len);
            }
            paths.insert(node_name.to_owned(), files);
        }
        Ok(Self {
            root: root.clone(),
            participants: participants.clone(),
            runtime_names,
            paths,
            offsets,
        })
    }

    fn read_new_lines(&mut self, node_name: &str) -> Result<Vec<ObservedLogLine>, String> {
        let mut new_lines = Vec::new();
        let runtime_name = self
            .runtime_names
            .get(node_name)
            .ok_or_else(|| format!("no runtime name recorded for {node_name}"))?;
        let runtime_prefix = format!("{runtime_name}_");
        let log_prefix = format!("__logs-{runtime_name}");
        let mut discovered = Vec::new();
        for entry in fs::read_dir(&self.root).map_err(|error| {
            format!(
                "cannot refresh scenario log root {}: {error}",
                self.root.display()
            )
        })? {
            let entry = entry.map_err(|error| error.to_string())?;
            if !entry
                .file_type()
                .map_err(|error| error.to_string())?
                .is_dir()
                || !entry
                    .file_name()
                    .to_string_lossy()
                    .starts_with(&runtime_prefix)
            {
                continue;
            }
            for log in fs::read_dir(entry.path()).map_err(|error| error.to_string())? {
                let log = log.map_err(|error| error.to_string())?;
                if !log.file_type().map_err(|error| error.to_string())?.is_dir()
                    && log.file_name().to_string_lossy().starts_with(&log_prefix)
                {
                    discovered.push(log.path());
                }
            }
        }
        discovered.sort();
        let paths = self.paths.entry(node_name.to_owned()).or_default();
        for path in discovered {
            if !paths.contains(&path) {
                self.offsets.insert(path.clone(), 0);
                paths.push(path);
            }
        }
        paths.sort();
        let files = self
            .paths
            .get(node_name)
            .ok_or_else(|| format!("no log paths recorded for {node_name}"))?;
        for path in files {
            let mut file = match File::open(path) {
                Ok(file) => file,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                Err(error) => {
                    return Err(format!(
                        "cannot read live node log {}: {error}",
                        path.display()
                    ));
                }
            };
            let mut offset = self.offsets.get(path).copied().unwrap_or(0);
            let len = file.metadata().map_err(|error| error.to_string())?.len();
            if len < offset {
                offset = 0;
            }
            file.seek(SeekFrom::Start(offset))
                .map_err(|error| error.to_string())?;
            let mut reader = BufReader::new(file);
            let mut bytes_read = 0u64;
            loop {
                let mut line = String::new();
                let bytes_in_line = reader
                    .read_line(&mut line)
                    .map_err(|error| error.to_string())?;
                if bytes_in_line == 0 {
                    break;
                }
                let line_offset = offset.saturating_add(bytes_read);
                bytes_read = bytes_read.saturating_add(
                    u64::try_from(bytes_in_line).map_err(|error| error.to_string())?,
                );
                new_lines.push(ObservedLogLine {
                    text: line.trim_end_matches(['\r', '\n']).to_owned(),
                    path: path.clone(),
                    offset: line_offset,
                });
            }
            self.offsets
                .insert(path.clone(), offset.saturating_add(bytes_read));
        }
        Ok(new_lines)
    }
}

fn copy_node_logs(world: &CucumberWorld, output: &Path) -> Result<Vec<String>, String> {
    let participants = audit_participants(world)?;
    let mut copied = Vec::new();
    for node_name in std::iter::once(&participants.producer).chain(&participants.honest_nodes) {
        let node = world
            .nodes_info
            .get(node_name)
            .ok_or_else(|| format!("node {node_name} was not started"))?;
        let logs = read_manual_node_logs(&world.lifecycle.scenario_base_dir, &node.runtime_name);
        let path = output.join(format!("{node_name}.log"));
        fs::write(&path, logs).map_err(|error| error.to_string())?;
        copied.push(path.display().to_string());
    }
    Ok(copied)
}

fn parse_log_fields(line: &str) -> HashMap<String, String> {
    let mut fields = line
        .split_whitespace()
        .filter_map(|field| field.split_once('='))
        .map(|(key, value)| (key.to_owned(), value.trim_matches('"').to_owned()))
        .collect::<HashMap<_, _>>();
    if let Some(reason) = parse_quoted_log_field(line, "reason") {
        fields.insert("reason".to_owned(), reason);
    }
    fields
}

fn parse_quoted_log_field(line: &str, key: &str) -> Option<String> {
    let marker = format!("{key}=");
    let (start, _) = line.match_indices(&marker).find(|(index, _)| {
        *index == 0
            || line
                .get(..*index)
                .is_some_and(|prefix| prefix.chars().next_back().is_some_and(char::is_whitespace))
    })?;
    let mut value = line.get(start + marker.len()..)?.chars();
    if value.next()? != '"' {
        return None;
    }
    let mut output = String::new();
    let mut escaped = false;
    for character in value {
        if escaped {
            output.push(character);
            escaped = false;
        } else if character == '\\' {
            escaped = true;
        } else if character == '"' {
            return Some(output);
        } else {
            output.push(character);
        }
    }
    None
}

fn parse_log_timestamp(line: &str) -> Option<[u32; 7]> {
    let stamp = line
        .split_whitespace()
        .next()?
        .trim_start_matches('[')
        .trim_end_matches(']');
    let stamp = stamp.strip_suffix('Z')?;
    let (date, time) = stamp.split_once('T')?;
    let mut date_parts = date.split('-').map(str::parse::<u32>);
    let year = date_parts.next()?.ok()?;
    let month = date_parts.next()?.ok()?;
    let day = date_parts.next()?.ok()?;
    if date_parts.next().is_some() {
        return None;
    }
    let (clock, fraction) = time.split_once('.').unwrap_or((time, ""));
    let mut time_parts = clock.split(':').map(str::parse::<u32>);
    let hour = time_parts.next()?.ok()?;
    let minute = time_parts.next()?.ok()?;
    let second = time_parts.next()?.ok()?;
    if time_parts.next().is_some() || fraction.len() > 9 {
        return None;
    }
    let nanos = if fraction.is_empty() {
        0
    } else {
        let fractional = fraction.parse::<u32>().ok()?;
        fractional.checked_mul(10u32.checked_pow((9 - fraction.len()) as u32)?)?
    };
    Some([year, month, day, hour, minute, second, nanos])
}

fn format_log_timestamp(timestamp: Option<[u32; 7]>) -> Option<String> {
    let [year, month, day, hour, minute, second, nanos] = timestamp?;
    Some(format!(
        "{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}.{nanos:09}Z"
    ))
}

fn parse_usize(fields: &HashMap<String, String>, key: &str) -> Option<usize> {
    fields.get(key)?.parse().ok()
}

fn identity_bool(fields: &HashMap<String, String>, key: &str) -> bool {
    fields.get(key).is_some_and(|value| value == "true")
}

fn sha256_file(path: &Path) -> Result<String, String> {
    let output = Command::new("sha256sum")
        .arg(path)
        .output()
        .map_err(|error| format!("cannot hash {} with sha256sum: {error}", path.display()))?;
    if !output.status.success() {
        return Err(format!("sha256sum failed for {}", path.display()));
    }
    String::from_utf8_lossy(&output.stdout)
        .split_whitespace()
        .next()
        .map(str::to_owned)
        .ok_or_else(|| format!("sha256sum returned no digest for {}", path.display()))
}

fn command_output(command: &str, args: &[&str]) -> Result<String, String> {
    let output = Command::new(command)
        .args(args)
        .output()
        .map_err(|error| format!("cannot run {command}: {error}"))?;
    if !output.status.success() {
        return Err(format!("{command} {} failed", args.join(" ")));
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exhausted_nonempty_workload_keeps_observing_until_deadline() {
        let start = Instant::now();
        let deadline = start + Duration::from_secs(30);
        let pending_end = start + Duration::from_secs(2);
        assert_eq!(
            nonempty_workload_action(start, deadline, None, false, 0, 1),
            NonemptyWorkloadAction::SubmitNext
        );
        for seconds in [2, 10, 29] {
            assert_eq!(
                nonempty_workload_action(
                    start + Duration::from_secs(seconds),
                    deadline,
                    Some(pending_end),
                    true,
                    1,
                    1,
                ),
                NonemptyWorkloadAction::Observe
            );
        }
        assert_eq!(
            nonempty_workload_action(deadline, deadline, Some(pending_end), true, 1, 1),
            NonemptyWorkloadAction::Finished
        );

        // The only qualifying block arrives after the last transaction's
        // pending window. It remains selectable before the overall deadline.
        let submitted = BTreeSet::from(["submitted".to_owned()]);
        let mut found = false;
        for (seconds, body) in [(2, vec![]), (8, vec!["submitted".to_owned()])] {
            if nonempty_workload_action(
                start + Duration::from_secs(seconds),
                deadline,
                Some(pending_end),
                true,
                1,
                1,
            ) == NonemptyWorkloadAction::Observe
                && has_nonempty_submitted_body(&body, &submitted)
            {
                found = true;
                break;
            }
        }
        assert!(found);
    }

    #[test]
    fn nonempty_workload_schedules_next_only_within_budget_and_deadline() {
        let start = Instant::now();
        let deadline = start + Duration::from_secs(10);
        let pending_end = start + Duration::from_secs(4);
        assert_eq!(
            nonempty_workload_action(start, deadline, Some(pending_end), false, 1, 2),
            NonemptyWorkloadAction::Observe
        );
        assert_eq!(
            nonempty_workload_action(start, deadline, Some(pending_end), true, 1, 2),
            NonemptyWorkloadAction::SubmitNext
        );
        assert_eq!(
            nonempty_workload_action(pending_end, deadline, Some(pending_end), false, 1, 2),
            NonemptyWorkloadAction::SubmitNext
        );
        assert_eq!(
            nonempty_workload_action(deadline, deadline, Some(pending_end), true, 1, 2),
            NonemptyWorkloadAction::Finished
        );
    }

    #[test]
    fn finality_requires_complete_observations_and_respects_deadline() {
        let complete = FinalityConditions {
            producer_ready: true,
            all_responsive: true,
            all_progressed: true,
            aligned: true,
            common_beyond_boundary: true,
            ancestry_verified: true,
            continuity_verified: true,
            no_finalized_conflict: true,
        };
        assert!(complete.satisfied(30_000, 30_000));
        assert!(!complete.satisfied(30_001, 30_000));
        for incomplete in [
            FinalityConditions {
                all_responsive: false,
                ..complete
            },
            FinalityConditions {
                aligned: false,
                ..complete
            },
            FinalityConditions {
                ancestry_verified: false,
                ..complete
            },
            FinalityConditions {
                continuity_verified: false,
                ..complete
            },
            FinalityConditions {
                no_finalized_conflict: false,
                ..complete
            },
        ] {
            assert!(!incomplete.satisfied(10_000, 30_000));
        }
        let (started, _) =
            update_stable_finality_window(complete.satisfied(0, 30_000), 0, None, 10_000);
        let (started, completion) = update_stable_finality_window(false, 5_000, started, 10_000);
        assert!(started.is_none() && completion.is_none());
        let (started, _) = update_stable_finality_window(true, 10_000, started, 10_000);
        let (_, completion) = update_stable_finality_window(
            complete.satisfied(20_000, 30_000),
            20_000,
            started,
            10_000,
        );
        assert!(completion.is_some());
    }

    #[test]
    fn finalized_safety_evidence_remains_sticky_after_agreement() {
        let mut safety = FinalizedSafety::default();
        safety.record_relation(FinalizedHistoryRelation::Unverified);
        safety.record_relation(FinalizedHistoryRelation::Conflicting);
        safety.record_relation(FinalizedHistoryRelation::Shared);
        safety.record_relation(FinalizedHistoryRelation::Ancestral);
        assert!(safety.conflict_observed);
        assert!(safety.ancestry_unverified);

        let mut incomplete = FinalizedSafety::default();
        incomplete.record_pair_reports(&[json!({"unexpected": "missing status"})]);
        assert!(incomplete.ancestry_unverified);
        assert!(!incomplete.conflict_observed);
    }

    #[test]
    fn stability_state_restarts_after_an_unverified_sample() {
        let mut state = StableFinalityState::default();
        assert!(!state.observe(true, 0, 10_000));
        assert!(!state.observe(false, 9_000, 10_000));
        assert!(!state.observe(true, 10_000, 10_000));
        assert!(!state.observe(true, 19_999, 10_000));
        assert!(state.observe(true, 20_000, 10_000));
        assert_eq!(state.completion.unwrap().started_elapsed_ms, 10_000);
    }

    #[test]
    fn continuity_classification_preserves_evidence_statuses() {
        assert!(LibContinuity::Verified.is_verified());
        assert!(LibContinuity::Unchanged.is_verified());
        assert!(LibContinuity::Baseline.is_verified());
        for status in [
            LibContinuity::Unverified,
            LibContinuity::MissingPreviousLibBaseline,
            LibContinuity::RegressionOrUnrelated,
        ] {
            assert!(!status.is_verified());
        }
        assert_eq!(
            json!(LibContinuity::RegressionOrUnrelated),
            json!("regression_or_unrelated")
        );
        assert_eq!(
            json!(LibContinuity::MissingPreviousLibBaseline),
            json!("missing_previous_lib_baseline")
        );
    }

    #[test]
    fn binary_provenance_distinguishes_override_and_expected_features() {
        let selected = Path::new(env!("CARGO_MANIFEST_DIR"))
            .canonicalize()
            .unwrap();
        assert_eq!(
            binary_selection_mode(None, &selected),
            "framework_provider_unspecified"
        );
        assert_eq!(
            binary_selection_mode(Some(&selected), &selected),
            "environment_override_matches_selected_path"
        );
        assert_eq!(
            binary_selection_mode(Some(Path::new("/missing-audit-binary")), &selected),
            "environment_override_configured_selection_unverified"
        );
        assert_eq!(
            expected_node_build_features(&NodeBinaryProfile::Normal),
            ["testing", "security-audit"]
        );
        assert_eq!(
            expected_node_build_features(&NodeBinaryProfile::TokioConsole),
            ["testing", "security-audit", "tokio-console"]
        );
        let mut provenance =
            json!({"observed_audit_functionality": {"status": "not_observed_yet"}});
        record_observed_audit_opportunity(&mut provenance, 2);
        assert_eq!(
            provenance["observed_audit_functionality"]["complete_producer_opportunities"],
            2
        );
        assert_eq!(
            provenance["observed_audit_functionality"]["executable_feature_set_independently_verified"],
            false
        );
    }

    const SIBLING_ID: &str = "0xsibling";
    const PARENT_ID: &str = "0xparent";

    fn participant_rows(entries: &[(&str, &str, &str)]) -> Vec<Vec<String>> {
        std::iter::once(("node", "role", "canonical_reference"))
            .chain(entries.iter().copied())
            .map(|(node, role, reference)| {
                vec![node.to_owned(), role.to_owned(), reference.to_owned()]
            })
            .collect()
    }

    #[test]
    fn participant_declarations_accept_arbitrary_cluster_names_and_honest_counts() {
        let nodes = HashSet::from(["leader".to_owned(), "alice".to_owned(), "bob".to_owned()]);
        let one = participant_rows(&[("leader", "producer", "no"), ("alice", "honest", "yes")]);
        let two_node_cluster = HashSet::from(["leader".to_owned(), "alice".to_owned()]);
        let participants = parse_audit_participants(&one, &two_node_cluster).unwrap();
        assert!(parse_audit_participants(&one, &nodes).is_err());
        assert_eq!(participants.producer, "leader");
        assert_eq!(participants.honest_nodes, ["alice"]);
        assert_eq!(participants.canonical_reference, "alice");
        let two = participant_rows(&[
            ("leader", "producer", "no"),
            ("alice", "honest", "yes"),
            ("bob", "honest", "no"),
        ]);
        assert_eq!(
            parse_audit_participants(&two, &nodes)
                .unwrap()
                .honest_nodes
                .len(),
            2
        );
    }

    #[test]
    fn participant_declarations_reject_duplicate_missing_and_conflicting_roles() {
        let nodes = HashSet::from(["leader".to_owned(), "alice".to_owned(), "bob".to_owned()]);
        for entries in [
            vec![("leader", "producer", "no"), ("leader", "honest", "yes")],
            vec![("alice", "honest", "yes")],
            vec![("leader", "producer", "no")],
            vec![("leader", "producer", "yes"), ("alice", "honest", "yes")],
            vec![("leader", "producer", "no"), ("alice", "honest", "no")],
            vec![
                ("leader", "producer", "no"),
                ("alice", "honest", "yes"),
                ("bob", "honest", "yes"),
            ],
            vec![("leader", "producer", "no"), ("missing", "honest", "yes")],
            vec![("leader", "producer", "no"), ("alice", "other", "yes")],
            vec![
                ("leader", "producer", "no"),
                ("bob", "producer", "no"),
                ("alice", "honest", "yes"),
            ],
        ] {
            assert!(parse_audit_participants(&participant_rows(&entries), &nodes).is_err());
        }
    }

    fn header_id(byte: u8) -> HeaderId {
        HeaderId::try_from([byte; 32].as_slice()).expect("32-byte test header ID")
    }

    fn event(
        event_name: &str,
        stage_id: &str,
        parent: &str,
        slot: &str,
        second: u32,
        offset: u64,
    ) -> ObservedLogLine {
        ObservedLogLine {
            text: format!(
                "2026-10-11T10:00:{second:02}.000Z INFO event=\"{event_name}\" {stage_id}=\"{SIBLING_ID}\" parent_block_id=\"{parent}\" slot=\"{slot}\""
            ),
            path: PathBuf::from("NODE_2.log"),
            offset,
        }
    }

    fn stage(stage: &str, second: u32, offset: u64) -> ObservedLogLine {
        let event_name = match stage {
            "blend_received" => "security_audit_blend_payload_decapsulated",
            "chain_proposal_received" => "security_audit_chain_proposal_received",
            "reconstructed" => "security_audit_proposal_reconstructed",
            "accepted" => "security_audit_proposal_accepted",
            _ => panic!("unsupported test stage: {stage}"),
        };
        event(event_name, "block_id", PARENT_ID, "42", second, offset)
    }

    fn matched_evidence(events: Vec<ObservedLogLine>) -> ReceiverEvidence {
        let mut logs = BTreeMap::new();
        logs.insert("NODE_2".to_owned(), events);
        let ordinary = HashMap::from([
            ("parent_block_id".to_owned(), PARENT_ID.to_owned()),
            ("slot".to_owned(), "42".to_owned()),
        ]);
        receiver_stages(
            &logs,
            &BTreeSet::from([SIBLING_ID.to_owned()]),
            &ordinary,
            &["NODE_2"],
        )
        .remove(&("NODE_2".to_owned(), SIBLING_ID.to_owned()))
        .unwrap_or_default()
    }

    fn producer_log(configured: usize, indices: &[usize]) -> Vec<ObservedLogLine> {
        let mut lines = vec![format!(
            "event=security_audit_ordinary_submitted ordinary_block_id=ordinary parent_block_id=parent slot=42 configured_siblings={configured} ordinary_self_applied=true ordinary_relay_enqueued=true"
        )];
        lines.extend(indices.iter().map(|index| {
            format!(
                "event=security_audit_sibling_submitted ordinary_block_id=ordinary sibling_block_id=sibling-{index} parent_block_id=parent slot=42 sibling_index={index}"
            )
        }));
        lines.push(format!(
            "event=security_audit_leadership_timing parent_block_id=parent slot=42 configured_siblings={configured} constructed_siblings={configured} ordinary_self_applied=true ordinary_relay_enqueued=true"
        ));
        lines
            .into_iter()
            .enumerate()
            .map(|(index, line)| ObservedLogLine {
                text: format!("2026-10-11T10:00:{index:02}.000Z INFO {line}"),
                path: PathBuf::from("producer.log"),
                offset: u64::try_from(index).expect("small test offset"),
            })
            .collect()
    }

    #[test]
    fn producer_opportunities_use_the_recorded_configured_sibling_count() {
        for count in [1, 2, 4, 6] {
            let indices = (1..=count).collect::<Vec<_>>();
            let opportunities = find_producer_opportunities(&producer_log(count, &indices));
            assert_eq!(opportunities.len(), 1);
            assert_eq!(opportunities[0].siblings.len(), count);
        }
    }

    #[test]
    fn producer_opportunity_rejects_missing_duplicate_and_out_of_range_indices() {
        for (count, indices) in [
            (0, vec![]),
            (3, vec![1, 2]),
            (3, vec![1, 1, 3]),
            (3, vec![1, 2, 4]),
        ] {
            assert!(find_producer_opportunities(&producer_log(count, &indices)).is_empty());
        }
    }

    #[test]
    fn producer_opportunity_rejects_mismatched_timing_count() {
        let mut lines = producer_log(2, &[1, 2]);
        let timing = lines.last_mut().expect("timing event");
        timing.text = timing
            .text
            .replace("constructed_siblings=2", "constructed_siblings=3");
        assert!(find_producer_opportunities(&lines).is_empty());
    }

    #[test]
    fn finality_boundary_uses_the_last_pre_shutdown_attack_slot() {
        let opportunities = vec![
            json!({"slot": "12"}),
            json!({"slot": "19"}),
            json!({"slot": "15"}),
        ];
        assert_eq!(last_observed_attack_slot(&opportunities, 12).unwrap(), 19);
        assert_eq!(last_observed_attack_slot(&[], 12).unwrap(), 12);
        assert!(last_observed_attack_slot(&[json!({"slot": "invalid"})], 12).is_err());
        assert!(last_observed_attack_slot(&[json!({})], 12).is_err());
    }

    #[test]
    fn ordered_receiver_sequence_on_one_honest_node_is_accepted() {
        let evidence = matched_evidence(vec![
            stage("blend_received", 1, 1),
            stage("chain_proposal_received", 2, 2),
            stage("reconstructed", 3, 3),
            stage("accepted", 4, 4),
        ]);

        assert!(evidence.has_ordered_blend_to_acceptance());
    }

    #[test]
    fn reverse_order_is_not_accepted() {
        let evidence = matched_evidence(vec![
            stage("accepted", 1, 1),
            stage("reconstructed", 2, 2),
            stage("chain_proposal_received", 3, 3),
            stage("blend_received", 4, 4),
        ]);

        assert!(!evidence.has_ordered_blend_to_acceptance());
    }

    #[test]
    fn stages_from_different_honest_nodes_are_not_combined() {
        let mut logs = BTreeMap::new();
        logs.insert("NODE_2".to_owned(), vec![stage("blend_received", 1, 1)]);
        logs.insert(
            "NODE_3".to_owned(),
            vec![
                event(
                    "security_audit_chain_proposal_received",
                    "block_id",
                    PARENT_ID,
                    "42",
                    2,
                    2,
                ),
                event(
                    "security_audit_proposal_reconstructed",
                    "block_id",
                    PARENT_ID,
                    "42",
                    3,
                    3,
                ),
                event(
                    "security_audit_proposal_accepted",
                    "block_id",
                    PARENT_ID,
                    "42",
                    4,
                    4,
                ),
            ],
        );
        let ordinary = HashMap::from([
            ("parent_block_id".to_owned(), PARENT_ID.to_owned()),
            ("slot".to_owned(), "42".to_owned()),
        ]);
        let evidence = receiver_stages(
            &logs,
            &BTreeSet::from([SIBLING_ID.to_owned()]),
            &ordinary,
            &["NODE_2", "NODE_3"],
        );

        assert!(
            evidence
                .values()
                .all(|item| !item.has_ordered_blend_to_acceptance())
        );
    }

    #[test]
    fn receiver_correlation_uses_only_the_supplied_honest_nodes() {
        let complete = vec![
            stage("blend_received", 1, 1),
            stage("chain_proposal_received", 2, 2),
            stage("reconstructed", 3, 3),
            stage("accepted", 4, 4),
        ];
        let logs = BTreeMap::from([
            ("producer".to_owned(), complete.clone()),
            ("peer-a".to_owned(), complete.clone()),
            ("peer-b".to_owned(), complete),
        ]);
        let ordinary = HashMap::from([
            ("parent_block_id".to_owned(), PARENT_ID.to_owned()),
            ("slot".to_owned(), "42".to_owned()),
        ]);
        let ids = BTreeSet::from([SIBLING_ID.to_owned()]);
        let one_peer = receiver_stages(&logs, &ids, &ordinary, &["peer-a"]);
        assert_eq!(one_peer.len(), 1);
        assert!(
            one_peer[&("peer-a".to_owned(), SIBLING_ID.to_owned())]
                .has_ordered_blend_to_acceptance()
        );
        let two_peers = receiver_stages(&logs, &ids, &ordinary, &["peer-a", "peer-b"]);
        assert_eq!(two_peers.len(), 2);
        assert!(!two_peers.contains_key(&("producer".to_owned(), SIBLING_ID.to_owned())));
        assert!(receiver_stages(&logs, &ids, &ordinary, &["missing"]).is_empty());
    }

    #[test]
    fn missing_reconstruction_is_not_accepted() {
        let evidence = matched_evidence(vec![
            stage("blend_received", 1, 1),
            stage("chain_proposal_received", 2, 2),
            stage("accepted", 4, 4),
        ]);

        assert!(!evidence.has_ordered_blend_to_acceptance());
    }

    #[test]
    fn wrong_parent_or_slot_events_are_excluded() {
        for (parent, slot) in [("0xother", "42"), (PARENT_ID, "43")] {
            let evidence = matched_evidence(vec![
                event(
                    "security_audit_blend_payload_decapsulated",
                    "block_id",
                    parent,
                    slot,
                    1,
                    1,
                ),
                stage("chain_proposal_received", 2, 2),
                stage("reconstructed", 3, 3),
                stage("accepted", 4, 4),
            ]);

            assert!(!evidence.has_ordered_blend_to_acceptance());
            assert_eq!(evidence.mismatched_events.len(), 1);
        }
    }

    #[test]
    fn repeated_events_are_accepted_only_when_a_valid_ordered_path_exists() {
        let valid = matched_evidence(vec![
            stage("blend_received", 1, 1),
            stage("blend_received", 2, 2),
            stage("chain_proposal_received", 3, 3),
            stage("reconstructed", 4, 4),
            stage("accepted", 5, 5),
        ]);
        let invalid = matched_evidence(vec![
            stage("blend_received", 1, 1),
            stage("reconstructed", 2, 2),
            stage("chain_proposal_received", 3, 3),
            stage("accepted", 4, 4),
            stage("chain_proposal_received", 5, 5),
        ]);

        assert!(valid.has_ordered_blend_to_acceptance());
        assert!(!invalid.has_ordered_blend_to_acceptance());
    }

    #[test]
    fn distinct_timestamps_order_events_across_rotated_logs() {
        let mut events = vec![
            stage("blend_received", 1, 1),
            stage("chain_proposal_received", 2, 2),
            stage("reconstructed", 3, 3),
            stage("accepted", 4, 4),
        ];
        events[0].path = PathBuf::from("NODE_2.log.1");
        events[1].path = PathBuf::from("NODE_2.log.2");
        events[2].path = PathBuf::from("NODE_2.log.1");
        events[3].path = PathBuf::from("NODE_2.log.2");

        assert!(matched_evidence(events).has_ordered_blend_to_acceptance());
    }

    #[test]
    fn missing_timestamps_and_equal_time_cross_file_events_are_incomplete() {
        let mut missing_timestamp = vec![
            stage("blend_received", 1, 1),
            stage("chain_proposal_received", 2, 2),
            stage("reconstructed", 3, 3),
            stage("accepted", 4, 4),
        ];
        missing_timestamp[0].text = "INFO event=\"security_audit_blend_payload_decapsulated\" block_id=\"0xsibling\" parent_block_id=\"0xparent\" slot=\"42\"".to_owned();
        let tied_cross_file = vec![
            stage("blend_received", 1, 1),
            stage("chain_proposal_received", 1, 2),
            stage("reconstructed", 1, 3),
            stage("accepted", 1, 4),
        ]
        .into_iter()
        .enumerate()
        .map(|(index, mut event)| {
            event.path = PathBuf::from(format!("NODE_2.log.{}", index % 2));
            event
        })
        .collect::<Vec<_>>();

        assert!(!matched_evidence(missing_timestamp).has_ordered_blend_to_acceptance());
        assert!(!matched_evidence(tied_cross_file).has_ordered_blend_to_acceptance());
    }

    fn body_evidence(block_id: &str, transaction_hashes: &[&str]) -> BlockBodyEvidence {
        BlockBodyEvidence {
            block_id: block_id.to_owned(),
            parent_block_id: PARENT_ID.to_owned(),
            slot: 42,
            body_root: "0xbody-root".to_owned(),
            voucher_commitment: "ordinary-voucher".to_owned(),
            transaction_hashes: transaction_hashes
                .iter()
                .map(|hash| (*hash).to_owned())
                .collect(),
        }
    }

    fn opportunity_with_siblings() -> ProducerOpportunity {
        let siblings = (1..=SIBLING_COUNT)
            .map(|index| {
                (
                    index,
                    HashMap::from([
                        ("sibling_block_id".to_owned(), format!("0xsibling-{index}")),
                        ("parent_block_id".to_owned(), PARENT_ID.to_owned()),
                        ("slot".to_owned(), "42".to_owned()),
                    ]),
                )
            })
            .collect();
        ProducerOpportunity {
            ordinary: HashMap::from([
                ("ordinary_block_id".to_owned(), "0xordinary".to_owned()),
                ("parent_block_id".to_owned(), PARENT_ID.to_owned()),
                ("slot".to_owned(), "42".to_owned()),
                ("configured_siblings".to_owned(), SIBLING_COUNT.to_string()),
            ]),
            ordinary_line: 1,
            siblings,
            timing: HashMap::new(),
            timing_line: 10,
        }
    }

    #[test]
    fn shutdown_trigger_requires_selected_sibling_and_matching_parent_and_slot() {
        let mut opportunity = opportunity_with_siblings();
        opportunity.siblings[0]
            .1
            .insert("sibling_block_id".to_owned(), SIBLING_ID.to_owned());
        let nodes = vec!["NODE_2".to_owned()];
        let mut logs = BTreeMap::from([(
            "NODE_2".to_owned(),
            vec![event(
                "security_audit_blend_payload_decapsulated",
                "block_id",
                "wrong",
                "42",
                1,
                1,
            )],
        )]);
        assert!(selected_sibling_reception(&logs, &opportunity, &nodes).is_none());
        logs.get_mut("NODE_2").unwrap().push(event(
            "security_audit_blend_payload_decapsulated",
            "block_id",
            PARENT_ID,
            "43",
            2,
            2,
        ));
        assert!(selected_sibling_reception(&logs, &opportunity, &nodes).is_none());
        logs.get_mut("NODE_2")
            .unwrap()
            .push(stage("blend_received", 3, 3));
        let (node, sibling, observed) =
            selected_sibling_reception(&logs, &opportunity, &nodes).unwrap();
        assert_eq!(
            (node.as_str(), sibling.as_str(), observed.offset),
            ("NODE_2", SIBLING_ID, 3)
        );
        assert!(selected_sibling_reception(&logs, &opportunity, &["other".to_owned()]).is_none());
        opportunity.siblings.clear();
        assert!(selected_sibling_reception(&logs, &opportunity, &nodes).is_none());
    }

    #[test]
    fn nonempty_body_selection_rejects_empty_and_unrelated_transactions() {
        let submitted = BTreeSet::from(["0xtest-transaction".to_owned()]);

        assert!(!has_nonempty_submitted_body(&[], &submitted));
        assert!(!has_nonempty_submitted_body(
            &["0xother-transaction".to_owned()],
            &submitted
        ));
        assert!(has_nonempty_submitted_body(
            &[
                "0xother-transaction".to_owned(),
                "0xtest-transaction".to_owned()
            ],
            &submitted
        ));
    }

    #[test]
    fn sibling_body_requires_identical_commitment_and_ordered_transaction_hashes() {
        let ordinary = body_evidence("0xordinary", &["0xtx-a", "0xtx-b"]);
        assert!(same_transaction_body(
            &ordinary,
            "0xbody-root",
            &["0xtx-a".to_owned(), "0xtx-b".to_owned()]
        ));
        assert!(!same_transaction_body(
            &ordinary,
            "0xdifferent-root",
            &["0xtx-a".to_owned(), "0xtx-b".to_owned()]
        ));
        assert!(!same_transaction_body(
            &ordinary,
            "0xbody-root",
            &["0xtx-b".to_owned(), "0xtx-a".to_owned()]
        ));
        assert!(!same_transaction_body(
            &ordinary,
            "0xbody-root",
            &["0xtx-a".to_owned(), "0xtx-c".to_owned()]
        ));
    }

    #[test]
    fn retrieved_vouchers_must_be_distinct_and_consistent() {
        let mut seen = BTreeMap::new();
        assert!(record_voucher_commitment(
            &mut seen,
            "ordinary",
            "a",
            Some("voucher-a")
        ));
        assert!(record_voucher_commitment(
            &mut seen,
            "ordinary",
            "a",
            Some("voucher-a")
        ));
        assert!(!record_voucher_commitment(
            &mut seen,
            "ordinary",
            "b",
            Some("voucher-a")
        ));
        assert!(!record_voucher_commitment(
            &mut seen,
            "ordinary",
            "c",
            Some("ordinary")
        ));
        assert!(!record_voucher_commitment(&mut seen, "ordinary", "d", None));
        assert!(!record_voucher_commitment(
            &mut seen,
            "ordinary",
            "a",
            Some("different")
        ));
    }

    #[test]
    fn sibling_identity_check_requires_distinct_same_parent_same_slot_ids() {
        let ordinary = body_evidence("0xordinary", &["0xtx-a"]);
        let mut opportunity = opportunity_with_siblings();
        assert!(sibling_body_integrity(&ordinary, &opportunity));

        opportunity.siblings[0]
            .1
            .insert("parent_block_id".to_owned(), "0xother-parent".to_owned());
        assert!(!sibling_body_integrity(&ordinary, &opportunity));
    }

    #[test]
    fn configured_sibling_count_controls_identity_and_minimum_checks() {
        let ordinary = body_evidence("0xordinary", &["0xtx-a"]);
        let mut opportunity = opportunity_with_siblings();
        opportunity.siblings.truncate(2);
        opportunity
            .ordinary
            .insert("configured_siblings".to_owned(), "2".to_owned());
        assert!(sibling_body_integrity(&ordinary, &opportunity));
        assert!(valid_sibling_minimum(2, 1));
        assert!(valid_sibling_minimum(2, 2));
        assert!(!valid_sibling_minimum(2, 3));
        assert!(!valid_sibling_minimum(2, 0));
        opportunity.siblings[1]
            .1
            .insert("sibling_block_id".to_owned(), "0xsibling-1".to_owned());
        assert!(!sibling_body_integrity(&ordinary, &opportunity));
        opportunity.ordinary.remove("configured_siblings");
        assert!(!sibling_body_integrity(&ordinary, &opportunity));
    }

    fn attack_evidence_for_branch_checks() -> AttackEvidence {
        AttackEvidence {
            ordinary_id: header_id(1),
            ordinary_id_text: header_id(1).encode_hex::<String>(),
            parent_id: header_id(9),
            parent_id_text: header_id(9).encode_hex::<String>(),
            slot: 42,
            body_root: "test-body-root".to_owned(),
            transaction_hashes: vec!["test-transaction".to_owned()],
            siblings: (1..=SIBLING_COUNT)
                .map(|index| {
                    (
                        index,
                        AttackCandidate {
                            id: header_id(index as u8 + 1),
                        },
                    )
                })
                .collect(),
        }
    }

    fn consensus_state_with_lib(tip: u8, lib: u8, lib_slot: u64) -> ConsensusState {
        ConsensusState {
            tip_id: header_id(tip),
            tip_id_text: header_id(tip).encode_hex::<String>(),
            tip_height: u64::from(tip),
            tip_slot: u64::from(tip),
            lib_id: header_id(lib),
            lib_id_text: header_id(lib).encode_hex::<String>(),
            lib_slot,
            tip_descends_from_lib: Some(true),
        }
    }

    fn below_target_path(block: u8, parent: u8, slot: u64) -> AncestryPath {
        AncestryPath {
            links: vec![HeaderLink {
                id: header_id(block),
                parent: header_id(parent),
                slot,
            }],
            target_reached: false,
            complete_below_slot: true,
            error: None,
        }
    }

    #[test]
    fn ancestry_presence_distinguishes_proven_absence_from_missing_evidence() {
        let target = header_id(2);
        let parent_edge_proof = AncestryPath {
            links: vec![HeaderLink {
                id: header_id(1),
                parent: target,
                slot: 43,
            }],
            target_reached: true,
            complete_below_slot: true,
            error: None,
        };
        let absent = AncestryPath {
            links: vec![HeaderLink {
                id: header_id(1),
                parent: header_id(9),
                slot: 43,
            }],
            target_reached: false,
            complete_below_slot: true,
            error: None,
        };
        let incomplete = AncestryPath {
            links: absent.links.clone(),
            target_reached: false,
            complete_below_slot: false,
            error: Some("block API unavailable".to_owned()),
        };
        assert_eq!(ancestry_presence(&parent_edge_proof, target), Some(true));
        assert_eq!(ancestry_presence(&absent, target), Some(false));
        assert_eq!(ancestry_presence(&incomplete, target), None);
    }

    #[test]
    fn self_parented_header_is_incomplete_not_proven_ancestry_absence() {
        let self_parented = header_id(7);
        let path = self_parented_ancestry_result(
            vec![HeaderLink {
                id: self_parented,
                parent: self_parented,
                slot: 0,
            }],
            self_parented,
        );

        assert!(!path.target_reached);
        assert!(!path.complete_below_slot);
        assert!(
            path.error
                .as_deref()
                .is_some_and(|error| error.contains("self-parented"))
        );
        assert_eq!(ancestry_presence(&path, header_id(8)), None);
    }

    #[test]
    fn pre_attack_lib_slot_proves_equivocation_is_not_finalized_without_genesis_lookup() {
        let attack = attack_evidence_for_branch_checks();
        let path = proven_below_target_slot_path(0, attack.slot)
            .expect("a LIB below the attack slot proves it cannot contain the attack block");

        assert_eq!(ancestry_presence(&path, attack.parent_id), Some(false));
        assert_eq!(
            classify_attack_branch(&path.links, path.complete_below_slot, &attack),
            AttackBranch::None
        );
        assert!(proven_below_target_slot_path(attack.slot, attack.slot).is_none());
    }

    #[test]
    fn producer_log_reader_uses_only_the_current_runtime_directory() {
        let test_root = std::env::temp_dir().join(format!(
            "security-audit-runtime-logs-{}-{}",
            std::process::id(),
            unix_timestamp_ms()
        ));
        let current_runtime = test_root.join("NODE_1_current");
        let stale_runtime = test_root.join("NODE_1_stale");
        fs::create_dir_all(&current_runtime).expect("create current runtime directory");
        fs::create_dir_all(&stale_runtime).expect("create stale runtime directory");
        fs::write(current_runtime.join("__logs-NODE_1"), "current run")
            .expect("write current producer log");
        fs::write(stale_runtime.join("__logs-NODE_1"), "stale run")
            .expect("write stale producer log");

        assert_eq!(
            read_runtime_node_logs(&current_runtime, "NODE_1").expect("read current producer log"),
            "current run"
        );

        fs::remove_dir_all(test_root).expect("remove temporary runtime directories");
    }

    #[test]
    fn canonical_branch_resolution_distinguishes_equivocation_candidates() {
        let attack = attack_evidence_for_branch_checks();
        let matching = |id| HeaderLink {
            id,
            parent: attack.parent_id,
            slot: attack.slot,
        };

        assert_eq!(
            classify_attack_branch(&[matching(attack.ordinary_id)], true, &attack),
            AttackBranch::Ordinary
        );
        assert_eq!(
            classify_attack_branch(&[matching(attack.siblings[2].1.id)], true, &attack),
            AttackBranch::Sibling(3)
        );
        assert_eq!(
            classify_attack_branch(&[matching(header_id(8))], true, &attack),
            AttackBranch::OtherCompeting(header_id(8))
        );
        assert_eq!(
            classify_attack_branch(&[], true, &attack),
            AttackBranch::None
        );
        assert_eq!(
            classify_attack_branch(&[], false, &attack),
            AttackBranch::Unverified
        );
    }

    #[test]
    fn equal_tip_heights_do_not_imply_canonical_alignment() {
        assert!(canonical_tips_aligned(
            0,
            &[header_id(1), header_id(1), header_id(1)],
            1
        ));
        assert!(canonical_tips_aligned(
            1,
            &[header_id(1), header_id(1), header_id(1)],
            1
        ));
        assert!(!canonical_tips_aligned(
            0,
            &[header_id(1), header_id(2), header_id(1)],
            1
        ));
        assert!(!canonical_tips_aligned(
            2,
            &[header_id(1), header_id(1), header_id(1)],
            1
        ));
        assert!(!canonical_tips_aligned(0, &[], 1));
    }

    #[test]
    fn brief_common_lib_then_different_final_libs_cannot_complete_stability() {
        let common_id = header_id(50);
        let briefly_common = vec![
            ("NODE_2".to_owned(), consensus_state_with_lib(60, 50, 50)),
            ("NODE_3".to_owned(), consensus_state_with_lib(61, 50, 50)),
            ("NODE_4".to_owned(), consensus_state_with_lib(62, 50, 50)),
        ];
        assert_eq!(
            common_finalized_lib(&briefly_common, 3),
            Some((common_id, 50))
        );
        let (stable_since, completion) = update_stable_finality_window(true, 0, None, 10_000);
        assert_eq!(stable_since, Some(0));
        assert_eq!(completion, None);

        let final_libs_differ = vec![
            ("NODE_2".to_owned(), consensus_state_with_lib(70, 50, 50)),
            ("NODE_3".to_owned(), consensus_state_with_lib(71, 51, 51)),
            ("NODE_4".to_owned(), consensus_state_with_lib(72, 50, 50)),
        ];
        assert_eq!(common_finalized_lib(&final_libs_differ, 3), None);
        let (stable_since, completion) =
            update_stable_finality_window(false, 5_000, stable_since, 10_000);
        assert_eq!(stable_since, None);
        assert_eq!(completion, None);
    }

    #[test]
    fn common_lib_first_seen_at_timeout_boundary_cannot_complete_stability() {
        let timeout_ms = FINALITY_OBSERVATION_TIMEOUT.as_millis();
        let (stable_since, completion) =
            update_stable_finality_window(true, timeout_ms, None, 10_000);
        assert_eq!(stable_since, Some(timeout_ms));
        assert_eq!(completion, None);

        let (stable_since, completion) =
            update_stable_finality_window(false, timeout_ms + 1, stable_since, 10_000);
        assert_eq!(stable_since, None);
        assert_eq!(completion, None);
    }

    #[test]
    fn ten_seconds_of_verified_common_finality_completes_stability() {
        let (stable_since, completion) = update_stable_finality_window(true, 100, None, 10_000);
        assert_eq!(stable_since, Some(100));
        assert_eq!(completion, None);

        let (_, completion) = update_stable_finality_window(true, 10_100, stable_since, 10_000);
        assert_eq!(
            completion,
            Some(StableFinalityCompletion {
                started_elapsed_ms: 100,
                completed_elapsed_ms: 10_100,
            })
        );
    }

    #[test]
    fn producer_prompt_stop_uses_the_same_thirty_second_limit_everywhere() {
        let limit = Duration::from_secs(30);
        let limit_ms = u64::try_from(limit.as_millis()).unwrap();
        assert_eq!(limit_ms, 30_000);
        assert!(producer_stop_is_prompt(true, limit_ms - 1, limit));
        assert!(producer_stop_is_prompt(true, limit_ms, limit));
        assert!(!producer_stop_is_prompt(true, limit_ms + 1, limit));
        assert!(!producer_stop_is_prompt(false, 0, limit));
    }

    #[test]
    fn finalized_histories_with_the_same_lib_are_shared() {
        assert_eq!(
            classify_finalized_history_relation(
                header_id(50),
                50,
                header_id(50),
                50,
                Some(true),
                Some(true),
            ),
            FinalizedHistoryRelation::Shared
        );
    }

    #[test]
    fn different_libs_with_a_proven_ancestor_relationship_are_not_conflicts() {
        let earlier = header_id(50);
        let later_path = AncestryPath {
            links: vec![HeaderLink {
                id: header_id(60),
                parent: earlier,
                slot: 60,
            }],
            target_reached: true,
            complete_below_slot: true,
            error: None,
        };
        assert_eq!(ancestry_presence(&later_path, earlier), Some(true));
        assert_eq!(
            classify_finalized_history_relation(
                earlier,
                50,
                header_id(60),
                60,
                Some(false),
                Some(true),
            ),
            FinalizedHistoryRelation::Ancestral
        );
    }

    #[test]
    fn different_attack_slot_sibling_libs_are_conflicting() {
        let first = header_id(2);
        let second = header_id(3);
        let parent = header_id(9);
        let first_to_second = below_target_path(2, 9, 42);
        let second_to_first = below_target_path(3, 9, 42);
        assert_eq!(ancestry_presence(&first_to_second, second), Some(false));
        assert_eq!(ancestry_presence(&second_to_first, first), Some(false));
        assert_eq!(
            classify_finalized_history_relation(
                first,
                42,
                second,
                42,
                ancestry_presence(&first_to_second, second),
                ancestry_presence(&second_to_first, first),
            ),
            FinalizedHistoryRelation::Conflicting
        );
        assert_eq!(first_to_second.links[0].parent, parent);
    }

    #[test]
    fn attack_slot_sibling_and_competing_descendant_are_conflicting() {
        let finalized_sibling = header_id(2);
        let competing_sibling = header_id(3);
        let descendant = header_id(4);
        let earlier_path = proven_below_target_slot_path(42, 50).unwrap();
        let later_path = AncestryPath {
            links: vec![HeaderLink {
                id: descendant,
                parent: competing_sibling,
                slot: 50,
            }],
            target_reached: false,
            complete_below_slot: true,
            error: None,
        };
        assert_eq!(ancestry_presence(&earlier_path, descendant), Some(false));
        assert_eq!(
            ancestry_presence(&later_path, finalized_sibling),
            Some(false)
        );
        assert_eq!(
            classify_finalized_history_relation(
                finalized_sibling,
                42,
                descendant,
                50,
                ancestry_presence(&earlier_path, descendant),
                ancestry_presence(&later_path, finalized_sibling),
            ),
            FinalizedHistoryRelation::Conflicting
        );
    }

    #[test]
    fn different_later_finalized_histories_are_conflicting() {
        let first = header_id(5);
        let second = header_id(6);
        let first_path = below_target_path(5, 1, 60);
        let second_path = below_target_path(6, 1, 62);
        assert_eq!(
            classify_finalized_history_relation(
                first,
                60,
                second,
                62,
                ancestry_presence(&first_path, second),
                ancestry_presence(&second_path, first),
            ),
            FinalizedHistoryRelation::Conflicting
        );
    }

    #[test]
    fn missing_finalized_lib_ancestry_is_unverified() {
        let first = header_id(5);
        let second = header_id(6);
        let absent = below_target_path(5, 1, 60);
        let missing = AncestryPath {
            links: vec![HeaderLink {
                id: header_id(6),
                parent: header_id(4),
                slot: 62,
            }],
            target_reached: false,
            complete_below_slot: false,
            error: Some("block API returned no block".to_owned()),
        };
        assert_eq!(
            classify_finalized_history_relation(
                first,
                60,
                second,
                62,
                ancestry_presence(&absent, second),
                ancestry_presence(&missing, first),
            ),
            FinalizedHistoryRelation::Unverified
        );
    }

    #[test]
    fn tip_change_to_descendant_is_not_a_reorganization() {
        assert_eq!(
            classify_tip_transition(header_id(1), header_id(2), Some(true)),
            TipTransition::Descendant
        );
    }

    #[test]
    fn tip_reorganization_above_same_attack_sibling_is_recorded_separately() {
        let attack_branch_before = AttackBranch::Sibling(1);
        let attack_branch_after = AttackBranch::Sibling(1);
        assert_eq!(attack_branch_before, attack_branch_after);
        assert_eq!(
            classify_tip_transition(header_id(7), header_id(8), Some(false)),
            TipTransition::Reorganization
        );
    }

    #[test]
    fn missing_tip_ancestry_is_unverified_not_no_reorganization() {
        assert_eq!(
            classify_tip_transition(header_id(7), header_id(8), None),
            TipTransition::Unverified
        );
    }

    #[test]
    fn sibling_index_parser_accepts_log_strings_and_json_numbers() {
        assert_eq!(json_usize(&json!(3)), Some(3));
        assert_eq!(json_usize(&json!("4")), Some(4));
        assert_eq!(json_usize(&json!("not-an-index")), None);
    }

    #[test]
    fn quoted_receiver_failure_reason_preserves_spaces() {
        let fields = parse_log_fields(
            "2026-10-11T10:00:00.000Z INFO event=security_audit_proposal_reconstruction_failed reason=\"UnresolvedReference: transaction hash not in mempool\"",
        );

        assert_eq!(
            fields.get("reason").map(String::as_str),
            Some("UnresolvedReference: transaction hash not in mempool")
        );
    }
}
