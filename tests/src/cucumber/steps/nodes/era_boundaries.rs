//! System-level evidence for ordinary era transitions and late-joining IBD.
//! The real node's deployment.yaml, HTTP APIs, and runtime state are the
//! sources.

use std::{
    collections::{BTreeSet, HashMap, HashSet},
    fs::{self, OpenOptions},
    io::Write as _,
    time::Duration,
};

use cucumber::{given, then, when};
use lb_common_http_client::Error as HttpClientError;
use lb_core::header::HeaderId;
use lb_node::config::DeploymentSettings;
use lb_testing_framework::{NodeHttpClient, configs::deployment::EraBoundarySchedule};
use reqwest::StatusCode;
use tokio::time::{Instant, sleep};
use tracing::info;

use crate::cucumber::{
    error::{StepError, StepResult},
    steps::TARGET,
    world::CucumberWorld,
};

const EVIDENCE_FILE: &str = "era_boundaries_evidence.ndjson";
const MAX_ANCESTOR_WALK: usize = 1_000_000;
fn failure(message: impl Into<String>) -> StepError {
    StepError::StepFail {
        message: message.into(),
    }
}

#[derive(Debug)]
struct HistoryInspectionError {
    message: String,
    retryable: bool,
}

impl HistoryInspectionError {
    fn retryable(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            retryable: true,
        }
    }

    fn immediate(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            retryable: false,
        }
    }
}

fn transient_http_status(status: StatusCode) -> bool {
    status.is_server_error()
        || status == StatusCode::REQUEST_TIMEOUT
        || status == StatusCode::TOO_MANY_REQUESTS
}

fn transient_block_fetch_error(error: &HttpClientError) -> bool {
    match error {
        HttpClientError::Request(error) => {
            error.is_connect() || error.is_timeout() || error.is_body()
        }
        HttpClientError::HttpStatus { status, .. } => transient_http_status(*status),
        HttpClientError::Timeout(_) => true,
        HttpClientError::Server(_) | HttpClientError::Client(_) | HttpClientError::Url(_) => false,
    }
}

fn observe_history_error(
    node: &str,
    error: HistoryInspectionError,
    last_observations: &mut HashMap<String, String>,
) -> StepResult {
    if error.retryable {
        last_observations.insert(node.to_owned(), error.message);
        return Ok(());
    }

    Err(failure(format!(
        "Node `{node}` ancestry inspection failed: {}",
        error.message
    )))
}

fn polling_timeout_message(
    summary: &str,
    nodes: &[String],
    last_observations: &HashMap<String, String>,
) -> String {
    let observations = nodes
        .iter()
        .filter_map(|node| {
            last_observations
                .get(node)
                .map(|observation| format!("{node}: {observation}"))
        })
        .collect::<Vec<_>>();

    if observations.is_empty() {
        summary.to_owned()
    } else {
        format!("{summary}; last observations: {}", observations.join("; "))
    }
}

fn deployment_for_node(world: &CucumberWorld, name: &str) -> Result<DeploymentSettings, StepError> {
    let node = world
        .nodes_info
        .get(name)
        .ok_or_else(|| failure(format!("Node `{name}` has not started")))?;
    let path = node.runtime_dir.join("deployment.yaml");
    let yaml = fs::read_to_string(&path)
        .map_err(|error| failure(format!("Cannot read {}: {error}", path.display())))?;
    serde_yaml::from_str(&yaml)
        .map_err(|error| failure(format!("Cannot decode {}: {error}", path.display())))
}

#[derive(Clone, Debug)]
struct EraProof {
    previous_era: u32,
    target_era: u32,
    previous_first_epoch: u32,
    first_epoch: u32,
    previous_first_slot: u64,
    boundary_slot: u64,
    previous_fork: String,
    target_fork: String,
    previous_ruleset: String,
    target_ruleset: String,
}

fn era_proof(
    deployment: &DeploymentSettings,
    requested: EraBoundarySchedule,
) -> Result<EraProof, StepError> {
    let eras = deployment.era_schedule().iter().collect::<Vec<_>>();
    let previous_era = requested.previous_era();
    let target_era = requested.target_era();
    let activation_epoch = requested.activation_epoch();
    if previous_era.checked_add(1) != Some(target_era) {
        return Err(failure(format!(
            "Era {target_era} does not immediately follow era {previous_era}"
        )));
    }
    let previous_index = usize::try_from(previous_era)
        .map_err(|error| failure(format!("Invalid previous era {previous_era}: {error}")))?;
    let target_index = usize::try_from(target_era)
        .map_err(|error| failure(format!("Invalid target era {target_era}: {error}")))?;
    let expected_era_count = target_index
        .checked_add(1)
        .ok_or_else(|| failure("Expected era count overflows usize"))?;
    if eras.len() != expected_era_count {
        return Err(failure(format!(
            "Expected {expected_era_count} eras through era {target_era}; found {}",
            eras.len(),
        )));
    }
    let previous = eras
        .get(previous_index)
        .ok_or_else(|| failure(format!("Deployment is missing era {previous_era}")))?;
    let target = eras
        .get(target_index)
        .ok_or_else(|| failure(format!("Deployment is missing era {target_era}")))?;
    if eras.windows(2).any(|pair| {
        pair[0].first_epoch.into_inner() >= pair[1].first_epoch.into_inner()
            || pair[0].first_slot.into_inner() >= pair[1].first_slot.into_inner()
    }) {
        return Err(failure(
            "Deployment era boundaries are not strictly increasing",
        ));
    }
    let previous_first_epoch = previous.first_epoch.into_inner();
    let actual_epoch = target.first_epoch.into_inner();
    if actual_epoch != activation_epoch || previous_first_epoch >= actual_epoch {
        return Err(failure(format!(
            "Wrong schedule for era {previous_era} to era {target_era}: expected target epoch {activation_epoch} after previous epoch {previous_first_epoch}; got epochs {previous_first_epoch} and {actual_epoch}"
        )));
    }
    let boundary_slot = target.first_slot.into_inner();
    let previous_first_slot = previous.first_slot.into_inner();
    if boundary_slot <= previous_first_slot {
        return Err(failure(format!(
            "Era {target_era} begins at slot {boundary_slot}, not after era {previous_era} at slot {previous_first_slot}"
        )));
    }
    let previous_ruleset = serde_yaml::to_string(&previous.parameters.ruleset)
        .map_err(|error| failure(format!("Cannot encode era {previous_era}: {error}")))?;
    let target_ruleset = serde_yaml::to_string(&target.parameters.ruleset)
        .map_err(|error| failure(format!("Cannot encode era {target_era}: {error}")))?;
    Ok(EraProof {
        previous_era,
        target_era,
        previous_first_epoch,
        first_epoch: actual_epoch,
        previous_first_slot,
        boundary_slot,
        previous_fork: format!("{:?}", previous.parameters.fork_digest),
        target_fork: format!("{:?}", target.parameters.fork_digest),
        previous_ruleset,
        target_ruleset,
    })
}

fn proof_for_node(world: &CucumberWorld, node: &str) -> Result<EraProof, StepError> {
    let schedule = world
        .startup
        .era_boundary_schedule
        .ok_or_else(|| failure("No era-boundary test schedule configured"))?;
    era_proof(&deployment_for_node(world, node)?, schedule)
}

fn record(world: &CucumberWorld, event: &str, evidence: &serde_json::Value) -> StepResult {
    let path = world.lifecycle.scenario_base_dir.join(EVIDENCE_FILE);
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .map_err(|error| failure(format!("Cannot open {}: {error}", path.display())))?;
    let entry = serde_json::json!({
        "event": event,
        "recorded_at": time::OffsetDateTime::now_utc().to_string(),
        "evidence": evidence,
    });
    writeln!(file, "{entry}")
        .map_err(|error| failure(format!("Cannot write {}: {error}", path.display())))?;
    Ok(())
}

#[given(
    expr = "the cluster uses the accelerated era schedule from era {int} to era {int} starting at epoch {int}"
)]
fn configure_era_schedule(
    world: &mut CucumberWorld,
    previous_era: u32,
    target_era: u32,
    first_epoch: u32,
) -> StepResult {
    let schedule =
        EraBoundarySchedule::new(previous_era, target_era, first_epoch).map_err(failure)?;
    if world.cluster.manual_cluster_spec.is_some() || !world.nodes_info.is_empty() {
        return Err(failure(
            "Configure the era schedule before building the cluster",
        ));
    }
    world.startup.era_boundary_schedule = Some(schedule);
    Ok(())
}

#[then(expr = "the running nodes use the same effective deployment")]
#[expect(
    clippy::needless_pass_by_ref_mut,
    reason = "Cucumber step arguments require &mut World"
)]
async fn verify_era_deployment(world: &mut CucumberWorld) -> StepResult {
    let mut names = world.nodes_info.keys().cloned().collect::<Vec<_>>();
    names.sort();
    if names.is_empty() {
        return Err(failure("No nodes are running"));
    }
    let mut original: Option<String> = None;
    for name in &names {
        let deployment = deployment_for_node(world, name)?;
        let proof = proof_for_node(world, name)?;
        let yaml = serde_yaml::to_string(&deployment)
            .map_err(|error| failure(format!("Cannot re-encode deployment: {error}")))?;
        if let Some(reference) = &original {
            if reference != &yaml {
                return Err(failure(format!(
                    "Node `{name}` has a different era deployment"
                )));
            }
        } else {
            original = Some(yaml);
        }
        let time = world.resolve_node_http_client(name)?.time_info().await?;
        let expected_epochs = deployment
            .era_schedule()
            .iter()
            .map(|era| era.first_epoch.into_inner())
            .collect::<BTreeSet<_>>();
        let observed_epochs = time.era_timings.keys().copied().collect::<BTreeSet<_>>();
        if observed_epochs != expected_epochs {
            return Err(failure(format!(
                "Node `{name}` time service reports epochs {observed_epochs:?}, expected deployment epochs {expected_epochs:?}"
            )));
        }
        info!(target: TARGET, node = %name, ?proof, "Verified live era schedule");
        record(
            world,
            "effective_era_schedule",
            &serde_json::json!({
                "node": name,
                "previous_first_epoch": proof.previous_first_epoch,
                "first_epoch": proof.first_epoch,
                "previous_first_slot": proof.previous_first_slot,
                "boundary_slot": proof.boundary_slot,
                "previous_era": proof.previous_era,
                "target_era": proof.target_era,
                "previous_fork": proof.previous_fork,
                "target_fork": proof.target_fork,
                "current_epoch": time.current_epoch,
                "current_slot": time.current_slot,
            }),
        )?;
    }
    Ok(())
}

#[then(expr = "the source and target eras have the same ruleset and distinct fork digests")]
#[expect(
    clippy::needless_pass_by_ref_mut,
    reason = "Cucumber step arguments require &mut World"
)]
fn verify_era_transition_properties(world: &mut CucumberWorld) -> StepResult {
    let names = world.nodes_info.keys().cloned().collect::<Vec<_>>();
    if names.is_empty() {
        return Err(failure("No nodes are running"));
    }
    for name in names {
        let proof = proof_for_node(world, &name)?;
        if proof.previous_ruleset != proof.target_ruleset {
            return Err(failure(format!(
                "Source era {} and target era {} have different rulesets",
                proof.previous_era, proof.target_era
            )));
        }
        if proof.previous_fork == proof.target_fork {
            return Err(failure(format!(
                "Source era {} and target era {} have identical fork digests",
                proof.previous_era, proof.target_era
            )));
        }
        record(
            world,
            "era_transition_properties",
            &serde_json::json!({
                "node": name,
                "previous_era": proof.previous_era,
                "target_era": proof.target_era,
                "previous_fork": proof.previous_fork,
                "target_fork": proof.target_fork,
            }),
        )?;
    }
    Ok(())
}

#[when(expr = "I capture a non-genesis finalized block before the era boundary on node {string}")]
async fn capture_pre_boundary_lib(world: &mut CucumberWorld, name: String) -> StepResult {
    let proof = proof_for_node(world, &name)?;
    let source_era = proof.previous_era;
    let target_era = proof.target_era;
    let client = world.resolve_node_http_client(&name)?;
    let time = client.time_info().await?;
    if time.current_slot >= proof.boundary_slot {
        return Err(failure(format!(
            "Cannot capture era {source_era} finality before era {target_era}: node `{name}` is at slot {}, boundary {}",
            time.current_slot, proof.boundary_slot
        )));
    }
    let consensus = client.consensus_info().await?.cryptarchia_info;
    let anchor = consensus.lib;
    let block = client.block(&anchor).await?.ok_or_else(|| {
        failure(format!(
            "Node `{name}` does not serve its finalized block {anchor:?}"
        ))
    })?;
    let slot = block.header.slot.into_inner();
    if slot == 0 || slot < proof.previous_first_slot || slot >= proof.boundary_slot {
        return Err(failure(format!(
            "Expected a non-genesis finalized era {source_era} block in slots {}..{} before era {target_era}, got slot {slot}",
            proof.previous_first_slot, proof.boundary_slot,
        )));
    }
    world.chain.era_boundary_finalized_anchor = Some(anchor);
    record(
        world,
        "pre_boundary_finalized_anchor",
        &serde_json::json!({
            "node": name, "source_era": source_era, "target_era": target_era,
            "block": format!("{anchor:?}"), "slot": slot,
            "observed_clock_slot": time.current_slot,
        }),
    )?;
    Ok(())
}

#[when(expr = "all nodes enter the target era within {int} seconds")]
#[expect(
    clippy::needless_pass_by_ref_mut,
    reason = "Cucumber step arguments require &mut World"
)]
async fn wait_all_nodes_enter_era(world: &mut CucumberWorld, seconds: u64) -> StepResult {
    let names = world.nodes_info.keys().cloned().collect::<Vec<_>>();
    if names.is_empty() {
        return Err(failure("No running nodes"));
    }
    let proof = proof_for_node(world, &names[0])?;
    let target_era = proof.target_era;
    let boundary = proof.boundary_slot;
    let deadline = Instant::now() + Duration::from_secs(seconds);
    loop {
        let mut ready = true;
        for name in &names {
            match world.resolve_node_http_client(name)?.time_info().await {
                Ok(time)
                    if time.current_slot >= boundary && time.current_epoch >= proof.first_epoch => {
                }
                Ok(_) | Err(_) => ready = false,
            }
        }
        if ready {
            for name in &names {
                let time = world.resolve_node_http_client(name)?.time_info().await?;
                record(
                    world,
                    "observed_era_time",
                    &serde_json::json!({
                        "node": name, "target_era": target_era,
                        "current_epoch": time.current_epoch,
                        "current_slot": time.current_slot, "boundary_slot": boundary,
                    }),
                )?;
            }
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(failure(format!(
                "Nodes did not reach era {target_era} by slot {boundary} and epoch {} within {seconds}s",
                proof.first_epoch,
            )));
        }
        sleep(Duration::from_secs(1)).await;
    }
}

#[then(expr = "all nodes report the target-era fork digest within {int} seconds")]
#[expect(
    clippy::needless_pass_by_ref_mut,
    reason = "Cucumber step arguments require &mut World"
)]
async fn verify_target_era_fork_digest(world: &mut CucumberWorld, seconds: u64) -> StepResult {
    let mut names = world.nodes_info.keys().cloned().collect::<Vec<_>>();
    names.sort();
    if names.is_empty() {
        return Err(failure("No running nodes"));
    }
    let deadline = Instant::now() + Duration::from_secs(seconds);
    let mut last_observations = HashMap::new();
    let mut observed_fork_digests = HashMap::new();
    loop {
        let mut all_target = true;
        for name in &names {
            let proof = proof_for_node(world, name)?;
            match world.resolve_node_http_client(name)?.chain_fork().await {
                Ok(fork) if format!("{fork:?}") == proof.target_fork => {
                    last_observations.remove(name);
                    observed_fork_digests.insert(name.clone(), format!("{fork:?}"));
                }
                Ok(fork) => {
                    all_target = false;
                    observed_fork_digests.remove(name);
                    last_observations.insert(
                        name.clone(),
                        format!(
                            "active fork digest {fork:?}, expected {}",
                            proof.target_fork
                        ),
                    );
                }
                Err(error) => {
                    all_target = false;
                    observed_fork_digests.remove(name);
                    last_observations
                        .insert(name.clone(), format!("chain fork request failed: {error}"));
                }
            }
        }
        if all_target {
            for name in &names {
                let proof = proof_for_node(world, name)?;
                let fork_digest = observed_fork_digests.get(name).ok_or_else(|| {
                    failure(format!(
                        "No successful fork digest observation for `{name}`"
                    ))
                })?;
                record(
                    world,
                    "target_era_fork_digest_observed",
                    &serde_json::json!({
                        "node": name,
                        "target_era": proof.target_era,
                        "fork_digest": fork_digest,
                        "deployment_fork_digest": proof.target_fork,
                        "source": "node_chain_fork_http_api",
                    }),
                )?;
            }
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(failure(polling_timeout_message(
                &format!("Not all nodes report the target-era fork digest within {seconds}s"),
                &names,
                &last_observations,
            )));
        }
        sleep(Duration::from_secs(1)).await;
    }
}

#[when(expr = "I prepare node {string} to join with fresh state from node {string}")]
async fn prepare_fresh_late_joiner(
    world: &mut CucumberWorld,
    joiner: String,
    source: String,
) -> StepResult {
    if world.nodes_info.contains_key(&joiner) {
        return Err(failure(format!("Node `{joiner}` has already started")));
    }
    if world.snapshots.node_snapshot_on_startup.is_some() {
        return Err(failure(format!(
            "Node `{joiner}` cannot be proven fresh while a startup snapshot is configured"
        )));
    }
    let anchor = world
        .chain
        .era_boundary_finalized_anchor
        .ok_or_else(|| failure("No pre-boundary finalized anchor captured"))?;
    let joiner_prefix = format!("{joiner}_");
    let existing_runtime_dirs = fs::read_dir(&world.lifecycle.scenario_base_dir)
        .map_err(|error| {
            failure(format!(
                "Cannot inspect scenario runtime directory {}: {error}",
                world.lifecycle.scenario_base_dir.display()
            ))
        })?
        .filter_map(Result::ok)
        .filter(|entry| entry.file_type().is_ok_and(|kind| kind.is_dir()))
        .map(|entry| entry.file_name().to_string_lossy().into_owned())
        .filter(|name| name.starts_with(&joiner_prefix))
        .collect::<Vec<_>>();

    let source_proof = proof_for_node(world, &source)?;
    let source_client = world.resolve_node_http_client(&source)?;
    let source_block = source_client.block(&anchor).await?.ok_or_else(|| {
        failure(format!(
            "Source node `{source}` no longer serves captured finalized block {anchor:?} before `{joiner}` starts"
        ))
    })?;
    let source_slot = source_block.header.slot.into_inner();
    if source_slot < source_proof.previous_first_slot || source_slot >= source_proof.boundary_slot {
        return Err(failure(format!(
            "Captured block {anchor:?} on `{source}` is not in source era {}: slot {source_slot}",
            source_proof.previous_era
        )));
    }

    world.chain.era_boundary_fresh_late_joiner = Some(joiner.clone());
    world
        .chain
        .era_boundary_late_joiner_prior_runtime_dirs
        .clone_from(&existing_runtime_dirs);
    record(
        world,
        "late_joiner_fresh_state_before_start",
        &serde_json::json!({
            "node": joiner,
            "source_node": source,
            "source_block_id": format!("{anchor:?}"),
            "source_block_slot": source_slot,
            "source_era": source_proof.previous_era,
            "startup_snapshot_configured": false,
            "prior_runtime_directories": existing_runtime_dirs,
            "source_node_served_block_before_joiner_start": true,
            "source": "node_block_http_api_and_test_framework_runtime_state",
        }),
    )
}

#[then(expr = "node {string} started in the target era")]
#[expect(
    clippy::needless_pass_by_ref_mut,
    reason = "Cucumber step arguments require &mut World"
)]
async fn verify_late_join_era(world: &mut CucumberWorld, name: String) -> StepResult {
    if world.chain.era_boundary_fresh_late_joiner.as_deref() != Some(name.as_str()) {
        return Err(failure(format!(
            "Node `{name}` was not recorded as a fresh late joiner before startup"
        )));
    }
    let proof = proof_for_node(world, &name)?;
    let target_era = proof.target_era;
    let node = world
        .nodes_info
        .get(&name)
        .ok_or_else(|| failure(format!("Node `{name}` has not started")))?;
    let runtime_dir_name = node
        .runtime_dir
        .file_name()
        .and_then(|value| value.to_str())
        .ok_or_else(|| {
            failure(format!(
                "Node `{name}` runtime directory has no valid final path component: {}",
                node.runtime_dir.display()
            ))
        })?;
    if node.runtime_dir.parent() != Some(world.lifecycle.scenario_base_dir.as_path())
        || !runtime_dir_name.starts_with(&format!("{name}_"))
    {
        return Err(failure(format!(
            "Node `{name}` runtime directory {} is outside its fresh scenario runtime location",
            node.runtime_dir.display()
        )));
    }
    if world
        .chain
        .era_boundary_late_joiner_prior_runtime_dirs
        .iter()
        .any(|existing| existing == runtime_dir_name)
    {
        return Err(failure(format!(
            "Node `{name}` reused a runtime directory that existed before startup: {runtime_dir_name}"
        )));
    }
    if world.snapshots.node_snapshot_on_startup.is_some() {
        return Err(failure(format!(
            "Node `{name}` started with a configured chain-state snapshot"
        )));
    }
    let time = world.resolve_node_http_client(&name)?.time_info().await?;
    if time.current_slot < proof.boundary_slot || time.current_epoch < proof.first_epoch {
        return Err(failure(format!(
            "Node `{name}` is not in era {target_era}: slot {}, epoch {}",
            time.current_slot, time.current_epoch
        )));
    }
    record(
        world,
        "late_joiner_current_era",
        &serde_json::json!({
            "node": name, "target_era": target_era,
            "current_slot": time.current_slot,
            "current_epoch": time.current_epoch,
            "fresh_state_verified_before_start": true,
            "runtime_directory": runtime_dir_name,
            "runtime_directory_created_after_preparation": true,
            "prior_runtime_directories": world.chain.era_boundary_late_joiner_prior_runtime_dirs,
            "startup_snapshot_configured": false,
        }),
    )
}

#[derive(Debug, Clone)]
struct CanonicalHistory {
    previous_era_blocks: usize,
    target_era_blocks: usize,
    includes_anchor: bool,
    anchor_slot: Option<u64>,
}

fn validate_source_era_anchor(
    history: &CanonicalHistory,
    proof: &EraProof,
    anchor: HeaderId,
) -> StepResult {
    if !history.includes_anchor {
        return Ok(());
    }
    let Some(slot) = history.anchor_slot else {
        return Err(failure(format!(
            "Captured block {anchor:?} was found in ancestry without a slot"
        )));
    };
    if slot == 0 || slot < proof.previous_first_slot || slot >= proof.boundary_slot {
        return Err(failure(format!(
            "Captured block {anchor:?} has slot {slot}, outside source era {} slots {}..{}",
            proof.previous_era, proof.previous_first_slot, proof.boundary_slot
        )));
    }
    Ok(())
}

async fn inspect_history(
    client: &NodeHttpClient,
    tip: HeaderId,
    genesis: HeaderId,
    previous_era_first_slot: u64,
    target_era_first_slot: u64,
    anchor: Option<HeaderId>,
    stop_after_counts: Option<(usize, usize)>,
) -> Result<CanonicalHistory, HistoryInspectionError> {
    let mut current = tip;
    let mut child_slot = None;
    let mut visited = HashSet::new();
    let mut result = CanonicalHistory {
        previous_era_blocks: 0,
        target_era_blocks: 0,
        includes_anchor: false,
        anchor_slot: None,
    };
    for _ in 0..MAX_ANCESTOR_WALK {
        if current == genesis {
            result.includes_anchor = anchor == Some(current);
            result.anchor_slot = result.includes_anchor.then_some(0);
            return Ok(result);
        }
        if !visited.insert(current) {
            return Err(HistoryInspectionError::immediate(format!(
                "Canonical ancestry contains a cycle at block {current:?}"
            )));
        }
        let block = match client.block(&current).await {
            Ok(Some(block)) => block,
            Ok(None) => {
                return Err(HistoryInspectionError::retryable(format!(
                    "Canonical ancestor {current:?} is not available yet (HTTP 404)"
                )));
            }
            Err(error) => {
                let message = format!("Failed to retrieve canonical ancestor {current:?}: {error}");
                return Err(if transient_block_fetch_error(&error) {
                    HistoryInspectionError::retryable(message)
                } else {
                    HistoryInspectionError::immediate(message)
                });
            }
        };
        let slot = block.header.slot.into_inner();
        if let Some(child_slot) = child_slot
            && slot > child_slot
        {
            return Err(HistoryInspectionError::immediate(format!(
                "Invalid canonical ancestry: parent {current:?} at slot {slot} follows its child at slot {child_slot}"
            )));
        }
        if slot >= target_era_first_slot {
            result.target_era_blocks += 1;
        } else if slot > 0 && slot >= previous_era_first_slot {
            result.previous_era_blocks += 1;
        }
        if anchor == Some(current) {
            result.includes_anchor = true;
            result.anchor_slot = Some(slot);
            return Ok(result);
        }
        if let Some((minimum_previous, minimum_target)) = stop_after_counts
            && result.previous_era_blocks >= minimum_previous
            && result.target_era_blocks >= minimum_target
        {
            return Ok(result);
        }
        if slot == 0 {
            return Ok(result);
        }
        if slot < previous_era_first_slot {
            return Ok(result);
        }
        current = block.header.parent_block;
        child_slot = Some(slot);
    }
    Err(HistoryInspectionError::immediate(format!(
        "Canonical ancestry walk exceeded its internal safety limit of {MAX_ANCESTOR_WALK} blocks"
    )))
}

#[then(
    expr = "all nodes have at least {int} canonical target-era blocks and at least {int} canonical source-era blocks within {int} seconds"
)]
#[expect(
    clippy::needless_pass_by_ref_mut,
    reason = "Cucumber step arguments require &mut World"
)]
async fn wait_for_era_blocks(
    world: &mut CucumberWorld,
    minimum_target: usize,
    minimum_previous: usize,
    seconds: u64,
) -> StepResult {
    let names = world.nodes_info.keys().cloned().collect::<Vec<_>>();
    if names.is_empty() {
        return Err(failure("No running nodes"));
    }
    let deadline = Instant::now() + Duration::from_secs(seconds);
    let mut last_observations = HashMap::new();
    let genesis = world
        .chain
        .genesis_block_id
        .ok_or_else(|| failure("Genesis block id not recorded"))?;
    loop {
        let mut complete = true;
        for name in &names {
            let proof = proof_for_node(world, name)?;
            let client = world.resolve_node_http_client(name)?;
            match client.consensus_info().await {
                Ok(consensus) => {
                    let history = inspect_history(
                        &client,
                        consensus.cryptarchia_info.tip,
                        genesis,
                        proof.previous_first_slot,
                        proof.boundary_slot,
                        None,
                        Some((minimum_previous, minimum_target)),
                    )
                    .await;
                    match history {
                        Ok(history)
                            if history.target_era_blocks >= minimum_target
                                && history.previous_era_blocks >= minimum_previous =>
                        {
                            last_observations.remove(name);
                            record(
                                world,
                                "canonical_cross_era_history",
                                &serde_json::json!({
                                    "node": name, "previous_era": proof.previous_era,
                                    "target_era": proof.target_era,
                                    "minimum_previous_era_blocks": minimum_previous,
                                    "minimum_target_era_blocks": minimum_target,
                                    "previous_era_count": history.previous_era_blocks,
                                    "target_era_count": history.target_era_blocks,
                                    "previous_era_first_slot": proof.previous_first_slot,
                                    "boundary_slot": proof.boundary_slot,
                                }),
                            )?;
                        }
                        Ok(history) => {
                            complete = false;
                            last_observations.insert(
                                name.clone(),
                                format!(
                                    "tip {:?} has {} target-era and {} source-era blocks; requires {minimum_target} and {minimum_previous}",
                                    consensus.cryptarchia_info.tip,
                                    history.target_era_blocks,
                                    history.previous_era_blocks,
                                ),
                            );
                        }
                        Err(error) => {
                            observe_history_error(name, error, &mut last_observations)?;
                            complete = false;
                        }
                    }
                }
                Err(error) => {
                    last_observations.insert(
                        name.clone(),
                        format!("Consensus info request failed: {error}"),
                    );
                    complete = false;
                }
            }
        }
        if complete {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(failure(polling_timeout_message(
                &format!(
                    "Not all canonical chains contain at least {minimum_target} target-era blocks and {minimum_previous} source-era blocks within {seconds}s"
                ),
                &names,
                &last_observations,
            )));
        }
        sleep(Duration::from_secs(2)).await;
    }
}

#[then(
    expr = "node {string} acquires the captured source-era finalized block in canonical history and all nodes preserve it on their LIB ancestry within {int} seconds"
)]
#[expect(
    clippy::needless_pass_by_ref_mut,
    reason = "Cucumber step arguments require &mut World"
)]
#[expect(
    clippy::too_many_lines,
    reason = "Keeps late-join canonical acquisition and LIB continuity in one bounded poll"
)]
async fn verify_late_joiner_history_and_finality(
    world: &mut CucumberWorld,
    late_joiner: String,
    seconds: u64,
) -> StepResult {
    if world.chain.era_boundary_fresh_late_joiner.as_deref() != Some(late_joiner.as_str()) {
        return Err(failure(format!(
            "Node `{late_joiner}` was not prepared as a fresh late joiner before startup"
        )));
    }
    let anchor = world
        .chain
        .era_boundary_finalized_anchor
        .ok_or_else(|| failure("No pre-boundary finalized anchor captured"))?;
    let mut names = world.nodes_info.keys().cloned().collect::<Vec<_>>();
    names.sort();
    if !names.iter().any(|name| name == &late_joiner) {
        return Err(failure(format!("No running node `{late_joiner}`")));
    }
    let genesis = world
        .chain
        .genesis_block_id
        .ok_or_else(|| failure("Genesis block id not recorded"))?;
    let deadline = Instant::now() + Duration::from_secs(seconds);
    let mut last_observations = HashMap::new();

    loop {
        let mut all_preserved = true;
        let mut canonical_late_history = None;
        let mut finalized_histories = HashMap::new();

        for name in &names {
            let proof = proof_for_node(world, name)?;
            let client = world.resolve_node_http_client(name)?;
            let mut late_canonical_acquired = name != &late_joiner;
            let consensus = match client.consensus_info().await {
                Ok(consensus) => consensus,
                Err(error) => {
                    all_preserved = false;
                    last_observations.insert(
                        name.clone(),
                        format!("Consensus info request failed: {error}"),
                    );
                    continue;
                }
            };

            if name == &late_joiner {
                match inspect_history(
                    &client,
                    consensus.cryptarchia_info.tip,
                    genesis,
                    proof.previous_first_slot,
                    proof.boundary_slot,
                    Some(anchor),
                    None,
                )
                .await
                {
                    Ok(history) => {
                        validate_source_era_anchor(&history, &proof, anchor)?;
                        if history.includes_anchor {
                            canonical_late_history = Some((proof.clone(), history));
                            late_canonical_acquired = true;
                        } else {
                            all_preserved = false;
                            last_observations.insert(
                                name.clone(),
                                format!(
                                    "canonical tip {:?} has not acquired captured block {anchor:?}; observed {} target-era and {} source-era blocks",
                                    consensus.cryptarchia_info.tip,
                                    history.target_era_blocks,
                                    history.previous_era_blocks,
                                ),
                            );
                        }
                    }
                    Err(error) => {
                        observe_history_error(name, error, &mut last_observations)?;
                        all_preserved = false;
                    }
                }
            }

            match inspect_history(
                &client,
                consensus.cryptarchia_info.lib,
                genesis,
                proof.previous_first_slot,
                proof.boundary_slot,
                Some(anchor),
                None,
            )
            .await
            {
                Ok(history) => {
                    validate_source_era_anchor(&history, &proof, anchor)?;
                    if history.includes_anchor {
                        if late_canonical_acquired {
                            last_observations.remove(name);
                        }
                        finalized_histories.insert(
                            name.clone(),
                            (proof, consensus.cryptarchia_info.lib, history),
                        );
                    } else {
                        all_preserved = false;
                        last_observations.insert(
                            name.clone(),
                            format!(
                                "LIB {:?} ancestry has not reached finalized anchor {anchor:?}; observed {} target-era and {} source-era blocks",
                                consensus.cryptarchia_info.lib,
                                history.target_era_blocks,
                                history.previous_era_blocks,
                            ),
                        );
                    }
                }
                Err(error) => {
                    observe_history_error(name, error, &mut last_observations)?;
                    all_preserved = false;
                }
            }
        }

        if all_preserved && let Some((late_proof, canonical_history)) = canonical_late_history {
            record(
                world,
                "late_joiner_canonical_source_history_acquired",
                &serde_json::json!({
                    "node": late_joiner,
                    "source_era": late_proof.previous_era,
                    "target_era": late_proof.target_era,
                    "block_id": format!("{anchor:?}"),
                    "block_slot": canonical_history.anchor_slot,
                    "canonical_history_contains_block": canonical_history.includes_anchor,
                    "canonical_tip_previous_era_blocks": canonical_history.previous_era_blocks,
                    "canonical_tip_target_era_blocks": canonical_history.target_era_blocks,
                    "fresh_state_verified_before_start": true,
                    "source_node_served_block_before_joiner_start": true,
                    "source": "node_block_and_consensus_http_apis",
                }),
            )?;
            for (name, (proof, lib, history)) in finalized_histories {
                record(
                    world,
                    "finality_continuity",
                    &serde_json::json!({
                        "node": name,
                        "source_era": proof.previous_era,
                        "target_era": proof.target_era,
                        "original_lib": format!("{anchor:?}"),
                        "current_lib": format!("{lib:?}"),
                        "anchor_slot": history.anchor_slot,
                        "lib_ancestry_contains_anchor": history.includes_anchor,
                        "previous_era_blocks": history.previous_era_blocks,
                        "target_era_blocks": history.target_era_blocks,
                        "source": "node_block_and_consensus_http_apis",
                    }),
                )?;
            }
            return Ok(());
        }

        if Instant::now() >= deadline {
            return Err(failure(polling_timeout_message(
                &format!(
                    "Node `{late_joiner}` did not acquire block {anchor:?} in canonical history and all nodes did not preserve it on their LIB ancestry within {seconds}s"
                ),
                &names,
                &last_observations,
            )));
        }
        sleep(Duration::from_secs(2)).await;
    }
}

#[then(
    expr = "all nodes preserve the captured finalized block on their LIB ancestry after the era boundary within {int} seconds"
)]
#[expect(
    clippy::needless_pass_by_ref_mut,
    reason = "Cucumber step arguments require &mut World"
)]
async fn preserve_finality(world: &mut CucumberWorld, seconds: u64) -> StepResult {
    let anchor = world
        .chain
        .era_boundary_finalized_anchor
        .ok_or_else(|| failure("No pre-boundary finalized anchor captured"))?;
    let names = world.nodes_info.keys().cloned().collect::<Vec<_>>();
    if names.is_empty() {
        return Err(failure("No running nodes"));
    }
    let deadline = Instant::now() + Duration::from_secs(seconds);
    let mut last_observations = HashMap::new();
    let genesis = world
        .chain
        .genesis_block_id
        .ok_or_else(|| failure("Genesis block id not recorded"))?;
    loop {
        let mut all_preserved = true;
        for name in &names {
            let client = world.resolve_node_http_client(name)?;
            match client.consensus_info().await {
                Ok(consensus) => {
                    let proof = proof_for_node(world, name)?;
                    let history = inspect_history(
                        &client,
                        consensus.cryptarchia_info.lib,
                        genesis,
                        proof.previous_first_slot,
                        proof.boundary_slot,
                        Some(anchor),
                        None,
                    )
                    .await;
                    match history {
                        Ok(history) if history.includes_anchor => {
                            last_observations.remove(name);
                            record(
                                world,
                                "finality_continuity",
                                &serde_json::json!({
                                    "source_era": proof.previous_era,
                                    "target_era": proof.target_era,
                                    "previous_era_blocks": history.previous_era_blocks,
                                    "node": name, "original_lib": format!("{anchor:?}"),
                                    "current_lib": format!("{:?}", consensus.cryptarchia_info.lib),
                                }),
                            )?;
                        }
                        Ok(history) => {
                            all_preserved = false;
                            last_observations.insert(
                                name.clone(),
                                format!(
                                    "LIB {:?} ancestry has not reached finalized anchor {anchor:?}; observed {} target-era and {} source-era blocks",
                                    consensus.cryptarchia_info.lib,
                                    history.target_era_blocks,
                                    history.previous_era_blocks,
                                ),
                            );
                        }
                        Err(error) => {
                            observe_history_error(name, error, &mut last_observations)?;
                            all_preserved = false;
                        }
                    }
                }
                Err(error) => {
                    last_observations.insert(
                        name.clone(),
                        format!("Consensus info request failed: {error}"),
                    );
                    all_preserved = false;
                }
            }
        }
        if all_preserved {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(failure(polling_timeout_message(
                &format!(
                    "Not all nodes preserve the captured finalized block on their LIB ancestry after the era boundary within {seconds}s"
                ),
                &names,
                &last_observations,
            )));
        }
        sleep(Duration::from_secs(2)).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn transient_block_fetch_statuses_are_retryable_only_for_transient_responses() {
        for status in [
            StatusCode::REQUEST_TIMEOUT,
            StatusCode::TOO_MANY_REQUESTS,
            StatusCode::SERVICE_UNAVAILABLE,
        ] {
            assert!(transient_http_status(status), "{status} should be retried");
            assert!(transient_block_fetch_error(&HttpClientError::HttpStatus {
                status,
                body: "temporary failure".to_owned(),
            }));
        }

        assert!(!transient_http_status(StatusCode::BAD_REQUEST));
        assert!(!transient_block_fetch_error(&HttpClientError::HttpStatus {
            status: StatusCode::BAD_REQUEST,
            body: "invalid request".to_owned(),
        }));
    }

    #[test]
    fn polling_retains_transient_errors_and_fails_immediately_on_ancestry_violations() {
        let mut observations = HashMap::new();
        observe_history_error(
            "NODE_2",
            HistoryInspectionError::retryable("ancestor block is temporarily unavailable"),
            &mut observations,
        )
        .expect("retryable retrieval errors should continue polling");
        assert_eq!(
            observations.get("NODE_2").map(String::as_str),
            Some("ancestor block is temporarily unavailable")
        );

        let error = observe_history_error(
            "NODE_2",
            HistoryInspectionError::immediate("ancestry cycle detected"),
            &mut observations,
        )
        .expect_err("invariant violations should fail immediately");
        assert!(error.to_string().contains("ancestry cycle detected"));

        let timeout = polling_timeout_message(
            "history deadline elapsed",
            &["NODE_2".to_owned()],
            &observations,
        );
        assert!(timeout.contains("NODE_2"));
        assert!(timeout.contains("ancestor block is temporarily unavailable"));
    }
}
