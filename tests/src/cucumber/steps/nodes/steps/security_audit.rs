use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    fs::{self, File},
    io::{BufRead as _, BufReader, Seek as _, SeekFrom},
    path::{Path, PathBuf},
    process::Command,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use cucumber::then;
use futures_util::future::join_all;
use hex::ToHex as _;
use lb_common_http_client::ApiBlock;
use lb_core::{header::HeaderId, mantle::traits::Hashable as _};
use lb_testing_framework::{NodeHttpClient, USER_CONFIG_FILE};
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
        world::CucumberWorld,
    },
};

const PRODUCER: &str = "NODE_1";
const HONEST_NODES: [&str; 3] = ["NODE_2", "NODE_3", "NODE_4"];
const SIBLING_COUNT: usize = 4;
const READINESS_TIMEOUT: Duration = Duration::from_secs(60);
const LEADERSHIP_TIMEOUT: Duration = Duration::from_secs(300);
const RECEIVER_TIMEOUT: Duration = Duration::from_secs(30);
const NONEMPTY_TRANSACTION_BUDGET: usize = 16;
const NONEMPTY_TRANSACTION_WINDOW: Duration = Duration::from_secs(180);
const TRANSACTION_PENDING_TIMEOUT: Duration = Duration::from_secs(6);
const NONEMPTY_TRANSACTION_AMOUNT: u64 = 10;
const TRANSACTION_SENDER: &str = "SECURITY_AUDIT_SENDER";
const TRANSACTION_RECEIVER: &str = "SECURITY_AUDIT_RECEIVER";
const FINALITY_OBSERVATION_TIMEOUT: Duration = Duration::from_secs(600);
const FINALITY_OBSERVATION_INTERVAL: Duration = Duration::from_secs(2);
const PRODUCER_PROMPT_STOP_LIMIT: Duration = Duration::from_secs(30);
const FINALITY_STABILITY_WINDOW: Duration = Duration::from_secs(10);
const FINALITY_MAX_ANCESTRY_DEPTH: usize = 100_000;
const FINALITY_MAX_TIP_HEIGHT_SKEW: u64 = 1;

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
    transaction_hashes: Vec<String>,
}

struct AuditFailure {
    message: String,
    details: Value,
}

struct AuditLogTail {
    root: PathBuf,
    runtime_names: BTreeMap<String, String>,
    paths: BTreeMap<String, Vec<PathBuf>>,
    offsets: HashMap<PathBuf, u64>,
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

struct FinalityNodeObservation {
    snapshot: Value,
    consensus: Option<ConsensusState>,
    tip_path_complete: bool,
    finalized_path_complete: bool,
    attack_branch: AttackBranch,
    finalized_attack_branch: AttackBranch,
    continuity: String,
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

#[then(
    "Blend core peers are ready and NODE_1 produces four valid siblings accepted by an honest node"
)]
#[expect(
    clippy::needless_pass_by_ref_mut,
    reason = "Cucumber step entrypoints must take `&mut World`"
)]
async fn step_verify_sibling_propagation(world: &mut CucumberWorld) -> StepResult {
    let artifact_dir = world
        .lifecycle
        .scenario_base_dir
        .join("security-audit-evidence");
    fs::create_dir_all(&artifact_dir).map_err(|error| StepError::StepFail {
        message: format!("cannot create audit evidence directory: {error}"),
    })?;

    let result = observe_sibling_propagation(world).await;
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

#[then("NODE_1 produces four valid nonempty siblings containing submitted transactions")]
async fn step_verify_nonempty_sibling_propagation(world: &mut CucumberWorld) -> StepResult {
    record_nonempty_sibling_propagation(world, true).await
}

#[then(
    "NODE_1 produces four valid nonempty siblings containing submitted transactions for the selected equivocation opportunity"
)]
async fn step_verify_selected_nonempty_sibling_propagation(
    world: &mut CucumberWorld,
) -> StepResult {
    record_nonempty_sibling_propagation(world, false).await
}

async fn record_nonempty_sibling_propagation(
    world: &mut CucumberWorld,
    require_full_observation_window: bool,
) -> StepResult {
    // The lifecycle step starts the shared wallet scanner when the first
    // honest peers come up, before NODE_1's funded sender wallet is
    // registered. Stop that scanner now; the observation step starts it again
    // after Blend readiness, when all four nodes and wallets are registered.
    world.reset_wallet_scanner_after_current_iteration().await;

    let artifact_dir = world
        .lifecycle
        .scenario_base_dir
        .join("security-audit-nonempty-evidence");
    fs::create_dir_all(&artifact_dir).map_err(|error| StepError::StepFail {
        message: format!("cannot create nonempty audit evidence directory: {error}"),
    })?;

    let result = observe_nonempty_sibling_propagation(world, require_full_observation_window).await;
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
    report["observation_completed_unix_ms"] = json!(unix_timestamp_ms());
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
    let observations = join_all(HONEST_NODES.iter().map(async |node_name| {
        let Some(node) = world.nodes_info.get(*node_name) else {
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
        if link.slot < target_slot || link.parent == current {
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

#[then("the honest nodes converge and finalize beyond the nonempty equivocation")]
#[expect(
    clippy::needless_pass_by_ref_mut,
    reason = "Cucumber step functions require the world as the first `&mut` argument"
)]
async fn step_verify_finality_after_nonempty_equivocation(world: &mut CucumberWorld) -> StepResult {
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

    let report = observe_finality_after_equivocation(world, &opportunity, &evidence).await;
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
            "Honest nodes converged and advanced a common LIB beyond the isolated nonempty equivocation"
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
                continuity: "unverified".to_owned(),
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
            ("verified", None)
        } else {
            let path = walk_to_target(client, lib_id, previous_id, previous_slot, cache).await;
            match ancestry_presence(&path, previous_id) {
                Some(true) => ("verified", Some(path)),
                Some(false) => ("regression_or_unrelated", Some(path)),
                None => ("unverified", Some(path)),
            }
        }
    } else {
        ("missing_previous_lib_baseline", None)
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
        snapshot,
        consensus: Some(state),
        tip_path_complete: tip_path.complete_below_slot,
        finalized_path_complete: finalized_path.complete_below_slot,
        attack_branch,
        finalized_attack_branch,
        continuity: continuity.to_owned(),
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

fn canonical_tips_aligned(height_skew: u64, block_ids_at_common_height: &[HeaderId]) -> bool {
    height_skew <= FINALITY_MAX_TIP_HEIGHT_SKEW
        && block_ids_at_common_height
            .first()
            .is_some_and(|first| block_ids_at_common_height.iter().all(|id| id == first))
}

fn producer_stop_is_prompt(producer_stopped: bool, delay_ms: u64) -> bool {
    producer_stopped
        && delay_ms <= u64::try_from(PRODUCER_PROMPT_STOP_LIMIT.as_millis()).unwrap_or(u64::MAX)
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

fn producer_equivocation_evidence(log: &str) -> Vec<Value> {
    let parsed = log
        .lines()
        .enumerate()
        .map(|(line, text)| (line + 1, text, parse_log_fields(text)))
        .collect::<Vec<_>>();
    parsed
        .iter()
        .filter_map(|(line, _, fields)| {
            if fields.get("event")? != "security_audit_ordinary_submitted"
                || parse_usize(fields, "configured_siblings") != Some(SIBLING_COUNT)
            {
                return None;
            }
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
                "configured_siblings": SIBLING_COUNT,
                "observed_sibling_events": sibling_ids,
            }))
        })
        .collect()
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

#[expect(
    clippy::too_many_lines,
    reason = "Coordinates one bounded post-equivocation fork-choice and finality experiment"
)]
#[expect(
    clippy::cognitive_complexity,
    reason = "Keeps the bounded observation and its failure classifications together"
)]
async fn observe_finality_after_equivocation(
    world: &CucumberWorld,
    opportunity_report: &Value,
    attack: &AttackEvidence,
) -> Result<Value, String> {
    let producer = world
        .nodes_info
        .get(PRODUCER)
        .ok_or_else(|| "NODE_1 was not started".to_owned())?;
    let producer_log = read_runtime_node_logs(&producer.runtime_dir, &producer.runtime_name)?;
    let mut opportunities_by_id = BTreeMap::new();
    for opportunity in producer_equivocation_evidence(&producer_log) {
        if let Some(id) = opportunity["ordinary_block_id"].as_str() {
            opportunities_by_id.insert(id.to_owned(), opportunity);
        }
    }
    let producer_opportunities = opportunities_by_id.into_values().collect::<Vec<_>>();
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
                .is_some_and(|siblings| siblings.len() == SIBLING_COUNT)
            && observed_siblings == expected_siblings
    });
    let observation_completed_unix_ms = opportunity_report["observation_completed_unix_ms"]
        .as_u64()
        .unwrap_or_default();
    let stop_observed_unix_ms = unix_timestamp_ms();
    let stop_upper_bound_delay_ms =
        stop_observed_unix_ms.saturating_sub(observation_completed_unix_ms);
    let producer_stop_confirmation = match producer.client.consensus_info().await {
        Ok(info) => json!({
            "responsive_after_stop_step": true,
            "tip_id": info.cryptarchia_info.tip.encode_hex::<String>(),
            "tip_height": info.cryptarchia_info.height,
            "note": "NODE_1 still answered consensus_info after the stop step",
        }),
        Err(error) => json!({
            "responsive_after_stop_step": false,
            "error": error.to_string(),
            "evidence": "NodeHttpClient::consensus_info failed after I stop node NODE_1",
        }),
    };
    let producer_stopped =
        producer_stop_confirmation["responsive_after_stop_step"].as_bool() == Some(false);
    let baselines = parse_initial_lib_baselines(opportunity_report);
    let mut previous_tips = parse_initial_tip_baselines(opportunity_report);
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

    let start_unix_ms = unix_timestamp_ms();
    let start = Instant::now();
    let deadline = start + FINALITY_OBSERVATION_TIMEOUT;
    let mut header_caches = HONEST_NODES
        .iter()
        .map(|node| ((*node).to_owned(), HeaderCache::default()))
        .collect::<BTreeMap<_, _>>();
    let mut last_verified_libs = baselines.clone();
    let mut snapshots = Vec::new();
    let mut attack_branch_changes = Vec::new();
    let mut previous_branches = BTreeMap::<String, AttackBranch>::new();
    let mut tip_transition_observations = Vec::new();
    let mut tip_level_reorganizations = Vec::new();
    let mut tip_reorganizations_unverified = Vec::new();
    let mut continuity_regressions = Vec::new();
    let mut continuity_unverified = Vec::new();
    let mut ancestry_gaps = Vec::new();
    let mut finalized_history_checks = Vec::new();
    let mut safety_conflict_observed = false;
    let mut missing_finalized_ancestry = false;
    let mut earliest_common_finalized_history: Option<(u64, HeaderId, u64)> = None;
    let mut stable_since_elapsed_ms = None;
    let mut stable_completion = None;
    let (final_nodes, final_alignment, final_progress) = loop {
        let mut observations = Vec::with_capacity(HONEST_NODES.len());
        for node_name in HONEST_NODES {
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
                match observation.continuity.as_str() {
                    "verified" => {
                        last_verified_libs.insert(
                            node_name.to_owned(),
                            (consensus.lib_id, consensus.lib_slot),
                        );
                    }
                    "regression_or_unrelated" => continuity_regressions.push(json!({
                        "node": node_name,
                        "observed_at_unix_ms": observation.snapshot["observed_at_unix_ms"],
                        "current_lib_id": consensus.lib_id_text,
                        "current_lib_slot": consensus.lib_slot,
                        "previous_verified_lib_id": previous_lib.map(|(id, _)| id.encode_hex::<String>()),
                        "ancestry": observation.snapshot["lib_continuity_ancestry"],
                    })),
                    "unverified" | "missing_previous_lib_baseline" => {
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
                    || observation
                        .snapshot
                        .pointer("/tip_descends_from_lib")
                        .and_then(Value::as_bool)
                        != Some(true)
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
                    previous_branches
                        .insert(node_name.to_owned(), observation.attack_branch.clone());
                }

                let new_tip = CanonicalTipState {
                    id: consensus.tip_id,
                    id_text: consensus.tip_id_text.clone(),
                    height: consensus.tip_height,
                    slot: consensus.tip_slot,
                    observed_at_unix_ms: observation.snapshot["observed_at_unix_ms"]
                        .as_u64()
                        .unwrap_or_default(),
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

        let live_states = HONEST_NODES
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
        let alignment = if live_states.len() == HONEST_NODES.len() {
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
            if height_skew > FINALITY_MAX_TIP_HEIGHT_SKEW {
                json!({
                    "aligned": false,
                    "reason": "honest tip heights exceed the existing one-block alignment tolerance",
                    "common_height": min_height,
                    "height_skew": height_skew,
                    "max_allowed_height_skew": FINALITY_MAX_TIP_HEIGHT_SKEW,
                })
            } else {
                let mut errors = Vec::new();
                for (node_name, state) in &live_states {
                    let node = &world.nodes_info[node_name];
                    let cache = header_caches
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
                        && canonical_tips_aligned(height_skew, &common_height_ids),
                    "common_height": min_height,
                    "height_skew": height_skew,
                    "max_allowed_height_skew": FINALITY_MAX_TIP_HEIGHT_SKEW,
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
        let common_lib = common_finalized_lib(&live_states, HONEST_NODES.len());
        let common_lib_id = common_lib.map(|(id, _)| id);
        let common_lib_slot = common_lib.map(|(_, slot)| slot);
        let common_lib_beyond_attack = common_lib.is_some_and(|(_, slot)| slot > attack.slot);
        if common_lib_beyond_attack && earliest_common_finalized_history.is_none() {
            earliest_common_finalized_history = live_states
                .first()
                .map(|(_, state)| (unix_timestamp_ms(), state.lib_id, state.lib_slot));
        }

        let mut progress_nodes = Vec::new();
        let progress = live_states.len() == HONEST_NODES.len()
            && HONEST_NODES.iter().all(|node| {
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
            &mut header_caches,
            attack.slot,
        )
        .await;
        for check in &pair_checks {
            match check["status"].as_str() {
                Some("conflicting_finalized_histories") => safety_conflict_observed = true,
                Some("ancestry_unverified") => missing_finalized_ancestry = true,
                _ => {}
            }
        }
        finalized_history_checks.push(json!({
            "observed_at_unix_ms": unix_timestamp_ms(),
            "pairs": pair_checks.clone(),
        }));

        let continuity_ok = continuity_regressions.is_empty() && continuity_unverified.is_empty();
        let ancestry_ok = observations.len() == HONEST_NODES.len()
            && observations.iter().all(|observation| {
                observation.tip_path_complete
                    && observation.finalized_path_complete
                    && observation.snapshot["tip_descends_from_lib"].as_bool() == Some(true)
            });
        let snapshot = json!({
            "elapsed_ms": start.elapsed().as_millis(),
            "observed_at_unix_ms": unix_timestamp_ms(),
            "nodes": observations.iter().map(|observation| &observation.snapshot).collect::<Vec<_>>(),
            "honest_tips_aligned": alignment.clone(),
            "progress_since_pre_attack": {
                "all_three_nodes_progressed": progress,
                "nodes": progress_nodes,
            },
            "common_lib": {
                "same_lib_id": common_lib_id.map(|id| id.encode_hex::<String>()),
                "same_lib_slot": common_lib_slot,
                "beyond_attack_slot": common_lib_beyond_attack,
                "attack_slot": attack.slot,
            },
            "finalized_history_pair_checks": pair_checks,
            "ancestry_sufficient_for_this_snapshot": ancestry_ok,
            "lib_continuity_sufficient_for_this_snapshot": continuity_ok,
        });
        snapshots.push(snapshot);

        let elapsed_ms = start.elapsed().as_millis();
        let stable_predicate = producer_stop_is_prompt(producer_stopped, stop_upper_bound_delay_ms)
            && progress
            && aligned
            && common_lib_beyond_attack
            && ancestry_ok
            && continuity_ok
            && !safety_conflict_observed
            && !missing_finalized_ancestry
            && elapsed_ms <= FINALITY_OBSERVATION_TIMEOUT.as_millis();
        let (next_stable_since, completion) = update_stable_finality_window(
            stable_predicate,
            elapsed_ms,
            stable_since_elapsed_ms,
            FINALITY_STABILITY_WINDOW.as_millis(),
        );
        stable_since_elapsed_ms = next_stable_since;
        if completion.is_some() {
            stable_completion = completion;
            break (observations, alignment, progress);
        }
        if Instant::now() >= deadline {
            break (observations, alignment, progress);
        }
        sleep(FINALITY_OBSERVATION_INTERVAL).await;
    };

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
                observation.snapshot["node"].as_str()?.to_owned(),
                observation.consensus.clone()?,
            ))
        })
        .collect::<Vec<_>>();
    let final_common_lib = common_finalized_lib(&final_live_states, HONEST_NODES.len());
    let final_common_lib_beyond_attack =
        final_common_lib.is_some_and(|(_, slot)| slot > attack.slot);
    let final_common_lib_evidence = json!({
        "all_honest_nodes_responsive": final_live_states.len() == HONEST_NODES.len(),
        "same_lib_id": final_common_lib.map(|(id, _)| id.encode_hex::<String>()),
        "same_lib_slot": final_common_lib.map(|(_, slot)| slot),
        "beyond_attack_slot": final_common_lib_beyond_attack,
        "attack_slot": attack.slot,
        "node_libs": final_nodes.iter().map(|observation| json!({
            "node": observation.snapshot["node"],
            "responsive": observation.snapshot["responsive"],
            "lib_id": observation.snapshot["lib_id"],
            "lib_slot": observation.snapshot["lib_slot"],
        })).collect::<Vec<_>>(),
    });
    let stable_completion_verified = stable_completion.is_some();
    let stable_completion_evidence = stable_completion.map(|completion| {
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
            "required_stability_duration_ms": FINALITY_STABILITY_WINDOW.as_millis(),
        })
    });
    let prompt_stop_verified = producer_stop_is_prompt(producer_stopped, stop_upper_bound_delay_ms);
    let ancestry_verified = !ancestry_gaps.iter().any(|gap| {
        gap["tip_path_complete_below_attack"].as_bool() != Some(true)
            || gap["tip_descends_from_reported_lib"].as_bool() != Some(true)
            || gap["finalized_path_complete_below_attack"].as_bool() != Some(true)
    }) && !missing_finalized_ancestry;
    let continuity_verified = continuity_regressions.is_empty() && continuity_unverified.is_empty();
    let status = if acceptance_path
        && single_isolated_opportunity
        && prompt_stop_verified
        && final_progress
        && final_alignment["aligned"].as_bool() == Some(true)
        && final_common_lib_beyond_attack
        && stable_completion_verified
        && ancestry_verified
        && continuity_verified
        && !safety_conflict_observed
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
    if attack_record_count > 1 {
        failure_classifications.push("multiple_equivocations_preventing_isolation");
    } else if !selected_is_logged {
        failure_classifications.push("selected_equivocation_not_confirmed_in_stopped_producer_log");
    }
    if !prompt_stop_verified {
        failure_classifications.push("producer_stop_not_confirmed_or_not_prompt");
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
    if safety_conflict_observed {
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
    let consensus_configuration = json!({
        "diagnostic_parameter_set": "testnet_representative",
        "security_parameter_k": 5,
        "slot_duration_secs": 1,
        "epoch_stake_distribution_stabilization": 3,
        "epoch_period_nonce_buffer": 3,
        "epoch_period_nonce_stabilization": 4,
        "interpretation": "small-k functional experiment; not a production security threshold",
    });
    let attack_evidence = json!({
        "producer": PRODUCER,
        "honest_nodes": HONEST_NODES,
        "ordinary_block_id": attack.ordinary_id_text,
        "parent_block_id": attack.parent_id_text,
        "attack_slot": attack.slot,
        "four_sibling_ids": attack.siblings.iter().map(|(index, sibling)| json!({
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
        "prompt_stop_limit_ms": PRODUCER_PROMPT_STOP_LIMIT.as_millis(),
        "prompt_stop_verified": prompt_stop_verified,
        "producer_unresponsive_after_stop_step": producer_stopped,
        "producer_consensus_probe": producer_stop_confirmation,
    });
    let observation_window = json!({
        "started_unix_ms": start_unix_ms,
        "duration_ms": duration_ms,
        "timeout_ms": FINALITY_OBSERVATION_TIMEOUT.as_millis(),
        "sample_interval_ms": FINALITY_OBSERVATION_INTERVAL.as_millis(),
        "stability_window_ms": FINALITY_STABILITY_WINDOW.as_millis(),
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
                "path": "tests/cucumber_tests/features/security_audit.feature",
                "sha256": sha256_file(&repository_root.join("tests/cucumber_tests/features/security_audit.feature")).ok(),
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
            let first_name = HONEST_NODES[first_index];
            let second_name = HONEST_NODES[second_index];
            let (first_to_second, second_to_first) = if first.lib_id == second.lib_id {
                (None, None)
            } else {
                let first_node = &world.nodes_info[first_name];
                let second_node = &world.nodes_info[second_name];
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
async fn observe_sibling_propagation(world: &CucumberWorld) -> Result<Value, AuditFailure> {
    let provenance = verify_runtime_configuration(
        world,
        "Honest peers validate valid same-slot sibling blocks",
    )
    .map_err(|message| AuditFailure {
        message,
        details: json!({}),
    })?;
    let (readiness, mut tails) =
        wait_for_blend_readiness(world)
            .await
            .map_err(|message| AuditFailure {
                message,
                details: json!({ "provenance": provenance }),
            })?;
    let mut logs: BTreeMap<String, Vec<ObservedLogLine>> = BTreeMap::new();
    let opportunity =
        wait_for_producer_opportunity(&mut tails, logs.entry(PRODUCER.to_owned()).or_default())
            .await
            .map_err(|message| AuditFailure {
                message,
                details: json!({
                    "provenance": provenance,
                    "blend_readiness": readiness,
                }),
            })?;
    let (receiver_stages, no_subscriber_errors) =
        observe_honest_receivers(&mut tails, &mut logs, &opportunity, false)
            .await
            .map_err(|message| AuditFailure {
                message,
                details: json!({
                    "provenance": provenance,
                    "blend_readiness": readiness,
                    "ordinary_block_id": opportunity.ordinary.get("ordinary_block_id"),
                    "parent_block_id": opportunity.ordinary.get("parent_block_id"),
                    "slot": opportunity.ordinary.get("slot"),
                }),
            })?;
    let storage_events = query_accepted_blocks(world, &opportunity, &receiver_stages).await;

    let mut sibling_outcomes = Vec::new();
    let mut accepted_via_blend = false;
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

        for node in HONEST_NODES {
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
        accepted_via_blend |= sibling_accepted_via_blend;

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
        "cluster_nodes": 4,
        "blend_provider_count": 4,
        "producer": PRODUCER,
        "producer_runtime_name": world.nodes_info.get(PRODUCER).map(|node| node.runtime_name.as_str()),
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
async fn observe_nonempty_sibling_propagation(
    world: &mut CucumberWorld,
    require_full_observation_window: bool,
) -> Result<Value, AuditFailure> {
    let scenario = "Honest peers validate nonempty same-slot sibling blocks";
    let provenance =
        verify_runtime_configuration(world, scenario).map_err(|message| AuditFailure {
            message,
            details: json!({}),
        })?;
    let (readiness, mut tails) =
        wait_for_blend_readiness(world)
            .await
            .map_err(|message| AuditFailure {
                message,
                details: json!({ "provenance": provenance }),
            })?;

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
    let mut producer_logs = Vec::new();
    let mut submitted_hashes = BTreeSet::new();
    let mut submitted_transactions = Vec::new();
    let mut transaction_observations = Vec::new();
    let mut skipped_opportunities = Vec::new();
    let mut seen_ordinary_ids = BTreeSet::new();
    let global_deadline = Instant::now() + NONEMPTY_TRANSACTION_WINDOW;
    let mut selected: Option<(ProducerOpportunity, ApiBlock)> = None;
    let mut last_mempool_status = "not_observed".to_owned();

    'transactions: for transaction_index in 1..=NONEMPTY_TRANSACTION_BUDGET {
        if Instant::now() >= global_deadline {
            break;
        }
        let alias = format!("SECURITY_AUDIT_NONEMPTY_{transaction_index}");
        prepare_transfer_transaction(
            world,
            "security audit nonempty sibling workload",
            alias.clone(),
            NONEMPTY_TRANSACTION_AMOUNT,
            TRANSACTION_SENDER.to_owned(),
            TRANSACTION_RECEIVER.to_owned(),
        )
        .await
        .map_err(|error| AuditFailure {
            message: format!("could not prepare independent workload transaction {alias}: {error}"),
            details: json!({
                "provenance": provenance,
                "blend_readiness": readiness,
                "transactions_prepared": submitted_transactions,
            }),
        })?;
        let signed = world
            .resolve_prepared_transaction(&alias)
            .map_err(|error| AuditFailure {
                message: format!("prepared workload transaction {alias} disappeared: {error}"),
                details: json!({ "provenance": provenance }),
            })?;
        let transaction_hash = tx_hash_to_hex(&signed.hash());
        submitted_hashes.insert(transaction_hash.clone());
        submitted_transactions.push(json!({
            "alias": alias,
            "transaction_hash": transaction_hash,
        }));
        submit_prepared_transaction_to_nodes(
            world,
            "security audit nonempty sibling workload",
            alias.clone(),
            [PRODUCER, HONEST_NODES[0], HONEST_NODES[1], HONEST_NODES[2]]
                .into_iter()
                .map(str::to_owned)
                .collect(),
        )
        .await
        .map_err(|error| AuditFailure {
            message: format!(
                "could not submit workload transaction {alias} to all four mempools: {error}"
            ),
            details: json!({
                "provenance": provenance,
                "blend_readiness": readiness,
                "pre_attack_consensus_snapshots": pre_attack_consensus,
                "submitted_transactions": submitted_transactions,
            }),
        })?;

        let submitted_hash = signed.hash();
        let transaction_deadline =
            (Instant::now() + TRANSACTION_PENDING_TIMEOUT).min(global_deadline);
        let mut saw_transaction_pending = false;
        let mut mempool_samples = Vec::new();

        loop {
            producer_logs.extend(tails.read_new_lines(PRODUCER).map_err(|message| {
                AuditFailure {
                    message,
                    details: json!({
                        "provenance": provenance,
                        "blend_readiness": readiness,
                        "submitted_transactions": submitted_transactions,
                        "skipped_opportunities": skipped_opportunities,
                    }),
                }
            })?);

            for opportunity in find_producer_opportunities(&producer_logs) {
                let Some(ordinary_id) = opportunity.ordinary.get("ordinary_block_id").cloned()
                else {
                    continue;
                };
                if seen_ordinary_ids.contains(&ordinary_id) {
                    continue;
                }
                let Some(node) = world.nodes_info.get(PRODUCER) else {
                    return Err(AuditFailure {
                        message: "NODE_1 was not started".to_owned(),
                        details: json!({ "provenance": provenance }),
                    });
                };
                let header_id = match parse_header_id(&ordinary_id) {
                    Ok(id) => id,
                    Err(error) => {
                        skipped_opportunities.push(json!({
                            "ordinary_block_id": ordinary_id,
                            "reason": "invalid_ordinary_block_id",
                            "detail": error,
                        }));
                        seen_ordinary_ids.insert(ordinary_id);
                        continue;
                    }
                };
                let block = match node.client.block(&header_id).await {
                    Ok(Some(block)) => block,
                    Ok(None) => continue,
                    Err(error) => {
                        last_mempool_status = format!("ordinary block API request failed: {error}");
                        continue;
                    }
                };
                let body = summarize_api_block(&block);
                seen_ordinary_ids.insert(ordinary_id.clone());
                if body.block_id != ordinary_id
                    || Some(&body.parent_block_id) != opportunity.ordinary.get("parent_block_id")
                    || Some(&body.slot.to_string()) != opportunity.ordinary.get("slot")
                {
                    return Err(AuditFailure {
                        message:
                            "NODE_1 block API identity disagrees with the ordinary submission event"
                                .to_owned(),
                        details: json!({
                            "provenance": provenance,
                            "ordinary_submission": opportunity.ordinary,
                            "ordinary_block_api": body,
                            "submitted_transactions": submitted_transactions,
                        }),
                    });
                }
                if has_nonempty_submitted_body(&body.transaction_hashes, &submitted_hashes) {
                    transaction_observations.push(json!({
                        "alias": alias,
                        "transaction_hash": transaction_hash,
                        "observed_pending_before_candidate": saw_transaction_pending,
                        "mempool_observations": mempool_samples,
                        "last_mempool_status": last_mempool_status,
                    }));
                    selected = Some((opportunity, block));
                    break 'transactions;
                }
                skipped_opportunities.push(json!({
                    "ordinary_block_id": body.block_id,
                    "parent_block_id": body.parent_block_id,
                    "slot": body.slot,
                    "transaction_count": body.transaction_hashes.len(),
                    "transaction_hashes": body.transaction_hashes,
                    "body_root": body.body_root,
                    "reason": if body.transaction_hashes.is_empty() {
                        "empty_ordinary_body"
                    } else {
                        "no_submitted_transaction_in_ordinary_body"
                    },
                }));
            }

            if Instant::now() >= global_deadline || Instant::now() >= transaction_deadline {
                break;
            }

            let node_1 = &world.nodes_info[PRODUCER].client;
            let node_2 = &world.nodes_info[HONEST_NODES[0]].client;
            let node_3 = &world.nodes_info[HONEST_NODES[1]].client;
            let node_4 = &world.nodes_info[HONEST_NODES[2]].client;
            let (view_1, view_2, view_3, view_4) = tokio::join!(
                node_1.test_mempool_view(),
                node_2.test_mempool_view(),
                node_3.test_mempool_view(),
                node_4.test_mempool_view(),
            );
            let views = [
                (PRODUCER, view_1),
                (HONEST_NODES[0], view_2),
                (HONEST_NODES[1], view_3),
                (HONEST_NODES[2], view_4),
            ];
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
            if saw_transaction_pending && all_views_succeeded && pending_nodes.is_empty() {
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
        return Err(AuditFailure {
            message: format!(
                "workload/setup failure: NODE_1 did not produce a four-sibling ordinary block containing one of the submitted transactions after {} bounded submissions",
                submitted_transactions.len()
            ),
            details: json!({
                "provenance": provenance,
                "blend_readiness": readiness,
                "submitted_transactions": submitted_transactions,
                "transaction_observations": transaction_observations,
                "producer_opportunities_examined": seen_ordinary_ids.len(),
                "empty_or_nonmatching_opportunities_skipped": skipped_opportunities.len(),
                "skipped_opportunities": skipped_opportunities,
                "last_mempool_status": last_mempool_status,
                "failure_classification": "workload_or_leadership_setup",
            }),
        });
    };

    let ordinary_body = summarize_api_block(&ordinary_block);
    let mut producer_log_map = BTreeMap::from([(PRODUCER.to_owned(), producer_logs)]);
    let (receiver_stages, no_subscriber_errors) = observe_honest_receivers(
        &mut tails,
        &mut producer_log_map,
        &opportunity,
        require_full_observation_window,
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
    let storage_events = query_accepted_blocks(world, &opportunity, &receiver_stages).await;
    let ordinary_hashes = &ordinary_body.transaction_hashes;
    let mut sibling_outcomes = Vec::new();
    let mut accepted_body_path = false;
    let mut accepted_bodies_all_match = true;
    for (_, identity) in &opportunity.siblings {
        let sibling_id = identity
            .get("sibling_block_id")
            .cloned()
            .unwrap_or_default();
        let mut receiver_reports = Vec::new();
        let mut accepted = false;
        for node in HONEST_NODES {
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
                .is_some_and(|(root, hashes)| same_transaction_body(&ordinary_body, root, hashes));
            let ordered_path = evidence.has_ordered_blend_to_acceptance();
            accepted |= evidence.has_stage("accepted");
            if evidence.has_stage("accepted") && (!has_stored_block || !body_matches) {
                accepted_bodies_all_match = false;
            }
            accepted_body_path |= ordered_path && has_stored_block && body_matches;
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
    let integrity = sibling_body_integrity(&ordinary_body, &opportunity);
    let passed = accepted_body_path
        && accepted_bodies_all_match
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
        "cluster_nodes": 4,
        "blend_provider_count": 4,
        "pre_attack_consensus_snapshots": pre_attack_consensus,
        "producer": PRODUCER,
        "producer_runtime_name": world.nodes_info.get(PRODUCER).map(|node| node.runtime_name.as_str()),
        "blend_readiness": readiness,
        "provenance": provenance,
        "transaction_budget": NONEMPTY_TRANSACTION_BUDGET,
        "transactions_submitted": submitted_transactions.len(),
        "submitted_transactions": submitted_transactions,
        "transaction_observations": transaction_observations,
        "producer_opportunities_examined": seen_ordinary_ids.len(),
        "empty_or_nonmatching_opportunities_skipped": skipped_opportunities.len(),
        "skipped_opportunities": skipped_opportunities,
        "selected_ordinary_block": {
            "block_id": ordinary_body.block_id,
            "parent_block_id": ordinary_body.parent_block_id,
            "slot": ordinary_body.slot,
            "body_root": ordinary_body.body_root,
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
        "accepted_bodies_all_match_ordinary": accepted_bodies_all_match,
        "siblings": sibling_outcomes,
        "storage_observations": storage_events,
        "receiver_observability": {
            "blend_topic_subscription_api": "not exposed; matching decapsulated sibling event is direct per-payload reception evidence",
            "canonical_selection": "not asserted; accepted sibling may be non-canonical",
            "producer_enqueue_is_not_peer_delivery": true,
        },
    }))
}

async fn wait_for_blend_readiness(world: &CucumberWorld) -> Result<(Value, AuditLogTail), String> {
    let deadline = Instant::now() + READINESS_TIMEOUT;
    let mut last_status;
    loop {
        let mut all_ready = true;
        let mut samples = Vec::new();
        for node_name in [PRODUCER, HONEST_NODES[0], HONEST_NODES[1], HONEST_NODES[2]] {
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
            for name in [PRODUCER, HONEST_NODES[0], HONEST_NODES[1], HONEST_NODES[2]] {
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
                READINESS_TIMEOUT.as_secs()
            ));
        }
        sleep(Duration::from_secs(1)).await;
    }
}

async fn wait_for_producer_opportunity(
    tails: &mut AuditLogTail,
    logs: &mut Vec<ObservedLogLine>,
) -> Result<ProducerOpportunity, String> {
    let deadline = Instant::now() + LEADERSHIP_TIMEOUT;
    loop {
        logs.extend(tails.read_new_lines(PRODUCER)?);
        if let Some(opportunity) = find_producer_opportunity(logs) {
            return Ok(opportunity);
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "NODE_1 did not complete a four-sibling genuine leadership opportunity within {}s after Blend readiness",
                LEADERSHIP_TIMEOUT.as_secs()
            ));
        }
        sleep(Duration::from_millis(250)).await;
    }
}

fn find_producer_opportunity(lines: &[ObservedLogLine]) -> Option<ProducerOpportunity> {
    find_producer_opportunities(lines).into_iter().next()
}

fn find_producer_opportunities(lines: &[ObservedLogLine]) -> Vec<ProducerOpportunity> {
    let parsed = lines
        .iter()
        .enumerate()
        .map(|(line, raw)| (line, raw, parse_log_fields(&raw.text)))
        .collect::<Vec<_>>();
    let mut opportunities = Vec::new();
    for (ordinary_line, ordinary_record, ordinary) in &parsed {
        if ordinary
            .get("event")
            .is_none_or(|event| event != "security_audit_ordinary_submitted")
            || parse_usize(ordinary, "configured_siblings") != Some(SIBLING_COUNT)
        {
            continue;
        }
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
        if siblings.len() != SIBLING_COUNT {
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
        if indexes != BTreeSet::from([1, 2, 3, 4])
            || ids.len() != SIBLING_COUNT
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
        if parse_usize(timing, "configured_siblings") != Some(SIBLING_COUNT)
            || parse_usize(timing, "constructed_siblings") != Some(SIBLING_COUNT)
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

fn sibling_body_integrity(ordinary: &BlockBodyEvidence, opportunity: &ProducerOpportunity) -> bool {
    let sibling_ids = opportunity
        .siblings
        .iter()
        .filter_map(|(_, sibling)| sibling.get("sibling_block_id"))
        .collect::<BTreeSet<_>>();
    sibling_ids.len() == SIBLING_COUNT
        && !sibling_ids.contains(&ordinary.block_id)
        && opportunity.siblings.len() == SIBLING_COUNT
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
    require_full_observation_window: bool,
) -> Result<(HashMap<(String, String), ReceiverEvidence>, bool), String> {
    let ids = opportunity
        .siblings
        .iter()
        .filter_map(|(_, event)| event.get("sibling_block_id").cloned())
        .collect::<BTreeSet<_>>();
    let deadline = Instant::now() + RECEIVER_TIMEOUT;
    let mut first_acceptance_at = None;
    loop {
        for node in HONEST_NODES {
            logs.entry(node.to_owned())
                .or_default()
                .extend(tails.read_new_lines(node)?);
        }
        logs.entry(PRODUCER.to_owned())
            .or_default()
            .extend(tails.read_new_lines(PRODUCER)?);
        let stages = receiver_stages(logs, &ids, &opportunity.ordinary);
        let has_valid_acceptance = stages
            .values()
            .any(ReceiverEvidence::has_ordered_blend_to_acceptance);
        if has_valid_acceptance && first_acceptance_at.is_none() {
            first_acceptance_at = Some(Instant::now());
        }
        let all_terminal = ids.iter().all(|id| {
            stages.iter().any(|((_, observed_id), observed)| {
                observed_id == id && observed.has_terminal_stage()
            })
        });
        if (has_valid_acceptance
            && (first_acceptance_at.is_some_and(|start| start.elapsed() >= Duration::from_secs(8))
                || (!require_full_observation_window && all_terminal)))
            || Instant::now() >= deadline
        {
            let no_subscriber_errors = logs.get(PRODUCER).is_some_and(|producer| {
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
) -> HashMap<(String, String), ReceiverEvidence> {
    let mut stages: HashMap<(String, String), ReceiverEvidence> = HashMap::new();
    for node in HONEST_NODES {
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

fn verify_runtime_configuration(world: &CucumberWorld, scenario: &str) -> Result<Value, String> {
    let binary = std::env::var_os("LOGOS_BLOCKCHAIN_NODE_BIN").ok_or_else(|| {
        "LOGOS_BLOCKCHAIN_NODE_BIN was not set by the Cucumber environment".to_owned()
    })?;
    let binary = PathBuf::from(binary)
        .canonicalize()
        .map_err(|error| format!("cannot resolve selected node binary: {error}"))?;
    let binary_sha256 = sha256_file(&binary)?;
    let commit = command_output("git", &["rev-parse", "HEAD"])?;
    let mut nodes = BTreeMap::new();

    for (node_name, expected) in [
        (PRODUCER, SIBLING_COUNT),
        (HONEST_NODES[0], 0),
        (HONEST_NODES[1], 0),
        (HONEST_NODES[2], 0),
    ] {
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
                "user_config_path": config_path,
                "user_config_sha256": sha256_file(&config_path)?,
                "executable_path": binary,
                "executable_sha256": binary_sha256,
            }),
        );
    }

    Ok(json!({
        "commit_sha": commit,
        "binary_path": binary,
        "binary_sha256": binary_sha256,
        "feature_set": "security-audit,testing",
        "scenario": scenario,
        "node_configs": nodes,
    }))
}

impl AuditLogTail {
    fn after_readiness(world: &CucumberWorld) -> Result<Self, String> {
        let root = &world.lifecycle.scenario_base_dir;
        let mut runtime_names = BTreeMap::new();
        let mut paths = BTreeMap::new();
        let mut offsets = HashMap::new();
        for node_name in [PRODUCER, HONEST_NODES[0], HONEST_NODES[1], HONEST_NODES[2]] {
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
    let mut copied = Vec::new();
    for node_name in [PRODUCER, HONEST_NODES[0], HONEST_NODES[1], HONEST_NODES[2]] {
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

    const SIBLING_ID: &str = "0xsibling";
    const PARENT_ID: &str = "0xparent";

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
        receiver_stages(&logs, &BTreeSet::from([SIBLING_ID.to_owned()]), &ordinary)
            .remove(&("NODE_2".to_owned(), SIBLING_ID.to_owned()))
            .unwrap_or_default()
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
        let evidence = receiver_stages(&logs, &BTreeSet::from([SIBLING_ID.to_owned()]), &ordinary);

        assert!(
            evidence
                .values()
                .all(|item| !item.has_ordered_blend_to_acceptance())
        );
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
            ]),
            ordinary_line: 1,
            siblings,
            timing: HashMap::new(),
            timing_line: 10,
        }
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
    fn sibling_identity_check_requires_four_distinct_same_parent_same_slot_ids() {
        let ordinary = body_evidence("0xordinary", &["0xtx-a"]);
        let mut opportunity = opportunity_with_siblings();
        assert!(sibling_body_integrity(&ordinary, &opportunity));

        opportunity.siblings[0]
            .1
            .insert("parent_block_id".to_owned(), "0xother-parent".to_owned());
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
            read_runtime_node_logs(&current_runtime, PRODUCER).expect("read current producer log"),
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
            &[header_id(1), header_id(1), header_id(1)]
        ));
        assert!(canonical_tips_aligned(
            1,
            &[header_id(1), header_id(1), header_id(1)]
        ));
        assert!(!canonical_tips_aligned(
            0,
            &[header_id(1), header_id(2), header_id(1)]
        ));
        assert!(!canonical_tips_aligned(
            2,
            &[header_id(1), header_id(1), header_id(1)]
        ));
        assert!(!canonical_tips_aligned(0, &[]));
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
        let limit_ms = u64::try_from(PRODUCER_PROMPT_STOP_LIMIT.as_millis()).unwrap();
        assert_eq!(limit_ms, 30_000);
        assert!(producer_stop_is_prompt(true, limit_ms - 1));
        assert!(producer_stop_is_prompt(true, limit_ms));
        assert!(!producer_stop_is_prompt(true, limit_ms + 1));
        assert!(!producer_stop_is_prompt(false, 0));
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
