use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    fs::{self, File},
    io::{BufRead as _, BufReader, Seek as _, SeekFrom},
    path::{Path, PathBuf},
    process::Command,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use cucumber::then;
use lb_common_http_client::ApiBlock;
use lb_core::{header::HeaderId, mantle::traits::Hashable as _};
use lb_testing_framework::USER_CONFIG_FILE;
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

    let result = observe_nonempty_sibling_propagation(world).await;
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
        true,
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
