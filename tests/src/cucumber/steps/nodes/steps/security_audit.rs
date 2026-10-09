use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    fs::{self, File},
    io::{BufRead as _, BufReader, Seek as _, SeekFrom},
    path::{Path, PathBuf},
    process::Command,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use cucumber::then;
use lb_core::header::HeaderId;
use lb_testing_framework::USER_CONFIG_FILE;
use serde_json::{Value, json};
use tokio::time::sleep;

use crate::{
    common::manual_cluster::read_manual_node_logs,
    cucumber::{
        error::{StepError, StepResult},
        steps::TARGET,
        world::CucumberWorld,
    },
};

const PRODUCER: &str = "NODE_1";
const HONEST_NODES: [&str; 3] = ["NODE_2", "NODE_3", "NODE_4"];
const SIBLING_COUNT: usize = 4;
const READINESS_TIMEOUT: Duration = Duration::from_secs(60);
const LEADERSHIP_TIMEOUT: Duration = Duration::from_secs(300);
const RECEIVER_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Clone)]
struct ProducerOpportunity {
    ordinary: HashMap<String, String>,
    ordinary_line: usize,
    siblings: Vec<(usize, HashMap<String, String>)>,
    timing: HashMap<String, String>,
    timing_line: usize,
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

#[expect(
    clippy::too_many_lines,
    reason = "Collects all correlated producer and receiver evidence for one selected opportunity"
)]
async fn observe_sibling_propagation(world: &CucumberWorld) -> Result<Value, AuditFailure> {
    let provenance = verify_runtime_configuration(world).map_err(|message| AuditFailure {
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
        observe_honest_receivers(&mut tails, &mut logs, &opportunity)
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
                    "source_file": event.path,
                    "source_offset": event.offset,
                })).collect::<Vec<_>>(),
                "mismatched_identity_events": evidence.mismatched_events.iter().map(|event| json!({
                    "stage": event.stage,
                    "parent_block_id": event.parent_block_id,
                    "slot": event.slot,
                    "timestamp": format_log_timestamp(event.timestamp),
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
    let parsed = lines
        .iter()
        .enumerate()
        .map(|(line, raw)| (line, raw, parse_log_fields(&raw.text)))
        .collect::<Vec<_>>();
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
        return Some(ProducerOpportunity {
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
    None
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

async fn observe_honest_receivers(
    tails: &mut AuditLogTail,
    logs: &mut BTreeMap<String, Vec<ObservedLogLine>>,
    opportunity: &ProducerOpportunity,
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
            && (all_terminal
                || first_acceptance_at
                    .is_some_and(|start| start.elapsed() >= Duration::from_secs(8))))
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
        let decoded = block_id
            .strip_prefix("0x")
            .map_or_else(|| hex::decode(block_id), hex::decode);
        let Ok(bytes) = decoded else {
            continue;
        };
        let Ok(header_id) = HeaderId::try_from(bytes.as_slice()) else {
            continue;
        };
        if !matches!(node.client.block(&header_id).await, Ok(Some(_))) {
            continue;
        }
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
            "stored": true,
            "evidence_source": "NodeHttpClient::block",
        }));
    }
    observations
}

fn verify_runtime_configuration(world: &CucumberWorld) -> Result<Value, String> {
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
        "scenario": "Honest peers validate valid same-slot sibling blocks",
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
    line.split_whitespace()
        .filter_map(|field| field.split_once('='))
        .map(|(key, value)| (key.to_owned(), value.trim_matches('"').to_owned()))
        .collect()
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
}
