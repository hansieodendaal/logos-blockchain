//! Scenario provenance, evidence records, and artifact checksums.

use std::{collections::HashMap, fs, fs::OpenOptions, io::Write as _, path::Path};

use cucumber::given;
use serde_json::Value;

use super::{
    events::{RuntimeObservation, collect_observations, node_peer_id},
    lifecycle,
    scenario::{participants, provider_name, requester_name, validate_participant_topology},
};
use crate::cucumber::{
    error::{StepError, StepResult},
    world::{CucumberWorld, NodeInfo},
};

const EVIDENCE_FILE: &str = "era_boundary_recovery_evidence.ndjson";
const RFC_REVISION: &str = "f5a1f368532cf8b60fb3107576e9fdd98f456718";

fn failure(message: impl Into<String>) -> StepError {
    StepError::StepFail {
        message: message.into(),
    }
}

#[expect(
    clippy::needless_pass_by_value,
    reason = "Evidence callers build each JSON record inline for immediate serialization"
)]
pub(super) fn append_record(world: &CucumberWorld, event: &str, data: Value) -> StepResult {
    let path = world.lifecycle.scenario_base_dir.join(EVIDENCE_FILE);
    let file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .map_err(|error| failure(format!("Cannot open {}: {error}", path.display())))?;
    let mut writer = std::io::BufWriter::new(file);
    serde_json::to_writer(
        &mut writer,
        &serde_json::json!({
            "event": event,
            "timestamp_unix_ms": now_unix_ms(),
            "timestamp_unix_ns": now_unix_ns(),
            "data": data,
        }),
    )
    .map_err(|error| failure(format!("Cannot encode recovery evidence: {error}")))?;
    writer
        .write_all(b"\n")
        .map_err(|error| failure(format!("Cannot write {}: {error}", path.display())))?;
    writer
        .flush()
        .map_err(|error| failure(format!("Cannot flush {}: {error}", path.display())))
}

fn now_unix_ms() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}

fn now_unix_ns() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
}

pub fn initialize_evidence_file(scenario_dir: &Path) -> std::io::Result<()> {
    fs::create_dir_all(scenario_dir)?;
    let path = scenario_dir.join(EVIDENCE_FILE);
    let entry = serde_json::json!({
        "event": "experiment_provenance",
        "timestamp_unix_ms": now_unix_ms(),
        "experiment": "RFC 439 era-transition recovery",
        "rfc": "RFC 439: Bedrock Eras",
        "rfc_revision": RFC_REVISION,
        "rfc_url": "https://github.com/logos-co/logos-lips/blob/Bedrock-RFC-Eras/docs/blockchain/raw/bedrock-eras.md",
        "issue": "https://github.com/logos-blockchain/logos-blockchain/issues/3902",
    });
    fs::write(&path, format!("{}\n", serde_json::to_string(&entry)?))
}

#[given("era-boundary recovery evidence is active")]
#[expect(
    clippy::needless_pass_by_ref_mut,
    reason = "Cucumber step callbacks require &mut World"
)]
fn evidence_is_active(world: &mut CucumberWorld) -> StepResult {
    let path = world.lifecycle.scenario_base_dir.join(EVIDENCE_FILE);
    if !path.is_file() {
        return Err(failure(format!(
            "Scenario evidence was not initialized at {}",
            path.display()
        )));
    }
    append_record(
        world,
        "assertion",
        serde_json::json!({
            "assertion": "scenario_local_evidence_initialized",
            "status": "passed",
            "evidence": path.display().to_string(),
        }),
    )
}

pub(super) fn record_assertion(
    world: &CucumberWorld,
    assertion: &str,
    passed: bool,
    detail: &str,
) -> StepResult {
    append_record(
        world,
        "assertion",
        serde_json::json!({
            "assertion": assertion,
            "status": if passed { "passed" } else { "failed" },
            "detail": detail,
        }),
    )
}

pub fn capture_scenario_evidence(world: &CucumberWorld) -> StepResult {
    let events = match collect_observations(world, true) {
        Ok(events) => events,
        Err(error) => {
            append_record(
                world,
                "evidence_collection_failure",
                serde_json::json!({ "reason": error }),
            )?;
            return Err(failure(error));
        }
    };
    let verification = verify_scenario_evidence(world, &events);
    append_record(
        world,
        "evidence_verification",
        serde_json::json!({
            "status": if verification.is_ok() { "passed" } else { "failed" },
            "failures": verification.as_ref().err(),
            "runtime_event_count": events.len(),
        }),
    )?;
    verification.map_err(|failures| failure(failures.join("; ")))
}

#[expect(
    clippy::too_many_lines,
    reason = "the top-level evidence gate enumerates all mandatory independent acceptance records"
)]
fn verify_scenario_evidence(
    world: &CucumberWorld,
    events: &[RuntimeObservation],
) -> Result<(), Vec<String>> {
    let mut failures = Vec::new();
    let participant_map =
        participants(world).map_err(|error| vec![format!("participants: {error:?}")])?;
    let provider = provider_name(world).map_err(|error| vec![format!("provider: {error:?}")])?;
    let requester = requester_name(world).map_err(|error| vec![format!("requester: {error:?}")])?;
    let (f0, f1) = super::scenario::runtime_binding(world, &provider)
        .ok()
        .and_then(|binding| {
            super::events::era_protocols_from_path(&binding.runtime_dir.join("deployment.yaml"))
                .ok()
        })
        .map(|(_, f0, f1)| (f0, f1))
        .ok_or_else(|| vec!["provider deployment does not define eras 0 and 1".to_owned()])?;

    let records = read_json_lines(&world.lifecycle.scenario_base_dir.join(EVIDENCE_FILE))
        .map_err(|error| vec![format!("scenario evidence records: {error}")])?;
    let shared = read_json_lines(
        &world
            .lifecycle
            .scenario_base_dir
            .join("era_boundaries_evidence.ndjson"),
    )
    .map_err(|error| vec![format!("shared era-boundary evidence records: {error}")])?;

    let mut assertions = HashMap::<String, Vec<&Value>>::new();
    for record in &records {
        if record.get("event").and_then(Value::as_str) != Some("assertion") {
            continue;
        }
        let Some(data) = record.get("data") else {
            failures.push("assertion record has no data".to_owned());
            continue;
        };
        let Some(name) = data.get("assertion").and_then(Value::as_str) else {
            failures.push("assertion record has no assertion name".to_owned());
            continue;
        };
        assertions.entry(name.to_owned()).or_default().push(data);
    }

    let required_assertions = [
        "scenario_local_evidence_initialized",
        "established_f0_request_and_partial_progress",
        "expiry_cancellation_and_requester_detection",
        "automatic_f1_recovery",
        "partial_progress_retained",
        "ibd_completed",
        "canonical_convergence",
        "per_node_experiment_configuration",
        "request_capacity_and_node_health",
    ];
    for assertion in required_assertions {
        match assertions.get(assertion) {
            Some(entries)
                if entries
                    .iter()
                    .any(|entry| entry.get("status").and_then(Value::as_str) == Some("passed")) => {
            }
            Some(entries)
                if entries
                    .iter()
                    .any(|entry| entry.get("status").and_then(Value::as_str) == Some("failed")) =>
            {
                failures.push(format!("assertion `{assertion}` has a recorded failure"));
            }
            _ => failures.push(format!(
                "required assertion `{assertion}` is missing or unverified"
            )),
        }
    }

    for participant in participant_map.keys() {
        if !assertions
            .get("proposal_publishing_suppressed")
            .into_iter()
            .flatten()
            .any(|entry| participant_suppression_is_verified(entry, participant))
        {
            failures.push(format!(
                "participant `{participant}` lacks a disabled runtime publication state observation"
            ));
        }
        let config = records
            .iter()
            .any(|record| participant_configuration_is_verified(record, participant));
        if !config {
            failures.push(format!(
                "participant `{participant}` lacks recorded effective configuration"
            ));
        }
        let healthy = records.iter().any(|record| {
            record.get("event").and_then(Value::as_str) == Some("node_health")
                && record
                    .get("data")
                    .and_then(|data| data.get("node"))
                    .and_then(Value::as_str)
                    == Some(participant)
                && record
                    .get("data")
                    .and_then(|data| data.get("status"))
                    .and_then(Value::as_str)
                    == Some("healthy")
        });
        if !healthy {
            failures.push(format!(
                "participant `{participant}` has no healthy runtime observation"
            ));
        }
    }

    let provider_peer = node_peer_id(world, &provider);
    let requester_peer = node_peer_id(world, &requester);
    match (provider_peer, requester_peer) {
        (Some(provider_peer), Some(requester_peer)) => verify_runtime_lifecycle(
            events,
            &provider,
            &requester,
            &provider_peer,
            &requester_peer,
            &f0,
            &f1,
            &mut failures,
        ),
        _ => failures.push("provider or requester runtime peer identity is missing".to_owned()),
    }
    verify_timeout_cause(events, &records, &provider, &requester, &f0, &mut failures);
    verify_shared_convergence(
        &shared,
        participant_map.keys().map(String::as_str),
        &mut failures,
    );
    verify_partial_progress_record(&records, &mut failures);
    if failures.is_empty() {
        Ok(())
    } else {
        Err(failures)
    }
}

pub(super) fn participant_suppression_is_verified(assertion: &Value, participant: &str) -> bool {
    assertion.get("node").and_then(Value::as_str) == Some(participant)
        && assertion.get("status").and_then(Value::as_str) == Some("passed")
        && assertion
            .get("publication_enabled")
            .and_then(Value::as_bool)
            == Some(false)
}

pub(super) fn participant_configuration_is_verified(record: &Value, participant: &str) -> bool {
    record.get("event").and_then(Value::as_str) == Some("node_configuration_provenance")
        && record
            .get("data")
            .and_then(|data| data.get("node"))
            .and_then(Value::as_str)
            == Some(participant)
        && record
            .get("data")
            .and_then(|data| data.get("node_config_path"))
            .and_then(Value::as_str)
            .is_some()
        && record
            .get("data")
            .and_then(|data| data.get("deployment_path"))
            .and_then(Value::as_str)
            .is_some()
}

fn verify_timeout_cause(
    events: &[RuntimeObservation],
    records: &[Value],
    provider: &str,
    requester: &str,
    f0: &str,
    failures: &mut Vec<String>,
) {
    use lb_cryptarchia_sync::observation::Observation;
    let rotations: Vec<_> = events.iter().filter(|event| event.node == provider && matches!(&event.record.event, Observation::EraRotationStarted { previous_protocol, .. } if previous_protocol == f0)).collect();
    let expiries: Vec<_> = events.iter().filter(|event| event.node == provider && matches!(&event.record.event, Observation::EraTransitionExpired { old_protocol, .. } if old_protocol == f0)).collect();
    let ([rotation], [expiry]) = (rotations.as_slice(), expiries.as_slice()) else {
        failures.push("provider rotation or expiry is missing or ambiguous".to_owned());
        return;
    };
    let Some(grace_ns) = expiry
        .record
        .elapsed_ns
        .checked_sub(rotation.record.elapsed_ns)
        .filter(|duration| *duration > 0)
    else {
        failures.push("provider monotonic grace duration is invalid".to_owned());
        return;
    };
    for node in [provider, requester] {
        let config = records
            .iter()
            .find(|record| participant_configuration_is_verified(record, node))
            .and_then(|record| record.get("data"));
        for key in ["peer_response_timeout", "inbound_download_stream_timeout"] {
            let timeout = config
                .and_then(|config| config.get(key))
                .and_then(Value::as_str)
                .and_then(|value| humantime::parse_duration(value).ok());
            if timeout.is_none_or(|timeout| timeout.as_nanos() <= u128::from(grace_ns)) {
                failures.push(format!("{node} {key} is missing, invalid, or no longer than the provider's monotonic grace duration"));
            }
        }
    }
}

#[expect(
    clippy::too_many_arguments,
    reason = "the lifecycle is bound to declared peers and protocols"
)]
pub(super) fn verify_runtime_lifecycle(
    events: &[RuntimeObservation],
    provider: &str,
    requester: &str,
    provider_peer: &str,
    requester_peer: &str,
    f0: &str,
    f1: &str,
    failures: &mut Vec<String>,
) {
    let result = lifecycle::original(
        events,
        provider,
        requester,
        provider_peer,
        requester_peer,
        f0,
    )
    .and_then(|original| lifecycle::expiry(events, &original, provider, requester, f0, f1))
    .and_then(|expiry| {
        lifecycle::replacement(
            events,
            &expiry,
            provider,
            requester,
            provider_peer,
            requester_peer,
            f1,
        )
    })
    .and_then(|replacement| lifecycle::completed(events, &replacement, provider, requester));
    if let Err(error) = result {
        failures.push(error.to_owned());
    }
}

pub(super) fn verify_partial_progress_record(records: &[Value], failures: &mut Vec<String>) {
    let Some(data) = records.iter().find_map(|record| {
        (record.get("event").and_then(Value::as_str) == Some("partial_progress_and_ibd_completion"))
            .then(|| record.get("data"))
            .flatten()
    }) else {
        failures.push("partial-progress and IBD target-resolution record is missing".to_owned());
        return;
    };
    for field in [
        "partial_block_available_after_recovery",
        "partial_block_is_ancestor_of_replacement_tip",
        "partial_block_is_ancestor_of_final_tip",
        "original_target_available_after_recovery",
        "original_target_is_ancestor_of_final_tip",
    ] {
        if data.get(field).and_then(Value::as_bool) != Some(true) {
            failures.push(format!("partial-progress record does not prove `{field}`"));
        }
    }
}

pub(super) fn verify_shared_convergence<'a>(
    records: &[Value],
    participants: impl Iterator<Item = &'a str>,
    failures: &mut Vec<String>,
) {
    let participants = participants.collect::<Vec<_>>();
    let convergence = records.iter().find(|record| {
        record.get("event").and_then(Value::as_str) == Some("canonical_convergence")
    });
    if let Some(record) = convergence {
        let evidence = record.get("evidence").unwrap_or(&Value::Null);
        let within_height_bound = evidence
            .get("height_difference")
            .and_then(Value::as_u64)
            .zip(
                evidence
                    .get("allowed_height_difference")
                    .and_then(Value::as_u64),
            )
            .is_some_and(|(difference, allowed)| difference <= allowed);
        let nodes = evidence.get("nodes").and_then(Value::as_array);
        if !within_height_bound {
            failures.push(
                "canonical convergence record has no passing bounded-height result".to_owned(),
            );
        }
        if evidence
            .get("common_block")
            .and_then(Value::as_str)
            .is_none_or(str::is_empty)
        {
            failures.push("canonical convergence record has no common canonical block".to_owned());
        }
        for participant in &participants {
            if !nodes.is_some_and(|nodes| {
                nodes.iter().any(|node| {
                    node.get("node").and_then(Value::as_str) == Some(*participant)
                        && node.get("tip").and_then(Value::as_str).is_some()
                })
            }) {
                failures.push(format!(
                    "canonical convergence does not include participant `{participant}`"
                ));
            }
        }
    } else {
        failures.push("fresh canonical convergence evidence is missing".to_owned());
    }
    for record in records
        .iter()
        .filter(|record| record.get("event").and_then(Value::as_str) == Some("finality_continuity"))
    {
        let evidence = record.get("evidence").unwrap_or(&Value::Null);
        if evidence
            .get("lib_ancestry_contains_anchor")
            .and_then(Value::as_bool)
            != Some(true)
        {
            failures.push(
                "a finality-continuity record does not explicitly confirm the anchor in LIB ancestry"
                    .to_owned(),
            );
        }
    }
    for participant in participants {
        if !records.iter().any(|record| {
            record.get("event").and_then(Value::as_str) == Some("finality_continuity")
                && record
                    .get("evidence")
                    .and_then(|evidence| evidence.get("node"))
                    .and_then(Value::as_str)
                    == Some(participant)
                && record
                    .get("evidence")
                    .and_then(|evidence| evidence.get("original_lib"))
                    .and_then(Value::as_str)
                    .is_some()
        }) {
            failures.push(format!(
                "finality continuity on LIB ancestry is missing for `{participant}`"
            ));
        }
    }
}

fn read_json_lines(path: &Path) -> std::io::Result<Vec<Value>> {
    fs::read_to_string(path)?
        .lines()
        .map(|line| {
            serde_json::from_str(line)
                .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))
        })
        .collect()
}

pub fn capture_node_configuration_evidence(world: &CucumberWorld) -> StepResult {
    validate_participant_topology(world)?;
    for participant in participants(world)?.values() {
        let node = world
            .nodes_info
            .get(&participant.node_name)
            .ok_or_else(|| {
                failure(format!(
                    "participant `{}` has not started",
                    participant.node_name
                ))
            })?;
        capture_node_configuration(world, &participant.node_name, node)?;
    }
    Ok(())
}

fn capture_node_configuration(
    world: &CucumberWorld,
    node_name: &str,
    node: &NodeInfo,
) -> StepResult {
    let config_path = node.runtime_dir.join("node.yaml");
    let deployment_path = node.runtime_dir.join("deployment.yaml");
    let config_text = fs::read_to_string(&config_path)
        .map_err(|error| failure(format!("Cannot read {}: {error}", config_path.display())))?;
    let config_value = serde_yaml::from_str::<serde_yaml::Value>(&config_text)
        .map_err(|error| failure(format!("Cannot parse {}: {error}", config_path.display())))?;
    let testing_settings = yaml_path(
        &config_value,
        "network.backend.swarm.chain_sync.testing_era_boundaries",
    )
    .cloned();
    let tip_poll_enabled =
        yaml_path(&config_value, "cryptarchia.network.sync.tip_poll.enabled").cloned();
    let peer_response_timeout = yaml_path(
        &config_value,
        "network.backend.swarm.chain_sync.peer_response_timeout",
    )
    .cloned();
    let inbound_download_stream_timeout = yaml_path(
        &config_value,
        "network.backend.swarm.chain_sync.inbound_download_stream_timeout",
    )
    .cloned();
    append_record(
        world,
        "node_configuration_provenance",
        serde_json::json!({
            "node": node_name,
            "peer_id": node_peer_id(world, node_name),
            "runtime_dir": node.runtime_dir.display().to_string(),
            "node_config_path": config_path.display().to_string(),
            "deployment_path": deployment_path.display().to_string(),
            "testing_era_boundaries": testing_settings,
            "tip_poll_enabled": tip_poll_enabled,
            "peer_response_timeout": peer_response_timeout,
            "inbound_download_stream_timeout": inbound_download_stream_timeout,
        }),
    )
}

pub(super) fn node_config_value(
    world: &CucumberWorld,
    node_name: &str,
    path: &str,
) -> Option<serde_yaml::Value> {
    let node = world.nodes_info.get(node_name)?;
    let contents = fs::read_to_string(node.runtime_dir.join("node.yaml")).ok()?;
    let config = serde_yaml::from_str::<serde_yaml::Value>(&contents).ok()?;
    yaml_path(&config, path).cloned()
}

fn yaml_path<'a>(value: &'a serde_yaml::Value, path: &str) -> Option<&'a serde_yaml::Value> {
    path.split('.').try_fold(value, |current, part| {
        current
            .as_mapping()?
            .get(serde_yaml::Value::String(part.to_owned()))
    })
}
