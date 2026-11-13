//! Incremental typed per-node observation collection.

use std::{
    collections::HashMap,
    fs,
    path::{Path, PathBuf},
    time::Duration,
};

use lb_cryptarchia_sync::observation::{ObservationReader, ObservationRecord};
use lb_libp2p::protocol_name::ProtocolScope;
use lb_node::config::DeploymentSettings;
use tokio::time::{Instant, sleep};

use super::scenario::{
    participants, runtime_binding, validate_participant_topology,
    validate_started_participant_topology,
};
use crate::cucumber::{
    error::StepError,
    world::{CucumberWorld, EraTransitionJournal, NodeInfo},
};

/// A journal record together with its scenario-local, authoritative owner.
#[derive(Clone, Debug)]
pub(super) struct RuntimeObservation {
    pub node: String,
    pub peer_id: String,
    pub path: PathBuf,
    pub record: ObservationRecord,
}

/// Read each participant's journal from its saved cursor. At shutdown require
/// the final record to be complete as well.
pub(super) fn collect_observations(
    world: &CucumberWorld,
    finished: bool,
) -> Result<Vec<RuntimeObservation>, String> {
    if finished {
        validate_participant_topology(world).map_err(|error| format!("{error:?}"))?;
    } else {
        validate_started_participant_topology(world).map_err(|error| format!("{error:?}"))?;
    }
    let state = world
        .era_transition
        .as_ref()
        .ok_or("era-transition state is missing")?;
    let mut journals = state
        .observation_journals
        .lock()
        .map_err(|_| "observation collector lock poisoned")?;
    let mut owned_paths = HashMap::<PathBuf, String>::new();
    let mut events = Vec::new();
    for participant in participants(world)
        .map_err(|error| format!("{error:?}"))?
        .values()
    {
        if !finished && !world.nodes_info.contains_key(&participant.node_name) {
            continue;
        }
        let binding = runtime_binding(world, &participant.node_name)
            .map_err(|error| format!("participant runtime binding: {error:?}"))?;
        let path = binding.runtime_dir.join("era_observations.ndjson");
        if let Some(previous) = owned_paths.insert(path.clone(), participant.node_name.clone()) {
            return Err(format!(
                "journal {} belongs to both {previous} and {}",
                path.display(),
                participant.node_name
            ));
        }
        let journal = journals
            .entry(participant.node_name.clone())
            .or_insert_with(|| EraTransitionJournal {
                path: path.clone(),
                reader: ObservationReader::new(path.clone()),
                records: Vec::new(),
            });
        if journal.path != path {
            return Err(format!(
                "participant {} changed journal from {} to {}",
                participant.node_name,
                journal.path.display(),
                path.display()
            ));
        }
        let next = if finished {
            journal.reader.finish()
        } else {
            journal.reader.read_available()
        }
        .map_err(|error| {
            format!(
                "participant {} journal {}: {error}; last sequence {:?}",
                participant.node_name,
                path.display(),
                journal.records.last().map(|record| record.sequence)
            )
        })?;
        journal.records.extend(next);
        events.extend(
            journal
                .records
                .iter()
                .cloned()
                .map(|record| RuntimeObservation {
                    node: participant.node_name.clone(),
                    peer_id: binding.peer_id.clone(),
                    path: path.clone(),
                    record,
                }),
        );
    }
    drop(journals);
    Ok(events)
}

pub(super) async fn wait_for_observations(
    world: &CucumberWorld,
    timeout_seconds: u64,
    mut predicate: impl FnMut(&[RuntimeObservation]) -> bool,
) -> Result<Vec<RuntimeObservation>, StepError> {
    let deadline = Instant::now() + Duration::from_secs(timeout_seconds);
    loop {
        let observations = collect_observations(world, false).map_err(failure)?;
        if predicate(&observations) {
            return Ok(observations);
        }
        if Instant::now() >= deadline {
            let recent = observations
                .iter()
                .rev()
                .take(12)
                .map(|event| {
                    format!(
                        "{} {}#{} {:?}",
                        event.node,
                        event.path.display(),
                        event.record.sequence,
                        event.record.event
                    )
                })
                .collect::<Vec<_>>()
                .join("; ");
            return Err(failure(format!(
                "timed out after {timeout_seconds}s waiting for typed observations; recent: {recent}"
            )));
        }
        sleep(Duration::from_millis(200)).await;
    }
}

pub(super) fn observation_reference(event: &RuntimeObservation) -> serde_json::Value {
    serde_json::json!({
        "node": event.node,
        "peer_id": event.peer_id,
        "journal": event.path,
        "sequence": event.record.sequence,
        "event_type": serde_json::to_value(&event.record.event).ok().and_then(|value| value.get("type").cloned()),
    })
}

pub(super) fn era_protocols(node: &NodeInfo) -> Result<(u64, String, String), StepError> {
    era_protocols_from_path(&node.runtime_dir.join("deployment.yaml"))
}

pub(super) fn era_protocols_from_path(
    deployment_path: &Path,
) -> Result<(u64, String, String), StepError> {
    let deployment_yaml = fs::read_to_string(deployment_path).map_err(|error| {
        failure(format!(
            "Cannot read {}: {error}",
            deployment_path.display()
        ))
    })?;
    let deployment: DeploymentSettings =
        serde_yaml::from_str(&deployment_yaml).map_err(|error| {
            failure(format!(
                "Cannot decode {}: {error}",
                deployment_path.display()
            ))
        })?;
    let eras = deployment.era_schedule().iter().collect::<Vec<_>>();
    if eras.len() < 2 {
        return Err(failure("Recovery regression requires eras 0 and 1"));
    }
    let boundary_slot = eras[1].first_slot.into_inner();
    let f0 = ProtocolScope::Fork(eras[0].parameters.fork_digest).to_string_with_name("chainsync");
    let f1 = ProtocolScope::Fork(eras[1].parameters.fork_digest).to_string_with_name("chainsync");
    Ok((boundary_slot, f0, f1))
}

pub(super) fn era_protocols_for_world(
    world: &CucumberWorld,
    node_name: &str,
) -> Result<(u64, String, String), StepError> {
    let node = world
        .nodes_info
        .get(node_name)
        .ok_or_else(|| failure(format!("Node `{node_name}` has not started")))?;
    era_protocols(node)
}

pub(super) fn node_peer_id(world: &CucumberWorld, node_name: &str) -> Option<String> {
    world
        .cluster
        .node_peer_ids
        .get(node_name)
        .map(ToString::to_string)
        .or_else(|| {
            world
                .era_transition
                .as_ref()?
                .runtime_bindings
                .get(node_name)
                .map(|binding| binding.peer_id.clone())
        })
}

fn failure(message: impl Into<String>) -> StepError {
    StepError::StepFail {
        message: message.into(),
    }
}
