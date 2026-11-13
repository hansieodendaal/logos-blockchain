use lb_core::header::HeaderId;
use lb_cryptarchia_sync::observation::{
    InterruptionKind, Observation, ObservationRecord, RequestObservation, SCHEMA_VERSION,
};
use libp2p::PeerId;

use super::{
    events::RuntimeObservation,
    evidence::{
        participant_configuration_is_verified, participant_suppression_is_verified,
        verify_partial_progress_record, verify_runtime_lifecycle, verify_shared_convergence,
    },
    lifecycle,
};

fn id(n: u8) -> HeaderId {
    HeaderId::try_from([n; 32].as_slice()).unwrap()
}

#[expect(
    clippy::too_many_lines,
    reason = "the fixture specifies the complete two-node lifecycle"
)]
fn fixture() -> (Vec<RuntimeObservation>, String, String) {
    let provider = PeerId::random();
    let requester = PeerId::random();
    let original = RequestObservation {
        local_request_id: 1,
        remote_peer: provider.into(),
        protocol: "f0".into(),
        target: id(1),
        requester_tip: id(2),
        latest_immutable: id(3),
    };
    let original_provider = RequestObservation {
        local_request_id: 11,
        remote_peer: requester.into(),
        ..original.clone()
    };
    let replacement = RequestObservation {
        local_request_id: 2,
        protocol: "f1".into(),
        requester_tip: id(4),
        ..original.clone()
    };
    let replacement_provider = RequestObservation {
        local_request_id: 12,
        remote_peer: requester.into(),
        ..replacement.clone()
    };
    let mut events = Vec::new();
    let mut push = |node: &str, sequence: u64, event: Observation| {
        let (peer_id, timestamp_unix_ms) = if node == "provider" {
            (provider.to_string(), 1)
        } else {
            (requester.to_string(), 1000)
        };
        events.push(RuntimeObservation {
            node: node.into(),
            peer_id,
            path: format!("{node}.ndjson").into(),
            record: ObservationRecord {
                schema_version: SCHEMA_VERSION,
                sequence,
                timestamp_unix_ms,
                elapsed_ns: sequence * 10,
                event,
            },
        });
    };
    push(
        "requester",
        1,
        Observation::RequesterRequestStarted {
            request: original.clone(),
        },
    );
    push(
        "provider",
        1,
        Observation::ProviderRequestAccepted {
            request: original_provider.clone(),
        },
    );
    push(
        "provider",
        2,
        Observation::ProviderBlockResponseSent {
            request: original_provider.clone(),
            response_index: 1,
        },
    );
    push(
        "requester",
        2,
        Observation::RequesterBlockResponseReceived {
            request: original.clone(),
            response_index: 1,
        },
    );
    push(
        "requester",
        3,
        Observation::RequesterFirstBlockDecoded {
            remote_peer: provider.into(),
            target: id(1),
            requester_tip: id(2),
            latest_immutable: id(3),
            block_id: id(5),
        },
    );
    push(
        "requester",
        4,
        Observation::IbdDownloaderDownloading { target: id(1) },
    );
    push(
        "requester",
        5,
        Observation::IbdBlockApplied { block_id: id(5) },
    );
    push(
        "provider",
        3,
        Observation::ProviderResponseHoldActivated {
            request: original_provider.clone(),
            blocks_sent: 1,
        },
    );
    push(
        "provider",
        4,
        Observation::EraRotationStarted {
            previous_protocol: "f0".into(),
            new_protocol: "f1".into(),
            pending_provider_responses: 1,
        },
    );
    push(
        "requester",
        6,
        Observation::EraRotationStarted {
            previous_protocol: "f0".into(),
            new_protocol: "f1".into(),
            pending_provider_responses: 0,
        },
    );
    push(
        "provider",
        5,
        Observation::EraTransitionExpired {
            old_protocol: "f0".into(),
            pending_provider_responses: 1,
        },
    );
    push(
        "provider",
        6,
        Observation::ProviderResponseHoldCancelled {
            request: original_provider,
            blocks_sent: 1,
        },
    );
    push(
        "requester",
        7,
        Observation::RequesterInterruptionDetected {
            request: original,
            response_count: 1,
            kind: InterruptionKind::StreamError,
        },
    );
    push(
        "requester",
        8,
        Observation::IbdDownloadStreamError {
            target: id(1),
            blocks_received: 1,
        },
    );
    push(
        "requester",
        9,
        Observation::IbdRequestStarted {
            target: id(1),
            requester_tip: id(4),
            latest_immutable: id(3),
        },
    );
    push(
        "requester",
        10,
        Observation::RequesterRequestStarted {
            request: replacement.clone(),
        },
    );
    push(
        "provider",
        7,
        Observation::ProviderRequestAccepted {
            request: replacement_provider.clone(),
        },
    );
    push(
        "provider",
        8,
        Observation::ProviderRequestCompleted {
            request: replacement_provider,
            blocks_sent: 1,
        },
    );
    push(
        "requester",
        11,
        Observation::RequesterRequestCompleted {
            request: replacement,
            response_count: 1,
        },
    );
    push("requester", 12, Observation::IbdCompleted);
    (events, provider.to_string(), requester.to_string())
}

fn failures(events: &[RuntimeObservation], provider: &str, requester: &str) -> Vec<String> {
    let mut failures = Vec::new();
    verify_runtime_lifecycle(
        events,
        "provider",
        "requester",
        provider,
        requester,
        "f0",
        "f1",
        &mut failures,
    );
    failures
}

#[expect(
    clippy::too_many_lines,
    reason = "the fixture keeps each participant's monotonic event sequence explicit"
)]
fn add_unselected_request(events: &mut Vec<RuntimeObservation>, provider_peer: &str) -> String {
    let unselected_peer = PeerId::random();
    let unselected_peer_id = unselected_peer.to_string();
    for event in events
        .iter_mut()
        .filter(|event| event.node == "provider" && event.record.sequence >= 4)
    {
        event.record.sequence += 4;
    }
    let provider_identity = provider_peer.parse::<PeerId>().unwrap();
    let request = RequestObservation {
        local_request_id: 5,
        remote_peer: provider_identity.into(),
        protocol: "f0".into(),
        target: id(1),
        requester_tip: id(2),
        latest_immutable: id(3),
    };
    let provider_request = RequestObservation {
        local_request_id: 99,
        remote_peer: unselected_peer.into(),
        ..request.clone()
    };
    let mut push = |node: &str, peer_id: &str, path: &str, sequence: u64, event| {
        events.push(RuntimeObservation {
            node: node.into(),
            peer_id: peer_id.into(),
            path: path.into(),
            record: ObservationRecord {
                schema_version: SCHEMA_VERSION,
                sequence,
                timestamp_unix_ms: 0,
                elapsed_ns: sequence * 10,
                event,
            },
        });
    };
    push(
        "provider",
        provider_peer,
        "provider.ndjson",
        4,
        Observation::ProviderRequestAccepted {
            request: provider_request.clone(),
        },
    );
    push(
        "provider",
        provider_peer,
        "provider.ndjson",
        5,
        Observation::ProviderBlockResponseSent {
            request: provider_request.clone(),
            response_index: 1,
        },
    );
    push(
        "provider",
        provider_peer,
        "provider.ndjson",
        6,
        Observation::ProviderBlockResponseSent {
            request: provider_request.clone(),
            response_index: 2,
        },
    );
    push(
        "provider",
        provider_peer,
        "provider.ndjson",
        7,
        Observation::ProviderRequestCompleted {
            request: provider_request,
            blocks_sent: 2,
        },
    );
    push(
        "unselected",
        &unselected_peer_id,
        "unselected.ndjson",
        1,
        Observation::RequesterRequestStarted {
            request: request.clone(),
        },
    );
    push(
        "unselected",
        &unselected_peer_id,
        "unselected.ndjson",
        2,
        Observation::RequesterBlockResponseReceived {
            request: request.clone(),
            response_index: 1,
        },
    );
    push(
        "unselected",
        &unselected_peer_id,
        "unselected.ndjson",
        3,
        Observation::RequesterBlockResponseReceived {
            request: request.clone(),
            response_index: 2,
        },
    );
    push(
        "unselected",
        &unselected_peer_id,
        "unselected.ndjson",
        4,
        Observation::RequesterRequestCompleted {
            request,
            response_count: 2,
        },
    );
    push(
        "unselected",
        &unselected_peer_id,
        "unselected.ndjson",
        5,
        Observation::IbdCompleted,
    );
    unselected_peer_id
}

fn concurrent_completion<'a>(
    events: &'a [RuntimeObservation],
    provider_peer: &str,
    requester_peer: &str,
    unselected_peer: &str,
) -> Result<lifecycle::UnselectedCompletion<'a>, &'static str> {
    let selected = lifecycle::original(
        events,
        "provider",
        "requester",
        provider_peer,
        requester_peer,
        "f0",
    )?;
    lifecycle::unselected_completion(
        events,
        &selected,
        "provider",
        provider_peer,
        requester_peer,
        "unselected",
        unselected_peer,
        "f0",
    )
}

#[test]
fn typed_lifecycle_accepts_reversed_wall_clocks_and_distinct_local_ids() {
    let (events, provider, requester) = fixture();
    assert_eq!(
        failures(&events, &provider, &requester),
        Vec::<String>::new()
    );
}

#[test]
fn concurrent_f0_requests_overlap_and_unselected_request_completes_normally() {
    let (mut events, provider, requester) = fixture();
    let unselected_peer = add_unselected_request(&mut events, &provider);
    let completion = concurrent_completion(&events, &provider, &requester, &unselected_peer)
        .expect("normal F0 completion should correlate independently");
    assert_eq!(completion.provider_request.protocol, "f0");
    assert_eq!(completion.request.protocol, "f0");
    assert!(completion.provider_request.local_request_id != 11);
}

#[test]
fn ambiguous_or_incomplete_unselected_lifecycle_fails_closed() {
    let (mut events, provider, requester) = fixture();
    let unselected_peer = add_unselected_request(&mut events, &provider);
    let duplicate_start = events
        .iter()
        .find(|event| {
            event.node == "unselected"
                && matches!(
                    event.record.event,
                    Observation::RequesterRequestStarted { .. }
                )
        })
        .unwrap()
        .clone();
    let mut duplicate_start = duplicate_start;
    duplicate_start.record.sequence = 4;
    events.push(duplicate_start);
    assert!(concurrent_completion(&events, &provider, &requester, &unselected_peer).is_err());

    let (mut events, provider, requester) = fixture();
    let unselected_peer = add_unselected_request(&mut events, &provider);
    events.retain(|event| {
        !(event.node == "provider"
            && matches!(&event.record.event, Observation::ProviderRequestCompleted { request, .. } if request.local_request_id == 99))
    });
    assert!(concurrent_completion(&events, &provider, &requester, &unselected_peer).is_err());
}

#[test]
fn contradictory_unselected_interruption_or_late_completion_is_rejected() {
    let (mut events, provider, requester) = fixture();
    let unselected_peer = add_unselected_request(&mut events, &provider);
    let request = events
        .iter()
        .find_map(|event| match &event.record.event {
            Observation::RequesterRequestStarted { request } if event.node == "unselected" => {
                Some(request.clone())
            }
            _ => None,
        })
        .unwrap();
    events.push(RuntimeObservation {
        node: "unselected".into(),
        peer_id: unselected_peer.clone(),
        path: "unselected.ndjson".into(),
        record: ObservationRecord {
            schema_version: SCHEMA_VERSION,
            sequence: 4,
            timestamp_unix_ms: 0,
            elapsed_ns: 40,
            event: Observation::RequesterInterruptionDetected {
                request,
                response_count: 1,
                kind: InterruptionKind::StreamError,
            },
        },
    });
    assert!(concurrent_completion(&events, &provider, &requester, &unselected_peer).is_err());

    let (mut events, provider, requester) = fixture();
    let unselected_peer = add_unselected_request(&mut events, &provider);
    let expiry = events
        .iter_mut()
        .find(|event| {
            event.node == "provider"
                && matches!(event.record.event, Observation::EraTransitionExpired { .. })
        })
        .unwrap();
    expiry.record.sequence = 5;
    assert!(concurrent_completion(&events, &provider, &requester, &unselected_peer).is_err());
}

#[test]
fn requester_interruption_before_its_f0_to_f1_rotation_is_rejected() {
    let (mut events, provider, requester) = fixture();
    let rotation = events
        .iter()
        .position(|event| {
            event.node == "requester"
                && matches!(event.record.event, Observation::EraRotationStarted { .. })
        })
        .unwrap();
    let interruption = events
        .iter()
        .position(|event| {
            event.node == "requester"
                && matches!(
                    event.record.event,
                    Observation::RequesterInterruptionDetected { .. }
                )
        })
        .unwrap();

    // Keep all observations, but put the interruption at sequence 6 and the
    // requester's rotation at sequence 7. The older verifier accepted this.
    events[rotation].record.sequence = 7;
    events[interruption].record.sequence = 6;
    let rotation_sequence = events
        .iter()
        .find(|event| {
            event.node == "requester"
                && matches!(event.record.event, Observation::EraRotationStarted { .. })
        })
        .unwrap()
        .record
        .sequence;
    let interruption_sequence = events
        .iter()
        .find(|event| {
            event.node == "requester"
                && matches!(
                    event.record.event,
                    Observation::RequesterInterruptionDetected { .. }
                )
        })
        .unwrap()
        .record
        .sequence;
    assert_eq!((rotation_sequence, interruption_sequence), (7, 6));

    assert_ne!(
        failures(&events, &provider, &requester),
        Vec::<String>::new()
    );
}

#[test]
fn normal_completion_of_held_f0_request_after_expiry_is_rejected() {
    let (mut events, provider, requester) = fixture();
    let (provider_peer, path, held_request) = events
        .iter()
        .find_map(|event| {
            (event.node == "provider")
                .then(|| match &event.record.event {
                    Observation::ProviderRequestAccepted { request }
                        if request.protocol == "f0" =>
                    {
                        Some((event.peer_id.clone(), event.path.clone(), request.clone()))
                    }
                    _ => None,
                })
                .flatten()
        })
        .unwrap();
    events.push(RuntimeObservation {
        node: "provider".into(),
        peer_id: provider_peer,
        path,
        record: ObservationRecord {
            schema_version: SCHEMA_VERSION,
            sequence: 9,
            timestamp_unix_ms: 2,
            elapsed_ns: 90,
            event: Observation::ProviderRequestCompleted {
                request: held_request,
                blocks_sent: 1,
            },
        },
    });

    assert_ne!(
        failures(&events, &provider, &requester),
        Vec::<String>::new()
    );
}

#[test]
fn mandatory_lifecycle_observations_are_required() {
    let (events, provider, requester) = fixture();
    for index in 0..events.len() {
        let mut missing = events.clone();
        missing.remove(index);
        assert!(
            !failures(&missing, &provider, &requester).is_empty(),
            "missing index {index} unexpectedly passed"
        );
    }
}

#[test]
fn reused_request_context_is_rejected_but_unrelated_f1_traffic_is_allowed() {
    let (mut events, provider, requester) = fixture();
    for event in &mut events {
        if event.node == "requester" && event.record.sequence >= 10 {
            event.record.sequence += 1;
        }
    }
    let started = events.iter().find(|e| matches!(&e.record.event, Observation::RequesterRequestStarted { request } if request.protocol == "f1")).unwrap().clone();
    let mut unrelated = started.clone();
    unrelated.record.sequence = 10;
    if let Observation::RequesterRequestStarted { request } = &mut unrelated.record.event {
        request.local_request_id = 99;
        request.target = id(9);
    }
    events.push(unrelated);
    assert_eq!(
        failures(&events, &provider, &requester),
        Vec::<String>::new()
    );
    let mut duplicate = started;
    duplicate.record.sequence = 21;
    if let Observation::RequesterRequestStarted { request } = &mut duplicate.record.event {
        request.local_request_id = 100;
    }
    events.push(duplicate);
    assert_ne!(
        failures(&events, &provider, &requester),
        Vec::<String>::new()
    );
}

#[test]
fn wrong_owner_protocol_or_request_identity_is_rejected() {
    let (events, provider, requester) = fixture();
    for index in [0, 1, 2, 3, 10, 17, 18] {
        let mut bad = events.clone();
        match &mut bad[index].record.event {
            Observation::RequesterRequestStarted { request } => request.protocol = "wrong".into(),
            Observation::ProviderRequestAccepted { request } => {
                request.remote_peer = PeerId::random().into();
            }
            Observation::RequesterBlockResponseReceived { request, .. } => {
                request.target = id(9);
            }
            Observation::EraTransitionExpired { old_protocol, .. } => {
                *old_protocol = "wrong".into();
            }
            Observation::ProviderBlockResponseSent { request, .. }
            | Observation::ProviderRequestCompleted { request, .. }
            | Observation::RequesterRequestCompleted { request, .. } => {
                request.local_request_id += 1;
            }
            _ => unreachable!(),
        }
        assert_ne!(failures(&bad, &provider, &requester), Vec::<String>::new());
    }
    let mut wrong_owner = events;
    wrong_owner[0].peer_id = provider.clone();
    assert_ne!(
        failures(&wrong_owner, &provider, &requester),
        Vec::<String>::new()
    );
}

#[test]
fn partial_progress_and_finality_require_explicit_state_evidence() {
    let mut errors = Vec::new();
    verify_partial_progress_record(&[], &mut errors);
    verify_shared_convergence(&[], ["provider", "requester"].into_iter(), &mut errors);
    assert_ne!(errors, Vec::<String>::new());

    for field in [
        "partial_block_available_after_recovery",
        "partial_block_is_ancestor_of_replacement_tip",
        "partial_block_is_ancestor_of_final_tip",
        "original_target_available_after_recovery",
        "original_target_is_ancestor_of_final_tip",
    ] {
        let mut record = serde_json::json!({"event":"partial_progress_and_ibd_completion", "data":{
            "partial_block_available_after_recovery":true,
            "partial_block_is_ancestor_of_replacement_tip":true,
            "partial_block_is_ancestor_of_final_tip":true,
            "original_target_available_after_recovery":true,
            "original_target_is_ancestor_of_final_tip":true
        }});
        record["data"][field] = serde_json::json!(false);
        let mut failures = Vec::new();
        verify_partial_progress_record(&[record], &mut failures);
        assert!(failures.iter().any(|failure| failure.contains(field)));
    }
}

#[test]
fn suppression_and_configuration_are_attributed_to_the_participant() {
    let disabled =
        serde_json::json!({"node": "provider", "status": "passed", "publication_enabled": false});
    assert!(participant_suppression_is_verified(&disabled, "provider"));
    assert!(!participant_suppression_is_verified(&disabled, "requester"));
    let config = serde_json::json!({"event": "node_configuration_provenance", "data": {"node": "provider", "node_config_path": "node.yaml", "deployment_path": "deployment.yaml"}});
    assert!(participant_configuration_is_verified(&config, "provider"));
    assert!(!participant_configuration_is_verified(&config, "requester"));
}
