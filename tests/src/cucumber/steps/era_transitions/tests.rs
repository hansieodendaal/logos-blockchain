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
        additional_blocks: Vec::new(),
    };
    let original_provider = RequestObservation {
        local_request_id: 1,
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
    reason = "the synthetic fixture models the complete IBD-to-orphan-to-F1 lifecycle"
)]
fn orphan_fixture() -> (Vec<RuntimeObservation>, String, String) {
    let provider_id = PeerId::random();
    let requester_id = PeerId::random();
    let ibd_request = RequestObservation {
        local_request_id: 1,
        remote_peer: provider_id.into(),
        protocol: "f0".into(),
        target: id(40),
        requester_tip: id(41),
        latest_immutable: id(42),
        additional_blocks: Vec::new(),
    };
    let ibd_provider_request = RequestObservation {
        local_request_id: 19,
        remote_peer: requester_id.into(),
        ..ibd_request.clone()
    };
    let f0_request = RequestObservation {
        local_request_id: 2,
        remote_peer: provider_id.into(),
        protocol: "f0".into(),
        target: id(30),
        requester_tip: id(41),
        latest_immutable: id(42),
        additional_blocks: Vec::new(),
    };
    let f0_provider_request = RequestObservation {
        local_request_id: 20,
        remote_peer: requester_id.into(),
        ..f0_request.clone()
    };
    let f1_request = RequestObservation {
        local_request_id: 3,
        protocol: "f1".into(),
        ..f0_request.clone()
    };
    let f1_provider_request = RequestObservation {
        local_request_id: 21,
        remote_peer: requester_id.into(),
        ..f1_request.clone()
    };
    let mut events = Vec::new();
    let mut push = |node: &str, sequence: u64, event| {
        let peer_id = if node == "provider" {
            provider_id.to_string()
        } else {
            requester_id.to_string()
        };
        events.push(RuntimeObservation {
            node: node.into(),
            peer_id,
            path: format!("{node}.ndjson").into(),
            record: ObservationRecord {
                schema_version: SCHEMA_VERSION,
                sequence,
                timestamp_unix_ms: if node == "provider" { 10_000 } else { 1 },
                elapsed_ns: sequence * 10,
                event,
            },
        });
    };
    push(
        "requester",
        1,
        Observation::IbdRequestStarted {
            target: ibd_request.target,
            requester_tip: ibd_request.requester_tip,
            latest_immutable: ibd_request.latest_immutable,
        },
    );
    push(
        "requester",
        2,
        Observation::RequesterRequestStarted {
            request: ibd_request,
        },
    );
    push(
        "requester",
        3,
        Observation::IbdBlockApplied { block_id: id(24) },
    );
    push("requester", 4, Observation::IbdCompleted);
    push(
        "requester",
        5,
        Observation::OrphanProposalMissingParent {
            orphan_id: id(30),
            missing_parent: id(29),
            requester_tip: id(41),
            latest_immutable: id(42),
        },
    );
    push(
        "requester",
        6,
        Observation::OrphanDownloadRequestStarted {
            orphan_id: id(30),
            missing_parent: Some(id(29)),
            requester_tip: id(41),
            latest_immutable: id(42),
            known_block: None,
        },
    );
    push(
        "requester",
        7,
        Observation::RequesterRequestStarted {
            request: f0_request.clone(),
        },
    );
    push(
        "requester",
        8,
        Observation::OrphanDownloaderDownloading { target: id(30) },
    );
    push(
        "provider",
        1,
        Observation::ProviderRequestAccepted {
            request: f0_provider_request.clone(),
        },
    );
    push(
        "provider",
        2,
        Observation::ProviderBlockResponseSent {
            request: f0_provider_request.clone(),
            response_index: 1,
        },
    );
    for response_index in 2u64..=5 {
        push(
            "provider",
            response_index + 1,
            Observation::ProviderBlockResponseSent {
                request: f0_provider_request.clone(),
                response_index,
            },
        );
    }
    push(
        "provider",
        8,
        Observation::ProviderResponseHoldActivated {
            request: f0_provider_request.clone(),
            blocks_sent: 5,
        },
    );
    push(
        "requester",
        9,
        Observation::RequesterBlockResponseReceived {
            request: f0_request.clone(),
            response_index: 1,
        },
    );
    push(
        "requester",
        10,
        Observation::RequesterFirstBlockDecoded {
            remote_peer: provider_id.into(),
            target: id(30),
            requester_tip: id(41),
            latest_immutable: id(42),
            block_id: id(24),
        },
    );
    push(
        "requester",
        11,
        Observation::RequesterBlockResponseReceived {
            request: f0_request.clone(),
            response_index: 2,
        },
    );
    for response_index in 3u64..=5 {
        push(
            "requester",
            response_index + 9,
            Observation::RequesterBlockResponseReceived {
                request: f0_request.clone(),
                response_index,
            },
        );
    }
    push(
        "requester",
        16,
        Observation::OrphanDownloadBlockApplied { block_id: id(25) },
    );
    push(
        "provider",
        9,
        Observation::EraRotationStarted {
            previous_protocol: "f0".into(),
            new_protocol: "f1".into(),
            pending_provider_responses: 1,
        },
    );
    push(
        "requester",
        17,
        Observation::EraRotationStarted {
            previous_protocol: "f0".into(),
            new_protocol: "f1".into(),
            pending_provider_responses: 0,
        },
    );
    push(
        "provider",
        10,
        Observation::EraTransitionExpired {
            old_protocol: "f0".into(),
            pending_provider_responses: 1,
        },
    );
    push(
        "provider",
        11,
        Observation::ProviderResponseHoldCancelled {
            request: f0_provider_request,
            blocks_sent: 5,
        },
    );
    push(
        "requester",
        18,
        Observation::RequesterInterruptionDetected {
            request: f0_request,
            response_count: 5,
            kind: InterruptionKind::StreamError,
        },
    );
    push(
        "requester",
        19,
        Observation::OrphanDownloadStreamError {
            orphan_id: id(30),
            blocks_received: 5,
        },
    );
    push(
        "requester",
        20,
        Observation::OrphanDownloadRecoveryScheduled {
            orphan_id: id(30),
            missing_parent: Some(id(29)),
            known_block: Some(id(25)),
            blocks_received: 5,
        },
    );
    push(
        "requester",
        21,
        Observation::OrphanDownloadRequestStarted {
            orphan_id: id(30),
            missing_parent: Some(id(29)),
            requester_tip: id(41),
            latest_immutable: id(42),
            known_block: Some(id(25)),
        },
    );
    push(
        "requester",
        22,
        Observation::RequesterRequestStarted {
            request: f1_request.clone(),
        },
    );
    push(
        "requester",
        23,
        Observation::OrphanDownloaderDownloading { target: id(30) },
    );
    push(
        "provider",
        12,
        Observation::ProviderRequestAccepted {
            request: f1_provider_request.clone(),
        },
    );
    push(
        "provider",
        13,
        Observation::ProviderBlockResponseSent {
            request: f1_provider_request.clone(),
            response_index: 1,
        },
    );
    push(
        "requester",
        24,
        Observation::RequesterBlockResponseReceived {
            request: f1_request.clone(),
            response_index: 1,
        },
    );
    push(
        "requester",
        25,
        Observation::RequesterRequestCompleted {
            request: f1_request,
            response_count: 1,
        },
    );
    push(
        "provider",
        14,
        Observation::ProviderRequestCompleted {
            request: f1_provider_request,
            blocks_sent: 1,
        },
    );
    push(
        "provider",
        15,
        Observation::ProviderRequestAccepted {
            request: ibd_provider_request.clone(),
        },
    );
    push(
        "provider",
        16,
        Observation::ProviderRequestCompleted {
            request: ibd_provider_request,
            blocks_sent: 1,
        },
    );
    push(
        "requester",
        26,
        Observation::OrphanDownloadCompleted {
            orphan_id: id(30),
            blocks_received: 1,
        },
    );
    push(
        "requester",
        27,
        Observation::OrphanDownloadBlockApplied { block_id: id(30) },
    );
    (events, provider_id.to_string(), requester_id.to_string())
}

fn orphan_lifecycle<'a>(
    events: &'a [RuntimeObservation],
    provider_peer: &str,
    requester_peer: &str,
) -> Result<
    (
        lifecycle::OrphanOriginal<'a>,
        lifecycle::OrphanExpiry<'a>,
        lifecycle::OrphanReplacement<'a>,
        lifecycle::OrphanCompletion<'a>,
    ),
    &'static str,
> {
    let original = lifecycle::orphan_original(
        events,
        "provider",
        "requester",
        provider_peer,
        requester_peer,
        "f0",
    )?;
    let expiry = lifecycle::orphan_expiry(events, &original, "provider", "requester", "f0", "f1")?;
    let replacement = lifecycle::orphan_replacement(
        events,
        &original,
        &expiry,
        "provider",
        "requester",
        provider_peer,
        requester_peer,
        "f1",
    )?;
    let completion =
        lifecycle::orphan_completed(events, &original, &replacement, "provider", "requester")?;
    Ok((original, expiry, replacement, completion))
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
        additional_blocks: Vec::new(),
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
    let control_provider_peer = provider_peer.to_owned();
    let control = lifecycle::InteractionPeers {
        provider: "provider",
        provider_peer: &control_provider_peer,
        requester: "unselected",
        requester_peer: unselected_peer,
    };
    lifecycle::unselected_completion(events, &selected, &control, "f0")
}

fn add_request_on_second_provider(
    events: &mut Vec<RuntimeObservation>,
    provider_peer: PeerId,
    requester_peer: PeerId,
) {
    let requester_request = RequestObservation {
        local_request_id: 1,
        remote_peer: provider_peer.into(),
        protocol: "f0".into(),
        target: id(30),
        requester_tip: id(31),
        latest_immutable: id(32),
        additional_blocks: Vec::new(),
    };
    let provider_request = RequestObservation {
        local_request_id: 1,
        remote_peer: requester_peer.into(),
        ..requester_request.clone()
    };
    let mut push = |node: &str, peer_id: PeerId, sequence: u64, event| {
        events.push(RuntimeObservation {
            node: node.into(),
            peer_id: peer_id.to_string(),
            path: format!("{node}.ndjson").into(),
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
        "control-provider",
        provider_peer,
        1,
        Observation::ProviderRequestAccepted {
            request: provider_request.clone(),
        },
    );
    push(
        "control-provider",
        provider_peer,
        2,
        Observation::ProviderBlockResponseSent {
            request: provider_request.clone(),
            response_index: 1,
        },
    );
    push(
        "control-provider",
        provider_peer,
        3,
        Observation::ProviderRequestCompleted {
            request: provider_request,
            blocks_sent: 1,
        },
    );
    push(
        "control-requester",
        requester_peer,
        1,
        Observation::RequesterRequestStarted {
            request: requester_request.clone(),
        },
    );
    push(
        "control-requester",
        requester_peer,
        2,
        Observation::RequesterBlockResponseReceived {
            request: requester_request.clone(),
            response_index: 1,
        },
    );
    push(
        "control-requester",
        requester_peer,
        3,
        Observation::RequesterRequestCompleted {
            request: requester_request,
            response_count: 1,
        },
    );
    push(
        "control-requester",
        requester_peer,
        4,
        Observation::IbdCompleted,
    );
}

fn completion_for_interaction<'a>(
    events: &'a [RuntimeObservation],
    selected_provider_peer: &str,
    selected_requester_peer: &str,
    control_provider: &str,
    control_provider_peer: &str,
    control_requester_peer: &str,
) -> Result<lifecycle::UnselectedCompletion<'a>, &'static str> {
    let selected = lifecycle::original(
        events,
        "provider",
        "requester",
        selected_provider_peer,
        selected_requester_peer,
        "f0",
    )?;
    let control = lifecycle::InteractionPeers {
        provider: control_provider,
        provider_peer: control_provider_peer,
        requester: "control-requester",
        requester_peer: control_requester_peer,
    };
    lifecycle::unselected_completion(events, &selected, &control, "f0")
}

#[test]
fn typed_lifecycle_accepts_reversed_wall_clocks_and_process_local_request_ids() {
    let (events, provider, requester) = fixture();
    assert_eq!(
        failures(&events, &provider, &requester),
        Vec::<String>::new()
    );
}

#[test]
fn selected_hold_identifies_one_of_multiple_same_peer_request_contexts() {
    let (mut events, provider, requester) = fixture();
    let selected = events
        .iter()
        .find_map(|event| match &event.record.event {
            Observation::RequesterRequestStarted { request } if event.node == "requester" => {
                Some(request.clone())
            }
            _ => None,
        })
        .unwrap();
    let mut competing = selected;
    competing.local_request_id = 42;
    competing.target = id(42);
    competing.requester_tip = id(43);
    events.push(RuntimeObservation {
        node: "requester".into(),
        peer_id: requester.clone(),
        path: "requester.ndjson".into(),
        record: ObservationRecord {
            schema_version: SCHEMA_VERSION,
            sequence: 100,
            timestamp_unix_ms: 999,
            elapsed_ns: 1000,
            event: Observation::RequesterRequestStarted { request: competing },
        },
    });
    assert_eq!(
        lifecycle::original(
            &events,
            "provider",
            "requester",
            &provider,
            &requester,
            "f0"
        )
        .unwrap()
        .request
        .local_request_id,
        1
    );
}

#[test]
fn identical_request_contexts_and_duplicate_hold_evidence_fail_closed() {
    let (mut events, provider, requester) = fixture();
    let mut duplicate_request = events
        .iter()
        .find_map(|event| match &event.record.event {
            Observation::RequesterRequestStarted { request } if event.node == "requester" => {
                Some(request.clone())
            }
            _ => None,
        })
        .unwrap();
    duplicate_request.local_request_id = 77;
    events.push(RuntimeObservation {
        node: "requester".into(),
        peer_id: requester.clone(),
        path: "requester.ndjson".into(),
        record: ObservationRecord {
            schema_version: SCHEMA_VERSION,
            sequence: 100,
            timestamp_unix_ms: 999,
            elapsed_ns: 1000,
            event: Observation::RequesterRequestStarted {
                request: duplicate_request,
            },
        },
    });
    assert!(
        lifecycle::original(
            &events,
            "provider",
            "requester",
            &provider,
            &requester,
            "f0"
        )
        .is_err()
    );

    let (mut events, provider, requester) = fixture();
    let duplicate_hold = events
        .iter()
        .find(|event| {
            matches!(
                event.record.event,
                Observation::ProviderResponseHoldActivated { .. }
            )
        })
        .unwrap()
        .clone();
    let mut duplicate_hold = duplicate_hold;
    duplicate_hold.record.sequence = 100;
    events.push(duplicate_hold);
    assert!(
        lifecycle::original(
            &events,
            "provider",
            "requester",
            &provider,
            &requester,
            "f0"
        )
        .is_err()
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
fn request_ids_are_local_across_distinct_provider_requester_interactions() {
    let (mut events, selected_provider_peer, selected_requester_peer) = fixture();
    let control_provider = PeerId::random();
    let control_requester = PeerId::random();
    add_request_on_second_provider(&mut events, control_provider, control_requester);
    let completion = completion_for_interaction(
        &events,
        &selected_provider_peer,
        &selected_requester_peer,
        "control-provider",
        &control_provider.to_string(),
        &control_requester.to_string(),
    )
    .expect("independent provider/requester request IDs may be equal");
    assert_eq!(completion.provider_request.local_request_id, 1);
    assert_eq!(completion.request.local_request_id, 1);
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
fn runtime_orphan_lifecycle_is_distinguished_from_completed_ibd() {
    let (events, provider, requester) = orphan_fixture();
    let (original, _, replacement, completion) = orphan_lifecycle(&events, &provider, &requester)
        .expect("typed post-IBD orphan request should complete under F1");
    assert_eq!(original.orphan_id, id(30));
    assert_eq!(original.missing_parent, id(29));
    assert_eq!(original.partial_block, id(25));
    assert_eq!(original.held_blocks, 5);
    assert_eq!(replacement.request.protocol, "f1");
    assert_eq!(completion.target_applied.node, "requester");

    let mut mislabeled = events;
    mislabeled.push(RuntimeObservation {
        node: "requester".into(),
        peer_id: requester.clone(),
        path: "requester.ndjson".into(),
        record: ObservationRecord {
            schema_version: SCHEMA_VERSION,
            sequence: 22,
            timestamp_unix_ms: 0,
            elapsed_ns: 220,
            event: Observation::IbdRequestStarted {
                target: id(30),
                requester_tip: id(41),
                latest_immutable: id(42),
            },
        },
    });
    assert!(orphan_lifecycle(&mislabeled, &provider, &requester).is_err());
}

#[test]
fn runtime_orphan_lifecycle_requires_initial_ibd_from_the_same_provider() {
    let (mut events, provider, requester) = orphan_fixture();
    let initial_ibd_request = events
        .iter_mut()
        .find(|event| {
            event.node == "requester"
                && matches!(&event.record.event, Observation::RequesterRequestStarted { request } if request.local_request_id == 1)
        })
        .unwrap();
    if let Observation::RequesterRequestStarted { request } = &mut initial_ibd_request.record.event
    {
        request.remote_peer = PeerId::random().into();
    }
    assert!(orphan_lifecycle(&events, &provider, &requester).is_err());
}

#[test]
fn orphan_partial_progress_must_be_newly_applied_before_the_hold_expires() {
    let (mut events, provider, requester) = orphan_fixture();
    let ibd_block = events
        .iter_mut()
        .find(|event| {
            event.node == "requester"
                && matches!(event.record.event, Observation::IbdBlockApplied { .. })
        })
        .unwrap();
    if let Observation::IbdBlockApplied { block_id } = &mut ibd_block.record.event {
        *block_id = id(25);
    }
    assert!(orphan_lifecycle(&events, &provider, &requester).is_err());

    let (mut events, provider, requester) = orphan_fixture();
    events.retain(|event| {
        !matches!(
            event.record.event,
            Observation::OrphanDownloadBlockApplied { block_id } if block_id == id(25)
        )
    });
    assert!(orphan_lifecycle(&events, &provider, &requester).is_err());
}

#[test]
fn orphan_retry_preserves_exact_partial_block_and_matches_f1_request_context() {
    let (mut events, provider, requester) = orphan_fixture();
    let (original, _, replacement, _) = orphan_lifecycle(&events, &provider, &requester).unwrap();
    assert_eq!(original.partial_block, id(25));
    assert_eq!(replacement.request.target, original.orphan_id);
    assert_eq!(
        replacement.request.remote_peer.0,
        provider.parse::<PeerId>().unwrap().to_bytes()
    );
    assert_eq!(
        replacement.provider_request.remote_peer.0,
        requester.parse::<PeerId>().unwrap().to_bytes()
    );

    let recovery = events
        .iter_mut()
        .find(|event| {
            event.node == "requester"
                && matches!(
                    event.record.event,
                    Observation::OrphanDownloadRecoveryScheduled { .. }
                )
        })
        .unwrap();
    if let Observation::OrphanDownloadRecoveryScheduled { known_block, .. } =
        &mut recovery.record.event
    {
        *known_block = Some(id(24));
    }
    assert!(orphan_lifecycle(&events, &provider, &requester).is_err());

    let (mut events, provider, requester) = orphan_fixture();
    let replacement_start = events
        .iter_mut()
        .find(|event| {
            event.node == "requester"
                && matches!(&event.record.event, Observation::RequesterRequestStarted { request } if request.protocol == "f1")
        })
        .unwrap();
    if let Observation::RequesterRequestStarted { request } = &mut replacement_start.record.event {
        request.target = id(31);
    }
    assert!(orphan_lifecycle(&events, &provider, &requester).is_err());
}

#[test]
fn missing_or_contradictory_orphan_lifecycle_evidence_fails_closed() {
    let (mut events, provider, requester) = orphan_fixture();
    events.retain(|event| {
        !matches!(
            event.record.event,
            Observation::OrphanDownloadCompleted { .. }
        )
    });
    assert!(orphan_lifecycle(&events, &provider, &requester).is_err());

    let (mut events, provider, requester) = orphan_fixture();
    let provider_acceptance = events
        .iter()
        .find(|event| {
            event.node == "provider"
                && matches!(&event.record.event, Observation::ProviderRequestAccepted { request } if request.protocol == "f1")
        })
        .unwrap()
        .clone();
    let mut duplicate = provider_acceptance;
    duplicate.record.sequence = 10;
    events.push(duplicate);
    assert!(orphan_lifecycle(&events, &provider, &requester).is_err());
}

#[test]
fn a_second_orphan_trigger_cannot_masquerade_as_autonomous_recovery() {
    let (mut events, provider, requester) = orphan_fixture();
    let trigger = events
        .iter()
        .find(|event| {
            event.node == "requester"
                && matches!(
                    event.record.event,
                    Observation::OrphanProposalMissingParent { .. }
                )
        })
        .unwrap()
        .clone();
    let mut retriggered = trigger;
    retriggered.record.sequence = 22;
    events.push(retriggered);
    assert!(orphan_lifecycle(&events, &provider, &requester).is_err());

    let (mut events, provider, requester) = orphan_fixture();
    let (peer_id, path) = events
        .iter()
        .find(|event| event.node == "requester")
        .map(|event| (event.peer_id.clone(), event.path.clone()))
        .unwrap();
    for (sequence, event) in [
        (
            28,
            Observation::OrphanProposalMissingParent {
                orphan_id: id(31),
                missing_parent: id(30),
                requester_tip: id(41),
                latest_immutable: id(42),
            },
        ),
        (
            29,
            Observation::OrphanDownloadRequestStarted {
                orphan_id: id(31),
                missing_parent: Some(id(30)),
                requester_tip: id(41),
                latest_immutable: id(42),
                known_block: None,
            },
        ),
    ] {
        events.push(RuntimeObservation {
            node: "requester".into(),
            peer_id: peer_id.clone(),
            path: path.clone(),
            record: ObservationRecord {
                schema_version: SCHEMA_VERSION,
                sequence,
                timestamp_unix_ms: 0,
                elapsed_ns: sequence * 10,
                event,
            },
        });
    }
    assert!(orphan_lifecycle(&events, &provider, &requester).is_ok());
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
