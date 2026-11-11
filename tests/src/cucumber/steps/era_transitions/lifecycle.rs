//! Typed correlation of the existing two-participant recovery lifecycle.

use lb_core::header::HeaderId;
use lb_cryptarchia_sync::observation::{
    InterruptionKind, Observation, PeerIdentity, RequestObservation,
};
use libp2p::PeerId;

use super::events::RuntimeObservation;

fn unique(
    events: &[RuntimeObservation],
    predicate: impl Fn(&RuntimeObservation) -> bool,
) -> Option<&RuntimeObservation> {
    let mut matches = events.iter().filter(|event| predicate(event));
    let first = matches.next()?;
    matches.next().is_none().then_some(first)
}

fn before(a: &RuntimeObservation, b: &RuntimeObservation) -> bool {
    a.node == b.node && a.path == b.path && a.record.sequence < b.record.sequence
}

fn peer(value: &str) -> Option<PeerIdentity> {
    value.parse::<PeerId>().ok().map(Into::into)
}

/// Request IDs and peer identities are local; context is compared separately.
fn same_context(a: &RequestObservation, b: &RequestObservation) -> bool {
    a.protocol == b.protocol
        && a.target == b.target
        && a.requester_tip == b.requester_tip
        && a.latest_immutable == b.latest_immutable
}

fn same_local_request(a: &RequestObservation, b: &RequestObservation) -> bool {
    a.local_request_id == b.local_request_id && a.remote_peer == b.remote_peer && same_context(a, b)
}

pub(super) struct Original<'a> {
    pub request: &'a RequestObservation,
    pub provider_request: &'a RequestObservation,
    pub request_event: &'a RuntimeObservation,
    pub accepted: &'a RuntimeObservation,
    pub sent: &'a RuntimeObservation,
    pub received: &'a RuntimeObservation,
    pub decoded: &'a RuntimeObservation,
    pub downloading: &'a RuntimeObservation,
    pub applied: &'a RuntimeObservation,
    pub hold: &'a RuntimeObservation,
    pub block_id: HeaderId,
}

pub(super) fn original<'a>(
    events: &'a [RuntimeObservation],
    provider: &str,
    requester: &str,
    provider_peer: &str,
    requester_peer: &str,
    f0: &str,
) -> Result<Original<'a>, &'static str> {
    if events.iter().any(|event| {
        (event.node == provider && event.peer_id != provider_peer)
            || (event.node == requester && event.peer_id != requester_peer)
    }) {
        return Err("observation ownership does not match participant peer identity");
    }
    let provider_id = peer(provider_peer).ok_or("invalid provider peer identity")?;
    let requester_id = peer(requester_peer).ok_or("invalid requester peer identity")?;
    let request_event = unique(events, |e| e.node == requester && matches!(&e.record.event, Observation::RequesterRequestStarted { request } if request.protocol == f0 && request.remote_peer == provider_id)).ok_or("original F0 requester request is missing or ambiguous")?;
    let Observation::RequesterRequestStarted { request } = &request_event.record.event else {
        unreachable!()
    };
    let interruption = events.iter().filter(|event| event.node == requester && matches!(&event.record.event, Observation::RequesterInterruptionDetected { request: observed, .. } if same_local_request(request, observed))).min_by_key(|event| event.record.sequence);
    let in_original_window = |event: &RuntimeObservation| {
        before(request_event, event) && interruption.is_none_or(|end| before(event, end))
    };
    let accepted = unique(events, |e| e.node == provider && matches!(&e.record.event, Observation::ProviderRequestAccepted { request: r } if r.remote_peer == requester_id && same_context(request, r))).ok_or("matching F0 provider acceptance is missing or ambiguous")?;
    let Observation::ProviderRequestAccepted {
        request: provider_request,
    } = &accepted.record.event
    else {
        unreachable!()
    };
    let sent = unique(events, |e| e.node == provider && matches!(&e.record.event, Observation::ProviderBlockResponseSent { request: r, response_index: 1 } if same_local_request(provider_request, r))).ok_or("provider first F0 response is missing or ambiguous")?;
    let received = unique(events, |e| e.node == requester && matches!(&e.record.event, Observation::RequesterBlockResponseReceived { request: r, response_index: 1 } if same_local_request(request, r))).ok_or("requester first F0 response is missing or ambiguous")?;
    let decoded = unique(events, |e| e.node == requester && in_original_window(e) && matches!(&e.record.event, Observation::RequesterFirstBlockDecoded { remote_peer, target, requester_tip, latest_immutable, .. } if *remote_peer == provider_id && *target == request.target && *requester_tip == request.requester_tip && *latest_immutable == request.latest_immutable)).ok_or("first decoded F0 block is missing or ambiguous")?;
    let Observation::RequesterFirstBlockDecoded { block_id, .. } = &decoded.record.event else {
        unreachable!()
    };
    let downloading = unique(events, |e| e.node == requester && in_original_window(e) && matches!(&e.record.event, Observation::IbdDownloaderDownloading { target } if *target == request.target)).ok_or("IBD Downloading observation is missing or ambiguous")?;
    let applied = unique(events, |e| e.node == requester && in_original_window(e) && matches!(&e.record.event, Observation::IbdBlockApplied { block_id: id } if id == block_id)).ok_or("first block application is missing or ambiguous")?;
    let hold = unique(events, |e| e.node == provider && matches!(&e.record.event, Observation::ProviderResponseHoldActivated { request: r, blocks_sent: 1 } if same_local_request(provider_request, r))).ok_or("provider hold for F0 request is missing or ambiguous")?;
    if !(before(request_event, received)
        && before(received, decoded)
        && before(decoded, downloading)
        && before(downloading, applied)
        && before(accepted, sent)
        && before(sent, hold))
    {
        return Err("F0 observations violate per-node ordering");
    }
    Ok(Original {
        request,
        provider_request,
        request_event,
        accepted,
        sent,
        received,
        decoded,
        downloading,
        applied,
        hold,
        block_id: *block_id,
    })
}

pub(super) struct Expiry<'a> {
    pub provider_rotation: &'a RuntimeObservation,
    pub requester_rotation: &'a RuntimeObservation,
    pub expiry: &'a RuntimeObservation,
    pub cancelled: &'a RuntimeObservation,
    pub interruption: &'a RuntimeObservation,
    pub ibd_error: &'a RuntimeObservation,
}

pub(super) fn expiry<'a>(
    events: &'a [RuntimeObservation],
    original: &Original<'a>,
    provider: &str,
    requester: &str,
    f0: &str,
    f1: &str,
) -> Result<Expiry<'a>, &'static str> {
    let rotation = |node: &str| {
        unique(events, |e| {
            e.node == node
                && matches!(&e.record.event, Observation::EraRotationStarted { previous_protocol, new_protocol, .. } if previous_protocol == f0 && new_protocol == f1)
        })
    };
    let provider_rotation =
        rotation(provider).ok_or("provider rotation is missing or ambiguous")?;
    let requester_rotation =
        rotation(requester).ok_or("requester rotation is missing or ambiguous")?;
    if !matches!(
        &provider_rotation.record.event,
        Observation::EraRotationStarted {
            pending_provider_responses: 1,
            ..
        }
    ) {
        return Err("provider rotation did not retain the F0 response");
    }
    let expiry = unique(events, |e| e.node == provider && matches!(&e.record.event, Observation::EraTransitionExpired { old_protocol, pending_provider_responses: 1 } if old_protocol == f0)).ok_or("provider expiry with pending response is missing or ambiguous")?;
    let cancelled = unique(events, |e| e.node == provider && matches!(&e.record.event, Observation::ProviderResponseHoldCancelled { request, blocks_sent: 1 } if same_local_request(original.provider_request, request))).ok_or("held provider request cancellation is missing or ambiguous")?;
    let interruption = unique(events, |e| e.node == requester && matches!(&e.record.event, Observation::RequesterInterruptionDetected { request, response_count: 1, kind: InterruptionKind::StreamError } if same_local_request(original.request, request))).ok_or("requester F0 interruption is missing or ambiguous")?;
    let ibd_error = unique(events, |e| e.node == requester && matches!(&e.record.event, Observation::IbdDownloadStreamError { target, blocks_received: 1 } if *target == original.request.target)).ok_or("IBD handling of F0 interruption is missing or ambiguous")?;
    if !(before(original.hold, provider_rotation)
        && before(provider_rotation, expiry)
        && before(expiry, cancelled)
        && before(original.request_event, requester_rotation)
        && before(original.applied, requester_rotation)
        && before(requester_rotation, interruption)
        && before(interruption, ibd_error))
    {
        return Err("expiry observations violate per-node ordering");
    }
    if events.iter().any(|e| e.node == provider && matches!(&e.record.event, Observation::ProviderRequestCompleted { request, .. } if same_local_request(original.provider_request, request))) { return Err("original held F0 provider request completed normally instead of remaining unfinished through cancellation"); }
    Ok(Expiry {
        provider_rotation,
        requester_rotation,
        expiry,
        cancelled,
        interruption,
        ibd_error,
    })
}

pub(super) struct Replacement<'a> {
    pub request: &'a RequestObservation,
    pub provider_request: &'a RequestObservation,
    pub started: &'a RuntimeObservation,
    pub accepted: &'a RuntimeObservation,
}

pub(super) fn replacement<'a>(
    events: &'a [RuntimeObservation],
    expiry: &Expiry<'a>,
    provider: &str,
    requester: &str,
    provider_peer: &str,
    requester_peer: &str,
    f1: &str,
) -> Result<Replacement<'a>, &'static str> {
    let provider_id = peer(provider_peer).ok_or("invalid provider peer identity")?;
    let requester_id = peer(requester_peer).ok_or("invalid requester peer identity")?;
    let ibd_start = events
        .iter()
        .filter(|e| {
            e.node == requester
                && before(expiry.ibd_error, e)
                && matches!(e.record.event, Observation::IbdRequestStarted { .. })
        })
        .min_by_key(|e| e.record.sequence)
        .ok_or("IBD replacement initiation is missing")?;
    let Observation::IbdRequestStarted {
        target,
        requester_tip,
        latest_immutable,
    } = &ibd_start.record.event
    else {
        unreachable!()
    };
    let started = unique(events, |e| e.node == requester && before(ibd_start, e) && matches!(&e.record.event, Observation::RequesterRequestStarted { request } if request.protocol == f1 && request.remote_peer == provider_id && &request.target == target && &request.requester_tip == requester_tip && &request.latest_immutable == latest_immutable)).ok_or("matching F1 replacement request is missing or ambiguous")?;
    let Observation::RequesterRequestStarted { request } = &started.record.event else {
        unreachable!()
    };
    if !before(expiry.requester_rotation, started) {
        return Err("F1 replacement began before the requester's F1 rotation");
    }
    if events.iter().filter(|e| e.node == requester && matches!(&e.record.event, Observation::RequesterRequestStarted { request: other } if same_context(request, other) && other.remote_peer == provider_id)).count() != 1 { return Err("F1 requester context is ambiguous"); }
    let accepted = unique(events, |e| e.node == provider && matches!(&e.record.event, Observation::ProviderRequestAccepted { request: other } if other.remote_peer == requester_id && same_context(request, other))).ok_or("matching F1 provider acceptance is missing or ambiguous")?;
    if !before(expiry.expiry, accepted) {
        return Err("F1 provider acceptance did not follow F0 expiry on the provider");
    }
    let Observation::ProviderRequestAccepted {
        request: provider_request,
    } = &accepted.record.event
    else {
        unreachable!()
    };
    Ok(Replacement {
        request,
        provider_request,
        started,
        accepted,
    })
}

pub(super) fn completed<'a>(
    events: &'a [RuntimeObservation],
    replacement: &Replacement<'a>,
    provider: &str,
    requester: &str,
) -> Result<&'a RuntimeObservation, &'static str> {
    let provider_done = unique(events, |e| e.node == provider && matches!(&e.record.event, Observation::ProviderRequestCompleted { request, .. } if same_local_request(replacement.provider_request, request))).ok_or("exact F1 provider completion is missing or ambiguous")?;
    let requester_done = unique(events, |e| e.node == requester && matches!(&e.record.event, Observation::RequesterRequestCompleted { request, .. } if same_local_request(replacement.request, request))).ok_or("exact F1 requester completion is missing or ambiguous")?;
    let ibd_done = unique(events, |e| {
        e.node == requester && matches!(&e.record.event, Observation::IbdCompleted)
    })
    .ok_or("IBD completion is missing or ambiguous")?;
    if !(before(replacement.accepted, provider_done)
        && before(replacement.started, requester_done)
        && before(requester_done, ibd_done))
    {
        return Err("F1 completion violates per-node ordering");
    }
    Ok(ibd_done)
}
