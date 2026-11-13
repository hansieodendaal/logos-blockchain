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

pub(super) struct UnselectedCompletion<'a> {
    pub request: &'a RequestObservation,
    pub provider_request: &'a RequestObservation,
    pub started: &'a RuntimeObservation,
    pub accepted: &'a RuntimeObservation,
    pub sent: &'a RuntimeObservation,
    pub received: &'a RuntimeObservation,
    pub provider_completed: &'a RuntimeObservation,
    pub requester_completed: &'a RuntimeObservation,
    pub ibd_completed: &'a RuntimeObservation,
}

/// Correlates the second request by its peer and full protocol context. Local
/// request IDs are checked only within the node that emitted them.
#[expect(
    clippy::too_many_arguments,
    reason = "the concurrent request proof is bound to both requester identities"
)]
#[expect(
    clippy::too_many_lines,
    reason = "ownership, correlation, and per-node ordering are one auditable proof"
)]
pub(super) fn unselected_completion<'a>(
    events: &'a [RuntimeObservation],
    selected: &Original<'a>,
    provider: &str,
    provider_peer: &str,
    requester_peer: &str,
    unselected_requester: &str,
    unselected_peer: &str,
    f0: &str,
) -> Result<UnselectedCompletion<'a>, &'static str> {
    if events.iter().any(|event| {
        (event.node == provider && event.peer_id != provider_peer)
            || (event.node == unselected_requester && event.peer_id != unselected_peer)
    }) {
        return Err("unselected request observation ownership does not match participant identity");
    }
    let provider_id = peer(provider_peer).ok_or("invalid provider peer identity")?;
    let selected_requester_id = peer(requester_peer).ok_or("invalid requester peer identity")?;
    let requester_id = peer(unselected_peer).ok_or("invalid unselected requester peer identity")?;
    if selected.provider_request.remote_peer != selected_requester_id {
        return Err("selected provider request is not owned by the declared requester");
    }
    // Tip polling can create later normal requests from this peer. Bind the
    // regression to the first provider-local F0 acceptance while the selected
    // request is held, then correlate that exact request across both journals.
    let accepted_candidates = events
        .iter()
        .filter(|event| {
            event.node == provider
                && before(selected.hold, event)
                && matches!(&event.record.event, Observation::ProviderRequestAccepted { request: accepted } if accepted.protocol == f0 && accepted.remote_peer == requester_id)
        })
        .collect::<Vec<_>>();
    let earliest_sequence = accepted_candidates
        .iter()
        .map(|event| event.record.sequence)
        .min()
        .ok_or("unselected F0 provider acceptance is missing")?;
    let earliest_acceptances = accepted_candidates
        .into_iter()
        .filter(|event| event.record.sequence == earliest_sequence)
        .collect::<Vec<_>>();
    let [accepted] = earliest_acceptances.as_slice() else {
        return Err("earliest unselected F0 provider acceptance is ambiguous");
    };
    let Observation::ProviderRequestAccepted {
        request: provider_request,
    } = &accepted.record.event
    else {
        unreachable!()
    };
    let started = unique(events, |event| {
        event.node == unselected_requester
            && matches!(&event.record.event, Observation::RequesterRequestStarted { request } if request.protocol == f0 && request.remote_peer == provider_id && same_context(request, provider_request))
    })
    .ok_or("unselected F0 requester request context is missing or ambiguous")?;
    let Observation::RequesterRequestStarted { request } = &started.record.event else {
        unreachable!()
    };
    let sent = unique(events, |event| {
        event.node == provider
            && matches!(&event.record.event, Observation::ProviderBlockResponseSent { request, response_index: 1 } if same_local_request(provider_request, request))
    })
    .ok_or("unselected F0 provider response is missing or ambiguous")?;
    let received = unique(events, |event| {
        event.node == unselected_requester
            && matches!(&event.record.event, Observation::RequesterBlockResponseReceived { request: received, response_index: 1 } if same_local_request(request, received))
    })
    .ok_or("unselected F0 requester response is missing or ambiguous")?;
    let provider_completed = unique(events, |event| {
        event.node == provider
            && matches!(&event.record.event, Observation::ProviderRequestCompleted { request: completed, .. } if same_local_request(provider_request, completed))
    })
    .ok_or("unselected F0 provider completion is missing or ambiguous")?;
    let requester_completed = unique(events, |event| {
        event.node == unselected_requester
            && matches!(&event.record.event, Observation::RequesterRequestCompleted { request: completed, .. } if same_local_request(request, completed))
    })
    .ok_or("unselected F0 requester completion is missing or ambiguous")?;
    let ibd_completed = unique(events, |event| {
        event.node == unselected_requester
            && matches!(event.record.event, Observation::IbdCompleted)
    })
    .ok_or("unselected requester IBD completion is missing or ambiguous")?;
    let Observation::ProviderRequestCompleted { blocks_sent, .. } =
        &provider_completed.record.event
    else {
        unreachable!()
    };
    let Observation::RequesterRequestCompleted { response_count, .. } =
        &requester_completed.record.event
    else {
        unreachable!()
    };

    if provider_request.remote_peer == selected.provider_request.remote_peer
        || provider_request.local_request_id == selected.provider_request.local_request_id
    {
        return Err("selected and unselected provider requests are not distinguishable");
    }
    if *blocks_sent == 0 || blocks_sent != response_count {
        return Err("unselected F0 response did not complete with matching nonempty progress");
    }
    let sent_count = events
        .iter()
        .filter(|event| {
            event.node == provider
                && matches!(&event.record.event, Observation::ProviderBlockResponseSent { request: sent, .. } if same_local_request(provider_request, sent))
        })
        .count();
    let received_count = events
        .iter()
        .filter(|event| {
            event.node == unselected_requester
                && matches!(&event.record.event, Observation::RequesterBlockResponseReceived { request: received, .. } if same_local_request(request, received))
        })
        .count();
    if usize::try_from(*blocks_sent).ok() != Some(sent_count)
        || usize::try_from(*response_count).ok() != Some(received_count)
    {
        return Err("unselected F0 completion has missing or contradictory block observations");
    }
    if events.iter().any(|event| {
        (event.node == provider
            && matches!(&event.record.event, Observation::ProviderResponseHoldActivated { request: held, .. } | Observation::ProviderResponseHoldCancelled { request: held, .. } if same_local_request(provider_request, held)))
            || (event.node == unselected_requester
                && matches!(&event.record.event, Observation::RequesterInterruptionDetected { request: interrupted, .. } if same_local_request(request, interrupted)))
    }) {
        return Err("unselected F0 request was held, cancelled, or interrupted");
    }
    if !(before(accepted, sent)
        && before(sent, provider_completed)
        && before(started, received)
        && before(received, requester_completed)
        && before(requester_completed, ibd_completed)
        && before(selected.hold, accepted)
        && before(selected.hold, provider_completed))
    {
        return Err("concurrent F0 requests do not overlap through normal completion");
    }
    let expiries = events.iter().filter(|event| {
        event.node == provider
            && matches!(&event.record.event, Observation::EraTransitionExpired { old_protocol, .. } if old_protocol == f0)
    });
    for expiry in expiries {
        if !before(provider_completed, expiry) {
            return Err("unselected F0 request did not complete before provider expiry");
        }
    }
    Ok(UnselectedCompletion {
        request,
        provider_request,
        started,
        accepted,
        sent,
        received,
        provider_completed,
        requester_completed,
        ibd_completed,
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

pub(super) struct OrphanOriginal<'a> {
    pub ibd_completed: &'a RuntimeObservation,
    pub proposal: &'a RuntimeObservation,
    pub download_started: &'a RuntimeObservation,
    pub request: &'a RequestObservation,
    pub provider_request: &'a RequestObservation,
    pub requester_request_started: &'a RuntimeObservation,
    pub accepted: &'a RuntimeObservation,
    pub sent: &'a RuntimeObservation,
    pub progress_sent: &'a RuntimeObservation,
    pub received: &'a RuntimeObservation,
    pub progress_received: &'a RuntimeObservation,
    pub decoded: &'a RuntimeObservation,
    pub downloading: &'a RuntimeObservation,
    pub applied: &'a RuntimeObservation,
    pub hold: &'a RuntimeObservation,
    pub held_blocks: u64,
    pub partial_block: HeaderId,
    pub orphan_id: HeaderId,
    pub missing_parent: HeaderId,
}

/// Identifies a post-IBD `ParentMissing` event and its runtime
/// orphan-downloader request. The initial IBD must complete from the same
/// provider that later serves the orphan ancestry.
#[expect(
    clippy::too_many_lines,
    reason = "the F0 orphan request proof keeps typed correlation and local ordering auditable"
)]
pub(super) fn orphan_original<'a>(
    events: &'a [RuntimeObservation],
    provider: &str,
    requester: &str,
    provider_peer: &str,
    requester_peer: &str,
    f0: &str,
) -> Result<OrphanOriginal<'a>, &'static str> {
    if events.iter().any(|event| {
        (event.node == provider && event.peer_id != provider_peer)
            || (event.node == requester && event.peer_id != requester_peer)
    }) {
        return Err("orphan observation ownership does not match participant peer identity");
    }
    let provider_id = peer(provider_peer).ok_or("invalid provider peer identity")?;
    let requester_id = peer(requester_peer).ok_or("invalid requester peer identity")?;
    let ibd_completed = unique(events, |event| {
        event.node == requester && matches!(event.record.event, Observation::IbdCompleted)
    })
    .ok_or("requester IBD completion is missing or ambiguous")?;
    let has_provider_ibd = events.iter().any(|ibd| {
        ibd.node == requester
            && before(ibd, ibd_completed)
            && matches!(&ibd.record.event, Observation::IbdRequestStarted { target, requester_tip, latest_immutable } if events.iter().any(|request| {
                request.node == requester
                    && before(ibd, request)
                    && before(request, ibd_completed)
                    && matches!(&request.record.event, Observation::RequesterRequestStarted { request } if request.remote_peer == provider_id && request.protocol == f0 && request.target == *target && request.requester_tip == *requester_tip && request.latest_immutable == *latest_immutable && events.iter().any(|accepted| {
                        accepted.node == provider
                            && matches!(&accepted.record.event, Observation::ProviderRequestAccepted { request: provider_request } if provider_request.remote_peer == requester_id && same_context(request, provider_request) && events.iter().any(|completed| {
                                completed.node == provider
                                    && matches!(&completed.record.event, Observation::ProviderRequestCompleted { request: completed_request, blocks_sent } if same_local_request(provider_request, completed_request) && *blocks_sent > 0)
                            }))
                    }))
            }))
    });
    if !has_provider_ibd {
        return Err(
            "requester IBD did not complete through a matching nonempty F0 request served normally by the provider",
        );
    }
    if events.iter().any(|event| {
        event.node == requester
            && before(ibd_completed, event)
            && matches!(
                event.record.event,
                Observation::IbdRequestStarted { .. }
                    | Observation::IbdDownloaderDownloading { .. }
                    | Observation::IbdDownloadStreamError { .. }
            )
    }) {
        return Err("IBD downloader observations occurred after initial IBD completed");
    }
    let hold = unique(events, |event| {
        event.node == provider
            && matches!(&event.record.event, Observation::ProviderResponseHoldActivated {
                request,
                blocks_sent,
            } if request.remote_peer == requester_id && request.protocol == f0 && *blocks_sent > 0)
    })
    .ok_or("selected F0 provider response hold is missing or ambiguous")?;
    let Observation::ProviderResponseHoldActivated {
        request: held_provider_request,
        ..
    } = &hold.record.event
    else {
        unreachable!()
    };
    let requester_request_started = unique(events, |event| {
        event.node == requester
            && matches!(&event.record.event, Observation::RequesterRequestStarted { request }
                if request.remote_peer == provider_id && same_context(request, held_provider_request))
    })
    .ok_or("held F0 request is missing or ambiguous on the requester")?;
    let Observation::RequesterRequestStarted { request } = &requester_request_started.record.event
    else {
        unreachable!()
    };
    let proposal = unique(events, |event| {
        event.node == requester
            && before(ibd_completed, event)
            && before(event, requester_request_started)
            && matches!(&event.record.event, Observation::OrphanProposalMissingParent {
                orphan_id,
                requester_tip,
                latest_immutable,
                ..
            } if *orphan_id == request.target
                && *requester_tip == request.requester_tip
                && *latest_immutable == request.latest_immutable)
    })
    .ok_or("held request has no matching post-IBD missing-parent proposal")?;
    let Observation::OrphanProposalMissingParent {
        orphan_id,
        missing_parent,
        requester_tip,
        latest_immutable,
    } = &proposal.record.event
    else {
        unreachable!()
    };
    let download_started = unique(events, |event| {
        event.node == requester
            && before(proposal, event)
            && matches!(&event.record.event, Observation::OrphanDownloadRequestStarted {
                orphan_id: id,
                missing_parent: Some(parent),
                requester_tip: tip,
                latest_immutable: lib,
                known_block: None,
            } if id == orphan_id && parent == missing_parent && tip == requester_tip && lib == latest_immutable)
    })
    .ok_or("initial runtime orphan-download request is missing or ambiguous")?;
    let Observation::OrphanDownloadRequestStarted {
        orphan_id: target,
        missing_parent: Some(_),
        requester_tip: tip,
        latest_immutable: lib,
        known_block: None,
    } = &download_started.record.event
    else {
        unreachable!()
    };
    if request.protocol != f0
        || request.target != *target
        || request.requester_tip != *tip
        || request.latest_immutable != *lib
        || !before(download_started, requester_request_started)
    {
        return Err("held F0 requester request does not match the original orphan context");
    }
    let interruption_end = events
        .iter()
        .filter(|event| {
            event.node == requester
                && matches!(&event.record.event, Observation::RequesterInterruptionDetected { request: observed, .. } if same_local_request(request, observed))
        })
        .min_by_key(|event| event.record.sequence);
    let within_f0_request = |event: &RuntimeObservation| {
        before(requester_request_started, event)
            && interruption_end.is_none_or(|end| before(event, end))
    };
    if request.target != *orphan_id || request.requester_tip != *requester_tip {
        return Err("F0 request context does not match the original orphan and requester state");
    }
    let accepted = unique(events, |event| {
        event.node == provider
            && matches!(&event.record.event, Observation::ProviderRequestAccepted { request: observed }
                if observed.remote_peer == requester_id && same_local_request(held_provider_request, observed))
    })
    .ok_or("matching provider acceptance for the orphan F0 request is missing or ambiguous")?;
    let Observation::ProviderRequestAccepted {
        request: provider_request,
    } = &accepted.record.event
    else {
        unreachable!()
    };
    let sent = unique(events, |event| {
        event.node == provider
            && matches!(&event.record.event, Observation::ProviderBlockResponseSent {
                request: observed,
                response_index: 1,
            } if same_local_request(provider_request, observed))
    })
    .ok_or("provider's first F0 orphan response is missing or ambiguous")?;
    let received = unique(events, |event| {
        event.node == requester
            && matches!(&event.record.event, Observation::RequesterBlockResponseReceived {
                request: observed,
                response_index: 1,
            } if same_local_request(request, observed))
    })
    .ok_or("requester's first F0 orphan response is missing or ambiguous")?;
    let Observation::ProviderResponseHoldActivated { blocks_sent, .. } = &hold.record.event else {
        unreachable!()
    };
    let held_blocks = *blocks_sent;
    if held_blocks <= 1 {
        return Err("orphan hold ended before new ancestry could be downloaded");
    }
    let progress_sent = unique(events, |event| {
        event.node == provider
            && matches!(&event.record.event, Observation::ProviderBlockResponseSent {
                request: observed,
                response_index,
            } if same_local_request(provider_request, observed) && *response_index == held_blocks)
    })
    .ok_or("provider did not send the final partial-progress block before its hold")?;
    let progress_received = unique(events, |event| {
        event.node == requester
            && matches!(&event.record.event, Observation::RequesterBlockResponseReceived {
                request: observed,
                response_index,
            } if same_local_request(request, observed) && *response_index == held_blocks)
    })
    .ok_or("requester did not receive the final partial-progress block before interruption")?;
    let decoded = unique(events, |event| {
        event.node == requester
            && within_f0_request(event)
            && matches!(&event.record.event, Observation::RequesterFirstBlockDecoded {
                remote_peer,
                target: observed_target,
                requester_tip: observed_tip,
                latest_immutable: observed_lib,
                ..
            } if *remote_peer == provider_id
                && *observed_target == request.target
                && *observed_tip == request.requester_tip
                && *observed_lib == request.latest_immutable)
    })
    .ok_or("first decoded F0 orphan block is missing or ambiguous")?;
    let Observation::RequesterFirstBlockDecoded { block_id, .. } = &decoded.record.event else {
        unreachable!()
    };
    if *block_id == *orphan_id || *block_id == *missing_parent {
        return Err("partial F0 progress is not an intermediate ancestor block");
    }
    let downloading = unique(events, |event| {
        event.node == requester
            && within_f0_request(event)
            && matches!(&event.record.event, Observation::OrphanDownloaderDownloading { target } if *target == *orphan_id)
    })
    .ok_or("runtime orphan downloader entry is missing or ambiguous")?;
    let applied_progress = events
        .iter()
        .filter(|event| {
            event.node == requester
                && within_f0_request(event)
                && matches!(
                    &event.record.event,
                    Observation::OrphanDownloadBlockApplied { .. }
                )
        })
        .collect::<Vec<_>>();
    let applied = *applied_progress
        .last()
        .ok_or("no new valid orphan-ancestry block was applied before interruption")?;
    let Observation::OrphanDownloadBlockApplied {
        block_id: partial_block,
    } = &applied.record.event
    else {
        unreachable!()
    };
    if *partial_block == *block_id
        || *partial_block == *orphan_id
        || events.iter().any(|event| {
            event.node == requester
                && matches!(
                    &event.record.event,
                    Observation::IbdBlockApplied { block_id } if block_id == partial_block
                )
        })
    {
        return Err("partial F0 progress was not a newly applied intermediate ancestry block");
    }
    let Observation::ProviderBlockResponseSent { response_index, .. } = &sent.record.event else {
        unreachable!()
    };
    let Observation::RequesterBlockResponseReceived {
        response_index: received_index,
        ..
    } = &received.record.event
    else {
        unreachable!()
    };
    if *response_index != *received_index
        || !before(ibd_completed, proposal)
        || !before(proposal, download_started)
        || !before(download_started, requester_request_started)
        || !before(accepted, sent)
        || !before(sent, hold)
        || !before(requester_request_started, received)
        || !before(received, decoded)
        || !before(progress_sent, hold)
        || !before(received, progress_received)
        || !before(progress_received, applied)
    {
        return Err("F0 orphan observations violate per-node ordering or response correlation");
    }
    if events.iter().any(|event| {
        event.node == requester
            && matches!(&event.record.event, Observation::IbdRequestStarted { target: id, .. } if *id == *orphan_id)
    }) {
        return Err("the orphan target was incorrectly attributed to the IBD downloader");
    }
    Ok(OrphanOriginal {
        ibd_completed,
        proposal,
        download_started,
        request,
        provider_request,
        requester_request_started,
        accepted,
        sent,
        progress_sent,
        received,
        progress_received,
        decoded,
        downloading,
        applied,
        hold,
        held_blocks,
        partial_block: *partial_block,
        orphan_id: *orphan_id,
        missing_parent: *missing_parent,
    })
}

pub(super) struct OrphanExpiry<'a> {
    pub provider_rotation: &'a RuntimeObservation,
    pub requester_rotation: &'a RuntimeObservation,
    pub expiry: &'a RuntimeObservation,
    pub cancelled: &'a RuntimeObservation,
    pub interruption: &'a RuntimeObservation,
    pub stream_error: &'a RuntimeObservation,
    pub recovery_scheduled: &'a RuntimeObservation,
}

pub(super) fn orphan_expiry<'a>(
    events: &'a [RuntimeObservation],
    original: &OrphanOriginal<'a>,
    provider: &str,
    requester: &str,
    f0: &str,
    f1: &str,
) -> Result<OrphanExpiry<'a>, &'static str> {
    let rotation = |node: &str| {
        unique(events, |event| {
            event.node == node
                && matches!(&event.record.event, Observation::EraRotationStarted { previous_protocol, new_protocol, .. } if previous_protocol == f0 && new_protocol == f1)
        })
    };
    let provider_rotation =
        rotation(provider).ok_or("provider F0-to-F1 rotation is missing or ambiguous")?;
    let requester_rotation =
        rotation(requester).ok_or("requester F0-to-F1 rotation is missing or ambiguous")?;
    if !matches!(
        provider_rotation.record.event,
        Observation::EraRotationStarted {
            pending_provider_responses: 1,
            ..
        }
    ) {
        return Err("provider rotation did not retain the selected orphan response");
    }
    let expiry = unique(events, |event| {
        event.node == provider
            && matches!(&event.record.event, Observation::EraTransitionExpired { old_protocol, pending_provider_responses: 1 } if old_protocol == f0)
    })
    .ok_or("provider expiry with the unfinished orphan response is missing or ambiguous")?;
    let cancelled = unique(events, |event| {
        event.node == provider
            && matches!(&event.record.event, Observation::ProviderResponseHoldCancelled {
                request,
                blocks_sent,
            } if same_local_request(original.provider_request, request) && *blocks_sent == original.held_blocks)
    })
    .ok_or("selected orphan provider response cancellation is missing or ambiguous")?;
    let interruption = unique(events, |event| {
        event.node == requester
            && matches!(&event.record.event, Observation::RequesterInterruptionDetected {
                request,
                response_count,
                kind: InterruptionKind::StreamError,
            } if same_local_request(original.request, request) && *response_count == original.held_blocks)
    })
    .ok_or("requester interruption for the held orphan request is missing or ambiguous")?;
    let stream_error = unique(events, |event| {
        event.node == requester
            && matches!(event.record.event, Observation::OrphanDownloadStreamError { orphan_id, blocks_received } if orphan_id == original.orphan_id && blocks_received == original.held_blocks)
    })
    .ok_or("runtime orphan downloader did not report the F0 stream interruption")?;
    let recovery_scheduled = unique(events, |event| {
        event.node == requester
            && matches!(&event.record.event, Observation::OrphanDownloadRecoveryScheduled {
                orphan_id,
                missing_parent: Some(parent),
                known_block: Some(known),
                blocks_received,
            } if *orphan_id == original.orphan_id && *parent == original.missing_parent && *known == original.partial_block && *blocks_received == original.held_blocks)
    })
    .ok_or("automatic runtime orphan continuation decision is missing or ambiguous")?;
    if !(before(original.hold, provider_rotation)
        && before(provider_rotation, expiry)
        && before(expiry, cancelled)
        && before(original.ibd_completed, original.proposal)
        && before(original.applied, requester_rotation)
        && before(requester_rotation, interruption)
        && before(interruption, stream_error)
        && before(stream_error, recovery_scheduled))
    {
        return Err("orphan expiry observations violate per-node causal ordering");
    }
    if events.iter().any(|event| {
        (event.node == provider
            && matches!(&event.record.event, Observation::ProviderRequestCompleted { request, .. } if same_local_request(original.provider_request, request)))
            || (event.node == requester
                && matches!(&event.record.event, Observation::RequesterRequestCompleted { request, .. } if same_local_request(original.request, request)))
    }) {
        return Err("held F0 orphan request completed normally instead of being cancelled");
    }
    Ok(OrphanExpiry {
        provider_rotation,
        requester_rotation,
        expiry,
        cancelled,
        interruption,
        stream_error,
        recovery_scheduled,
    })
}

pub(super) struct OrphanReplacement<'a> {
    pub request: &'a RequestObservation,
    pub provider_request: &'a RequestObservation,
    pub orphan_request_started: &'a RuntimeObservation,
    pub requester_request_started: &'a RuntimeObservation,
    pub accepted: &'a RuntimeObservation,
}

#[expect(
    clippy::too_many_arguments,
    reason = "the exact F1 replacement proof binds both participants, their peer identities and the selected protocol"
)]
pub(super) fn orphan_replacement<'a>(
    events: &'a [RuntimeObservation],
    original: &OrphanOriginal<'a>,
    expiry: &OrphanExpiry<'a>,
    provider: &str,
    requester: &str,
    provider_peer: &str,
    requester_peer: &str,
    f1: &str,
) -> Result<OrphanReplacement<'a>, &'static str> {
    let provider_id = peer(provider_peer).ok_or("invalid provider peer identity")?;
    let requester_id = peer(requester_peer).ok_or("invalid requester peer identity")?;
    let orphan_request_started = unique(events, |event| {
        event.node == requester
            && before(expiry.recovery_scheduled, event)
            && matches!(&event.record.event, Observation::OrphanDownloadRequestStarted {
                orphan_id,
                missing_parent: Some(parent),
                requester_tip,
                latest_immutable,
                known_block: Some(known),
            } if *orphan_id == original.orphan_id
                && *parent == original.missing_parent
                && *requester_tip == original.request.requester_tip
                && *latest_immutable == original.request.latest_immutable
                && *known == original.partial_block)
    })
    .ok_or("automatic F1 orphan replacement initiation is missing or ambiguous")?;
    let Observation::OrphanDownloadRequestStarted {
        orphan_id,
        requester_tip,
        latest_immutable,
        ..
    } = &orphan_request_started.record.event
    else {
        unreachable!()
    };
    let requester_request_started = unique(events, |event| {
        event.node == requester
            && before(orphan_request_started, event)
            && matches!(&event.record.event, Observation::RequesterRequestStarted { request }
                if request.protocol == f1
                    && request.remote_peer == provider_id
                    && request.target == *orphan_id
                    && request.requester_tip == *requester_tip
                    && request.latest_immutable == *latest_immutable)
    })
    .ok_or("exact matching F1 requester request is missing or ambiguous")?;
    let Observation::RequesterRequestStarted { request } = &requester_request_started.record.event
    else {
        unreachable!()
    };
    if !before(expiry.requester_rotation, requester_request_started) {
        return Err("orphan F1 replacement started before requester era rotation");
    }
    if events.iter().filter(|event| {
        event.node == requester
            && matches!(&event.record.event, Observation::OrphanDownloadRequestStarted { orphan_id, .. } if *orphan_id == original.orphan_id)
    }).count() != 2 {
        return Err("original orphan has an unexpected extra runtime download trigger");
    }
    if events
        .iter()
        .filter(|event| {
            event.node == requester
                && matches!(&event.record.event, Observation::OrphanProposalMissingParent { orphan_id, .. } if *orphan_id == original.orphan_id)
        })
        .count()
        != 1
    {
        return Err("the original orphan was retriggered by another external proposal");
    }
    if events.iter().any(|event| {
        event.node == requester
            && before(original.ibd_completed, event)
            && matches!(event.record.event, Observation::IbdRequestStarted { .. })
    }) {
        return Err("IBD or tip-poll work attempted to mask the orphan recovery path");
    }
    let accepted = unique(events, |event| {
        event.node == provider
            && matches!(&event.record.event, Observation::ProviderRequestAccepted { request: observed }
                if observed.remote_peer == requester_id && same_context(request, observed))
    })
    .ok_or("matching provider acceptance for the F1 orphan request is missing or ambiguous")?;
    if !before(expiry.expiry, accepted) {
        return Err("provider accepted the orphan F1 replacement before F0 expiry");
    }
    let Observation::ProviderRequestAccepted {
        request: provider_request,
    } = &accepted.record.event
    else {
        unreachable!()
    };
    Ok(OrphanReplacement {
        request,
        provider_request,
        orphan_request_started,
        requester_request_started,
        accepted,
    })
}

pub(super) struct OrphanCompletion<'a> {
    pub provider_completed: &'a RuntimeObservation,
    pub requester_completed: &'a RuntimeObservation,
    pub downloader_completed: &'a RuntimeObservation,
    pub target_applied: &'a RuntimeObservation,
}

pub(super) fn orphan_completed<'a>(
    events: &'a [RuntimeObservation],
    original: &OrphanOriginal<'a>,
    replacement: &OrphanReplacement<'a>,
    provider: &str,
    requester: &str,
) -> Result<OrphanCompletion<'a>, &'static str> {
    let provider_completed = unique(events, |event| {
        event.node == provider
            && matches!(&event.record.event, Observation::ProviderRequestCompleted { request, blocks_sent }
                if same_local_request(replacement.provider_request, request) && *blocks_sent > 0)
    })
    .ok_or("exact F1 provider request did not complete normally")?;
    let requester_completed = unique(events, |event| {
        event.node == requester
            && matches!(&event.record.event, Observation::RequesterRequestCompleted { request, response_count }
                if same_local_request(replacement.request, request) && *response_count > 0)
    })
    .ok_or("exact F1 requester request did not complete normally")?;
    let Observation::ProviderRequestCompleted {
        blocks_sent: sent_count,
        ..
    } = &provider_completed.record.event
    else {
        unreachable!()
    };
    let Observation::RequesterRequestCompleted { response_count, .. } =
        &requester_completed.record.event
    else {
        unreachable!()
    };
    if sent_count != response_count {
        return Err("provider and requester disagree about F1 response completion count");
    }
    let downloader_completed = unique(events, |event| {
        event.node == requester
            && matches!(&event.record.event, Observation::OrphanDownloadCompleted { orphan_id, blocks_received }
                if *orphan_id == original.orphan_id && *blocks_received > 0)
    })
    .ok_or("normal completion of the original orphan download is missing or ambiguous")?;
    let target_applied = unique(events, |event| {
        event.node == requester
            && matches!(&event.record.event, Observation::OrphanDownloadBlockApplied { block_id } if *block_id == original.orphan_id)
    })
    .ok_or("the original orphan block was not applied by the runtime downloader")?;
    let f1_downloading = unique(events, |event| {
        event.node == requester
            && before(replacement.orphan_request_started, event)
            && matches!(&event.record.event, Observation::OrphanDownloaderDownloading { target } if *target == original.orphan_id)
    })
    .ok_or("F1 runtime orphan downloader stream entry is missing or ambiguous")?;
    if events.iter().filter(|event| {
        event.node == requester
            && matches!(&event.record.event, Observation::OrphanDownloaderDownloading { target } if *target == original.orphan_id)
    }).count() != 2 {
        return Err("orphan target entered an unexpected number of runtime downloader streams");
    }
    if !(before(replacement.accepted, provider_completed)
        && before(replacement.requester_request_started, requester_completed)
        && before(replacement.requester_request_started, f1_downloading)
        && before(requester_completed, downloader_completed)
        && before(downloader_completed, target_applied))
    {
        return Err("F1 orphan completion observations violate per-node causal ordering");
    }
    if events.iter().any(|event| {
        (event.node == provider
            && matches!(&event.record.event, Observation::ProviderRequestCompleted { request, .. } if same_local_request(original.provider_request, request)))
            || (event.node == requester
                && matches!(&event.record.event, Observation::RequesterRequestCompleted { request, .. } if same_local_request(original.request, request)))
    }) {
        return Err("interrupted F0 orphan request has a contradictory normal completion");
    }
    Ok(OrphanCompletion {
        provider_completed,
        requester_completed,
        downloader_completed,
        target_applied,
    })
}
