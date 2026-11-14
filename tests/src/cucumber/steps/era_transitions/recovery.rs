//! Scenario-specific recovery assertions for RFC 439 era transitions.

use std::time::Duration;

use cucumber::{then, when};
use tokio::time::{Instant, sleep};

use super::{
    events::{
        collect_observations, era_protocols, era_protocols_for_world, node_peer_id,
        observation_reference, wait_for_observations,
    },
    evidence::{append_record, node_config_value, record_assertion},
    lifecycle,
    scenario::{
        participants, provider_name, requester_name, unselected_interactions,
        validate_participant_topology, validate_started_participant_topology,
    },
};
use crate::cucumber::{
    error::{StepError, StepResult},
    world::CucumberWorld,
};

fn failure(message: impl Into<String>) -> StepError {
    StepError::StepFail {
        message: message.into(),
    }
}

fn hold_setting_is_active(setting: Option<&serde_yaml::Value>) -> bool {
    setting.is_some_and(|value| !value.is_null())
}

#[then(
    expr = "the requester completes initial IBD from the provider participant within {int} seconds"
)]
#[expect(
    clippy::needless_pass_by_ref_mut,
    reason = "Cucumber step callbacks require a mutable world reference"
)]
async fn requester_completes_initial_ibd(
    world: &mut CucumberWorld,
    timeout_seconds: u64,
) -> StepResult {
    let requester = requester_name(world)?;
    let provider = provider_name(world)?;
    let provider_peer = node_peer_id(world, &provider)
        .ok_or_else(|| failure("provider peer identity was not retained"))?
        .parse::<libp2p::PeerId>()
        .map_err(|error| failure(format!("invalid provider peer identity: {error}")))?;
    let (_, f0, _) = era_protocols_for_world(world, &provider)?;
    let events = wait_for_observations(world, timeout_seconds, |events| {
        events.iter().any(|event| {
            event.node == requester
                && matches!(
                    event.record.event,
                    lb_cryptarchia_sync::observation::Observation::IbdCompleted
                )
        })
    })
    .await?;
    let ibd_completed = events
        .iter()
        .filter(|event| {
            event.node == requester
                && matches!(
                    event.record.event,
                    lb_cryptarchia_sync::observation::Observation::IbdCompleted
                )
        })
        .collect::<Vec<_>>();
    let [ibd_completed] = ibd_completed.as_slice() else {
        return Err(failure(
            "requester initial IBD completion is missing or ambiguous",
        ));
    };
    let provider_request = events.iter().any(|ibd| {
        ibd.node == requester
            && ibd.record.sequence < ibd_completed.record.sequence
            && matches!(&ibd.record.event, lb_cryptarchia_sync::observation::Observation::IbdRequestStarted { target, requester_tip, latest_immutable } if events.iter().any(|request| {
                request.node == requester
                    && request.record.sequence > ibd.record.sequence
                    && request.record.sequence < ibd_completed.record.sequence
                    && matches!(&request.record.event, lb_cryptarchia_sync::observation::Observation::RequesterRequestStarted { request } if request.remote_peer.0 == provider_peer.to_bytes() && request.protocol == f0 && request.target == *target && request.requester_tip == *requester_tip && request.latest_immutable == *latest_immutable)
            }))
    });
    if !provider_request {
        return Err(failure(
            "requester IBD did not issue a matching F0 chain-sync request to the provider",
        ));
    }
    append_record(
        world,
        "assertion",
        serde_json::json!({
            "assertion": "orphan_initial_ibd_completed",
            "status": "passed",
            "requester": requester,
            "provider_peer_id": provider_peer.to_string(),
            "ibd_completed": observation_reference(ibd_completed),
        }),
    )
}

#[then(
    expr = "the provider participant produces at least {int} blocks before era activation within {int} seconds"
)]
#[expect(
    clippy::needless_pass_by_ref_mut,
    reason = "Cucumber step callbacks require &mut World"
)]
async fn node_builds_pre_activation_history(
    world: &mut CucumberWorld,
    minimum_height: u64,
    timeout_seconds: u64,
) -> StepResult {
    let node_name = provider_name(world)?;
    let node = world
        .nodes_info
        .get(&node_name)
        .ok_or_else(|| failure(format!("Node `{node_name}` has not started")))?;
    let (boundary_slot, f0, f1) = era_protocols(node)?;
    let deadline = Instant::now() + Duration::from_secs(timeout_seconds);
    let mut last_height = None;
    let mut last_slot = None;

    loop {
        let consensus = node.client.consensus_info().await;
        let time = node.client.time_info().await;
        if let (Ok(consensus), Ok(time)) = (consensus, time) {
            last_height = Some(consensus.cryptarchia_info.height);
            last_slot = Some(time.current_slot);
            if time.current_slot >= boundary_slot {
                let message = format!(
                    "Era activation was missed while preparing the backlog: node `{node_name}` is at slot {}, boundary {boundary_slot}; observed height {}",
                    time.current_slot, consensus.cryptarchia_info.height
                );
                record_assertion(world, "pre_activation_backlog", false, &message)?;
                return Err(failure(message));
            }
            if consensus.cryptarchia_info.height >= minimum_height {
                let details = serde_json::json!({
                    "node": node_name,
                    "height": consensus.cryptarchia_info.height,
                    "slot": time.current_slot,
                    "boundary_slot": boundary_slot,
                    "source_era": 0,
                    "source_protocol": f0,
                    "target_era": 1,
                    "target_protocol": f1,
                    "canonical_tip": format!("{:?}", consensus.cryptarchia_info.tip),
                });
                record_assertion(
                    world,
                    "pre_activation_backlog",
                    true,
                    "genuine history observed before activation",
                )?;
                return append_record(world, "pre_activation_consensus_snapshot", details);
            }
        }

        if Instant::now() >= deadline {
            let message = format!(
                "Node `{node_name}` did not produce {minimum_height} blocks before activation within {timeout_seconds}s; last height {last_height:?}, slot {last_slot:?}, boundary {boundary_slot}"
            );
            record_assertion(world, "pre_activation_backlog", false, &message)?;
            return Err(failure(message));
        }
        sleep(Duration::from_millis(250)).await;
    }
}

#[then(
    expr = "the requester receives and applies its first F0 block while the provider holds the next response within {int} seconds"
)]
#[expect(
    clippy::needless_pass_by_ref_mut,
    reason = "Cucumber step callbacks require &mut World"
)]
async fn establish_f0_request_and_hold(
    world: &mut CucumberWorld,
    timeout_seconds: u64,
) -> StepResult {
    validate_started_participant_topology(world)?;
    let provider = provider_name(world)?;
    let requester = requester_name(world)?;
    let (_, f0, _) = era_protocols_for_world(world, &provider)?;
    let provider_peer =
        node_peer_id(world, &provider).ok_or_else(|| failure("provider peer is missing"))?;
    let requester_peer =
        node_peer_id(world, &requester).ok_or_else(|| failure("requester peer is missing"))?;
    let events = wait_for_observations(world, timeout_seconds, |events| {
        lifecycle::original(
            events,
            &provider,
            &requester,
            &provider_peer,
            &requester_peer,
            &f0,
        )
        .is_ok()
    })
    .await?;
    let original = lifecycle::original(
        &events,
        &provider,
        &requester,
        &provider_peer,
        &requester_peer,
        &f0,
    )
    .map_err(failure)?;
    record_assertion(
        world,
        "established_f0_request_and_partial_progress",
        true,
        "matching F0 request, first response and block application, and pending provider hold were observed",
    )?;
    append_record(
        world,
        "original_request_progress",
        serde_json::json!({
            "request": original.request, "provider_request": original.provider_request,
            "requester_start": observation_reference(original.request_event),
            "provider_acceptance": observation_reference(original.accepted),
            "provider_send": observation_reference(original.sent),
            "requester_receive": observation_reference(original.received),
            "decoded": observation_reference(original.decoded),
            "downloading": observation_reference(original.downloading),
            "applied": observation_reference(original.applied), "hold": observation_reference(original.hold)
        }),
    )?;
    append_record(
        world,
        "selected_request_hold_barrier",
        serde_json::json!({
            "selected_provider": provider,
            "selected_requester": requester,
            "selected_request": original.provider_request,
            "selected_hold": observation_reference(original.hold),
        }),
    )
}

#[then(
    expr = "the selected F0 request overlaps with an unselected F0 request that completes normally within {int} seconds"
)]
#[expect(
    clippy::needless_pass_by_ref_mut,
    reason = "Cucumber step callbacks require &mut World"
)]
async fn concurrent_f0_request_completes_normally(
    world: &mut CucumberWorld,
    timeout_seconds: u64,
) -> StepResult {
    let provider = provider_name(world)?;
    let requester = requester_name(world)?;
    let interactions = unselected_interactions(world)?;
    if interactions.is_empty() {
        return Err(failure("no unselected interaction was declared"));
    }
    let (_, f0, _) = era_protocols_for_world(world, &provider)?;
    let provider_peer =
        node_peer_id(world, &provider).ok_or_else(|| failure("provider peer is missing"))?;
    let requester_peer =
        node_peer_id(world, &requester).ok_or_else(|| failure("requester peer is missing"))?;
    let controls = interactions
        .into_iter()
        .map(|interaction| {
            let provider_peer = node_peer_id(world, &interaction.provider)
                .ok_or_else(|| failure("unselected provider peer is missing"))?;
            let requester_peer = node_peer_id(world, &interaction.requester)
                .ok_or_else(|| failure("unselected requester peer is missing"))?;
            Ok((interaction, provider_peer, requester_peer))
        })
        .collect::<Result<Vec<_>, StepError>>()?;
    let events = wait_for_observations(world, timeout_seconds, |events| {
        let Ok(selected) = lifecycle::original(
            events,
            &provider,
            &requester,
            &provider_peer,
            &requester_peer,
            &f0,
        ) else {
            return false;
        };
        controls
            .iter()
            .all(|(interaction, provider_peer, requester_peer)| {
                let control = lifecycle::InteractionPeers {
                    provider: &interaction.provider,
                    provider_peer,
                    requester: &interaction.requester,
                    requester_peer,
                };
                lifecycle::unselected_completion(events, &selected, &control, &f0).is_ok()
            })
    })
    .await?;
    let selected = lifecycle::original(
        &events,
        &provider,
        &requester,
        &provider_peer,
        &requester_peer,
        &f0,
    )
    .map_err(failure)?;
    let unselected = controls
        .iter()
        .map(|(interaction, provider_peer, requester_peer)| {
            let control = lifecycle::InteractionPeers {
                provider: &interaction.provider,
                provider_peer,
                requester: &interaction.requester,
                requester_peer,
            };
            lifecycle::unselected_completion(&events, &selected, &control, &f0).map_err(failure)
        })
        .collect::<Result<Vec<_>, StepError>>()?;
    record_assertion(
        world,
        "concurrent_f0_request_lifecycle",
        true,
        "typed participant-local events prove every unselected interaction completed normally while the selected request remained held",
    )?;
    append_record(
        world,
        "concurrent_f0_request_lifecycle",
        serde_json::json!({
            "selected_request": selected.provider_request,
            "selected_provider": provider,
            "selected_hold": observation_reference(selected.hold),
            "control_lifecycles": controls.iter().zip(&unselected).map(|(interaction, completion)| serde_json::json!({
                "interaction": interaction.0.name,
                "provider": interaction.0.provider,
                "requester": interaction.0.requester,
                "requester_request": completion.request,
                "provider_request": completion.provider_request,
                "request_start": observation_reference(completion.started),
                "provider_acceptance": observation_reference(completion.accepted),
                "first_response_sent": observation_reference(completion.sent),
                "first_response_received": observation_reference(completion.received),
                "provider_completion": observation_reference(completion.provider_completed),
                "requester_completion": observation_reference(completion.requester_completed),
                "ibd_completion": observation_reference(completion.ibd_completed),
                "provider_local_ids_compared_only_when_same_provider": provider == interaction.0.provider,
            })).collect::<Vec<_>>(),
            "request_ids_are_process_local": true
        }),
    )
}

#[then(
    expr = "transition expiry cancels the held F0 request and the requester detects the interruption within {int} seconds"
)]
#[expect(
    clippy::needless_pass_by_ref_mut,
    reason = "Cucumber step callbacks require &mut World"
)]
async fn expiry_cancels_and_requester_detects(
    world: &mut CucumberWorld,
    timeout_seconds: u64,
) -> StepResult {
    let provider = provider_name(world)?;
    let requester = requester_name(world)?;
    let (_, f0, f1) = era_protocols_for_world(world, &provider)?;
    let provider_peer =
        node_peer_id(world, &provider).ok_or_else(|| failure("provider peer is missing"))?;
    let requester_peer =
        node_peer_id(world, &requester).ok_or_else(|| failure("requester peer is missing"))?;
    let events = wait_for_observations(world, timeout_seconds, |events| {
        lifecycle::original(
            events,
            &provider,
            &requester,
            &provider_peer,
            &requester_peer,
            &f0,
        )
        .and_then(|original| lifecycle::expiry(events, &original, &provider, &requester, &f0, &f1))
        .is_ok()
    })
    .await?;
    let original = lifecycle::original(
        &events,
        &provider,
        &requester,
        &provider_peer,
        &requester_peer,
        &f0,
    )
    .map_err(failure)?;
    let expiry =
        lifecycle::expiry(&events, &original, &provider, &requester, &f0, &f1).map_err(failure)?;
    record_assertion(
        world,
        "expiry_cancellation_and_requester_detection",
        true,
        "provider retained F0 work until expiry and requester independently reported stream interruption",
    )?;
    append_record(
        world,
        "expiry_and_interruption",
        serde_json::json!({
            "provider_rotation": observation_reference(expiry.provider_rotation),
            "requester_rotation": observation_reference(expiry.requester_rotation),
            "expiry": observation_reference(expiry.expiry),
            "provider_cancellation": observation_reference(expiry.cancelled),
            "requester_interruption": observation_reference(expiry.interruption),
            "ibd_stream_error": observation_reference(expiry.ibd_error)
        }),
    )
}

#[then(
    expr = "the requester automatically replaces the interrupted request under F1 and the provider accepts it within {int} seconds"
)]
async fn f1_replacement_is_accepted(world: &mut CucumberWorld, timeout_seconds: u64) -> StepResult {
    let provider = provider_name(world)?;
    let requester = requester_name(world)?;
    let (_, f0, f1) = era_protocols_for_world(world, &provider)?;
    let provider_peer =
        node_peer_id(world, &provider).ok_or_else(|| failure("provider peer is missing"))?;
    let requester_peer =
        node_peer_id(world, &requester).ok_or_else(|| failure("requester peer is missing"))?;
    let events = wait_for_observations(world, timeout_seconds, |events| {
        lifecycle::original(
            events,
            &provider,
            &requester,
            &provider_peer,
            &requester_peer,
            &f0,
        )
        .and_then(|original| lifecycle::expiry(events, &original, &provider, &requester, &f0, &f1))
        .and_then(|expiry| {
            lifecycle::replacement(
                events,
                &expiry,
                &provider,
                &requester,
                &provider_peer,
                &requester_peer,
                &f1,
            )
        })
        .is_ok()
    })
    .await?;
    let original = lifecycle::original(
        &events,
        &provider,
        &requester,
        &provider_peer,
        &requester_peer,
        &f0,
    )
    .map_err(failure)?;
    let expiry =
        lifecycle::expiry(&events, &original, &provider, &requester, &f0, &f1).map_err(failure)?;
    let replacement = lifecycle::replacement(
        &events,
        &expiry,
        &provider,
        &requester,
        &provider_peer,
        &requester_peer,
        &f1,
    )
    .map_err(failure)?;
    world
        .era_transition
        .as_mut()
        .ok_or_else(|| failure("scenario state missing"))?
        .accepted_f1_request = Some(crate::cucumber::world::EraTransitionAcceptedRequest {
        requester: replacement.request.clone(),
        provider: replacement.provider_request.clone(),
    });
    record_assertion(
        world,
        "automatic_f1_recovery",
        true,
        "IBD started the matching F1 request after handling the interrupted download",
    )?;
    append_record(
        world,
        "f1_replacement_accepted",
        serde_json::json!({ "requester": replacement.request, "provider": replacement.provider_request, "start": observation_reference(replacement.started), "acceptance": observation_reference(replacement.accepted) }),
    )
}

#[then(
    expr = "the requester retains the first downloaded block and completes IBD within {int} seconds"
)]
#[expect(
    clippy::needless_pass_by_ref_mut,
    reason = "Cucumber step callbacks require &mut World"
)]
#[expect(
    clippy::too_many_lines,
    reason = "the step verifies the retained block, original target and accepted replacement lifecycle"
)]
async fn partial_block_retained_and_ibd_completes(
    world: &mut CucumberWorld,
    timeout_seconds: u64,
) -> StepResult {
    let provider = provider_name(world)?;
    let requester = requester_name(world)?;
    let (_, f0, f1) = era_protocols_for_world(world, &provider)?;
    let provider_peer =
        node_peer_id(world, &provider).ok_or_else(|| failure("provider peer is missing"))?;
    let requester_peer =
        node_peer_id(world, &requester).ok_or_else(|| failure("requester peer is missing"))?;
    let events = wait_for_observations(world, timeout_seconds, |events| {
        lifecycle::original(
            events,
            &provider,
            &requester,
            &provider_peer,
            &requester_peer,
            &f0,
        )
        .and_then(|original| lifecycle::expiry(events, &original, &provider, &requester, &f0, &f1))
        .and_then(|expiry| {
            lifecycle::replacement(
                events,
                &expiry,
                &provider,
                &requester,
                &provider_peer,
                &requester_peer,
                &f1,
            )
        })
        .and_then(|replacement| lifecycle::completed(events, &replacement, &provider, &requester))
        .is_ok()
    })
    .await?;
    let original = lifecycle::original(
        &events,
        &provider,
        &requester,
        &provider_peer,
        &requester_peer,
        &f0,
    )
    .map_err(failure)?;
    let expiry =
        lifecycle::expiry(&events, &original, &provider, &requester, &f0, &f1).map_err(failure)?;
    let replacement = lifecycle::replacement(
        &events,
        &expiry,
        &provider,
        &requester,
        &provider_peer,
        &requester_peer,
        &f1,
    )
    .map_err(failure)?;
    let accepted = world
        .era_transition
        .as_ref()
        .and_then(|s| s.accepted_f1_request.as_ref())
        .ok_or_else(|| failure("accepted F1 identity missing"))?;
    if accepted.requester != *replacement.request
        || accepted.provider != *replacement.provider_request
    {
        return Err(failure(
            "F1 completion does not match the accepted request identities",
        ));
    }
    let completion =
        lifecycle::completed(&events, &replacement, &provider, &requester).map_err(failure)?;
    let node = world
        .nodes_info
        .get(&requester)
        .ok_or_else(|| failure("requester node is missing"))?;
    let stored_partial = node.client.block(&original.block_id).await?;
    let stored_target = node.client.block(&original.request.target).await?;
    let final_tip = node.client.consensus_info().await?.cryptarchia_info.tip;
    let partial_in_replacement_tip =
        crate::cucumber::steps::nodes::era_boundaries::canonical_history_contains_block(
            world,
            &requester,
            replacement.request.requester_tip,
            original.block_id,
            Duration::from_secs(30),
        )
        .await?;
    let partial_in_final_tip =
        crate::cucumber::steps::nodes::era_boundaries::canonical_history_contains_block(
            world,
            &requester,
            final_tip,
            original.block_id,
            Duration::from_secs(30),
        )
        .await?;
    let target_in_final_tip =
        crate::cucumber::steps::nodes::era_boundaries::canonical_history_contains_block(
            world,
            &requester,
            final_tip,
            original.request.target,
            Duration::from_secs(30),
        )
        .await?;
    if stored_partial.is_none() || !partial_in_replacement_tip || !partial_in_final_tip {
        return Err(failure(
            "the first block was not retained on the replacement and final tip ancestry",
        ));
    }
    if stored_target.is_none() || !target_in_final_tip {
        return Err(failure(
            "the original target is not on final canonical ancestry",
        ));
    }
    record_assertion(
        world,
        "partial_progress_retained",
        true,
        "first block remains available on replacement tip ancestry; original target is resolved",
    )?;
    record_assertion(
        world,
        "ibd_completed",
        true,
        "both sides completed the accepted F1 request and IBD completed",
    )?;
    append_record(
        world,
        "partial_progress_and_ibd_completion",
        serde_json::json!({
            "first_received_block": original.block_id,
            "original_target": original.request.target,
            "replacement_tip": replacement.request.requester_tip,
            "final_tip": final_tip,
            "partial_block_available_after_recovery": true,
            "partial_block_is_ancestor_of_replacement_tip": partial_in_replacement_tip,
            "partial_block_is_ancestor_of_final_tip": partial_in_final_tip,
            "original_target_available_after_recovery": true,
            "original_target_is_ancestor_of_final_tip": target_in_final_tip,
            "ibd_completion": observation_reference(completion)
        }),
    )
}

#[then("all era-transition participants remain healthy after IBD recovery")]
#[then("all era-transition participants remain healthy after orphan recovery")]
#[expect(
    clippy::too_many_lines,
    reason = "the step verifies process health and outstanding request capacity for every declared participant"
)]
#[expect(
    clippy::needless_pass_by_ref_mut,
    reason = "Cucumber step callbacks require &mut World"
)]
async fn both_nodes_remain_healthy(world: &mut CucumberWorld) -> StepResult {
    validate_participant_topology(world)?;
    let provider = provider_name(world)?;
    let requester = requester_name(world)?;
    let expected_hold = world
        .era_transition
        .as_ref()
        .and_then(|state| state.provider_hold_after_blocks)
        .ok_or_else(|| failure("provider hold configuration is missing from scenario state"))?;
    collect_observations(world, false).map_err(failure)?;
    for node_name in participants(world)?.keys() {
        let node = world
            .nodes_info
            .get(node_name)
            .ok_or_else(|| failure(format!("Node `{node_name}` is not registered")))?;
        let consensus = node.client.consensus_info().await.map_err(|error| {
            failure(format!(
                "Node `{node_name}` is not answering consensus info: {error}"
            ))
        })?;
        let network = node.client.network_info().await.map_err(|error| {
            failure(format!(
                "Node `{node_name}` is not answering network info: {error}"
            ))
        })?;
        let details = serde_json::json!({
            "node": node_name,
            "peer_id": network.peer_id.to_string(),
            "height": consensus.cryptarchia_info.height,
            "tip": format!("{:?}", consensus.cryptarchia_info.tip),
            "status": "healthy",
        });
        append_record(world, "node_health", details)?;
    }
    let provider_hold = node_config_value(
        world,
        provider.as_str(),
        "network.backend.swarm.chain_sync.testing_era_boundaries.hold_provider_after_blocks",
    )
    .and_then(|value| value.as_u64());
    let requester_tip_poll = node_config_value(
        world,
        requester.as_str(),
        "cryptarchia.network.sync.tip_poll.enabled",
    )
    .and_then(|value| value.as_bool());
    let controls = unselected_interactions(world)?;
    let unselected_tip_poll = controls.iter().all(|interaction| {
        node_config_value(
            world,
            &interaction.requester,
            "cryptarchia.network.sync.tip_poll.enabled",
        )
        .and_then(|value| value.as_bool())
            == Some(true)
    });
    let configured_hold_peer = node_config_value(
        world,
        provider.as_str(),
        "network.backend.swarm.chain_sync.testing_era_boundaries.hold_provider_for_peer_id",
    )
    .and_then(|value| value.as_str().map(str::to_owned));
    let configured_context_tip = node_config_value(
        world,
        provider.as_str(),
        "network.backend.swarm.chain_sync.testing_era_boundaries.hold_provider_for_requester_tip_not_equal_to",
    )
    .and_then(|value| value.as_str().map(str::to_owned));
    let configured_hold_provider = node_config_value(
        world,
        provider.as_str(),
        "network.backend.swarm.chain_sync.testing_era_boundaries.hold_provider_for_provider_peer_id",
    )
    .and_then(|value| value.as_str().map(str::to_owned));
    let expected_hold_provider = world
        .era_transition
        .as_ref()
        .and_then(|state| state.provider_hold_provider_peer_id.clone());
    let orphan_recovery = world
        .era_transition
        .as_ref()
        .is_some_and(|state| state.orphan_recovery);
    let expected_context_tip = if orphan_recovery {
        world.chain.genesis_block_id.map(|id| format!("{id:?}"))
    } else {
        None
    };
    let expected_hold_peer = if controls.is_empty() {
        None
    } else {
        Some(
            node_peer_id(world, &requester)
                .ok_or_else(|| failure("selected requester peer identity is missing"))?,
        )
    };
    let unexpected_holds = participants(world)?
        .values()
        .filter(|participant| participant.node_name != provider)
        .filter_map(|participant| {
            let configured = node_config_value(
                world,
                &participant.node_name,
                "network.backend.swarm.chain_sync.testing_era_boundaries.hold_provider_after_blocks",
            );
            configured
                .filter(|value| hold_setting_is_active(Some(value)))
                .map(|value| (participant.node_name.clone(), value))
        })
        .collect::<Vec<_>>();
    if provider_hold != u64::try_from(expected_hold).ok()
        || !unexpected_holds.is_empty()
        || requester_tip_poll != Some(false)
        || !unselected_tip_poll
        || configured_hold_peer != expected_hold_peer
        || configured_context_tip != expected_context_tip
        || configured_hold_provider != expected_hold_provider
    {
        let message = format!(
            "Effective per-participant controls were unexpected: provider `{provider}` hold={provider_hold:?} expected={expected_hold} selected_peer={configured_hold_peer:?} expected_peer={expected_hold_peer:?}; selected_provider={configured_hold_provider:?} expected_provider={expected_hold_provider:?}; excluded_requester_tip={configured_context_tip:?} expected_tip={expected_context_tip:?}; non-provider holds={unexpected_holds:?}; requester `{requester}` tip polling={requester_tip_poll:?}; all control requesters tip polling enabled={unselected_tip_poll}"
        );
        record_assertion(world, "per_node_experiment_configuration", false, &message)?;
        return Err(failure(message));
    }
    record_assertion(
        world,
        "per_node_experiment_configuration",
        true,
        if orphan_recovery {
            "The provider hold excludes the requester's genesis-tip IBD context, and requester tip polling is disabled; typed lifecycle evidence binds the held request to the requester peer and F0 protocol"
        } else {
            "The provider hold selector matches the selected requester; only the unselected requester uses tip polling to maintain its live history"
        },
    )?;
    let events = collect_observations(world, false).map_err(failure)?;
    let request_identity = world
        .era_transition
        .as_ref()
        .and_then(|state| state.accepted_f1_request.as_ref())
        .ok_or_else(|| {
            failure("The accepted F1 replacement identity was not retained in scenario state")
        })?;
    let provider_peer = node_peer_id(world, provider.as_str())
        .ok_or_else(|| failure("Provider participant has no peer identity"))?;
    let requester_peer = node_peer_id(world, requester.as_str())
        .ok_or_else(|| failure("Requester participant has no peer identity"))?;
    let (_, f0, f1) = era_protocols_for_world(world, &provider)?;
    let complete_lifecycle = if orphan_recovery {
        (|| {
            let original = lifecycle::orphan_original(
                &events,
                &provider,
                &requester,
                &provider_peer,
                &requester_peer,
                &f0,
            )?;
            let expiry =
                lifecycle::orphan_expiry(&events, &original, &provider, &requester, &f0, &f1)?;
            let replacement = lifecycle::orphan_replacement(
                &events,
                &original,
                &expiry,
                &provider,
                &requester,
                &provider_peer,
                &requester_peer,
                &f1,
            )?;
            if request_identity.requester != *replacement.request
                || request_identity.provider != *replacement.provider_request
            {
                return Err("replacement differs from retained local request identities");
            }
            lifecycle::orphan_completed(&events, &original, &replacement, &provider, &requester)
                .map(|_| ())
        })()
        .is_ok()
    } else {
        lifecycle::original(
            &events,
            &provider,
            &requester,
            &provider_peer,
            &requester_peer,
            &f0,
        )
        .and_then(|original| lifecycle::expiry(&events, &original, &provider, &requester, &f0, &f1))
        .and_then(|expiry| {
            lifecycle::replacement(
                &events,
                &expiry,
                &provider,
                &requester,
                &provider_peer,
                &requester_peer,
                &f1,
            )
        })
        .and_then(|replacement| {
            if request_identity.requester != *replacement.request
                || request_identity.provider != *replacement.provider_request
            {
                return Err("replacement differs from retained local request identities");
            }
            lifecycle::completed(&events, &replacement, &provider, &requester)
        })
        .is_ok()
    };
    if !complete_lifecycle {
        let message = "The exact accepted F1 replacement did not complete on both provider and requester paths in causal order; request cleanup is unverified";
        record_assertion(world, "request_capacity_and_node_health", false, message)?;
        return Err(failure(message));
    }
    record_assertion(
        world,
        "request_capacity_and_node_health",
        true,
        "Every declared participant answers APIs and the exact accepted F1 request completed on both provider and requester paths",
    )
}

#[when(
    expr = "the provider publishes one descendant that starts a held F0 orphan request within {int} seconds"
)]
#[expect(
    clippy::needless_pass_by_ref_mut,
    reason = "Cucumber step callbacks require a mutable world reference"
)]
#[expect(
    clippy::too_many_lines,
    reason = "This step arranges one genuine missing-parent proposal and proves the held F0 lifecycle"
)]
async fn provider_starts_held_orphan_request(
    world: &mut CucumberWorld,
    timeout_seconds: u64,
) -> StepResult {
    validate_started_participant_topology(world)?;
    if !world
        .era_transition
        .as_ref()
        .is_some_and(|state| state.orphan_recovery)
    {
        return Err(failure("orphan recovery scenario mode is not active"));
    }
    let provider = provider_name(world)?;
    let requester = requester_name(world)?;
    let provider_peer = node_peer_id(world, &provider)
        .ok_or_else(|| failure("provider peer identity is missing"))?;
    let requester_peer = node_peer_id(world, &requester)
        .ok_or_else(|| failure("requester peer identity is missing"))?;
    let (_, f0, _) = era_protocols_for_world(world, &provider)?;
    let deadline = Instant::now() + Duration::from_secs(timeout_seconds);
    super::scenario::set_publication(world, &provider, true).await?;

    let proposal_events = loop {
        let events = collect_observations(world, false).map_err(failure)?;
        let proposals = events
            .iter()
            .filter(|event| {
                event.node == requester
                    && matches!(
                        event.record.event,
                        lb_cryptarchia_sync::observation::Observation::OrphanProposalMissingParent { .. }
                    )
            })
            .count();
        if proposals > 0 {
            super::scenario::set_publication(world, &provider, false).await?;
            if proposals != 1 {
                return Err(failure(format!(
                    "provider publication produced {proposals} missing-parent orphan proposals before suppression"
                )));
            }
            break events;
        }
        if Instant::now() >= deadline {
            super::scenario::set_publication(world, &provider, false).await?;
            return Err(failure(format!(
                "provider published no missing-ancestry block within {timeout_seconds}s"
            )));
        }
        sleep(Duration::from_millis(20)).await;
    };

    let remaining = deadline
        .saturating_duration_since(Instant::now())
        .as_secs()
        .max(1);
    let events = wait_for_observations(world, remaining, |events| {
        lifecycle::orphan_original(
            events,
            &provider,
            &requester,
            &provider_peer,
            &requester_peer,
            &f0,
        )
        .is_ok()
    })
    .await?;
    let original = lifecycle::orphan_original(
        &events,
        &provider,
        &requester,
        &provider_peer,
        &requester_peer,
        &f0,
    )
    .map_err(failure)?;
    if proposal_events.iter().filter(|event| {
        event.node == requester
            && matches!(event.record.event, lb_cryptarchia_sync::observation::Observation::OrphanProposalMissingParent { .. })
    }).count() != 1 {
        return Err(failure("orphan proposal trigger was ambiguous"));
    }
    append_record(
        world,
        "assertion",
        serde_json::json!({
            "assertion": "orphan_f0_partial_progress",
            "status": "passed",
            "orphan_id": original.orphan_id,
            "missing_parent": original.missing_parent,
            "initial_f0_request": observation_reference(original.requester_request_started),
            "orphan_downloader_request": observation_reference(original.download_started),
            "provider_acceptance": observation_reference(original.accepted),
            "provider_first_response": observation_reference(original.sent),
            "requester_first_response": observation_reference(original.received),
            "provider_partial_progress_response": observation_reference(original.progress_sent),
            "requester_partial_progress_response": observation_reference(original.progress_received),
            "requester_first_block_decoded": observation_reference(original.decoded),
            "runtime_orphan_downloader_entered": observation_reference(original.downloading),
            "partial_valid_block": original.partial_block,
            "partial_block_application": observation_reference(original.applied),
            "held_response_count": original.held_blocks,
            "provider_hold": observation_reference(original.hold),
        }),
    )
}

#[then(
    expr = "expiry cancels the held F0 orphan request and schedules autonomous recovery within {int} seconds"
)]
#[expect(
    clippy::needless_pass_by_ref_mut,
    reason = "Cucumber step callbacks require a mutable world reference"
)]
async fn orphan_expiry_schedules_recovery(
    world: &mut CucumberWorld,
    timeout_seconds: u64,
) -> StepResult {
    let provider = provider_name(world)?;
    let requester = requester_name(world)?;
    let provider_peer = node_peer_id(world, &provider)
        .ok_or_else(|| failure("provider peer identity is missing"))?;
    let requester_peer = node_peer_id(world, &requester)
        .ok_or_else(|| failure("requester peer identity is missing"))?;
    let (_, f0, f1) = era_protocols_for_world(world, &provider)?;
    let events = wait_for_observations(world, timeout_seconds, |events| {
        lifecycle::orphan_original(
            events,
            &provider,
            &requester,
            &provider_peer,
            &requester_peer,
            &f0,
        )
        .and_then(|original| {
            lifecycle::orphan_expiry(events, &original, &provider, &requester, &f0, &f1)
        })
        .is_ok()
    })
    .await?;
    let original = lifecycle::orphan_original(
        &events,
        &provider,
        &requester,
        &provider_peer,
        &requester_peer,
        &f0,
    )
    .map_err(failure)?;
    let expiry = lifecycle::orphan_expiry(&events, &original, &provider, &requester, &f0, &f1)
        .map_err(failure)?;
    append_record(
        world,
        "assertion",
        serde_json::json!({
            "assertion": "orphan_expiry_cancellation_and_detection",
            "status": "passed",
            "orphan_id": original.orphan_id,
            "provider_rotation": observation_reference(expiry.provider_rotation),
            "requester_rotation": observation_reference(expiry.requester_rotation),
            "provider_expiry": observation_reference(expiry.expiry),
            "provider_cancellation": observation_reference(expiry.cancelled),
            "requester_interruption": observation_reference(expiry.interruption),
            "runtime_orphan_stream_error": observation_reference(expiry.stream_error),
            "automatic_recovery_decision": observation_reference(expiry.recovery_scheduled),
        }),
    )
}

#[then(expr = "the requester completes its exact F1 orphan replacement within {int} seconds")]
async fn requester_completes_orphan_f1_replacement(
    world: &mut CucumberWorld,
    timeout_seconds: u64,
) -> StepResult {
    let provider = provider_name(world)?;
    let requester = requester_name(world)?;
    let provider_peer = node_peer_id(world, &provider)
        .ok_or_else(|| failure("provider peer identity is missing"))?;
    let requester_peer = node_peer_id(world, &requester)
        .ok_or_else(|| failure("requester peer identity is missing"))?;
    let (_, f0, f1) = era_protocols_for_world(world, &provider)?;
    let events = wait_for_observations(world, timeout_seconds, |events| {
        (|| {
            let original = lifecycle::orphan_original(
                events,
                &provider,
                &requester,
                &provider_peer,
                &requester_peer,
                &f0,
            )?;
            let expiry =
                lifecycle::orphan_expiry(events, &original, &provider, &requester, &f0, &f1)?;
            let replacement = lifecycle::orphan_replacement(
                events,
                &original,
                &expiry,
                &provider,
                &requester,
                &provider_peer,
                &requester_peer,
                &f1,
            )?;
            lifecycle::orphan_completed(events, &original, &replacement, &provider, &requester)
                .map(|_| ())
        })()
        .is_ok()
    })
    .await?;
    let original = lifecycle::orphan_original(
        &events,
        &provider,
        &requester,
        &provider_peer,
        &requester_peer,
        &f0,
    )
    .map_err(failure)?;
    let expiry = lifecycle::orphan_expiry(&events, &original, &provider, &requester, &f0, &f1)
        .map_err(failure)?;
    let replacement = lifecycle::orphan_replacement(
        &events,
        &original,
        &expiry,
        &provider,
        &requester,
        &provider_peer,
        &requester_peer,
        &f1,
    )
    .map_err(failure)?;
    let completion =
        lifecycle::orphan_completed(&events, &original, &replacement, &provider, &requester)
            .map_err(failure)?;
    world
        .era_transition
        .as_mut()
        .ok_or_else(|| failure("era-transition scenario state is missing"))?
        .accepted_f1_request = Some(crate::cucumber::world::EraTransitionAcceptedRequest {
        requester: replacement.request.clone(),
        provider: replacement.provider_request.clone(),
    });
    append_record(
        world,
        "assertion",
        serde_json::json!({
            "assertion": "orphan_automatic_f1_recovery",
            "status": "passed",
            "orphan_id": original.orphan_id,
            "replacement_request": observation_reference(replacement.requester_request_started),
            "replacement_acceptance": observation_reference(replacement.accepted),
            "provider_completion": observation_reference(completion.provider_completed),
            "requester_completion": observation_reference(completion.requester_completed),
            "orphan_download_completion": observation_reference(completion.downloader_completed),
            "orphan_block_application": observation_reference(completion.target_applied),
        }),
    )
}

#[then(
    expr = "the requester retains partial orphan progress and resolves the original ancestry within {int} seconds"
)]
#[expect(
    clippy::needless_pass_by_ref_mut,
    reason = "Cucumber step callbacks require a mutable world reference"
)]
#[expect(
    clippy::too_many_lines,
    reason = "This step proves retained valid progress, ancestry resolution and canonical state for the original orphan"
)]
async fn requester_retains_orphan_progress(
    world: &mut CucumberWorld,
    timeout_seconds: u64,
) -> StepResult {
    let provider = provider_name(world)?;
    let requester = requester_name(world)?;
    let provider_peer = node_peer_id(world, &provider)
        .ok_or_else(|| failure("provider peer identity is missing"))?;
    let requester_peer = node_peer_id(world, &requester)
        .ok_or_else(|| failure("requester peer identity is missing"))?;
    let (_, f0, f1) = era_protocols_for_world(world, &provider)?;
    let events = wait_for_observations(world, timeout_seconds, |events| {
        (|| {
            let original = lifecycle::orphan_original(
                events,
                &provider,
                &requester,
                &provider_peer,
                &requester_peer,
                &f0,
            )?;
            let expiry =
                lifecycle::orphan_expiry(events, &original, &provider, &requester, &f0, &f1)?;
            let replacement = lifecycle::orphan_replacement(
                events,
                &original,
                &expiry,
                &provider,
                &requester,
                &provider_peer,
                &requester_peer,
                &f1,
            )?;
            lifecycle::orphan_completed(events, &original, &replacement, &provider, &requester)
                .map(|_| ())
        })()
        .is_ok()
    })
    .await?;
    let original = lifecycle::orphan_original(
        &events,
        &provider,
        &requester,
        &provider_peer,
        &requester_peer,
        &f0,
    )
    .map_err(failure)?;
    let node = world
        .nodes_info
        .get(&requester)
        .ok_or_else(|| failure("requester node is missing"))?;
    let partial_block_available = node.client.block(&original.partial_block).await?.is_some();
    let missing_parent_available = node.client.block(&original.missing_parent).await?.is_some();
    let orphan_available = node.client.block(&original.orphan_id).await?.is_some();
    let final_tip = node.client.consensus_info().await?.cryptarchia_info.tip;
    let partial_on_final_history =
        crate::cucumber::steps::nodes::era_boundaries::canonical_history_contains_block(
            world,
            &requester,
            final_tip,
            original.partial_block,
            Duration::from_secs(timeout_seconds),
        )
        .await?;
    let parent_on_final_history =
        crate::cucumber::steps::nodes::era_boundaries::canonical_history_contains_block(
            world,
            &requester,
            final_tip,
            original.missing_parent,
            Duration::from_secs(timeout_seconds),
        )
        .await?;
    let orphan_on_final_history =
        crate::cucumber::steps::nodes::era_boundaries::canonical_history_contains_block(
            world,
            &requester,
            final_tip,
            original.orphan_id,
            Duration::from_secs(timeout_seconds),
        )
        .await?;
    if !partial_block_available
        || !missing_parent_available
        || !orphan_available
        || !partial_on_final_history
        || !parent_on_final_history
        || !orphan_on_final_history
    {
        return Err(failure(
            "retained block, missing parent, or original orphan is absent from canonical ancestry",
        ));
    }
    record_assertion(
        world,
        "orphan_progress_retained_and_target_resolved",
        true,
        "the F0 block, missing parent, and original orphan are available on final canonical ancestry after autonomous F1 recovery",
    )?;
    append_record(
        world,
        "orphan_progress_and_resolution",
        serde_json::json!({
            "orphan_id": original.orphan_id,
            "missing_parent": original.missing_parent,
            "partial_block": original.partial_block,
            "final_tip": final_tip,
            "partial_block_available": partial_block_available,
            "partial_block_is_ancestor_of_final_tip": partial_on_final_history,
            "missing_parent_available": missing_parent_available,
            "missing_parent_is_ancestor_of_final_tip": parent_on_final_history,
            "orphan_available": orphan_available,
            "orphan_is_ancestor_of_final_tip": orphan_on_final_history,
        }),
    )
}

#[then(
    expr = "all declared era-transition participants have at least {int} blocks and canonical histories converge to within {int} blocks within {int} seconds"
)]
async fn participants_reach_canonical_convergence(
    world: &mut CucumberWorld,
    minimum_height: u64,
    maximum_height_difference: u64,
    timeout_seconds: u64,
) -> StepResult {
    validate_participant_topology(world)?;
    let convergence = crate::cucumber::steps::nodes::wait_for_canonical_convergence(
        world,
        "era-transition canonical convergence",
        Some(minimum_height),
        maximum_height_difference,
        timeout_seconds,
    )
    .await?;
    crate::cucumber::steps::nodes::era_boundaries::record_canonical_convergence(
        world,
        &convergence,
    )?;
    append_record(
        world,
        "assertion",
        serde_json::json!({
            "assertion": "canonical_convergence",
            "status": "passed",
            "minimum_height": minimum_height,
            "maximum_height_difference": maximum_height_difference,
            "common_block": format!("{:?}", convergence.common_ancestor),
        }),
    )
}
