use std::time::{Duration, Instant};

use cucumber::{gherkin::Step, then, when};
use tokio::time::{sleep, timeout};
use tracing::info;

use crate::cucumber::{
    background_tasks::CONTINUOUS_NEXT_WALLET_LOAD_TASK,
    error::{StepError, StepResult},
    steps::{
        mempool::{
            actions::{
                prepare_transfer_transaction, submit_prepared_transaction_through_blend,
                submit_prepared_transaction_to_nodes, try_submit_invalid_transaction,
                wait_for_mempool_recovery_flush,
            },
            assertions::{
                assert_transaction_not_pending_on_all_nodes, assert_transaction_pending_on_nodes,
                assert_transaction_remains_not_pending_on_all_nodes,
            },
        },
        nodes::diagnostics::BlendDiagnosticEventLogger,
    },
    world::CucumberWorld,
};

#[when(expr = "I record mempool pending counts for {string} workload at {string}")]
#[expect(
    clippy::needless_pass_by_ref_mut,
    reason = "Cucumber step functions require `&mut World` as the first parameter"
)]
async fn step_record_mempool_pending_counts(
    world: &mut CucumberWorld,
    workload_mode: String,
    observation_phase: String,
) -> StepResult {
    record_mempool_pending_counts(world, &workload_mode, &observation_phase).await
}

#[when(expr = "I observe the {string} mempool load for {int} seconds")]
#[expect(
    clippy::needless_pass_by_ref_mut,
    reason = "Cucumber step functions require `&mut World` as the first parameter"
)]
async fn step_observe_mempool_load(
    world: &mut CucumberWorld,
    workload_mode: String,
    duration_seconds: u64,
) -> StepResult {
    observe_mempool_window(world, &workload_mode, duration_seconds, true).await
}

#[when(expr = "I observe the {string} mempool drain for {int} seconds")]
#[expect(
    clippy::needless_pass_by_ref_mut,
    reason = "Cucumber step functions require `&mut World` as the first parameter"
)]
async fn step_observe_mempool_drain(
    world: &mut CucumberWorld,
    workload_mode: String,
    duration_seconds: u64,
) -> StepResult {
    observe_mempool_window(world, &workload_mode, duration_seconds, false).await
}

#[when(expr = "I observe the {string} mempool drain for {int} epochs")]
#[expect(
    clippy::needless_pass_by_ref_mut,
    reason = "Cucumber step functions require `&mut World` as the first parameter"
)]
async fn step_observe_mempool_drain_for_epochs(
    world: &mut CucumberWorld,
    workload_mode: String,
    drain_epochs: u64,
) -> StepResult {
    observe_mempool_drain_for_epochs(world, &workload_mode, drain_epochs).await
}

async fn observe_mempool_window(
    world: &CucumberWorld,
    workload_mode: &str,
    duration_seconds: u64,
    check_load_health: bool,
) -> StepResult {
    let started = Instant::now();
    let duration = Duration::from_secs(duration_seconds);
    let mut sample = 0usize;

    loop {
        if check_load_health {
            world.ensure_background_task_healthy(CONTINUOUS_NEXT_WALLET_LOAD_TASK)?;
        }
        record_mempool_pending_counts(
            world,
            workload_mode,
            &format!(
                "{}_{sample}",
                if check_load_health {
                    "running"
                } else {
                    "draining"
                }
            ),
        )
        .await?;
        sample = sample.saturating_add(1);

        let remaining = duration.saturating_sub(started.elapsed());
        if remaining.is_zero() {
            break;
        }
        sleep(remaining.min(Duration::from_secs(30))).await;
    }

    if check_load_health {
        world.ensure_background_task_healthy(CONTINUOUS_NEXT_WALLET_LOAD_TASK)?;
    }
    BlendDiagnosticEventLogger::from_world(world).append_named_timeline_record(
        "mempool_diagnostic_observation_completed",
        &serde_json::json!({
            "workload_mode": workload_mode,
            "observation_phase": if check_load_health { "load" } else { "drain" },
            "duration_seconds": started.elapsed().as_secs(),
            "sample_count": sample,
        }),
    );
    Ok(())
}

struct MempoolDrainObservation {
    requested_epochs: u32,
    start_epoch: u32,
    target_epoch: u32,
    end_epoch: u32,
    end_slot: u64,
    end_height: Option<u64>,
    duration: Duration,
    sample_count: usize,
    reference_node: String,
}

fn log_mempool_drain_observation_completed(
    world: &CucumberWorld,
    workload_mode: &str,
    observation: &MempoolDrainObservation,
) {
    BlendDiagnosticEventLogger::from_world(world).append_named_timeline_record(
        "mempool_diagnostic_observation_completed",
        &serde_json::json!({
            "workload_mode": workload_mode,
            "observation_phase": "drain",
            "duration_epochs_requested": observation.requested_epochs,
            "start_epoch": observation.start_epoch,
            "target_epoch": observation.target_epoch,
            "end_epoch": observation.end_epoch,
            "end_slot": observation.end_slot,
            "end_height": observation.end_height,
            "duration_seconds": observation.duration.as_secs(),
            "sample_count": observation.sample_count,
            "reference_node": observation.reference_node,
        }),
    );
    info!(
        workload_mode,
        reference_node = observation.reference_node,
        start_epoch = observation.start_epoch,
        target_epoch = observation.target_epoch,
        end_epoch = observation.end_epoch,
        samples = observation.sample_count,
        duration_seconds = observation.duration.as_secs(),
        "Completed epoch-based mempool drain observation"
    );
}

async fn observe_mempool_drain_for_epochs(
    world: &CucumberWorld,
    workload_mode: &str,
    drain_epochs: u64,
) -> StepResult {
    if drain_epochs == 0 {
        return Err(StepError::InvalidArgument {
            message: "mempool drain epoch count must be greater than zero".to_owned(),
        });
    }
    let drain_epochs = u32::try_from(drain_epochs).map_err(|_| StepError::InvalidArgument {
        message: format!("mempool drain epoch count {drain_epochs} exceeds the supported range"),
    })?;

    let mut node_names = world.all_node_names();
    node_names.sort();
    let reference_node = node_names.first().ok_or(StepError::MissingTopology)?;
    let reference_client = world.resolve_node_http_client(reference_node)?;
    let initial_time = timeout(Duration::from_secs(10), reference_client.time_info())
        .await
        .map_err(|_| StepError::Timeout {
            message: format!("timed out reading the starting epoch from `{reference_node}`"),
        })?
        .map_err(|error| StepError::StepFail {
            message: format!(
                "failed to read the starting epoch from reference node `{reference_node}`: {error}"
            ),
        })?;
    let start_epoch = initial_time.current_epoch;
    let target_epoch = start_epoch.checked_add(drain_epochs).ok_or_else(|| {
        StepError::InvalidArgument {
            message: format!(
                "mempool drain target epoch overflows: current epoch {start_epoch}, requested {drain_epochs}"
            ),
        }
    })?;
    let started = Instant::now();
    let maximum_wait = Duration::from_secs(u64::from(drain_epochs).saturating_mul(15 * 60));
    let mut observed_epoch = start_epoch;
    let mut end_slot = initial_time.current_slot;
    let mut last_sample = started;
    let mut sample = 0usize;

    record_mempool_pending_counts(
        world,
        workload_mode,
        &format!("draining_epoch_{observed_epoch}_{sample}"),
    )
    .await?;
    sample = sample.saturating_add(1);

    while observed_epoch < target_epoch {
        if started.elapsed() >= maximum_wait {
            return Err(StepError::Timeout {
                message: format!(
                    "mempool drain on `{reference_node}` did not advance from epoch {start_epoch} to {target_epoch} within {} seconds",
                    maximum_wait.as_secs()
                ),
            });
        }
        sleep(Duration::from_secs(1)).await;
        let time_info = timeout(Duration::from_secs(10), reference_client.time_info())
            .await
            .map_err(|_| StepError::Timeout {
                message: format!("timed out reading the current epoch from `{reference_node}`"),
            })?
            .map_err(|error| StepError::StepFail {
                message: format!(
                    "failed to read the current epoch from reference node `{reference_node}`: {error}"
                ),
            })?;

        let epoch_advanced = time_info.current_epoch > observed_epoch;
        if epoch_advanced {
            observed_epoch = time_info.current_epoch;
            end_slot = time_info.current_slot;
        }
        if epoch_advanced || last_sample.elapsed() >= Duration::from_secs(30) {
            record_mempool_pending_counts(
                world,
                workload_mode,
                &format!("draining_epoch_{observed_epoch}_{sample}"),
            )
            .await?;
            sample = sample.saturating_add(1);
            last_sample = Instant::now();
        }
    }

    let final_consensus = reference_client.consensus_info().await.ok();
    log_mempool_drain_observation_completed(
        world,
        workload_mode,
        &MempoolDrainObservation {
            requested_epochs: drain_epochs,
            start_epoch,
            target_epoch,
            end_epoch: observed_epoch,
            end_slot,
            end_height: final_consensus.map(|info| info.cryptarchia_info.height),
            duration: started.elapsed(),
            sample_count: sample,
            reference_node: reference_node.clone(),
        },
    );
    Ok(())
}

pub(crate) async fn record_mempool_pending_counts(
    world: &CucumberWorld,
    workload_mode: &str,
    observation_phase: &str,
) -> StepResult {
    let mut node_names = world.all_node_names();
    node_names.sort();

    let mut nodes = Vec::with_capacity(node_names.len());
    let mut pending_counts = Vec::with_capacity(node_names.len());
    for node_name in node_names {
        let snapshot = match world.resolve_node_http_client(&node_name) {
            Ok(client) => {
                let (pending, consensus) =
                    tokio::join!(client.test_mempool_view(), client.consensus_info());
                let (pending_count, pending_view_error) = match pending {
                    Ok(items) => {
                        let count = items.len();
                        pending_counts.push(count);
                        (Some(count), None)
                    }
                    Err(error) => (None, Some(error.to_string())),
                };
                let (height, consensus_info_error) = match consensus {
                    Ok(info) => (Some(info.cryptarchia_info.height), None),
                    Err(error) => (None, Some(error.to_string())),
                };

                serde_json::json!({
                    "node_name": node_name,
                    "height": height,
                    "pending_count": pending_count,
                    "pending_view_error": pending_view_error,
                    "consensus_info_error": consensus_info_error,
                })
            }
            Err(error) => serde_json::json!({
                "node_name": node_name,
                "pending_count": serde_json::Value::Null,
                "pending_view_error": error.to_string(),
                "consensus_info_error": serde_json::Value::Null,
            }),
        };
        nodes.push(snapshot);
    }

    let total_pending_count = pending_counts.iter().sum::<usize>();
    let max_node_pending_count = pending_counts.iter().copied().max();
    info!(
        workload_mode,
        observation_phase,
        total_pending_count,
        max_node_pending_count,
        "Mempool pending-count diagnostic snapshot"
    );

    BlendDiagnosticEventLogger::from_world(world).append_named_timeline_record(
        "mempool_pending_count_snapshot",
        &serde_json::json!({
            "workload_mode": workload_mode,
            "observation_phase": observation_phase,
            "node_count": nodes.len(),
            "total_pending_count": total_pending_count,
            "max_node_pending_count": max_node_pending_count,
            "nodes": nodes,
        }),
    );

    Ok(())
}

#[when(
    expr = "I prepare transfer transaction {string} of {int} LGO from wallet {string} to wallet {string}"
)]
async fn step_prepare_transfer_transaction(
    world: &mut CucumberWorld,
    step: &Step,
    transaction_alias: String,
    amount: u64,
    sender_wallet_name: String,
    receiver_wallet_name: String,
) -> StepResult {
    prepare_transfer_transaction(
        world,
        &step.value,
        transaction_alias,
        amount,
        sender_wallet_name,
        receiver_wallet_name,
    )
    .await
}

#[when(expr = "I submit prepared transaction {string} to nodes:")]
#[expect(
    clippy::needless_pass_by_ref_mut,
    reason = "Cucumber step functions require `&mut World` as the first parameter"
)]
async fn step_submit_prepared_transaction_to_nodes(
    world: &mut CucumberWorld,
    step: &Step,
    transaction_alias: String,
) -> StepResult {
    let node_names = parse_node_names_table(step)?;

    submit_prepared_transaction_to_nodes(world, &step.value, transaction_alias, node_names).await
}

#[when(expr = "I submit prepared transaction {string} through Blend on node {string}")]
#[expect(
    clippy::needless_pass_by_ref_mut,
    reason = "Cucumber step functions require `&mut World` as the first parameter"
)]
async fn step_submit_prepared_transaction_through_blend(
    world: &mut CucumberWorld,
    step: &Step,
    transaction_alias: String,
    node_name: String,
) -> StepResult {
    submit_prepared_transaction_through_blend(world, &step.value, &transaction_alias, &node_name)
        .await
}

#[when(expr = "I try to submit invalid transaction {string} to node {string}")]
async fn step_try_submit_invalid_transaction(
    world: &mut CucumberWorld,
    step: &Step,
    transaction_alias: String,
    node_name: String,
) -> StepResult {
    try_submit_invalid_transaction(world, &step.value, transaction_alias, node_name).await
}

#[then(expr = "mempool recovery for node {string} contains transaction {string}")]
#[expect(
    clippy::needless_pass_by_ref_mut,
    reason = "Cucumber step functions require `&mut World` as the first parameter"
)]
async fn step_wait_for_pending_mempool_recovery_flush(
    world: &mut CucumberWorld,
    node_name: String,
    transaction_alias: String,
) -> StepResult {
    wait_for_mempool_recovery_flush(world, &node_name, &transaction_alias).await
}

#[then(expr = "transaction {string} is pending in mempool of nodes in {int} seconds:")]
#[expect(
    clippy::needless_pass_by_ref_mut,
    reason = "Cucumber step functions require `&mut World` as the first parameter"
)]
async fn step_transaction_pending_in_node_mempools(
    world: &mut CucumberWorld,
    step: &Step,
    transaction_alias: String,
    timeout_seconds: u64,
) -> StepResult {
    let node_names = parse_node_names_table(step)?;

    assert_transaction_pending_on_nodes(world, transaction_alias, node_names, timeout_seconds).await
}

#[then(expr = "transaction {string} is not pending in mempool of all nodes in {int} seconds")]
#[expect(
    clippy::needless_pass_by_ref_mut,
    reason = "Cucumber step functions require `&mut World` as the first parameter"
)]
async fn step_transaction_not_pending_in_all_mempools(
    world: &mut CucumberWorld,
    step: &Step,
    transaction_alias: String,
    timeout_seconds: u64,
) -> StepResult {
    let _ = step;

    assert_transaction_not_pending_on_all_nodes(world, transaction_alias, timeout_seconds).await
}

#[then(
    expr = "transaction {string} remains not pending in mempool of all nodes for {int} blocks in {int} seconds"
)]
#[expect(
    clippy::needless_pass_by_ref_mut,
    reason = "Cucumber step functions require `&mut World` as the first parameter"
)]
async fn step_transaction_remains_not_pending_in_all_mempools(
    world: &mut CucumberWorld,
    step: &Step,
    transaction_alias: String,
    blocks: u64,
    timeout_seconds: u64,
) -> StepResult {
    let _ = step;

    assert_transaction_remains_not_pending_on_all_nodes(
        world,
        transaction_alias,
        blocks,
        timeout_seconds,
    )
    .await
}

fn parse_node_names_table(step: &Step) -> Result<Vec<String>, StepError> {
    let table = step.table.as_ref().ok_or(StepError::MissingTable)?;

    if table.rows.is_empty() || table.rows[0].len() != 1 || table.rows[0][0].trim() != "node_name" {
        return Err(StepError::InvalidArgument {
            message: "Expected table columns: | node_name |".to_owned(),
        });
    }

    table
        .rows
        .iter()
        .skip(1)
        .map(|row| {
            if row.len() != 1 {
                return Err(StepError::InvalidArgument {
                    message: "Each node row must have exactly one column".to_owned(),
                });
            }

            Ok(row[0].trim().to_owned())
        })
        .collect()
}
