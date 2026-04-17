use std::time::Duration;

use clap::Parser as _;
use color_eyre::eyre::{Result, eyre};
use logos_blockchain_node::{
    UserConfig,
    config::{
        CliArgs, DeploymentType, OnUnknownKeys, RunConfig, deployment::DeploymentSettings,
        deserialize_config_at_path,
    },
    get_services_to_start, run_node_from_config,
};
use time::OffsetDateTime;

const WARM_UP_TIME: Duration = Duration::from_secs(20);
const MIN_SLEEP: Duration = Duration::from_secs(1);
const MAX_SLEEP: Duration = Duration::from_secs(24 * 60 * 60); // 24h
const MAX_SLEEP_WINDOW: Duration = Duration::from_secs(36 * 60 * 60); // 36h

#[tokio::main]
async fn main() -> Result<()> {
    #[cfg(feature = "dhat-heap")]
    let _dhat_drop_guard = logos_blockchain_node::global_allocators::dhat_heap::setup();

    let cli_args = CliArgs::parse();

    if let Some(command) = cli_args.command {
        match command {
            #[cfg(feature = "config-gen")]
            logos_blockchain_node::config::Command::Init(init_args) => {
                return logos_blockchain_node::init::run(&init_args);
            }
            logos_blockchain_node::config::Command::Inscribe(inscribe_args) => {
                logos_blockchain_tui_zone::run(inscribe_args).await;
                return Ok(());
            }
        }
    }

    let is_dry_run = cli_args.dry_run();

    // If we are dry-running the binary, fail in case unknown keys in one of the
    // configs are found or exit successfully if deserializations succeed.
    if is_dry_run {
        // Check user config.
        drop(deserialize_config_at_path::<UserConfig>(
            cli_args.config_path(),
            OnUnknownKeys::Fail,
        )?);
        // If custom, check deployment config.
        if let DeploymentType::Custom(custom_deployment_config_file) = cli_args.deployment_type() {
            drop(deserialize_config_at_path::<DeploymentSettings>(
                custom_deployment_config_file,
                OnUnknownKeys::Fail,
            )?);
        }
        #[expect(
            clippy::non_ascii_literal,
            reason = "Use of green checkmark for better UX."
        )]
        {
            println!("Configs are valid! ✅");
        };
        // Early return since we are dry-running.
        return Ok(());
    }

    let ignore_blockchain_start_time = cli_args.ignore_blockchain_start_time();
    let run_config = {
        let user_config =
            deserialize_config_at_path::<UserConfig>(cli_args.config_path(), OnUnknownKeys::Warn)
                .inspect_err(|e| {
                eprintln!("\nExiting... {e}.\n");
            })?;
        user_config.update_from_args(cli_args)?
    };
    wait_for_genesis_block_start_time(&run_config, ignore_blockchain_start_time).await?;

    let app = run_node_from_config(run_config)
        .map_err(|e| eyre!("{e}"))
        .inspect_err(|e| {
            eprintln!("\nExiting... {e}.\n");
        })?;

    let services_to_start = get_services_to_start(&app).await.inspect_err(|e| {
        eprintln!("\nExiting... {e}.\n");
    })?;

    app.handle()
        .start_service_sequence(services_to_start)
        .await
        .map_err(|e| eyre!("start_service_sequence failed: {e}"))
        .inspect_err(|e| {
            eprintln!("\nExiting... {e}.\n");
        })?;

    app.wait_finished().await;
    Ok(())
}

fn calculate_remaining(chain_start_time: OffsetDateTime) -> Duration {
    let offset = chain_start_time - OffsetDateTime::now_utc();
    match Duration::try_from(offset) {
        Ok(d) => d,
        Err(_) if offset.is_negative() => Duration::ZERO,
        Err(_) => Duration::MAX,
    }
}

async fn wait_for_genesis_block_start_time(
    run_config: &RunConfig,
    ignore_blockchain_start_time: bool,
) -> Result<()> {
    let remaining = calculate_remaining(run_config.deployment.time.chain_start_time);
    if ignore_blockchain_start_time && remaining > Duration::ZERO {
        println!(
            "Ignoring chain start time in the future ({:.2?}), still {remaining:.2?} to go ...",
            run_config.deployment.time.chain_start_time,
        );
        return Ok(());
    }
    if remaining == Duration::ZERO {
        return Ok(());
    }

    loop {
        let remaining = calculate_remaining(run_config.deployment.time.chain_start_time);
        if remaining <= WARM_UP_TIME {
            println!(
                "Chain start at {:.2?}, remaining {remaining:.2?}, starting all services now!",
                run_config.deployment.time.chain_start_time,
            );
            break;
        }

        let sleep_time = sleep_time_for(remaining);

        println!(
            "Chain start at {:.2?}, remaining {remaining:.2?}, sleeping {sleep_time:.2?} ...",
            run_config.deployment.time.chain_start_time,
        );

        tokio::time::sleep(sleep_time).await;
    }

    Ok(())
}

// Sleep in 24h chunks until inside the 36h window, then logarithmically
// decrease sleep time until we reach the break window.
fn sleep_time_for(remaining: Duration) -> Duration {
    let until_break = remaining.saturating_sub(WARM_UP_TIME);
    if until_break.is_zero() {
        return Duration::ZERO;
    }

    if until_break > MAX_SLEEP_WINDOW {
        return MAX_SLEEP;
    }

    // Inside the 36h window: logarithmic decrease in sleep duration.
    let secs = until_break.as_secs_f64().max(1.0);
    let scaled_secs = secs / (1.0 + secs.log2());
    let scaled = Duration::from_secs_f64(scaled_secs);

    scaled.max(MIN_SLEEP).min(until_break)
}

#[cfg(test)]
mod tests {
    use std::time::Instant;

    use lb_tests::{
        nodes::create_validator_config,
        topology::configs::{
            create_general_configs, deployment::e2e_deployment_settings_with_genesis_tx,
        },
    };

    use super::*;

    #[ignore = "For visualization only"]
    #[test]
    fn print_sleep_time_as_time_decrease() {
        // This is not a real test, just a way to visualize how the sleep time decreases
        // as we get closer to the break window.
        let start = 3 * MAX_SLEEP_WINDOW;
        let mut current = start;
        while current > WARM_UP_TIME {
            let sleep_time = sleep_time_for(current);
            println!("Remaining: {current:.2?}, Sleep time: {sleep_time:.2?}");
            current = current.saturating_sub(sleep_time);
        }
    }

    #[test]
    fn sleep_time_is_zero_inside_break_window() {
        assert_eq!(sleep_time_for(Duration::ZERO), Duration::ZERO);
        assert_eq!(sleep_time_for(WARM_UP_TIME), Duration::ZERO);
        assert_eq!(
            sleep_time_for(WARM_UP_TIME.saturating_sub(Duration::from_secs(1))),
            Duration::ZERO
        );
    }

    #[test]
    fn sleep_time_is_min_just_outside_break_window() {
        // until_break = 1s => should floor to MIN_SLEEP (currently 1s)
        let remaining = WARM_UP_TIME + Duration::from_secs(1);
        assert_eq!(sleep_time_for(remaining), MIN_SLEEP);
    }

    #[test]
    fn sleep_time_caps_to_max_when_far_away() {
        // until_break > MAX_SLEEP_WINDOW => hard cap to MAX_SLEEP
        let remaining = WARM_UP_TIME + MAX_SLEEP_WINDOW + Duration::from_secs(1);
        assert_eq!(sleep_time_for(remaining), MAX_SLEEP);
    }

    #[test]
    fn sleep_time_at_window_boundary_is_not_hard_capped() {
        // Boundary goes to logarithmic branch because condition is `>`
        let remaining = WARM_UP_TIME + MAX_SLEEP_WINDOW;
        let got = sleep_time_for(remaining);
        assert!(got <= MAX_SLEEP_WINDOW);
        assert!(got >= MIN_SLEEP);
        assert!(
            got < MAX_SLEEP,
            "boundary is currently logarithmic, not capped"
        );
    }

    #[test]
    fn sleep_time_is_bounded_in_log_branch() {
        // Representative "inside log window" value
        let remaining = WARM_UP_TIME + Duration::from_secs(600);
        let got = sleep_time_for(remaining);

        assert!(got >= MIN_SLEEP);
        assert!(got <= Duration::from_secs(600));
    }

    #[test]
    fn sleep_time_grows_with_more_remaining_time_in_log_branch() {
        // Monotonicity sanity check in log branch
        let small = sleep_time_for(WARM_UP_TIME + Duration::from_secs(60));
        let medium = sleep_time_for(WARM_UP_TIME + Duration::from_secs(600));
        let large = sleep_time_for(WARM_UP_TIME + Duration::from_secs(3600));

        assert!(small <= medium);
        assert!(medium <= large);
    }

    #[test]
    fn sleep_time_just_above_24h_is_log_not_24h_chunk() {
        let remaining = WARM_UP_TIME + Duration::from_secs_f64(24.0001 * 3600.0);
        let got = sleep_time_for(remaining);
        assert!(
            got < MAX_SLEEP,
            "24.0001h should use log branch, not 24h chunk"
        );
    }

    #[test]
    fn sleep_time_just_above_36h_is_24h_chunk() {
        let remaining = WARM_UP_TIME + Duration::from_secs_f64(36.0001 * 3600.0);
        let got = sleep_time_for(remaining);
        assert_eq!(got, MAX_SLEEP, "36.0001h should still use 24h chunk");
    }

    #[tokio::test]
    async fn test_wait_for_genesis_block_start_time() {
        let (configs, genesis_tx) = create_general_configs(1, None);
        let deployment_settings = e2e_deployment_settings_with_genesis_tx(genesis_tx);
        let mut config = create_validator_config(configs[0].clone(), deployment_settings);

        let future_time_delta = Duration::from_secs(33);

        // Wait for future time
        config.deployment.time.chain_start_time = OffsetDateTime::now_utc() + future_time_delta;
        let start = Instant::now();
        wait_for_genesis_block_start_time(&config, false)
            .await
            .unwrap();
        assert!(
            start.elapsed() >= future_time_delta.saturating_sub(WARM_UP_TIME),
            "Should have slept for at least the remaining time minus the max sleep window"
        );

        // Ignore future time
        config.deployment.time.chain_start_time = OffsetDateTime::now_utc() + future_time_delta;
        let start = Instant::now();
        wait_for_genesis_block_start_time(&config, true)
            .await
            .unwrap();
        assert!(
            start.elapsed() <= Duration::from_millis(100),
            "Should not sleep"
        );
    }
}
