use std::{num::NonZero, path::PathBuf, time::Duration};

use lb_config::consensus::UNITS_PER_LGO;
use lb_groth16::fr_to_bytes;
use lb_http_api_common::bodies::wallet::{
    balance::WalletBalanceResponseBody, transfer_funds::WalletTransferFundsRequestBody,
};
use lb_key_management_system_service::keys::ZkPublicKey;
use lb_node::config::{RunConfig, deployment::era::ruleset::v1::cryptarchia::EpochConfig};
use lb_testing_framework::{
    DeploymentBuilder, LbcEnv, NodeHttpClient, TopologyConfig as TfTopologyConfig,
    configs::wallet::{WalletAccount, WalletConfig},
};
use lb_utils::math::NonNegativeRatio;
use logos_blockchain_tests::{
    common::manual_cluster::{
        LocalManualClusterHarnessBase, ManualNodeLayout, api_url,
        start_local_manual_cluster_with_layout,
    },
    cucumber::defaults::E2E_ARTIFACTS_DIR,
};
use testing_framework_core::scenario::{DynError, StartedNode};
use tokio::time::{sleep, timeout};

const NODE_COUNT: usize = 1;

/// The leader reward is a note of each block, whose key the header hides; the
/// leader records it under its funding key and its wallet adopts it, also
/// across a restart.
#[tokio::test]
async fn leader_rewards_reach_the_leader_wallet() {
    let (base, nodes, leader_funding_pk, payer_pk) =
        setup_test_nodes("leader_rewards_reach_the_leader_wallet").await;
    let node = &nodes[0];
    let before = wallet_balance(&node.client, leader_funding_pk)
        .await
        .balance;
    // The genesis total stake is above its target, so the reserve releases
    // nothing and the block reward is what the pooled fees pay: a
    // transaction funds it, from another key so the fee does not come out
    // of the balance it should grow.
    node.client
        .transfer_funds(WalletTransferFundsRequestBody {
            tip: None,
            change_public_key: payer_pk,
            funding_public_keys: vec![payer_pk],
            recipient_public_key: payer_pk,
            amount: 1,
        })
        .await
        .expect("the transfer should be submitted");
    wait_for_balance_above(
        &node.client,
        leader_funding_pk,
        before,
        Duration::from_mins(3),
    )
    .await;

    // The notes of the blocks above the LIB are not in the state the wallet
    // restarts from: it adopts them again from the openings it persisted.
    let held = wallet_balance(&node.client, leader_funding_pk).await.notes;
    let cluster = base.cluster();
    cluster
        .restart_node(&node.name)
        .await
        .expect("the node should restart");
    cluster
        .wait_node_ready(&node.name)
        .await
        .expect("the node should become ready after its restart");
    let client = cluster
        .node_client(&node.name)
        .expect("the client of the node should be available after its restart");
    timeout(Duration::from_mins(1), async {
        loop {
            let notes = wallet_balance(&client, leader_funding_pk).await.notes;
            if held.keys().all(|commitment| notes.contains_key(commitment)) {
                return;
            }
            sleep(Duration::from_millis(500)).await;
        }
    })
    .await
    .expect("the wallet should hold its reward notes again after the restart");
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    clippy::large_futures,
    reason = "persistent-node lifecycle includes two restarts"
)]
async fn historical_wallet_recovery_survives_two_running_node_restarts() {
    use lb_core::mantle::note_set::NoteSetIndex;
    use lb_key_management_system_service::keys::{Key, ZkKey};
    use lb_testing_framework::LbcManualCluster;
    use lb_wallet_service::RecoveryOpening;
    use logos_blockchain_tests::common::{
        chain::wait_for_transactions_inclusion, manual_cluster::read_manual_node_logs,
    };
    use testing_framework_core::scenario::StartNodeOptions;

    let (base, nodes, leader_funding_pk, payer_pk) =
        setup_test_nodes("historical_wallet_recovery_survives_two_running_node_restarts").await;
    let node = &nodes[0];
    let recipient = ZkKey::new(1_234_567u64.into());
    let before = wallet_balance(&node.client, payer_pk).await.balance;
    let registrations = (0..8u64).map(|nonce| {
        let url = api_url(&node.client, "");
        async move {
            lb_common_http_client::CommonHttpClient::new(None)
                .import_note(
                    url,
                    lb_http_api_common::bodies::wallet::import::WalletImportNoteRequestBody {
                        note: lb_core::mantle::Note::new(1, (9000 + nonce).into(), payer_pk),
                        note_set: NoteSetIndex::Ledger,
                        historical_path: None,
                    },
                )
                .await
        }
    });
    futures::future::try_join_all(registrations)
        .await
        .expect("ordinary concurrent HTTP registrations succeed");
    assert_eq!(wallet_balance(&node.client, payer_pk).await.balance, before);
    assert_eq!(
        read_manual_node_logs(base.scenario_base_dir(), &node.name)
            .matches("Starting wallet historical recovery")
            .count(),
        0
    );
    let pk = recipient.to_public_key();
    let prepared = node
        .client
        .transfer_funds(WalletTransferFundsRequestBody {
            tip: None,
            change_public_key: payer_pk,
            funding_public_keys: vec![payer_pk],
            recipient_public_key: pk,
            amount: 1000,
        })
        .await
        .expect("prepare and publish a private note to a key not configured on the node");
    assert!(
        wait_for_transactions_inclusion(&node.client, &[prepared.hash], Duration::from_mins(3))
            .await
    );
    let note = prepared.recipient_note;
    let cluster = base.cluster();
    let state_path = std::fs::read_dir(base.scenario_base_dir())
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .find(|path| path.join(lb_testing_framework::USER_CONFIG_FILE).is_file())
        .expect("the original persistent node directory exists");
    let state_args = ["--state-path".to_owned(), state_path.display().to_string()];
    cluster
        .stop_node(&node.name)
        .await
        .expect("stop before changing operator configuration");
    // A new controller applies the changed configuration. Its launch directory
    // is fresh, but --state-path reopens the original database and checkpoints.
    let cluster = LbcManualCluster::from_topology(base.deployment().clone());
    cluster
        .start_node_with(
            &node.name,
            StartNodeOptions::default()
                .with_persist_dir(base.scenario_base_dir().join(&node.name))
                .with_args(state_args.clone())
                .create_patch(move |mut config: lb_testing_framework::LbcNodeConfig| {
                    config.run_config = test_config(config.run_config, leader_funding_pk);
                    config
                        .run_config
                        .user
                        .kms
                        .backend
                        .keys
                        .insert("recovered".into(), Key::Zk(recipient.clone()));
                    config
                        .run_config
                        .user
                        .wallet
                        .known_keys
                        .insert("recovered".into(), pk);
                    config.run_config.user.wallet.historical_recovery = vec![RecoveryOpening {
                        note,
                        note_set: NoteSetIndex::Ledger,
                    }];
                    Ok::<_, DynError>(config)
                }),
        )
        .await
        .expect("restart with operator recovery configured");
    cluster
        .wait_node_ready(&node.name)
        .await
        .expect("startup recovery finishes");
    let client = cluster.node_client(&node.name).unwrap();
    assert_eq!(wait_for_wallet_balance(&client, pk).await.balance, 1000);
    let before = read_manual_node_logs(base.scenario_base_dir(), &node.name)
        .matches("Starting wallet historical recovery")
        .count();
    assert!(before > 0);
    cluster
        .restart_node_with(
            &node.name,
            StartNodeOptions::default().with_args(state_args),
        )
        .await
        .expect("restart using the durable recovered checkpoint");
    cluster
        .wait_node_ready(&node.name)
        .await
        .expect("completed recovery does not scan genesis again");
    let client = cluster.node_client(&node.name).unwrap();
    assert_eq!(wait_for_wallet_balance(&client, pk).await.balance, 1000);
    assert_eq!(
        read_manual_node_logs(base.scenario_base_dir(), &node.name)
            .matches("Starting wallet historical recovery")
            .count(),
        before
    );
}

async fn wait_for_wallet_balance(
    node: &NodeHttpClient,
    pk: ZkPublicKey,
) -> WalletBalanceResponseBody {
    let pk_hex = hex::encode(fr_to_bytes(&pk.into()));
    timeout(Duration::from_mins(1), async {
        loop {
            let response = reqwest::Client::new()
                .get(api_url(node, &format!("wallet/{pk_hex}/balance")))
                .timeout(Duration::from_secs(5))
                .send()
                .await
                .expect("the node HTTP API is ready");
            if response.status() == reqwest::StatusCode::SERVICE_UNAVAILABLE {
                sleep(Duration::from_millis(100)).await;
                continue;
            }
            assert!(response.status().is_success());
            return response.json().await.unwrap();
        }
    })
    .await
    .expect("the wallet exits startup maintenance")
}

async fn wallet_balance(node: &NodeHttpClient, pk: ZkPublicKey) -> WalletBalanceResponseBody {
    let pk_hex = hex::encode(fr_to_bytes(&pk.into()));
    let response = reqwest::Client::new()
        .get(api_url(node, &format!("wallet/{pk_hex}/balance")))
        .send()
        .await
        .expect("balance request should not fail");
    assert!(
        response.status().is_success(),
        "balance request should succeed, got status: {}",
        response.status()
    );
    response
        .json()
        .await
        .expect("balance response should be valid JSON")
}

async fn wait_for_balance_above(
    node: &NodeHttpClient,
    pk: ZkPublicKey,
    before: u64,
    duration: Duration,
) {
    timeout(duration, async {
        while wallet_balance(node, pk).await.balance <= before {
            sleep(Duration::from_millis(500)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("the leader's wallet balance should grow above {before}"));
}

fn test_config(mut config: RunConfig, leader_funding_pk: ZkPublicKey) -> RunConfig {
    config
        .deployment
        .update_genesis_era_parameters(|parameters| {
            parameters.time.slot_duration_in_seconds = 1.try_into().unwrap();
            parameters.cryptarchia.epoch_config = EpochConfig {
                epoch_stake_distribution_stabilization: 1.try_into().unwrap(),
                epoch_period_nonce_buffer: 1.try_into().unwrap(),
                epoch_period_nonce_stabilization: 1.try_into().unwrap(),
            };
            parameters.cryptarchia.security_param = NonZero::new(2).unwrap();
            parameters.cryptarchia.slot_activation_coeff =
                NonNegativeRatio::new(1, 2.try_into().unwrap())
                    .try_into()
                    .unwrap();
            // The widest uncle reference window k allows, ⌊0.6k⌋.
            parameters.cryptarchia.uncle_reference_window_in_block = 1;
        })
        .expect("The changed deployment must be valid.");
    config.user.cryptarchia.leader.wallet.funding_pk = leader_funding_pk;

    config
}

async fn setup_test_nodes(
    test_context: &str,
) -> (
    LocalManualClusterHarnessBase,
    Vec<StartedNode<LbcEnv>>,
    ZkPublicKey,
    ZkPublicKey,
) {
    let (wallet_config, leader_funding_pk, payer_pk) = leader_funding_wallet_config();
    let (base, nodes) = start_local_manual_cluster_with_layout(
        "leader-claim",
        "mantle-leader",
        DeploymentBuilder::new(
            TfTopologyConfig::with_node_numbers(NODE_COUNT)
                .with_allow_multiple_genesis_tokens(true)
                .with_test_context(Some(test_context.to_owned())),
        )
        .with_wallet_config(wallet_config),
        NODE_COUNT,
        ManualNodeLayout::SelectNodeSeed(0),
        move |config| Ok::<_, DynError>(test_config(config, leader_funding_pk)),
        Some(PathBuf::from(E2E_ARTIFACTS_DIR)),
    )
    .await;

    (base, nodes, leader_funding_pk, payer_pk)
}

fn leader_funding_wallet_config() -> (WalletConfig, ZkPublicKey, ZkPublicKey) {
    let account = WalletAccount::deterministic(42, 100_000 * UNITS_PER_LGO, false)
        .expect("leader funding account should be valid");
    let funding_pk = account.public_key();
    let payer = WalletAccount::deterministic(43, 100_000 * UNITS_PER_LGO, false)
        .expect("payer account should be valid");
    let payer_pk = payer.public_key();
    let accounts = (0..3)
        .map(|idx| {
            WalletAccount::new(
                format!("leader-funding-{idx}"),
                account.secret_key.clone(),
                100_000 * UNITS_PER_LGO,
                false,
            )
            .expect("leader funding account should be valid")
        })
        .chain([payer])
        .collect();

    (WalletConfig::new(accounts), funding_pk, payer_pk)
}
