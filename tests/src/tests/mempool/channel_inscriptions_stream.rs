use std::{collections::HashSet, time::Duration};

use futures::StreamExt as _;
use lb_common_http_client::CommonHttpClient;
use lb_core::mantle::{
    Transaction as _,
    ops::channel::{ChannelId, MsgId},
    tx::TxHash,
};
use lb_http_api_common::bodies::channel_inscriptions::ChannelInscriptionsStreamEvent;
use lb_key_management_system_service::keys::Ed25519Key;
use logos_blockchain_tests::{
    common::{chain::scan_chain_until, mantle_tx::create_channel_inscribe_tx},
    nodes::validator::Validator,
    topology::{Topology, TopologyConfig},
};
use reqwest::Url;
use serial_test::serial;
use tokio::time::{sleep, timeout};

async fn wait_until_tx_is_in_chain(
    validator: &Validator,
    tx_hash: TxHash,
    timeout_duration: Duration,
) {
    timeout(timeout_duration, async {
        let mut scanned_blocks = HashSet::new();
        loop {
            let info = validator.consensus_info(false).await;
            let found = scan_chain_until(
                info.tip,
                &mut scanned_blocks,
                |header_id| validator.get_block(header_id),
                |block| {
                    block
                        .transactions()
                        .any(|tx| tx.hash() == tx_hash)
                        .then_some(())
                },
            )
            .await;
            if found.is_some() {
                break;
            }
            sleep(Duration::from_millis(250)).await;
        }
    })
    .await
    .expect("timed out waiting for tx to be visible in channel history");
}

async fn wait_until_validators_synced(a: &Validator, b: &Validator, timeout_duration: Duration) {
    timeout(timeout_duration, async {
        loop {
            let a_info = a.consensus_info(false).await;
            let b_info = b.consensus_info(false).await;
            // Gate test setup on both validators being out of genesis and in sync.
            if a_info.height >= 1 && b_info.height >= 1 && a_info.tip == b_info.tip {
                break;
            }
            sleep(Duration::from_millis(250)).await;
        }
    })
    .await
    .expect("timed out waiting for validators to reach height >= 1 and sync tips");
}

#[tokio::test]
#[serial]
async fn stream_bootstrap_ordering_and_snapshot_duplicate_suppression() {
    let topology = Topology::spawn(TopologyConfig::two_validators()).await;
    let validator = &topology.validators()[0];
    let peer_validator = &topology.validators()[1];
    wait_until_validators_synced(validator, peer_validator, Duration::from_secs(180)).await;
    let validator_url = Url::parse(
        format!(
            "http://{}",
            validator.config().user.api.backend.listen_address
        )
        .as_str(),
    )
    .expect("valid validator URL");
    let client = CommonHttpClient::new(None);
    let signing_key = Ed25519Key::from_bytes(&[7u8; 32]);
    let channel_id = ChannelId::from(signing_key.public_key().to_bytes());
    // Seed one inscription after nodes are synced so it must arrive via bootstrap.
    let tx_bootstrap = create_channel_inscribe_tx(
        &signing_key,
        channel_id,
        b"bootstrap".to_vec(),
        MsgId::root(),
    );
    let bootstrap_hash = tx_bootstrap.hash();
    client
        .post_transaction(validator_url.clone(), tx_bootstrap)
        .await
        .expect("bootstrap tx submission should succeed");
    wait_until_tx_is_in_chain(validator, bootstrap_hash, Duration::from_secs(180)).await;
    let mut stream = Box::pin(
        client
            .subscribe_channel_inscriptions(validator_url.clone(), channel_id, Some(0), false)
            .await
            .expect("channel stream subscription should succeed"),
    );
    let mut observed_bootstrap = false;
    let mut observed_snapshot_lib_advance = false;
    let mut bootstrap_occurrences = 0u32;
    let mut bootstrap_msg_id: Option<MsgId> = None;
    timeout(Duration::from_secs(30), async {
        while !(observed_bootstrap && observed_snapshot_lib_advance) {
            let event = stream
                .next()
                .await
                .expect("stream should produce bootstrap events");
            match event {
                ChannelInscriptionsStreamEvent::InscriptionObserved { item }
                    if item.tx_hash == bootstrap_hash =>
                {
                    observed_bootstrap = true;
                    bootstrap_occurrences += 1;
                    bootstrap_msg_id = Some(item.this_msg_id);
                }
                ChannelInscriptionsStreamEvent::LibAdvanced { .. } if observed_bootstrap => {
                    observed_snapshot_lib_advance = true;
                }
                _ => {}
            }
        }
    })
    .await
    .expect("timed out waiting for bootstrap phase ordering");
    assert!(
        observed_bootstrap,
        "bootstrap inscription must be emitted before live tail"
    );
    assert!(
        observed_snapshot_lib_advance,
        "snapshot lib/tip progress event should be emitted after bootstrap inscriptions"
    );
    // Submit a new inscription after subscribe; it must arrive from live tail,
    // and the bootstrap tx must not be duplicated across the snapshot boundary.
    let live_parent = bootstrap_msg_id
        .expect("bootstrap inscription MsgId must have been captured from the stream");
    let tx_live =
        create_channel_inscribe_tx(&signing_key, channel_id, b"live".to_vec(), live_parent);
    let live_hash = tx_live.hash();
    client
        .post_transaction(validator_url.clone(), tx_live)
        .await
        .expect("live tx submission should succeed");
    let mut observed_live = false;
    timeout(Duration::from_secs(180), async {
        while !observed_live {
            let event = stream
                .next()
                .await
                .expect("stream should continue producing live events");
            if let ChannelInscriptionsStreamEvent::InscriptionObserved { item } = event {
                if item.tx_hash == bootstrap_hash {
                    bootstrap_occurrences += 1;
                }
                if item.tx_hash == live_hash {
                    observed_live = true;
                }
            }
        }
    })
    .await
    .expect("timed out waiting for live inscription event");
    assert_eq!(
        bootstrap_occurrences, 1,
        "bootstrap inscription should be emitted exactly once across bootstrap/live boundary"
    );
}
