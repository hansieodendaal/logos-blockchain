use std::time::Duration;

use futures::StreamExt as _;
use lb_common_http_client::{ChannelInscriptionsRequest, CommonHttpClient};
use lb_core::mantle::{
    Transaction as _,
    ops::channel::{ChannelId, MsgId},
};
use lb_http_api_common::bodies::channel_inscriptions::ChannelInscriptionsStreamEvent;
use lb_key_management_system_service::keys::Ed25519Key;
use logos_blockchain_tests::{
    common::mantle_tx::create_channel_inscribe_tx,
    topology::{Topology, TopologyConfig},
};
use reqwest::Url;
use serial_test::serial;
use tokio::time::timeout;

async fn setup_node_and_channel() -> (Topology, Url, CommonHttpClient, Ed25519Key, ChannelId) {
    let topology = Topology::spawn(
        TopologyConfig::two_validators(),
        Some("test_get_channel_inscriptions_include_mutable_flag"),
    )
    .await;
    let validator = &topology.validators()[0];

    let validator_url = Url::parse(
        format!(
            "http://{}",
            validator.config().user.api.backend.listen_address
        )
        .as_str(),
    )
    .expect("valid validator URL");

    let client = CommonHttpClient::new(None);
    let signing_key = Ed25519Key::from_bytes(&[11u8; 32]);
    let channel_id = ChannelId::from(signing_key.public_key().to_bytes());
    (topology, validator_url, client, signing_key, channel_id)
}

#[tokio::test]
#[serial]
async fn stream_bootstrap_ordering_and_snapshot_duplicate_suppression() {
    let (topology, validator_url, client, signing_key, channel_id) = setup_node_and_channel().await;
    let validator = &topology.validators()[0];

    // Seed one inscription after nodes are synced so it must arrive via bootstrap.
    let (tx_bootstrap, _) = create_channel_inscribe_tx(
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

    validator
        .wait_for_transactions_inclusion(&[bootstrap_hash], Duration::from_secs(180))
        .await;
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
    let (tx_live, _) =
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

#[tokio::test]
#[serial]
async fn test_get_channel_inscriptions_cursor_pagination() {
    let (topology, validator_url, client, signing_key, channel_id) = setup_node_and_channel().await;
    let validator = &topology.validators()[0];

    let mut parent_msg_id = MsgId::root();
    for i in 0..3 {
        let inscription_msg = format!("page-{i}").as_bytes().to_vec();
        let (tx, msg_id) =
            create_channel_inscribe_tx(&signing_key, channel_id, inscription_msg, parent_msg_id);
        parent_msg_id = msg_id;
        let tx_hash = tx.hash();
        client
            .post_transaction(validator_url.clone(), tx)
            .await
            .expect("first inscription submission should succeed");
        validator
            .wait_for_transactions_inclusion(&[tx_hash], Duration::from_secs(60))
            .await;
    }

    let tip_slot: u64 = validator.consensus_info(false).await.slot.into();

    let full_response = client
        .get_channel_inscriptions(
            validator_url.clone(),
            channel_id,
            ChannelInscriptionsRequest::new(0, tip_slot)
                .with_include_inscription(true)
                .with_include_mutable(true),
        )
        .await
        .expect("full channel inscriptions query should succeed");

    assert!(
        full_response.items.len() >= 3,
        "test setup should produce at least three inscriptions to paginate"
    );
    assert!(
        full_response
            .items
            .iter()
            .all(|item| item.inscription.is_some()),
        "include_inscription=true should include inscription payloads"
    );

    let mut stitched_items = Vec::new();
    let mut cursor = None;
    let mut page_count = 0usize;
    loop {
        let request = ChannelInscriptionsRequest::new(0, tip_slot)
            .with_include_inscription(true)
            .with_include_mutable(true)
            .with_cursor(cursor)
            .with_limit(Some(2));

        let page = client
            .get_channel_inscriptions(validator_url.clone(), channel_id, request)
            .await
            .expect("paginated channel inscriptions query should succeed");

        page_count += 1;
        assert!(
            page.items.len() <= 2,
            "page size should respect the requested limit"
        );
        stitched_items.extend(page.items);

        if page.next_cursor.is_none() {
            break;
        }
        cursor = page.next_cursor;
    }

    assert!(
        page_count >= 2,
        "expected pagination to produce multiple pages"
    );
    assert_eq!(stitched_items, full_response.items);

    let out_of_range_request = ChannelInscriptionsRequest::new(0, tip_slot)
        .with_include_inscription(true)
        .with_include_mutable(true)
        .with_cursor(Some(full_response.items.len() as u64 + 10))
        .with_limit(Some(2));
    let out_of_range_page = client
        .get_channel_inscriptions(validator_url, channel_id, out_of_range_request)
        .await
        .expect("out-of-range cursor query should succeed");
    assert!(out_of_range_page.items.is_empty());
    assert_eq!(out_of_range_page.next_cursor, None);
}

#[tokio::test]
#[serial]
async fn test_get_channel_inscriptions_include_mutable_flag() {
    let (topology, validator_url, client, signing_key, channel_id) = setup_node_and_channel().await;
    let validator = &topology.validators()[0];

    let (tx, _) = create_channel_inscribe_tx(
        &signing_key,
        channel_id,
        b"mutable-visibility".to_vec(),
        MsgId::root(),
    );
    let tx_hash = tx.hash();
    client
        .post_transaction(validator_url.clone(), tx)
        .await
        .expect("inscription submission should succeed");
    validator
        .wait_for_transactions_inclusion(&[tx_hash], Duration::from_secs(60))
        .await;

    let tip_slot: u64 = validator.consensus_info(false).await.slot.into();

    let immutable_only = client
        .get_channel_inscriptions(
            validator_url.clone(),
            channel_id,
            ChannelInscriptionsRequest::new(0, tip_slot).with_include_inscription(true),
        )
        .await
        .expect("immutable-only query should succeed");
    let include_mutable = client
        .get_channel_inscriptions(
            validator_url,
            channel_id,
            ChannelInscriptionsRequest::new(0, tip_slot)
                .with_include_inscription(true)
                .with_include_mutable(true),
        )
        .await
        .expect("mutable-inclusive query should succeed");

    assert!(
        include_mutable.items.len() >= immutable_only.items.len(),
        "include_mutable=true must return at least as many items as immutable-only"
    );
}
