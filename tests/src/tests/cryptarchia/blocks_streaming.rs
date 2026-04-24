use std::{collections::BTreeMap, num::NonZero, time::Duration};

use futures::stream::{self, StreamExt as _};
use lb_common_http_client::ProcessedBlockEvent;
use lb_core::header::HeaderId;
use logos_blockchain_tests::{
    common::time::max_block_propagation_time,
    nodes::{Validator, create_validator_config},
    topology::configs::{
        create_general_configs, deployment::e2e_deployment_settings_with_genesis_tx,
    },
};
use serial_test::serial;
use tokio::time::timeout;

const CHAIN_LENGTH_MULTIPLIER: u32 = 3;

struct CanonicalChain {
    ids_by_height: BTreeMap<usize, HeaderId>,
    lib_height: usize,
    tip_height: usize,
}

async fn spawn_two_validators(test_name: &str) -> [Validator; 2] {
    let (configs, genesis_tx) = create_general_configs(2, Some(test_name));
    let deployment_settings = e2e_deployment_settings_with_genesis_tx(genesis_tx);

    let configs = configs
        .into_iter()
        .map(|c| {
            let mut config = create_validator_config(c, deployment_settings.clone());
            config.deployment.time.slot_duration = Duration::from_secs(1);
            config
                .user
                .cryptarchia
                .service
                .bootstrap
                .prolonged_bootstrap_period = Duration::ZERO;
            config.deployment.cryptarchia.security_param = NonZero::new(7).unwrap();
            config
        })
        .collect::<Vec<_>>();

    let nodes = futures_util::future::join_all(configs.into_iter().map(Validator::spawn))
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    let mut iter = nodes.into_iter();
    [iter.next().unwrap(), iter.next().unwrap()]
}

async fn wait_for_lib_and_tip(nodes: &[Validator; 2]) -> lb_chain_service::CryptarchiaInfo {
    let config = nodes[0].config();

    let min_height = config.deployment.cryptarchia.security_param.get() * 2 - 1;
    let timeout = max_block_propagation_time(
        min_height * CHAIN_LENGTH_MULTIPLIER,
        nodes.len().try_into().unwrap(),
        &config.deployment,
        3.0,
    );
    println!(
        "waiting for canonical chain with height >= {min_height}, lib height >= 3 and tip at least 2 above LIB: timeout:{timeout:?}",
    );
    let timeout = tokio::time::sleep(timeout);
    let mut tick: u32 = 0;
    tokio::select! {
        () = timeout => panic!("timed out waiting for 'lib_slot >= 1 && tip_slot > lib_slot'"),

        () = async { loop {
                let infos = stream::iter(nodes)
                    .then(async |n| n.consensus_info(tick == 0).await)
                    .collect::<Vec<_>>()
                    .await;

                let all_reached_min_height = infos
                    .iter()
                    .all(|info| info.height >= u64::from(min_height));

                if all_reached_min_height {
                    let chain = canonical_chain(&nodes[0], &infos[0]).await;
                    if chain.lib_height >= 3 && chain.tip_height >= chain.lib_height + 2 {
                        break;
                    }
                }

                if tick.is_multiple_of(20) {
                    println!(
                        "waiting... {}",
                        infos.iter()
                            .map(|info| format!("{}/{:?}/{:?}", info.height, info.slot, info.lib_slot))
                            .collect::<Vec<_>>()
                            .join(" | ")
                    );
                }
                tick = tick.wrapping_add(1);
                tokio::time::sleep(Duration::from_millis(100)).await;
            }

        } => {}
    }
    // Print final stats
    let final_infos = stream::iter(nodes)
        .then(async |n| {
            let info = n.consensus_info(false).await;
            format!("{}/{:?}/{:?}", info.height, info.slot, info.lib_slot)
        })
        .collect::<Vec<_>>()
        .await;
    println!("final: {}", final_infos.join(" | "));

    nodes[0].consensus_info(false).await
}

async fn canonical_chain(
    node: &Validator,
    final_info: &lb_chain_service::CryptarchiaInfo,
) -> CanonicalChain {
    let mut ids_by_height = BTreeMap::new();
    let mut current_id = final_info.tip;
    let mut current_height = usize::try_from(final_info.height).unwrap();
    let mut lib_height = None;

    while current_height >= 1 {
        let block = node
            .get_api_block(current_id)
            .await
            .expect("canonical block request should succeed")
            .expect("canonical block should exist");
        ids_by_height.insert(current_height, block.header.id);

        if current_id == final_info.lib {
            lib_height = Some(current_height);
        }

        if current_height == 1 {
            break;
        }

        current_id = block.header.parent_block;
        current_height -= 1;
    }

    CanonicalChain {
        ids_by_height,
        lib_height: lib_height.expect("lib must be on the canonical chain"),
        tip_height: usize::try_from(final_info.height).unwrap(),
    }
}

async fn setup_nodes_and_chain(test_name: &str) -> ([Validator; 2], CanonicalChain) {
    let nodes = spawn_two_validators(test_name).await;
    let final_info = wait_for_lib_and_tip(&nodes).await;
    let chain = canonical_chain(&nodes[0], &final_info).await;
    (nodes, chain)
}

async fn collect_stream_events(
    node: &Validator,
    number_of_blocks: NonZero<usize>,
    blocks_to: HeaderId,
    chunk_size: Option<NonZero<usize>>,
    immutable_only: bool,
) -> Vec<ProcessedBlockEvent> {
    let stream = node
        .get_blocks_stream_in_range_with_chunk_size(
            Some(number_of_blocks),
            Some(blocks_to),
            chunk_size,
            Some(immutable_only),
        )
        .await
        .expect("blocks stream request should succeed");

    timeout(Duration::from_secs(5), stream.collect::<Vec<_>>())
        .await
        .expect("timed out collecting blocks stream events")
}

async fn request_stream_events(
    node: &Validator,
    blocks_from: NonZero<usize>,
    blocks_to: NonZero<usize>,
    chunk_size: Option<NonZero<usize>>,
    immutable_only: bool,
) -> (CanonicalChain, Vec<ProcessedBlockEvent>) {
    let info = node.consensus_info(false).await;
    let chain = canonical_chain(node, &info).await;
    let (number_of_blocks, blocks_to) = blocks_request(&chain, blocks_from, blocks_to);
    let events = collect_stream_events(
        node,
        number_of_blocks,
        blocks_to,
        chunk_size,
        immutable_only,
    )
    .await;
    (chain, events)
}

fn assert_stream_integrity(chain: &CanonicalChain, events: &[ProcessedBlockEvent]) {
    let chain_lib_header_id = chain
        .ids_by_height
        .get(&chain.lib_height)
        .copied()
        .expect("chain should have at least one block");
    let chain_tip_header_id = chain
        .ids_by_height
        .get(&chain.tip_height)
        .copied()
        .expect("chain should have at least one block");
    assert_eq!(chain_tip_header_id, events[0].tip);
    assert_eq!(chain_lib_header_id, events[0].lib);
}

fn blocks_request(
    chain: &CanonicalChain,
    from_height: NonZero<usize>,
    to_height: NonZero<usize>,
) -> (NonZero<usize>, HeaderId) {
    let number_of_blocks =
        NonZero::new(to_height.get() - from_height.get() + 1).expect("must be non-zero");
    let blocks_to = chain
        .ids_by_height
        .get(&to_height.get())
        .copied()
        .expect("expected height must exist on canonical chain");
    (number_of_blocks, blocks_to)
}

#[tokio::test]
#[serial]
async fn test_blocks_streaming() {
    let (nodes, chain) = setup_nodes_and_chain("blocks_streaming_use_cases_share_one_setup").await;
    assert!(
        chain.lib_height > 1,
        "lib height must allow a block below LIB"
    );
    assert!(
        chain.tip_height > chain.lib_height,
        "tip height must allow a block above LIB"
    );
    assert!(chain.lib_height >= 3, "lib height must be at least 3");
    assert!(
        chain.tip_height >= chain.lib_height + 2,
        "tip height must allow streaming three blocks starting from LIB"
    );

    // case: single block below LIB
    println!("case: single block below LIB");

    let target_height = nz(chain.lib_height - 3);
    let (request_chain, events) =
        request_stream_events(&nodes[0], target_height, target_height, None, false).await;

    assert_eq!(events.len(), 1);
    assert_stream_integrity(&request_chain, &events);

    // case: single block at LIB
    println!("case: single block at LIB");

    let target_height = nz(chain.lib_height);
    let (request_chain, events) =
        request_stream_events(&nodes[0], target_height, target_height, None, false).await;

    assert_eq!(events.len(), 1);
    assert_stream_integrity(&request_chain, &events);

    // case: single block above LIB
    println!("case: single block above LIB");

    let target_height = nz(chain.lib_height + 3);
    let (request_chain, events) =
        request_stream_events(&nodes[0], target_height, target_height, None, false).await;
    assert_stream_integrity(&request_chain, &events);

    assert_eq!(events.len(), 1);

    // case: single block above LIB (immutable only, should cap results at LIB)
    println!("case: single block above LIB (immutable only, should cap results at LIB)");

    let target_height = nz(chain.lib_height + 3);
    let (_, events) =
        request_stream_events(&nodes[0], target_height, target_height, None, true).await;

    assert_eq!(
        events.len(),
        0,
        "single block above LIB (immutable only) should return at most one block"
    );

    // case: three blocks up to LIB
    println!("case: three blocks up to LIB");

    let blocks_from = nz(chain.lib_height - 2);
    let blocks_to = nz(chain.lib_height);
    let (request_chain, events) =
        request_stream_events(&nodes[0], blocks_from, blocks_to, Some(nz(1)), false).await;
    assert_stream_integrity(&request_chain, &events);

    assert_eq!(events.len(), 3);

    // case: three blocks from LIB and up
    println!("case: three blocks from LIB and up");

    let blocks_from = nz(chain.lib_height);
    let blocks_to = nz(chain.lib_height + 2);
    let (request_chain, events) =
        request_stream_events(&nodes[0], blocks_from, blocks_to, None, false).await;
    assert_stream_integrity(&request_chain, &events);

    assert_eq!(events.len(), 3);

    // case: three blocks from LIB and up (immutable only, should cap results at
    // LIB)
    println!("case: three blocks from LIB and up (immutable only, should cap results at LIB)");

    let blocks_from = nz(chain.lib_height);
    let blocks_to = nz(chain.lib_height + 2);
    let (request_chain, events) =
        request_stream_events(&nodes[0], blocks_from, blocks_to, None, true).await;
    assert_stream_integrity(&request_chain, &events);

    assert_eq!(events.len(), 1);
    assert_eq!(
        events.last().unwrap().block.header.id,
        events[0].lib,
        "last block should be LIB when immutable_only=true"
    );

    // case: all blocks, small chunked
    println!("case: all blocks, small chunked");

    let blocks_from = nz(1);
    let blocks_to = nz(13);
    let (request_chain, events) =
        request_stream_events(&nodes[0], blocks_from, blocks_to, Some(nz(4)), false).await;
    assert_stream_integrity(&request_chain, &events);

    assert_eq!(events.len(), blocks_to.get());

    // case: all blocks, small chunked (immutable only, should cap results at LIB)
    println!("case: all blocks, small chunked (immutable only, should cap results at LIB)");

    let blocks_from = nz(1);
    let blocks_to = nz(13);
    let (request_chain, events) =
        request_stream_events(&nodes[0], blocks_from, blocks_to, Some(nz(4)), true).await;
    assert_stream_integrity(&request_chain, &events);

    assert_eq!(events.len(), request_chain.lib_height);
    assert_eq!(
        events.last().unwrap().block.header.id,
        events[0].lib,
        "last block should be LIB when immutable_only=true"
    );
}

const fn nz(value: usize) -> NonZero<usize> {
    NonZero::new(value).unwrap()
}
