use std::{
    fmt::{Debug, Display},
    num::NonZero,
};

use ::libp2p::PeerId;
use axum::{
    Json,
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse as _, Response},
};
use futures::FutureExt as _;
use lb_api_service::http::{
    DynError, blend,
    consensus::{self, Cryptarchia},
    libp2p, mantle, mempool,
    storage::StorageAdapter,
};
use lb_chain_broadcast_service::BlockBroadcastService;
use lb_chain_leader_service::api::ChainLeaderServiceData;
use lb_core::{
    block::Block,
    header::HeaderId,
    mantle::{
        Op, SignedMantleTx, Transaction, TxHash, gas::MainnetGasConstants, ops::channel::ChannelId,
        tx_builder::MantleTxBuilder,
    },
};
use lb_http_api_common::{
    bodies::{
        channel::{ChannelDepositRequestBody, ChannelDepositResponseBody},
        wallet::{
            balance::WalletBalanceResponseBody,
            transfer_funds::{WalletTransferFundsRequestBody, WalletTransferFundsResponseBody},
        },
    },
    paths,
    paths::{MAX_BLOCKS_STREAM_BLOCKS, MAX_BLOCKS_STREAM_CHUNK_SIZE},
};
use lb_libp2p::libp2p::bytes::Bytes;
use lb_network_service::backends::libp2p::Libp2p as Libp2pNetworkBackend;
use lb_sdp_service::{mempool::SdpMempoolAdapter, wallet::SdpWalletAdapter};
use lb_storage_service::{
    StorageService, api::chain::StorageChainApi, backends::rocksdb::RocksBackend,
};
use lb_tx_service::{
    TxMempoolService, backend::Mempool,
    network::adapters::libp2p::Libp2pAdapter as MempoolNetworkAdapter,
};
use lb_utils::non_zero;
use lb_wallet_service::api::{WalletApi, WalletServiceData};
use overwatch::{
    overwatch::handle::OverwatchHandle,
    services::{AsServiceId, ServiceData},
};
use serde::{Deserialize, Serialize};

use crate::api::{
    openapi::schema,
    queries::{BlockRangeQuery, BlocksStreamQuery},
    responses::{self, overwatch::get_relay_or_500},
    serializers::{
        blocks::{ApiBlock, ApiProcessedBlockEvent},
        transactions::ApiSignedTransactionRef,
    },
};

/// This is a safe default number of blocks to present the canonical chain
/// at the tip but not too much to overburden a client.
const DEFAULT_NUMBER_OF_BLOCKS_TO_STREAM: usize = 100;
const DEFAULT_BLOCKS_STREAM_CHUNK_SIZE: usize = 100;

struct BlocksStreamRequest {
    number_of_blocks: NonZero<usize>,
    blocks_to: Option<HeaderId>,
    server_batch_size: NonZero<usize>,
    immutable_only: bool,
}

fn parse_blocks_stream_request(query: &BlocksStreamQuery) -> Result<BlocksStreamRequest, String> {
    let number_of_blocks_raw = query
        .number_of_blocks
        .unwrap_or(DEFAULT_NUMBER_OF_BLOCKS_TO_STREAM);

    if number_of_blocks_raw > MAX_BLOCKS_STREAM_BLOCKS {
        return Err(format!(
            "'number_of_blocks' must be <= {MAX_BLOCKS_STREAM_BLOCKS}, got {number_of_blocks_raw}"
        ));
    }

    let number_of_blocks = non_zero!("number_of_blocks", number_of_blocks_raw)?;

    let server_batch_size_raw = query
        .server_batch_size
        .unwrap_or(DEFAULT_BLOCKS_STREAM_CHUNK_SIZE);

    if server_batch_size_raw > MAX_BLOCKS_STREAM_CHUNK_SIZE {
        return Err(format!(
            "'server_batch_size' must be <= {MAX_BLOCKS_STREAM_CHUNK_SIZE}, got {server_batch_size_raw}"
        ));
    }

    let server_batch_size = non_zero!("server_batch_size", server_batch_size_raw)?;

    Ok(BlocksStreamRequest {
        number_of_blocks,
        blocks_to: query.blocks_to,
        server_batch_size,
        immutable_only: query.immutable_only.unwrap_or_default(),
    })
}

async fn fetch_blocks_stream_chunk<StorageBackend, RuntimeServiceId>(
    handle: &OverwatchHandle<RuntimeServiceId>,
    chain_info: &lb_chain_service::CryptarchiaInfo,
    from: usize,
    to: usize,
    immutable_only: bool,
) -> Result<Vec<ApiProcessedBlockEvent>, DynError>
where
    StorageBackend: lb_storage_service::backends::StorageBackend + Send + Sync + 'static,
    StorageBackend::Block: Serialize,
    <StorageBackend as StorageChainApi>::Block:
        TryFrom<Block<SignedMantleTx>> + TryInto<Block<SignedMantleTx>>,
    <StorageBackend as StorageChainApi>::Tx: From<Bytes> + AsRef<[u8]>,
    RuntimeServiceId: Debug
        + Send
        + Sync
        + Display
        + 'static
        + AsServiceId<Cryptarchia<RuntimeServiceId>>
        + AsServiceId<StorageService<StorageBackend, RuntimeServiceId>>,
{
    let chunk = mantle::get_blocks_in_range_with_snapshot::<_, _, RuntimeServiceId>(
        handle,
        from,
        to,
        immutable_only,
        chain_info,
    )
    .await?;

    Ok(chunk
        .into_iter()
        .map(ApiProcessedBlockEvent::from)
        .collect())
}

fn build_blocks_stream<StorageBackend, RuntimeServiceId>(
    handle: OverwatchHandle<RuntimeServiceId>,
    chain_info: lb_chain_service::CryptarchiaInfo,
    first_chunk: Vec<ApiProcessedBlockEvent>,
    next_from: usize,
    blocks_to: usize,
    chunk_size: usize,
    immutable_only: bool,
) -> impl futures::Stream<Item = ApiProcessedBlockEvent>
where
    StorageBackend: lb_storage_service::backends::StorageBackend + Send + Sync + 'static,
    StorageBackend::Block: Serialize,
    <StorageBackend as StorageChainApi>::Block:
        TryFrom<Block<SignedMantleTx>> + TryInto<Block<SignedMantleTx>>,
    <StorageBackend as StorageChainApi>::Tx: From<Bytes> + AsRef<[u8]>,
    RuntimeServiceId: Debug
        + Send
        + Sync
        + Display
        + 'static
        + AsServiceId<Cryptarchia<RuntimeServiceId>>
        + AsServiceId<StorageService<StorageBackend, RuntimeServiceId>>,
{
    futures::stream::unfold(
        (
            first_chunk.into_iter(),
            next_from,
            blocks_to,
            chunk_size,
            immutable_only,
            chain_info,
            handle,
        ),
        async |(
            mut buffered,
            mut next_from,
            blocks_to,
            chunk_size,
            immutable_only,
            chain_info,
            handle,
        )| {
            loop {
                if let Some(item) = buffered.next() {
                    return Some((
                        item,
                        (
                            buffered,
                            next_from,
                            blocks_to,
                            chunk_size,
                            immutable_only,
                            chain_info,
                            handle,
                        ),
                    ));
                }

                if next_from > blocks_to {
                    return None;
                }

                let chunk_to =
                    blocks_to.min(next_from.saturating_add(chunk_size.saturating_sub(1)));
                let Ok(next_chunk) = fetch_blocks_stream_chunk::<StorageBackend, RuntimeServiceId>(
                    &handle,
                    &chain_info,
                    next_from,
                    chunk_to,
                    immutable_only,
                )
                .await
                else {
                    return None;
                };

                next_from = chunk_to.saturating_add(1);
                buffered = next_chunk.into_iter();
            }
        },
    )
}

#[macro_export]
macro_rules! make_request_and_return_response {
    ($cond:expr) => {{
        match $cond.await {
            ::std::result::Result::Ok(val) => ::axum::response::IntoResponse::into_response((
                ::axum::http::StatusCode::OK,
                ::axum::Json(val),
            )),
            ::std::result::Result::Err(e) => ::axum::response::IntoResponse::into_response((
                ::axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                e.to_string(),
            )),
        }
    }};
}

#[utoipa::path(
    get,
    path = paths::MANTLE_METRICS,
    responses(
        (status = 200, description = "Get the mempool metrics of the cl service", body = inline(schema::MempoolMetrics)),
        (status = 500, description = "Internal server error", body = String),
    )
)]
pub async fn mantle_metrics<StorageAdapter, RuntimeServiceId>(
    State(handle): State<OverwatchHandle<RuntimeServiceId>>,
) -> Response
where
    StorageAdapter: lb_tx_service::storage::MempoolStorageAdapter<
            RuntimeServiceId,
            Item = SignedMantleTx,
            Key = <SignedMantleTx as Transaction>::Hash,
        > + Send
        + Sync
        + Clone
        + 'static,
    StorageAdapter::Error: Debug,
    RuntimeServiceId: Debug
        + Send
        + Sync
        + Display
        + 'static
        + AsServiceId<
            TxMempoolService<
                MempoolNetworkAdapter<
                    SignedMantleTx,
                    <SignedMantleTx as Transaction>::Hash,
                    RuntimeServiceId,
                >,
                Mempool<
                    HeaderId,
                    SignedMantleTx,
                    <SignedMantleTx as Transaction>::Hash,
                    StorageAdapter,
                    RuntimeServiceId,
                >,
                StorageAdapter,
                RuntimeServiceId,
            >,
        >,
{
    make_request_and_return_response!(mantle::mantle_mempool_metrics::<
        StorageAdapter,
        RuntimeServiceId,
    >(&handle))
}

#[utoipa::path(
    post,
    path = paths::MANTLE_STATUS,
    responses(
        (status = 200, description = "Query the mempool status of the cl service", body = Vec<<T as Transaction>::Hash>),
        (status = 500, description = "Internal server error", body = String),
    )
)]
pub async fn mantle_status<StorageAdapter, RuntimeServiceId>(
    State(handle): State<OverwatchHandle<RuntimeServiceId>>,
    Json(items): Json<Vec<<SignedMantleTx as Transaction>::Hash>>,
) -> Response
where
    StorageAdapter: lb_tx_service::storage::MempoolStorageAdapter<
            RuntimeServiceId,
            Item = SignedMantleTx,
            Key = <SignedMantleTx as Transaction>::Hash,
        > + Send
        + Sync
        + Clone
        + 'static,
    StorageAdapter::Error: Debug,
    RuntimeServiceId: Debug
        + Send
        + Sync
        + Display
        + 'static
        + AsServiceId<
            TxMempoolService<
                MempoolNetworkAdapter<
                    SignedMantleTx,
                    <SignedMantleTx as Transaction>::Hash,
                    RuntimeServiceId,
                >,
                Mempool<
                    HeaderId,
                    SignedMantleTx,
                    <SignedMantleTx as Transaction>::Hash,
                    StorageAdapter,
                    RuntimeServiceId,
                >,
                StorageAdapter,
                RuntimeServiceId,
            >,
        >,
{
    make_request_and_return_response!(mantle::mantle_mempool_status::<
        StorageAdapter,
        RuntimeServiceId,
    >(&handle, items))
}

#[derive(Deserialize)]
pub struct CryptarchiaInfoQuery {
    from: Option<HeaderId>,
    to: Option<HeaderId>,
}

#[utoipa::path(
    get,
    path = paths::CRYPTARCHIA_INFO,
    responses(
        (status = 200, description = "Query consensus information", body = lb_consensus::CryptarchiaInfo),
        (status = 500, description = "Internal server error", body = String),
    )
)]
pub async fn cryptarchia_info<RuntimeServiceId>(
    State(handle): State<OverwatchHandle<RuntimeServiceId>>,
) -> Response
where
    RuntimeServiceId:
        Debug + Send + Sync + Display + 'static + AsServiceId<Cryptarchia<RuntimeServiceId>>,
{
    make_request_and_return_response!(consensus::cryptarchia_info::<RuntimeServiceId>(&handle))
}

#[utoipa::path(
    get,
    path = paths::CRYPTARCHIA_HEADERS,
    responses(
        (status = 200, description = "Query header ids", body = Vec<HeaderId>),
        (status = 500, description = "Internal server error", body = String),
    )
)]
pub async fn cryptarchia_headers<RuntimeServiceId>(
    State(handle): State<OverwatchHandle<RuntimeServiceId>>,
    Query(query): Query<CryptarchiaInfoQuery>,
) -> Response
where
    RuntimeServiceId:
        Debug + Send + Sync + Display + 'static + AsServiceId<Cryptarchia<RuntimeServiceId>>,
{
    let CryptarchiaInfoQuery { from, to } = query;
    make_request_and_return_response!(consensus::cryptarchia_headers::<RuntimeServiceId>(
        &handle, from, to
    ))
}

#[utoipa::path(
    get,
    path = paths::CRYPTARCHIA_LIB_STREAM,
    responses(
        (status = 200, description = "Request a stream for lib blocks"),
        (status = 500, description = "Internal server error", body = StreamBody),
    )
)]
pub async fn cryptarchia_lib_stream<RuntimeServiceId>(
    State(handle): State<OverwatchHandle<RuntimeServiceId>>,
) -> Response
where
    RuntimeServiceId:
        Debug + Sync + Display + AsServiceId<BlockBroadcastService<RuntimeServiceId>> + 'static,
{
    let stream = mantle::lib_block_stream(&handle).await;
    match stream {
        Ok(stream) => responses::ndjson::from_stream_result(stream),
        Err(error) => (StatusCode::INTERNAL_SERVER_ERROR, error.to_string()).into_response(),
    }
}

#[utoipa::path(
    get,
    path = paths::NETWORK_INFO,
    responses(
        (status = 200, description = "Query the network information", body = lb_network_service::backends::libp2p::Libp2pInfo),
        (status = 500, description = "Internal server error", body = String),
    )
)]
pub async fn libp2p_info<RuntimeServiceId>(
    State(handle): State<OverwatchHandle<RuntimeServiceId>>,
) -> Response
where
    RuntimeServiceId: Debug
        + Sync
        + Display
        + 'static
        + AsServiceId<
            lb_network_service::NetworkService<
                lb_network_service::backends::libp2p::Libp2p,
                RuntimeServiceId,
            >,
        >,
{
    make_request_and_return_response!(libp2p::libp2p_info::<RuntimeServiceId>(&handle))
}

#[utoipa::path(
    get,
    path = paths::BLEND_NETWORK_INFO,
    responses(
        (status = 200, description = "Query the blend network information", body = Option<lb_blend_service::message::BlendNetworkInfo>),
        (status = 500, description = "Internal server error", body = String),
    )
)]
pub async fn blend_info<BlendService, BroadcastSettings, RuntimeServiceId>(
    State(handle): State<OverwatchHandle<RuntimeServiceId>>,
) -> Response
where
    BlendService: ServiceData<Message = lb_blend_service::message::ServiceMessage<BroadcastSettings, PeerId>>
        + 'static,
    BroadcastSettings: Send + 'static,
    RuntimeServiceId: Debug + Sync + Display + 'static + AsServiceId<BlendService>,
{
    make_request_and_return_response!(blend::blend_info::<
        BlendService,
        BroadcastSettings,
        RuntimeServiceId,
    >(&handle))
}

#[utoipa::path(
    post,
    path = paths::MEMPOOL_ADD_TX,
    responses(
        (status = 200, description = "Add transaction to the mempool"),
        (status = 500, description = "Internal server error", body = String),
    )
)]
pub async fn add_tx<StorageAdapter, RuntimeServiceId>(
    State(handle): State<OverwatchHandle<RuntimeServiceId>>,
    Json(tx): Json<SignedMantleTx>,
) -> Response
where
    StorageAdapter: lb_tx_service::storage::MempoolStorageAdapter<
            RuntimeServiceId,
            Item = SignedMantleTx,
            Key = <SignedMantleTx as Transaction>::Hash,
        > + Send
        + Sync
        + Clone
        + 'static,
    StorageAdapter::Error: Debug,
    RuntimeServiceId: Debug
        + Sync
        + Send
        + Display
        + 'static
        + AsServiceId<
            TxMempoolService<
                MempoolNetworkAdapter<
                    SignedMantleTx,
                    <SignedMantleTx as Transaction>::Hash,
                    RuntimeServiceId,
                >,
                Mempool<
                    HeaderId,
                    SignedMantleTx,
                    <SignedMantleTx as Transaction>::Hash,
                    StorageAdapter,
                    RuntimeServiceId,
                >,
                StorageAdapter,
                RuntimeServiceId,
            >,
        >,
{
    make_request_and_return_response!(mempool::add_tx::<
        Libp2pNetworkBackend,
        MempoolNetworkAdapter<
            SignedMantleTx,
            <SignedMantleTx as Transaction>::Hash,
            RuntimeServiceId,
        >,
        StorageAdapter,
        SignedMantleTx,
        <SignedMantleTx as Transaction>::Hash,
        RuntimeServiceId,
    >(&handle, tx, Transaction::hash))
}

#[utoipa::path(
    get,
    path = paths::CHANNEL,
    responses(
        (status = 200, description = "Channel state"),
        (status = 500, description = "Internal server error", body = String),
    )
)]
pub async fn channel<RuntimeServiceId>(
    State(handle): State<OverwatchHandle<RuntimeServiceId>>,
    Path(id): Path<ChannelId>,
) -> Response
where
    RuntimeServiceId:
        Debug + Send + Sync + Display + 'static + AsServiceId<Cryptarchia<RuntimeServiceId>>,
{
    make_request_and_return_response!(mantle::channel::<RuntimeServiceId>(&handle, id))
}

#[utoipa::path(
    post,
    path = paths::CHANNEL_DEPOSIT,
    responses(
        (status = 200, description = "Submit a channel deposit"),
        (status = 500, description = "Internal server error", body = String),
    )
)]
pub async fn channel_deposit<WalletService, StorageAdapter, RuntimeServiceId>(
    State(handle): State<OverwatchHandle<RuntimeServiceId>>,
    Json(req): Json<ChannelDepositRequestBody>,
) -> Response
where
    WalletService: WalletServiceData,
    StorageAdapter: lb_tx_service::storage::MempoolStorageAdapter<
            RuntimeServiceId,
            Item = SignedMantleTx,
            Key = <SignedMantleTx as Transaction>::Hash,
        > + Send
        + Sync
        + Clone
        + 'static,
    StorageAdapter::Error: Debug,
    RuntimeServiceId: Debug
        + Display
        + Send
        + Sync
        + 'static
        + AsServiceId<WalletService>
        + AsServiceId<
            TxMempoolService<
                MempoolNetworkAdapter<
                    SignedMantleTx,
                    <SignedMantleTx as Transaction>::Hash,
                    RuntimeServiceId,
                >,
                Mempool<
                    HeaderId,
                    SignedMantleTx,
                    <SignedMantleTx as Transaction>::Hash,
                    StorageAdapter,
                    RuntimeServiceId,
                >,
                StorageAdapter,
                RuntimeServiceId,
            >,
        >,
{
    make_request_and_return_response!(async {
        let wallet = WalletApi::<WalletService, RuntimeServiceId>::new(
            handle.relay::<WalletService>().await?,
        );

        let tx_context = wallet.get_tx_context(None).await?;
        let tx_builder = MantleTxBuilder::new(tx_context).push_op(Op::ChannelDeposit(req.deposit));
        let lb_wallet_service::TipResponse {
            tip,
            response: funded_tx_builder,
        } = wallet
            .fund_tx(
                None,
                tx_builder,
                req.change_public_key,
                req.funding_public_keys,
            )
            .await?;

        let tx_fee = funded_tx_builder.gas_cost::<MainnetGasConstants>()?;
        if tx_fee > req.max_tx_fee {
            return Err(overwatch::DynError::from(format!(
                "tx_fee({tx_fee}) exceeds max_tx_fee({})",
                req.max_tx_fee
            )));
        }

        let signed_tx = wallet.sign_tx(Some(tip), funded_tx_builder).await?.response;
        let tx_hash = signed_tx.hash();

        mempool::add_tx::<
            Libp2pNetworkBackend,
            MempoolNetworkAdapter<
                SignedMantleTx,
                <SignedMantleTx as Transaction>::Hash,
                RuntimeServiceId,
            >,
            StorageAdapter,
            SignedMantleTx,
            <SignedMantleTx as Transaction>::Hash,
            RuntimeServiceId,
        >(&handle, signed_tx, Transaction::hash)
        .await?;

        Ok(ChannelDepositResponseBody { hash: tx_hash })
    })
}

#[utoipa::path(
    post,
    path = paths::SDP_POST_DECLARATION,
    responses(
        (status = 200, description = "Post declaration to SDP service", body = lb_core::sdp::DeclarationId),
        (status = 500, description = "Internal server error", body = String),
    )
)]
pub async fn post_declaration<MempoolAdapter, WalletAdapter, ChainService, RuntimeServiceId>(
    State(handle): State<OverwatchHandle<RuntimeServiceId>>,
    Json(declaration): Json<lb_core::sdp::DeclarationMessage>,
) -> Response
where
    MempoolAdapter: SdpMempoolAdapter + Send + Sync + 'static,
    WalletAdapter: SdpWalletAdapter + Send + Sync + 'static,
    ChainService: lb_chain_service::api::CryptarchiaServiceData + Send + Sync + 'static,
    RuntimeServiceId: Debug
        + Sync
        + Send
        + Display
        + 'static
        + AsServiceId<ChainService>
        + AsServiceId<
            lb_sdp_service::SdpService<
                MempoolAdapter,
                WalletAdapter,
                ChainService,
                RuntimeServiceId,
            >,
        >,
{
    make_request_and_return_response!(lb_api_service::http::sdp::post_declaration_handler::<
        MempoolAdapter,
        WalletAdapter,
        ChainService,
        RuntimeServiceId,
    >(handle, declaration))
}

#[utoipa::path(
    post,
    path = paths::SDP_POST_ACTIVITY,
    responses(
        (status = 200, description = "Post activity to SDP service"),
        (status = 500, description = "Internal server error", body = String),
    )
)]
pub async fn post_activity<MempoolAdapter, WalletAdapter, ChainService, RuntimeServiceId>(
    State(handle): State<OverwatchHandle<RuntimeServiceId>>,
    Json(metadata): Json<lb_core::sdp::ActivityMetadata>,
) -> Response
where
    MempoolAdapter: SdpMempoolAdapter + Send + Sync + 'static,
    WalletAdapter: SdpWalletAdapter + Send + Sync + 'static,
    ChainService: lb_chain_service::api::CryptarchiaServiceData + Send + Sync + 'static,
    RuntimeServiceId: Debug
        + Sync
        + Send
        + Display
        + 'static
        + AsServiceId<ChainService>
        + AsServiceId<
            lb_sdp_service::SdpService<
                MempoolAdapter,
                WalletAdapter,
                ChainService,
                RuntimeServiceId,
            >,
        >,
{
    make_request_and_return_response!(lb_api_service::http::sdp::post_activity_handler::<
        MempoolAdapter,
        WalletAdapter,
        ChainService,
        RuntimeServiceId,
    >(handle, metadata))
}

#[utoipa::path(
    post,
    path = paths::SDP_POST_WITHDRAWAL,
    responses(
        (status = 200, description = "Post withdrawal to SDP service"),
        (status = 500, description = "Internal server error", body = String),
    )
)]
pub async fn post_withdrawal<MempoolAdapter, WalletAdapter, ChainService, RuntimeServiceId>(
    State(handle): State<OverwatchHandle<RuntimeServiceId>>,
    Json(declaration_id): Json<lb_core::sdp::DeclarationId>,
) -> Response
where
    MempoolAdapter: SdpMempoolAdapter + Send + Sync + 'static,
    WalletAdapter: SdpWalletAdapter + Send + Sync + 'static,
    ChainService: lb_chain_service::api::CryptarchiaServiceData + Send + Sync + 'static,
    RuntimeServiceId: Debug
        + Sync
        + Send
        + Display
        + 'static
        + AsServiceId<ChainService>
        + AsServiceId<
            lb_sdp_service::SdpService<
                MempoolAdapter,
                WalletAdapter,
                ChainService,
                RuntimeServiceId,
            >,
        >,
{
    make_request_and_return_response!(lb_api_service::http::sdp::post_withdrawal_handler::<
        MempoolAdapter,
        WalletAdapter,
        ChainService,
        RuntimeServiceId,
    >(handle, declaration_id))
}

#[utoipa::path(
    post,
    path = paths::LEADER_CLAIM,
    responses(
        (status = 200, description = "Leader claim transaction submitted"),
        (status = 500, description = "Internal server error", body = String),
    )
)]
pub async fn leader_claim<ChainLeader, RuntimeServiceId>(
    State(handle): State<OverwatchHandle<RuntimeServiceId>>,
) -> Response
where
    ChainLeader: ChainLeaderServiceData,
    RuntimeServiceId: Debug + Send + Sync + Display + 'static + AsServiceId<ChainLeader>,
{
    make_request_and_return_response!(consensus::leader::claim(&handle))
}

#[utoipa::path(
    get,
    path = paths::IMMUTABLE_BLOCKS,
    params(BlockRangeQuery),
    responses(
        (status = 200, description = "Get blocks"),
        (status = 500, description = "Internal server error", body = String),
    )
)]
pub async fn immutable_blocks<StorageBackend, RuntimeServiceId>(
    State(handle): State<OverwatchHandle<RuntimeServiceId>>,
    Query(query): Query<BlockRangeQuery>,
) -> Response
where
    StorageBackend: lb_storage_service::backends::StorageBackend + Send + Sync + 'static, /* TODO: StorageChainApi */
    StorageBackend::Block: Serialize,
    <StorageBackend as StorageChainApi>::Block:
        TryFrom<Block<SignedMantleTx>> + TryInto<Block<SignedMantleTx>>,
    <StorageBackend as StorageChainApi>::Tx: From<Bytes> + AsRef<[u8]>,
    RuntimeServiceId: Debug
        + Sync
        + Display
        + 'static
        + AsServiceId<StorageService<StorageBackend, RuntimeServiceId>>,
{
    let api_blocks =
        mantle::get_immutable_blocks(&handle, query.slot_from, query.slot_to).map(|blocks| {
            let api_blocks = blocks?.into_iter().map(ApiBlock::from).collect::<Vec<_>>();
            Ok::<Vec<ApiBlock>, DynError>(api_blocks)
        });
    make_request_and_return_response!(api_blocks)
}

#[utoipa::path(
    post,
    path = paths::BLOCKS_DETAIL,
    responses(
        (status = 200, description = "Block found"),
        (status = 404, description = "Block not found"),
        (status = 500, description = "Internal server error", body = String),
    )
)]
pub async fn block<HttpStorageAdapter, RuntimeServiceId>(
    State(handle): State<OverwatchHandle<RuntimeServiceId>>,
    Path(id): Path<HeaderId>,
) -> Response
where
    HttpStorageAdapter: StorageAdapter<RuntimeServiceId> + Send + Sync + 'static,
    RuntimeServiceId:
        AsServiceId<StorageService<RocksBackend, RuntimeServiceId>> + Debug + Sync + Display,
{
    let relay = match get_relay_or_500(&handle).await {
        Ok(relay) => relay,
        Err(error_response) => return error_response,
    };
    let block = HttpStorageAdapter::get_block::<SignedMantleTx>(relay, id).await;
    match block {
        Ok(Some(block)) => {
            let api_block = ApiBlock::from(block);
            (StatusCode::OK, Json(api_block)).into_response()
        }
        Ok(None) => (StatusCode::NOT_FOUND,).into_response(),
        Err(_) => (StatusCode::INTERNAL_SERVER_ERROR, "Internal server error").into_response(),
    }
}

#[utoipa::path(
    get,
    path = paths::BLOCKS_STREAM,
    params(BlocksStreamQuery),
    responses(
        (status = 200, description = "Stream of processed blocks with chain state"),
        (status = 400, description = "Invalid request parameters", body = String),
        (status = 404, description = "Requested header not found on canonical chain", body = String),
        (status = 500, description = "Internal server error", body = String),
    )
)]
pub async fn blocks_stream<StorageBackend, RuntimeServiceId>(
    State(handle): State<OverwatchHandle<RuntimeServiceId>>,
    Query(query): Query<BlocksStreamQuery>,
) -> Response
where
    StorageBackend: lb_storage_service::backends::StorageBackend + Send + Sync + 'static,
    StorageBackend::Block: Serialize,
    <StorageBackend as StorageChainApi>::Block:
        TryFrom<Block<SignedMantleTx>> + TryInto<Block<SignedMantleTx>>,
    <StorageBackend as StorageChainApi>::Tx: From<Bytes> + AsRef<[u8]>,
    RuntimeServiceId: Debug
        + Send
        + Sync
        + Display
        + 'static
        + AsServiceId<Cryptarchia<RuntimeServiceId>>
        + AsServiceId<StorageService<StorageBackend, RuntimeServiceId>>,
{
    let request = match parse_blocks_stream_request(&query) {
        Ok(request) => request,
        Err(message) => return (StatusCode::BAD_REQUEST, message).into_response(),
    };

    let chain_info = match consensus::cryptarchia_info::<RuntimeServiceId>(&handle).await {
        Ok(info) => info,
        Err(error) => {
            return (StatusCode::INTERNAL_SERVER_ERROR, error.to_string()).into_response();
        }
    };

    let blocks_to_header = request.blocks_to.unwrap_or(chain_info.tip);
    let blocks_to_height = match mantle::find_canonical_height_by_header_with_snapshot::<
        _,
        _,
        RuntimeServiceId,
    >(&handle, blocks_to_header, &chain_info)
    .await
    {
        Ok(Some(height)) => height,
        Ok(None) => {
            if request.blocks_to.is_some() {
                return (
                    StatusCode::NOT_FOUND,
                    format!("Header '{blocks_to_header}' not found in canonical chain"),
                )
                    .into_response();
            }
            let empty = futures::stream::empty::<ApiProcessedBlockEvent>();
            return responses::ndjson::from_stream(empty);
        }
        Err(error) => {
            return (StatusCode::INTERNAL_SERVER_ERROR, error.to_string()).into_response();
        }
    };

    let blocks_from = blocks_to_height
        .saturating_sub(request.number_of_blocks.get().saturating_sub(1))
        .max(1);

    let first_chunk_to = blocks_to_height
        .min(blocks_from.saturating_add(request.server_batch_size.get().saturating_sub(1)));

    let first_chunk = match fetch_blocks_stream_chunk::<StorageBackend, RuntimeServiceId>(
        &handle,
        &chain_info,
        blocks_from,
        first_chunk_to,
        request.immutable_only,
    )
    .await
    {
        Ok(events) => events,
        Err(error) => {
            return (StatusCode::INTERNAL_SERVER_ERROR, error.to_string()).into_response();
        }
    };

    let stream = build_blocks_stream::<StorageBackend, RuntimeServiceId>(
        handle,
        chain_info,
        first_chunk,
        first_chunk_to.saturating_add(1),
        blocks_to_height,
        request.server_batch_size.get(),
        request.immutable_only,
    );

    responses::ndjson::from_stream(stream)
}

#[utoipa::path(
    post,
    path = paths::TRANSACTION,
    responses(
        (status = 200, description = "Transaction found"),
        (status = 404, description = "Transaction not found"),
        (status = 500, description = "Internal server error", body = String),
    )
)]
pub async fn transaction<HttpStorageAdapter, RuntimeServiceId>(
    State(handle): State<OverwatchHandle<RuntimeServiceId>>,
    Path(id): Path<TxHash>,
) -> Response
where
    HttpStorageAdapter: StorageAdapter<RuntimeServiceId> + Send + Sync + 'static,
    RuntimeServiceId:
        AsServiceId<StorageService<RocksBackend, RuntimeServiceId>> + Debug + Sync + Display,
{
    let relay = match get_relay_or_500(&handle).await {
        Ok(relay) => relay,
        Err(error_response) => return error_response,
    };
    let Ok(transactions) = HttpStorageAdapter::get_transactions::<SignedMantleTx>(relay, id).await
    else {
        return (StatusCode::INTERNAL_SERVER_ERROR, "Internal server error").into_response();
    };
    match transactions.as_slice() {
        [] => (StatusCode::NOT_FOUND,).into_response(),
        [transaction] => {
            let api_transaction = ApiSignedTransactionRef::from(transaction);
            (StatusCode::OK, Json(api_transaction)).into_response()
        }
        _ => {
            let error_body = serde_json::json!({
                "error": "Multiple transactions found",
                "len": transactions.len()
            });
            (StatusCode::INTERNAL_SERVER_ERROR, Json(error_body)).into_response()
        }
    }
}

pub mod wallet {
    use lb_http_api_common::bodies::wallet::sign::{
        WalletSignTxEd25519RequestBody, WalletSignTxEd25519ResponseBody, WalletSignTxZkRequestBody,
        WalletSignTxZkResponseBody,
    };
    use lb_key_management_system_service::keys::ZkPublicKey;

    use super::*;

    #[derive(Deserialize)]
    pub struct TipQuery {
        tip: Option<HeaderId>,
    }

    #[utoipa::path(
    get,
    path = paths::wallet::BALANCE,
    responses(
        (status = 200, description = "Get wallet balance"),
        (status = 500, description = "Internal server error", body = String),
    )
    )]
    pub async fn get_balance<WalletService, RuntimeServiceId>(
        State(handle): State<OverwatchHandle<RuntimeServiceId>>,
        Path(address): Path<ZkPublicKey>,
        Query(query): Query<TipQuery>,
    ) -> Response
    where
        WalletService: WalletServiceData + 'static,
        RuntimeServiceId: Debug + Send + Sync + Display + 'static + AsServiceId<WalletService>,
    {
        let wallet_api = {
            let wallet_relay = match get_relay_or_500::<WalletService, _>(&handle).await {
                Ok(relay) => relay,
                Err(error_response) => return error_response,
            };
            WalletApi::<WalletService, RuntimeServiceId>::new(wallet_relay)
        };

        let balance = wallet_api.get_balance(query.tip, address).await;
        match balance {
            Ok(lb_wallet_service::TipResponse {
                tip,
                response: Some(balance),
            }) => WalletBalanceResponseBody {
                tip,
                balance: balance.balance,
                notes: balance.notes,
                address,
            }
            .into_response(),
            Ok(lb_wallet_service::TipResponse { response: None, .. }) => (
                StatusCode::NOT_FOUND,
                "The requested address could not be found in the wallet",
            )
                .into_response(),
            Err(error) => (StatusCode::INTERNAL_SERVER_ERROR, error.to_string()).into_response(),
        }
    }

    #[utoipa::path(
    post,
    path = paths::wallet::TRANSACTIONS_TRANSFER_FUNDS,
    responses(
        (status = 200, description = "Make transfer"),
        (status = 500, description = "Internal server error", body = String),
    )
    )]
    pub async fn post_transactions_transfer_funds<WalletService, StorageAdapter, RuntimeServiceId>(
        State(handle): State<OverwatchHandle<RuntimeServiceId>>,
        Json(body): Json<WalletTransferFundsRequestBody>,
    ) -> Response
    where
        WalletService: WalletServiceData + 'static,
        StorageAdapter: lb_tx_service::storage::MempoolStorageAdapter<
                RuntimeServiceId,
                Item = SignedMantleTx,
                Key = <SignedMantleTx as Transaction>::Hash,
            > + Send
            + Sync
            + Clone
            + 'static,
        StorageAdapter::Error: Debug,
        RuntimeServiceId: Debug
            + Send
            + Sync
            + Display
            + 'static
            + AsServiceId<WalletService>
            + AsServiceId<
                TxMempoolService<
                    MempoolNetworkAdapter<
                        SignedMantleTx,
                        <SignedMantleTx as Transaction>::Hash,
                        RuntimeServiceId,
                    >,
                    Mempool<
                        HeaderId,
                        SignedMantleTx,
                        <SignedMantleTx as Transaction>::Hash,
                        StorageAdapter,
                        RuntimeServiceId,
                    >,
                    StorageAdapter,
                    RuntimeServiceId,
                >,
            >,
    {
        let wallet_api = {
            let wallet_relay = match get_relay_or_500::<WalletService, _>(&handle).await {
                Ok(relay) => relay,
                Err(error_response) => return error_response,
            };
            WalletApi::<WalletService, RuntimeServiceId>::new(wallet_relay)
        };

        let transfer_funds = wallet_api
            .transfer_funds(
                body.tip,
                body.change_public_key,
                body.funding_public_keys,
                body.recipient_public_key,
                body.amount,
            )
            .await;

        match transfer_funds {
            Ok(lb_wallet_service::TipResponse {
                response: transaction,
                ..
            }) => {
                // Submit to mempool
                if let Err(e) = mempool::add_tx::<
                    Libp2pNetworkBackend,
                    MempoolNetworkAdapter<
                        SignedMantleTx,
                        <SignedMantleTx as Transaction>::Hash,
                        RuntimeServiceId,
                    >,
                    StorageAdapter,
                    SignedMantleTx,
                    <SignedMantleTx as Transaction>::Hash,
                    RuntimeServiceId,
                >(&handle, transaction.clone(), Transaction::hash)
                .await
                {
                    return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response();
                }

                WalletTransferFundsResponseBody::from(transaction).into_response()
            }
            Err(error) => (StatusCode::INTERNAL_SERVER_ERROR, error.to_string()).into_response(),
        }
    }

    #[utoipa::path(
        post,
        path = paths::wallet::SIGN_TX_ED25519,
        responses(
            (status = 200, description = "Signed transaction"),
            (status = 500, description = "Internal server error", body = String),
        )
    )]
    pub async fn sign_tx_ed25519<WalletService, StorageAdapter, RuntimeServiceId>(
        State(handle): State<OverwatchHandle<RuntimeServiceId>>,
        Json(req): Json<WalletSignTxEd25519RequestBody>,
    ) -> Response
    where
        WalletService: WalletServiceData,
        StorageAdapter: lb_tx_service::storage::MempoolStorageAdapter<
                RuntimeServiceId,
                Item = SignedMantleTx,
                Key = <SignedMantleTx as Transaction>::Hash,
            > + Send
            + Sync
            + Clone
            + 'static,
        StorageAdapter::Error: Debug,
        RuntimeServiceId: Debug
            + Display
            + Send
            + Sync
            + 'static
            + AsServiceId<WalletService>
            + AsServiceId<
                TxMempoolService<
                    MempoolNetworkAdapter<
                        SignedMantleTx,
                        <SignedMantleTx as Transaction>::Hash,
                        RuntimeServiceId,
                    >,
                    Mempool<
                        HeaderId,
                        SignedMantleTx,
                        <SignedMantleTx as Transaction>::Hash,
                        StorageAdapter,
                        RuntimeServiceId,
                    >,
                    StorageAdapter,
                    RuntimeServiceId,
                >,
            >,
    {
        make_request_and_return_response!(async {
            let wallet = WalletApi::<WalletService, RuntimeServiceId>::new(
                handle.relay::<WalletService>().await?,
            );

            let sig = wallet.sign_tx_with_ed25519(req.tx_hash, req.pk).await?;
            Ok::<_, DynError>(WalletSignTxEd25519ResponseBody { sig })
        })
    }

    #[utoipa::path(
        post,
        path = paths::wallet::SIGN_TX_ZK,
        responses(
            (status = 200, description = "Signed transaction"),
            (status = 500, description = "Internal server error", body = String),
        )
    )]
    pub async fn sign_tx_zk<WalletService, StorageAdapter, RuntimeServiceId>(
        State(handle): State<OverwatchHandle<RuntimeServiceId>>,
        Json(req): Json<WalletSignTxZkRequestBody>,
    ) -> Response
    where
        WalletService: WalletServiceData,
        StorageAdapter: lb_tx_service::storage::MempoolStorageAdapter<
                RuntimeServiceId,
                Item = SignedMantleTx,
                Key = <SignedMantleTx as Transaction>::Hash,
            > + Send
            + Sync
            + Clone
            + 'static,
        StorageAdapter::Error: Debug,
        RuntimeServiceId: Debug
            + Display
            + Send
            + Sync
            + 'static
            + AsServiceId<WalletService>
            + AsServiceId<
                TxMempoolService<
                    MempoolNetworkAdapter<
                        SignedMantleTx,
                        <SignedMantleTx as Transaction>::Hash,
                        RuntimeServiceId,
                    >,
                    Mempool<
                        HeaderId,
                        SignedMantleTx,
                        <SignedMantleTx as Transaction>::Hash,
                        StorageAdapter,
                        RuntimeServiceId,
                    >,
                    StorageAdapter,
                    RuntimeServiceId,
                >,
            >,
    {
        make_request_and_return_response!(async {
            let wallet = WalletApi::<WalletService, RuntimeServiceId>::new(
                handle.relay::<WalletService>().await?,
            );

            let sig = wallet.sign_tx_with_zk(req.tx_hash, req.pks).await?;
            Ok::<_, DynError>(WalletSignTxZkResponseBody { sig })
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_header_id(byte: u8) -> HeaderId {
        HeaderId::from([byte; 32])
    }

    #[test]
    fn blocks_stream_request_defaults_to_recent_blocks_from_tip() {
        let request = parse_blocks_stream_request(&BlocksStreamQuery {
            number_of_blocks: None,
            blocks_to: None,
            server_batch_size: None,
            immutable_only: None,
        })
        .expect("query without explicit range should parse");

        assert_eq!(
            request.number_of_blocks,
            NonZero::new(DEFAULT_NUMBER_OF_BLOCKS_TO_STREAM).unwrap()
        );
        assert_eq!(request.blocks_to, None);
        assert_eq!(
            request.server_batch_size,
            NonZero::new(MAX_BLOCKS_STREAM_CHUNK_SIZE).unwrap()
        );
        assert!(!request.immutable_only);
    }

    #[test]
    fn blocks_stream_request_uses_tip_when_only_number_of_blocks_is_given() {
        let request = parse_blocks_stream_request(&BlocksStreamQuery {
            number_of_blocks: Some(7),
            blocks_to: None,
            server_batch_size: None,
            immutable_only: None,
        })
        .expect("query with explicit number_of_blocks should parse");

        assert_eq!(request.number_of_blocks, NonZero::new(7).unwrap());
        assert_eq!(request.blocks_to, None);
    }

    #[test]
    fn blocks_stream_request_defaults_to_100_blocks_when_only_blocks_to_is_given() {
        let blocks_to = sample_header_id(7);
        let request = parse_blocks_stream_request(&BlocksStreamQuery {
            number_of_blocks: None,
            blocks_to: Some(blocks_to),
            server_batch_size: None,
            immutable_only: None,
        })
        .expect("query with explicit blocks_to should parse");

        assert_eq!(
            request.number_of_blocks,
            NonZero::new(DEFAULT_NUMBER_OF_BLOCKS_TO_STREAM).unwrap()
        );
        assert_eq!(request.blocks_to, Some(blocks_to));
    }

    #[test]
    fn blocks_stream_request_rejects_zero_blocks() {
        let result = parse_blocks_stream_request(&BlocksStreamQuery {
            number_of_blocks: Some(0),
            blocks_to: None,
            server_batch_size: None,
            immutable_only: None,
        });

        assert_eq!(
            result.err(),
            Some("number_of_blocks must be greater than or equal to 1".to_owned())
        );
    }
}
