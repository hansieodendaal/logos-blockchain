#![allow(clippy::needless_for_each, reason = "Utoipa implementation")]

use std::{
    fmt::{Debug, Display},
    marker::PhantomData,
};

use axum::{Router, routing};
use http::StatusCode;
use lb_api_service::{Backend, http::consensus::Cryptarchia};
use lb_chain_broadcast_service::BlockBroadcastService;
use lb_chain_leader_service::api::ChainLeaderServiceData;
use lb_chain_service::CryptarchiaConsensus;
use lb_core::{header::HeaderId, mantle::traits::Hashable};
use lb_http_api_common::metrics::http_metrics_middleware;
pub use lb_http_api_common::settings::AxumBackendSettings;
use lb_sdp_service::{
    mempool::SdpMempoolAdapter, state::SdpStateStorage as SdpStateStorageTrait,
    wallet::SdpWalletAdapter,
};
use lb_storage_service::StorageService;
use lb_time_service::backends::NtpTimeBackend;
use lb_tx_service::{TxMempoolService, backend::Mempool};
use overwatch::{overwatch::handle::OverwatchHandle, services::AsServiceId};
use tokio::net::TcpListener;
use tower::limit::ConcurrencyLimitLayer;
use tower_http::{
    limit::RequestBodyLimitLayer,
    timeout::TimeoutLayer,
    trace::{DefaultOnRequest, DefaultOnResponse, TraceLayer},
};
use tracing::Level as TracingLevel;
use utoipa::OpenApi as _;
use utoipa_swagger_ui::SwaggerUi;

use super::handlers::{
    add_tx, blend_info, blend_pending_transactions, blend_tx, block, block_events,
    blocks_range_stream, blocks_stream, chain_fork, chain_id, cryptarchia_headers,
    cryptarchia_info, cryptarchia_lib_stream, dial_peer, get_finalized_sdp_declaration,
    get_finalized_sdp_declarations, get_gas_prices, get_limits, get_sdp_declarations,
    get_sdp_snapshot, immutable_blocks, libp2p_info, mantle_metrics, mantle_status, mempool_view,
    time_info, transaction, version, wallet,
};
use crate::{
    BlendService, PoWService, TracingService, ValidatedTx, WalletService,
    api::{
        browser_access::restrict_browser_access,
        handlers::{
            blend_join_network, channel, channel_step, channel_withdraw, post_activity,
            post_declaration, post_set_declaration_id, post_withdrawal, pow_claim,
            pow_claimable_rewards, pow_start_auto_claim, pow_start_mining, pow_status,
            pow_stop_auto_claim, pow_stop_mining, prepare_channel_deposit,
        },
        openapi::ApiDoc,
        routes::api_routes,
        tracing::reload_tracing_filter,
    },
};

async fn wallet_readiness<RuntimeServiceId>(
    axum::extract::State(handle): axum::extract::State<OverwatchHandle<RuntimeServiceId>>,
    request: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response
where
    RuntimeServiceId: AsServiceId<WalletService> + Debug + Display + Send + Sync + 'static,
{
    use axum::response::IntoResponse as _;
    use lb_http_api_common::paths;
    use overwatch::services::status::ServiceStatus;

    let path = request.uri().path();
    if path.starts_with("/wallet/")
        || [
            paths::LEADER_AGED_NOTES,
            paths::CHANNEL_STEP,
            paths::CHANNEL_WITHDRAW,
            paths::CHANNEL_PREPARE_DEPOSIT,
        ]
        .contains(&path)
    {
        let ready = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            handle.status_watcher::<WalletService>(),
        )
        .await;
        if !matches!(ready, Ok(Ok(status)) if status.current() == ServiceStatus::Ready) {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                "wallet unavailable during startup or recovery",
            )
                .into_response();
        }
    }
    next.run(request).await
}

/// Builds the axum router from the shared route table.
///
/// Only the `$handler` half of each row is used; the `OpenAPI` half is matched
/// and discarded. Expanded inside `AxumBackend::serve`, where the handler type
/// parameters are in scope.
macro_rules! build_router {
    ($( $method:ident $path:expr => $doc:path, $handler:expr ; )*) => {
        Router::new()$(.route($path, routing::$method($handler)))*
    };
}

type BlockStorageService<RuntimeServiceId> = StorageService<RuntimeServiceId>;

pub struct AxumBackend<
    TimeBackend,
    MempoolStorageAdapter,
    SdpMempool,
    SdpWallet,
    SdpStateStorage,
    ChainLeader,
> {
    settings: AxumBackendSettings,
    _phantom: PhantomData<(
        TimeBackend,
        MempoolStorageAdapter,
        SdpMempool,
        SdpWallet,
        SdpStateStorage,
        ChainLeader,
    )>,
}

#[async_trait::async_trait]
impl<
    TimeBackend,
    MempoolStorageAdapter,
    SdpMempool,
    SdpWallet,
    SdpStateStorage,
    ChainLeader,
    RuntimeServiceId,
> Backend<RuntimeServiceId>
    for AxumBackend<
        TimeBackend,
        MempoolStorageAdapter,
        SdpMempool,
        SdpWallet,
        SdpStateStorage,
        ChainLeader,
    >
where
    TimeBackend: lb_time_service::backends::TimeBackend + Send + 'static,
    TimeBackend::Settings: Clone + Send + Sync,
    MempoolStorageAdapter: lb_tx_service::storage::MempoolStorageAdapter<
            RuntimeServiceId,
            Item = ValidatedTx,
            Key = <ValidatedTx as Hashable>::Hash,
            BlockId = HeaderId,
        > + Send
        + Sync
        + Clone
        + 'static,
    MempoolStorageAdapter::Error: Debug,
    SdpMempool: SdpMempoolAdapter + Send + Sync + 'static,
    SdpWallet: SdpWalletAdapter + Send + Sync + 'static,
    ChainLeader: ChainLeaderServiceData,
    SdpStateStorage: SdpStateStorageTrait<RuntimeServiceId> + Send + 'static,
    RuntimeServiceId: Debug
        + Sync
        + Send
        + Display
        + Clone
        + 'static
        + AsServiceId<Cryptarchia<RuntimeServiceId>>
        + AsServiceId<crate::TimeService>
        + AsServiceId<BlockBroadcastService<RuntimeServiceId>>
        + AsServiceId<
            lb_network_service::NetworkService<
                lb_network_service::backends::libp2p::Libp2p,
                NtpTimeBackend,
                RuntimeServiceId,
            >,
        >
        + AsServiceId<BlockStorageService<RuntimeServiceId>>
        + AsServiceId<StorageService<RuntimeServiceId>>
        + AsServiceId<
            TxMempoolService<
                lb_tx_service::network::adapters::libp2p::Libp2pAdapter<
                    ValidatedTx,
                    <ValidatedTx as Hashable>::Hash,
                    RuntimeServiceId,
                >,
                Mempool<HeaderId, ValidatedTx, MempoolStorageAdapter, RuntimeServiceId>,
                MempoolStorageAdapter,
                NtpTimeBackend,
                RuntimeServiceId,
            >,
        >
        + AsServiceId<
            lb_sdp_service::SdpService<
                SdpMempool,
                SdpWallet,
                Cryptarchia<RuntimeServiceId>,
                SdpStateStorage,
                RuntimeServiceId,
            >,
        >
        + AsServiceId<WalletService>
        + AsServiceId<ChainLeader>
        + AsServiceId<BlendService>
        + AsServiceId<PoWService>
        + AsServiceId<TracingService>,
{
    type Error = std::io::Error;
    type Settings = AxumBackendSettings;

    async fn new(settings: Self::Settings) -> Result<Self, Self::Error>
    where
        Self: Sized,
    {
        Ok(Self {
            settings,
            _phantom: PhantomData,
        })
    }

    async fn serve(self, handle: OverwatchHandle<RuntimeServiceId>) -> Result<(), Self::Error> {
        let app = api_routes!(build_router)
            .merge(SwaggerUi::new("/swagger-ui").url("/api-docs/openapi.json", ApiDoc::openapi()));

        let app = app
            .with_state(handle.clone())
            .layer(axum::middleware::from_fn_with_state(
                handle.clone(),
                wallet_readiness::<RuntimeServiceId>,
            ))
            // The chain ID is a constant of the deployment, so it rides along
            // as a request extension instead of a service round trip.
            .layer(axum::Extension(self.settings.chain_id.clone()))
            // Each era's fork digest is a constant of the deployment too; the
            // era in force comes from the time service.
            .layer(axum::Extension(self.settings.fork_digests.clone()))
            .layer(axum::middleware::from_fn(http_metrics_middleware))
            .layer(axum::extract::DefaultBodyLimit::max(
                self.settings.max_body_size,
            ))
            .layer(TimeoutLayer::with_status_code(
                StatusCode::REQUEST_TIMEOUT,
                self.settings.timeout,
            ))
            .layer(RequestBodyLimitLayer::new(self.settings.max_body_size))
            .layer(ConcurrencyLimitLayer::new(
                self.settings.max_concurrent_requests,
            ))
            .layer(
                TraceLayer::new_for_http()
                    .on_request(DefaultOnRequest::new().level(TracingLevel::TRACE))
                    .on_response(DefaultOnResponse::new().level(TracingLevel::TRACE)),
            );

        #[cfg(feature = "profiling")]
        let app = {
            let pprof_routes = lb_http_api_common::pprof::create_pprof_router().layer(
                TraceLayer::new_for_http()
                    .on_request(DefaultOnRequest::new().level(TracingLevel::TRACE))
                    .on_response(DefaultOnResponse::new().level(TracingLevel::TRACE)),
            );

            app.merge(pprof_routes)
        };

        let app =
            restrict_browser_access(app, self.settings.cors_origins, self.settings.allowed_hosts);

        let listener = TcpListener::bind(&self.settings.address)
            .await
            .map_err(|e| {
                std::io::Error::new(
                    e.kind(),
                    format!("Failed to bind to address {}: {}", self.settings.address, e),
                )
            })?;

        let app = app.into_make_service_with_connect_info::<std::net::SocketAddr>();
        axum::serve(listener, app).await
    }
}

#[cfg(test)]
mod recovery_admission_tests {
    use axum::{
        body::Body,
        extract::State,
        http::Request,
        routing::{get, post},
    };
    use lb_core::mantle::{
        Note,
        note_set::{CommitmentMmr, NoteSetIndex},
    };
    use lb_http_api_common::bodies::wallet::import::WalletImportNoteRequestBody;
    use lb_key_management_system_service::keys::ZkPublicKey;
    use tower::Service as _;

    use super::*;

    fn unavailable_handle() -> OverwatchHandle<crate::RuntimeServiceId> {
        let (sender, receiver) = tokio::sync::mpsc::channel(1);
        drop(receiver);
        OverwatchHandle::new(tokio::runtime::Handle::current(), sender)
    }

    #[tokio::test]
    async fn wallet_http_is_unavailable_until_the_wallet_is_ready() {
        let mut app = Router::new()
            .route("/wallet/notes/import", post(async || StatusCode::OK))
            .route("/version", get(async || StatusCode::OK))
            .layer(axum::middleware::from_fn_with_state(
                unavailable_handle(),
                wallet_readiness,
            ));
        let wallet = app
            .call(
                Request::post("/wallet/notes/import")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(wallet.status(), StatusCode::SERVICE_UNAVAILABLE);
        let ordinary = app
            .call(Request::get("/version").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(ordinary.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn historical_http_import_is_rejected_without_querying_any_service() {
        let note = Note::new(1, 7u64.into(), ZkPublicKey::zero());
        let (_, path) = CommitmentMmr::new()
            .push_with_paths(note.commitment(), &mut [])
            .unwrap();
        for _ in 0..3 {
            // A disconnected handle would fail if the handler queried a
            // service before checking the recovery request shape.
            let response = wallet::import_note::<WalletService, crate::RuntimeServiceId>(
                State(unavailable_handle()),
                axum::Json(WalletImportNoteRequestBody {
                    note,
                    note_set: NoteSetIndex::Ledger,
                    historical_path: Some(path.clone()),
                }),
            )
            .await;
            assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        }
    }
}
