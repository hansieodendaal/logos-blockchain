use std::fmt::{Debug, Display};

use async_trait::async_trait;
use backends::{BanningSynchronizer, NetworkBackend};
use lb_banning_service::{BanningService, BanningServiceApi};
use lb_log_targets::network_service;
use overwatch::{
    OpaqueServiceResourcesHandle,
    services::{
        AsServiceId, ServiceCore, ServiceData,
        state::{NoOperator, NoState},
    },
};
use tokio::time::{Duration, sleep, timeout};

use crate::{config::NetworkConfig, message::BackendNetworkMsg};

pub mod backends;
pub mod config;
mod local_ban_view;
pub mod message;
mod metrics;

pub use local_ban_view::LocalBanView;

const LOG_TARGET: &str = network_service::ROOT;
const BANNING_RELAY_TIMEOUT: Duration = Duration::from_secs(1);
const BANNING_RETRY_INITIAL: Duration = Duration::from_secs(1);
const BANNING_RETRY_MAX: Duration = Duration::from_secs(30);

pub struct NetworkService<Backend, RuntimeServiceId>
where
    Backend: NetworkBackend<RuntimeServiceId> + 'static,
{
    backend: Backend,
    service_resources_handle: OpaqueServiceResourcesHandle<Self, RuntimeServiceId>,
}

impl<Backend, RuntimeServiceId> ServiceData for NetworkService<Backend, RuntimeServiceId>
where
    Backend: NetworkBackend<RuntimeServiceId> + 'static,
{
    type Settings = NetworkConfig<Backend::Settings>;
    type State = NoState<NetworkConfig<Backend::Settings>>;
    type StateOperator = NoOperator<Self::State>;
    type Message = BackendNetworkMsg<Backend, RuntimeServiceId>;
}

#[async_trait]
impl<Backend, RuntimeServiceId> ServiceCore<RuntimeServiceId>
    for NetworkService<Backend, RuntimeServiceId>
where
    Backend: BanningSynchronizer<RuntimeServiceId> + Send + 'static,
    RuntimeServiceId: AsServiceId<Self> + Clone + Display + Send,
{
    fn init(
        service_resources_handle: OpaqueServiceResourcesHandle<Self, RuntimeServiceId>,
        _initial_state: Self::State,
    ) -> Result<Self, overwatch::DynError> {
        Ok(Self {
            backend: Backend::new(
                service_resources_handle
                    .settings_handle
                    .notifier()
                    .get_updated_settings()
                    .backend,
                service_resources_handle.overwatch_handle.clone(),
            ),
            service_resources_handle,
        })
    }

    async fn run(mut self) -> Result<(), overwatch::DynError> {
        let Self {
            service_resources_handle:
                OpaqueServiceResourcesHandle::<Self, RuntimeServiceId> {
                    overwatch_handle,
                    mut inbound_relay,
                    ..
                },
            mut backend,
        } = self;

        self.service_resources_handle.status_updater.notify_ready();
        tracing::info!(
            target: LOG_TARGET,
            "Service '{}' is ready.",
            <RuntimeServiceId as AsServiceId<Self>>::SERVICE_ID
        );

        let mut banning_configured = false;
        let mut banning_synchronizer =
            Box::pin(backend.start_banning_synchronizer(overwatch_handle));

        loop {
            tokio::select! {
                Some(msg) = inbound_relay.recv() => {
                    Self::handle_network_service_message(msg, &mut backend).await;
                }
                () = &mut banning_synchronizer, if !banning_configured => {
                    banning_configured = true;
                }
                else => break,
            }
        }

        Ok(())
    }
}

pub(crate) async fn acquire_banning_service<RuntimeServiceId>(
    overwatch_handle: overwatch::overwatch::handle::OverwatchHandle<RuntimeServiceId>,
) -> BanningServiceApi<RuntimeServiceId>
where
    RuntimeServiceId:
        AsServiceId<BanningService<RuntimeServiceId>> + Debug + Display + Send + Sync + 'static,
{
    let mut retry_backoff = BANNING_RETRY_INITIAL;
    let mut reported_failure = false;

    loop {
        match timeout(
            BANNING_RELAY_TIMEOUT,
            overwatch_handle.relay::<BanningService<RuntimeServiceId>>(),
        )
        .await
        {
            Ok(Ok(relay)) => return BanningServiceApi::new(relay),
            Ok(Err(error)) if !reported_failure => {
                tracing::warn!(
                    target: LOG_TARGET,
                    %error,
                    "BanningService relay unavailable; network banning remains fail-open"
                );
                reported_failure = true;
            }
            Err(_) if !reported_failure => {
                tracing::warn!(
                    target: LOG_TARGET,
                    "BanningService relay acquisition timed out; network banning remains fail-open"
                );
                reported_failure = true;
            }
            Ok(Err(_)) | Err(_) => {}
        }

        sleep(retry_backoff).await;
        retry_backoff = retry_backoff.saturating_mul(2).min(BANNING_RETRY_MAX);
    }
}

impl<Backend, RuntimeServiceId> NetworkService<Backend, RuntimeServiceId>
where
    Backend: NetworkBackend<RuntimeServiceId> + Send + 'static,
{
    async fn handle_network_service_message(
        msg: BackendNetworkMsg<Backend, RuntimeServiceId>,
        backend: &mut Backend,
    ) {
        match msg {
            BackendNetworkMsg::<Backend, _>::Process(msg) => {
                // split sending in two steps to help the compiler understand we do not
                // need to hold an instance of &I (which is not send) across an await point
                let send = backend.process(msg);
                send.await;
            }
            BackendNetworkMsg::<Backend, _>::SubscribeToPubSub { sender } => sender
                .send(backend.subscribe_to_pubsub().await)
                .unwrap_or_else(|_| {
                    tracing::warn!(
                        target: LOG_TARGET,
                        "client hung up before a subscription handle could be established"
                    );
                }),
            BackendNetworkMsg::<Backend, _>::SubscribeToChainSync { sender } => sender
                .send(backend.subscribe_to_chainsync().await)
                .unwrap_or_else(|_| {
                    tracing::warn!(
                        target: LOG_TARGET,
                        "client hung up before a subscription handle could be established"
                    );
                }),
        }
    }
}
