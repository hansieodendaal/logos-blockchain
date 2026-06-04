/// Re-export for `OpenAPI`
#[cfg(feature = "openapi")]
pub mod openapi {
    pub use crate::backend::Status;
}

use std::{
    collections::{BTreeSet, HashMap, HashSet, VecDeque},
    fmt::{Debug, Display},
    hash::Hash,
    marker::PhantomData,
    pin::Pin,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use futures::{Stream, StreamExt as _};
use lb_chain_service::{
    LibUpdate, ProcessedBlockEvent,
    api::{CryptarchiaServiceApi, CryptarchiaServiceData},
};
use lb_core::mantle::Transaction;
use lb_log_targets::mempool;
use lb_network_service::{NetworkService, message::BackendNetworkMsg};
use lb_services_utils::{
    overwatch::{
        JsonFileBackend, RecoveryOperator,
        recovery::operators::RecoveryBackend as RecoveryBackendTrait,
    },
    wait_until_services_are_ready,
};
use lb_storage_service::StorageService;
use overwatch::{
    OpaqueServiceResourcesHandle,
    services::{AsServiceId, ServiceCore, ServiceData, relay::OutboundRelay},
};
use tokio::sync::oneshot;
use tokio_stream::wrappers::BroadcastStream;
use tracing::{debug, error};

use crate::{
    MempoolMetrics, MempoolMsg, MempoolRemoveReason, TransactionsByHashesResponse,
    TxLifecycleStatus, backend,
    backend::{MemPool as MemPoolTrait, MempoolAdapter, MempoolError, RecoverableMempool},
    network::NetworkAdapter as NetworkAdapterTrait,
    storage::MempoolStorageAdapter,
    tx::{settings::TxMempoolSettings, state::TxMempoolState},
};

const LOG_TARGET: &str = mempool::SERVICE;
const TX_LIFECYCLE_RETENTION: usize = 50_000;

#[derive(Clone, Copy, Debug)]
enum TxLifecycleSource {
    LocalSubmit,
    Gossip,
    Unknown,
}

#[derive(Clone, Copy, Debug)]
enum TxLifecycleRecordState {
    SeenInMempool,
    IncludedInCanonicalBlock,
    RemovedFromMempool,
    Rejected,
}

#[derive(Clone, Copy, Debug)]
struct TxLifecycleRecord {
    last_seq: u64,
    _timestamp_ms: u64,
    _source: TxLifecycleSource,
    state: TxLifecycleRecordState,
}

struct TxLifecycleTracker<TxHash>
where
    TxHash: Clone + Eq + Hash,
{
    entries: HashMap<TxHash, TxLifecycleRecord>,
    order: VecDeque<(u64, TxHash)>,
    next_seq: u64,
    capacity: usize,
}

impl<TxHash> TxLifecycleTracker<TxHash>
where
    TxHash: Clone + Eq + Hash,
{
    fn new(capacity: usize) -> Self {
        Self {
            entries: HashMap::new(),
            order: VecDeque::new(),
            next_seq: 1,
            capacity,
        }
    }

    fn record_seen_in_mempool(&mut self, hash: TxHash, source: TxLifecycleSource) {
        self.record(hash, TxLifecycleRecordState::SeenInMempool, source);
    }

    fn record_removed(&mut self, hash: TxHash, reason: MempoolRemoveReason) {
        let state = match reason {
            MempoolRemoveReason::CanonicalBlockApplied => {
                TxLifecycleRecordState::IncludedInCanonicalBlock
            }
            MempoolRemoveReason::ProposalValidationFailed
            | MempoolRemoveReason::ExplicitRemoval => TxLifecycleRecordState::RemovedFromMempool,
        };
        self.record(hash, state, TxLifecycleSource::Unknown);
    }

    fn record_rejected(&mut self, hash: TxHash, source: TxLifecycleSource) {
        self.record(hash, TxLifecycleRecordState::Rejected, source);
    }

    fn classify(&self, hash: &TxHash, in_mempool_now: bool) -> TxLifecycleStatus {
        if in_mempool_now {
            return TxLifecycleStatus::InMempool;
        }

        match self.entries.get(hash).map(|record| record.state) {
            Some(TxLifecycleRecordState::IncludedInCanonicalBlock) => {
                TxLifecycleStatus::IncludedInCanonicalBlock
            }
            Some(TxLifecycleRecordState::RemovedFromMempool) => {
                TxLifecycleStatus::RemovedFromMempool
            }
            Some(TxLifecycleRecordState::Rejected) => TxLifecycleStatus::Rejected,
            Some(TxLifecycleRecordState::SeenInMempool) => TxLifecycleStatus::SeenButNotInMempool,
            None => TxLifecycleStatus::NeverSeen,
        }
    }

    fn record(&mut self, hash: TxHash, state: TxLifecycleRecordState, source: TxLifecycleSource) {
        let seq = self.next_seq;
        self.next_seq = self.next_seq.saturating_add(1);

        self.entries.insert(
            hash.clone(),
            TxLifecycleRecord {
                last_seq: seq,
                _timestamp_ms: current_timestamp_millis(),
                _source: source,
                state,
            },
        );

        self.order.push_back((seq, hash));
        self.prune_if_needed();
    }

    fn prune_if_needed(&mut self) {
        while self.entries.len() > self.capacity {
            let Some((old_seq, old_hash)) = self.order.pop_front() else {
                break;
            };

            if self
                .entries
                .get(&old_hash)
                .is_some_and(|record| record.last_seq == old_seq)
            {
                self.entries.remove(&old_hash);
            }
        }
    }
}

fn current_timestamp_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

type MempoolStateUpdater<Pool, NetworkAdapter, RuntimeServiceId> =
    overwatch::services::state::StateUpdater<
        Option<
            TxMempoolState<
                <Pool as RecoverableMempool>::RecoveryState,
                <Pool as MemPoolTrait>::Settings,
                <NetworkAdapter as NetworkAdapterTrait<RuntimeServiceId>>::Settings,
            >,
        >,
    >;

type TxMempoolRecoveryState<Pool, NetworkAdapter, RuntimeServiceId> = TxMempoolState<
    <Pool as RecoverableMempool>::RecoveryState,
    <Pool as MemPoolTrait>::Settings,
    <NetworkAdapter as NetworkAdapterTrait<RuntimeServiceId>>::Settings,
>;

type TxMempoolRecoverySettings<Pool, NetworkAdapter, RuntimeServiceId> = TxMempoolSettings<
    <Pool as MemPoolTrait>::Settings,
    <NetworkAdapter as NetworkAdapterTrait<RuntimeServiceId>>::Settings,
>;

type TxMempoolRecoveryBackend<Pool, NetworkAdapter, RuntimeServiceId> = JsonFileBackend<
    TxMempoolRecoveryState<Pool, NetworkAdapter, RuntimeServiceId>,
    TxMempoolRecoverySettings<Pool, NetworkAdapter, RuntimeServiceId>,
>;

/// A tx mempool service that uses a [`JsonFileBackend`] as a recovery
/// mechanism.
pub type TxMempoolService<
    MempoolNetworkAdapter,
    Pool,
    StorageAdapter,
    Cryptarchia,
    RuntimeServiceId,
> = GenericTxMempoolService<
    Pool,
    MempoolNetworkAdapter,
    TxMempoolRecoveryBackend<Pool, MempoolNetworkAdapter, RuntimeServiceId>,
    StorageAdapter,
    Cryptarchia,
    RuntimeServiceId,
>;

/// A generic tx mempool service which wraps around a mempool, a network
/// adapter, and a recovery backend.
pub struct GenericTxMempoolService<
    Pool,
    NetworkAdapter,
    RecoveryBackend,
    Adapter,
    ChainService,
    RuntimeServiceId,
> where
    Pool: MemPoolTrait<Adapter = Adapter> + RecoverableMempool + Send + Sync,
    Adapter: Clone + Send + Sync,
    ChainService: CryptarchiaServiceData,
    <Pool as MemPoolTrait>::Settings: Clone,
    NetworkAdapter: NetworkAdapterTrait<RuntimeServiceId> + Send + Sync,
    NetworkAdapter::Settings: Clone,
    RecoveryBackend: RecoveryBackendTrait + Send + Sync,
{
    service_resources_handle: OpaqueServiceResourcesHandle<Self, RuntimeServiceId>,
    initial_state: <Self as ServiceData>::State,
    _phantom: PhantomData<(Pool, NetworkAdapter, RecoveryBackend, Adapter, ChainService)>,
}

impl<Pool, NetworkAdapter, RecoveryBackend, Adapter, ChainService, RuntimeServiceId>
    GenericTxMempoolService<
        Pool,
        NetworkAdapter,
        RecoveryBackend,
        Adapter,
        ChainService,
        RuntimeServiceId,
    >
where
    Pool: MemPoolTrait<Adapter = Adapter> + RecoverableMempool + Send + Sync,
    Adapter: Clone + Send + Sync,
    ChainService: CryptarchiaServiceData,
    <Pool as MemPoolTrait>::Settings: Clone,
    NetworkAdapter: NetworkAdapterTrait<RuntimeServiceId> + Send + Sync,
    NetworkAdapter::Settings: Clone,
    RecoveryBackend: RecoveryBackendTrait + Send + Sync,
{
    pub const fn new(
        service_resources_handle: OpaqueServiceResourcesHandle<Self, RuntimeServiceId>,
        initial_state: <Self as ServiceData>::State,
    ) -> Self {
        Self {
            service_resources_handle,
            initial_state,
            _phantom: PhantomData,
        }
    }
}

impl<Pool, NetworkAdapter, RecoveryBackend, Adapter, ChainService, RuntimeServiceId> ServiceData
    for GenericTxMempoolService<
        Pool,
        NetworkAdapter,
        RecoveryBackend,
        Adapter,
        ChainService,
        RuntimeServiceId,
    >
where
    Pool: MemPoolTrait<Adapter = Adapter> + RecoverableMempool + Send + Sync,
    Adapter: Clone + Send + Sync,
    ChainService: CryptarchiaServiceData,
    <Pool as MemPoolTrait>::Settings: Clone,
    NetworkAdapter: NetworkAdapterTrait<RuntimeServiceId> + Send + Sync,
    NetworkAdapter::Settings: Clone,
    RecoveryBackend: RecoveryBackendTrait + Send + Sync,
{
    type Settings = TxMempoolSettings<<Pool as MemPoolTrait>::Settings, NetworkAdapter::Settings>;
    type State = TxMempoolState<
        <Pool as RecoverableMempool>::RecoveryState,
        <Pool as MemPoolTrait>::Settings,
        NetworkAdapter::Settings,
    >;
    type StateOperator = RecoveryOperator<RecoveryBackend>;
    type Message = MempoolMsg<Pool::BlockId, Pool::Tx, Pool::TxHash>;
}

#[async_trait::async_trait]
impl<Pool, NetworkAdapter, RecoveryBackend, Adapter, ChainService, RuntimeServiceId>
    ServiceCore<RuntimeServiceId>
    for GenericTxMempoolService<
        Pool,
        NetworkAdapter,
        RecoveryBackend,
        Adapter,
        ChainService,
        RuntimeServiceId,
    >
where
    Pool: MemPoolTrait<Adapter = Adapter> + RecoverableMempool + Send + Sync,
    Adapter: MempoolAdapter<Pool::Tx, RuntimeServiceId> + Clone + Send + Sync,
    <Pool as RecoverableMempool>::RecoveryState: Debug + Send + Sync,
    Pool::TxHash: Eq + Hash + Clone + Send + Sync + 'static,
    Pool::Tx: Transaction<Hash = Pool::TxHash> + Debug + Eq + Clone + Send + Sync + 'static,
    Pool::Settings: Clone + Sync + Send,
    NetworkAdapter:
        NetworkAdapterTrait<RuntimeServiceId, Payload = Pool::Tx, Key = Pool::TxHash> + Send + Sync,
    NetworkAdapter::Settings: Clone + Send + Sync + 'static,
    RecoveryBackend: RecoveryBackendTrait + Send + Sync,
    RuntimeServiceId: Display
        + Debug
        + Sync
        + Send
        + Clone
        + 'static
        + AsServiceId<Self>
        + AsServiceId<NetworkService<NetworkAdapter::Backend, RuntimeServiceId>>
        + AsServiceId<
            StorageService<
                <Adapter as MempoolStorageAdapter<RuntimeServiceId>>::Backend,
                RuntimeServiceId,
            >,
        >
        + AsServiceId<ChainService>,
    ChainService: CryptarchiaServiceData<Tx = Pool::Tx> + Send + Sync,
{
    fn init(
        service_resources_handle: OpaqueServiceResourcesHandle<Self, RuntimeServiceId>,
        initial_state: Self::State,
    ) -> Result<Self, overwatch::DynError> {
        tracing::trace!(
            target: LOG_TARGET,
            "Initializing TxMempoolService with initial state {:#?}",
            initial_state.pool
        );
        Ok(Self::new(service_resources_handle, initial_state))
    }

    async fn run(mut self) -> Result<(), overwatch::DynError> {
        let settings_handle = &self.service_resources_handle.settings_handle;
        let settings = settings_handle.notifier().get_updated_settings();

        let overwatch_handle = &self.service_resources_handle.overwatch_handle;

        let cryptarchia_api: CryptarchiaServiceApi<ChainService, RuntimeServiceId> =
            CryptarchiaServiceApi::new(
                overwatch_handle
                    .relay::<ChainService>()
                    .await
                    .expect("Cryptarchia service relay should be available"),
            );

        let mut blocks_stream = BroadcastStream::new(cryptarchia_api.subscribe_new_blocks().await?);
        let mut lib_stream = BroadcastStream::new(cryptarchia_api.subscribe_lib_updates().await?);

        let pool_adapter = Adapter::new(overwatch_handle.clone()).await?;

        let pool_state = self.initial_state.pool.take();

        let mut pool = match pool_state {
            None => <Pool as MemPoolTrait>::new(settings.pool.clone(), pool_adapter),
            Some(recovered_pool_state) => <Pool as RecoverableMempool>::recover(
                settings.pool.clone(),
                recovered_pool_state,
                pool_adapter,
            ),
        };

        let network_service_relay = overwatch_handle
            .relay::<NetworkService<_, _>>()
            .await
            .expect("Relay connection with NetworkService should succeed");

        // Queue for network messages
        let mut network_items = NetworkAdapter::new(
            settings_handle
                .notifier()
                .get_updated_settings()
                .network_adapter,
            network_service_relay.clone(),
        )
        .await
        .payload_stream()
        .await;

        self.service_resources_handle.status_updater.notify_ready();
        tracing::info!(
            target: LOG_TARGET,
            "Service '{}' is ready.",
            <RuntimeServiceId as AsServiceId<Self>>::SERVICE_ID
        );

        wait_until_services_are_ready!(
            &overwatch_handle,
            Some(Duration::from_mins(1)),
            NetworkService<_, _>
        )
        .await?;

        self.run_event_loop(
            &mut pool,
            network_service_relay,
            &mut network_items,
            &mut blocks_stream,
            &mut lib_stream,
        )
        .await
    }
}

impl<Pool, NetworkAdapter, RecoveryBackend, Adapter, Cryptarchia, RuntimeServiceId>
    GenericTxMempoolService<
        Pool,
        NetworkAdapter,
        RecoveryBackend,
        Adapter,
        Cryptarchia,
        RuntimeServiceId,
    >
where
    Pool: MemPoolTrait<Adapter = Adapter> + RecoverableMempool + Send + Sync,
    Adapter: Clone + Send + Sync,
    Cryptarchia: CryptarchiaServiceData,
    Pool::Tx: Transaction<Hash = Pool::TxHash> + Clone + Send + 'static,
    Pool::TxHash: Eq + Hash + Clone,
    Pool::Settings: Clone,
    NetworkAdapter: NetworkAdapterTrait<RuntimeServiceId, Payload = Pool::Tx> + Send + Sync,
    NetworkAdapter::Settings: Clone + Send + 'static,
    RecoveryBackend: RecoveryBackendTrait + Send + Sync,
    RuntimeServiceId: 'static,
{
    #[expect(
        clippy::cognitive_complexity,
        reason = "event loop handles many message types by design"
    )]
    async fn run_event_loop(
        &mut self,
        pool: &mut Pool,
        network_service_relay: OutboundRelay<
            BackendNetworkMsg<NetworkAdapter::Backend, RuntimeServiceId>,
        >,
        network_items: &mut Box<dyn Stream<Item = (Pool::TxHash, Pool::Tx)> + Unpin + Send>,
        blocks_stream: &mut BroadcastStream<ProcessedBlockEvent>,
        lib_stream: &mut BroadcastStream<LibUpdate>,
    ) -> Result<(), overwatch::DynError>
    where
        Pool::Settings: Send + Sync,
        NetworkAdapter::Settings: Send + Sync,
        Pool::TxHash: Eq + Hash + Clone,
    {
        let mut lifecycle_tracker = TxLifecycleTracker::<Pool::TxHash>::new(TX_LIFECYCLE_RETENTION);

        loop {
            tokio::select! {
                // Queue for relay messages
                Some(relay_msg) = self.service_resources_handle.inbound_relay.recv() => {
                    let state_updater = self.service_resources_handle.state_updater.clone();
                    let settings = self
                        .service_resources_handle
                        .settings_handle
                        .notifier()
                        .get_updated_settings()
                        .network_adapter;

                    Self::handle_mempool_message(
                        pool,
                        relay_msg,
                        network_service_relay.clone(),
                        state_updater,
                        settings,
                        &mut lifecycle_tracker,
                    )
                    .await;
                }
                Some(new_block_event) = blocks_stream.next() => {
                    match new_block_event {
                        Ok(new_block_event) => {
                            debug!("Processing new block event: {:#?}", new_block_event);
                            pool.process_new_block_event(new_block_event).await;
                        },
                        Err(e) => {
                            error!("Error processing new block event: {e}");
                        }
                    }

                }
                Some(lib_update_event) = lib_stream.next() => {
                    match lib_update_event {
                        Ok(lib_update_event) => {
                            debug!("Processing lib update event: {:#?}", lib_update_event);
                            pool.process_lib_event(lib_update_event);
                        },
                        Err(e) => {
                            error!("Error processing new lib event: {e}");
                        }
                    }

                }
                Some((key, item)) = network_items.next() => {
                    Self::handle_network_item(
                        pool,
                        key,
                        item,
                        &self.service_resources_handle.state_updater,
                        &mut lifecycle_tracker,
                    )
                    .await;
                }
            }
        }
    }

    async fn handle_mempool_message(
        pool: &mut Pool,
        message: MempoolMsg<Pool::BlockId, Pool::Tx, Pool::TxHash>,
        network_relay: OutboundRelay<BackendNetworkMsg<NetworkAdapter::Backend, RuntimeServiceId>>,
        state_updater: MempoolStateUpdater<Pool, NetworkAdapter, RuntimeServiceId>,
        settings: NetworkAdapter::Settings,
        lifecycle_tracker: &mut TxLifecycleTracker<Pool::TxHash>,
    ) where
        Pool::Settings: Send + Sync,
        NetworkAdapter::Settings: Send + Sync,
        Pool::TxHash: Eq + Hash + Clone,
    {
        match message {
            MempoolMsg::Add {
                key,
                payload,
                reply_channel,
                ..
            } => {
                Self::handle_add_message(
                    pool,
                    key,
                    payload,
                    reply_channel,
                    network_relay,
                    state_updater,
                    settings,
                    lifecycle_tracker,
                    TxLifecycleSource::LocalSubmit,
                )
                .await;
            }
            MempoolMsg::View {
                ancestor_hint,
                reply_channel,
            } => {
                Self::handle_view_message(pool, ancestor_hint, reply_channel).await;
            }
            MempoolMsg::GetTransactionsByHashes {
                hashes,
                reply_channel,
            } => {
                let result = Self::partition_transactions_by_availability(pool, hashes).await;

                if let Err(_e) = reply_channel.send(result) {
                    tracing::debug!(target: LOG_TARGET, "Failed to send transactions reply");
                }
            }
            MempoolMsg::Remove { ids, reason } => {
                pool.remove(&ids).await;
                for id in ids {
                    lifecycle_tracker.record_removed(id, reason);
                }
            }
            MempoolMsg::ClassifyTransactions {
                hashes,
                reply_channel,
            } => {
                Self::handle_classify_transactions_message(
                    pool,
                    lifecycle_tracker,
                    hashes,
                    reply_channel,
                )
                .await;
            }
            MempoolMsg::Metrics { reply_channel } => {
                Self::handle_metrics_message(pool, reply_channel).await;
            }
            MempoolMsg::Status {
                items,
                reply_channel,
            } => {
                Self::handle_status_message(pool, &items, reply_channel);
            }
        }
    }

    #[expect(clippy::too_many_arguments, reason = "Need all args")]
    async fn handle_add_message(
        pool: &mut Pool,
        item_key: Pool::TxHash,
        item: Pool::Tx,
        reply_channel: oneshot::Sender<Result<(), MempoolError>>,
        network_relay: OutboundRelay<BackendNetworkMsg<NetworkAdapter::Backend, RuntimeServiceId>>,
        state_updater: MempoolStateUpdater<Pool, NetworkAdapter, RuntimeServiceId>,
        settings: NetworkAdapter::Settings,
        lifecycle_tracker: &mut TxLifecycleTracker<Pool::TxHash>,
        source: TxLifecycleSource,
    ) where
        Pool::Settings: Send + Sync,
        NetworkAdapter::Settings: Send + Sync,
        Pool::TxHash: Eq + Hash + Clone,
    {
        let item_for_broadcast = item.clone();

        match pool.add_item(item).await {
            Ok(_id) => {
                lifecycle_tracker.record_seen_in_mempool(item_key, source);
                Self::handle_add_success(
                    pool,
                    &state_updater,
                    settings,
                    network_relay,
                    item_for_broadcast,
                    reply_channel,
                );
            }
            Err(MempoolError::ExistingItem) => {
                lifecycle_tracker.record_seen_in_mempool(item_key, source);
                // Tx already in pool, but since this came from a local submission
                // (not gossip), re-gossip it so leader nodes can pick it up.
                tokio::spawn(async move {
                    let adapter = NetworkAdapter::new(settings, network_relay).await;
                    adapter.send(item_for_broadcast).await;
                });
                if let Err(e) = reply_channel.send(Ok(())) {
                    tracing::debug!(target: LOG_TARGET, "Failed to send add reply: {:?}", e);
                }
            }
            Err(e) => {
                lifecycle_tracker.record_rejected(item_key, source);
                Self::handle_add_error(e, reply_channel);
            }
        }
    }

    async fn handle_classify_transactions_message(
        pool: &Pool,
        lifecycle_tracker: &TxLifecycleTracker<Pool::TxHash>,
        hashes: Vec<Pool::TxHash>,
        reply_channel: oneshot::Sender<Vec<TxLifecycleStatus>>,
    ) where
        Pool::TxHash: Eq + Hash + Clone,
    {
        let in_mempool: HashSet<Pool::TxHash> =
            match pool.get_items_by_keys(hashes.iter().cloned()).await {
                Ok(stream) => stream.map(|tx| tx.hash()).collect().await,
                Err(e) => {
                    tracing::debug!(
                        target: LOG_TARGET,
                        "failed to classify current mempool membership for hashes: {e}"
                    );
                    HashSet::new()
                }
            };

        let statuses = hashes
            .iter()
            .map(|hash| lifecycle_tracker.classify(hash, in_mempool.contains(hash)))
            .collect::<Vec<_>>();

        if let Err(_e) = reply_channel.send(statuses) {
            tracing::debug!(target: LOG_TARGET, "Failed to send classify-transactions reply");
        }
    }

    async fn handle_view_message(
        pool: &Pool,
        ancestor_hint: Pool::BlockId,
        reply_channel: oneshot::Sender<Pin<Box<dyn Stream<Item = Pool::Tx> + Send>>>,
    ) {
        tracing::trace!(target: LOG_TARGET, "Handling mempool View message");

        let items = match pool.view(ancestor_hint).await {
            Ok(stream) => stream,
            Err(e) => {
                error!(
                    target: LOG_TARGET,
                    "mempool view failed; returning empty stream: {e:?}",
                );
                Box::pin(futures::stream::iter(Vec::new()))
            }
        };

        if let Err(_e) = reply_channel.send(Box::pin(items)) {
            tracing::debug!(target: LOG_TARGET, "Failed to send view reply");
        }
    }

    async fn handle_metrics_message(pool: &Pool, reply_channel: oneshot::Sender<MempoolMetrics>) {
        let pending_items = pool.pending_item_count().await.unwrap_or_else(|e| {
            error!(target: LOG_TARGET, "Failed to get pending item count: {e:?}");
            usize::MAX
        });
        let info = MempoolMetrics {
            pending_items,
            last_item_timestamp: pool.last_item_timestamp(),
        };

        if let Err(_e) = reply_channel.send(info) {
            tracing::debug!(target: LOG_TARGET, "Failed to send metrics reply");
        }
    }

    fn handle_status_message(
        pool: &Pool,
        items: &[Pool::TxHash],
        reply_channel: oneshot::Sender<Vec<backend::Status>>,
    ) {
        let statuses = pool.status(items);

        if let Err(_e) = reply_channel.send(statuses) {
            tracing::debug!(target: LOG_TARGET, "Failed to send status reply");
        }
    }

    async fn partition_transactions_by_availability(
        pool: &Pool,
        hashes: Vec<Pool::TxHash>,
    ) -> Result<TransactionsByHashesResponse<Pool::Tx, Pool::TxHash>, MempoolError> {
        // Preserve the requested order. Block reconstruction recomputes the
        // content merkle root over the transaction sequence, so it only
        // matches the proposal's header when transactions come back in the
        // same order they were committed — collecting into a `BTreeSet` here
        // would re-sort by hash and break reconstruction.
        let items_stream = pool
            .get_items_by_keys(hashes.iter().cloned())
            .await
            .map_err(|e| {
                MempoolError::StorageError(format!("Failed to get items by keys: {e:?}"))
            })?;

        let found_transactions: Vec<Pool::Tx> = items_stream.collect().await;

        if found_transactions.len() == hashes.len() {
            return Ok(TransactionsByHashesResponse::new(
                found_transactions,
                BTreeSet::new(),
            ));
        }

        let found_hashes: BTreeSet<Pool::TxHash> =
            found_transactions.iter().map(Transaction::hash).collect();

        let not_found_hashes: BTreeSet<Pool::TxHash> = hashes
            .into_iter()
            .filter(|hash| !found_hashes.contains(hash))
            .collect();

        Ok(TransactionsByHashesResponse::new(
            found_transactions,
            not_found_hashes,
        ))
    }

    fn handle_add_success(
        pool: &Pool,
        state_updater: &MempoolStateUpdater<Pool, NetworkAdapter, RuntimeServiceId>,
        settings: NetworkAdapter::Settings,
        network_relay: OutboundRelay<BackendNetworkMsg<NetworkAdapter::Backend, RuntimeServiceId>>,
        item_for_broadcast: Pool::Tx,
        reply_channel: oneshot::Sender<Result<(), MempoolError>>,
    ) {
        state_updater.update(Some(<Pool as RecoverableMempool>::save(pool).into()));

        tokio::spawn(async move {
            let adapter = NetworkAdapter::new(settings, network_relay).await;
            adapter.send(item_for_broadcast).await;
        });

        if let Err(e) = reply_channel.send(Ok(())) {
            tracing::debug!(target: LOG_TARGET, "Failed to send add reply: {:?}", e);
        }
    }

    fn handle_add_error(
        error: MempoolError,
        reply_channel: oneshot::Sender<Result<(), MempoolError>>,
    ) {
        tracing::debug!(target: LOG_TARGET, "Could not add item to the pool: {}", error);
        if let Err(e) = reply_channel.send(Err(error)) {
            tracing::debug!(target: LOG_TARGET, "Failed to send error reply: {:?}", e);
        }
    }

    async fn handle_network_item(
        pool: &mut Pool,
        key: Pool::TxHash,
        item: Pool::Tx,
        state_updater: &MempoolStateUpdater<Pool, NetworkAdapter, RuntimeServiceId>,
        lifecycle_tracker: &mut TxLifecycleTracker<Pool::TxHash>,
    ) where
        Pool::Settings: Send + Sync,
        NetworkAdapter::Settings: Send + Sync,
        Pool::TxHash: Eq + Hash + Clone,
    {
        if let Err(e) = pool.add_item(item).await {
            match e {
                MempoolError::ExistingItem => {
                    lifecycle_tracker.record_seen_in_mempool(key, TxLifecycleSource::Gossip);
                    tracing::trace!(
                        target: LOG_TARGET,
                        "network item already exists in the mempool"
                    );
                }
                err => {
                    lifecycle_tracker.record_rejected(key, TxLifecycleSource::Gossip);
                    tracing::debug!(
                        target: LOG_TARGET,
                        "could not add item to the pool due to: {err}"
                    );
                }
            }
            return;
        }

        lifecycle_tracker.record_seen_in_mempool(key, TxLifecycleSource::Gossip);

        Self::log_mempool_pending_items(pool).await;

        state_updater.update(Some(<Pool as RecoverableMempool>::save(pool).into()));
    }

    async fn log_mempool_pending_items(pool: &Pool) {
        match pool.pending_item_count().await {
            Ok(pending_items) => {
                tracing::trace!(
                    target: LOG_TARGET,
                    {
                        counter.tx_mempool_pending_items = pending_items,
                    },
                    "mempool pending items updated"
                );
            }
            Err(e) => {
                tracing::debug!(target: LOG_TARGET, "failed to update mempool pending items: {e}");
            }
        }
    }
}
