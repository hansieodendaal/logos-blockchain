use std::{
    collections::{BTreeSet, HashMap},
    fmt::Debug,
    hash::Hash,
    marker::PhantomData,
    pin::Pin,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use futures::{Stream, StreamExt as _, stream};
use lb_chain_service::{LibUpdate, ProcessedBlockEvent};
use lb_core::{
    header::HeaderId,
    mantle::{Transaction, TxDependencies, TxPriorityTip},
};
use lb_log_targets::mempool;
use serde::{Deserialize, Serialize};
use tracing::error;

use super::Status;
use crate::{
    backend::{
        MemPool, MempoolError, RecoverableMempool,
        forks::{BlockInfoGetter, ForksTracker, ForksTrackerState, LedgerStateGetter},
    },
    metrics::{
        mempool_transactions_added, mempool_transactions_pending, mempool_transactions_removed,
    },
    storage::MempoolStorageAdapter,
};

const LOG_TARGET: &str = mempool::POOL;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PoolRecoveryState<Key>
where
    Key: Hash + Eq + Ord,
{
    pub forks_state: ForksTrackerState<Key>,
    pub last_item_timestamp: u64,
}

pub struct Mempool<Tx, TxHash, Adapter, RuntimeServiceId>
where
    TxHash: Eq + Hash,
{
    last_item_timestamp: u64,
    forks_tracker: ForksTracker<Tx, TxHash, Adapter>,
    adapter: Adapter,
    _phantom: PhantomData<RuntimeServiceId>,
}

impl<Tx, TxHash, Adapter, RuntimeServiceId> Debug for Mempool<Tx, TxHash, Adapter, RuntimeServiceId>
where
    TxHash: Eq + Hash + Debug,
    Tx: Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Mempool")
            .field("last_item_timestamp", &self.last_item_timestamp)
            .field("storage_adapter", &"<StorageAdapter>")
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl<Tx, Adapter, RuntimeServiceId> MemPool for Mempool<Tx, Tx::Hash, Adapter, RuntimeServiceId>
where
    Tx: TxDependencies
        + TxPriorityTip
        + Clone
        + Send
        + Sync
        + 'static
        + Serialize
        + for<'de> Deserialize<'de>,
    <Tx as Transaction>::Hash: Hash + Eq + Ord + Clone + Send + Sync + 'static,
    Adapter: MempoolStorageAdapter<RuntimeServiceId, Tx = Tx> + Send + Sync + 'static,
    Adapter: BlockInfoGetter<Tx> + LedgerStateGetter + Clone,
    <Adapter as MempoolStorageAdapter<RuntimeServiceId>>::Error: Debug,
    RuntimeServiceId: Send + Sync,
{
    type Settings = ();
    type Tx = Tx;
    type TxHash = Tx::Hash;
    type BlockId = HeaderId;
    type Adapter = Adapter;

    fn new(_settings: Self::Settings, adapter: Self::Adapter) -> Self {
        Self {
            last_item_timestamp: 0,
            adapter: adapter.clone(),
            forks_tracker: ForksTracker::new(adapter),
            _phantom: PhantomData,
        }
    }

    async fn add_item<I: Into<Self::Tx> + Send>(&mut self, item: I) -> Result<(), MempoolError> {
        let tx = item.into();
        self.adapter
            .store_tx(tx.clone())
            .await
            .map_err(|e| MempoolError::StorageError(format!("{e:?}")))?;
        self.forks_tracker.process_new_tx(&tx).await;
        self.last_item_timestamp = current_timestamp_millis();
        let pending_items = self.pending_item_count().await.unwrap_or_else(|e| {
            tracing::error!(target: LOG_TARGET, "Failed to get pending item count: {}", e);
            usize::MAX
        });
        mempool_transactions_added();
        mempool_transactions_pending(pending_items);
        Ok(())
    }

    async fn view(
        &self,
        ancestor_hint: HeaderId,
    ) -> Result<Pin<Box<dyn Stream<Item = Self::Tx> + Send>>, MempoolError> {
        Ok(Box::pin(stream::iter(
            self.forks_tracker
                .get_frontier_txs(ancestor_hint)
                .await
                .map_err(|e| MempoolError::DynamicPoolError(Box::new(e)))?,
        )))
    }

    async fn get_items_by_keys<I>(
        &self,
        keys: I,
    ) -> Result<Pin<Box<dyn Stream<Item = Self::Tx> + Send>>, MempoolError>
    where
        I: IntoIterator<Item = Self::TxHash> + Send,
        <I as IntoIterator>::IntoIter: Send,
    {
        let keys_set: BTreeSet<Self::TxHash> = keys.into_iter().collect();
        self.adapter
            .get_txs(&keys_set)
            .await
            .map_err(|e| MempoolError::StorageError(format!("{e:?}")))
    }

    async fn remove(&mut self, keys: &[Self::TxHash]) {
        let removed_count = self.forks_tracker.force_remove_txs(keys);
        let pending_items = self.pending_item_count().await.unwrap_or_else(|e| {
            tracing::error!(target: LOG_TARGET, "Failed to get pending item count: {}", e);
            usize::MAX
        });
        log_removed_items(removed_count, pending_items);
        mempool_transactions_removed(removed_count);
        mempool_transactions_pending(pending_items);
    }

    async fn pending_item_count(&self) -> Result<usize, MempoolError> {
        let tip = self
            .adapter
            .get_tip_id()
            .await
            .map_err(|e| MempoolError::DynamicPoolError(Box::new(e)))?;
        self.forks_tracker
            .pending_item_count(tip)
            .map_err(|e| MempoolError::DynamicPoolError(Box::new(e)))
    }

    fn last_item_timestamp(&self) -> u64 {
        self.last_item_timestamp
    }

    fn status(&self, _items: &[Self::TxHash]) -> Vec<Status> {
        // TODO: were to check from here? Canonical chain? or change to query by tip?
        vec![]
    }

    async fn process_new_block_event(&mut self, event: ProcessedBlockEvent) {
        if let Err(e) = self.forks_tracker.process_new_block(&event).await {
            error!("Failed to process new block event: {e:?}");
        }
    }

    fn process_lib_event(&mut self, event: LibUpdate) {
        self.forks_tracker.process_lib(&event);
    }
}

impl<Tx, Adapter, RuntimeServiceId> RecoverableMempool
    for Mempool<Tx, Tx::Hash, Adapter, RuntimeServiceId>
where
    Tx::Hash:
        Hash + Eq + Ord + Clone + Send + Sync + 'static + Serialize + for<'de> Deserialize<'de>,
    Tx: TxDependencies
        + TxPriorityTip
        + Clone
        + Send
        + Sync
        + 'static
        + Serialize
        + for<'de> Deserialize<'de>,
    Adapter: MempoolStorageAdapter<RuntimeServiceId, Tx = Tx>
        + BlockInfoGetter<Tx>
        + LedgerStateGetter
        + Clone
        + Send
        + Sync
        + 'static,
    <Adapter as MempoolStorageAdapter<RuntimeServiceId>>::Error: Debug,
    RuntimeServiceId: Send + Sync,
{
    type RecoveryState = PoolRecoveryState<Tx::Hash>;

    fn save(&self) -> Self::RecoveryState {
        PoolRecoveryState {
            forks_state: self.forks_tracker.to_state(),
            last_item_timestamp: self.last_item_timestamp,
        }
    }

    fn recover(
        _settings: <Self as MemPool>::Settings,
        state: Self::RecoveryState,
        adapter: <Self as MemPool>::Adapter,
    ) -> Self {
        let recover_txs = state.forks_state.recover_txs();
        let fetch_adapter = adapter.clone();
        // not beautiful but we need to collect the txs from storage and the interface
        // is async
        let txs: HashMap<Tx::Hash, Arc<Tx>> =
            tokio::runtime::Handle::current().block_on(async move {
                let stream = fetch_adapter
                    .get_txs(&recover_txs)
                    .await
                    .unwrap_or_else(|e| panic!("Could not recover txs from storage: {e:?}"));
                stream.map(|tx| (tx.hash(), Arc::new(tx))).collect().await
            });
        Self {
            last_item_timestamp: state.last_item_timestamp,
            adapter: adapter.clone(),
            forks_tracker: ForksTracker::from_state_and_adapter(state.forks_state, &txs, adapter),
            _phantom: PhantomData,
        }
    }
}

fn current_timestamp_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

fn log_removed_items(removed_count: usize, pending_items: usize) {
    if removed_count == 0 {
        tracing::trace!(
            target: LOG_TARGET,
            "Removed {removed_count} items from mempool; pending_items={pending_items}"
        );
    } else {
        tracing::debug!(
            target: LOG_TARGET,
            "Removed {removed_count} items from mempool; pending_items={pending_items}"
        );
    }
}
