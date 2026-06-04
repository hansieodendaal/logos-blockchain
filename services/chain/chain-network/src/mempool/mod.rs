use async_trait::async_trait;
use lb_core::mantle::TxHash;
use lb_tx_service::{MempoolMetrics, TransactionsByHashesResponse, TxLifecycleStatus};

pub mod adapter;

#[async_trait]
pub trait MempoolAdapter<Tx>: Send + Sync {
    async fn add_transaction(&self, tx: Tx) -> Result<(), overwatch::DynError>;

    async fn remove_transactions(&self, ids: &[TxHash]) -> Result<(), overwatch::DynError>;

    async fn classify_transactions(
        &self,
        hashes: Vec<TxHash>,
    ) -> Result<Vec<TxLifecycleStatus>, overwatch::DynError>;

    async fn metrics(&self) -> Result<MempoolMetrics, overwatch::DynError>;

    async fn get_transactions_by_hashes(
        &self,
        hashes: Vec<TxHash>,
    ) -> Result<TransactionsByHashesResponse<Tx, TxHash>, overwatch::DynError>;
}
