use lb_core::{
    header::HeaderId,
    mantle::{Transaction, TxHash},
};
use lb_tx_service::{
    MempoolMetrics, MempoolMsg, MempoolRemoveReason, TransactionsByHashesResponse,
    TxLifecycleStatus,
};
use overwatch::services::relay::OutboundRelay;
use tokio::sync::oneshot;

use super::MempoolAdapter as MempoolAdapterTrait;

#[derive(Clone)]
pub struct MempoolAdapter<Tx> {
    mempool_relay: OutboundRelay<MempoolMsg<HeaderId, Tx, TxHash>>,
}

impl<Tx> MempoolAdapter<Tx> {
    #[must_use]
    pub const fn new(mempool_relay: OutboundRelay<MempoolMsg<HeaderId, Tx, TxHash>>) -> Self {
        Self { mempool_relay }
    }
}

#[async_trait::async_trait]
impl<Tx> MempoolAdapterTrait<Tx> for MempoolAdapter<Tx>
where
    Tx: Transaction<Hash = TxHash> + Send + Sync + 'static,
{
    async fn add_transaction(&self, tx: Tx) -> Result<(), overwatch::DynError> {
        let (reply_sender, reply_receiver) = oneshot::channel();
        self.mempool_relay
            .send(MempoolMsg::Add {
                key: tx.hash(),
                payload: tx,
                reply_channel: reply_sender,
            })
            .await
            .map_err(|(e, _)| format!("Could not add transactions to mempool: {e}"))?;
        reply_receiver
            .await
            .map_err(|e| format!("Could not receive response: {e}"))?
            .map_err(|e| format!("Mempool error: {e}"))?;
        Ok(())
    }

    async fn remove_transactions(&self, ids: &[TxHash]) -> Result<(), overwatch::DynError> {
        self.mempool_relay
            .send(MempoolMsg::Remove {
                ids: ids.to_vec(),
                reason: MempoolRemoveReason::CanonicalBlockApplied,
            })
            .await
            .map_err(|(e, _)| format!("Could not remove transactions from mempool: {e}"))?;

        Ok(())
    }

    async fn classify_transactions(
        &self,
        hashes: Vec<TxHash>,
    ) -> Result<Vec<TxLifecycleStatus>, overwatch::DynError> {
        let (resp_tx, resp_rx) = oneshot::channel();

        self.mempool_relay
            .send(MempoolMsg::ClassifyTransactions {
                hashes,
                reply_channel: resp_tx,
            })
            .await
            .map_err(|(e, _)| format!("Could not classify transactions by hashes: {e}"))?;

        resp_rx
            .await
            .map_err(|e| format!("Could not receive classification response: {e}").into())
    }

    async fn metrics(&self) -> Result<MempoolMetrics, overwatch::DynError> {
        let (resp_tx, resp_rx) = oneshot::channel();

        self.mempool_relay
            .send(MempoolMsg::Metrics {
                reply_channel: resp_tx,
            })
            .await
            .map_err(|(e, _)| format!("Could not get mempool metrics: {e}"))?;

        resp_rx
            .await
            .map_err(|e| format!("Could not receive metrics response: {e}").into())
    }

    async fn get_transactions_by_hashes(
        &self,
        hashes: Vec<TxHash>,
    ) -> Result<TransactionsByHashesResponse<Tx, TxHash>, overwatch::DynError> {
        let (resp_tx, resp_rx) = oneshot::channel();

        self.mempool_relay
            .send(MempoolMsg::GetTransactionsByHashes {
                hashes,
                reply_channel: resp_tx,
            })
            .await
            .map_err(|(e, _)| format!("Could not get transactions by hashes: {e}"))?;

        let response = resp_rx
            .await
            .map_err(|e| format!("Could not receive response: {e}"))?;

        Ok(response.map_err(|e| format!("Mempool error: {e}"))?)
    }
}
