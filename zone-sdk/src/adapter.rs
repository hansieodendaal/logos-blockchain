use std::pin::Pin;

use async_trait::async_trait;
use futures::{Stream, StreamExt as _, stream};
use lb_common_http_client::{
    BlockFilter, BlockInfo, BlockSortOrder, BlocksRangeStreamParams, ChainServiceInfo,
    CommonHttpClient, Error, ProcessedBlockEvent, Slot,
};
use lb_core::{
    header::HeaderId,
    mantle::{Op, SignedMantleTx, ops::channel::ChannelId},
};
use reqwest::Url;

use crate::{Deposit, Withdraw, ZoneBlock, ZoneMessage};

/// A boxed, pinned, Send stream.
pub type BoxStream<T> = Pin<Box<dyn Stream<Item = T> + Send>>;

#[async_trait]
pub trait Node {
    async fn consensus_info(&self) -> Result<ChainServiceInfo, Error>;

    async fn block_stream(&self) -> Result<BoxStream<ProcessedBlockEvent>, Error>;

    async fn blocks_range_stream(
        &self,
        params: BlocksRangeStreamParams,
    ) -> Result<BoxStream<ProcessedBlockEvent>, Error>;

    async fn lib_stream(&self) -> Result<BoxStream<BlockInfo>, Error>;

    async fn zone_messages_in_block(
        &self,
        id: HeaderId,
        channel_id: ChannelId,
    ) -> Result<BoxStream<ZoneMessage>, Error>;

    async fn zone_messages_in_blocks(
        &self,
        slot_from: Slot,
        slot_to: Slot,
        channel_id: ChannelId,
    ) -> Result<BoxStream<(ZoneMessage, Slot)>, Error>;

    async fn post_transaction(&self, tx: SignedMantleTx) -> Result<(), Error>;
}

#[derive(Clone)]
pub struct NodeHttpClient {
    client: CommonHttpClient,
    base_url: Url,
}

impl NodeHttpClient {
    #[must_use]
    pub const fn new(client: CommonHttpClient, base_url: Url) -> Self {
        Self { client, base_url }
    }
}

#[async_trait]
impl Node for NodeHttpClient {
    async fn consensus_info(&self) -> Result<ChainServiceInfo, Error> {
        self.client.consensus_info(self.base_url.clone()).await
    }

    async fn block_stream(&self) -> Result<BoxStream<ProcessedBlockEvent>, Error> {
        let stream = self.client.get_blocks_stream(self.base_url.clone()).await?;
        Ok(Box::pin(stream))
    }

    async fn blocks_range_stream(
        &self,
        params: BlocksRangeStreamParams,
    ) -> Result<BoxStream<ProcessedBlockEvent>, Error> {
        let stream = self
            .client
            .get_blocks_range_stream(self.base_url.clone(), params)
            .await?;
        Ok(Box::pin(stream))
    }

    async fn lib_stream(&self) -> Result<BoxStream<BlockInfo>, Error> {
        let stream = self.client.get_lib_stream(self.base_url.clone()).await?;
        Ok(Box::pin(stream))
    }

    async fn zone_messages_in_block(
        &self,
        id: HeaderId,
        channel_id: ChannelId,
    ) -> Result<BoxStream<ZoneMessage>, Error> {
        let transactions = self
            .client
            .get_block_by_id(self.base_url.clone(), id)
            .await?
            .map_or_else(|| Vec::with_capacity(0), |block| block.transactions);

        Ok(Box::pin(stream::iter(
            transactions
                .into_iter()
                .flat_map(|tx| tx.mantle_tx.0)
                .filter_map(move |op| op_to_zone_message(&op, channel_id)),
        )))
    }

    async fn zone_messages_in_blocks(
        &self,
        slot_from: Slot,
        slot_to: Slot,
        channel_id: ChannelId,
    ) -> Result<BoxStream<(ZoneMessage, Slot)>, Error> {
        let messages = self
            .blocks_range_stream(BlocksRangeStreamParams {
                slot_from: Some(slot_from.into_inner()),
                slot_to: Some(slot_to.into_inner()),
                order: Some(BlockSortOrder::Ascending),
                blocks_limit: None,
                server_batch_size: None,
                block_filter: Some(BlockFilter::ImmutableOnly),
            })
            .await?
            .flat_map(move |event| {
                let slot = event.block.header.slot;
                stream::iter(
                    event
                        .block
                        .transactions
                        .into_iter()
                        .flat_map(|tx| tx.mantle_tx.0)
                        .filter_map(move |op| op_to_zone_message(&op, channel_id))
                        .map(move |msg| (msg, slot)),
                )
            });

        Ok(Box::pin(messages))
    }

    async fn post_transaction(&self, tx: SignedMantleTx) -> Result<(), Error> {
        self.client
            .post_transaction(self.base_url.clone(), tx)
            .await
    }
}

/// Converts [`Op`] to [`ZoneMessage`] if it belongs to the given channel.
///
/// Returns [`None`] if the op is not relevant for the channel.
fn op_to_zone_message(op: &Op, channel_id: ChannelId) -> Option<ZoneMessage> {
    match op {
        Op::ChannelInscribe(inscribe) if inscribe.channel_id == channel_id => {
            Some(ZoneMessage::Block(ZoneBlock {
                id: inscribe.id(),
                data: inscribe.inscription.clone(),
            }))
        }
        Op::ChannelDeposit(deposit) if deposit.channel_id == channel_id => {
            Some(ZoneMessage::Deposit(Deposit {
                inputs: deposit.inputs.clone(),
                metadata: deposit.metadata.clone(),
            }))
        }
        Op::ChannelWithdraw(withdraw) if withdraw.channel_id == channel_id => {
            Some(ZoneMessage::Withdraw(Withdraw {
                outputs: withdraw.outputs.clone(),
            }))
        }
        _ => None,
    }
}
