use lb_core::{
    header::HeaderId,
    mantle::{
        ops::channel::{ChannelId, MsgId},
        tx::TxHash,
    },
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChannelInscriptionItem {
    pub channel_id: ChannelId,
    pub header_id: HeaderId,
    pub parent_block: HeaderId,
    pub slot: u64,
    pub tx_hash: TxHash,
    pub parent_msg_id: MsgId,
    pub this_msg_id: MsgId,
    pub inscription: Option<Vec<u8>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChannelInscriptionsResponseBody {
    pub items: Vec<ChannelInscriptionItem>,
    pub next_cursor: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "event", rename_all = "snake_case")]
pub enum ChannelInscriptionsStreamEvent {
    InscriptionObserved { item: ChannelInscriptionItem },
    LibAdvanced { tip: HeaderId, lib: HeaderId },
}
