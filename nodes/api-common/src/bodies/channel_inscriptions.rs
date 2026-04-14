use lb_core::{
    header::HeaderId,
    mantle::{
        ops::channel::{ChannelId, MsgId},
        tx::TxHash,
    },
};
use serde::{Deserialize, Serialize};

/// An item representing an inscription observed in a channel, along with its
/// context.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChannelInscriptionItem {
    /// The ID of the channel.
    pub channel_id: ChannelId,
    /// The ID of the block header.
    pub header_id: HeaderId,
    /// The ID of the parent block header that the inscription's message is
    /// included in.
    pub parent_block: HeaderId,
    /// The slot number of the block.
    pub slot: u64,
    /// The hash of the transaction.
    pub tx_hash: TxHash,
    /// The parent inscription message id.
    pub parent_msg_id: MsgId,
    /// The inscription's message id.
    pub this_msg_id: MsgId,
    /// The content of the inscription.
    pub inscription: Option<Vec<u8>>,
}

/// Response body for the channel inscriptions query, containing a list of
/// observed inscriptions and a cursor for pagination.
///
/// By default, list and stream APIs expose immutable history only. Clients can
/// request mutable canonical tail data with `include_mutable=true`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChannelInscriptionsResponseBody {
    /// A list of observed inscriptions in channels, along with their context.
    pub items: Vec<ChannelInscriptionItem>,
    /// Cursor to resume the same filtered query from the next item offset. When
    /// `None`, the current page is the last page.
    pub next_cursor: Option<u64>,
}

/// An event emitted in the channel inscriptions stream, either when a new
/// inscription is observed or when the lib advances.
///
/// Unless `include_mutable=true` is requested, inscription events are emitted
/// only when blocks become immutable.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "event", rename_all = "snake_case")]
pub enum ChannelInscriptionsStreamEvent {
    InscriptionObserved { item: ChannelInscriptionItem },
    LibAdvanced { tip: HeaderId, lib: HeaderId },
}
