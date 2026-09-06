use core::fmt::{self, Debug, Formatter};

use async_trait::async_trait;
use futures::Stream;
use lb_blend::scheduling::message_blend::provers::WinningPolInfoStream;
use lb_core::header::HeaderId;
use lb_cryptarchia_engine::{Epoch, Slot};
use lb_groth16::Fr;
use overwatch::overwatch::OverwatchHandle;

/// Private `PoL` information for an epoch, as returned by the `PoL` info
/// provider.
///
/// `state` identifies the chain-derived epoch state against which the lazy
/// winning-slot stream was constructed. The stream carries the secret inputs
/// for winning slots and is consumed lazily.
pub struct PolEpochInfo {
    pub epoch: Epoch,
    pub state: PolEpochState,
    /// The stream of `PoL` secret inputs for the slots found to be winning in
    /// this epoch.
    pub winning_pol_info_stream: WinningPolInfoStream,
}

/// Chain-derived state associated with an epoch's private `PoL` information.
pub struct PolEpochState {
    pub nonce: Fr,
    pub aged_utxo_root: Fr,
    pub lottery_0: Fr,
    pub lottery_1: Fr,
    pub source: PolEpochStateSource,
}

/// Provenance of the chain-derived state associated with an epoch's private
/// `PoL` information.
pub struct PolEpochStateSource {
    pub tip_id: HeaderId,
    pub tip_slot: Slot,
    pub lib_id: HeaderId,
    pub lib_slot: Slot,
}

impl Debug for PolEpochInfo {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("PolEpochInfo")
            .field("epoch", &self.epoch)
            .finish_non_exhaustive()
    }
}

#[async_trait]
pub trait PolInfoProvider<RuntimeServiceId> {
    type Stream: Stream<Item = PolEpochInfo>;

    async fn subscribe(
        overwatch_handle: &OverwatchHandle<RuntimeServiceId>,
    ) -> Option<Self::Stream>;
}
