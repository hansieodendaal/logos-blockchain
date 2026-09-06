use async_trait::async_trait;
use futures::{
    Stream,
    future::ready,
    stream::{once, repeat},
};
use lb_blend::proofs::quota::inputs::prove::private::ProofOfLeadershipQuotaInputs;
use lb_chain_service::{Epoch, Slot};
use lb_core::{crypto::ZkHash, header::HeaderId};
use lb_groth16::{AdditiveGroup as _, Fr};
use overwatch::overwatch::OverwatchHandle;

use crate::epoch_info::{PolEpochInfo, PolEpochState, PolEpochStateSource, PolInfoProvider};

pub struct OncePolStreamProvider;

#[async_trait]
impl<RuntimeServiceId> PolInfoProvider<RuntimeServiceId> for OncePolStreamProvider {
    type Stream = Box<dyn Stream<Item = PolEpochInfo> + Send + Unpin>;

    async fn subscribe(
        _overwatch_handle: &OverwatchHandle<RuntimeServiceId>,
    ) -> Option<Self::Stream> {
        Some(Box::new(once(ready(PolEpochInfo {
            epoch: Epoch::new(0),
            state: PolEpochState {
                nonce: Fr::ZERO,
                aged_utxo_root: Fr::ZERO,
                lottery_0: Fr::ZERO,
                lottery_1: Fr::ZERO,
                source: PolEpochStateSource {
                    tip_id: HeaderId::from([0; 32]),
                    tip_slot: Slot::from(0),
                    lib_id: HeaderId::from([0; 32]),
                    lib_slot: Slot::from(0),
                },
            },
            winning_pol_info_stream: Box::pin(repeat(ProofOfLeadershipQuotaInputs {
                slot: 1,
                note_value: 1,
                transaction_hash: ZkHash::ZERO,
                output_number: 1,
                aged_path_and_selectors: [(ZkHash::ZERO, false); _],
                secret_key: ZkHash::ZERO,
            })),
        }))))
    }
}
