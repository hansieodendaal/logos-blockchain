use std::time::Duration;

use libp2p::PeerId;
use overwatch::services::relay::OutboundRelay;
use tokio::{runtime::RuntimeFlavor, time::timeout};

use crate::{BanStatus, BanningRequest};

/// Checks if a peer is currently banned by querying the banning subsystem via
/// the provided `banning_relay`. If `peer_id` is `None`, returns `false`.
#[must_use]
pub fn is_banned(
    banning_relay: &Option<OutboundRelay<BanningRequest>>,
    peer_id: Option<PeerId>,
) -> bool {
    if let Some(banning_relay) = banning_relay
        && let Some(peer_id) = peer_id
    {
        return block_on_now(async {
            let (tx, rx) = tokio::sync::oneshot::channel();
            // Treat failures as "not banned" (service is going down or relay closed)
            if banning_relay
                .send(BanningRequest::GetBanState { peer_id, reply: tx })
                .await
                .is_err()
            {
                return false;
            }
            let ban_status = timeout(Duration::from_secs(2), rx)
                .await
                .ok() // timeout -> None
                .and_then(Result::ok) // channel closed -> None
                .unwrap_or(BanStatus::NotBanned);
            matches!(ban_status, BanStatus::Banned { .. })
        });
    }
    false
}

// A helper function to block on a future, handling the case where we might
// already be inside a Tokio runtime (the default in most cases), and ensuring
// we don't deadlock or panic.
fn block_on_now<T>(fut: impl Future<Output = T>) -> T {
    if let Ok(handle) = tokio::runtime::Handle::try_current() {
        if handle.runtime_flavor() == RuntimeFlavor::MultiThread {
            // Current runtime is multi-threaded, we can block in place
            tokio::task::block_in_place(|| handle.block_on(fut))
        } else {
            // Current runtime is not multi-threaded, create a temporary multi-threaded
            // runtime
            let rt = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_time()
                .build()
                .expect("temp runtime");
            rt.block_on(fut)
        }
    } else {
        // We do not have a runtime available, create a new one
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .expect("temp runtime");
        rt.block_on(fut)
    }
}
