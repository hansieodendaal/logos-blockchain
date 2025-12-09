//! Utility functions for the banning subsystem, providing a bridge/interface between synchronous
//!callers and the asynchronous banning service.

use std::time::Duration;

use crate::types::{BanningEvent, Violation};
use crate::{BanStatus, BanningRequest};
use libp2p::PeerId;
use overwatch::services::relay::OutboundRelay;
use tokio::sync::{broadcast, oneshot};
use tokio::{runtime::RuntimeFlavor, time::timeout};

/// Checks if a peer is currently banned by querying the banning subsystem via the provided
/// `banning_relay`. If `peer_id` is `None`, returns `false`.
#[must_use]
pub fn banning_is_banned(
    banning_relay: &Option<OutboundRelay<BanningRequest>>,
    peer_id: Option<&PeerId>,
) -> bool {
    if let Some(banning_relay) = banning_relay
        && let Some(peer_id) = peer_id
    {
        return block_on_now(async {
            let (tx, rx) = oneshot::channel();
            // Treat failures as "not banned" (service is going down or relay closed)
            if banning_relay
                .send(BanningRequest::GetBanState {
                    peer_id: peer_id.clone(),
                    reply: tx,
                })
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

/// Report a banning violation to get a peer banned using the banning subsystem via the provided
/// `banning_relay`. If `peer_id` is `None`, returns `false`.
#[must_use]
pub fn banning_ban_peer(
    banning_relay: &Option<OutboundRelay<BanningRequest>>,
    violation: Violation,
) -> Result<bool, overwatch::DynError> {
    if let Some(banning_relay) = banning_relay {
        return block_on_now(async {
            let (tx, rx) = oneshot::channel();
            if let Err(err) = banning_relay
                .send(BanningRequest::BanPeer {
                    violation,
                    reply: Some(tx),
                })
                .await
            {
                return Err(format!("Banning ban peer oneshot error: {:?}", err).into());
            }
            let ban_status = timeout(Duration::from_secs(2), rx)
                .await
                .ok() // timeout -> None
                .and_then(Result::ok) // channel closed -> None
                .unwrap_or(BanStatus::NotBanned);
            Ok(matches!(ban_status, BanStatus::Banned { .. }))
        });
    }
    Ok(false)
}

/// Unban a peer using the banning subsystem via the provided `banning_relay`. If `peer_id` is
/// `None`, returns `false`.
#[must_use]
pub fn banning_unban_peer(
    banning_relay: &Option<OutboundRelay<BanningRequest>>,
    peer_id: &PeerId,
) -> Result<bool, overwatch::DynError> {
    if let Some(banning_relay) = banning_relay {
        return block_on_now(async {
            let (tx, rx) = oneshot::channel();
            if let Err(err) = banning_relay
                .send(BanningRequest::UnbanPeer {
                    peer_id: peer_id.clone(),
                    reply: tx,
                })
                .await
            {
                return Err(format!("Banning unban peer oneshot error: {:?}", err).into());
            }
            let unbanned = timeout(Duration::from_secs(2), rx)
                .await
                .ok() // timeout -> None
                .and_then(Result::ok) // channel closed -> None
                .unwrap_or_default();
            Ok(unbanned)
        });
    }
    Ok(false)
}

/// Subscribe to banning events using the banning subsystem via the provided `banning_relay`.
#[must_use]
pub fn banning_subscribe(
    banning_relay: &Option<OutboundRelay<BanningRequest>>,
) -> Result<Option<broadcast::Receiver<BanningEvent>>, overwatch::DynError> {
    if let Some(banning_relay) = banning_relay {
        block_on_now(async {
            let (tx, rx) = oneshot::channel();
            match banning_relay
                .send(BanningRequest::Subscribe { reply: tx })
                .await
            {
                Ok(_) => match timeout(Duration::from_secs(2), rx).await {
                    Ok(Ok(val)) => Ok(Some(val)),
                    Ok(Err(err)) => Err(format!("Banning subscribe oneshot error: {}", err).into()),
                    Err(_) => Err("Timeout while subscribing to banning events".into()),
                },
                Err(err) => Err(format!(
                    "Banning relay send error while subscribing to banning events: {:?}",
                    err
                )
                .into()),
            }
        })
    } else {
        Ok(None)
    }
}

/// A helper function to block on a future, handling the case where we might already be inside a
/// Tokio runtime (the default in most cases), and ensuring we don't deadlock or panic.
pub fn block_on_now<T>(fut: impl Future<Output = T>) -> T {
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
