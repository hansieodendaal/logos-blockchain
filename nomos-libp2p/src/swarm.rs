#![allow(
    clippy::multiple_inherent_impl,
    reason = "We split the `Swarm` impls into different modules for better code modularity."
)]

use crate::behaviour::BehaviourConfig;
pub use crate::{
    SwarmConfig,
    behaviour::{Behaviour, BehaviourEvent},
};
use libp2p::{
    Multiaddr, PeerId, TransportError,
    identity::ed25519,
    swarm::{ConnectionId, DialError, SwarmEvent, dial_opts::DialOpts},
};
use multiaddr::multiaddr;
use nomos_banning::{
    BanningEvent, BanningRequest, banning_list_active_bans, banning_subscribe, block_on_now,
};
use overwatch::services::relay::OutboundRelay;
use rand::RngCore;
use std::collections::HashSet;
use std::sync::{
    Arc, Mutex, RwLock,
    atomic::{AtomicBool, Ordering},
};
use std::time::Instant;
use std::{
    error::Error,
    io,
    net::Ipv4Addr,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};
use tokio::sync::{Notify, broadcast};

// How long to keep a connection alive once it is idling.
const IDLE_CONN_TIMEOUT: Duration = Duration::from_secs(300);
// The maximum amount of time to spent draining banning events per poll to avoid starvation.
const BAN_DRAIN_TIME_BUDGET: Duration = Duration::from_millis(10);

/// Wraps [`libp2p::Swarm`], and config it for use within Nomos.
pub struct Swarm<R: Clone + Send + RngCore + 'static> {
    // A core libp2p swarm
    pub(crate) swarm: libp2p::Swarm<Behaviour<R>>,
    pub(crate) banning_relay: Option<OutboundRelay<BanningRequest>>,
    pub(crate) banning_events_rx: Arc<Mutex<Option<broadcast::Receiver<BanningEvent>>>>,
    pub(crate) banned_peers: Arc<RwLock<HashSet<PeerId>>>,
    pub(crate) banned_peers_background_sync_in_progress: Arc<AtomicBool>,
    pub(crate) banned_peers_background_sync_notify: Arc<Notify>,
}

impl<R: Clone + Send + RngCore + 'static> Swarm<R> {
    /// Builds a [`Swarm`] configured for use with Nomos on top of a tokio
    /// executor.
    pub fn build(
        config: SwarmConfig,
        rng: R,
        banning_relay: Option<OutboundRelay<BanningRequest>>,
    ) -> Result<Self, Box<dyn Error>> {
        let keypair =
            libp2p::identity::Keypair::from(ed25519::Keypair::from(config.node_key.clone()));
        let peer_id = PeerId::from(keypair.public());
        tracing::info!("libp2p peer_id:{}", peer_id);

        let SwarmConfig {
            gossipsub_config,
            kademlia_config,
            identify_config,
            chain_sync_config,
            nat_config,
            identify_protocol_name,
            kad_protocol_name,
            host,
            port,
            ..
        } = config;

        let swarm = libp2p::SwarmBuilder::with_existing_identity(keypair)
            .with_tokio()
            .with_quic()
            .with_dns()?
            .with_behaviour(move |keypair| {
                Behaviour::new(
                    BehaviourConfig {
                        gossipsub_config,
                        kademlia_config: kademlia_config.clone(),
                        identify_config,
                        nat_config,
                        kad_protocol_name: kad_protocol_name.into(),
                        identify_protocol_name: identify_protocol_name.into(),
                        public_key: keypair.public(),
                        chain_sync_config,
                    },
                    rng,
                )
                .expect("Behaviour should not fail to set up.")
            })?
            .with_swarm_config(|c| c.with_idle_connection_timeout(IDLE_CONN_TIMEOUT))
            .build();

        let nomos_swarm = {
            let listen_addr = multiaddr(host, port);
            let banning_events_rx = Arc::new(Mutex::new(
                banning_subscribe(&banning_relay)
                    .inspect_err(|e| tracing::warn!("Could not subscribe to banning events: {e}"))
                    .unwrap_or_default(),
            ));
            let mut s = Self {
                swarm,
                banning_relay,
                banning_events_rx,
                banned_peers: Arc::new(RwLock::new(HashSet::default())),
                banned_peers_background_sync_in_progress: Arc::new(AtomicBool::new(false)),
                banned_peers_background_sync_notify: Arc::new(Notify::new()),
            };
            // We start listening on the provided address, which triggers the Identify flow,
            // which in turn triggers our NAT traversal state machine.
            s.start_listening_on(listen_addr.clone())
                .map_err(|e| format!("Failed to listen on {listen_addr}: {e}"))?;
            Ok::<_, Box<dyn Error>>(s)
        }?;

        Ok(nomos_swarm)
    }

    /// Initiates a connection attempt to a peer
    pub fn connect(&mut self, peer_addr: &Multiaddr) -> Result<ConnectionId, DialError> {
        let opt = DialOpts::from(peer_addr.clone());
        if let Some(peer_id) = opt.get_peer_id().as_ref() {
            let (mut banned_peers, lagged) = self.update_banned_peer_list(None);
            if lagged && self.wait_for_banned_peers_background_sync(Duration::from_millis(100)) {
                banned_peers = self.banned_peers.read().unwrap().iter().copied().collect();
            }
            if banned_peers.contains(peer_id) {
                return Err(DialError::Transport(vec![(
                    peer_addr.clone(),
                    TransportError::Other(io::Error::other("Attempted to dial a banned peer")),
                )]));
            }
        }
        let connection_id = opt.connection_id();

        tracing::debug!("attempting to dial {peer_addr}. connection_id:{connection_id:?}");
        self.swarm.dial(opt)?;
        Ok(connection_id)
    }

    pub fn start_listening_on(&mut self, addr: Multiaddr) -> Result<(), TransportError<io::Error>> {
        self.swarm.listen_on(addr)?;
        Ok(())
    }

    /// Returns a reference to the underlying [`libp2p::Swarm`]
    pub const fn swarm(&self) -> &libp2p::Swarm<Behaviour<R>> {
        &self.swarm
    }

    // This is a light-weight banned event polling implementation that limits time spent processing
    // banning events to avoid starving the swarm event polling. Acting on ban and unban events only
    // change the internal state of the swarm wrapper, so that targeted disconnects can be processed
    // when needed.
    // This function returns the current list of banned peers after processing available events as
    // a 'Vec<PeerId>', also indicating if the subscriber events channel lagged. In normal operation
    // we expect this list to be small or empty, so copying into a Vec is a cheap overhead.
    fn update_banned_peer_list(&self, cx: Option<&Context<'_>>) -> (Vec<PeerId>, bool) {
        let mut banning_events_rx = self.banning_events_rx.lock().unwrap();
        let Some(rx) = banning_events_rx.as_mut() else {
            return (Vec::default(), false);
        };

        let start = Instant::now();
        loop {
            if start.elapsed() >= BAN_DRAIN_TIME_BUDGET {
                // Re-schedule to continue processing remaining events later
                if let Some(context) = cx {
                    context.waker().wake_by_ref();
                }
                break (self.snapshot_banned_peers(), false);
            }

            match rx.try_recv() {
                Ok(event) => self.process_banning_event(event),
                Err(broadcast::error::TryRecvError::Empty) => {
                    break (self.snapshot_banned_peers(), false);
                }
                Err(broadcast::error::TryRecvError::Lagged(skipped)) => {
                    tracing::warn!(
                        "We missed {skipped} banning events, refreshing the banned peer list."
                    );
                    // Drop the mutex guard before doing the potentially-blocking work
                    drop(banning_events_rx);
                    self.background_refresh_active_bans_from_relay();
                    // Ensure we re-poll so updated bans are acted on (disconnects in poll_next).
                    if let Some(context) = cx {
                        context.waker().wake_by_ref();
                    }
                    break (self.snapshot_banned_peers(), true);
                }
                Err(broadcast::error::TryRecvError::Closed) => {
                    *banning_events_rx = None;
                    drop(banning_events_rx);
                    break (self.snapshot_banned_peers(), false);
                }
            }
        }
    }

    // Take a snapshot of the current banned peer list.
    fn snapshot_banned_peers(&self) -> Vec<PeerId> {
        let banned_peers = self.banned_peers.read().unwrap();
        banned_peers.iter().copied().collect()
    }

    // Process a single banning event, updating the internal banned peer list accordingly.
    fn process_banning_event(&self, event: BanningEvent) {
        match event {
            BanningEvent::Banned {
                peer_id,
                banned_until,
                offense,
                context,
            } => {
                tracing::debug!(
                    "Peer {peer_id} banned until {banned_until:?} for offense {offense:?} with \
                    context {context:?}"
                );
                let mut banned_peers = self.banned_peers.write().unwrap();
                banned_peers.insert(peer_id);
            }
            BanningEvent::Unbanned { peer_id, .. } => {
                tracing::debug!("Peer {peer_id} unbanned (event)");
                let mut banned_peers = self.banned_peers.write().unwrap();
                banned_peers.remove(&peer_id);
            }
        }
    }

    // This will spawn a background task to refresh the active bans from the banning relay, and
    // should only be called if reading banning events lagged. The authoritive state will be seen
    // the next time `update_banned_peer_list` is called.
    fn background_refresh_active_bans_from_relay(&self) {
        if self
            .banned_peers_background_sync_in_progress
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return;
        }

        let banning_relay_clone = self.banning_relay.clone();
        let banned_peers_clone = Arc::clone(&self.banned_peers);
        let in_progress = Arc::clone(&self.banned_peers_background_sync_in_progress);
        let notify = Arc::clone(&self.banned_peers_background_sync_notify);
        tokio::spawn(async move {
            // This will tiemout internally if the banning service is unresponsive.
            match banning_list_active_bans(&banning_relay_clone) {
                Ok(authoritative_banned_peer_list) => {
                    let mut banned_peers = banned_peers_clone.write().unwrap();
                    banned_peers.clear();
                    for (peer_id, _) in authoritative_banned_peer_list {
                        banned_peers.insert(peer_id);
                    }
                }
                Err(err) => {
                    tracing::warn!("Error retrieving banned peer list: {err}");
                }
            }
            in_progress.store(false, Ordering::SeqCst);
            notify.notify_waiters();
        });
    }

    // Wait for any in-progress banned peer background sync to complete, up to the provided timeout.
    // Returns 'true' if the sync completed, 'false' if timed out.
    fn wait_for_banned_peers_background_sync(&self, timeout: Duration) -> bool {
        let in_progress = &self.banned_peers_background_sync_in_progress;
        block_on_now(async {
            let wait_future = async {
                while self.banned_peers_background_sync_in_progress.load(Ordering::SeqCst)
                {
                    self.banned_peers_background_sync_notify.notified().await;
                }
            };
            let _ = tokio::time::timeout(timeout, wait_future).await;
            !in_progress.load(Ordering::SeqCst)
        })
    }
}

impl<R: Clone + Send + RngCore + 'static> futures::Stream for Swarm<R> {
    type Item = SwarmEvent<BehaviourEvent<R>>;

    // This polls the inner swarm, inspects connection events and disconnects peers flagged by
    // `is_banned`. Banned connection events are dropped and polling continues until a non-banned event
    // is returned or Pending.
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            // If getting the banned peer list lagged, we ignore that for this poll, as the background
            // refresh will update the list soon enough.
            let (banned_peers, _) = self.update_banned_peer_list(Some(cx));

            match Pin::new(&mut self.swarm).poll_next(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Ready(Some(event)) => {
                    // Inspect connection/peer-related events and drop banned peers. Pattern matches
                    // are intentionally broad to handle common variants.
                    match &event {
                        SwarmEvent::ConnectionEstablished { peer_id, .. }
                        | SwarmEvent::NewExternalAddrOfPeer { peer_id, .. }
                        | SwarmEvent::Dialing {
                            peer_id: Some(peer_id),
                            ..
                        } => {
                            if banned_peers.contains(peer_id) {
                                // Disconnect the peer and drop this event.
                                let _ = self.swarm.disconnect_peer_id(*peer_id);
                                // Continue the loop to poll for the next event
                                continue;
                            }
                        }
                        SwarmEvent::IncomingConnection { send_back_addr, .. } => {
                            let opt = DialOpts::from(send_back_addr.clone());
                            if let Some(peer_id) = opt.get_peer_id().as_ref()
                                && banned_peers.contains(peer_id)
                            {
                                // Disconnect the peer and drop this event.
                                let _ = self.swarm.disconnect_peer_id(*peer_id);
                                // Continue the loop to poll for the next event
                                continue;
                            }
                        }
                        _ => {}
                    }

                    // Not banned (or not a connection event we care about) — forward it.
                    return Poll::Ready(Some(event));
                }
            }
        }
    }
}

#[must_use]
pub fn multiaddr(ip: Ipv4Addr, port: u16) -> Multiaddr {
    multiaddr!(Ip4(ip), Udp(port), QuicV1)
}
