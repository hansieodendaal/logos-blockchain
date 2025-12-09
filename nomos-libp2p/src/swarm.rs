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
use nomos_banning::{BanningRequest, banning_subscribe};
use overwatch::services::relay::OutboundRelay;
use rand::RngCore;
use std::collections::HashSet;
use std::time::Instant;
use std::{
    error::Error,
    io,
    net::Ipv4Addr,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};
use tokio::sync::broadcast;

// How long to keep a connection alive once it is idling.
const IDLE_CONN_TIMEOUT: Duration = Duration::from_secs(300);
// The maximum amount of time to spent draining banning events per poll to avoid starvation.
const BAN_DRAIN_TIME_BUDGET: Duration = Duration::from_millis(10);

/// Wraps [`libp2p::Swarm`], and config it for use within Nomos.
pub struct Swarm<R: Clone + Send + RngCore + 'static> {
    // A core libp2p swarm
    pub(crate) swarm: libp2p::Swarm<Behaviour<R>>,
    pub(crate) banning_events_rx: Option<broadcast::Receiver<nomos_banning::BanningEvent>>,
    pub(crate) banned_peers: HashSet<PeerId>,
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
            let banning_events_rx = banning_subscribe(&banning_relay)
                .inspect_err(|e| tracing::warn!("Could not subscribe to banning events: {e}"))
                .unwrap_or_default();
            let mut s = Self {
                swarm,
                banning_events_rx,
                banned_peers: Default::default(),
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
            self.poll_banning_events(None);
            if self.banned_peers.contains(peer_id) {
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

    // A light-weight version of poll_banning_events that limits the time spent processing
    // banning events to avoid starving the swarm event polling. Ban and unban events only change
    // the internal state of the swarm wrapper, so that targeted disconnects can be processed when
    // needed.
    fn poll_banning_events(&mut self, cx: Option<&Context<'_>>) {
        let Some(rx) = &mut self.banning_events_rx else {
            return;
        };

        let start = Instant::now();
        loop {
            if start.elapsed() >= BAN_DRAIN_TIME_BUDGET {
                // Re-schedule to continue processing remaining events later
                if let Some(context) = cx {
                    context.waker().wake_by_ref();
                }
                break;
            }

            match rx.try_recv() {
                Ok(event) => {
                    use nomos_banning::BanningEvent;
                    match event {
                        BanningEvent::Banned {
                            peer_id,
                            banned_until,
                            offense,
                            context,
                        } => {
                            tracing::debug!(
                                "Peer {peer_id} banned until {banned_until:?} for offense {offense:?} with context \
                                {context:?}"
                            );
                            self.banned_peers.insert(peer_id.clone());
                        }
                        BanningEvent::Unbanned { peer_id, .. } => {
                            tracing::debug!("Peer {peer_id} unbanned (event)");
                            self.banned_peers.remove(&peer_id);
                        }
                    }
                    // loop continues until time budget exhausted
                }
                Err(broadcast::error::TryRecvError::Empty) => break,
                Err(broadcast::error::TryRecvError::Lagged(_)) => continue,
                Err(broadcast::error::TryRecvError::Closed) => {
                    self.banning_events_rx = None;
                    break;
                }
            }
        }
    }
}

impl<R: Clone + Send + RngCore + 'static> futures::Stream for Swarm<R> {
    type Item = SwarmEvent<BehaviourEvent<R>>;

    // This polls the inner swarm, inspects connection events and disconnects peers flagged by
    // `is_banned`. Banned connection events are dropped and polling continues until a non-banned event
    // is returned or Pending.
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            self.poll_banning_events(Some(cx));

            match Pin::new(&mut self.swarm).poll_next(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Ready(Some(event)) => {
                    // Inspect connection/peer-related events and drop banned peers. Pattern matches
                    // are intentionally broad to handle common variants.
                    match &event {
                        SwarmEvent::ConnectionEstablished { peer_id, .. }
                        | SwarmEvent::Dialing {
                            peer_id: Some(peer_id),
                            ..
                        } => {
                            if self.banned_peers.contains(peer_id) {
                                // Disconnect the peer and drop this event.
                                let _ = self.swarm.disconnect_peer_id(peer_id.clone());
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
