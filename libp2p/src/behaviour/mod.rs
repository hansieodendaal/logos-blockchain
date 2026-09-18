#![allow(
    clippy::multiple_inherent_impl,
    reason = "We split the `Behaviour` impls into different modules for better code modularity."
)]

use std::{
    error::Error,
    fmt,
    task::{Context, Poll},
};

use lb_cryptarchia_sync::ChainSyncError;
use libp2p::{
    Multiaddr, PeerId, StreamProtocol, autonat,
    core::{Endpoint, transport::PortUse},
    identify, identity, kad,
    swarm::{
        ConnectionDenied, ConnectionId, FromSwarm, NetworkBehaviour, THandler, THandlerInEvent,
        THandlerOutEvent, ToSwarm,
    },
};
use multiaddr::Protocol;
use rand::RngCore;
use thiserror::Error;

use crate::{
    IdentifySettings, KademliaSettings, NatSettings, behaviour::gossipsub::compute_message_id,
};

pub mod chainsync;
pub mod gossipsub;
pub mod kademlia;
pub mod nat;

const DATA_LIMIT: usize = 16 * 1024 * 1024; // 16 MiB (gossipsub default is 64 KiB)

pub(crate) struct BehaviourConfig {
    pub gossipsub_config: libp2p::gossipsub::Config,
    pub kademlia_config: KademliaSettings,
    pub identify_config: IdentifySettings,
    pub nat_config: NatSettings,
    pub kad_protocol_name: StreamProtocol,
    pub identify_protocol_name: StreamProtocol,
    pub chain_sync_protocol_name: StreamProtocol,
    pub public_key: identity::PublicKey,
    pub chain_sync_config: lb_cryptarchia_sync::Config,
    pub chain_sync_peer_block_predicate: Option<lb_cryptarchia_sync::PeerBlockPredicate>,
}

/// Common synchronous policy boundary for every protocol in the composite
/// behaviour.
///
/// It must not make network progress depend on an async banning-service
/// request.
pub struct GlobalPeerGate<B> {
    pub(crate) inner: B,
    block_predicate: Option<lb_cryptarchia_sync::PeerBlockPredicate>,
}

#[derive(Debug)]
pub(crate) struct GloballyBlockedPeer {
    pub(crate) peer_id: PeerId,
}

impl fmt::Display for GloballyBlockedPeer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "peer {} is globally blocked", self.peer_id)
    }
}

impl Error for GloballyBlockedPeer {}

impl<B> GlobalPeerGate<B> {
    pub(crate) fn new(
        inner: B,
        block_predicate: Option<lb_cryptarchia_sync::PeerBlockPredicate>,
    ) -> Self {
        Self {
            inner,
            block_predicate,
        }
    }

    fn check(&self, peer_id: PeerId) -> Result<(), ConnectionDenied> {
        if self
            .block_predicate
            .as_ref()
            .is_some_and(|predicate| predicate(peer_id))
        {
            return Err(ConnectionDenied::new(GloballyBlockedPeer { peer_id }));
        }

        Ok(())
    }
}

impl<B> NetworkBehaviour for GlobalPeerGate<B>
where
    B: NetworkBehaviour,
{
    type ConnectionHandler = B::ConnectionHandler;
    type ToSwarm = B::ToSwarm;

    fn handle_pending_inbound_connection(
        &mut self,
        connection_id: ConnectionId,
        local_addr: &Multiaddr,
        remote_addr: &Multiaddr,
    ) -> Result<(), ConnectionDenied> {
        self.inner
            .handle_pending_inbound_connection(connection_id, local_addr, remote_addr)
    }

    fn handle_established_inbound_connection(
        &mut self,
        connection_id: ConnectionId,
        peer: PeerId,
        local_addr: &Multiaddr,
        remote_addr: &Multiaddr,
    ) -> Result<THandler<Self>, ConnectionDenied> {
        self.check(peer)?;
        self.inner.handle_established_inbound_connection(
            connection_id,
            peer,
            local_addr,
            remote_addr,
        )
    }

    fn handle_pending_outbound_connection(
        &mut self,
        connection_id: ConnectionId,
        maybe_peer: Option<PeerId>,
        addresses: &[Multiaddr],
        effective_role: Endpoint,
    ) -> Result<Vec<Multiaddr>, ConnectionDenied> {
        if let Some(peer) = maybe_peer {
            self.check(peer)?;
        } else if let Some(peer) = addresses.iter().find_map(peer_id_from_multiaddr) {
            self.check(peer)?;
        }
        self.inner.handle_pending_outbound_connection(
            connection_id,
            maybe_peer,
            addresses,
            effective_role,
        )
    }

    fn handle_established_outbound_connection(
        &mut self,
        connection_id: ConnectionId,
        peer: PeerId,
        addr: &Multiaddr,
        role_override: Endpoint,
        port_use: PortUse,
    ) -> Result<THandler<Self>, ConnectionDenied> {
        self.check(peer)?;
        self.inner.handle_established_outbound_connection(
            connection_id,
            peer,
            addr,
            role_override,
            port_use,
        )
    }

    fn on_swarm_event(&mut self, event: FromSwarm) {
        self.inner.on_swarm_event(event);
    }

    fn on_connection_handler_event(
        &mut self,
        peer_id: PeerId,
        connection_id: ConnectionId,
        event: THandlerOutEvent<Self>,
    ) {
        self.inner
            .on_connection_handler_event(peer_id, connection_id, event);
    }

    fn poll(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<ToSwarm<Self::ToSwarm, THandlerInEvent<Self>>> {
        self.inner.poll(cx)
    }
}

fn peer_id_from_multiaddr(address: &Multiaddr) -> Option<PeerId> {
    address.iter().find_map(|protocol| match protocol {
        Protocol::P2p(multihash) => PeerId::from_multihash(multihash.into()).ok(),
        _ => None,
    })
}

#[derive(Debug, Error)]
pub enum BehaviourError {
    #[error("Operation not supported")]
    OperationNotSupported,
    #[error("Chainsync error: {0}")]
    ChainSyncError(#[from] ChainSyncError),
}

#[derive(NetworkBehaviour)]
pub struct Behaviour<Rng: Clone + Send + RngCore + 'static> {
    pub(crate) gossipsub: libp2p::gossipsub::Behaviour,
    // todo: support persistent store if needed
    pub(crate) kademlia: kad::Behaviour<kad::store::MemoryStore>,
    pub(crate) identify: identify::Behaviour,
    pub(crate) chain_sync: lb_cryptarchia_sync::Behaviour,
    // The spec makes it mandatory to run an autonat server for a public node.
    pub(crate) autonat_server: autonat::v2::server::Behaviour<Rng>,
    pub(crate) nat: nat::Behaviour<Rng>,
}

impl<Rng: Clone + Send + RngCore + 'static> Behaviour<Rng> {
    pub(crate) fn new(config: BehaviourConfig, rng: Rng) -> Result<Self, Box<dyn Error>> {
        let BehaviourConfig {
            gossipsub_config,
            kademlia_config,
            identify_config,
            chain_sync_config,
            nat_config,
            kad_protocol_name,
            identify_protocol_name,
            chain_sync_protocol_name,
            public_key,
            chain_sync_peer_block_predicate,
        } = config;

        let peer_id = PeerId::from(public_key.clone());

        let gossipsub = libp2p::gossipsub::Behaviour::new(
            libp2p::gossipsub::MessageAuthenticity::Author(peer_id),
            libp2p::gossipsub::ConfigBuilder::from(gossipsub_config)
                .validation_mode(libp2p::gossipsub::ValidationMode::None)
                .message_id_fn(compute_message_id)
                .max_transmit_size(DATA_LIMIT)
                .build()?,
        )?;

        let identify = identify::Behaviour::new(
            identify_config.to_libp2p_config(public_key, &identify_protocol_name),
        );

        let kademlia = kad::Behaviour::with_config(
            peer_id,
            kad::store::MemoryStore::new(peer_id),
            kademlia_config.to_libp2p_config(kad_protocol_name),
        );

        let autonat_server = autonat::v2::server::Behaviour::new(rng.clone());
        let nat = nat::Behaviour::new(rng, &nat_config);

        let chain_sync = lb_cryptarchia_sync::Behaviour::new_with_peer_block_predicate(
            chain_sync_protocol_name,
            chain_sync_config,
            chain_sync_peer_block_predicate,
        );

        Ok(Self {
            gossipsub,
            kademlia,
            identify,
            chain_sync,
            autonat_server,
            nat,
        })
    }
}
