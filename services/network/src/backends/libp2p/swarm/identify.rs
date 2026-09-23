use std::collections::HashSet;

use lb_libp2p::{Multiaddr, PeerId, Protocol, libp2p::identify};
use lb_log_targets::network_service;
use rand::RngCore;

use crate::backends::libp2p::swarm::{ProtocolContract, SwarmHandler};

const LOG_TARGET: &str = network_service::backends::libp2p::IDENTIFY;

#[derive(Debug)]
struct ProtocolCapabilities {
    network_matches: bool,
    supports_kademlia: bool,
    supports_chainsync: bool,
}

impl<R: Clone + Send + RngCore + 'static> SwarmHandler<R> {
    pub(super) fn handle_identify_event(&mut self, event: identify::Event) {
        match event {
            identify::Event::Received { peer_id, info, .. } => {
                self.handle_identify_received(peer_id, info);
            }
            event => {
                tracing::trace!(target: LOG_TARGET, "Identify event: {:?}", event);
            }
        }
    }

    fn handle_identify_received(&mut self, peer_id: PeerId, info: identify::Info) {
        tracing::trace!(
            target: LOG_TARGET,
            "Identified peer {} with addresses {:?}",
            peer_id,
            info.listen_addrs
        );

        let advertised_protocols = info.protocols.into_iter().collect::<HashSet<_>>();
        let capabilities = protocol_capabilities(
            &info.protocol_version,
            &advertised_protocols,
            &self.protocol_contract,
        );
        tracing::debug!(
            target: LOG_TARGET,
            peer = %peer_id,
            protocol_version = %info.protocol_version,
            network_matches = capabilities.network_matches,
            supports_kademlia = capabilities.supports_kademlia,
            supports_chainsync = capabilities.supports_chainsync,
            protocols = ?advertised_protocols,
            "Classified peer protocol capabilities"
        );

        self.peer_advertised_protocols
            .insert(peer_id, advertised_protocols);

        self.add_identified_kademlia_addresses(
            peer_id,
            &info.listen_addrs,
            capabilities.supports_kademlia,
        );

        if !capabilities.supports_chainsync {
            tracing::debug!(
                target: LOG_TARGET,
                "Peer {peer_id} is not chainsync eligible because it does not advertise the \
                configured chainsync protocol"
            );
        }
    }

    fn add_identified_kademlia_addresses(
        &mut self,
        peer_id: PeerId,
        listen_addrs: &[Multiaddr],
        supports_kademlia: bool,
    ) {
        if !supports_kademlia {
            return;
        }

        tracing::trace!(
            target: LOG_TARGET,
            "Adding discovered node to Kademlia, seen addresses: {:?}",
            listen_addrs
        );
        // We need to add the peer to the Kademlia routing table in order to
        // enable peer discovery.
        for addr in listen_addrs {
            if !is_kademlia_candidate_address(addr) {
                tracing::trace!(
                    target: LOG_TARGET,
                    "Skipping non-routable identify address for Kademlia: {}",
                    addr
                );
                continue;
            }
            self.swarm.kademlia_add_address(peer_id, addr);
        }
    }
}

fn protocol_capabilities(
    protocol_version: &str,
    protocols: &HashSet<lb_libp2p::libp2p::StreamProtocol>,
    contract: &ProtocolContract,
) -> ProtocolCapabilities {
    ProtocolCapabilities {
        network_matches: protocol_version == contract.identify_protocol_version.as_ref(),
        supports_kademlia: protocols.contains(&contract.kademlia_protocol),
        supports_chainsync: protocols.contains(&contract.chain_sync_protocol),
    }
}

fn is_kademlia_candidate_address(addr: &Multiaddr) -> bool {
    // Tests run entirely on local/private interfaces; keep production
    // filtering enabled while allowing all identify addresses in test builds.
    let filter_identify_addrs = !cfg!(test);
    if !filter_identify_addrs {
        return true;
    }

    for protocol in addr {
        match protocol {
            Protocol::Ip4(ip) => {
                return !ip.is_loopback()
                    && !ip.is_private()
                    && !ip.is_unspecified()
                    && !ip.is_link_local();
            }
            Protocol::Ip6(ip) => {
                return !ip.is_loopback()
                    && !ip.is_unspecified()
                    && !ip.is_unique_local()
                    && !ip.is_unicast_link_local();
            }
            _ => {}
        }
    }

    true
}

#[cfg(test)]
mod tests {
    use lb_libp2p::libp2p::StreamProtocol;

    use super::*;

    fn contract() -> ProtocolContract {
        ProtocolContract {
            identify_protocol_version: StreamProtocol::new("/network/1.0.0"),
            kademlia_protocol: StreamProtocol::new("/network/kad/1.0.0"),
            chain_sync_protocol: StreamProtocol::new("/network/chainsync/1.0.0"),
        }
    }

    fn advertised_protocols(protocols: &[&'static str]) -> HashSet<StreamProtocol> {
        protocols
            .iter()
            .map(|protocol| StreamProtocol::new(protocol))
            .collect()
    }

    #[test]
    fn protocol_capabilities_are_classified() {
        let cases = [
            (
                "exact chainsync",
                "/network/1.0.0",
                vec!["/network/chainsync/1.0.0"],
                true,
            ),
            (
                "chainsync with Kademlia",
                "/network/1.0.0",
                vec!["/network/kad/1.0.0", "/network/chainsync/1.0.0"],
                true,
            ),
            (
                "chainsync with unrelated protocols",
                "/network/1.0.0",
                vec!["/network/gossipsub/1.0.0", "/network/chainsync/1.0.0"],
                true,
            ),
            (
                "wrong chainsync protocol",
                "/network/1.0.0",
                vec!["/network/kad/1.0.0", "/network/chainsync/0.9.0"],
                false,
            ),
            (
                "missing chainsync protocol",
                "/network/1.0.0",
                vec!["/network/kad/1.0.0"],
                false,
            ),
        ];

        for (name, protocol_version, protocols, expected) in cases {
            let capabilities = protocol_capabilities(
                protocol_version,
                &advertised_protocols(&protocols),
                &contract(),
            );
            assert_eq!(capabilities.supports_chainsync, expected, "{name}");
            assert!(capabilities.network_matches, "{name}");
        }

        let capabilities = protocol_capabilities(
            "/other-network/1.0.0",
            &advertised_protocols(&["/network/chainsync/1.0.0"]),
            &contract(),
        );
        assert!(!capabilities.network_matches);
        assert!(capabilities.supports_chainsync);
        assert!(!capabilities.supports_kademlia);
    }
}
