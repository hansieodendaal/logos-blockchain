use std::collections::HashSet;

use lb_banning_service::{BanScope, BanningServiceApi, ConfiguredBanPolicy, Subsystem};
use lb_network_service::{LocalBanView, backends::libp2p::PeerId};

/// `ChainSync`'s opt-in view of the central banning service.
///
/// The reusable local view owns synchronization and authority handling. This
/// wrapper keeps the `ChainSync` scope at its call sites and prevents those
/// consumers from accidentally querying another service's scope.
#[derive(Clone)]
pub struct ChainSyncBanView {
    local: LocalBanView,
}

impl ChainSyncBanView {
    pub(crate) fn new<RuntimeServiceId>(
        banning_service: Option<BanningServiceApi<RuntimeServiceId>>,
        configured_policy: ConfiguredBanPolicy,
    ) -> Self
    where
        RuntimeServiceId: Send + Sync + 'static,
    {
        Self {
            local: LocalBanView::new(banning_service, configured_policy),
        }
    }

    pub(crate) fn filter(&self, peers: &HashSet<PeerId>) -> HashSet<PeerId> {
        self.local.filter_for(peers, &Self::scope())
    }

    pub(crate) fn is_banned(&self, peer_id: PeerId) -> bool {
        self.local.is_banned_for(peer_id, &Self::scope())
    }

    #[cfg(test)]
    pub(crate) fn install_snapshot(&self, records: Vec<lb_banning_service::BanRecord>) {
        self.local.replace_dynamic_snapshot(records);
    }

    #[cfg(test)]
    pub(crate) fn disable(&self) {
        self.local.invalidate_dynamic_authority();
    }

    const fn scope() -> BanScope {
        BanScope::Service(Subsystem::ChainSync)
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, SystemTime};

    use lb_banning_service::{BanRecord, BanSource, BanningConfig, OffenseKind};

    use super::*;

    fn record(peer_id: PeerId, scope: BanScope, source: BanSource) -> BanRecord {
        BanRecord {
            peer_id,
            source,
            scope,
            offense: OffenseKind::ProtocolViolation,
            context: Some("test".to_owned()),
            reported_at: SystemTime::UNIX_EPOCH,
            expires_at: Some(SystemTime::now() + Duration::from_secs(60)),
        }
    }

    #[test]
    fn chain_sync_view_filters_only_applicable_scopes() {
        let chain_peer = PeerId::random();
        let blend_peer = PeerId::random();
        let global_peer = PeerId::random();
        let configured_peer = PeerId::random();
        let policy = ConfiguredBanPolicy::from_config(&BanningConfig {
            blacklist: vec![configured_peer],
            ..Default::default()
        });
        let view = ChainSyncBanView::new::<()>(None, policy);
        let peers = HashSet::from([chain_peer, blend_peer, global_peer, configured_peer]);
        view.install_snapshot(vec![
            record(
                chain_peer,
                BanScope::Service(Subsystem::ChainSync),
                BanSource::Service(Subsystem::ChainSync),
            ),
            record(
                blend_peer,
                BanScope::Service(Subsystem::Blend),
                BanSource::Service(Subsystem::Blend),
            ),
            record(
                global_peer,
                BanScope::Global,
                BanSource::Service(Subsystem::Other("policy".to_owned())),
            ),
        ]);

        assert_eq!(view.filter(&peers), HashSet::from([blend_peer]));
        assert!(view.is_banned(chain_peer));
        assert!(!view.is_banned(blend_peer));
        assert!(view.is_banned(global_peer));
        assert!(view.is_banned(configured_peer));
    }

    #[test]
    fn dynamic_authority_loss_keeps_configured_blacklist_enforced() {
        let configured_peer = PeerId::random();
        let dynamic_peer = PeerId::random();
        let view = ChainSyncBanView::new::<()>(
            None,
            ConfiguredBanPolicy::from_config(&BanningConfig {
                blacklist: vec![configured_peer],
                ..Default::default()
            }),
        );
        view.install_snapshot(vec![record(
            dynamic_peer,
            BanScope::Service(Subsystem::ChainSync),
            BanSource::Service(Subsystem::ChainSync),
        )]);
        view.disable();

        assert!(!view.is_banned(dynamic_peer));
        assert!(view.is_banned(configured_peer));
    }
}
