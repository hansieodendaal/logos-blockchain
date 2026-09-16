use std::{
    collections::{HashMap, HashSet},
    time::{Duration, SystemTime},
};

use lb_libp2p::PeerId;

use crate::{
    BanningConfig, ConfiguredBanPolicy,
    types::{BanRecord, BanScope, Violation},
};

pub trait Clock: Clone + Send + Sync + 'static {
    fn now(&self) -> SystemTime;
}

#[derive(Clone, Copy, Debug, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> SystemTime {
        SystemTime::now()
    }
}

#[derive(Clone, Debug)]
pub enum BanMutation {
    Created(BanRecord),
    Updated(BanRecord),
    Unchanged(BanRecord),
}

impl BanMutation {
    pub const fn record(&self) -> &BanRecord {
        match self {
            Self::Created(record) | Self::Updated(record) | Self::Unchanged(record) => record,
        }
    }
}

/// An in-memory store keyed by both peer and enforcement scope.
#[derive(Clone, Debug)]
pub struct BanStore<C = SystemClock> {
    records: HashMap<(PeerId, BanScope), BanRecord>,
    config: BanningConfig,
    configured_policy: ConfiguredBanPolicy,
    clock: C,
}

impl BanStore<SystemClock> {
    pub(crate) fn from_config(config: &BanningConfig) -> Self {
        Self::with_clock(config, SystemClock)
    }
}

impl<C: Clock> BanStore<C> {
    pub(crate) fn with_clock(config: &BanningConfig, clock: C) -> Self {
        let configured_policy = config.configured_ban_policy();
        let mut store = Self {
            records: HashMap::new(),
            config: config.clone(),
            configured_policy: configured_policy.clone(),
            clock,
        };

        for peer_id in configured_policy.blacklisted_peers() {
            if let Some(record) = configured_policy.record(*peer_id, store.clock.now()) {
                store.records.insert((*peer_id, BanScope::Global), record);
            }
        }

        store
    }

    pub(crate) fn report(&mut self, violation: Violation) -> Option<BanMutation> {
        self.expire();
        if self.config.whitelist.contains(&violation.peer_id) {
            return None;
        }

        if self.configured_policy.contains(&violation.peer_id) {
            return self
                .records
                .get(&(violation.peer_id, BanScope::Global))
                .cloned()
                .map(BanMutation::Unchanged);
        }

        let key = (violation.peer_id, violation.scope.clone());
        let existing = self.records.get(&key).cloned();
        let had_existing = existing.is_some();

        let now = self.clock.now();
        let duration = self.config.ban_duration(violation.offense);
        let Some(expires_at) = now.checked_add(duration) else {
            tracing::warn!(
                peer_id = %violation.peer_id,
                "ban duration overflowed SystemTime; refusing to create ban"
            );
            return existing.map(BanMutation::Unchanged);
        };

        if let Some(existing) = existing
            && existing
                .expires_at
                .is_none_or(|expiry| expiry >= expires_at)
        {
            return Some(BanMutation::Unchanged(existing));
        }

        let record = BanRecord {
            peer_id: violation.peer_id,
            source: violation.source,
            scope: violation.scope,
            offense: violation.offense,
            context: violation.context,
            reported_at: now,
            expires_at: Some(expires_at),
        };
        self.records.insert(key, record.clone());
        Some(if had_existing {
            BanMutation::Updated(record)
        } else {
            BanMutation::Created(record)
        })
    }

    pub(crate) fn replace(&mut self, violation: Violation) -> Option<BanMutation> {
        self.expire();
        if self.config.whitelist.contains(&violation.peer_id) {
            return None;
        }

        if self.configured_policy.contains(&violation.peer_id) {
            return self
                .records
                .get(&(violation.peer_id, BanScope::Global))
                .cloned()
                .map(BanMutation::Unchanged);
        }

        let key = (violation.peer_id, violation.scope.clone());
        let had_existing = self.records.contains_key(&key);
        let now = self.clock.now();
        let duration = self.config.ban_duration(violation.offense);
        let Some(expires_at) = now.checked_add(duration) else {
            tracing::warn!(
                peer_id = %violation.peer_id,
                "ban duration overflowed SystemTime; refusing to replace ban"
            );
            return self.records.get(&key).cloned().map(BanMutation::Unchanged);
        };

        let record = BanRecord {
            peer_id: violation.peer_id,
            source: violation.source,
            scope: violation.scope,
            offense: violation.offense,
            context: violation.context,
            reported_at: now,
            expires_at: Some(expires_at),
        };
        self.records.insert(key, record.clone());
        Some(if had_existing {
            BanMutation::Updated(record)
        } else {
            BanMutation::Created(record)
        })
    }

    pub(crate) fn query(&mut self, peer_id: PeerId, scope: &BanScope) -> Vec<BanRecord> {
        self.expire();
        self.query_without_expiry(&HashSet::from([peer_id]), scope)
    }

    pub(crate) fn query_many(
        &mut self,
        peer_ids: &HashSet<PeerId>,
        scope: &BanScope,
    ) -> Vec<BanRecord> {
        self.expire();
        self.query_without_expiry(peer_ids, scope)
    }

    fn query_without_expiry(&self, peer_ids: &HashSet<PeerId>, scope: &BanScope) -> Vec<BanRecord> {
        self.records
            .values()
            .filter(|record| peer_ids.contains(&record.peer_id) && record.applies_to(scope))
            .cloned()
            .collect()
    }

    pub(crate) fn active(&mut self) -> Vec<BanRecord> {
        self.expire();
        self.records.values().cloned().collect()
    }

    #[expect(
        clippy::needless_collect,
        reason = "Keys must be snapshotted before mutating the map."
    )]
    pub(crate) fn unban(&mut self, peer_id: PeerId, scope: Option<&BanScope>) -> Vec<BanRecord> {
        let keys = self
            .records
            .iter()
            .filter(|((record_peer_id, record_scope), record)| {
                *record_peer_id == peer_id
                    && scope.is_none_or(|scope| scope == record_scope)
                    && record.expires_at.is_some()
            })
            .map(|(key, _)| key.clone())
            .collect::<Vec<_>>();

        keys.into_iter()
            .filter_map(|key| self.records.remove(&key))
            .filter(|record| record.expires_at.is_some())
            .collect()
    }

    /// Remove expired records and return them so the service can emit a
    /// scoped expiry event for each one.
    #[expect(
        clippy::needless_collect,
        reason = "Keys must be snapshotted before mutating the map."
    )]
    pub(crate) fn expire(&mut self) -> Vec<BanRecord> {
        let now = self.clock.now();
        let expired_keys = self
            .records
            .iter()
            .filter_map(|(key, record)| {
                record
                    .expires_at
                    .is_some_and(|expires_at| now >= expires_at)
                    .then_some(key.clone())
            })
            .collect::<Vec<_>>();

        expired_keys
            .into_iter()
            .filter_map(|key| self.records.remove(&key))
            .collect()
    }

    pub(crate) const fn expiry_check_interval(&self) -> Duration {
        self.config.expiry_check_interval
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use super::*;
    use crate::OffenseKind;

    #[derive(Clone)]
    struct ManualClock(Arc<Mutex<SystemTime>>);

    impl ManualClock {
        fn new(now: SystemTime) -> Self {
            Self(Arc::new(Mutex::new(now)))
        }

        fn advance(&self, duration: Duration) {
            let mut now = self.0.lock().expect("clock lock");
            *now = now.checked_add(duration).expect("test time overflow");
        }
    }

    impl Clock for ManualClock {
        fn now(&self) -> SystemTime {
            *self.0.lock().expect("clock lock")
        }
    }

    fn config() -> BanningConfig {
        BanningConfig {
            offenses: crate::OffenseDurations {
                spam_msg: Duration::from_secs(10),
                invalid_sig: Duration::from_secs(20),
                protocol_violation: Duration::from_secs(5),
                ..Default::default()
            },
            ..Default::default()
        }
    }

    fn violation(
        peer_id: PeerId,
        source: crate::BanSource,
        scope: BanScope,
        offense: OffenseKind,
    ) -> Violation {
        Violation::with_source(
            peer_id,
            source,
            scope,
            offense,
            Some("deterministic test context".to_owned()),
        )
    }

    fn chain_violation(peer_id: PeerId, scope: BanScope, offense: OffenseKind) -> Violation {
        violation(
            peer_id,
            crate::BanSource::Service(crate::Subsystem::ChainSync),
            scope,
            offense,
        )
    }

    #[test]
    fn records_scope_metadata_duration_and_expiry() {
        let now = SystemTime::UNIX_EPOCH;
        let clock = ManualClock::new(now);
        let scope = BanScope::Service(crate::Subsystem::ChainSync);
        let peer_id = PeerId::random();
        let mut store = BanStore::with_clock(&config(), clock.clone());

        let record = store
            .report(chain_violation(
                peer_id,
                scope.clone(),
                OffenseKind::SpamMsg,
            ))
            .expect("ban")
            .record()
            .clone();
        assert_eq!(record.peer_id, peer_id);
        assert_eq!(
            record.source,
            crate::BanSource::Service(crate::Subsystem::ChainSync)
        );
        assert_eq!(record.scope, scope.clone());
        assert_eq!(record.offense, OffenseKind::SpamMsg);
        assert_eq!(
            record.context.as_deref(),
            Some("deterministic test context")
        );
        assert_eq!(record.expires_at, Some(now + Duration::from_secs(10)));
        assert_eq!(store.query(peer_id, &scope).len(), 1);

        clock.advance(Duration::from_secs(10));
        assert!(store.query(peer_id, &scope).is_empty());
    }

    #[test]
    fn explicit_unban_and_peer_isolation_work() {
        let clock = ManualClock::new(SystemTime::UNIX_EPOCH);
        let scope = BanScope::Service(crate::Subsystem::ChainSync);
        let peer = PeerId::random();
        let other_peer = PeerId::random();
        let mut store = BanStore::with_clock(&config(), clock);
        store.report(chain_violation(peer, scope.clone(), OffenseKind::SpamMsg));
        store.report(chain_violation(
            other_peer,
            scope.clone(),
            OffenseKind::SpamMsg,
        ));

        assert_eq!(store.unban(peer, Some(&scope)).len(), 1);
        assert!(store.query(peer, &scope).is_empty());
        assert_eq!(store.query(other_peer, &scope).len(), 1);
    }

    #[test]
    fn whitelist_and_blacklist_are_explicitly_scoped() {
        let whitelisted = PeerId::random();
        let blacklisted = PeerId::random();
        let config = BanningConfig {
            whitelist: vec![whitelisted],
            blacklist: vec![whitelisted, blacklisted],
            ..config()
        };
        let scope = BanScope::Service(crate::Subsystem::ChainSync);
        let mut store = BanStore::with_clock(&config, ManualClock::new(SystemTime::UNIX_EPOCH));

        assert!(
            store
                .report(chain_violation(
                    whitelisted,
                    scope.clone(),
                    OffenseKind::SpamMsg,
                ))
                .is_none()
        );
        let blacklisted_records = store.query(blacklisted, &scope);
        assert_eq!(blacklisted_records.len(), 1);
        assert_eq!(blacklisted_records[0].scope, BanScope::Global);
        assert_eq!(
            blacklisted_records[0].source,
            crate::BanSource::Configuration
        );
        assert!(blacklisted_records[0].expires_at.is_none());

        assert!(store.unban(blacklisted, None).is_empty());
        let replacement = store
            .replace(chain_violation(blacklisted, scope, OffenseKind::SpamMsg))
            .expect("blacklist replacement response");
        assert!(matches!(replacement, BanMutation::Unchanged(_)));
        assert_eq!(replacement.record().source, crate::BanSource::Configuration);
        assert!(replacement.record().expires_at.is_none());
    }

    #[test]
    fn listing_contains_only_active_records() {
        let clock = ManualClock::new(SystemTime::UNIX_EPOCH);
        let scope = BanScope::Service(crate::Subsystem::ChainSync);
        let mut store = BanStore::with_clock(&config(), clock.clone());
        store.report(chain_violation(
            PeerId::random(),
            scope,
            OffenseKind::SpamMsg,
        ));
        assert_eq!(store.active().len(), 1);
        clock.advance(Duration::from_secs(11));
        assert!(store.active().is_empty());
    }

    #[test]
    fn service_scope_does_not_implicitly_ban_another_service() {
        let clock = ManualClock::new(SystemTime::UNIX_EPOCH);
        let peer_id = PeerId::random();
        let chain_scope = BanScope::Service(crate::Subsystem::ChainSync);
        let blend_scope = BanScope::Service(crate::Subsystem::Blend);
        let mut store = BanStore::with_clock(&config(), clock);

        store.report(chain_violation(
            peer_id,
            chain_scope.clone(),
            OffenseKind::SpamMsg,
        ));

        assert_eq!(store.query(peer_id, &chain_scope).len(), 1);
        assert!(store.query(peer_id, &blend_scope).is_empty());
    }

    #[test]
    fn global_ban_retains_source_and_applies_to_each_consumer_scope() {
        let peer_id = PeerId::random();
        let clock = ManualClock::new(SystemTime::UNIX_EPOCH);
        let mut store = BanStore::with_clock(&config(), clock);
        let source = crate::BanSource::Service(crate::Subsystem::ChainSync);
        let record = store
            .report(violation(
                peer_id,
                source.clone(),
                BanScope::Global,
                OffenseKind::ProtocolViolation,
            ))
            .expect("global ban")
            .record()
            .clone();

        assert_eq!(record.source, source);
        assert_eq!(
            store
                .query(peer_id, &BanScope::Service(crate::Subsystem::ChainSync))
                .len(),
            1
        );
        assert_eq!(
            store
                .query(peer_id, &BanScope::Service(crate::Subsystem::Blend))
                .len(),
            1
        );
    }

    #[test]
    fn multiple_scopes_for_one_peer_coexist() {
        let peer_id = PeerId::random();
        let clock = ManualClock::new(SystemTime::UNIX_EPOCH);
        let chain_scope = BanScope::Service(crate::Subsystem::ChainSync);
        let blend_scope = BanScope::Service(crate::Subsystem::Blend);
        let mut store = BanStore::with_clock(&config(), clock);

        store.report(chain_violation(
            peer_id,
            chain_scope.clone(),
            OffenseKind::SpamMsg,
        ));
        store.report(violation(
            peer_id,
            crate::BanSource::Service(crate::Subsystem::Blend),
            blend_scope.clone(),
            OffenseKind::ProtocolViolation,
        ));

        assert_eq!(store.active().len(), 2);
        assert_eq!(store.unban(peer_id, Some(&chain_scope)).len(), 1);
        assert_eq!(store.query(peer_id, &blend_scope).len(), 1);
    }

    #[test]
    fn normal_reports_never_shorten_an_active_ban() {
        let peer_id = PeerId::random();
        let clock = ManualClock::new(SystemTime::UNIX_EPOCH);
        let scope = BanScope::Service(crate::Subsystem::ChainSync);
        let mut store = BanStore::with_clock(&config(), clock.clone());

        let long = store
            .report(chain_violation(
                peer_id,
                scope.clone(),
                OffenseKind::InvalidSig,
            ))
            .expect("long ban")
            .record()
            .clone();
        clock.advance(Duration::from_secs(1));
        let short = store
            .report(chain_violation(peer_id, scope, OffenseKind::SpamMsg))
            .expect("short report")
            .record()
            .clone();

        assert_eq!(short.expires_at, long.expires_at);
        assert_eq!(short.offense, OffenseKind::InvalidSig);
    }

    #[test]
    fn a_stronger_follow_up_report_extends_the_active_ban() {
        let peer_id = PeerId::random();
        let clock = ManualClock::new(SystemTime::UNIX_EPOCH);
        let scope = BanScope::Service(crate::Subsystem::ChainSync);
        let mut store = BanStore::with_clock(&config(), clock.clone());

        let short = store
            .report(chain_violation(
                peer_id,
                scope.clone(),
                OffenseKind::SpamMsg,
            ))
            .expect("short ban")
            .record()
            .clone();
        clock.advance(Duration::from_secs(1));
        let long = store
            .report(chain_violation(peer_id, scope, OffenseKind::InvalidSig))
            .expect("long report")
            .record()
            .clone();

        assert!(long.expires_at > short.expires_at);
        assert_eq!(long.offense, OffenseKind::InvalidSig);
    }

    #[test]
    fn explicit_unban_allows_a_shorter_ban() {
        let peer_id = PeerId::random();
        let clock = ManualClock::new(SystemTime::UNIX_EPOCH);
        let scope = BanScope::Service(crate::Subsystem::ChainSync);
        let mut store = BanStore::with_clock(&config(), clock);

        store.report(chain_violation(
            peer_id,
            scope.clone(),
            OffenseKind::InvalidSig,
        ));
        assert_eq!(store.unban(peer_id, Some(&scope)).len(), 1);
        let short = store
            .report(chain_violation(peer_id, scope, OffenseKind::SpamMsg))
            .expect("short ban after unban")
            .record()
            .clone();
        assert_eq!(
            short.expires_at,
            Some(SystemTime::UNIX_EPOCH + Duration::from_secs(10))
        );
    }

    #[test]
    fn explicit_replace_can_shorten_a_finite_ban() {
        let peer_id = PeerId::random();
        let clock = ManualClock::new(SystemTime::UNIX_EPOCH);
        let scope = BanScope::Service(crate::Subsystem::ChainSync);
        let mut store = BanStore::with_clock(&config(), clock);

        store.report(chain_violation(
            peer_id,
            scope.clone(),
            OffenseKind::InvalidSig,
        ));
        let replacement = store
            .replace(chain_violation(peer_id, scope, OffenseKind::SpamMsg))
            .expect("replacement")
            .record()
            .clone();

        assert_eq!(
            replacement.expires_at,
            Some(SystemTime::UNIX_EPOCH + Duration::from_secs(10))
        );
        assert_eq!(replacement.offense, OffenseKind::SpamMsg);
    }

    #[test]
    fn expiry_returns_the_complete_record_for_event_propagation() {
        let peer_id = PeerId::random();
        let clock = ManualClock::new(SystemTime::UNIX_EPOCH);
        let scope = BanScope::Service(crate::Subsystem::ChainSync);
        let mut store = BanStore::with_clock(&config(), clock.clone());

        store.report(chain_violation(
            peer_id,
            scope.clone(),
            OffenseKind::SpamMsg,
        ));
        clock.advance(Duration::from_secs(10));
        let expired = store.expire();

        assert_eq!(expired.len(), 1);
        assert_eq!(expired[0].peer_id, peer_id);
        assert_eq!(expired[0].scope, scope);
        assert_eq!(
            expired[0].source,
            crate::BanSource::Service(crate::Subsystem::ChainSync)
        );
    }
}
