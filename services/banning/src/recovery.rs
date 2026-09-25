use std::{
    collections::HashMap,
    fmt::{Debug, Display},
    time::SystemTime,
};

use lb_binary_codec::bincode::{DeserializeOp as _, SerializeOp as _};
use lb_services_utils::overwatch::recovery::{
    RecoveryBackend, RecoveryData, RecoveryError, RecoveryResult, StorageRecoverySettings as _,
};
use lb_storage_service::{StorageService, api::StorageApi, recovery::recovery_key};
use overwatch::{
    DynError,
    overwatch::OverwatchHandle,
    services::{AsServiceId, state::ServiceState},
};
use serde::{Deserialize, Serialize};
use tokio::sync::OnceCell;

use crate::{
    BanRecord, BanScope, BanSource, BanningConfig, OffenseKind, Subsystem, ban_store::BanStore,
};

const FORMAT_VERSION: u16 = 1;
const RECOVERY_LOG_TARGET: &str = lb_log_targets::utils::RECOVERY;

/// A versioned full-state checkpoint for dynamic bans.
///
/// Runtime store records and configured blacklist entries are deliberately not
/// used as the disk schema. Checkpoints may be coalesced by Overwatch and are
/// not transactional durability acknowledgements for individual ban API calls.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct BanningRecoveryState {
    version: u16,
    records: Vec<PersistedBanRecordV1>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct PersistedBanRecordV1 {
    peer_id: lb_libp2p::PeerId,
    source: PersistedSubsystemV1,
    scope: PersistedBanScopeV1,
    offense: PersistedOffenseKindV1,
    context: Option<String>,
    reported_at: SystemTime,
    expires_at: SystemTime,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
enum PersistedSubsystemV1 {
    ChainSync,
    Libp2p,
    DataAvailability,
    Blend,
    Other(String),
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
enum PersistedBanScopeV1 {
    Global,
    Service(PersistedSubsystemV1),
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
enum PersistedOffenseKindV1 {
    SpamMsg,
    InvalidSig,
    ProtocolViolation,
    TooManyDials,
    DoSConnectionFlood,
    InvalidBlob,
    GossipsubScoreDrop,
    Other,
    BlackListed,
}

impl Default for BanningRecoveryState {
    fn default() -> Self {
        Self {
            version: FORMAT_VERSION,
            records: Vec::new(),
        }
    }
}

impl BanningRecoveryState {
    pub(crate) fn from_store<C: crate::Clock>(store: &BanStore<C>) -> Self {
        let records = store
            .active()
            .into_iter()
            .filter(|record| {
                record
                    .expires_at
                    .is_some_and(|expires_at| SystemTime::now() < expires_at)
            })
            .filter_map(PersistedBanRecordV1::from_runtime)
            .collect();
        Self {
            version: FORMAT_VERSION,
            records,
        }
    }

    fn reconcile(
        self,
        config: &BanningConfig,
        now: SystemTime,
    ) -> Result<(Self, Vec<BanRecord>, RecoveryStats), String> {
        if self.version != FORMAT_VERSION {
            return Err(format!(
                "unsupported banning recovery version {}",
                self.version
            ));
        }

        let configured_policy = config.configured_ban_policy();
        let mut canonical = HashMap::<(lb_libp2p::PeerId, BanScope), PersistedBanRecordV1>::new();
        let mut stats = RecoveryStats::default();

        for persisted in self.records {
            if persisted.reported_at >= persisted.expires_at {
                stats.invalid += 1;
                continue;
            }
            if now >= persisted.expires_at {
                stats.expired += 1;
                continue;
            }
            if config.whitelist.contains(&persisted.peer_id)
                || configured_policy.contains(&persisted.peer_id)
            {
                stats.overridden_by_config += 1;
                continue;
            }

            let key = (persisted.peer_id, persisted.scope.clone().runtime());
            match canonical.entry(key) {
                std::collections::hash_map::Entry::Vacant(entry) => {
                    entry.insert(persisted);
                }
                std::collections::hash_map::Entry::Occupied(mut entry) => {
                    stats.duplicates += 1;
                    let existing = entry.get();
                    if persisted.expires_at > existing.expires_at
                        || (persisted.expires_at == existing.expires_at
                            && canonical_metadata(&persisted) < canonical_metadata(existing))
                    {
                        entry.insert(persisted);
                    }
                }
            }
        }

        let records = canonical
            .into_values()
            .map(PersistedBanRecordV1::runtime)
            .collect::<Vec<_>>();
        let normalized = Self {
            version: FORMAT_VERSION,
            records: records
                .iter()
                .cloned()
                .filter_map(PersistedBanRecordV1::from_runtime)
                .collect(),
        };
        Ok((normalized, records, stats))
    }

    pub(crate) fn runtime_records(
        self,
        config: &BanningConfig,
        now: SystemTime,
    ) -> Result<Vec<BanRecord>, String> {
        self.reconcile(config, now).map(|(_, records, _)| records)
    }
}

fn canonical_metadata(
    record: &PersistedBanRecordV1,
) -> (
    SystemTime,
    PersistedSubsystemV1,
    PersistedOffenseKindV1,
    Option<String>,
) {
    (
        record.reported_at,
        record.source.clone(),
        record.offense,
        record.context.clone(),
    )
}

impl PersistedBanRecordV1 {
    fn from_runtime(record: BanRecord) -> Option<Self> {
        let BanSource::Service(source) = record.source else {
            return None;
        };
        Some(Self {
            peer_id: record.peer_id,
            source: source.into(),
            scope: record.scope.into(),
            offense: record.offense.into(),
            context: record.context,
            reported_at: record.reported_at,
            expires_at: record.expires_at?,
        })
    }

    fn runtime(self) -> BanRecord {
        BanRecord {
            peer_id: self.peer_id,
            source: BanSource::Service(self.source.runtime()),
            scope: self.scope.runtime(),
            offense: self.offense.runtime(),
            context: self.context,
            reported_at: self.reported_at,
            expires_at: Some(self.expires_at),
        }
    }
}

impl From<Subsystem> for PersistedSubsystemV1 {
    fn from(subsystem: Subsystem) -> Self {
        match subsystem {
            Subsystem::ChainSync => Self::ChainSync,
            Subsystem::Libp2p => Self::Libp2p,
            Subsystem::DataAvailability => Self::DataAvailability,
            Subsystem::Blend => Self::Blend,
            Subsystem::Other(name) => Self::Other(name),
        }
    }
}

impl PersistedSubsystemV1 {
    fn runtime(self) -> Subsystem {
        match self {
            Self::ChainSync => Subsystem::ChainSync,
            Self::Libp2p => Subsystem::Libp2p,
            Self::DataAvailability => Subsystem::DataAvailability,
            Self::Blend => Subsystem::Blend,
            Self::Other(name) => Subsystem::Other(name),
        }
    }
}

impl From<BanScope> for PersistedBanScopeV1 {
    fn from(scope: BanScope) -> Self {
        match scope {
            BanScope::Global => Self::Global,
            BanScope::Service(subsystem) => Self::Service(subsystem.into()),
        }
    }
}

impl PersistedBanScopeV1 {
    fn runtime(self) -> BanScope {
        match self {
            Self::Global => BanScope::Global,
            Self::Service(subsystem) => BanScope::Service(subsystem.runtime()),
        }
    }
}

impl From<OffenseKind> for PersistedOffenseKindV1 {
    fn from(offense: OffenseKind) -> Self {
        match offense {
            OffenseKind::SpamMsg => Self::SpamMsg,
            OffenseKind::InvalidSig => Self::InvalidSig,
            OffenseKind::ProtocolViolation => Self::ProtocolViolation,
            OffenseKind::TooManyDials => Self::TooManyDials,
            OffenseKind::DoSConnectionFlood => Self::DoSConnectionFlood,
            OffenseKind::InvalidBlob => Self::InvalidBlob,
            OffenseKind::GossipsubScoreDrop => Self::GossipsubScoreDrop,
            OffenseKind::Other => Self::Other,
            OffenseKind::BlackListed => Self::BlackListed,
        }
    }
}

impl PersistedOffenseKindV1 {
    const fn runtime(self) -> OffenseKind {
        match self {
            Self::SpamMsg => OffenseKind::SpamMsg,
            Self::InvalidSig => OffenseKind::InvalidSig,
            Self::ProtocolViolation => OffenseKind::ProtocolViolation,
            Self::TooManyDials => OffenseKind::TooManyDials,
            Self::DoSConnectionFlood => OffenseKind::DoSConnectionFlood,
            Self::InvalidBlob => OffenseKind::InvalidBlob,
            Self::GossipsubScoreDrop => OffenseKind::GossipsubScoreDrop,
            Self::Other => OffenseKind::Other,
            Self::BlackListed => OffenseKind::BlackListed,
        }
    }
}

#[derive(Default)]
struct RecoveryStats {
    invalid: usize,
    expired: usize,
    overridden_by_config: usize,
    duplicates: usize,
}

impl ServiceState for BanningRecoveryState {
    type Settings = BanningConfig;
    type Error = DynError;

    fn from_settings(_settings: &Self::Settings) -> Result<Self, Self::Error> {
        Ok(Self::default())
    }
}

/// Banning-specific recovery backend. It mirrors each checkpoint into the
/// shared process recovery cache because `RecoveryData::take` consumes an
/// entry when an individual service starts or restarts.
pub struct BanningRecoveryBackend<RuntimeServiceId> {
    overwatch_handle: OverwatchHandle<RuntimeServiceId>,
    storage: OnceCell<StorageApi>,
    recovery_data: RecoveryData,
}

impl<RuntimeServiceId> Clone for BanningRecoveryBackend<RuntimeServiceId>
where
    OverwatchHandle<RuntimeServiceId>: Clone,
{
    fn clone(&self) -> Self {
        Self {
            overwatch_handle: self.overwatch_handle.clone(),
            storage: self.storage.clone(),
            recovery_data: self.recovery_data.clone(),
        }
    }
}

impl<RuntimeServiceId> BanningRecoveryBackend<RuntimeServiceId> {
    fn cache_state(
        recovery_data: &RecoveryData,
        state: &BanningRecoveryState,
    ) -> RecoveryResult<()> {
        let encoded = state
            .to_bytes()
            .map_err(|error| RecoveryError::Backend(error.to_string()))?;
        recovery_data.insert(
            recovery_key(BanningConfig::RECOVERY_KEY_SUFFIX).to_vec(),
            encoded,
        )
    }

    fn decode_reconcile(
        bytes: &[u8],
        config: &BanningConfig,
    ) -> Result<BanningRecoveryState, String> {
        let state = BanningRecoveryState::from_bytes(bytes).map_err(|error| error.to_string())?;
        let (normalized, _records, stats) = state.reconcile(config, SystemTime::now())?;
        if stats.invalid + stats.expired + stats.overridden_by_config + stats.duplicates > 0 {
            tracing::warn!(
                target: RECOVERY_LOG_TARGET,
                invalid = stats.invalid,
                expired = stats.expired,
                overridden_by_config = stats.overridden_by_config,
                duplicates = stats.duplicates,
                "banning recovery checkpoint was reconciled"
            );
        }
        Ok(normalized)
    }

    fn load(settings: &BanningConfig) -> Option<BanningRecoveryState> {
        let bytes = match settings
            .recovery_data
            .take(&recovery_key(BanningConfig::RECOVERY_KEY_SUFFIX))
        {
            Ok(bytes) => bytes,
            Err(error) => {
                tracing::error!(target: RECOVERY_LOG_TARGET, %error, "failed to read in-memory banning recovery; starting with empty dynamic state");
                return None;
            }
        };
        let bytes = bytes?;

        match Self::decode_reconcile(&bytes, settings) {
            Ok(state) => {
                if let Err(error) = Self::cache_state(&settings.recovery_data, &state) {
                    tracing::error!(target: RECOVERY_LOG_TARGET, %error, "failed to retain loaded banning recovery in the in-process cache");
                }
                Some(state)
            }
            Err(error) => {
                tracing::error!(target: RECOVERY_LOG_TARGET, %error, "banning recovery is invalid or unsupported; starting with empty dynamic state");
                None
            }
        }
    }
}

#[async_trait::async_trait]
impl<RuntimeServiceId> RecoveryBackend<RuntimeServiceId>
    for BanningRecoveryBackend<RuntimeServiceId>
where
    RuntimeServiceId: Clone
        + Debug
        + Display
        + Send
        + Sync
        + 'static
        + AsServiceId<StorageService<RuntimeServiceId>>,
{
    type State = BanningRecoveryState;

    fn from_settings(
        settings: &BanningConfig,
        overwatch_handle: OverwatchHandle<RuntimeServiceId>,
    ) -> Self {
        Self {
            overwatch_handle,
            storage: OnceCell::new(),
            recovery_data: settings.recovery_data.clone(),
        }
    }

    fn load_state(settings: &BanningConfig) -> RecoveryResult<Option<Self::State>> {
        Ok(Self::load(settings))
    }

    async fn save_state(&mut self, state: Self::State) -> RecoveryResult<()> {
        Self::cache_state(&self.recovery_data, &state)?;
        let storage = self
            .storage
            .get_or_try_init(async || {
                StorageApi::from_overwatch_handle(&self.overwatch_handle)
                    .await
                    .map_err(|error| RecoveryError::Backend(error.to_string()))
            })
            .await?;

        storage
            .store(recovery_key(BanningConfig::RECOVERY_KEY_SUFFIX), state)
            .await
            .map_err(|error| RecoveryError::Backend(error.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashSet, time::Duration};

    use super::*;
    fn dynamic_record(
        peer_id: lb_libp2p::PeerId,
        scope: BanScope,
        source: Subsystem,
        reported_at: SystemTime,
        expires_at: SystemTime,
    ) -> BanRecord {
        BanRecord {
            peer_id,
            source: BanSource::Service(source),
            scope,
            offense: OffenseKind::ProtocolViolation,
            context: Some("test record".to_owned()),
            reported_at,
            expires_at: Some(expires_at),
        }
    }

    fn config(recovery_data: RecoveryData) -> BanningConfig {
        BanningConfig {
            recovery_data,
            ..BanningConfig::default()
        }
    }

    #[test]
    fn checkpoints_contain_only_finite_dynamic_records() {
        let configured_peer = lb_libp2p::PeerId::random();
        let dynamic_peer = lb_libp2p::PeerId::random();
        let config = BanningConfig {
            blacklist: vec![configured_peer],
            ..BanningConfig::default()
        };
        let dynamic = dynamic_record(
            dynamic_peer,
            BanScope::Global,
            Subsystem::ChainSync,
            SystemTime::now(),
            SystemTime::now() + Duration::from_secs(60),
        );
        let mut store = BanStore::from_config(&config);
        store.restore_dynamic([dynamic]);
        let snapshot = BanningRecoveryState::from_store(&store);
        assert_eq!(snapshot.records.len(), 1);
        assert_eq!(snapshot.records[0].peer_id, dynamic_peer);
    }

    #[test]
    fn recovery_preserves_absolute_expiry_and_reconciles_current_configuration() {
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(100);
        let whitelisted = lb_libp2p::PeerId::random();
        let blacklisted = lb_libp2p::PeerId::random();
        let active = lb_libp2p::PeerId::random();
        let expired = lb_libp2p::PeerId::random();
        let records = [
            dynamic_record(
                whitelisted,
                BanScope::Global,
                Subsystem::Libp2p,
                now - Duration::from_secs(10),
                now + Duration::from_secs(10),
            ),
            dynamic_record(
                blacklisted,
                BanScope::Service(Subsystem::ChainSync),
                Subsystem::ChainSync,
                now - Duration::from_secs(10),
                now + Duration::from_secs(10),
            ),
            dynamic_record(
                active,
                BanScope::Service(Subsystem::ChainSync),
                Subsystem::ChainSync,
                now - Duration::from_secs(10),
                now + Duration::from_secs(60),
            ),
            dynamic_record(
                expired,
                BanScope::Global,
                Subsystem::Blend,
                now - Duration::from_secs(60),
                now,
            ),
        ];
        let checkpoint = BanningRecoveryState {
            version: FORMAT_VERSION,
            records: records
                .into_iter()
                .filter_map(PersistedBanRecordV1::from_runtime)
                .collect(),
        };
        let config = BanningConfig {
            whitelist: vec![whitelisted],
            blacklist: vec![blacklisted],
            ..BanningConfig::default()
        };

        let (restored, records, stats) = checkpoint.reconcile(&config, now).unwrap();

        assert_eq!(records.len(), 1);
        assert_eq!(records[0].peer_id, active);
        assert_eq!(records[0].expires_at, Some(now + Duration::from_secs(60)));
        assert_eq!(restored.records.len(), 1);
        assert_eq!(stats.expired, 1);
        assert_eq!(stats.overridden_by_config, 2);

        let mut store = BanStore::from_config(&config);
        store.restore_dynamic(records);
        let restored_peers = store
            .active()
            .into_iter()
            .map(|record| record.peer_id)
            .collect::<HashSet<_>>();
        assert_eq!(restored_peers, HashSet::from([active, blacklisted]));
    }

    #[test]
    fn recovery_deduplicates_by_furthest_expiry_with_deterministic_ties() {
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(100);
        let peer_id = lb_libp2p::PeerId::random();
        let scope = BanScope::Service(Subsystem::ChainSync);
        let first = dynamic_record(
            peer_id,
            scope.clone(),
            Subsystem::Blend,
            now - Duration::from_secs(2),
            now + Duration::from_secs(20),
        );
        let second = dynamic_record(
            peer_id,
            scope,
            Subsystem::Libp2p,
            now - Duration::from_secs(1),
            now + Duration::from_secs(40),
        );
        let checkpoint = BanningRecoveryState {
            version: FORMAT_VERSION,
            records: vec![
                PersistedBanRecordV1::from_runtime(first).unwrap(),
                PersistedBanRecordV1::from_runtime(second.clone()).unwrap(),
            ],
        };
        let config = BanningConfig::default();

        let (_, records, stats) = checkpoint.reconcile(&config, now).unwrap();
        assert_eq!(records, vec![second]);
        assert_eq!(stats.duplicates, 1);
    }

    #[test]
    fn malformed_record_is_rejected_without_resurrecting_it() {
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(100);
        let peer_id = lb_libp2p::PeerId::random();
        let malformed = dynamic_record(peer_id, BanScope::Global, Subsystem::ChainSync, now, now);
        let checkpoint = BanningRecoveryState {
            version: FORMAT_VERSION,
            records: vec![PersistedBanRecordV1::from_runtime(malformed).unwrap()],
        };

        let (normalized, records, stats) = checkpoint
            .reconcile(&BanningConfig::default(), now - Duration::from_secs(1))
            .unwrap();
        assert!(records.is_empty());
        assert!(normalized.records.is_empty());
        assert_eq!(stats.invalid, 1);
    }

    #[test]
    fn unsupported_corrupt_and_missing_checkpoints_fail_open() {
        let data = RecoveryData::default();
        let settings = config(data.clone());

        assert!(BanningRecoveryBackend::<()>::load(&settings).is_none());

        data.insert(
            recovery_key(BanningConfig::RECOVERY_KEY_SUFFIX).to_vec(),
            b"not bincode".as_slice().into(),
        )
        .unwrap();
        assert!(BanningRecoveryBackend::<()>::load(&settings).is_none());

        let unsupported = BanningRecoveryState {
            version: FORMAT_VERSION + 1,
            records: Vec::new(),
        }
        .to_bytes()
        .unwrap();
        data.insert(
            recovery_key(BanningConfig::RECOVERY_KEY_SUFFIX).to_vec(),
            unsupported,
        )
        .unwrap();
        assert!(BanningRecoveryBackend::<()>::load(&settings).is_none());
    }

    #[test]
    fn recovery_cache_contains_latest_checkpoint_after_a_service_restart_take() {
        let data = RecoveryData::default();
        let settings = config(data.clone());
        let peer_id = lb_libp2p::PeerId::random();
        let now = SystemTime::now();
        let checkpoint = BanningRecoveryState {
            version: FORMAT_VERSION,
            records: vec![
                PersistedBanRecordV1::from_runtime(dynamic_record(
                    peer_id,
                    BanScope::Global,
                    Subsystem::ChainSync,
                    now,
                    now + Duration::from_secs(60),
                ))
                .unwrap(),
            ],
        };

        BanningRecoveryBackend::<()>::cache_state(&data, &checkpoint).unwrap();
        let first_start =
            BanningRecoveryBackend::<()>::load(&settings).expect("recovery data should load");
        assert_eq!(first_start.records.len(), 1);
        // Loading consumes RecoveryData::take(), so the banning backend
        // immediately retains the normalized snapshot for a same-process
        // service restart, even before the initial state update is saved.
        let restarted = BanningRecoveryBackend::<()>::load(&settings)
            .expect("latest checkpoint should survive service restart");
        assert_eq!(restarted.records, first_start.records);
        assert_eq!(restarted.records[0].peer_id, peer_id);
    }

    #[test]
    fn duplicate_recovery_selection_is_independent_of_input_order() {
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(100);
        let peer = lb_libp2p::PeerId::random();
        let scope = BanScope::Global;
        let a = dynamic_record(
            peer,
            scope.clone(),
            Subsystem::Blend,
            now - Duration::from_secs(1),
            now + Duration::from_secs(40),
        );
        let b = dynamic_record(
            peer,
            scope,
            Subsystem::ChainSync,
            now - Duration::from_secs(2),
            now + Duration::from_secs(40),
        );
        let a = PersistedBanRecordV1::from_runtime(a).unwrap();
        let b = PersistedBanRecordV1::from_runtime(b).unwrap();
        let config = BanningConfig::default();
        let first = BanningRecoveryState {
            version: FORMAT_VERSION,
            records: vec![a.clone(), b.clone()],
        }
        .reconcile(&config, now)
        .unwrap()
        .1;
        let second = BanningRecoveryState {
            version: FORMAT_VERSION,
            records: vec![b, a],
        }
        .reconcile(&config, now)
        .unwrap()
        .1;

        assert_eq!(first, second);
    }

    #[test]
    fn recovered_records_preserve_monotonic_store_semantics() {
        let peer = lb_libp2p::PeerId::random();
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(10_000);
        let expires_at = now + Duration::from_secs(120);
        let record = dynamic_record(
            peer,
            BanScope::Service(Subsystem::ChainSync),
            Subsystem::ChainSync,
            now,
            expires_at,
        );
        let state = BanningRecoveryState {
            version: FORMAT_VERSION,
            records: vec![PersistedBanRecordV1::from_runtime(record).unwrap()],
        };
        let config = BanningConfig::default();
        let (_, restored, _) = state
            .reconcile(&config, now + Duration::from_secs(20))
            .unwrap();
        let mut store = BanStore::with_clock(&config, FixedClock(now + Duration::from_secs(20)));
        store.restore_dynamic(restored);

        let unchanged = store
            .report(crate::Violation::new(
                peer,
                BanSource::Service(Subsystem::Blend),
                BanScope::Service(Subsystem::ChainSync),
                OffenseKind::Other,
                Duration::from_secs(30),
                Some("weaker".to_owned()),
            ))
            .unwrap();
        assert_eq!(unchanged.record().expires_at, Some(expires_at));

        let extended = store
            .report(crate::Violation::new(
                peer,
                BanSource::Service(Subsystem::Blend),
                BanScope::Service(Subsystem::ChainSync),
                OffenseKind::Other,
                Duration::from_secs(200),
                Some("extension".to_owned()),
            ))
            .unwrap();
        assert_eq!(
            extended.record().expires_at,
            Some(now + Duration::from_secs(220))
        );

        let replaced = store
            .replace(crate::Violation::new(
                peer,
                BanSource::Service(Subsystem::Libp2p),
                BanScope::Service(Subsystem::ChainSync),
                OffenseKind::InvalidSig,
                Duration::from_secs(10),
                Some("deliberately shortened".to_owned()),
            ))
            .unwrap();
        assert_eq!(
            replaced.record().expires_at,
            Some(now + Duration::from_secs(30))
        );

        assert_eq!(store.unban(peer, None).len(), 1);
        let shorter = store
            .report(crate::Violation::new(
                peer,
                BanSource::Service(Subsystem::ChainSync),
                BanScope::Service(Subsystem::ChainSync),
                OffenseKind::Other,
                Duration::from_secs(5),
                None,
            ))
            .unwrap();
        assert_eq!(
            shorter.record().expires_at,
            Some(now + Duration::from_secs(25))
        );
    }

    #[derive(Clone)]
    struct FixedClock(SystemTime);

    impl crate::Clock for FixedClock {
        fn now(&self) -> SystemTime {
            self.0
        }
    }

    #[test]
    fn normalization_keeps_each_scope_as_an_independent_record() {
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(10);
        let peer = lb_libp2p::PeerId::random();
        let state = BanningRecoveryState {
            version: FORMAT_VERSION,
            records: vec![
                dynamic_record(
                    peer,
                    BanScope::Global,
                    Subsystem::Libp2p,
                    now - Duration::from_secs(1),
                    now + Duration::from_secs(10),
                ),
                dynamic_record(
                    peer,
                    BanScope::Service(Subsystem::ChainSync),
                    Subsystem::ChainSync,
                    now - Duration::from_secs(1),
                    now + Duration::from_secs(20),
                ),
            ]
            .into_iter()
            .filter_map(PersistedBanRecordV1::from_runtime)
            .collect(),
        };

        let (_, records, _) = state.reconcile(&BanningConfig::default(), now).unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(
            records
                .iter()
                .map(|record| record.peer_id)
                .collect::<HashSet<_>>()
                .len(),
            1
        );
    }
}
