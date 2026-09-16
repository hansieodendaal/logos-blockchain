use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex, RwLock},
    time::{Duration, SystemTime},
};

use lb_banning_service::{
    BanEvent, BanRecord, BanScope, BanSource, BanningApiError, BanningServiceApi,
    ConfiguredBanPolicy,
};
use lb_libp2p::PeerId;
use lb_log_targets::network_service;
use lb_utils::tokio::task::{CancellableHandle, spawn};
use tokio::{sync::broadcast, time::Instant as TokioInstant};

const LOG_TARGET: &str = network_service::ROOT;
const DYNAMIC_AUTHORITY_LEASE: Duration = Duration::from_secs(10);
const DYNAMIC_REFRESH_INTERVAL: Duration = Duration::from_secs(5);
const INITIAL_RETRY_BACKOFF: Duration = Duration::from_secs(1);
const MAX_RETRY_BACKOFF: Duration = Duration::from_secs(30);

/// A local, fail-open view of bans.
///
/// The view always enforces the configured blacklist. Dynamic records are
/// enforced only while the view has a valid lease from the central service.
/// Consumers perform synchronous reads; all interaction with the central
/// service happens in the single background synchronizer.
#[derive(Clone)]
pub struct LocalBanView {
    inner: Arc<LocalBanViewInner>,
}

struct LocalBanViewInner {
    configured_policy: ConfiguredBanPolicy,
    state: Arc<RwLock<LocalBanViewState>>,
    synchronizer: Mutex<Option<CancellableHandle<()>>>,
}

#[derive(Debug, Default)]
struct LocalBanViewState {
    dynamic_authority_valid_until: Option<TokioInstant>,
    dynamic_records: HashMap<(PeerId, BanScope), BanRecord>,
}

impl LocalBanView {
    /// Creates a view and, when an API is supplied, starts its one
    /// cancellation-bound synchronizer.
    #[must_use]
    pub fn new<RuntimeServiceId>(
        banning_service: Option<BanningServiceApi<RuntimeServiceId>>,
        configured_policy: ConfiguredBanPolicy,
    ) -> Self
    where
        RuntimeServiceId: Send + Sync + 'static,
    {
        let view = Self {
            inner: Arc::new(LocalBanViewInner {
                configured_policy,
                state: Arc::new(RwLock::new(LocalBanViewState::default())),
                synchronizer: Mutex::new(None),
            }),
        };

        if let Some(banning_service) = banning_service {
            view.start_synchronizer(banning_service);
        }

        view
    }

    /// Starts synchronization if this view does not already own a
    /// synchronizer. This is useful when a relay is obtained after the view
    /// has been constructed; repeated calls never create another task.
    pub fn start_synchronizer<RuntimeServiceId>(
        &self,
        banning_service: BanningServiceApi<RuntimeServiceId>,
    ) where
        RuntimeServiceId: Send + Sync + 'static,
    {
        let Ok(mut synchronizer) = self.inner.synchronizer.lock() else {
            tracing::error!(target: LOG_TARGET, "failed to lock local banning synchronizer state");
            return;
        };

        if synchronizer.is_some() {
            return;
        }

        let state = Arc::clone(&self.inner.state);
        *synchronizer = Some(CancellableHandle::new(spawn(
            "logos/network/chainsync-ban-sync",
            async move {
                Self::run_synchronizer(banning_service, state).await;
            },
        )));
    }

    /// Returns whether a peer is banned for the requested consumer scope.
    /// This method never waits for the central service.
    #[must_use]
    pub fn is_banned_for(&self, peer_id: PeerId, scope: &BanScope) -> bool {
        let now = SystemTime::now();
        let Ok(state) = self.inner.state.read() else {
            return self.inner.configured_policy.contains(&peer_id);
        };

        self.inner.configured_policy.contains(&peer_id)
            || (Self::authority_valid_at(&state, TokioInstant::now())
                && state.dynamic_records.values().any(|record| {
                    record.peer_id == peer_id
                        && record.applies_to(scope)
                        && Self::is_effective_at(record, now)
                }))
    }

    /// Removes banned peers from a candidate set before a consumer applies
    /// its own bounded selection policy.
    #[must_use]
    pub fn filter_for(&self, peers: &HashSet<PeerId>, scope: &BanScope) -> HashSet<PeerId> {
        let now = SystemTime::now();
        let Ok(state) = self.inner.state.read() else {
            return peers
                .iter()
                .copied()
                .filter(|peer_id| !self.inner.configured_policy.contains(peer_id))
                .collect();
        };

        let authority_valid = Self::authority_valid_at(&state, TokioInstant::now());
        peers
            .iter()
            .copied()
            .filter(|peer_id| {
                !self.inner.configured_policy.contains(peer_id)
                    && (!authority_valid
                        || !state.dynamic_records.values().any(|record| {
                            record.peer_id == *peer_id
                                && record.applies_to(scope)
                                && Self::is_effective_at(record, now)
                        }))
            })
            .collect()
    }

    /// Installs an authoritative dynamic snapshot atomically.
    pub fn replace_dynamic_snapshot(&self, records: Vec<BanRecord>) {
        Self::install_snapshot_in(&self.inner.state, records);
    }

    /// Invalidates dynamic authority while preserving configured policy.
    pub fn invalidate_dynamic_authority(&self) {
        Self::disable_in(&self.inner.state);
    }

    fn is_effective_at(record: &BanRecord, now: SystemTime) -> bool {
        record.expires_at.is_none_or(|expiry| now < expiry)
    }

    fn authority_valid_at(state: &LocalBanViewState, now: TokioInstant) -> bool {
        state
            .dynamic_authority_valid_until
            .is_some_and(|valid_until| now < valid_until)
    }

    fn install_snapshot_in(state: &Arc<RwLock<LocalBanViewState>>, records: Vec<BanRecord>) {
        let Ok(mut state) = state.write() else {
            return;
        };

        let wall_now = SystemTime::now();
        state.dynamic_records = records
            .into_iter()
            .filter(|record| {
                record.source != BanSource::Configuration && Self::is_effective_at(record, wall_now)
            })
            .map(|record| ((record.peer_id, record.scope.clone()), record))
            .collect();
        state.dynamic_authority_valid_until = Some(TokioInstant::now() + DYNAMIC_AUTHORITY_LEASE);
    }

    fn disable_in(state: &Arc<RwLock<LocalBanViewState>>) {
        let Ok(mut state) = state.write() else {
            return;
        };
        state.dynamic_authority_valid_until = None;
        state.dynamic_records.clear();
    }

    fn apply_event_in(state: &Arc<RwLock<LocalBanViewState>>, event: BanEvent) {
        let Ok(mut state) = state.write() else {
            return;
        };

        if !Self::authority_valid_at(&state, TokioInstant::now()) {
            state.dynamic_authority_valid_until = None;
            state.dynamic_records.clear();
            return;
        }

        match event {
            BanEvent::Banned(record) | BanEvent::Replaced(record) => {
                if record.source == BanSource::Configuration {
                    return;
                }
                let key = (record.peer_id, record.scope.clone());
                if Self::is_effective_at(&record, SystemTime::now()) {
                    state.dynamic_records.insert(key, record);
                } else {
                    state.dynamic_records.remove(&key);
                }
            }
            BanEvent::Unbanned { record, .. } => {
                if record.source != BanSource::Configuration {
                    state
                        .dynamic_records
                        .remove(&(record.peer_id, record.scope));
                }
            }
        }
    }

    async fn synchronize<RuntimeServiceId>(
        banning_service: &BanningServiceApi<RuntimeServiceId>,
        state: &Arc<RwLock<LocalBanViewState>>,
    ) -> Result<broadcast::Receiver<BanEvent>, BanningApiError>
    where
        RuntimeServiceId: Send + Sync,
    {
        let receiver = banning_service.subscribe().await.inspect_err(|_| {
            Self::disable_in(state);
        })?;
        Self::refresh_dynamic(banning_service, state)
            .await
            .inspect_err(|_| {
                Self::disable_in(state);
            })?;
        Ok(receiver)
    }

    async fn refresh_dynamic<RuntimeServiceId>(
        banning_service: &BanningServiceApi<RuntimeServiceId>,
        state: &Arc<RwLock<LocalBanViewState>>,
    ) -> Result<(), BanningApiError>
    where
        RuntimeServiceId: Send + Sync,
    {
        match banning_service.list_active().await {
            Ok(records) => {
                Self::install_snapshot_in(state, records);
                Ok(())
            }
            Err(error) => {
                Self::disable_in(state);
                Err(error)
            }
        }
    }

    async fn run_synchronizer<RuntimeServiceId>(
        banning_service: BanningServiceApi<RuntimeServiceId>,
        state: Arc<RwLock<LocalBanViewState>>,
    ) where
        RuntimeServiceId: Send + Sync + 'static,
    {
        Self::run_synchronizer_with_backoff(
            banning_service,
            state,
            INITIAL_RETRY_BACKOFF,
            MAX_RETRY_BACKOFF,
        )
        .await;
    }

    #[cfg(test)]
    async fn run_synchronizer_for_test<RuntimeServiceId>(
        banning_service: BanningServiceApi<RuntimeServiceId>,
        state: Arc<RwLock<LocalBanViewState>>,
        initial_retry_backoff: Duration,
        max_retry_backoff: Duration,
    ) where
        RuntimeServiceId: Send + Sync + 'static,
    {
        Self::run_synchronizer_with_backoff(
            banning_service,
            state,
            initial_retry_backoff,
            max_retry_backoff,
        )
        .await;
    }

    #[expect(
        clippy::cognitive_complexity,
        reason = "The retry state machine deliberately keeps authority transitions together."
    )]
    async fn run_synchronizer_with_backoff<RuntimeServiceId>(
        banning_service: BanningServiceApi<RuntimeServiceId>,
        state: Arc<RwLock<LocalBanViewState>>,
        initial_retry_backoff: Duration,
        max_retry_backoff: Duration,
    ) where
        RuntimeServiceId: Send + Sync + 'static,
    {
        let mut retry_backoff = initial_retry_backoff;
        let mut unavailable = false;

        loop {
            match Self::synchronize(&banning_service, &state).await {
                Ok(mut receiver) => {
                    tracing::info!(target: LOG_TARGET, "dynamic banning authority established");
                    retry_backoff = initial_retry_backoff;
                    unavailable = false;
                    Self::monitor_subscription(&banning_service, &mut receiver, &state).await;
                }
                Err(error) => {
                    Self::disable_in(&state);
                    if unavailable {
                        tracing::debug!(
                            target: LOG_TARGET,
                            %error,
                            "dynamic banning authority refresh failed; enforcement remains disabled"
                        );
                    } else {
                        tracing::warn!(
                            target: LOG_TARGET,
                            %error,
                            "dynamic banning authority unavailable; enforcement disabled"
                        );
                        unavailable = true;
                    }
                }
            }

            tokio::time::sleep(retry_backoff).await;
            retry_backoff = retry_backoff.saturating_mul(2).min(max_retry_backoff);
        }
    }

    #[cfg(test)]
    fn next_retry_backoff(current: Duration, maximum: Duration) -> Duration {
        current.saturating_mul(2).min(maximum)
    }

    #[expect(
        clippy::cognitive_complexity,
        reason = "The subscription monitor owns the bounded lease and refresh transitions."
    )]
    async fn monitor_subscription<RuntimeServiceId>(
        banning_service: &BanningServiceApi<RuntimeServiceId>,
        receiver: &mut broadcast::Receiver<BanEvent>,
        state: &Arc<RwLock<LocalBanViewState>>,
    ) where
        RuntimeServiceId: Send + Sync,
    {
        let deadline = Self::lease_deadline(state).unwrap_or_else(TokioInstant::now);
        let mut lease_expiry = Box::pin(tokio::time::sleep_until(deadline));
        let mut refresh = Box::pin(tokio::time::sleep(DYNAMIC_REFRESH_INTERVAL));

        loop {
            if !Self::authority_valid(state) {
                Self::disable_in(state);
                tracing::warn!(target: LOG_TARGET, "dynamic banning authority lease expired");
                return;
            }

            tokio::select! {
                event = receiver.recv() => match event {
                    Ok(event) => Self::apply_event_in(state, event),
                    Err(broadcast::error::RecvError::Closed) => {
                        Self::disable_in(state);
                        tracing::warn!(target: LOG_TARGET, "banning subscription closed; dynamic enforcement disabled");
                        return;
                    }
                    Err(broadcast::error::RecvError::Lagged(skipped)) => {
                        Self::disable_in(state);
                        tracing::warn!(target: LOG_TARGET, skipped, "banning subscription lagged; dynamic enforcement disabled");
                        return;
                    }
                },
                () = &mut lease_expiry => {
                    Self::disable_in(state);
                    tracing::warn!(target: LOG_TARGET, "dynamic banning authority lease expired");
                    return;
                }
                () = &mut refresh => {
                    match Self::refresh_dynamic(banning_service, state).await {
                        Ok(()) => {
                            refresh.as_mut().reset(TokioInstant::now() + DYNAMIC_REFRESH_INTERVAL);
                            if let Some(deadline) = Self::lease_deadline(state) {
                                lease_expiry.as_mut().reset(deadline);
                            }
                        }
                        Err(error) => {
                            Self::disable_in(state);
                            tracing::warn!(target: LOG_TARGET, %error, "authoritative banning refresh failed; dynamic enforcement disabled");
                            return;
                        }
                    }
                }
            }
        }
    }

    fn lease_deadline(state: &Arc<RwLock<LocalBanViewState>>) -> Option<TokioInstant> {
        state.read().ok()?.dynamic_authority_valid_until
    }

    fn authority_valid(state: &Arc<RwLock<LocalBanViewState>>) -> bool {
        state
            .read()
            .is_ok_and(|state| Self::authority_valid_at(&state, TokioInstant::now()))
    }
}

#[cfg(test)]
mod tests {
    use lb_banning_service::{BanningConfig, BanningRequest, OffenseKind, Subsystem};
    use overwatch::services::relay::OutboundRelay;
    use tokio::sync::{mpsc, oneshot};

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
    fn scope_filtering_and_configured_policy_are_independent() {
        let chain_peer = PeerId::random();
        let blend_peer = PeerId::random();
        let global_peer = PeerId::random();
        let configured_peer = PeerId::random();
        let policy = ConfiguredBanPolicy::from_config(&BanningConfig {
            blacklist: vec![configured_peer],
            ..Default::default()
        });
        let view = LocalBanView::new::<()>(None, policy);
        view.replace_dynamic_snapshot(vec![
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

        let chain_scope = BanScope::Service(Subsystem::ChainSync);
        let blend_scope = BanScope::Service(Subsystem::Blend);
        assert!(view.is_banned_for(chain_peer, &chain_scope));
        assert!(!view.is_banned_for(blend_peer, &chain_scope));
        assert!(view.is_banned_for(global_peer, &chain_scope));
        assert!(view.is_banned_for(configured_peer, &blend_scope));
    }

    #[tokio::test(start_paused = true)]
    async fn lease_expiry_fails_open_but_configured_policy_remains() {
        let dynamic_peer = PeerId::random();
        let configured_peer = PeerId::random();
        let policy = ConfiguredBanPolicy::from_config(&BanningConfig {
            blacklist: vec![configured_peer],
            ..Default::default()
        });
        let view = LocalBanView::new::<()>(None, policy);
        let scope = BanScope::Service(Subsystem::ChainSync);
        view.replace_dynamic_snapshot(vec![record(
            dynamic_peer,
            scope.clone(),
            BanSource::Service(Subsystem::ChainSync),
        )]);

        assert!(view.is_banned_for(dynamic_peer, &scope));
        tokio::time::advance(DYNAMIC_AUTHORITY_LEASE + Duration::from_secs(1)).await;
        assert!(!view.is_banned_for(dynamic_peer, &scope));
        assert!(view.is_banned_for(configured_peer, &scope));
    }

    #[test]
    fn expired_finite_records_are_not_enforced_without_an_event() {
        let peer = PeerId::random();
        let mut record = record(
            peer,
            BanScope::Service(Subsystem::ChainSync),
            BanSource::Service(Subsystem::ChainSync),
        );
        record.expires_at = Some(SystemTime::UNIX_EPOCH);
        let view = LocalBanView::new::<()>(None, ConfiguredBanPolicy::default());
        view.replace_dynamic_snapshot(vec![record]);
        assert!(!view.is_banned_for(peer, &BanScope::Service(Subsystem::ChainSync)));
    }

    #[test]
    fn invalidation_does_not_clear_configured_policy() {
        let peer = PeerId::random();
        let policy = ConfiguredBanPolicy::from_config(&BanningConfig {
            blacklist: vec![peer],
            ..Default::default()
        });
        let view = LocalBanView::new::<()>(None, policy);
        view.invalidate_dynamic_authority();
        assert!(view.is_banned_for(peer, &BanScope::Global));
    }

    #[tokio::test]
    async fn synchronizer_installs_snapshot_and_applies_unban_events() {
        let peer = PeerId::random();
        let active = record(
            peer,
            BanScope::Service(Subsystem::ChainSync),
            BanSource::Service(Subsystem::ChainSync),
        );
        let (sender, mut requests) = mpsc::channel(2);
        let (event_sender, _) = broadcast::channel(4);
        let event_sender_for_handler = event_sender.clone();
        let handler = tokio::spawn(async move {
            let BanningRequest::Subscribe { reply } = requests.recv().await.expect("subscribe")
            else {
                panic!("expected subscribe request");
            };
            reply
                .send(event_sender_for_handler.subscribe())
                .expect("subscription reply");
            let BanningRequest::ListActive { reply } = requests.recv().await.expect("snapshot")
            else {
                panic!("expected snapshot request");
            };
            reply.send(vec![active]).expect("snapshot reply");
        });
        let api = BanningServiceApi::<()>::new(OutboundRelay::new(sender));
        let view = LocalBanView::new(Some(api), ConfiguredBanPolicy::default());
        handler.await.expect("request handler");

        let scope = BanScope::Service(Subsystem::ChainSync);
        assert!(view.is_banned_for(peer, &scope));

        let state = Arc::clone(&view.inner.state);
        LocalBanView::apply_event_in(
            &state,
            BanEvent::Unbanned {
                record: record(
                    peer,
                    BanScope::Service(Subsystem::ChainSync),
                    BanSource::Service(Subsystem::ChainSync),
                ),
                expired: false,
            },
        );
        assert!(!view.is_banned_for(peer, &scope));
    }

    #[tokio::test]
    async fn subscription_close_and_lag_fail_open_without_clearing_configuration() {
        let dynamic_peer = PeerId::random();
        let configured_peer = PeerId::random();
        let policy = ConfiguredBanPolicy::from_config(&BanningConfig {
            blacklist: vec![configured_peer],
            ..Default::default()
        });
        let view = LocalBanView::new::<()>(None, policy);
        let scope = BanScope::Service(Subsystem::ChainSync);
        view.replace_dynamic_snapshot(vec![record(
            dynamic_peer,
            scope.clone(),
            BanSource::Service(Subsystem::ChainSync),
        )]);

        let (event_sender, receiver) = broadcast::channel(1);
        let (sender, _) = mpsc::channel(1);
        let api = BanningServiceApi::<()>::new(OutboundRelay::new(sender));
        let state = Arc::clone(&view.inner.state);
        let monitor = tokio::spawn(async move {
            let mut receiver = receiver;
            LocalBanView::monitor_subscription(&api, &mut receiver, &state).await;
        });
        drop(event_sender);
        monitor.await.expect("closed monitor");
        assert!(!view.is_banned_for(dynamic_peer, &scope));
        assert!(view.is_banned_for(configured_peer, &scope));

        view.replace_dynamic_snapshot(vec![record(
            dynamic_peer,
            scope.clone(),
            BanSource::Service(Subsystem::ChainSync),
        )]);
        let (event_sender, receiver) = broadcast::channel(1);
        let event = BanEvent::Banned(record(
            dynamic_peer,
            scope.clone(),
            BanSource::Service(Subsystem::ChainSync),
        ));
        event_sender.send(event.clone()).expect("first event");
        event_sender.send(event).expect("second event");
        let (sender, _) = mpsc::channel(1);
        let api = BanningServiceApi::<()>::new(OutboundRelay::new(sender));
        let state = Arc::clone(&view.inner.state);
        let monitor = tokio::spawn(async move {
            let mut receiver = receiver;
            LocalBanView::monitor_subscription(&api, &mut receiver, &state).await;
        });
        monitor.await.expect("lagged monitor");
        assert!(!view.is_banned_for(dynamic_peer, &scope));
        assert!(view.is_banned_for(configured_peer, &scope));
    }

    #[tokio::test(start_paused = true)]
    async fn stalled_refresh_times_out_and_preserves_configured_policy() {
        let dynamic_peer = PeerId::random();
        let configured_peer = PeerId::random();
        let view = LocalBanView::new::<()>(
            None,
            ConfiguredBanPolicy::from_config(&BanningConfig {
                blacklist: vec![configured_peer],
                ..Default::default()
            }),
        );
        let scope = BanScope::Service(Subsystem::ChainSync);
        view.replace_dynamic_snapshot(vec![record(
            dynamic_peer,
            scope.clone(),
            BanSource::Service(Subsystem::ChainSync),
        )]);
        let (sender, mut requests) = mpsc::channel(1);
        let (seen_sender, seen_receiver) = oneshot::channel();
        let handler = tokio::spawn(async move {
            let BanningRequest::ListActive { reply } = requests.recv().await.expect("refresh")
            else {
                panic!("expected refresh request");
            };
            seen_sender.send(()).expect("refresh observed");
            let _reply = reply;
            std::future::pending::<()>().await;
        });
        let api = BanningServiceApi::<()>::new(OutboundRelay::new(sender));
        let state = Arc::clone(&view.inner.state);
        let refresh =
            tokio::spawn(async move { LocalBanView::refresh_dynamic(&api, &state).await });
        seen_receiver.await.expect("refresh observed");
        tokio::time::advance(Duration::from_secs(2)).await;
        assert!(matches!(
            refresh.await.expect("refresh task"),
            Err(BanningApiError::Timeout)
        ));
        assert!(!view.is_banned_for(dynamic_peer, &scope));
        assert!(view.is_banned_for(configured_peer, &scope));
        handler.abort();
    }

    #[test]
    fn retry_backoff_is_exponential_and_capped() {
        let mut backoff = INITIAL_RETRY_BACKOFF;
        let mut observed = [Duration::ZERO; 8];
        for delay in &mut observed {
            *delay = backoff;
            backoff = LocalBanView::next_retry_backoff(backoff, MAX_RETRY_BACKOFF);
        }
        assert_eq!(
            observed,
            [
                Duration::from_secs(1),
                Duration::from_secs(2),
                Duration::from_secs(4),
                Duration::from_secs(8),
                Duration::from_secs(16),
                Duration::from_secs(30),
                Duration::from_secs(30),
                Duration::from_secs(30),
            ]
        );
    }

    #[tokio::test]
    async fn synchronizer_recovers_after_more_failures_than_the_old_budget() {
        let peer = PeerId::random();
        let active = record(
            peer,
            BanScope::Service(Subsystem::ChainSync),
            BanSource::Service(Subsystem::ChainSync),
        );
        let (sender, mut requests) = mpsc::channel(8);
        let (event_sender, _) = broadcast::channel(8);
        let (recovered_sender, recovered_receiver) = oneshot::channel();
        let handler = tokio::spawn(async move {
            let mut failed = 0;
            let mut recovered_sender = Some(recovered_sender);
            while let Some(request) = requests.recv().await {
                match request {
                    BanningRequest::Subscribe { reply } if failed < 8 => {
                        failed += 1;
                        drop(reply);
                    }
                    BanningRequest::Subscribe { reply } => {
                        reply
                            .send(event_sender.subscribe())
                            .expect("subscription reply");
                    }
                    BanningRequest::ListActive { reply } => {
                        reply.send(vec![active.clone()]).expect("snapshot reply");
                        if let Some(sender) = recovered_sender.take() {
                            sender.send(()).expect("recovery remains alive");
                        }
                    }
                    _ => panic!("unexpected banning request"),
                }
            }
        });
        let api = BanningServiceApi::<()>::new(OutboundRelay::new(sender));
        let state = Arc::new(RwLock::new(LocalBanViewState::default()));
        let synchronizer = tokio::spawn(LocalBanView::run_synchronizer_for_test(
            api,
            Arc::clone(&state),
            Duration::ZERO,
            Duration::ZERO,
        ));
        recovered_receiver.await.expect("synchronizer recovers");
        let scope = BanScope::Service(Subsystem::ChainSync);
        assert!(
            state
                .read()
                .expect("state")
                .dynamic_records
                .values()
                .any(|record| record.peer_id == peer && record.applies_to(&scope))
        );
        synchronizer.abort();
        handler.abort();
    }

    #[tokio::test]
    async fn dropping_the_last_view_cancels_its_synchronizer() {
        let (sender, mut requests) = mpsc::channel(1);
        let api = BanningServiceApi::<()>::new(OutboundRelay::new(sender));
        let view = LocalBanView::new(Some(api), ConfiguredBanPolicy::default());
        let view_clone = view.clone();
        let _request = requests.recv().await.expect("subscribe request");
        drop(view);
        drop(view_clone);
        assert!(
            tokio::time::timeout(Duration::from_secs(1), requests.recv())
                .await
                .expect("relay closes after view drop")
                .is_none()
        );
    }
}
