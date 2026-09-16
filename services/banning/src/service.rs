use std::{
    collections::HashSet,
    fmt::{Debug, Display},
};

use async_trait::async_trait;
use lb_libp2p::PeerId;
use overwatch::{
    OpaqueServiceResourcesHandle,
    services::{
        AsServiceId, ServiceCore, ServiceData,
        state::{NoOperator, ServiceState},
    },
};
use tokio::sync::broadcast;

use crate::{
    BanningConfig,
    ban_store::{BanMutation, BanStore, Clock, SystemClock},
    types::{BanEvent, BanRecord, BanScope, BanningRequest, Violation},
};

const EVENT_BUFFER_SIZE: usize = 256;

/// Mutable ban state retained by [`BanningService`].
///
/// Runtime event plumbing intentionally lives on the service itself so clones
/// retained by the state machinery cannot keep subscriptions alive after the
/// service stops.
#[derive(Debug, Clone)]
pub struct BanningState<C = SystemClock> {
    pub(crate) store: BanStore<C>,
}

impl BanningState<SystemClock> {
    pub(crate) fn from_config(config: &BanningConfig) -> Self {
        Self::with_store(BanStore::from_config(config))
    }
}

impl<C: Clock> BanningState<C> {
    pub(crate) const fn with_store(store: BanStore<C>) -> Self {
        Self { store }
    }

    pub(crate) fn expire(&mut self) -> Vec<BanEvent> {
        self.store
            .expire()
            .into_iter()
            .map(|record| BanEvent::Unbanned {
                record,
                expired: true,
            })
            .collect()
    }

    fn publish(sender: &broadcast::Sender<BanEvent>, events: impl IntoIterator<Item = BanEvent>) {
        for event in events {
            // A send error only means that there are no current subscribers;
            // the authoritative state remains in the store.
            let _unused = sender.send(event);
        }
    }

    pub(crate) fn report(
        &mut self,
        violation: Violation,
        events: &broadcast::Sender<BanEvent>,
    ) -> Option<BanRecord> {
        Self::publish(events, self.expire());
        let mutation = self.store.report(violation);
        if let Some(mutation) = &mutation
            && !matches!(mutation, BanMutation::Unchanged(_))
        {
            Self::publish(events, [BanEvent::Banned(mutation.record().clone())]);
        }
        mutation.map(|mutation| mutation.record().clone())
    }

    pub(crate) fn replace(
        &mut self,
        violation: Violation,
        events: &broadcast::Sender<BanEvent>,
    ) -> Option<BanRecord> {
        Self::publish(events, self.expire());
        let mutation = self.store.replace(violation);
        if let Some(mutation) = &mutation {
            if matches!(mutation, BanMutation::Unchanged(_)) {
                return Some(mutation.record().clone());
            }
            Self::publish(events, [BanEvent::Replaced(mutation.record().clone())]);
        }
        mutation.map(|mutation| mutation.record().clone())
    }

    pub(crate) fn query(
        &mut self,
        peer_id: PeerId,
        scope: &BanScope,
        events: &broadcast::Sender<BanEvent>,
    ) -> Vec<BanRecord> {
        Self::publish(events, self.expire());
        self.store.query(peer_id, scope)
    }

    pub(crate) fn query_many(
        &mut self,
        peer_ids: &HashSet<PeerId>,
        scope: &BanScope,
        events: &broadcast::Sender<BanEvent>,
    ) -> Vec<BanRecord> {
        Self::publish(events, self.expire());
        self.store.query_many(peer_ids, scope)
    }

    pub(crate) fn active(&mut self, events: &broadcast::Sender<BanEvent>) -> Vec<BanRecord> {
        Self::publish(events, self.expire());
        self.store.active()
    }

    pub(crate) fn unban(
        &mut self,
        peer_id: PeerId,
        scope: Option<&BanScope>,
        events: &broadcast::Sender<BanEvent>,
    ) -> bool {
        Self::publish(events, self.expire());
        let removed = self.store.unban(peer_id, scope);
        let was_unbanned = !removed.is_empty();
        Self::publish(
            events,
            removed.into_iter().map(|record| BanEvent::Unbanned {
                record,
                expired: false,
            }),
        );
        was_unbanned
    }
}

impl ServiceState for BanningState<SystemClock> {
    type Settings = BanningConfig;
    type Error = overwatch::DynError;

    fn from_settings(settings: &Self::Settings) -> Result<Self, Self::Error> {
        Ok(Self::from_config(settings))
    }
}

/// Central in-memory peer banning service.
pub struct BanningService<RuntimeServiceId> {
    service_resources_handle: OpaqueServiceResourcesHandle<Self, RuntimeServiceId>,
    state: BanningState,
    events: broadcast::Sender<BanEvent>,
}

impl<RuntimeServiceId> ServiceData for BanningService<RuntimeServiceId> {
    type Settings = BanningConfig;
    type State = BanningState;
    type StateOperator = NoOperator<Self::State>;
    type Message = BanningRequest;
}

#[async_trait]
impl<RuntimeServiceId> ServiceCore<RuntimeServiceId> for BanningService<RuntimeServiceId>
where
    RuntimeServiceId: AsServiceId<Self> + Clone + Display + Send + Sync + 'static + Debug,
{
    fn init(
        service_resources_handle: OpaqueServiceResourcesHandle<Self, RuntimeServiceId>,
        initial_state: Self::State,
    ) -> Result<Self, overwatch::DynError> {
        let events = broadcast::channel(EVENT_BUFFER_SIZE).0;
        Ok(Self {
            service_resources_handle,
            state: initial_state,
            events,
        })
    }

    async fn run(mut self) -> Result<(), overwatch::DynError> {
        self.service_resources_handle.status_updater.notify_ready();
        let mut expiry = tokio::time::interval(self.state.store.expiry_check_interval());
        expiry.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                _ = expiry.tick() => {
                    self.publish_expiry_events();
                }
                message = self.service_resources_handle.inbound_relay.recv() => {
                    let Some(message) = message else { break; };
                    self.handle(message);
                }
            }
        }

        Ok(())
    }
}

impl<RuntimeServiceId> BanningService<RuntimeServiceId> {
    fn publish(sender: &broadcast::Sender<BanEvent>, events: impl IntoIterator<Item = BanEvent>) {
        for event in events {
            let _unused = sender.send(event);
        }
    }

    fn publish_expiry_events(&mut self) {
        Self::publish(&self.events, self.state.expire());
    }

    fn handle(&mut self, message: BanningRequest) {
        match message {
            BanningRequest::BanPeer { violation, reply } => {
                let _unused = reply.send(self.state.report(violation, &self.events));
            }
            BanningRequest::ReplaceBan { violation, reply } => {
                let _unused = reply.send(self.state.replace(violation, &self.events));
            }
            BanningRequest::QueryApplicable {
                peer_id,
                scope,
                reply,
            } => {
                let _unused = reply.send(self.state.query(peer_id, &scope, &self.events));
            }
            BanningRequest::QueryApplicableMany {
                peer_ids,
                scope,
                reply,
            } => {
                let _unused = reply.send(self.state.query_many(&peer_ids, &scope, &self.events));
            }
            BanningRequest::ListActive { reply } => {
                let _unused = reply.send(self.state.active(&self.events));
            }
            BanningRequest::UnbanPeer {
                peer_id,
                scope,
                reply,
            } => {
                let _unused = reply.send(self.state.unban(peer_id, scope.as_ref(), &self.events));
            }
            BanningRequest::Subscribe { reply } => {
                let _unused = reply.send(self.events.subscribe());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{Arc, Mutex},
        time::{Duration, SystemTime},
    };

    use super::*;
    use crate::{BanSource, OffenseDurations, OffenseKind, Subsystem};

    #[derive(Clone)]
    struct ManualClock(Arc<Mutex<SystemTime>>);

    impl ManualClock {
        fn new() -> Self {
            Self(Arc::new(Mutex::new(SystemTime::UNIX_EPOCH)))
        }
    }

    impl Clock for ManualClock {
        fn now(&self) -> SystemTime {
            *self.0.lock().expect("clock lock")
        }
    }

    #[test]
    fn events_retain_scope_for_report_and_expiry() {
        let config = BanningConfig {
            offenses: OffenseDurations {
                other: Duration::from_secs(5),
                ..Default::default()
            },
            ..Default::default()
        };
        let clock = ManualClock::new();
        let mut state = BanningState::with_store(BanStore::with_clock(&config, clock.clone()));
        let event_sender = broadcast::channel(EVENT_BUFFER_SIZE).0;
        let mut events = event_sender.subscribe();
        let peer_id = PeerId::random();
        let scope = BanScope::Service(Subsystem::ChainSync);
        let source = BanSource::Service(Subsystem::Other("validator".to_owned()));

        state.report(
            Violation::with_source(
                peer_id,
                source.clone(),
                scope.clone(),
                OffenseKind::Other,
                Some("bad chain response".to_owned()),
            ),
            &event_sender,
        );
        let BanEvent::Banned(record) = events.try_recv().expect("ban event") else {
            panic!("expected ban event");
        };
        assert_eq!(record.source, source);
        assert_eq!(record.scope, scope);
        assert_eq!(record.context.as_deref(), Some("bad chain response"));

        *clock.0.lock().expect("clock lock") = record.expires_at.expect("expiry");
        BanningState::<ManualClock>::publish(&event_sender, state.expire());
        let BanEvent::Unbanned {
            record: expired_record,
            expired,
        } = events.try_recv().expect("expiry event")
        else {
            panic!("expected expiry event");
        };
        assert!(expired);
        assert_eq!(expired_record.peer_id, peer_id);
        assert_eq!(expired_record.source, source);
        assert_eq!(expired_record.scope, scope);
        assert_eq!(
            expired_record.context.as_deref(),
            Some("bad chain response")
        );
    }

    #[test]
    fn replacement_and_explicit_unban_events_retain_the_complete_record() {
        let config = BanningConfig {
            offenses: OffenseDurations {
                spam_msg: Duration::from_secs(5),
                invalid_sig: Duration::from_secs(10),
                ..Default::default()
            },
            ..Default::default()
        };
        let clock = ManualClock::new();
        let mut state = BanningState::with_store(BanStore::with_clock(&config, clock));
        let event_sender: broadcast::Sender<BanEvent> = broadcast::channel(EVENT_BUFFER_SIZE).0;
        let mut events = event_sender.subscribe();
        let peer_id = PeerId::random();
        let scope = BanScope::Service(Subsystem::ChainSync);
        let source = BanSource::Service(Subsystem::Other("operator".to_owned()));

        state.report(
            Violation::with_source(
                peer_id,
                source.clone(),
                scope.clone(),
                OffenseKind::InvalidSig,
                Some("initial".to_owned()),
            ),
            &event_sender,
        );
        let _unused = events.try_recv().expect("initial event");

        state.replace(
            Violation::with_source(
                peer_id,
                source.clone(),
                scope.clone(),
                OffenseKind::SpamMsg,
                Some("replacement".to_owned()),
            ),
            &event_sender,
        );
        let BanEvent::Replaced(replaced) = events.try_recv().expect("replacement event") else {
            panic!("expected replacement event");
        };
        assert_eq!(replaced.source, source);
        assert_eq!(replaced.scope, scope);
        assert_eq!(replaced.offense, OffenseKind::SpamMsg);
        assert_eq!(replaced.context.as_deref(), Some("replacement"));

        assert!(state.unban(peer_id, Some(&scope), &event_sender));
        let BanEvent::Unbanned {
            record: unbanned,
            expired,
        } = events.try_recv().expect("unban event")
        else {
            panic!("expected unban event");
        };
        assert!(!expired);
        assert_eq!(unbanned.source, source);
        assert_eq!(unbanned.scope, scope);
        assert_eq!(unbanned.offense, OffenseKind::SpamMsg);
        assert_eq!(unbanned.context.as_deref(), Some("replacement"));
    }
}
