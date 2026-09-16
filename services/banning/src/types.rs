use std::{collections::HashSet, time::SystemTime};

use lb_libp2p::PeerId;
use serde::{Deserialize, Serialize};
use tokio::sync::{broadcast, oneshot};

/// The service or subsystem that reported a peer offense.
///
/// `Other` deliberately owns its label so that the reporting scope is not
/// lost when a violation crosses the service boundary.
#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub enum Subsystem {
    ChainSync,
    Libp2p,
    DataAvailability,
    Blend,
    Other(String),
}

/// Provenance of a ban independently from where it is enforced.
#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub enum BanSource {
    Service(Subsystem),
    Configuration,
}

/// Scope in which a ban is enforced.
///
/// A global ban is explicit. A service-scoped ban only applies when that
/// service asks the banning service for applicable bans.
#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub enum BanScope {
    Global,
    Service(Subsystem),
}

/// Kind of offense committed by a peer.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub enum OffenseKind {
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

/// A complete report submitted to the banning service.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Violation {
    pub peer_id: PeerId,
    pub source: BanSource,
    pub scope: BanScope,
    pub offense: OffenseKind,
    pub context: Option<String>,
}

impl Violation {
    /// Construct a report with explicit provenance and enforcement scope.
    #[must_use]
    pub const fn new(
        peer_id: PeerId,
        source: BanSource,
        scope: BanScope,
        offense: OffenseKind,
        context: Option<String>,
    ) -> Self {
        Self::with_source(peer_id, source, scope, offense, context)
    }

    #[must_use]
    pub const fn with_source(
        peer_id: PeerId,
        source: BanSource,
        scope: BanScope,
        offense: OffenseKind,
        context: Option<String>,
    ) -> Self {
        Self {
            peer_id,
            source,
            scope,
            offense,
            context,
        }
    }
}

/// The in-memory record retained for an active ban.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct BanRecord {
    pub peer_id: PeerId,
    pub source: BanSource,
    pub scope: BanScope,
    pub offense: OffenseKind,
    pub context: Option<String>,
    pub reported_at: SystemTime,
    /// `None` is reserved for configured blacklist entries.
    pub expires_at: Option<SystemTime>,
}

impl BanRecord {
    #[must_use]
    pub fn applies_to(&self, scope: &BanScope) -> bool {
        self.scope == BanScope::Global || &self.scope == scope
    }
}

/// Events emitted for changes to active ban state.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum BanEvent {
    Banned(BanRecord),
    Replaced(BanRecord),
    Unbanned { record: BanRecord, expired: bool },
}

/// Requests handled by [`crate::BanningService`].
#[derive(Debug)]
pub enum BanningRequest {
    BanPeer {
        violation: Violation,
        reply: oneshot::Sender<Option<BanRecord>>,
    },
    ReplaceBan {
        violation: Violation,
        reply: oneshot::Sender<Option<BanRecord>>,
    },
    QueryApplicable {
        peer_id: PeerId,
        scope: BanScope,
        reply: oneshot::Sender<Vec<BanRecord>>,
    },
    QueryApplicableMany {
        peer_ids: HashSet<PeerId>,
        scope: BanScope,
        reply: oneshot::Sender<Vec<BanRecord>>,
    },
    ListActive {
        reply: oneshot::Sender<Vec<BanRecord>>,
    },
    UnbanPeer {
        peer_id: PeerId,
        scope: Option<BanScope>,
        reply: oneshot::Sender<bool>,
    },
    Subscribe {
        reply: oneshot::Sender<broadcast::Receiver<BanEvent>>,
    },
}
