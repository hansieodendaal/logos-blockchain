use std::{collections::HashSet, time::Duration};

use lb_libp2p::PeerId;
use overwatch::services::relay::OutboundRelay;
use thiserror::Error;
use tokio::{sync::broadcast, time::timeout};

use crate::{
    BanEvent, BanRecord, BanScope, BanSource, BanningRequest, OffenseKind, Violation,
    service::BanningService,
};

const API_TIMEOUT: Duration = Duration::from_secs(2);

#[derive(Debug, Error)]
pub enum BanningApiError {
    #[error("banning service relay is unavailable: {0}")]
    Relay(String),
    #[error("banning service request timed out")]
    Timeout,
    #[error("banning service response channel was closed")]
    ResponseClosed,
    #[error("a global ban requires explicit provenance")]
    GlobalBanRequiresSource,
}

/// Async client API for the central banning service.
#[derive(Clone)]
pub struct BanningServiceApi<RuntimeServiceId> {
    relay: OutboundRelay<
        <BanningService<RuntimeServiceId> as overwatch::services::ServiceData>::Message,
    >,
}

impl<RuntimeServiceId> std::fmt::Debug for BanningServiceApi<RuntimeServiceId> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("BanningServiceApi")
            .finish_non_exhaustive()
    }
}

impl<RuntimeServiceId> BanningServiceApi<RuntimeServiceId> {
    #[must_use]
    pub const fn new(
        relay: OutboundRelay<
            <BanningService<RuntimeServiceId> as overwatch::services::ServiceData>::Message,
        >,
    ) -> Self {
        Self { relay }
    }

    /// Erases the runtime service-id marker. The relay message is independent
    /// of that marker, which lets a consumer pass the API through a generic
    /// network command without adding a banning-service service-id bound.
    #[must_use]
    pub fn into_untyped(self) -> BanningServiceApi<()> {
        BanningServiceApi { relay: self.relay }
    }

    async fn request<T>(
        &self,
        request: impl FnOnce(tokio::sync::oneshot::Sender<T>) -> BanningRequest,
    ) -> Result<T, BanningApiError> {
        timeout(API_TIMEOUT, async {
            let (sender, receiver) = tokio::sync::oneshot::channel();
            self.relay
                .send(request(sender))
                .await
                .map_err(|(error, _)| BanningApiError::Relay(error.to_string()))?;
            receiver.await.map_err(|_| BanningApiError::ResponseClosed)
        })
        .await
        .map_err(|_| BanningApiError::Timeout)?
    }

    pub async fn report(&self, violation: Violation) -> Result<Option<BanRecord>, BanningApiError> {
        self.request(|reply| BanningRequest::BanPeer { violation, reply })
            .await
    }

    pub async fn ban_peer(
        &self,
        peer_id: PeerId,
        scope: BanScope,
        offense: OffenseKind,
        context: Option<String>,
    ) -> Result<Option<BanRecord>, BanningApiError> {
        let BanScope::Service(subsystem) = scope else {
            return Err(BanningApiError::GlobalBanRequiresSource);
        };
        self.report(Violation::new(
            peer_id,
            BanSource::Service(subsystem.clone()),
            BanScope::Service(subsystem),
            offense,
            context,
        ))
        .await
    }

    pub async fn ban_peer_with_source(
        &self,
        peer_id: PeerId,
        source: BanSource,
        scope: BanScope,
        offense: OffenseKind,
        context: Option<String>,
    ) -> Result<Option<BanRecord>, BanningApiError> {
        self.report(Violation::with_source(
            peer_id, source, scope, offense, context,
        ))
        .await
    }

    pub async fn replace_ban(
        &self,
        violation: Violation,
    ) -> Result<Option<BanRecord>, BanningApiError> {
        self.request(|reply| BanningRequest::ReplaceBan { violation, reply })
            .await
    }

    pub async fn query_applicable(
        &self,
        peer_id: PeerId,
        scope: BanScope,
    ) -> Result<Vec<BanRecord>, BanningApiError> {
        self.request(|reply| BanningRequest::QueryApplicable {
            peer_id,
            scope,
            reply,
        })
        .await
    }

    pub async fn query_applicable_many(
        &self,
        peer_ids: &HashSet<PeerId>,
        scope: BanScope,
    ) -> Result<Vec<BanRecord>, BanningApiError> {
        self.request(|reply| BanningRequest::QueryApplicableMany {
            peer_ids: peer_ids.clone(),
            scope,
            reply,
        })
        .await
    }

    pub async fn is_banned(
        &self,
        peer_id: PeerId,
        scope: BanScope,
    ) -> Result<bool, BanningApiError> {
        Ok(!self.query_applicable(peer_id, scope).await?.is_empty())
    }

    pub async fn list_active(&self) -> Result<Vec<BanRecord>, BanningApiError> {
        self.request(|reply| BanningRequest::ListActive { reply })
            .await
    }

    pub async fn unban(
        &self,
        peer_id: PeerId,
        scope: Option<BanScope>,
    ) -> Result<bool, BanningApiError> {
        self.request(|reply| BanningRequest::UnbanPeer {
            peer_id,
            scope,
            reply,
        })
        .await
    }

    pub async fn subscribe(&self) -> Result<broadcast::Receiver<BanEvent>, BanningApiError> {
        self.request(|reply| BanningRequest::Subscribe { reply })
            .await
    }
}

#[cfg(test)]
mod tests {
    use tokio::sync::mpsc;

    use super::*;

    #[tokio::test]
    async fn convenience_api_requires_explicit_source_for_global_bans() {
        let (sender, _receiver) = mpsc::channel(1);
        let api = BanningServiceApi::<()>::new(OutboundRelay::new(sender));

        assert!(matches!(
            api.ban_peer(
                PeerId::random(),
                BanScope::Global,
                OffenseKind::ProtocolViolation,
                None,
            )
            .await,
            Err(BanningApiError::GlobalBanRequiresSource)
        ));
    }
}
