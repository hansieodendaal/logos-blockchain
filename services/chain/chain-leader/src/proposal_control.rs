//! Test-only coordination of proposal publication.

use std::sync::Arc;

use tokio::sync::RwLock;

/// Shared state for one node. Clones refer to the same publication gate.
///
/// A state change waits for any publication already admitted through the gate.
/// Once disabling returns, no admitted publication remains in progress and no
/// new one can start until publication is enabled again.
#[derive(Clone, Debug)]
pub struct ProposalPublicationControl {
    enabled: Arc<RwLock<bool>>,
}

impl Default for ProposalPublicationControl {
    fn default() -> Self {
        Self::new(true)
    }
}

impl ProposalPublicationControl {
    #[must_use]
    pub fn new(enabled: bool) -> Self {
        Self {
            enabled: Arc::new(RwLock::new(enabled)),
        }
    }

    pub async fn set_enabled(&self, enabled: bool) -> bool {
        let mut state = self.enabled.write().await;
        *state = enabled;
        *state
    }

    pub async fn is_enabled(&self) -> bool {
        *self.enabled.read().await
    }

    /// Holds the read lock across publication, so a disable acknowledgment
    /// cannot race an already admitted publish operation.
    pub async fn publish_if_enabled(&self, publish: impl Future<Output = ()>) -> bool {
        let enabled = self.enabled.read().await;
        if !*enabled {
            return false;
        }
        publish.await;
        drop(enabled);
        true
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    use super::ProposalPublicationControl;

    #[tokio::test]
    async fn publication_defaults_on_and_can_be_disabled_and_restored_per_node() {
        let first = ProposalPublicationControl::default();
        let second = ProposalPublicationControl::default();
        let calls = AtomicUsize::new(0);
        assert!(first.is_enabled().await);
        assert!(!first.set_enabled(false).await);
        assert!(!first.set_enabled(false).await);
        assert!(second.is_enabled().await);
        assert!(
            !first
                .publish_if_enabled(async {
                    calls.fetch_add(1, Ordering::SeqCst);
                })
                .await
        );
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert!(first.set_enabled(true).await);
        assert!(first.set_enabled(true).await);
        assert!(
            first
                .publish_if_enabled(async {
                    calls.fetch_add(1, Ordering::SeqCst);
                })
                .await
        );
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn disable_acknowledgment_waits_for_an_admitted_publication() {
        let control = ProposalPublicationControl::default();
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (finish_tx, finish_rx) = tokio::sync::oneshot::channel();
        let publisher = control.clone();
        let task = tokio::spawn(async move {
            publisher
                .publish_if_enabled(async move {
                    let _ = started_tx.send(());
                    let _ = finish_rx.await;
                })
                .await
        });
        started_rx.await.unwrap();
        let acknowledged = Arc::new(AtomicUsize::new(0));
        let acknowledged_by_task = Arc::clone(&acknowledged);
        let disabler = control.clone();
        let disable = tokio::spawn(async move {
            disabler.set_enabled(false).await;
            acknowledged_by_task.store(1, Ordering::SeqCst);
        });
        tokio::task::yield_now().await;
        assert_eq!(acknowledged.load(Ordering::SeqCst), 0);
        finish_tx.send(()).unwrap();
        assert!(task.await.unwrap());
        disable.await.unwrap();
        assert!(!control.is_enabled().await);
    }
}
