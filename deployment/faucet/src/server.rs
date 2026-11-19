use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use axum::{
    Router,
    extract::{Path, State},
    http::HeaderMap,
    response::IntoResponse,
    routing::post,
};
use lb_groth16::fr_from_bytes;
use lb_key_management_system_keys::keys::ZkPublicKey;
use reqwest::StatusCode;
use tokio::sync::mpsc;

use crate::faucet::{DripRequest, FaucetReadiness, FaucetStatus};

pub struct FaucetServerState {
    queue: mpsc::Sender<DripRequest>,
    cooldowns: Mutex<HashMap<ZkPublicKey, Instant>>,
    cooldown: Duration,
    status: Arc<FaucetStatus>,
}

impl FaucetServerState {
    #[must_use]
    pub fn new(
        queue: mpsc::Sender<DripRequest>,
        cooldown: Duration,
        status: Arc<FaucetStatus>,
    ) -> Self {
        Self {
            queue,
            cooldowns: Mutex::new(HashMap::new()),
            cooldown,
            status,
        }
    }

    /// Registers a drip attempt for `key`. Returns the remaining cooldown if
    /// the key is still cooling down from a previous request.
    fn try_start_cooldown(&self, key: ZkPublicKey) -> Result<(), Duration> {
        let mut cooldowns = self
            .cooldowns
            .lock()
            .expect("cooldown mutex should not be poisoned");
        let now = Instant::now();
        cooldowns.retain(|_, expires_at| *expires_at > now);

        if let Some(expires_at) = cooldowns.get(&key) {
            return Err(expires_at.duration_since(now));
        }

        cooldowns.insert(key, now + self.cooldown);
        drop(cooldowns);
        Ok(())
    }

    fn clear_cooldown(&self, key: ZkPublicKey) {
        self.cooldowns
            .lock()
            .expect("cooldown mutex should not be poisoned")
            .remove(&key);
    }
}

async fn transfer_funds(
    State(state): State<Arc<FaucetServerState>>,
    Path(key_id): Path<String>,
    headers: HeaderMap,
) -> impl IntoResponse {
    let Some(id) = headers
        .get("idempotency-key")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| hex::decode(value).ok())
        .and_then(|bytes| <[u8; 32]>::try_from(bytes).ok())
    else {
        return (
            StatusCode::BAD_REQUEST,
            "Idempotency-Key must be a 32-byte hex value",
        )
            .into_response();
    };
    let bytes = match hex::decode(&key_id) {
        Ok(b) => b,
        Err(e) => return (StatusCode::BAD_REQUEST, format!("Invalid hex: {e}")).into_response(),
    };

    let recipient_pk = match fr_from_bytes(&bytes) {
        Ok(fr) => ZkPublicKey::new(fr),
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                format!("Invalid key format: {e:?}"),
            )
                .into_response();
        }
    };

    let status = state.status.get();
    if matches!(
        status,
        FaucetReadiness::NodeUnavailable | FaucetReadiness::InterventionRequired
    ) {
        let reason = match status {
            FaucetReadiness::NodeUnavailable => "node_unavailable",
            _ => "intervention_required",
        };
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            serde_json::json!({"status": reason}).to_string(),
        )
            .into_response();
    }

    if let Err(remaining) = state.try_start_cooldown(recipient_pk) {
        let retry_after_secs = remaining.as_secs().max(1);
        return (
            StatusCode::TOO_MANY_REQUESTS,
            serde_json::json!({
                "status": "cooldown",
                "retry_after_secs": retry_after_secs,
            })
            .to_string(),
        )
            .into_response();
    }

    if state
        .queue
        .try_send(DripRequest {
            recipient: recipient_pk,
            id,
        })
        .is_ok()
    {
        (
            StatusCode::ACCEPTED,
            serde_json::json!({
                "status": "queued",
            })
            .to_string(),
        )
            .into_response()
    } else {
        state.clear_cooldown(recipient_pk);
        (
            StatusCode::SERVICE_UNAVAILABLE,
            serde_json::json!({
                "status": "queue_full",
            })
            .to_string(),
        )
            .into_response()
    }
}

pub fn faucet_app(state: Arc<FaucetServerState>) -> Router {
    Router::new()
        .route("/faucet/{pk}", post(transfer_funds))
        .with_state(state)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn requests_require_and_queue_a_stable_id() {
        let (sender, mut receiver) = mpsc::channel(1);
        let state = Arc::new(FaucetServerState::new(
            sender,
            Duration::from_secs(1),
            Arc::new(FaucetStatus::new(FaucetReadiness::Ready)),
        ));
        let recipient = "00".repeat(32);
        let response = transfer_funds(
            State(Arc::clone(&state)),
            Path(recipient.clone()),
            HeaderMap::new(),
        )
        .await
        .into_response();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert!(receiver.try_recv().is_err());
        let mut headers = HeaderMap::new();
        headers.insert("idempotency-key", "ab".repeat(32).parse().unwrap());
        let response = transfer_funds(State(state), Path(recipient), headers)
            .await
            .into_response();
        assert_eq!(response.status(), StatusCode::ACCEPTED);
        assert_eq!(receiver.recv().await.unwrap().id, [0xab; 32]);
    }

    #[tokio::test]
    async fn unavailable_or_blocked_worker_rejects_new_payments_without_cooldown() {
        let (sender, mut receiver) = mpsc::channel(1);
        let status = Arc::new(FaucetStatus::new(FaucetReadiness::NodeUnavailable));
        let state = Arc::new(FaucetServerState::new(
            sender,
            Duration::from_secs(60),
            Arc::clone(&status),
        ));
        let mut headers = HeaderMap::new();
        headers.insert("idempotency-key", "ab".repeat(32).parse().unwrap());
        for readiness in [
            FaucetReadiness::NodeUnavailable,
            FaucetReadiness::InterventionRequired,
        ] {
            status.set(readiness);
            let response = transfer_funds(
                State(Arc::clone(&state)),
                Path("00".repeat(32)),
                headers.clone(),
            )
            .await
            .into_response();
            assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
            assert!(receiver.try_recv().is_err());
        }
        status.set(FaucetReadiness::Ready);
        let response = transfer_funds(State(state), Path("00".repeat(32)), headers)
            .await
            .into_response();
        assert_eq!(response.status(), StatusCode::ACCEPTED);
        assert_eq!(receiver.recv().await.unwrap().id, [0xab; 32]);
    }
}
