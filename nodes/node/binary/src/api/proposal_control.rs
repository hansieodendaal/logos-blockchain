//! Loopback-only test control, independent of chain-leader readiness.

use std::net::SocketAddr;

use axum::{Extension, Json, extract::ConnectInfo, http::StatusCode};
use lb_chain_leader_service::proposal_control::ProposalPublicationControl;
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize)]
pub struct PublicationState {
    enabled: bool,
}

pub async fn get(
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    Extension(control): Extension<ProposalPublicationControl>,
) -> Result<Json<PublicationState>, StatusCode> {
    if !peer.ip().is_loopback() {
        return Err(StatusCode::FORBIDDEN);
    }
    Ok(Json(PublicationState {
        enabled: control.is_enabled().await,
    }))
}

pub async fn set(
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    Extension(control): Extension<ProposalPublicationControl>,
    Json(state): Json<PublicationState>,
) -> Result<Json<PublicationState>, StatusCode> {
    if !peer.ip().is_loopback() {
        return Err(StatusCode::FORBIDDEN);
    }
    Ok(Json(PublicationState {
        enabled: control.set_enabled(state.enabled).await,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn api_shares_gate_and_rejects_non_loopback_clients() {
        let control = ProposalPublicationControl::new(false);
        let local = "127.0.0.1:3000".parse().unwrap();
        let remote = "192.0.2.1:3000".parse().unwrap();
        assert!(
            !get(ConnectInfo(local), Extension(control.clone()))
                .await
                .unwrap()
                .0
                .enabled
        );
        assert!(
            set(
                ConnectInfo(local),
                Extension(control.clone()),
                Json(PublicationState { enabled: true })
            )
            .await
            .unwrap()
            .0
            .enabled
        );
        assert!(control.is_enabled().await);
        assert!(
            !set(
                ConnectInfo(local),
                Extension(control.clone()),
                Json(PublicationState { enabled: false })
            )
            .await
            .unwrap()
            .0
            .enabled
        );
        assert!(
            !control
                .publish_if_enabled(async { panic!("disabled publication ran") })
                .await
        );
        assert!(matches!(
            get(ConnectInfo(remote), Extension(control)).await,
            Err(StatusCode::FORBIDDEN)
        ));
    }
}
