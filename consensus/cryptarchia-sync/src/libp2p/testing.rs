use std::sync::atomic::{AtomicU64, Ordering};

use lb_core::header::HeaderId;
use libp2p::{PeerId, StreamProtocol};

static NEXT_REQUEST_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Clone, Debug)]
pub struct RequestEvidence {
    pub request_id: u64,
    pub request_key: String,
    pub peer_id: PeerId,
    pub protocol: String,
    pub target_block: HeaderId,
    pub requester_tip: HeaderId,
    pub latest_immutable_block: HeaderId,
    pub additional_blocks: Vec<String>,
}

impl RequestEvidence {
    pub fn observation(&self) -> crate::observation::RequestObservation {
        crate::observation::RequestObservation {
            local_request_id: self.request_id,
            remote_peer: self.peer_id.into(),
            protocol: self.protocol.clone(),
            target: self.target_block,
            requester_tip: self.requester_tip,
            latest_immutable: self.latest_immutable_block,
            additional_blocks: self.additional_blocks.clone(),
        }
    }

    pub fn new(
        peer_id: PeerId,
        protocol: &StreamProtocol,
        target_block: HeaderId,
        requester_tip: HeaderId,
        latest_immutable_block: HeaderId,
        mut additional_blocks: Vec<String>,
    ) -> Self {
        additional_blocks.sort();
        let request_key = format!(
            "{peer_id}:{protocol}:{target_block:?}:{requester_tip:?}:{latest_immutable_block:?}:{additional_blocks:?}"
        );
        Self {
            request_id: NEXT_REQUEST_ID.fetch_add(1, Ordering::Relaxed),
            request_key,
            peer_id,
            protocol: protocol.to_string(),
            target_block,
            requester_tip,
            latest_immutable_block,
            additional_blocks,
        }
    }
}

pub fn timestamp_unix_ms() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}

pub fn timestamp_unix_ns() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
}
