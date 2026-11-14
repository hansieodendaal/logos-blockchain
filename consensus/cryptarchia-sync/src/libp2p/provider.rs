#[cfg(feature = "testing-era-boundaries")]
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::time::Duration;

#[cfg(feature = "testing-era-boundaries")]
use futures::{StreamExt as _, future};
use futures::{TryStreamExt as _, stream::BoxStream};
use lb_log_targets::cryptarchia;
use libp2p::{PeerId, Stream as Libp2pStream};
use serde::Serialize;
use tokio::{sync::mpsc, time};
use tracing::{debug, error};

#[cfg(feature = "testing-era-boundaries")]
use crate::libp2p::testing::{RequestEvidence, timestamp_unix_ms, timestamp_unix_ns};
#[cfg(feature = "testing-era-boundaries")]
use crate::observation::{self, Observation};
use crate::{
    BlocksProviderResponse, BlocksUnavailableReason, DynError, ProviderResponse,
    TipProviderResponse,
    libp2p::{
        errors::{ChainSyncError, ChainSyncErrorKind},
        messages::{BlocksResponse, RequestMessage},
        packing::unpack_from_reader,
        utils::{close_stream, send_message, send_message_up_to},
    },
    messages::{SerialisedBlock, TipResponse, TipResponseFailureReason},
};

const LOG_TARGET: &str = cryptarchia::sync::libp2p::PROVIDER;

pub const MAX_ADDITIONAL_BLOCKS: usize = 5;

pub struct Provider;

/// A request read from a stream a peer opened, and the stream to answer it on.
pub struct ReceivedRequest {
    pub peer_id: PeerId,
    pub stream: Libp2pStream,
    pub request: RequestMessage,
}

#[cfg(feature = "testing-era-boundaries")]
#[derive(Clone)]
pub struct EraBoundaryResponseHold {
    pub(super) evidence: Option<RequestEvidence>,
    pub(super) provider_peer_id: PeerId,
    pub(super) after_blocks: Option<usize>,
    pub(super) selected_peer_id: Option<String>,
    pub(super) selected_protocol: Option<String>,
    pub(super) requester_tip_not_equal_to: Option<String>,
    pub(super) selected_provider_peer_id: Option<String>,
    pub(super) selected_target: Option<String>,
    pub(super) selected_requester_tip: Option<String>,
    pub(super) selected_latest_immutable: Option<String>,
    pub(super) selected_additional_blocks: Option<Vec<String>>,
    pub(super) claimed: Arc<AtomicBool>,
}

impl Provider {
    pub async fn receive_request(
        peer_id: PeerId,
        mut stream: Libp2pStream,
        timeout: Duration,
    ) -> Result<ReceivedRequest, ChainSyncError> {
        let request: RequestMessage = time::timeout(timeout, unpack_from_reader(&mut stream))
            .await
            .map_err(|e| ChainSyncError::from((peer_id, e)))?
            .map_err(|e| ChainSyncError::from((peer_id, e)))?;

        Ok(ReceivedRequest {
            peer_id,
            stream,
            request,
        })
    }

    pub async fn provide_tip(
        mut reply_receiver: mpsc::Receiver<TipProviderResponse>,
        peer_id: PeerId,
        mut libp2p_stream: Libp2pStream,
    ) -> Result<(), ChainSyncError> {
        let response = reply_receiver.recv().await.unwrap_or_else(|| {
            debug!(target: LOG_TARGET, %peer_id, "no tip provider answered; replying unknown error");
            ProviderResponse::Unavailable {
                reason: TipResponseFailureReason::Unknown,
            }
        });

        let send_result = match response {
            ProviderResponse::Available(tip) => {
                send_message(peer_id, &mut libp2p_stream, &tip).await
            }
            ProviderResponse::Unavailable { reason } => {
                let response = TipResponse::Failure(reason);
                send_message(peer_id, &mut libp2p_stream, &response).await
            }
        };

        drop(close_stream(peer_id, libp2p_stream).await);

        send_result
    }

    /// Serves one block download request: the blocks the service answers with,
    /// each in a response of at most `max_response_size` bytes. The whole
    /// response, from waiting for the block provider until the stream is
    /// closed, is bounded by `stream_timeout`, and each write by
    /// `send_timeout`.
    pub async fn provide_blocks(
        reply_receiver: mpsc::Receiver<BlocksProviderResponse>,
        peer_id: PeerId,
        libp2p_stream: Libp2pStream,
        max_response_size: usize,
        send_timeout: Duration,
        stream_timeout: Duration,
        #[cfg(feature = "testing-era-boundaries")] response_hold: EraBoundaryResponseHold,
    ) -> Result<(), ChainSyncError> {
        time::timeout(
            stream_timeout,
            Self::respond_to_blocks_request(
                reply_receiver,
                peer_id,
                libp2p_stream,
                max_response_size,
                send_timeout,
                #[cfg(feature = "testing-era-boundaries")]
                response_hold,
            ),
        )
        .await
        .map_err(|e| ChainSyncError::from((peer_id, e)))?
    }

    async fn respond_to_blocks_request(
        mut reply_receiver: mpsc::Receiver<BlocksProviderResponse>,
        peer_id: PeerId,
        mut libp2p_stream: Libp2pStream,
        max_response_size: usize,
        send_timeout: Duration,
        #[cfg(feature = "testing-era-boundaries")] response_hold: EraBoundaryResponseHold,
    ) -> Result<(), ChainSyncError> {
        let response = reply_receiver.recv().await.unwrap_or_else(|| {
            debug!(target: LOG_TARGET, %peer_id, "no block provider answered; replying unknown error");
            ProviderResponse::Unavailable {
                reason: BlocksUnavailableReason::Unknown,
            }
        });

        match response {
            ProviderResponse::Available(stream) => {
                Self::send_blocks(
                    peer_id,
                    stream,
                    libp2p_stream,
                    max_response_size,
                    send_timeout,
                    #[cfg(feature = "testing-era-boundaries")]
                    response_hold,
                )
                .await
            }
            ProviderResponse::Unavailable { reason } => {
                let response = BlocksResponse::Failure(reason);
                send_message_up_to(peer_id, &mut libp2p_stream, &response, max_response_size)
                    .await?;
                drop(close_stream(peer_id, libp2p_stream).await);
                Ok(())
            }
        }
    }

    /// Sends every block of `stream` to the peer, each in a response of at
    /// most `max_response_size` bytes. Each write is bounded by
    /// `send_timeout`, so a peer that stops reading cannot hold the download
    /// slot indefinitely.
    async fn send_blocks(
        peer_id: PeerId,
        stream: BoxStream<'static, Result<SerialisedBlock, DynError>>,
        mut libp2p_stream: Libp2pStream,
        max_response_size: usize,
        send_timeout: Duration,
        #[cfg(feature = "testing-era-boundaries")] response_hold: EraBoundaryResponseHold,
    ) -> Result<(), ChainSyncError> {
        #[cfg(feature = "testing-era-boundaries")]
        let (result, sent_blocks) = if provider_hold_is_armed(&response_hold) {
            send_blocks_with_hold(
                peer_id,
                stream,
                &mut libp2p_stream,
                max_response_size,
                send_timeout,
                &response_hold,
            )
            .await
        } else {
            send_blocks_normally(
                peer_id,
                stream,
                &mut libp2p_stream,
                max_response_size,
                send_timeout,
                Some(&response_hold),
            )
            .await
        };
        #[cfg(not(feature = "testing-era-boundaries"))]
        let (result, sent_blocks) = send_blocks_normally(
            peer_id,
            stream,
            &mut libp2p_stream,
            max_response_size,
            send_timeout,
            (),
        )
        .await;
        #[cfg(not(feature = "testing-era-boundaries"))]
        let _ = sent_blocks;

        let final_result = finish_blocks_response(
            peer_id,
            &mut libp2p_stream,
            result,
            max_response_size,
            send_timeout,
            #[cfg(feature = "testing-era-boundaries")]
            &response_hold,
            #[cfg(feature = "testing-era-boundaries")]
            sent_blocks,
        )
        .await;

        drop(close_stream(peer_id, libp2p_stream).await);

        final_result
    }
}

/// Keeps the ordinary streaming implementation in one path for feature
/// disabled builds and feature enabled requests without an armed hold.
async fn send_blocks_normally(
    peer_id: PeerId,
    stream: BoxStream<'static, Result<SerialisedBlock, DynError>>,
    libp2p_stream: &mut Libp2pStream,
    max_response_size: usize,
    send_timeout: Duration,
    #[cfg(feature = "testing-era-boundaries")] response_hold: Option<&EraBoundaryResponseHold>,
    #[cfg(not(feature = "testing-era-boundaries"))] _response_hold: (),
) -> (Result<(), ChainSyncError>, usize) {
    #[cfg(feature = "testing-era-boundaries")]
    {
        let result = stream
            .map_err(|error| ChainSyncError {
                peer: peer_id,
                kind: ChainSyncErrorKind::BlockProviderError(format!(
                    "Failed to receive block from stream: {error}"
                )),
            })
            .try_fold(
                (libp2p_stream, 0usize),
                async |(stream, sent_blocks), block| {
                    let block_size = block.len();
                    let message = BlocksResponse::Block(block);
                    send_message_with_timeout(
                        peer_id,
                        stream,
                        &message,
                        max_response_size,
                        send_timeout,
                    )
                    .await?;
                    let sent_blocks = sent_blocks + 1;
                    if let Some(evidence) = response_hold.and_then(|hold| hold.evidence.as_ref()) {
                        if observation::node_journal_is_active() {
                            observation::emit(Observation::ProviderBlockResponseSent {
                                request: evidence.observation(),
                                response_index: u64::try_from(sent_blocks)
                                    .expect("response count fits u64"),
                            });
                        }
                        tracing::info!(
                            target: LOG_TARGET,
                            era_transition_event = "provider_block_response_sent",
                            timestamp_unix_ms = timestamp_unix_ms(),
                            timestamp_unix_ns = timestamp_unix_ns(),
                            request_id = evidence.request_id,
                            request_key = %evidence.request_key,
                            peer_id = %evidence.peer_id,
                            protocol = %evidence.protocol,
                            target_block = ?evidence.target_block,
                            requester_tip = ?evidence.requester_tip,
                            latest_immutable_block = ?evidence.latest_immutable_block,
                            response_index = sent_blocks,
                            block_bytes = block_size,
                            "era-boundary experiment observed a successfully sent block response"
                        );
                    }
                    Ok((stream, sent_blocks))
                },
            )
            .await;
        match result {
            Ok((_, sent_blocks)) => (Ok(()), sent_blocks),
            Err(error) => (Err(error), 0),
        }
    }

    #[cfg(not(feature = "testing-era-boundaries"))]
    {
        let result = stream
            .map_err(|error| ChainSyncError {
                peer: peer_id,
                kind: ChainSyncErrorKind::BlockProviderError(format!(
                    "Failed to receive block from stream: {error}"
                )),
            })
            .try_fold(libp2p_stream, async |stream, block| {
                let message = BlocksResponse::Block(block);
                send_message_with_timeout(
                    peer_id,
                    stream,
                    &message,
                    max_response_size,
                    send_timeout,
                )
                .await?;
                Ok(stream)
            })
            .await
            .map(|_| ());
        (result, 0)
    }
}

#[cfg(feature = "testing-era-boundaries")]
fn provider_hold_is_armed(response_hold: &EraBoundaryResponseHold) -> bool {
    response_hold.after_blocks.is_some()
        && response_hold.evidence.as_ref().is_some_and(|evidence| {
            response_hold
                .selected_provider_peer_id
                .as_ref()
                .is_none_or(|peer_id| peer_id == &response_hold.provider_peer_id.to_string())
                && response_hold
                    .selected_peer_id
                    .as_ref()
                    .is_none_or(|peer_id| peer_id == &evidence.peer_id.to_string())
                && response_hold
                    .selected_protocol
                    .as_ref()
                    .is_none_or(|protocol| protocol == &evidence.protocol)
                && response_hold
                    .requester_tip_not_equal_to
                    .as_ref()
                    .is_none_or(|tip| tip != &format!("{:?}", evidence.requester_tip))
                && response_hold
                    .selected_target
                    .as_ref()
                    .is_none_or(|target| target == &format!("{:?}", evidence.target_block))
                && response_hold
                    .selected_requester_tip
                    .as_ref()
                    .is_none_or(|tip| tip == &format!("{:?}", evidence.requester_tip))
                && response_hold
                    .selected_latest_immutable
                    .as_ref()
                    .is_none_or(|block| block == &format!("{:?}", evidence.latest_immutable_block))
                && response_hold
                    .selected_additional_blocks
                    .as_ref()
                    .is_none_or(|blocks| {
                        let mut selected = blocks.clone();
                        selected.sort();
                        selected == evidence.additional_blocks
                    })
        })
        && !response_hold.claimed.load(Ordering::Acquire)
}

#[cfg(feature = "testing-era-boundaries")]
#[expect(
    clippy::cognitive_complexity,
    clippy::too_many_lines,
    reason = "the experimental hold records the lifecycle at each protocol boundary"
)]
async fn send_blocks_with_hold(
    peer_id: PeerId,
    stream: BoxStream<'static, Result<SerialisedBlock, DynError>>,
    libp2p_stream: &mut Libp2pStream,
    max_response_size: usize,
    send_timeout: Duration,
    response_hold: &EraBoundaryResponseHold,
) -> (Result<(), ChainSyncError>, usize) {
    let mut stream = Box::pin(stream.peekable());
    let mut sent_blocks = 0usize;
    let result = loop {
        if response_hold.after_blocks == Some(sent_blocks)
            && !response_hold.claimed.load(Ordering::Acquire)
        {
            let Some(next_block) = stream.as_mut().peek().await else {
                break Ok(());
            };
            if claim_provider_response_hold(
                response_hold.after_blocks,
                sent_blocks,
                next_block.is_ok(),
                &response_hold.claimed,
            ) {
                let evidence = response_hold
                    .evidence
                    .as_ref()
                    .expect("an armed hold has request evidence")
                    .clone();
                if observation::node_journal_is_active() {
                    observation::emit(Observation::ProviderResponseHoldActivated {
                        request: evidence.observation(),
                        blocks_sent: u64::try_from(sent_blocks).expect("response count fits u64"),
                    });
                }
                tracing::info!(
                    target: LOG_TARGET,
                    era_transition_event = "provider_response_hold_activated",
                    timestamp_unix_ms = timestamp_unix_ms(),
                    timestamp_unix_ns = timestamp_unix_ns(),
                    request_id = evidence.request_id,
                    request_key = %evidence.request_key,
                    peer_id = %evidence.peer_id,
                    protocol = %evidence.protocol,
                    target_block = ?evidence.target_block,
                    requester_tip = ?evidence.requester_tip,
                    latest_immutable_block = ?evidence.latest_immutable_block,
                    blocks_sent = sent_blocks,
                    additional_response_pending = true,
                    "era-boundary experiment entered the configured provider response hold"
                );
                let _guard = ResponseHoldGuard {
                    evidence,
                    blocks_sent: sent_blocks,
                    #[cfg(test)]
                    cancellation_observed: None,
                };
                future::pending::<()>().await;
            }
        }

        let Some(block_result) = stream.as_mut().next().await else {
            break Ok(());
        };
        let block = match block_result {
            Ok(block) => block,
            Err(error) => {
                break Err(ChainSyncError {
                    peer: peer_id,
                    kind: ChainSyncErrorKind::BlockProviderError(format!(
                        "Failed to receive block from stream: {error}"
                    )),
                });
            }
        };
        let block_size = block.len();
        let message = BlocksResponse::Block(block);
        if let Err(error) = send_message_with_timeout(
            peer_id,
            libp2p_stream,
            &message,
            max_response_size,
            send_timeout,
        )
        .await
        {
            break Err(error);
        }
        sent_blocks += 1;
        if let Some(evidence) = &response_hold.evidence {
            if observation::node_journal_is_active() {
                observation::emit(Observation::ProviderBlockResponseSent {
                    request: evidence.observation(),
                    response_index: u64::try_from(sent_blocks).expect("response count fits u64"),
                });
            }
            tracing::info!(
                target: LOG_TARGET,
                era_transition_event = "provider_block_response_sent",
                timestamp_unix_ms = timestamp_unix_ms(),
                    timestamp_unix_ns = timestamp_unix_ns(),
                request_id = evidence.request_id,
                request_key = %evidence.request_key,
                peer_id = %evidence.peer_id,
                protocol = %evidence.protocol,
                target_block = ?evidence.target_block,
                requester_tip = ?evidence.requester_tip,
                latest_immutable_block = ?evidence.latest_immutable_block,
                response_index = sent_blocks,
                block_bytes = block_size,
                "era-boundary experiment observed a successfully sent block response"
            );
        }
    };
    (result, sent_blocks)
}

async fn finish_blocks_response(
    peer_id: PeerId,
    stream: &mut Libp2pStream,
    result: Result<(), ChainSyncError>,
    max_response_size: usize,
    send_timeout: Duration,
    #[cfg(feature = "testing-era-boundaries")] response_hold: &EraBoundaryResponseHold,
    #[cfg(feature = "testing-era-boundaries")] sent_blocks: usize,
) -> Result<(), ChainSyncError> {
    match result {
        Ok(()) => {
            let message = BlocksResponse::NoMoreBlocks;
            let terminal_result = send_message_with_timeout(
                peer_id,
                stream,
                &message,
                max_response_size,
                send_timeout,
            )
            .await;
            #[cfg(feature = "testing-era-boundaries")]
            after_terminal_send(terminal_result, || {
                if let Some(evidence) = &response_hold.evidence {
                    if observation::node_journal_is_active() {
                        observation::emit(Observation::ProviderRequestCompleted {
                            request: evidence.observation(),
                            blocks_sent: u64::try_from(sent_blocks)
                                .expect("response count fits u64"),
                        });
                    }
                    tracing::info!(
                        target: LOG_TARGET,
                        era_transition_event = "provider_request_completed",
                        timestamp_unix_ms = timestamp_unix_ms(),
                        timestamp_unix_ns = timestamp_unix_ns(),
                        request_id = evidence.request_id,
                        request_key = %evidence.request_key,
                        peer_id = %evidence.peer_id,
                        protocol = %evidence.protocol,
                        target_block = ?evidence.target_block,
                        requester_tip = ?evidence.requester_tip,
                        latest_immutable_block = ?evidence.latest_immutable_block,
                        blocks_sent = sent_blocks,
                        "era-boundary experiment observed normal block response completion"
                    );
                }
            })?;
            #[cfg(not(feature = "testing-era-boundaries"))]
            terminal_result?;
            Ok(())
        }
        Err(error) => {
            error!(target: LOG_TARGET, "Failed to send blocks to peer {}: {}", peer_id, error);
            let message = BlocksResponse::Failure(BlocksUnavailableReason::Unknown);
            drop(
                send_message_with_timeout(
                    peer_id,
                    stream,
                    &message,
                    max_response_size,
                    send_timeout,
                )
                .await,
            );
            Err(error)
        }
    }
}

#[cfg(feature = "testing-era-boundaries")]
fn after_terminal_send<E>(
    result: Result<(), E>,
    observe_completion: impl FnOnce(),
) -> Result<(), E> {
    result?;
    observe_completion();
    Ok(())
}

#[cfg(feature = "testing-era-boundaries")]
struct ResponseHoldGuard {
    evidence: RequestEvidence,
    blocks_sent: usize,
    #[cfg(test)]
    cancellation_observed: Option<Arc<AtomicBool>>,
}

#[cfg(feature = "testing-era-boundaries")]
impl Drop for ResponseHoldGuard {
    fn drop(&mut self) {
        #[cfg(test)]
        if let Some(cancellation_observed) = &self.cancellation_observed {
            cancellation_observed.store(true, Ordering::Release);
        }
        if observation::node_journal_is_active() {
            observation::emit(Observation::ProviderResponseHoldCancelled {
                request: self.evidence.observation(),
                blocks_sent: u64::try_from(self.blocks_sent).expect("response count fits u64"),
            });
        }
        tracing::info!(
            target: LOG_TARGET,
            era_transition_event = "provider_response_hold_cancelled",
            timestamp_unix_ms = timestamp_unix_ms(),
                    timestamp_unix_ns = timestamp_unix_ns(),
            request_id = self.evidence.request_id,
            request_key = %self.evidence.request_key,
            peer_id = %self.evidence.peer_id,
            protocol = %self.evidence.protocol,
            target_block = ?self.evidence.target_block,
            requester_tip = ?self.evidence.requester_tip,
            latest_immutable_block = ?self.evidence.latest_immutable_block,
            blocks_sent = self.blocks_sent,
            "era-boundary experiment observed the held provider future being dropped"
        );
    }
}

#[cfg(feature = "testing-era-boundaries")]
fn claim_provider_response_hold(
    hold_after_blocks: Option<usize>,
    blocks_sent: usize,
    next_block_is_valid: bool,
    hold_claimed: &AtomicBool,
) -> bool {
    hold_after_blocks == Some(blocks_sent)
        && next_block_is_valid
        && hold_claimed
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
}

/// Sends a message of at most `max_bytes` bytes, giving up after `timeout`.
async fn send_message_with_timeout<M: Serialize + Sync>(
    peer_id: PeerId,
    stream: &mut Libp2pStream,
    message: &M,
    max_bytes: usize,
    timeout: Duration,
) -> Result<(), ChainSyncError> {
    time::timeout(
        timeout,
        send_message_up_to(peer_id, stream, message, max_bytes),
    )
    .await
    .map_err(|e| ChainSyncError::from((peer_id, e)))?
}

#[cfg(all(test, feature = "testing-era-boundaries"))]
mod era_boundary_tests {
    use std::sync::{Arc, atomic::AtomicBool};

    use lb_core::header::HeaderId;
    use libp2p::{PeerId, StreamProtocol};

    use super::{
        EraBoundaryResponseHold, ResponseHoldGuard, after_terminal_send,
        claim_provider_response_hold, provider_hold_is_armed,
    };
    use crate::libp2p::testing::RequestEvidence;

    #[test]
    fn failed_terminal_send_does_not_record_completion() {
        let mut observed = false;
        let result = after_terminal_send(Err("send failed"), || observed = true);
        assert_eq!(result, Err("send failed"));
        assert!(!observed);
        after_terminal_send(Ok::<(), &str>(()), || observed = true).unwrap();
        assert!(observed);
    }

    #[test]
    fn provider_hold_is_absent_by_default_and_one_shot_after_successful_responses() {
        let hold_claimed = AtomicBool::new(false);

        assert!(!claim_provider_response_hold(None, 0, true, &hold_claimed));
        assert!(!claim_provider_response_hold(
            Some(1),
            0,
            true,
            &hold_claimed
        ));
        assert!(!claim_provider_response_hold(
            Some(1),
            1,
            false,
            &hold_claimed
        ));
        assert!(claim_provider_response_hold(
            Some(1),
            1,
            true,
            &hold_claimed
        ));
        assert!(!claim_provider_response_hold(
            Some(1),
            1,
            true,
            &hold_claimed
        ));
    }

    #[test]
    fn absent_or_consumed_control_keeps_the_normal_response_path_armed() {
        let claimed = Arc::new(AtomicBool::new(false));
        let absent = EraBoundaryResponseHold {
            evidence: None,
            provider_peer_id: PeerId::random(),
            after_blocks: None,
            selected_peer_id: None,
            selected_protocol: None,
            requester_tip_not_equal_to: None,
            selected_provider_peer_id: None,
            selected_target: None,
            selected_requester_tip: None,
            selected_latest_immutable: None,
            selected_additional_blocks: None,
            claimed: Arc::clone(&claimed),
        };
        assert!(!provider_hold_is_armed(&absent));

        let evidence = RequestEvidence::new(
            PeerId::random(),
            &StreamProtocol::new("/test/era-boundary"),
            HeaderId::from([1; 32]),
            HeaderId::from([2; 32]),
            HeaderId::from([3; 32]),
            Vec::new(),
        );
        let configured = EraBoundaryResponseHold {
            evidence: Some(evidence),
            provider_peer_id: PeerId::random(),
            after_blocks: Some(1),
            selected_peer_id: None,
            selected_protocol: Some("/test/era-boundary".into()),
            requester_tip_not_equal_to: None,
            selected_provider_peer_id: None,
            selected_target: None,
            selected_requester_tip: None,
            selected_latest_immutable: None,
            selected_additional_blocks: None,
            claimed: Arc::clone(&claimed),
        };
        assert!(provider_hold_is_armed(&configured));
        claimed.store(true, std::sync::atomic::Ordering::Release);
        assert!(!provider_hold_is_armed(&configured));
    }

    #[test]
    fn provider_hold_selector_matches_peer_and_protocol() {
        let claimed = Arc::new(AtomicBool::new(false));
        let requester = PeerId::random();
        let evidence = RequestEvidence::new(
            requester,
            &StreamProtocol::new("/test/era-boundary"),
            HeaderId::from([1; 32]),
            HeaderId::from([2; 32]),
            HeaderId::from([3; 32]),
            Vec::new(),
        );
        let selected = EraBoundaryResponseHold {
            evidence: Some(evidence),
            provider_peer_id: PeerId::random(),
            after_blocks: Some(1),
            selected_peer_id: Some(requester.to_string()),
            selected_protocol: Some("/test/era-boundary".into()),
            requester_tip_not_equal_to: None,
            selected_provider_peer_id: None,
            selected_target: None,
            selected_requester_tip: None,
            selected_latest_immutable: None,
            selected_additional_blocks: None,
            claimed: Arc::clone(&claimed),
        };
        assert!(provider_hold_is_armed(&selected));

        let wrong_peer = EraBoundaryResponseHold {
            selected_peer_id: Some(PeerId::random().to_string()),
            ..selected.clone()
        };
        assert!(!provider_hold_is_armed(&wrong_peer));

        let wrong_protocol = EraBoundaryResponseHold {
            selected_protocol: Some("/test/another-era".into()),
            ..selected
        };
        assert!(!provider_hold_is_armed(&wrong_protocol));
    }

    #[test]
    fn exact_hold_selector_matches_provider_peer_protocol_and_full_context() {
        let provider = PeerId::random();
        let requester = PeerId::random();
        let protocol = StreamProtocol::new("/test/era-boundary");
        let target = HeaderId::from([1; 32]);
        let requester_tip = HeaderId::from([2; 32]);
        let latest_immutable = HeaderId::from([3; 32]);
        let evidence = RequestEvidence::new(
            requester,
            &protocol,
            target,
            requester_tip,
            latest_immutable,
            vec!["block-b".into(), "block-a".into()],
        );
        let selected = EraBoundaryResponseHold {
            evidence: Some(evidence),
            provider_peer_id: provider,
            after_blocks: Some(1),
            selected_peer_id: Some(requester.to_string()),
            selected_protocol: Some(protocol.to_string()),
            requester_tip_not_equal_to: None,
            selected_provider_peer_id: Some(provider.to_string()),
            selected_target: Some(format!("{target:?}")),
            selected_requester_tip: Some(format!("{requester_tip:?}")),
            selected_latest_immutable: Some(format!("{latest_immutable:?}")),
            selected_additional_blocks: Some(vec!["block-a".into(), "block-b".into()]),
            claimed: Arc::new(AtomicBool::new(false)),
        };
        assert!(provider_hold_is_armed(&selected));

        let wrong_provider = EraBoundaryResponseHold {
            provider_peer_id: PeerId::random(),
            ..selected.clone()
        };
        assert!(!provider_hold_is_armed(&wrong_provider));

        let candidates = [
            RequestEvidence::new(
                requester,
                &protocol,
                HeaderId::from([9; 32]),
                requester_tip,
                latest_immutable,
                vec!["block-a".into(), "block-b".into()],
            ),
            RequestEvidence::new(
                requester,
                &protocol,
                target,
                HeaderId::from([9; 32]),
                latest_immutable,
                vec!["block-a".into(), "block-b".into()],
            ),
            RequestEvidence::new(
                requester,
                &protocol,
                target,
                requester_tip,
                HeaderId::from([9; 32]),
                vec!["block-a".into(), "block-b".into()],
            ),
            RequestEvidence::new(
                requester,
                &protocol,
                target,
                requester_tip,
                latest_immutable,
                vec!["block-a".into()],
            ),
        ];
        for evidence in candidates {
            let wrong_context = EraBoundaryResponseHold {
                evidence: Some(evidence),
                ..selected.clone()
            };
            assert!(!provider_hold_is_armed(&wrong_context));
        }
    }

    #[test]
    fn provider_hold_context_selector_skips_only_the_configured_requester_tip() {
        let claimed = Arc::new(AtomicBool::new(false));
        let evidence = RequestEvidence::new(
            PeerId::random(),
            &StreamProtocol::new("/test/era-boundary"),
            HeaderId::from([1; 32]),
            HeaderId::from([2; 32]),
            HeaderId::from([3; 32]),
            Vec::new(),
        );
        let genesis_tip = format!("{:?}", evidence.requester_tip);
        let selected = EraBoundaryResponseHold {
            evidence: Some(evidence),
            provider_peer_id: PeerId::random(),
            after_blocks: Some(1),
            selected_peer_id: None,
            selected_protocol: Some("/test/era-boundary".into()),
            requester_tip_not_equal_to: Some(genesis_tip),
            selected_provider_peer_id: None,
            selected_target: None,
            selected_requester_tip: None,
            selected_latest_immutable: None,
            selected_additional_blocks: None,
            claimed,
        };
        assert!(!provider_hold_is_armed(&selected));

        let advanced_tip = EraBoundaryResponseHold {
            requester_tip_not_equal_to: Some(format!("{:?}", HeaderId::from([4; 32]))),
            ..selected
        };
        assert!(provider_hold_is_armed(&advanced_tip));
    }

    #[tokio::test]
    async fn dropping_the_held_provider_future_observes_cancellation() {
        let evidence = RequestEvidence::new(
            PeerId::random(),
            &StreamProtocol::new("/test/era-boundary"),
            HeaderId::from([1; 32]),
            HeaderId::from([2; 32]),
            HeaderId::from([3; 32]),
            Vec::new(),
        );
        let cancellation_observed = Arc::new(AtomicBool::new(false));
        let observed_by_task = Arc::clone(&cancellation_observed);
        let guard = ResponseHoldGuard {
            evidence,
            blocks_sent: 1,
            cancellation_observed: Some(observed_by_task),
        };
        let task = tokio::spawn(async move {
            let _guard = guard;
            std::future::pending::<()>().await;
        });

        tokio::task::yield_now().await;
        task.abort();
        let _unused = task.await;

        assert!(cancellation_observed.load(std::sync::atomic::Ordering::Acquire));
    }
}
