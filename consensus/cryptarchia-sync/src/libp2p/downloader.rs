use futures::stream;
use lb_log_targets::cryptarchia;
use lb_utils::tokio::stream::ReadAhead;
use libp2p::{PeerId, StreamProtocol};
use libp2p_stream::Control;
use tokio::{sync::oneshot, time, time::Duration};
use tracing::{debug, warn};

#[cfg(feature = "testing-era-boundaries")]
use crate::libp2p::testing::{RequestEvidence, timestamp_unix_ms, timestamp_unix_ns};
#[cfg(feature = "testing-era-boundaries")]
use crate::observation::{self, Observation};
use crate::{
    BlocksRequest, TipResponse,
    libp2p::{
        behaviour::{BlocksResponseReader, BoxedStream, TipResponseReader},
        errors::{ChainSyncError, ChainSyncErrorKind},
        messages::{BlocksResponse, RequestMessage},
        packing::{unpack_from_reader, unpack_from_reader_up_to},
        utils::{self, open_stream, send_message},
    },
    messages::SerialisedBlock,
};

const LOG_TARGET: &str = cryptarchia::sync::libp2p::DOWNLOADER;

/// The maximum number of block bytes read ahead of the consumer of a
/// download. Reading ahead lets the providing peer finish the batch, and free
/// its download slot, while blocks are still being applied.
const DOWNLOAD_BUFFER_BYTES: usize = 64 * 1024 * 1024;

/// The maximum number of blocks read ahead of the consumer of a download.
/// Bounds blocks with little or no payload, which barely count towards
/// [`DOWNLOAD_BUFFER_BYTES`]. Fits a whole batch at the default provider
/// batch size (1000 blocks).
const DOWNLOAD_BUFFER_ITEMS: usize = 1024;

pub struct Downloader;

impl Downloader {
    pub async fn send_tip_request(
        peer_id: PeerId,
        control: &mut Control,
        protocol_name: StreamProtocol,
        reply_sender: oneshot::Sender<Result<TipResponse, ChainSyncError>>,
    ) -> Result<TipResponseReader, ChainSyncError> {
        let mut stream = open_stream(peer_id, control, protocol_name).await?;

        let tip_request = RequestMessage::Tip;
        send_message(peer_id, &mut stream, &tip_request).await?;

        Ok(TipResponseReader::new(peer_id, stream, reply_sender))
    }

    pub async fn send_blocks_request(
        peer_id: PeerId,
        mut control: Control,
        request: BlocksRequest,
        protocol_name: StreamProtocol,
        reply_sender: oneshot::Sender<BoxedStream<Result<SerialisedBlock, ChainSyncError>>>,
        #[cfg(feature = "testing-era-boundaries")] evidence: Option<RequestEvidence>,
    ) -> Result<BlocksResponseReader, ChainSyncError> {
        let mut stream = open_stream(peer_id, &mut control, protocol_name).await?;

        let blocks_request = RequestMessage::Blocks(request);

        #[cfg(feature = "testing-era-boundaries")]
        if let Some(evidence) = &evidence {
            if observation::node_journal_is_active() {
                observation::emit(Observation::RequesterRequestStarted {
                    request: evidence.observation(),
                });
            }
            tracing::info!(
                target: LOG_TARGET,
                era_transition_event = "requester_request_started",
                timestamp_unix_ms = timestamp_unix_ms(),
                    timestamp_unix_ns = timestamp_unix_ns(),
                request_id = evidence.request_id,
                request_key = %evidence.request_key,
                peer_id = %evidence.peer_id,
                protocol = %evidence.protocol,
                target_block = ?evidence.target_block,
                requester_tip = ?evidence.requester_tip,
                latest_immutable_block = ?evidence.latest_immutable_block,
                "era-boundary experiment observed an outbound block request"
            );
        }

        send_message(peer_id, &mut stream, &blocks_request).await?;

        Ok(BlocksResponseReader::new(
            peer_id,
            stream,
            reply_sender,
            #[cfg(feature = "testing-era-boundaries")]
            evidence,
        ))
    }

    pub async fn receive_tip_response(
        tip_response_reader: TipResponseReader,
        timeout: Duration,
    ) -> Result<(), ChainSyncError> {
        let TipResponseReader {
            mut stream,
            peer_id,
            reply_channel,
        } = tip_response_reader;

        let response = time::timeout(timeout, unpack_from_reader::<TipResponse, _>(&mut stream))
            .await
            .map_err(|e| {
                warn!(target: LOG_TARGET, "Timeout while receiving tip from peer {}", peer_id);
                ChainSyncError::from((peer_id, e))
            })?
            .map_err(|e| ChainSyncError::from((peer_id, e)))
            .and_then(|response| match response {
                tip @ TipResponse::Tip { .. } => Ok(tip),
                TipResponse::Failure(reason) => Err(ChainSyncError {
                    peer: peer_id,
                    kind: ChainSyncErrorKind::RequestTipError(reason),
                }),
            });

        if let Err(e) = reply_channel.send(response) {
            debug!(
                target: LOG_TARGET,
                "Tip requester dropped before receiving response from peer {peer_id}: {e:?}"
            );
        }

        utils::close_stream(peer_id, stream).await
    }

    /// Receives the blocks response from the peer and delivers it to the
    /// caller of `request_blocks` as a stream of blocks, each in a response of
    /// at most `max_response_size` bytes. The response frames are read lazily
    /// as the caller polls the stream.
    #[expect(
        clippy::too_many_lines,
        reason = "keep ordered response evidence beside its protocol error handling"
    )]
    pub fn receive_blocks_response(
        blocks_response_reader: BlocksResponseReader,
        timeout: Duration,
        max_response_size: usize,
    ) -> Result<(), ChainSyncError> {
        let libp2p_stream = blocks_response_reader.stream;
        let peer_id = blocks_response_reader.peer_id;
        let reply_channel = blocks_response_reader.reply_channel;
        #[cfg(feature = "testing-era-boundaries")]
        let evidence = blocks_response_reader.evidence;

        #[cfg_attr(
            not(feature = "testing-era-boundaries"),
            expect(
                closure_returning_async_block,
                reason = "The try_unfold closure needs an async block to capture per-response evidence"
            )
        )]
        let stream = stream::try_unfold(
            (libp2p_stream, 0usize),
            move |(mut stream, response_count)| {
                #[cfg(feature = "testing-era-boundaries")]
                let evidence = evidence.clone();
                async move {
                    let response = time::timeout(
                        timeout,
                        unpack_from_reader_up_to(&mut stream, max_response_size),
                    )
                    .await;

                    match response {
                        Ok(Ok(BlocksResponse::Block(block))) => {
                            #[cfg(feature = "testing-era-boundaries")]
                            if let Some(evidence) = &evidence {
                                if observation::node_journal_is_active() {
                                    observation::emit(
                                        Observation::RequesterBlockResponseReceived {
                                            request: evidence.observation(),
                                            response_index: u64::try_from(response_count + 1)
                                                .expect("response index fits u64"),
                                        },
                                    );
                                }
                                tracing::info!(
                                                target: LOG_TARGET,
                                                era_transition_event = "requester_block_response_received",
                                                timestamp_unix_ms = timestamp_unix_ms(),
                                timestamp_unix_ns = timestamp_unix_ns(),
                                                request_id = evidence.request_id,
                                                request_key = %evidence.request_key,
                                                peer_id = %evidence.peer_id,
                                                protocol = %evidence.protocol,
                                                target_block = ?evidence.target_block,
                                                response_index = response_count + 1,
                                                block_bytes = block.len(),
                                                "era-boundary experiment observed a decoded block response"
                                            );
                            }
                            Ok(Some((block, (stream, response_count + 1))))
                        }
                        Ok(Ok(BlocksResponse::NoMoreBlocks)) => {
                            #[cfg(feature = "testing-era-boundaries")]
                            if let Some(evidence) = &evidence {
                                if observation::node_journal_is_active() {
                                    observation::emit(Observation::RequesterRequestCompleted {
                                        request: evidence.observation(),
                                        response_count: u64::try_from(response_count)
                                            .expect("response count fits u64"),
                                    });
                                }
                                tracing::info!(
                                                target: LOG_TARGET,
                                                era_transition_event = "requester_request_completed",
                                                timestamp_unix_ms = timestamp_unix_ms(),
                                timestamp_unix_ns = timestamp_unix_ns(),
                                                request_id = evidence.request_id,
                                                request_key = %evidence.request_key,
                                                peer_id = %evidence.peer_id,
                                                protocol = %evidence.protocol,
                                                target_block = ?evidence.target_block,
                                                response_count,
                                                "era-boundary experiment observed normal response completion"
                                            );
                            }
                            utils::close_stream(peer_id, stream).await?;
                            Ok(None)
                        }
                        Ok(Ok(BlocksResponse::Failure(reason))) => {
                            #[cfg(feature = "testing-era-boundaries")]
                            if let Some(evidence) = &evidence {
                                tracing::info!(
                                                target: LOG_TARGET,
                                                era_transition_event = "requester_request_rejected",
                                                timestamp_unix_ms = timestamp_unix_ms(),
                                timestamp_unix_ns = timestamp_unix_ns(),
                                                request_id = evidence.request_id,
                                                request_key = %evidence.request_key,
                                                peer_id = %evidence.peer_id,
                                                protocol = %evidence.protocol,
                                                target_block = ?evidence.target_block,
                                                response_count,
                                                failure_reason = %reason,
                                                "era-boundary experiment observed an explicit provider failure"
                                            );
                            }
                            utils::close_stream(peer_id, stream).await?;

                            Err(ChainSyncError {
                                peer: peer_id,
                                kind: ChainSyncErrorKind::BlockProviderUnavailable(reason),
                            })
                        }
                        Ok(Err(e)) => {
                            #[cfg(feature = "testing-era-boundaries")]
                            if let Some(evidence) = &evidence {
                                if observation::node_journal_is_active() {
                                    observation::emit(Observation::RequesterInterruptionDetected {
                                        request: evidence.observation(),
                                        response_count: u64::try_from(response_count)
                                            .expect("response count fits u64"),
                                        kind: observation::InterruptionKind::StreamError,
                                    });
                                }
                                tracing::info!(
                                                target: LOG_TARGET,
                                                era_transition_event = "requester_interruption_detected",
                                                timestamp_unix_ms = timestamp_unix_ms(),
                                timestamp_unix_ns = timestamp_unix_ns(),
                                                request_id = evidence.request_id,
                                                request_key = %evidence.request_key,
                                                peer_id = %evidence.peer_id,
                                                protocol = %evidence.protocol,
                                                target_block = ?evidence.target_block,
                                                response_count,
                                                interruption_kind = "stream_error",
                                                interruption = %e,
                                                "era-boundary experiment observed response-stream termination"
                                            );
                            }
                            drop(utils::close_stream(peer_id, stream).await);
                            Err(ChainSyncError::from((peer_id, e)))
                        }
                        Err(e) => {
                            #[cfg(feature = "testing-era-boundaries")]
                            if let Some(evidence) = &evidence {
                                if observation::node_journal_is_active() {
                                    observation::emit(Observation::RequesterInterruptionDetected {
                                        request: evidence.observation(),
                                        response_count: u64::try_from(response_count)
                                            .expect("response count fits u64"),
                                        kind: observation::InterruptionKind::ResponseTimeout,
                                    });
                                }
                                tracing::info!(
                                                target: LOG_TARGET,
                                                era_transition_event = "requester_interruption_detected",
                                                timestamp_unix_ms = timestamp_unix_ms(),
                                timestamp_unix_ns = timestamp_unix_ns(),
                                                request_id = evidence.request_id,
                                                request_key = %evidence.request_key,
                                                peer_id = %evidence.peer_id,
                                                protocol = %evidence.protocol,
                                                target_block = ?evidence.target_block,
                                                response_count,
                                                interruption_kind = "response_timeout",
                                                interruption = %e,
                                                "era-boundary experiment observed a response timeout"
                                            );
                            }
                            drop(utils::close_stream(peer_id, stream).await);
                            Err(ChainSyncError::from((peer_id, e)))
                        }
                    }
                }
            },
        );

        let boxed_stream: BoxedStream<_> =
            Box::new(
                ReadAhead::<_, DOWNLOAD_BUFFER_BYTES, DOWNLOAD_BUFFER_ITEMS>::new(Box::pin(stream)),
            );
        reply_channel
            .send(boxed_stream)
            .map_err(|_| ChainSyncError {
                peer: peer_id,
                kind: ChainSyncErrorKind::ChannelSendError(
                    "Failed to send blocks stream".to_owned(),
                ),
            })
    }
}
