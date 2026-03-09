use std::time::Duration;

use futures::{StreamExt as _, future::BoxFuture, stream::FuturesUnordered};
use lb_common_http_client::{BasicAuthCredentials, CommonHttpClient, ProcessedBlockEvent, Slot};
use lb_core::{
    codec::SerializeOp as _,
    header::HeaderId,
    mantle::{
        MantleTx, SignedMantleTx, Transaction as _,
        ledger::Tx as LedgerTx,
        ops::{
            Op, OpProof,
            channel::{ChannelId, MsgId, inscribe::InscriptionOp},
        },
        tx::TxHash,
    },
};
use lb_key_management_system_service::keys::{Ed25519Key, ZkKey};
use reqwest::Url;
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, info, warn};

use crate::state::{ChainTipResolution, MsgIdState, TxState, TxStatus};

const DEFAULT_RESUBMIT_INTERVAL: Duration = Duration::from_secs(30);
const DEFAULT_RECONNECT_DELAY: Duration = Duration::from_secs(5);
const DEFAULT_PUBLISH_CHANNEL_CAPACITY: usize = 256;

/// Inscription identifier.
pub type InscriptionId = TxHash;

/// Inscription status.
pub type InscriptionStatus = TxStatus;

/// Checkpoint for stop/resume functionality.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SequencerCheckpoint {
    /// Last message ID for chain continuity.
    pub last_msg_id: MsgId,
    /// Pending transactions to restore.
    pub pending_txs: Vec<(TxHash, SignedMantleTx)>,
    /// Last known LIB.
    pub lib: HeaderId,
    /// Last known LIB slot (for backfill range queries).
    pub lib_slot: Slot,
}

/// Result of a publish operation.
#[derive(Debug, Clone)]
pub struct PublishResult {
    /// The inscription ID (transaction hash).
    pub inscription_id: InscriptionId,
    /// Current checkpoint for persistence.
    pub checkpoint: SequencerCheckpoint,
    /// The message ID of the parent inscription (for lineage tracking).
    pub parent_msg_id: MsgId,
}

/// Configuration for the zone sequencer.
#[derive(Clone)]
pub struct SequencerConfig {
    pub resubmit_interval: Duration,
    pub reconnect_delay: Duration,
    pub publish_channel_capacity: usize,
}

impl Default for SequencerConfig {
    fn default() -> Self {
        Self {
            resubmit_interval: DEFAULT_RESUBMIT_INTERVAL,
            reconnect_delay: DEFAULT_RECONNECT_DELAY,
            publish_channel_capacity: DEFAULT_PUBLISH_CHANNEL_CAPACITY,
        }
    }
}

/// Sequencer errors.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("sequencer unavailable: {reason}")]
    Unavailable { reason: &'static str },
}

enum ActorRequest {
    Publish {
        data: Vec<u8>,
        reply: oneshot::Sender<Result<(SignedMantleTx, PublishResult), Error>>,
    },
    Status {
        id: InscriptionId,
        reply: oneshot::Sender<Result<TxStatus, Error>>,
    },
    Checkpoint {
        reply: oneshot::Sender<Result<SequencerCheckpoint, Error>>,
    },
}

enum InFlight {
    ResubmittedBatch {
        results: Vec<(InscriptionId, Result<(), String>)>,
    },
}

/// Zone sequencer client.
pub struct ZoneSequencer {
    request_tx: mpsc::Sender<ActorRequest>,
    node_url: Url,
    http_client: CommonHttpClient,
}

impl ZoneSequencer {
    #[must_use]
    pub fn init(
        channel_id: ChannelId,
        signing_key: Ed25519Key,
        node_url: Url,
        auth: Option<BasicAuthCredentials>,
        checkpoint: Option<SequencerCheckpoint>,
    ) -> Self {
        Self::init_with_config(
            channel_id,
            signing_key,
            node_url,
            auth,
            SequencerConfig::default(),
            checkpoint,
        )
    }

    #[must_use]
    pub fn init_with_config(
        channel_id: ChannelId,
        signing_key: Ed25519Key,
        node_url: Url,
        auth: Option<BasicAuthCredentials>,
        config: SequencerConfig,
        checkpoint: Option<SequencerCheckpoint>,
    ) -> Self {
        let http_client = CommonHttpClient::new(auth);
        let (request_tx, request_rx) = mpsc::channel(config.publish_channel_capacity);

        tokio::spawn(run_loop(
            request_rx,
            channel_id,
            signing_key,
            node_url.clone(),
            http_client.clone(),
            config,
            checkpoint,
        ));

        Self {
            request_tx,
            node_url,
            http_client,
        }
    }

    /// Publish an inscription to the zone's channel.
    ///
    /// Returns the inscription ID and a checkpoint for persistence.
    pub async fn publish(&self, data: Vec<u8>) -> Result<PublishResult, Error> {
        let (reply_tx, reply_rx) = oneshot::channel();
        let request = ActorRequest::Publish {
            data,
            reply: reply_tx,
        };

        self.request_tx
            .send(request)
            .await
            .map_err(|_| Error::Unavailable {
                reason: "actor channel closed",
            })?;

        let (signed_tx, result) = reply_rx.await.map_err(|_| Error::Unavailable {
            reason: "actor dropped reply",
        })??;

        info!(
            "Created inscription - hash: {}, this msg_id: {}, parent msg_id: {}",
            hex::encode(result.inscription_id.to_bytes().unwrap_or_default()),
            hex::encode(result.checkpoint.last_msg_id.as_ref()),
            hex::encode(result.parent_msg_id.as_ref()),
        );

        // Post to network (best effort, will be resubmitted if needed)
        if let Err(e) = self
            .http_client
            .post_transaction(self.node_url.clone(), signed_tx)
            .await
        {
            warn!("Failed to post transaction: {e}");
        }

        Ok(result)
    }

    /// Get the status of an inscription.
    pub async fn status(&self, id: InscriptionId) -> Result<InscriptionStatus, Error> {
        let (reply_tx, reply_rx) = oneshot::channel();
        let request = ActorRequest::Status {
            id,
            reply: reply_tx,
        };

        self.request_tx
            .send(request)
            .await
            .map_err(|_| Error::Unavailable {
                reason: "actor channel closed",
            })?;

        reply_rx.await.map_err(|_| Error::Unavailable {
            reason: "actor dropped reply",
        })?
    }

    /// Get the current checkpoint for persistence.
    pub async fn checkpoint(&self) -> Result<SequencerCheckpoint, Error> {
        let (reply_tx, reply_rx) = oneshot::channel();
        let request = ActorRequest::Checkpoint { reply: reply_tx };

        self.request_tx
            .send(request)
            .await
            .map_err(|_| Error::Unavailable {
                reason: "actor channel closed",
            })?;

        reply_rx.await.map_err(|_| Error::Unavailable {
            reason: "actor dropped reply",
        })?
    }
}

async fn initialize_from_checkpoint(
    http_client: &CommonHttpClient,
    node_url: &Url,
    reconnect_delay: Duration,
    checkpoint: Option<SequencerCheckpoint>,
) -> (TxState, HeaderId, Slot, MsgId, bool) {
    // Get current network state
    let info = loop {
        match http_client.consensus_info(node_url.clone()).await {
            Ok(info) => {
                info!(
                    "Sequencer connected: tip={:?}, lib={:?}",
                    info.tip, info.lib
                );
                break info;
            }
            Err(e) => {
                warn!("Failed to fetch consensus info: {e}, retrying in {reconnect_delay:?}",);
                tokio::time::sleep(reconnect_delay).await;
            }
        }
    };

    if let Some(cp) = checkpoint {
        info!(
            "Restoring from checkpoint: {} pending txs, lib={:?}, lib_slot={}",
            cp.pending_txs.len(),
            cp.lib,
            cp.lib_slot.into_inner(),
        );
        let mut state = TxState::new(cp.lib);
        // Restore pending transactions
        for (hash, tx) in cp.pending_txs {
            state.submit(hash, tx);
        }
        // Use checkpoint's lib_slot as starting point for backfill
        (state, info.tip, cp.lib_slot, cp.last_msg_id, true)
    } else {
        // Fresh start: get lib slot from network
        let lib_slot = get_lib_slot(http_client, node_url, info.lib).await;
        info!("Starting fresh (no checkpoint)");
        (
            TxState::new(info.lib),
            info.tip,
            lib_slot,
            MsgId::root(),
            false,
        )
    }
}

async fn bootstrap_history_on_clean_start(
    state: &mut TxState,
    tip: HeaderId,
    channel_id: ChannelId,
    http_client: &CommonHttpClient,
    node_url: &Url,
) {
    let tip_slot = match http_client.get_block(node_url.clone(), tip).await {
        Ok(Some(block)) => block.header().slot(),
        Ok(None) => Slot::genesis(),
        Err(e) => {
            warn!("Failed to fetch tip block for startup backfill: {e}");
            return;
        }
    };

    let to: u64 = tip_slot.into();
    if to == 0 {
        return;
    }

    debug!("Bootstrapping inscription lineage from genesis to tip slot {to}",);

    match http_client.get_blocks(node_url.clone(), 1, to).await {
        Ok(blocks) => {
            let current_lib = state.lib();
            for block in blocks {
                let block_id = block.header.id;
                let parent_id = block.header.parent_block;
                let our_txs = collect_msg_id_state_from_block(
                    block.header.id,
                    block.header.slot,
                    block.transactions.iter(),
                    channel_id,
                );
                state.process_block(block_id, parent_id, current_lib, &our_txs);
            }
        }
        Err(e) => {
            warn!("Failed to bootstrap startup lineage: {e}");
        }
    }
}

async fn get_lib_slot(http_client: &CommonHttpClient, node_url: &Url, lib: HeaderId) -> Slot {
    // Try to get the block to find its slot
    match http_client.get_block(node_url.clone(), lib).await {
        Ok(Some(block)) => block.header().slot(),
        Ok(None) => {
            // Genesis case - slot 0
            Slot::genesis()
        }
        Err(e) => {
            warn!("Failed to get lib block slot: {e}, assuming slot 0");
            Slot::genesis()
        }
    }
}

async fn connect_blocks_stream(
    http_client: &CommonHttpClient,
    node_url: &Url,
    reconnect_delay: Duration,
) -> impl futures::Stream<Item = ProcessedBlockEvent> {
    loop {
        match http_client.get_blocks_stream(node_url.clone()).await {
            Ok(stream) => return stream,
            Err(e) => {
                warn!("Failed to connect to blocks stream: {e}, retrying in {reconnect_delay:?}",);
                tokio::time::sleep(reconnect_delay).await;
            }
        }
    }
}

async fn run_loop(
    mut request_rx: mpsc::Receiver<ActorRequest>,
    channel_id: ChannelId,
    signing_key: Ed25519Key,
    node_url: Url,
    http_client: CommonHttpClient,
    config: SequencerConfig,
    checkpoint: Option<SequencerCheckpoint>,
) {
    let (state, current_tip, lib_slot, last_msg_id, restored_from_checkpoint) =
        initialize_from_checkpoint(&http_client, &node_url, config.reconnect_delay, checkpoint)
            .await;
    let mut state = Some(state);
    let mut current_tip = Some(current_tip);
    let mut lib_slot = lib_slot;
    let mut last_msg_id = last_msg_id;

    if let (Some(s), Some(tip)) = (state.as_mut(), current_tip) {
        bootstrap_history_on_clean_start(s, tip, channel_id, &http_client, &node_url).await;

        match s.resolve_inscription_chain_tip() {
            ChainTipResolution::Determinate(msg_id) => {
                last_msg_id = msg_id;
                info!(
                    "Startup lineage tip resolved to '{}'",
                    hex::encode(msg_id.as_ref())
                );
            }
            ChainTipResolution::NoInscriptions => {
                if restored_from_checkpoint {
                    info!(
                        "No on-chain inscriptions found on startup; keeping checkpoint parent {:?}",
                        hex::encode(last_msg_id.as_ref())
                    );
                }
            }
            ChainTipResolution::Ambiguous => {
                warn!(
                    "Startup lineage is ambiguous; publish will pause until chain tip becomes \
                    determinate"
                );
            }
        }
    }

    let mut resubmit_interval = tokio::time::interval(config.resubmit_interval);
    let mut resubmit_active = false;
    let mut in_flight: FuturesUnordered<BoxFuture<'static, InFlight>> = FuturesUnordered::new();

    loop {
        let blocks_stream =
            connect_blocks_stream(&http_client, &node_url, config.reconnect_delay).await;
        tokio::pin!(blocks_stream);

        loop {
            tokio::select! {
                Some(request) = request_rx.recv() => {
                    handle_request(
                        request,
                        &mut state,
                        current_tip,
                        lib_slot,
                        channel_id,
                        &signing_key,
                        &mut last_msg_id,
                    );
                }
                maybe_event = blocks_stream.next() => {
                    if let Some(ref event) = maybe_event {
                        handle_block_event(
                            event,
                            &mut state,
                            &mut current_tip,
                            &mut lib_slot,
                            channel_id,
                            &http_client,
                            &node_url,
                        )
                        .await;
                    } else {
                        warn!("Blocks stream disconnected, reconnecting...");
                        break;
                    }
                }
                Some(event) = in_flight.next(), if !in_flight.is_empty() => {
                    handle_inflight(event, &mut resubmit_active);
                }
                _ = resubmit_interval.tick(), if !resubmit_active && state.is_some() && current_tip.is_some() => {
                    enqueue_resubmit(
                        state.as_ref().unwrap(),
                        current_tip.unwrap(),
                        &http_client,
                        &node_url,
                        &in_flight,
                        &mut resubmit_active,
                    );
                }
            }
        }
    }
}

fn handle_request(
    request: ActorRequest,
    state: &mut Option<TxState>,
    current_tip: Option<HeaderId>,
    lib_slot: Slot,
    channel_id: ChannelId,
    signing_key: &Ed25519Key,
    last_msg_id: &mut MsgId,
) {
    let Some(tx_state) = state else {
        match request {
            ActorRequest::Publish { reply, .. } => {
                drop(reply.send(Err(Error::Unavailable {
                    reason: "not initialized",
                })));
            }
            ActorRequest::Status { reply, .. } => {
                drop(reply.send(Err(Error::Unavailable {
                    reason: "not initialized",
                })));
            }
            ActorRequest::Checkpoint { reply } => {
                drop(reply.send(Err(Error::Unavailable {
                    reason: "not initialized",
                })));
            }
        }
        return;
    };

    match request {
        ActorRequest::Publish { data, reply } => {
            // If we already have unpublished/pending inscriptions, keep extending that
            // local chain tip so multiple publishes before the next block remain ordered.
            let has_local_pending_chain = tx_state.pending_count() > 0;
            let parent_to_use = match current_tip {
                // TODO: This will be fixed in PR#2375 as there is no proof another sequencer
                // TODO: did not publish in the meantime and newly found inscriptions currently do
                // TODO: not invalidate invalid pending inscriptions
                Some(_) if has_local_pending_chain => *last_msg_id,
                Some(_) => match tx_state.resolve_inscription_chain_tip() {
                    ChainTipResolution::Determinate(this_id) => this_id,
                    ChainTipResolution::NoInscriptions => *last_msg_id,
                    ChainTipResolution::Ambiguous => {
                        drop(reply.send(Err(Error::Unavailable {
                            reason: "inscription fork is ambiguous",
                        })));
                        return;
                    }
                },
                None => *last_msg_id,
            };

            let (signed_tx, new_msg_id) =
                create_inscribe_tx(channel_id, signing_key, data, parent_to_use);
            let tx_hash = signed_tx.mantle_tx.hash();

            tx_state.submit(tx_hash, signed_tx.clone());
            *last_msg_id = new_msg_id;

            let checkpoint = build_checkpoint(tx_state, *last_msg_id, lib_slot);
            let result = PublishResult {
                inscription_id: tx_hash,
                checkpoint,
                parent_msg_id: parent_to_use,
            };
            drop(reply.send(Ok((signed_tx, result))));
        }
        ActorRequest::Status { id, reply } => {
            let result = current_tip.map_or(
                Err(Error::Unavailable {
                    reason: "not synced (no tip yet)",
                }),
                |tip| Ok(tx_state.status(&id, tip)),
            );
            drop(reply.send(result));
        }
        ActorRequest::Checkpoint { reply } => {
            let checkpoint = build_checkpoint(tx_state, *last_msg_id, lib_slot);
            drop(reply.send(Ok(checkpoint)));
        }
    }
}

fn build_checkpoint(state: &TxState, last_msg_id: MsgId, lib_slot: Slot) -> SequencerCheckpoint {
    SequencerCheckpoint {
        last_msg_id,
        pending_txs: state
            .all_pending_txs()
            .map(|(h, tx)| (*h, tx.clone()))
            .collect(),
        lib: state.lib(),
        lib_slot,
    }
}

async fn handle_block_event(
    event: &ProcessedBlockEvent,
    state: &mut Option<TxState>,
    current_tip: &mut Option<HeaderId>,
    lib_slot: &mut Slot,
    channel_id: ChannelId,
    http_client: &CommonHttpClient,
    node_url: &Url,
) {
    let block_id = event.block.header.id;
    let parent_id = event.block.header.parent_block;
    let tip = event.tip;
    let lib = event.lib;

    // Initialize state on first event
    if state.is_none() {
        *state = Some(TxState::new(lib));
    }

    let Some(s) = state.as_mut() else {
        return;
    };

    // Backfill if needed (self-healing on every event)
    // 1. Backfill finalized blocks up to LIB (only when state's LIB is behind)
    if lib != s.lib() {
        let new_lib_slot = get_lib_slot(http_client, node_url, lib).await;
        if *lib_slot < new_lib_slot {
            backfill_to_lib(
                s,
                *lib_slot,
                new_lib_slot,
                channel_id,
                http_client,
                node_url,
            )
            .await;
        }
        *lib_slot = new_lib_slot;
    }

    // 2. Backfill canonical chain if parent is missing
    if !s.has_block(&parent_id) && parent_id != s.lib() {
        backfill_canonical(s, parent_id, channel_id, http_client, node_url).await;
    }

    // Extract channel tx lineage keyed by block header id.
    let our_txs = collect_msg_id_state_from_block(
        event.block.header.id,
        event.block.header.slot,
        event.block.transactions.iter(),
        channel_id,
    );

    // Process the actual event block with real lib (triggers finalization if lib
    // advanced)
    s.process_block(block_id, parent_id, lib, &our_txs);
    *current_tip = Some(tip);
}

fn handle_inflight(event: InFlight, resubmit_active: &mut bool) {
    *resubmit_active = false;

    match event {
        InFlight::ResubmittedBatch { results } => {
            for (id, result) in results {
                if let Err(e) = result {
                    warn!("Failed to resubmit inscription {:?}: {}", id, e);
                }
            }
        }
    }
}

/// Backfill finalized blocks from current `lib_slot` to new `lib_slot`.
///
/// Uses `state.lib()` during replay to avoid premature finalization.
/// The caller is responsible for triggering finalization after backfill
/// completes.
async fn backfill_to_lib(
    state: &mut TxState,
    from_slot: Slot,
    to_slot: Slot,
    channel_id: ChannelId,
    http_client: &CommonHttpClient,
    node_url: &Url,
) {
    let from: u64 = from_slot.into();
    let to: u64 = to_slot.into();

    if from >= to {
        return; // No-op
    }

    debug!(
        "Backfilling finalized blocks from slot {} to {}",
        from + 1,
        to
    );

    match http_client.get_blocks(node_url.clone(), from + 1, to).await {
        Ok(blocks) => {
            for block in blocks {
                let block_id = block.header.id;
                let parent_id = block.header.parent_block;

                let our_txs = collect_msg_id_state_from_block(
                    block.header.id,
                    block.header.slot,
                    block.transactions.iter(),
                    channel_id,
                );

                // Use current state lib to avoid premature finalization
                let current_lib = state.lib();
                state.process_block(block_id, parent_id, current_lib, &our_txs);
            }
            debug!("Backfilled {} finalized blocks", to - from);
        }
        Err(e) => {
            warn!("Failed to backfill finalized blocks: {e}");
        }
    }
}

/// Backfill canonical chain backwards from a missing parent to LIB.
///
/// Uses `state.lib()` during replay to avoid premature finalization.
/// The caller is responsible for triggering finalization after backfill
/// completes.
async fn backfill_canonical(
    state: &mut TxState,
    missing_parent: HeaderId,
    channel_id: ChannelId,
    http_client: &CommonHttpClient,
    node_url: &Url,
) {
    debug!("Backfilling canonical chain from {:?}", missing_parent);

    let mut blocks_to_process = Vec::new();
    let mut current = missing_parent;
    let lib = state.lib();

    // Walk backwards until we find a known block or reach lib
    while !state.has_block(&current) && current != lib {
        match http_client.get_block(node_url.clone(), current).await {
            Ok(Some(block)) => {
                let parent = block.header().parent_block();
                blocks_to_process.push(block);
                current = parent;
            }
            Ok(None) => {
                warn!("Block {:?} not found during canonical backfill", current);
                break;
            }
            Err(e) => {
                warn!(
                    "Failed to fetch block {:?} during canonical backfill: {e}",
                    current
                );
                break;
            }
        }
    }

    // Process blocks in forward order (oldest first)
    blocks_to_process.reverse();
    for block in blocks_to_process {
        let block_id = block.header().id();
        let parent_id = block.header().parent_block();

        let our_txs = collect_msg_id_state_from_block(
            block.header().id(),
            block.header().slot(),
            block.transactions(),
            channel_id,
        );

        // Use current state lib to avoid premature finalization
        state.process_block(block_id, parent_id, lib, &our_txs);
    }

    debug!("Canonical backfill complete");
}

fn enqueue_resubmit(
    state: &TxState,
    tip: HeaderId,
    http_client: &CommonHttpClient,
    node_url: &Url,
    in_flight: &FuturesUnordered<BoxFuture<'static, InFlight>>,
    resubmit_active: &mut bool,
) {
    let pending: Vec<(InscriptionId, SignedMantleTx)> = state
        .pending_txs(tip)
        .map(|(hash, tx)| (*hash, tx.clone()))
        .collect();

    if pending.is_empty() {
        return;
    }

    debug!("Resubmitting {} pending inscription(s)", pending.len());
    let all_inscriptions = state
        .parent_msg_id_map()
        .values()
        .cloned()
        .collect::<Vec<_>>();
    // Extract the pending inscription(s) 'self_id' from all_inscriptions where its
    // 'tx_hash' is equal to 'pending.0'
    let pending_msg_ids: Vec<MsgId> = pending
        .iter()
        .filter_map(|(hash, _)| {
            all_inscriptions.iter().find_map(|msg_state| {
                msg_state
                    .iter()
                    .find(|v| v.tx_hash() == *hash)
                    .map(MsgIdState::this_id)
            })
        })
        .collect();

    debug!(
        "Resubmitting inscriptions: {}",
        pending_msg_ids
            .iter()
            .map(|id| hex::encode(id.as_ref()))
            .collect::<Vec<_>>()
            .join(", ")
    );

    let client = http_client.clone();
    let url = node_url.clone();
    *resubmit_active = true;

    in_flight.push(Box::pin(async move {
        let mut results = Vec::with_capacity(pending.len());
        for (id, tx) in pending {
            let result = client
                .post_transaction(url.clone(), tx)
                .await
                .map_err(|e| e.to_string());
            results.push((id, result));
        }
        InFlight::ResubmittedBatch { results }
    }));
}

#[expect(
    single_use_lifetimes,
    reason = "stable Rust requires a named lifetime for this impl Iterator item reference"
)]
fn collect_msg_id_state_from_block<'a>(
    header_id: HeaderId,
    slot: Slot,
    txs: impl Iterator<Item = &'a SignedMantleTx>,
    channel_id: ChannelId,
) -> Vec<MsgIdState> {
    let mut inscriptions = Vec::new();

    for tx in txs {
        if let Some((msg_state, inscription_msg)) =
            extract_msg_id_state(header_id, slot, tx, channel_id)
        {
            info!(
                "Found inscription - block: '{}', slot: {}, msg: '{}', parent_id: {}, this_id: {}",
                msg_state.header_id(),
                msg_state.slot().into_inner(),
                inscription_msg,
                hex::encode(msg_state.parent_id().as_ref()),
                hex::encode(msg_state.this_id().as_ref())
            );
            inscriptions.push(msg_state);
        }
    }

    inscriptions
}

fn extract_msg_id_state(
    header_id: HeaderId,
    slot: Slot,
    tx: &SignedMantleTx,
    channel_id: ChannelId,
) -> Option<(MsgIdState, String)> {
    tx.mantle_tx.ops.iter().find_map(|op| {
        if let Op::ChannelInscribe(inscribe) = op
            && inscribe.channel_id == channel_id
        {
            let tx_hash = tx.mantle_tx.hash();
            let parent_id = inscribe.parent;
            let this_id = inscribe.id();
            let msg_state = MsgIdState::new(header_id, slot, tx_hash, parent_id, this_id);
            let inscription_msg = String::from_utf8_lossy(&inscribe.inscription).into_owned();
            return Some((msg_state, inscription_msg));
        }
        None
    })
}

fn create_inscribe_tx(
    channel_id: ChannelId,
    signing_key: &Ed25519Key,
    inscription: Vec<u8>,
    parent: MsgId,
) -> (SignedMantleTx, MsgId) {
    let signer = signing_key.public_key();

    let inscribe_op = InscriptionOp {
        channel_id,
        inscription,
        parent,
        signer,
    };
    let msg_id = inscribe_op.id();

    let ledger_tx = LedgerTx::new(vec![], vec![]);

    let inscribe_tx = MantleTx {
        ops: vec![Op::ChannelInscribe(inscribe_op)],
        ledger_tx,
        storage_gas_price: 0,
        execution_gas_price: 0,
    };

    let tx_hash = inscribe_tx.hash();
    let signature = signing_key.sign_payload(tx_hash.as_signing_bytes().as_ref());

    let signed_tx = SignedMantleTx {
        ops_proofs: vec![OpProof::Ed25519Sig(signature)],
        ledger_tx_proof: ZkKey::multi_sign(&[], tx_hash.as_ref())
            .expect("multi-sign with empty key set"),
        mantle_tx: inscribe_tx,
    };

    (signed_tx, msg_id)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn header_id(n: u8) -> HeaderId {
        let mut bytes = [0u8; 32];
        bytes[0] = n;
        HeaderId::from(bytes)
    }

    fn publish_parent_id(tx: &SignedMantleTx) -> MsgId {
        match tx.mantle_tx.ops.first() {
            Some(Op::ChannelInscribe(inscribe)) => inscribe.parent,
            _ => panic!("expected a ChannelInscribe op"),
        }
    }

    fn publish_this_id(tx: &SignedMantleTx) -> MsgId {
        match tx.mantle_tx.ops.first() {
            Some(Op::ChannelInscribe(inscribe)) => inscribe.id(),
            _ => panic!("expected a ChannelInscribe op"),
        }
    }

    fn test_tx_hash(seed: u8) -> TxHash {
        let channel_id = ChannelId::from([seed; 32]);
        let signing_key = Ed25519Key::from_bytes(&[seed; 32]);
        let (tx, _) = create_inscribe_tx(channel_id, &signing_key, vec![seed], MsgId::root());
        tx.mantle_tx.hash()
    }

    #[test]
    fn test_create_inscribe_tx_chains_parent() {
        // Verify that inscribe transactions properly chain parent IDs
        let channel_id = ChannelId::from([1u8; 32]);
        let signing_key = Ed25519Key::from_bytes(&[0u8; 32]);

        let root_msg_id = MsgId::root();

        // Create first inscription with root as parent
        let (tx1, msg_id_1) =
            create_inscribe_tx(channel_id, &signing_key, vec![1, 2, 3, 4], root_msg_id);

        // Verify transaction is well-formed
        assert_eq!(tx1.mantle_tx.ops.len(), 1);
        assert!(matches!(tx1.mantle_tx.ops[0], Op::ChannelInscribe(_)));

        // Create second inscription with first message ID as parent
        let (tx2, msg_id_2) =
            create_inscribe_tx(channel_id, &signing_key, vec![1, 2, 3, 4], msg_id_1);

        // Verify the parent chain is different
        assert_ne!(msg_id_1, msg_id_2);
        assert_ne!(msg_id_1, root_msg_id);

        // Verify both transactions are different
        assert_ne!(tx1.mantle_tx.hash(), tx2.mantle_tx.hash());

        // Verify the second transaction references the first as parent
        if let Op::ChannelInscribe(inscribe) = &tx2.mantle_tx.ops[0] {
            assert_eq!(inscribe.parent, msg_id_1);
        } else {
            panic!("Expected ChannelInscribe operation");
        }
    }

    #[test]
    fn test_collect_msg_id_state_from_block() {
        // Verify that collect_msg_id_state_from_block correctly extracts channel
        // inscriptions
        use lb_core::mantle::{MantleTx, Transaction as _, ledger::Tx as LedgerTx};

        let channel_id = ChannelId::from([1u8; 32]);
        let other_channel_id = ChannelId::from([2u8; 32]);
        let signing_key = Ed25519Key::from_bytes(&[0u8; 32]);
        let signer = signing_key.public_key();

        // Create inscription for our channel
        let inscribe_op_1 = InscriptionOp {
            channel_id,
            inscription: vec![1, 2, 3],
            parent: MsgId::root(),
            signer,
        };

        // Create inscription for different channel (should be filtered)
        let inscribe_op_2 = InscriptionOp {
            channel_id: other_channel_id,
            inscription: vec![4, 5, 6],
            parent: MsgId::root(),
            signer,
        };

        let ledger_tx = LedgerTx::new(vec![], vec![]);

        let tx1 = MantleTx {
            ops: vec![Op::ChannelInscribe(inscribe_op_1)],
            ledger_tx: ledger_tx.clone(),
            storage_gas_price: 0,
            execution_gas_price: 0,
        };

        let tx2 = MantleTx {
            ops: vec![Op::ChannelInscribe(inscribe_op_2)],
            ledger_tx,
            storage_gas_price: 0,
            execution_gas_price: 0,
        };

        let signed_tx1 = SignedMantleTx {
            ops_proofs: vec![],
            ledger_tx_proof: ZkKey::multi_sign(&[], tx1.hash().as_ref()).expect("multi-sign"),
            mantle_tx: tx1,
        };

        let signed_tx2 = SignedMantleTx {
            ops_proofs: vec![],
            ledger_tx_proof: ZkKey::multi_sign(&[], tx2.hash().as_ref()).expect("multi-sign"),
            mantle_tx: tx2,
        };

        let header = header_id(1);
        let txs = [signed_tx1, signed_tx2];

        // Collect only inscriptions for our channel
        let result =
            collect_msg_id_state_from_block(header, Slot::new(123), txs.iter(), channel_id);

        // Should have only 1 entry (from our channel)
        assert_eq!(result.len(), 1);

        let msg_state = &result[0];
        assert_eq!(msg_state.parent_id(), MsgId::root());
        assert_eq!(msg_state.header_id(), header);
    }

    #[tokio::test]
    async fn publish_uses_chain_tip_over_stale_last_msg_id() {
        let genesis = header_id(0);
        let b1 = header_id(1);
        let channel_id = ChannelId::from([9u8; 32]);
        let signing_key = Ed25519Key::from_bytes(&[3u8; 32]);

        let mut tx_state = TxState::new(genesis);
        let this_id = MsgId::from([42u8; 32]);
        let known_hash = test_tx_hash(7);
        tx_state.process_block(
            b1,
            genesis,
            genesis,
            &[MsgIdState::new(
                b1,
                Slot::new(123),
                known_hash,
                MsgId::root(),
                this_id,
            )],
        );

        let mut state = Some(tx_state);
        let mut stale_last_msg_id = MsgId::root();
        let (reply_tx, reply_rx) = oneshot::channel();
        handle_request(
            ActorRequest::Publish {
                data: b"after-restart".to_vec(),
                reply: reply_tx,
            },
            &mut state,
            Some(b1),
            Slot::genesis(),
            channel_id,
            &signing_key,
            &mut stale_last_msg_id,
        );

        let (signed_tx, _result) = reply_rx
            .await
            .expect("publish reply dropped")
            .expect("publish should succeed");
        assert_eq!(publish_parent_id(&signed_tx), this_id);
        assert_ne!(stale_last_msg_id, MsgId::root());
    }

    #[tokio::test]
    async fn publish_rejects_ambiguous_chain_tip() {
        let genesis = header_id(0);
        let b1 = header_id(1);
        let b2 = header_id(2);
        let channel_id = ChannelId::from([8u8; 32]);
        let signing_key = Ed25519Key::from_bytes(&[4u8; 32]);

        let mut tx_state = TxState::new(genesis);
        tx_state.process_block(
            b1,
            genesis,
            genesis,
            &[MsgIdState::new(
                b1,
                Slot::new(123),
                test_tx_hash(1),
                MsgId::root(),
                MsgId::from([11u8; 32]),
            )],
        );
        tx_state.process_block(
            b2,
            genesis,
            genesis,
            &[MsgIdState::new(
                b2,
                Slot::new(234),
                test_tx_hash(2),
                MsgId::root(),
                MsgId::from([12u8; 32]),
            )],
        );

        let mut state = Some(tx_state);
        let mut last_msg_id = MsgId::root();
        let (reply_tx, reply_rx) = oneshot::channel();
        handle_request(
            ActorRequest::Publish {
                data: b"should-fail".to_vec(),
                reply: reply_tx,
            },
            &mut state,
            Some(b2),
            Slot::genesis(),
            channel_id,
            &signing_key,
            &mut last_msg_id,
        );

        let err = reply_rx
            .await
            .expect("publish reply dropped")
            .expect_err("publish should fail when chain tip is ambiguous");
        assert!(matches!(
            err,
            Error::Unavailable {
                reason: "inscription fork is ambiguous"
            }
        ));
    }

    #[tokio::test]
    async fn publish_chains_multiple_pending_before_next_block() {
        let genesis = header_id(0);
        let b1 = header_id(1);
        let channel_id = ChannelId::from([7u8; 32]);
        let signing_key = Ed25519Key::from_bytes(&[7u8; 32]);

        // On-chain determinate tip.
        let mut tx_state = TxState::new(genesis);
        let chain_tip = MsgId::from([77u8; 32]);
        tx_state.process_block(
            b1,
            genesis,
            genesis,
            &[MsgIdState::new(
                b1,
                Slot::new(123),
                test_tx_hash(77),
                MsgId::root(),
                chain_tip,
            )],
        );

        let mut state = Some(tx_state);
        let mut last_msg_id = chain_tip;

        // First publish should build on chain tip.
        let (reply_tx_1, reply_rx_1) = oneshot::channel();
        handle_request(
            ActorRequest::Publish {
                data: b"m-1".to_vec(),
                reply: reply_tx_1,
            },
            &mut state,
            Some(b1),
            Slot::genesis(),
            channel_id,
            &signing_key,
            &mut last_msg_id,
        );
        let (tx_1, _) = reply_rx_1
            .await
            .expect("publish reply dropped")
            .expect("first publish should succeed");
        let msg_1 = publish_this_id(&tx_1);
        assert_eq!(publish_parent_id(&tx_1), chain_tip);

        // Second publish happens before any new block is processed.
        // It must build on the first publish, not on the same chain tip again.
        let (reply_tx_2, reply_rx_2) = oneshot::channel();
        handle_request(
            ActorRequest::Publish {
                data: b"m-2".to_vec(),
                reply: reply_tx_2,
            },
            &mut state,
            Some(b1),
            Slot::genesis(),
            channel_id,
            &signing_key,
            &mut last_msg_id,
        );
        let (tx_2, _) = reply_rx_2
            .await
            .expect("publish reply dropped")
            .expect("second publish should succeed");
        assert_eq!(publish_parent_id(&tx_2), msg_1);
    }
}
