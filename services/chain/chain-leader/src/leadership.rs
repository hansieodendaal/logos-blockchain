use std::{
    fmt::{Debug, Display},
    sync::Arc,
    time::Instant,
};

use futures::{StreamExt as _, stream};
use lb_chain_service::{
    EpochStateQueryResult,
    api::{CryptarchiaServiceApi, CryptarchiaServiceData},
};
use lb_core::{
    header::HeaderId,
    mantle::Utxo,
    proofs::leader_proof::{
        Error as LeaderProofError, Groth16LeaderProof, LeaderPrivate, LeaderProof as _,
        LeaderPublic,
    },
    sdp::blend::{PolEpochState, PolEpochStateSource},
};
use lb_cryptarchia_engine::{Epoch, Slot};
use lb_key_management_system_service::{
    api::KmsServiceApi, backend::preload::KeyId, keys::Ed25519Key,
    operators::zk::leader::BuildPrivateInputsWithLeaderKey,
};
use lb_ledger::{EpochState, UtxoTree};
use lb_log_targets::{chain, diagnostic::BLEND_REACHABILITY};
use lb_time_service::{EpochSlotTickStream, SlotTick, TimeServiceMessage};
use lb_utils::tokio::task::spawn_blocking;
use lb_wallet_service::{
    UtxoWithKeyId,
    api::{WalletApi, WalletApiError, WalletServiceData},
};
use overwatch::services::{AsServiceId, relay::OutboundRelay};
#[cfg(test)]
pub use pol_tests::test_config;
use rand::rngs::OsRng;
use tokio::{
    sync::{mpsc, oneshot},
    task::JoinError,
};

use crate::{
    WinningPolEpochSlots, WinningPolSlotStream, WinningSlotFuture,
    kms::{KmsAdapter, PreloadKmsService},
    metrics,
};

const LOG_TARGET: &str = chain::leader::LEADERSHIP;

/// Return a leadership proof and signing key if the current slot is a winning
/// one for any of the eligible UTXOs, for use in a block proposal.
///
/// If the slot is not a winning one, it returns `Ok(None)`.
#[expect(
    clippy::cognitive_complexity,
    reason = "TODO: address this in a dedicated refactor"
)]
#[expect(
    clippy::too_many_arguments,
    reason = "Parent identity and sibling count are needed for audit proof generation"
)]
#[expect(
    clippy::too_many_lines,
    reason = "Ordinary proof generation and best-effort siblings share one winning witness"
)]
pub async fn build_proof_batch_for<Wallet, RuntimeServiceId>(
    utxos: &[UtxoWithKeyId],
    latest_tree: &UtxoTree,
    epoch_state: &EpochState,
    slot: Slot,
    parent_block_id: HeaderId,
    additional_siblings: usize,
    wallet: &WalletApi<Wallet, RuntimeServiceId>,
    kms: &(impl KmsAdapter<RuntimeServiceId, KeyId = KeyId> + Sync),
) -> Result<Option<(Vec<(usize, Groth16LeaderProof)>, Ed25519Key)>, BuildProofError>
where
    Wallet: WalletServiceData,
    RuntimeServiceId: Debug + Display + Sync + AsServiceId<Wallet>,
{
    let (mut non_winning_utxos, mut winning_utxos, start) = (0usize, 0usize, Instant::now());
    for UtxoWithKeyId { utxo, key_id } in utxos {
        let public_inputs = public_inputs_for_slot(epoch_state, slot, latest_tree);
        let winning = match kms
            .check_winning_with_key(key_id.clone(), utxo, &public_inputs)
            .await
        {
            Ok(winning) => winning,
            Err(e) => {
                metrics::consensus_proposals_create_failed("leadership_check");
                tracing::error!(
                    target: LOG_TARGET,
                    "Failed to check winning utxo {:?} for {slot:?}: {e:?}",
                    utxo.id(),
                );
                continue;
            }
        };

        if winning {
            winning_utxos += 1;
            tracing::debug!(
                target: LOG_TARGET,
                "leader for slot {:?}, {:?}/{:?}",
                slot,
                utxo.note.value,
                epoch_state.total_stake()
            );

            let (private_inputs, leader_signing_key) = match kms
                .build_private_inputs_for_winning_utxo_and_slot(
                    key_id.clone(),
                    utxo,
                    epoch_state,
                    public_inputs,
                    latest_tree,
                )
                .await
            {
                Ok(result) => result,
                Err(e) => {
                    metrics::consensus_proposals_create_failed("private_inputs");
                    tracing::error!(
                        target: LOG_TARGET,
                        "Failed to build private inputs for winning utxo {:?} for {slot:?}: {e:?}",
                        utxo.id(),
                    );
                    continue;
                }
            };

            let ordinary_voucher_cm = match wallet.generate_new_voucher().await {
                Ok(voucher_cm) => voucher_cm,
                Err(e) => {
                    metrics::consensus_proposals_create_failed("voucher_generation");
                    tracing::error!(
                        target: LOG_TARGET,
                        "Failed to generate voucher for winning utxo {:?} for {slot:?}: {e:?}",
                        utxo.id(),
                    );
                    continue;
                }
            };

            let retained_witness = (additional_siblings > 0).then(|| private_inputs.clone());
            let res = spawn_blocking("logos/chain/leader-proof-blocking", move || {
                Groth16LeaderProof::prove(private_inputs, ordinary_voucher_cm)
            })
            .await;
            let ordinary_proof = match res {
                Ok(Ok(proof)) => proof,
                Ok(Err(e)) => {
                    metrics::consensus_proposals_create_failed("proof_generation");
                    tracing::error!(
                        target: LOG_TARGET,
                        "Failed to build proof for winning utxo {:?} for {slot:?}: {e:?}",
                        utxo.id(),
                    );
                    continue;
                }
                Err(e) => {
                    metrics::consensus_proposals_create_failed("proof_task");
                    tracing::error!(
                        target: LOG_TARGET,
                        "Failed to wait for proof task for winning utxo {:?} for {slot:?}: {e:?}",
                        utxo.id(),
                    );
                    continue;
                }
            };

            let mut proof_candidates = Vec::with_capacity(additional_siblings.saturating_add(1));
            let mut voucher_commitments = Vec::with_capacity(proof_candidates.capacity());
            voucher_commitments.push(*ordinary_proof.voucher_cm());
            proof_candidates.push((0, ordinary_proof));

            if additional_siblings == 0 {
                return Ok(Some((proof_candidates, leader_signing_key)));
            }

            tracing::info!(
                target: LOG_TARGET,
                diagnostic = BLEND_REACHABILITY,
                event = "security_audit_sibling_batch_started",
                epoch = u32::from(epoch_state.epoch),
                slot = u64::from(slot),
                parent_block_id = %parent_block_id,
                configured_additional_siblings = additional_siblings,
                total_candidates = additional_siblings.saturating_add(1),
                "Starting valid sibling proposal batch"
            );

            for sibling_index in 1..=additional_siblings {
                let voucher_cm = match wallet.generate_new_voucher().await {
                    Ok(voucher_cm) => voucher_cm,
                    Err(error) => {
                        metrics::consensus_proposals_create_failed("sibling_voucher_generation");
                        tracing::warn!(
                            target: LOG_TARGET,
                            diagnostic = BLEND_REACHABILITY,
                            event = "security_audit_sibling_proof_failed",
                            epoch = u32::from(epoch_state.epoch),
                            slot = u64::from(slot),
                            parent_block_id = %parent_block_id,
                            sibling_index,
                            configured_additional_siblings = additional_siblings,
                            proof_generation_result = "voucher_generation_failed",
                            error = %error,
                            "Could not generate a sibling voucher"
                        );
                        continue;
                    }
                };

                if voucher_commitments.contains(&voucher_cm) {
                    metrics::consensus_proposals_create_failed("sibling_duplicate_voucher");
                    tracing::warn!(
                        target: LOG_TARGET,
                        diagnostic = BLEND_REACHABILITY,
                        event = "security_audit_sibling_proof_failed",
                        epoch = u32::from(epoch_state.epoch),
                        slot = u64::from(slot),
                        parent_block_id = %parent_block_id,
                        sibling_index,
                        configured_additional_siblings = additional_siblings,
                        proof_generation_result = "duplicate_voucher_commitment",
                        "Wallet returned a duplicate sibling voucher commitment"
                    );
                    continue;
                }
                voucher_commitments.push(voucher_cm);

                let sibling_witness = retained_witness
                    .as_ref()
                    .expect("a winning witness is retained when siblings are configured")
                    .clone();
                let proof_result = spawn_blocking("logos/chain/leader-proof-blocking", move || {
                    Groth16LeaderProof::prove(sibling_witness, voucher_cm)
                })
                .await;
                match proof_result {
                    Ok(Ok(proof)) => proof_candidates.push((sibling_index, proof)),
                    Ok(Err(error)) => {
                        metrics::consensus_proposals_create_failed("sibling_proof_generation");
                        tracing::warn!(
                            target: LOG_TARGET,
                            diagnostic = BLEND_REACHABILITY,
                            event = "security_audit_sibling_proof_failed",
                            epoch = u32::from(epoch_state.epoch),
                            slot = u64::from(slot),
                            parent_block_id = %parent_block_id,
                            sibling_index,
                            configured_additional_siblings = additional_siblings,
                            proof_generation_result = "proof_generation_failed",
                            error = %error,
                            "Could not prove a valid sibling block"
                        );
                    }
                    Err(error) => {
                        metrics::consensus_proposals_create_failed("sibling_proof_task");
                        tracing::warn!(
                            target: LOG_TARGET,
                            diagnostic = BLEND_REACHABILITY,
                            event = "security_audit_sibling_proof_failed",
                            epoch = u32::from(epoch_state.epoch),
                            slot = u64::from(slot),
                            parent_block_id = %parent_block_id,
                            sibling_index,
                            configured_additional_siblings = additional_siblings,
                            proof_generation_result = "proof_task_failed",
                            error = %error,
                            "Could not await sibling proof generation"
                        );
                    }
                }
            }

            return Ok(Some((proof_candidates, leader_signing_key)));
        }
        non_winning_utxos += 1;
    }

    tracing::trace!(
        target: LOG_TARGET,
        "Leadership scan completed in {:.2?} - slot: {}, eligible: {}, winning: {winning_utxos}, \
        non winning: {non_winning_utxos}",
        start.elapsed(),
        slot.into_inner(),
        utxos.len(),
    );

    Ok(None)
}

pub fn operator_for_private_inputs_arguments_for_winning_utxo_and_slot(
    utxo: &Utxo,
    epoch_state: &EpochState,
    public_inputs: LeaderPublic,
    latest_tree: &UtxoTree,
) -> Result<
    (
        BuildPrivateInputsWithLeaderKey,
        oneshot::Receiver<LeaderPrivate>,
        Ed25519Key,
    ),
    PrivateInputsError,
> {
    let (sender, receiver) = oneshot::channel();
    let aged_path = epoch_state
        .utxo_merkle_path(utxo)
        .ok_or(PrivateInputsError::AgedNoteNotFound)?;
    let latest_path = latest_tree
        .path(&utxo.id())
        .ok_or(PrivateInputsError::LatestNoteNotFound)?;
    // Generate a random one-time Ed25519 key for P_LEAD (as per PoL spec)
    let leader_signing_key = Ed25519Key::generate(&mut OsRng);
    let leader_pk = leader_signing_key.public_key();

    Ok((
        BuildPrivateInputsWithLeaderKey::new(
            sender,
            *utxo,
            public_inputs,
            aged_path,
            latest_path,
            leader_pk,
        ),
        receiver,
        leader_signing_key,
    ))
}

fn public_inputs_for_slot(
    epoch_state: &EpochState,
    slot: Slot,
    latest_tree: &UtxoTree,
) -> LeaderPublic {
    LeaderPublic::new(
        epoch_state.utxo_merkle_root(),
        latest_tree.root(),
        epoch_state.nonce,
        slot.into(),
        epoch_state.lottery_0,
        epoch_state.lottery_1,
    )
}

#[derive(thiserror::Error, Debug)]
pub enum PrivateInputsError {
    #[error("Aged note not found from merkle tree")]
    AgedNoteNotFound,
    #[error("Latest note not found from merkle tree")]
    LatestNoteNotFound,
    #[error("KMS API error: {0}")]
    KmsApi(#[from] overwatch::DynError),
    #[error("KMS API did not respond")]
    KmsResponse,
}

#[derive(thiserror::Error, Debug)]
pub enum BuildProofError {
    #[error("Wallet API error: {0}")]
    Wallet(#[from] WalletApiError),
    #[error("Private input generation failed: {0}")]
    PrivateInputs(#[from] PrivateInputsError),
    #[error("Proof generation failed: {0}")]
    Proof(#[from] LeaderProofError),
    #[error("Proof generation task failed: {0}")]
    ProofTask(#[from] JoinError),
}

/// The per-epoch chain state needed to check winning slots, shared by the
/// per-slot block-proposal path and the per-epoch winning-slot scan.
///
/// These inputs are all fixed for the whole epoch: `epoch_state` (including the
/// aged UTXO tree) and the wallet's leader-eligible notes (aged at the end of
/// the previous epoch). The block-proposal path additionally needs the *latest*
/// ledger state (to prove a note is still unspent), which it fetches separately
/// per slot; the Blend winning-slot scan does not, since the leadership quota
/// proof only attests that a note was aged, not that it is unspent.
pub struct SlotContext {
    /// Tip explicitly passed to `get_leader_aged_notes` for the wallet query.
    pub wallet_tip: HeaderId,
    pub epoch_state: EpochState,
    pub eligible_aged: Vec<UtxoWithKeyId>,
    /// Tip/LIB provenance of the chain-derived epoch state.
    pub source: PolEpochStateSource,
}

/// Per-subscriber background task that hands one lazy winning-slot stream per
/// epoch.
///
/// On subscribe it starts at the *current* slot and, for each epoch, hands the
/// subscriber a lazy [`WinningPolSlotStream`] over that epoch's slot range
/// (from the current slot for the ongoing epoch — so a mid-epoch start wastes
/// no work — and in full for each later epoch). The stream is lazy: no slot is
/// scanned until the subscriber drives it, and the subscriber decides how far
/// ahead to pre-compute (e.g. via the `Buffered` adapter), so the whole epoch
/// is never materialized here. This task only produces the cheap per-epoch
/// handoffs; it exits when the subscriber drops its stream.
#[expect(
    clippy::cognitive_complexity,
    reason = "TODO: address this in a dedicated refactor"
)]
pub async fn search_for_winning_slots<CryptarchiaService, Wallet, RuntimeServiceId>(
    cryptarchia_api: CryptarchiaServiceApi<CryptarchiaService>,
    wallet_api: WalletApi<Wallet, RuntimeServiceId>,
    kms: KmsServiceApi<PreloadKmsService<RuntimeServiceId>, RuntimeServiceId>,
    time_relay: OutboundRelay<TimeServiceMessage>,
    ledger_config: lb_ledger::Config,
    epoch_handoff_sender: mpsc::Sender<WinningPolEpochSlots>,
) where
    CryptarchiaService: CryptarchiaServiceData<Tx: Send>,
    Wallet: WalletServiceData,
    RuntimeServiceId: AsServiceId<Wallet>
        + AsServiceId<PreloadKmsService<RuntimeServiceId>>
        + Debug
        + Display
        + Send
        + Sync
        + 'static,
{
    // Subscribe to future slot ticks (used to detect epoch boundaries) and read
    // the current slot to start scanning from immediately.
    let Some(mut slot_timer) = async {
        let (sender, receiver) = oneshot::channel();
        time_relay
            .send(TimeServiceMessage::Subscribe { sender })
            .await
            .ok()?;
        receiver.await.ok()
    }
    .await
    else {
        tracing::error!(target: LOG_TARGET, "Failed to subscribe to slot ticks; winning slots subscriber cannot run.");
        return;
    };

    // Process one epoch at a time, starting from whichever slot is current when
    // we subscribe. Each iteration handles a single epoch; the `tokio::select!`
    // at the end yields the first tick of the next epoch to process, or `None`
    // when the tick stream ends (which ends the loop).
    let mut current_slot_tick = slot_timer.next().await;
    while let Some(SlotTick { slot, epoch }) = current_slot_tick {
        let Some(slot_context) =
            fetch_slot_context(&cryptarchia_api, &wallet_api, &ledger_config, slot).await
        else {
            tracing::debug!(target: LOG_TARGET, "Could not fetch slot context for slot {slot:?}; retrying on the next tick.");
            current_slot_tick = slot_timer.next().await;
            continue;
        };

        let SlotContext {
            wallet_tip,
            epoch_state,
            eligible_aged,
            source,
        } = slot_context;
        let state = PolEpochState {
            nonce: epoch_state.nonce,
            aged_utxo_root: epoch_state.utxo_merkle_root(),
            lottery_0: epoch_state.lottery_0,
            lottery_1: epoch_state.lottery_1,
            source,
        };
        tracing::debug!(
            target: LOG_TARGET,
            diagnostic = BLEND_REACHABILITY,
            event = "pol_epoch_state_frozen",
            epoch = u32::from(epoch),
            slot = u64::from(slot),
            wallet_tip_id = %wallet_tip,
            source_tip_id = %state.source.tip_id,
            source_tip_slot = u64::from(state.source.tip_slot),
            source_lib_id = %state.source.lib_id,
            source_lib_slot = u64::from(state.source.lib_slot),
            wallet_tip_matches_epoch_state_source = wallet_tip == state.source.tip_id,
            nonce = ?state.nonce,
            aged_utxo_root = ?state.aged_utxo_root,
            lottery_0 = ?state.lottery_0,
            lottery_1 = ?state.lottery_1,
            "Frozen ChainLeader epoch state for winning PoL slots"
        );

        // Hand the subscriber a *lazy* stream over this epoch's slot range. No
        // slot is scanned until the subscriber drives the stream, and it decides
        // how far ahead to pre-compute, so the whole epoch is never materialized
        // here. A scan made stale by an epoch rollover is implicitly abandoned:
        // the subscriber just stops polling it once it moves to the next epoch.
        let winning_slots_stream = epoch_winning_slots_stream(
            &ledger_config,
            epoch_state,
            &eligible_aged,
            kms.clone(),
            slot,
        );
        if epoch_handoff_sender
            .send(WinningPolEpochSlots {
                epoch,
                state,
                slots: winning_slots_stream,
            })
            .await
            .is_err()
        {
            tracing::debug!(target: LOG_TARGET, "Winning slots subscriber dropped its handoff stream; exiting.");
            return;
        }

        // Wait for the first tick of the next epoch to produce a new winning slot
        // stream and pass it to consumers.
        current_slot_tick = next_epoch_tick(&mut slot_timer, epoch).await;
    }

    tracing::trace!(target: LOG_TARGET, "Slot tick stream ended; winning slots subscriber exiting.");
}

/// Awaits the first slot tick belonging to an epoch other than `epoch` (i.e.
/// the first tick of the next epoch), returning it, or `None` if the tick
/// stream ends.
async fn next_epoch_tick(
    slot_timer: &mut EpochSlotTickStream,
    current_epoch: Epoch,
) -> Option<SlotTick> {
    loop {
        match slot_timer.next().await {
            Some(tick) if tick.epoch > current_epoch => return Some(tick),
            Some(_) => {}
            None => return None,
        }
    }
}

/// Fetches the [`SlotContext`] for `slot` from the tip: the tip header, the
/// slot's epoch state, and the wallet's eligible leader UTXOs (with the faucet
/// UTXO filtered out). Returns `None` if any lookup fails.
pub async fn fetch_slot_context<CryptarchiaService, Wallet, RuntimeServiceId>(
    cryptarchia_api: &CryptarchiaServiceApi<CryptarchiaService>,
    wallet_api: &WalletApi<Wallet, RuntimeServiceId>,
    ledger_config: &lb_ledger::Config,
    slot: Slot,
) -> Option<SlotContext>
where
    CryptarchiaService: CryptarchiaServiceData<Tx: Send>,
    Wallet: WalletServiceData,
    RuntimeServiceId: AsServiceId<Wallet> + Debug + Display + Sync,
{
    let wallet_tip = cryptarchia_api.info().await.ok()?.cryptarchia_info.tip;
    let EpochStateQueryResult {
        epoch_state,
        source_tip_id,
        source_tip_slot,
        source_lib_id,
        source_lib_slot,
        ..
    } = cryptarchia_api
        .get_epoch_state_with_source(slot)
        .await
        .ok()?
        .ok()?;
    let eligible_utxos = wallet_api
        .get_leader_aged_notes(Some(wallet_tip))
        .await
        .ok()?;
    let eligible = match &ledger_config.faucet_pk {
        Some(faucet_pk) => eligible_utxos
            .response
            .into_iter()
            .filter(|utxo| utxo.utxo.note.pk != *faucet_pk)
            .collect(),
        None => eligible_utxos.response,
    };
    Some(SlotContext {
        wallet_tip,
        epoch_state,
        eligible_aged: eligible,
        source: PolEpochStateSource {
            tip_id: source_tip_id,
            tip_slot: source_tip_slot,
            lib_id: source_lib_id,
            lib_slot: source_lib_slot,
        },
    })
}

/// Builds a *lazy* stream of one epoch's per-slot leadership-proof work: one
/// [`WinningSlotFuture`] per slot from `start_slot` to the epoch's last slot.
///
/// The stream does no work until polled. Each item is a future that, when
/// driven, performs the KMS lottery check for that slot and — on a win — builds
/// the leadership private inputs, resolving to `Some(LeaderPrivate)` for a
/// winning slot or `None` otherwise. The consumer drives the futures (and
/// decides how far ahead to pre-compute, e.g. via the `Buffered` adapter), so
/// the whole epoch is never materialized at once.
///
/// The winning check uses the aged UTXO tree (`epoch_state.utxos`), since the
/// leadership quota proof only attests that a note was aged at the end of the
/// previous epoch, not that it is unspent. Slots earlier than `start_slot` are
/// skipped so a mid-epoch subscriber wastes no work.
fn epoch_winning_slots_stream<RuntimeServiceId>(
    ledger_config: &lb_ledger::Config,
    epoch_state: EpochState,
    eligible_aged: &[UtxoWithKeyId],
    kms: impl KmsAdapter<RuntimeServiceId, KeyId = KeyId> + Send + Sync + 'static,
    start_slot: Slot,
) -> WinningPolSlotStream {
    let slots_per_epoch = ledger_config.epoch_length();
    let epoch_first_slot: u64 = ledger_config
        .epoch_config
        .starting_slot(&epoch_state.epoch, ledger_config.base_period_length())
        .into();
    let epoch_last_slot = epoch_first_slot
        .checked_add(slots_per_epoch)
        .expect("Epoch slot calculation overflow.")
        - 1;
    // Skip slots earlier than the start slot: a mid-epoch subscriber does not
    // waste work on slots it has already passed.
    let scan_starting_slot = u64::from(start_slot).max(epoch_first_slot);

    // Share the read-only per-epoch inputs across all per-slot futures.
    // `UtxoWithKeyId` is not `Clone`, so collect owned `(Utxo, KeyId)` pairs
    // (`Utxo` is `Copy`, `KeyId` is `Clone`).
    let epoch_state = Arc::new(epoch_state);
    let eligible_aged: Arc<Vec<(Utxo, KeyId)>> = Arc::new(
        eligible_aged
            .iter()
            .map(|UtxoWithKeyId { utxo, key_id }| (*utxo, key_id.clone()))
            .collect(),
    );
    let kms = Arc::new(kms);

    let stream = stream::iter(scan_starting_slot..=epoch_last_slot).map(move |slot| {
        let epoch_state = Arc::clone(&epoch_state);
        let eligible_aged = Arc::clone(&eligible_aged);
        let kms = Arc::clone(&kms);
        let is_slot_winning_task: WinningSlotFuture = Box::pin(async move {
            let public_inputs = public_inputs_for_slot(&epoch_state, slot.into(), &epoch_state.utxos);
            for (utxo, key_id) in eligible_aged.iter() {
                let winning = match kms
                    .check_winning_with_key(key_id.clone(), utxo, &public_inputs)
                    .await
                {
                    Ok(winning) => winning,
                    Err(e) => {
                        tracing::error!(
                            target: LOG_TARGET,
                            "Failed to check winning utxo {:?} at slot {slot}: {e:?}",
                            utxo.id(),
                        );
                        continue;
                    }
                };
                if !winning {
                    continue;
                }
                tracing::trace!(target: LOG_TARGET, "Found winning utxo with ID {:?} for slot {slot}", utxo.id());
                match kms
                    .build_private_inputs_for_winning_utxo_and_slot(
                        key_id.clone(),
                        utxo,
                        &epoch_state,
                        public_inputs,
                        &epoch_state.utxos,
                    )
                    .await
                {
                    Ok((leader_private, _)) => return Some(leader_private),
                    Err(e) => tracing::error!(
                        target: LOG_TARGET,
                        "Failed to build private inputs for winning utxo {:?} at slot {slot}: {e:?}",
                        utxo.id(),
                    ),
                }
            }
            None
        });
        is_slot_winning_task
    });

    Box::pin(stream)
}

#[cfg(test)]
mod pol_tests {
    use core::fmt;
    use std::{
        collections::HashSet,
        fmt::Formatter,
        num::NonZero,
        slice,
        sync::atomic::{AtomicUsize, Ordering},
    };

    use lb_core::{
        block::{Block, BlockTransactions, UncleHeaders},
        mantle::{
            SignedOps,
            gas::MainnetGasProfile,
            ledger::{Inputs, Note, Outputs, verification_mode::StandardMode},
            ops::{
                leader_claim::{VoucherCm, VoucherSecret},
                transfer::TransferOp,
            },
            transactions::states::Preverified,
        },
        proofs::leader_proof::check_winning,
        sdp::{MinStake, ServiceParameters, ServiceType},
    };
    use lb_cryptarchia_engine::EpochConfig;
    use lb_groth16::{Fr, fr_from_bytes_unchecked};
    use lb_key_management_system_service::keys::{UnsecuredZkKey, ZkKey};
    use lb_ledger::{
        config::{BlendPoWConfig, ModulusShift, PoWConfig, RewardPoWConfig},
        mantle::sdp::{
            Config as SdpConfig, ServiceRewardsParameters, rewards::blend::RewardsParameters,
        },
    };
    use lb_utils::math::{NonNegativeRatio, PositiveF64};
    use lb_wallet_service::{WalletMsg, WalletServiceSettings};
    use overwatch::services::{
        ServiceData,
        state::{NoOperator, NoState},
    };

    use super::*;

    /// An ordinary winning-slot proof batch contains only candidate zero.
    #[tokio::test]
    async fn test_build_proof_batch_without_siblings() {
        let (config, parent_state, utxo, key_id) = ledger_test_fixtures();
        let parent_id = HeaderId::from([0u8; 32]);
        let (wallet, voucher_count) = DummyWallet::spawn_with_distinct_vouchers();
        let (proofs, signing_key, slot) = find_winning_slot_and_build_proof_batch(
            &parent_state,
            UtxoWithKeyId { utxo, key_id },
            parent_id,
            0,
            &wallet,
            &DummyKms,
        )
        .await;

        assert_eq!(proofs.len(), 1);
        assert_eq!(proofs[0].0, 0);
        assert_eq!(voucher_count.load(Ordering::SeqCst), 1);
        validate_candidates_as_ordinary_node(
            &config,
            parent_state,
            parent_id,
            slot,
            &proofs,
            &signing_key,
        );
    }

    #[tokio::test]
    async fn test_build_proof_batch_with_one_sibling() {
        let (config, parent_state, utxo, key_id) = ledger_test_fixtures();
        let parent_id = HeaderId::from([0u8; 32]);
        let (wallet, voucher_count) = DummyWallet::spawn_with_distinct_vouchers();
        let (proofs, signing_key, slot) = find_winning_slot_and_build_proof_batch(
            &parent_state,
            UtxoWithKeyId { utxo, key_id },
            parent_id,
            1,
            &wallet,
            &DummyKms,
        )
        .await;

        assert_eq!(proofs.len(), 2);
        assert_eq!(
            proofs.iter().map(|(index, _)| *index).collect::<Vec<_>>(),
            [0, 1]
        );
        assert_eq!(voucher_count.load(Ordering::SeqCst), 2);
        validate_candidates_as_ordinary_node(
            &config,
            parent_state,
            parent_id,
            slot,
            &proofs,
            &signing_key,
        );
    }

    #[tokio::test]
    async fn test_build_proof_batch_with_multiple_siblings() {
        let (config, parent_state, utxo, key_id) = ledger_test_fixtures();
        let parent_id = HeaderId::from([0u8; 32]);
        let (wallet, voucher_count) = DummyWallet::spawn_with_distinct_vouchers();
        let (proofs, signing_key, slot) = find_winning_slot_and_build_proof_batch(
            &parent_state,
            UtxoWithKeyId { utxo, key_id },
            parent_id,
            3,
            &wallet,
            &DummyKms,
        )
        .await;

        assert_eq!(proofs.len(), 4);
        assert_eq!(
            proofs.iter().map(|(index, _)| *index).collect::<Vec<_>>(),
            [0, 1, 2, 3]
        );
        assert_eq!(voucher_count.load(Ordering::SeqCst), 4);
        validate_candidates_as_ordinary_node(
            &config,
            parent_state,
            parent_id,
            slot,
            &proofs,
            &signing_key,
        );
    }

    #[tokio::test]
    async fn test_build_proof_for() {
        let config = test_config();

        // Create secret key and leader
        let kms = DummyKms;
        let key_id = KeyId::from("0");
        let sk = UnsecuredZkKey::new(Fr::from(0u64));
        let pk = sk.to_public_key();

        // Create a UTXO
        let transfer = TransferOp::new(Inputs::empty(), Outputs::new([Note::new(1000u64, pk)]));
        let utxo = transfer.outputs.utxo_by_index(0, &transfer).unwrap();

        // Create aged/latest UTXO trees
        let aged_tree = UtxoTree::new().insert(utxo.id(), utxo).0;
        let latest_tree = UtxoTree::new().insert(utxo.id(), utxo).0;

        // Create EpochState
        let total_stake = utxo.note.value;
        let (lottery_0, lottery_1) = config
            .lottery_constants()
            .compute_lottery_values(total_stake);
        let epoch_state = EpochState {
            epoch: 1.into(),
            nonce: Fr::from(999u64),
            blend_pow_difficulty: Fr::from(0u64),
            utxos: aged_tree.clone(),
            total_stake,
            lottery_0,
            lottery_1,
            active_declarations: Arc::new(lb_core::sdp::Declarations::default()),
        };

        // Create dummy wallet service
        let wallet = DummyWallet::spawn();

        // Find a winning slot by calling `build_proof_batch_for` until it succeeds
        let (proof, winning_slot) = find_winning_slot_and_build_proof(
            (0..1000).map(Slot::from),
            UtxoWithKeyId { utxo, key_id },
            &epoch_state,
            &latest_tree,
            &wallet,
            &kms,
        )
        .await
        .expect("should find a winning slot and build a proof");
        assert_eq!(proof.voucher_cm(), &dummy_voucher_cm());

        // Verify proof
        let public_inputs = LeaderPublic::new(
            aged_tree.root(),
            latest_tree.root(),
            epoch_state.nonce,
            winning_slot.into(),
            epoch_state.lottery_0,
            epoch_state.lottery_1,
        );
        assert!(
            proof.verify(&public_inputs),
            "proof verification should succeed"
        );
    }

    /// Find a winning slot by calling `build_proof_batch_for` until it succeeds
    async fn find_winning_slot_and_build_proof(
        slots: impl Iterator<Item = Slot>,
        utxo: UtxoWithKeyId,
        epoch_state: &EpochState,
        latest_tree: &UtxoTree,
        wallet: &WalletApi<DummyWallet, TestRuntimeServiceId>,
        kms: &(impl KmsAdapter<TestRuntimeServiceId, KeyId = KeyId> + Sync),
    ) -> Option<(Groth16LeaderProof, Slot)> {
        for slot in slots {
            if let Some((proofs, _signing_key)) = build_proof_batch_for(
                slice::from_ref(&utxo),
                latest_tree,
                epoch_state,
                slot,
                HeaderId::from([0u8; 32]),
                0,
                wallet,
                kms,
            )
            .await
            .expect("proof build should not fail")
            {
                let (_, proof) = proofs
                    .into_iter()
                    .next()
                    .expect("ordinary proof candidate should exist");
                return Some((proof, slot));
            }
        }
        None
    }

    fn ledger_test_fixtures() -> (lb_ledger::Config, lb_ledger::LedgerState, Utxo, KeyId) {
        let config = test_config();
        let key_id = KeyId::from("0");
        let pk = UnsecuredZkKey::new(Fr::from(0u64)).to_public_key();
        let transfer = TransferOp::new(Inputs::empty(), Outputs::new([Note::new(1000u64, pk)]));
        let utxo = transfer.outputs.utxo_by_index(0, &transfer).unwrap();
        let parent_state = lb_ledger::LedgerState::from_utxos([utxo], &config);

        (config, parent_state, utxo, key_id)
    }

    async fn find_winning_slot_and_build_proof_batch(
        parent_state: &lb_ledger::LedgerState,
        utxo: UtxoWithKeyId,
        parent_id: HeaderId,
        additional_siblings: usize,
        wallet: &WalletApi<DummyWallet, TestRuntimeServiceId>,
        kms: &(impl KmsAdapter<TestRuntimeServiceId, KeyId = KeyId> + Sync),
    ) -> (Vec<(usize, Groth16LeaderProof)>, Ed25519Key, Slot) {
        let latest_tree = parent_state.latest_utxos().clone();
        let epoch_state = parent_state.epoch_state().clone();

        for slot in (1..10_000).map(Slot::from) {
            if let Some((proofs, signing_key)) = build_proof_batch_for(
                slice::from_ref(&utxo),
                &latest_tree,
                &epoch_state,
                slot,
                parent_id,
                additional_siblings,
                wallet,
                kms,
            )
            .await
            .expect("proof batch generation should not fail")
            {
                return (proofs, signing_key, slot);
            }
        }

        panic!("test fixture should win a genuine leadership slot");
    }

    fn validate_candidates_as_ordinary_node(
        config: &lb_ledger::Config,
        parent_state: lb_ledger::LedgerState,
        parent_id: HeaderId,
        slot: Slot,
        proof_candidates: &[(usize, Groth16LeaderProof)],
        signing_key: &Ed25519Key,
    ) {
        let public_inputs = public_inputs_for_slot(
            parent_state.epoch_state(),
            slot,
            parent_state.latest_utxos(),
        );
        let uncle_headers = UncleHeaders::empty();
        let uncle_slots = uncle_headers.slots();
        let ordinary_ledger = lb_ledger::Ledger::new(parent_id, parent_state, config.clone());
        let mut voucher_commitments = HashSet::new();
        let mut proof_bytes = HashSet::new();
        let mut block_ids = HashSet::new();
        let mut built_blocks: Vec<(usize, Block<SignedOps<Preverified, StandardMode>>)> =
            Vec::with_capacity(proof_candidates.len());

        for (sibling_index, proof) in proof_candidates {
            assert!(voucher_commitments.insert(*proof.voucher_cm()));
            assert!(proof.verify(&public_inputs));
            assert!(proof_bytes.insert(proof.proof().to_bytes().to_vec()));
            assert_eq!(
                proof.leader_key(),
                signing_key.public_key().as_unverified(),
                "all candidates must share the winning-slot signing key"
            );

            let block = Block::<SignedOps<Preverified, StandardMode>>::create(
                parent_id,
                slot,
                uncle_headers.clone(),
                proof.clone(),
                BlockTransactions::empty(),
                signing_key,
            )
            .expect("each proof should produce a correctly signed block");
            let block_id = block.header().id();
            assert_ne!(block_id, parent_id);
            assert!(block_ids.insert(block_id));
            assert_eq!(block.header().parent(), parent_id);
            assert_eq!(block.header().slot(), slot);
            assert_eq!(block.uncle_headers(), &uncle_headers);

            let received_block = Block::reconstruct(
                block.header().clone(),
                block.uncle_headers().clone(),
                block.transactions().clone(),
                *block.signature(),
            )
            .expect("ordinary block reconstruction should verify the signature and body root");
            assert_eq!(received_block.header().id(), block_id);

            ordinary_ledger
                .prepare_update::<_, Groth16LeaderProof, MainnetGasProfile>(
                    block_id,
                    parent_id,
                    slot,
                    block.header().leader_proof(),
                    &uncle_slots,
                    block.transactions_iter().cloned(),
                )
                .expect("ordinary ledger validation should accept every sibling from the parent")
                .verify_batch_proofs()
                .expect("ordinary ledger batch validation should succeed");

            if let Some((_, first)) = built_blocks.first() {
                assert_eq!(block.header().body_root(), first.header().body_root());
                assert_eq!(block.transactions(), first.transactions());
            }
            built_blocks.push((*sibling_index, block));
        }
    }

    /// Build an [`EpochState`] and a winning UTXO for `scan` tests.
    fn scan_test_fixtures() -> (
        lb_ledger::Config,
        DummyKms,
        Vec<UtxoWithKeyId>,
        UtxoTree,
        EpochState,
    ) {
        let config = test_config();
        let kms = DummyKms;
        let key_id = KeyId::from("0");
        let sk = UnsecuredZkKey::new(Fr::from(0u64));
        let pk = sk.to_public_key();

        let transfer = TransferOp::new(Inputs::empty(), Outputs::new([Note::new(1000u64, pk)]));
        let utxo = transfer.outputs.utxo_by_index(0, &transfer).unwrap();

        let aged_tree = UtxoTree::new().insert(utxo.id(), utxo).0;
        let latest_tree = UtxoTree::new().insert(utxo.id(), utxo).0;

        let total_stake = utxo.note.value;
        let (lottery_0, lottery_1) = config
            .lottery_constants()
            .compute_lottery_values(total_stake);
        let epoch_state = EpochState {
            epoch: 1.into(),
            nonce: Fr::from(999u64),
            blend_pow_difficulty: Fr::from(0u64),
            utxos: aged_tree,
            total_stake,
            lottery_0,
            lottery_1,
            active_declarations: Arc::new(lb_core::sdp::Declarations::default()),
        };

        (
            config,
            kms,
            vec![UtxoWithKeyId { utxo, key_id }],
            latest_tree,
            epoch_state,
        )
    }

    /// The scan only emits winning slots within `[start_slot, epoch_end)`: it
    /// skips past slots (so a mid-epoch start wastes no work) and never runs
    /// off the end of the epoch.
    #[tokio::test]
    async fn scan_emits_only_slots_in_range() {
        let (config, kms, eligible, _, epoch_state) = scan_test_fixtures();

        let epoch_starting_slot: u64 = config
            .epoch_config
            .starting_slot(&epoch_state.epoch, config.base_period_length())
            .into();
        let epoch_end = epoch_starting_slot + config.epoch_length();
        let start_slot = epoch_starting_slot + config.epoch_length() / 2;

        // Drive every per-slot future and keep the winning ones.
        let winners: Vec<_> =
            epoch_winning_slots_stream(&config, epoch_state, &eligible, kms, start_slot.into())
                .filter_map(|winning_slot| winning_slot)
                .collect()
                .await;

        for leader_private in &winners {
            let slot = leader_private.input().chain.slot_number;
            assert!(
                slot >= start_slot && slot < epoch_end,
                "winning slot {slot} outside [{start_slot}, {epoch_end})",
            );
        }
        // With the easy test lottery (f = 1) and a mid-epoch start, there is at
        // least one winning slot to emit.
        assert!(
            !winners.is_empty(),
            "expected at least one winning slot in range"
        );
    }

    /// Starting the scan at the epoch's end emits nothing.
    #[tokio::test]
    async fn scan_past_epoch_end_emits_nothing() {
        let (config, kms, eligible, _, epoch_state) = scan_test_fixtures();

        let epoch_starting_slot: u64 = config
            .epoch_config
            .starting_slot(&epoch_state.epoch, config.base_period_length())
            .into();
        let epoch_end = epoch_starting_slot + config.epoch_length();

        let winners: Vec<_> =
            epoch_winning_slots_stream(&config, epoch_state, &eligible, kms, epoch_end.into())
                .filter_map(|winning_slot| winning_slot)
                .collect()
                .await;

        assert!(
            winners.is_empty(),
            "no winning slots should be emitted when starting past the epoch end",
        );
    }

    /// A reward config with claiming disabled, standing in for a real
    /// deployment config in tests.
    fn disabled_reward_config() -> RewardPoWConfig {
        RewardPoWConfig {
            reward_pool_genesis: 1_000_000_000,
            epoch_reward_genesis: 1_000_000,
            minimum_difficulty: ModulusShift::new::<26>(),
            ema_smoothing_factor: 9,
            ema_smoothing_precision: core::num::NonZeroU64::new(10).unwrap(),
            target_claims_per_block: 100,
            rate_num: 0,
            rate_den: core::num::NonZeroU64::MIN,
            target_claim_per_block: core::num::NonZeroU64::MIN,
            pow_share: 0,
            share_den: core::num::NonZeroU64::MIN,
            slot_window: core::num::NonZeroU64::new(100).unwrap(),
        }
    }

    pub fn test_config() -> lb_ledger::Config {
        lb_ledger::Config {
            epoch_config: EpochConfig {
                epoch_stake_distribution_stabilization: NonZero::new(3u8).unwrap(),
                epoch_period_nonce_buffer: NonZero::new(3).unwrap(),
                epoch_period_nonce_stabilization: NonZero::new(4).unwrap(),
            },
            consensus_config: lb_cryptarchia_engine::Config::new(
                NonZero::new(5).unwrap(),
                NonNegativeRatio::new(1, 10.try_into().unwrap()),
                1f64.try_into().expect("1 > 0"),
                NonZero::new(12).unwrap(),
            ),
            sdp_config: SdpConfig {
                service_params: Arc::new(
                    [(
                        ServiceType::BlendNetwork,
                        ServiceParameters {
                            inactivity_period: 20.try_into().unwrap(),
                            epoch: 0.into(),
                        },
                    )]
                    .into(),
                ),
                service_rewards_params: ServiceRewardsParameters {
                    blend: RewardsParameters {
                        rounds_per_epoch: NonZero::new(10u64).unwrap(),
                        message_frequency_per_round: PositiveF64::try_from(1.0).unwrap(),
                        num_blend_layers: NonZero::new(3u64).unwrap(),
                        minimum_network_size: NonZero::new(1u64).unwrap(),
                        data_replication_factor: 0,
                        activity_threshold_sensitivity: 1,
                    },
                },
                min_stake: MinStake {
                    threshold: 1,
                    timestamp: 0,
                },
            },
            faucet_pk: None,
            pow_config: PoWConfig {
                blend: BlendPoWConfig {
                    base_difficulty: ModulusShift::new::<19>(),
                    damping_den_offset: 0,
                    damping_num: 1.try_into().unwrap(),
                    max_step: 1.try_into().unwrap(),
                    target_transactions_per_block: 1.try_into().unwrap(),
                },
                reward: disabled_reward_config(),
            },
        }
    }

    struct DummyKms;

    #[async_trait::async_trait]
    impl KmsAdapter<TestRuntimeServiceId> for DummyKms {
        type KeyId = KeyId;

        async fn check_winning_with_key(
            &self,
            _: Self::KeyId,
            utxo: &Utxo,
            leader_public: &LeaderPublic,
        ) -> Result<bool, overwatch::DynError> {
            let sk = ZkKey::new(Fr::from(0u64));
            Ok(check_winning(
                *utxo,
                *leader_public,
                &sk.to_public_key(),
                Fr::from(0u64),
            ))
        }

        async fn build_private_inputs_for_winning_utxo_and_slot(
            &self,
            _: Self::KeyId,
            utxo: &Utxo,
            epoch_state: &EpochState,
            public_inputs: LeaderPublic,
            latest_tree: &UtxoTree,
        ) -> Result<(LeaderPrivate, Ed25519Key), PrivateInputsError> {
            let aged_path = epoch_state
                .utxo_merkle_path(utxo)
                .ok_or(PrivateInputsError::AgedNoteNotFound)?;
            let latest_path = latest_tree
                .path(&utxo.id())
                .ok_or(PrivateInputsError::LatestNoteNotFound)?;
            // Generate a random one-time Ed25519 key for P_LEAD (as per PoL spec)
            let leader_signing_key = Ed25519Key::generate(&mut OsRng);
            let leader_pk = leader_signing_key.public_key();
            let leader_private = LeaderPrivate::new(
                public_inputs,
                *utxo,
                &aged_path,
                &latest_path,
                Fr::from(0u64),
                &leader_pk,
            );
            Ok((leader_private, leader_signing_key))
        }
    }

    struct DummyWallet;

    impl ServiceData for DummyWallet {
        type Settings = WalletServiceSettings;
        type State = NoState<Self::Settings>;
        type StateOperator = NoOperator<Self::State>;
        type Message = WalletMsg;
    }

    impl WalletServiceData for DummyWallet {
        type Kms = ();
        type Cryptarchia = ();
        type Tx = ();
    }

    impl DummyWallet {
        fn spawn() -> WalletApi<Self, TestRuntimeServiceId> {
            let (msg_sender, mut msg_receiver) = mpsc::channel(10);

            tokio::spawn(async move {
                while let Some(msg) = msg_receiver.recv().await {
                    if let WalletMsg::GenerateNewVoucherSecret { resp_tx } = msg {
                        drop(resp_tx.send(Ok(dummy_voucher_cm())));
                    }
                }
            });

            WalletApi::<Self, TestRuntimeServiceId>::new(OutboundRelay::new(msg_sender))
        }

        fn spawn_with_distinct_vouchers()
        -> (WalletApi<Self, TestRuntimeServiceId>, Arc<AtomicUsize>) {
            let (msg_sender, mut msg_receiver) = mpsc::channel(10);
            let voucher_count = Arc::new(AtomicUsize::new(0));
            let voucher_count_for_task = Arc::clone(&voucher_count);

            tokio::spawn(async move {
                let mut next_secret = 100u64;
                while let Some(msg) = msg_receiver.recv().await {
                    if let WalletMsg::GenerateNewVoucherSecret { resp_tx } = msg {
                        let secret = VoucherSecret(Fr::from(next_secret));
                        next_secret += 1;
                        voucher_count_for_task.fetch_add(1, Ordering::SeqCst);
                        drop(resp_tx.send(Ok(VoucherCm::from_secret(secret))));
                    }
                }
            });

            (
                WalletApi::<Self, TestRuntimeServiceId>::new(OutboundRelay::new(msg_sender)),
                voucher_count,
            )
        }
    }

    const DUMMY_VOUCHER_CM_BYTES: [u8; 32] = [99u8; 32];

    fn dummy_voucher_cm() -> VoucherCm {
        fr_from_bytes_unchecked(&DUMMY_VOUCHER_CM_BYTES).into()
    }

    #[derive(Debug)]
    struct TestRuntimeServiceId;

    impl AsServiceId<DummyWallet> for TestRuntimeServiceId {
        const SERVICE_ID: Self = Self;
    }

    impl Display for TestRuntimeServiceId {
        fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
            write!(f, "TestRuntimeServiceId")
        }
    }
}
