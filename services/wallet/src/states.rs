#[cfg(test)]
use std::time::{Duration as ReplayDuration, Instant};
use std::{
    borrow::Borrow,
    collections::{HashMap, HashSet},
};

use lb_core::{
    header::HeaderId,
    mantle::{
        Note, Value,
        note_set::NoteSetIndex,
        notes::{InputNullifiers, NoteNullifier},
        ops::transfer::TransferOp,
        transactions::{
            MantleTxBuilder,
            builder::{PriorityFee, ZkTransferOpening},
            tx_list::ops::OpsContext,
        },
    },
};
use lb_groth16::Fr;
use lb_key_management_system_service::keys::{LeadershipPublicKeys, PK0, ZkPublicKey};
use lb_log_targets::wallet;
use lb_storage_service::api::StorageApi;
use lb_wallet::{KnownNote, KnownNotes, NoteSetSpend, WalletBlock, WalletError, WalletState};
use overwatch::services::state::StateUpdater;
use serde::{Deserialize, Serialize};
use tracing::{debug, warn};

use crate::{KeyId, WalletServiceError, WalletServiceSettings};

pub type Wallet = lb_wallet::Wallet<KeyId>;

/// Notes handed out as funding for transactions that have been built but not
/// yet observed on chain. Ephemeral: rebuilt empty on restart; entries are
/// dropped once the note is observed spent in a block, or expire after enough
/// LIB progress.
///
/// Unlike claim reservations, which must hold until the claim is finalised,
/// funded notes are released as soon as they are observed spent in a tip
/// block; eviction only covers transactions that never land. Releasing too
/// early merely reintroduces a conflicting tx, the same failure mode as
/// having no reservation at all.
#[derive(Clone, Debug, Default)]
struct PendingNotes {
    // The flag records that a funding request has claimed this reservation.
    notes: HashMap<(NoteSetIndex, NoteNullifier), ([u8; 32], u64, bool)>,
}

impl PendingNotes {
    fn reserve(
        &mut self,
        note_set: NoteSetIndex,
        nullifiers: impl IntoIterator<Item = NoteNullifier>,
        owner: [u8; 32],
    ) {
        for nullifier in nullifiers {
            debug!(
                target: wallet::SERVICE,
                ?nullifier,
                "Reserved pending note"
            );
            self.notes
                .entry((note_set, nullifier))
                .or_insert((owner, 0, false));
        }
    }

    fn release(
        &mut self,
        note_set: NoteSetIndex,
        nullifiers: impl IntoIterator<Item = NoteNullifier>,
        owner: [u8; 32],
    ) {
        for nullifier in nullifiers {
            let key = (note_set, nullifier);
            if self
                .notes
                .get(&key)
                .is_some_and(|(existing, _, _)| *existing == owner)
                && self.notes.remove(&key).is_some()
            {
                debug!(
                    target: wallet::SERVICE,
                    ?nullifier,
                    "Released pending note reservation"
                );
            }
        }
    }

    fn nullifiers(&self, note_set: NoteSetIndex) -> HashSet<NoteNullifier> {
        self.notes
            .keys()
            .filter_map(|(set, nullifier)| (*set == note_set).then_some(*nullifier))
            .collect()
    }

    /// Evict reservations whose LIB-progress age reached the configured limit.
    ///
    /// Each LIB update adds `new_immutable_blocks_count` to every reservation's
    /// counter. A reservation expires once that counter reaches
    /// `max_immutable_blocks_since_reservation`.
    fn evict_expired(
        &mut self,
        new_immutable_blocks_count: u64,
        max_immutable_blocks_since_reservation: u64,
    ) {
        if new_immutable_blocks_count == 0 {
            return;
        }

        self.notes
            .retain(|nullifier, (_, immutable_blocks_since_reservation, _)| {
                *immutable_blocks_since_reservation =
                    immutable_blocks_since_reservation.saturating_add(new_immutable_blocks_count);

                let expired =
                    *immutable_blocks_since_reservation >= max_immutable_blocks_since_reservation;

                if expired {
                    debug!(
                        target: wallet::SERVICE,
                        ?nullifier,
                        immutable_blocks_since_reservation,
                        max_immutable_blocks_since_reservation,
                        "Removing pending note reservation after LIB progress"
                    );
                }

                !expired
            });
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecoveryState {
    /// Revision of the confirmed recovery checkpoint; namespace coverage and
    /// explicit withdrawals are recorded independently of asynchronous state.
    pub recovery_generation: u64,
    /// [`WalletState`] at the last known LIB.
    /// `None` until the first recovery state update.
    lib_wallet_state: Option<(HeaderId, WalletState)>,
    // openings the wallet adopts once their commitments are appended
    known_notes: KnownNotes,
    leadership_lib_wallet_state: Option<(HeaderId, WalletState)>,
    leadership_known_notes: KnownNotes,
    configured_leadership_keys: HashMap<KeyId, LeadershipPublicKeys>,
    // the reward keys `0..next_reward_key_index` are derived again at start
    next_reward_key_index: u64,
}

impl RecoveryState {
    pub fn restore_spending_records(
        &mut self,
        known_notes: KnownNotes,
        next_reward_key_index: u64,
    ) {
        self.known_notes = known_notes;
        self.next_reward_key_index = next_reward_key_index;
    }

    pub fn restore_leadership_records(&mut self, notes: &KnownNotes, historical: bool) {
        // Only pending operator requirements demand reconstruction. A newly
        // registered opening alone does not imply an old commitment.
        if historical {
            self.lib_wallet_state = None;
            self.leadership_lib_wallet_state = None;
        }
        for (id, note) in notes.iter() {
            self.leadership_known_notes.insert_mut(*id, *note);
        }
    }

    pub fn take_lib_wallet_state(
        &mut self,
        settings: &WalletServiceSettings,
    ) -> Option<(HeaderId, WalletState)> {
        if self
            .leadership_lib_wallet_state
            .as_ref()
            .map(|(lib, _)| lib)
            != self.lib_wallet_state.as_ref().map(|(lib, _)| lib)
            || self.leadership_lib_wallet_state.is_none()
            || self.configured_leadership_keys != settings.leadership_keys
        {
            self.lib_wallet_state = None;
            self.leadership_lib_wallet_state = None;
            return None;
        }
        self.lib_wallet_state.take()
    }

    pub const fn leadership_known_notes(&self) -> &KnownNotes {
        &self.leadership_known_notes
    }

    pub fn update_leadership_note(&mut self, note: KnownNote) {
        self.leadership_known_notes.insert_mut(note.id(), note);
    }
}

impl overwatch::services::state::ServiceState for RecoveryState {
    type Settings = WalletServiceSettings;
    type Error = WalletServiceError;

    fn from_settings(_settings: &Self::Settings) -> Result<Self, Self::Error> {
        Ok(Self {
            recovery_generation: 0,
            lib_wallet_state: None,
            known_notes: KnownNotes::new_sync(),
            leadership_lib_wallet_state: None,
            leadership_known_notes: KnownNotes::new_sync(),
            configured_leadership_keys: HashMap::new(),
            next_reward_key_index: 0,
        })
    }
}

/// Provides operations on the states that must be synced to [`RecoveryState`].
#[derive(Clone)]
pub struct ServiceState<'u> {
    pub recovery_generation: u64,
    next_reward_key_index: u64,
    wallet: Wallet,
    leadership_wallet: Wallet,
    leadership_keys: HashMap<LeadershipPublicKeys, KeyId>,
    configured_leadership_keys: HashMap<KeyId, LeadershipPublicKeys>,
    lib: HeaderId,
    updater: &'u StateUpdater<Option<RecoveryState>>,
    pending_notes: PendingNotes,
    pending_note_expiry_blocks: u64,
    fee_margin_percent: u64,
    persist_enabled: bool,
    #[cfg(test)]
    last_historical_replay: Option<Instant>,
}

impl<'u> ServiceState<'u> {
    // Restores the wallet persisted at its LIB.
    pub fn from_lib_wallet_state(
        state: RecoveryState,
        lib: HeaderId,
        mut wallet_state: WalletState,
        settings: &WalletServiceSettings,
        updater: &'u StateUpdater<Option<RecoveryState>>,
    ) -> Self {
        let generation = state.recovery_generation;
        let RecoveryState {
            mut known_notes,
            next_reward_key_index,
            leadership_lib_wallet_state,
            leadership_known_notes,
            ..
        } = state;
        known_notes = known_notes
            .iter()
            .filter(|(_, known)| known.note.public_key != *PK0)
            .map(|(id, known)| (*id, *known))
            .collect();
        wallet_state.notes = wallet_state
            .notes
            .iter()
            .filter(|(_, known)| known.note.public_key != *PK0)
            .map(|(commitment, known)| (*commitment, known.clone()))
            .collect();
        let (_, leadership_state) = leadership_lib_wallet_state
            .filter(|(at, _)| *at == lib)
            .expect("leadership recovery state must match the wallet LIB");
        let leadership_wallet =
            Wallet::from_lib_wallet_state([], leadership_known_notes, lib, leadership_state);
        let wallet =
            Wallet::from_lib_wallet_state(known_keys(settings), known_notes, lib, wallet_state);
        Self::with_wallet(
            wallet,
            leadership_wallet,
            lib,
            next_reward_key_index,
            settings,
            updater,
        )
        .at_generation(generation)
    }

    // A fresh wallet follows the note sets from the genesis block.
    #[expect(
        clippy::too_many_arguments,
        reason = "Genesis initializes both tracked note collections"
    )]
    pub fn from_genesis(
        state: RecoveryState,
        settings: &WalletServiceSettings,
        genesis: HeaderId,
        genesis_notes: &[Note],
        genesis_service_notes: &[Note],
        genesis_known_notes: impl IntoIterator<Item = KnownNote>,
        genesis_leadership_notes: impl IntoIterator<Item = KnownNote>,
        updater: &'u StateUpdater<Option<RecoveryState>>,
    ) -> Result<Self, WalletError> {
        let generation = state.recovery_generation;
        let RecoveryState {
            mut known_notes,
            mut leadership_known_notes,
            next_reward_key_index,
            ..
        } = state;
        for known in genesis_known_notes {
            known_notes.insert_mut(known.id(), known);
        }
        known_notes = known_notes
            .iter()
            .filter(|(_, known)| known.note.public_key != *PK0)
            .map(|(id, known)| (*id, *known))
            .collect();
        for known in genesis_leadership_notes {
            leadership_known_notes.insert_mut(known.id(), known);
        }
        let leadership_wallet = Wallet::from_genesis(
            [],
            leadership_known_notes,
            genesis,
            genesis_notes,
            genesis_service_notes,
        )?;
        let wallet = Wallet::from_genesis(
            known_keys(settings),
            known_notes,
            genesis,
            genesis_notes,
            genesis_service_notes,
        )?;
        Ok(Self::with_wallet(
            wallet,
            leadership_wallet,
            genesis,
            next_reward_key_index,
            settings,
            updater,
        )
        .at_generation(generation))
    }

    fn with_wallet(
        wallet: Wallet,
        leadership_wallet: Wallet,
        lib: HeaderId,
        next_reward_key_index: u64,
        settings: &WalletServiceSettings,
        updater: &'u StateUpdater<Option<RecoveryState>>,
    ) -> Self {
        Self {
            recovery_generation: 0,
            next_reward_key_index,
            wallet,
            leadership_wallet,
            leadership_keys: settings
                .leadership_keys
                .iter()
                .map(|(id, public)| (*public, id.clone()))
                .collect(),
            configured_leadership_keys: settings.leadership_keys.clone(),
            lib,
            updater,
            pending_notes: PendingNotes::default(),
            pending_note_expiry_blocks: settings.pending_note_expiry_blocks,
            fee_margin_percent: settings.fee_margin_percent,
            persist_enabled: true,
            #[cfg(test)]
            last_historical_replay: None,
        }
    }

    const fn at_generation(mut self, generation: u64) -> Self {
        self.recovery_generation = generation;
        self
    }

    #[cfg(test)]
    pub fn replay_copy(&self) -> Self {
        let mut state = self.clone();
        state.persist_enabled = false;
        state
    }

    /// Admit at most one historical reconstruction per interval.
    #[cfg(test)]
    pub fn admit_historical_replay(&mut self) -> Result<(), WalletServiceError> {
        // A replay can occupy the wallet for up to 60 seconds. Leave a gap
        // after even a timed-out attempt for routine wallet messages.
        const INTERVAL: ReplayDuration = ReplayDuration::from_secs(90);
        let now = Instant::now();
        if self
            .last_historical_replay
            .is_some_and(|last| now.duration_since(last) < INTERVAL)
        {
            return Err(WalletServiceError::HistoricalReplayRateLimited);
        }
        self.last_historical_replay = Some(now);
        Ok(())
    }

    /// Restore finalized openings pruned from both discovery maps before
    /// rebuilding the wallets from genesis.
    #[cfg(test)]
    pub async fn restore_replay_openings<Tx>(
        &mut self,
        storage: &StorageApi<Tx>,
    ) -> Result<(), WalletServiceError> {
        let notes = storage
            .load_wallet_known_notes()
            .await
            .map_err(WalletServiceError::PersistedState)?;
        let leadership = storage
            .load_wallet_leadership_notes()
            .await
            .map_err(WalletServiceError::PersistedState)?;
        self.add_known_notes(notes.values().copied());
        self.add_leadership_notes(leadership.values().copied());
        Ok(())
    }

    #[cfg(test)]
    pub fn rewind_to_genesis(
        &mut self,
        genesis: HeaderId,
        notes: &[Note],
        service_notes: &[Note],
    ) -> Result<(), WalletError> {
        let keys = self
            .wallet
            .known_keys()
            .iter()
            .map(|(pk, id)| (*pk, id.clone()))
            .collect::<Vec<_>>();
        self.wallet = Wallet::from_genesis(
            keys,
            self.wallet.known_notes().clone(),
            genesis,
            notes,
            service_notes,
        )?;
        self.leadership_wallet = Wallet::from_genesis(
            [],
            self.leadership_wallet.known_notes().clone(),
            genesis,
            notes,
            service_notes,
        )?;
        self.lib = genesis;
        Ok(())
    }

    pub fn persist_after_replay(&mut self) {
        self.persist_enabled = true;
        self.update_state();
    }

    // the priority fee percentage of a funding that names none
    pub const fn fee_margin_percent(&self) -> u64 {
        self.fee_margin_percent
    }

    pub const fn lib(&self) -> HeaderId {
        self.lib
    }

    // the number of reward keys derived so far
    pub const fn reward_key_count(&self) -> u64 {
        self.next_reward_key_index
    }

    pub async fn store_reward_key_count<Tx>(
        &self,
        storage: &StorageApi<Tx>,
    ) -> Result<(), WalletServiceError> {
        storage
            .store_wallet_reward_key_count(self.next_reward_key_index)
            .await
            .map_err(WalletServiceError::PersistedState)
    }

    pub const fn next_reward_key_index(&mut self) -> u64 {
        let index = self.next_reward_key_index;
        self.next_reward_key_index = index.saturating_add(1);
        index
    }

    pub fn add_known_key(&mut self, pk: ZkPublicKey, key_id: KeyId) {
        self.wallet.add_known_key(pk, key_id);
    }

    /// Persist spendable openings before exposing them to callers.
    pub async fn store_known_notes<Tx>(
        &mut self,
        storage: &StorageApi<Tx>,
        notes: &[KnownNote],
    ) -> Result<(), WalletServiceError> {
        let leadership = notes
            .iter()
            .copied()
            .filter(|known| {
                self.leadership_keys.contains_key(&LeadershipPublicKeys {
                    nullifier_public_key: known.note.public_key,
                    staking_public_key: known.note.stake_public_key,
                })
            })
            .collect::<Vec<_>>();
        storage
            .store_wallet_openings(notes, &leadership)
            .await
            .map_err(WalletServiceError::PersistedState)?;
        self.add_known_notes(notes.iter().copied());
        Ok(())
    }

    pub async fn store_leadership_notes<Tx>(
        &mut self,
        storage: &StorageApi<Tx>,
        notes: &[KnownNote],
    ) -> Result<(), WalletServiceError> {
        storage
            .store_wallet_leadership_notes(notes)
            .await
            .map_err(WalletServiceError::PersistedState)?;
        self.add_leadership_notes(notes.iter().copied());
        Ok(())
    }

    pub fn add_leadership_key(&mut self, key_id: KeyId, public: LeadershipPublicKeys) {
        self.leadership_keys.insert(public, key_id);
    }

    pub fn add_known_notes(&mut self, notes: impl IntoIterator<Item = KnownNote>) {
        let notes: Vec<_> = notes.into_iter().collect();
        self.leadership_wallet
            .add_known_notes(notes.iter().copied().filter(|known| {
                self.leadership_keys.contains_key(&LeadershipPublicKeys {
                    nullifier_public_key: known.note.public_key,
                    staking_public_key: known.note.stake_public_key,
                })
            }));
        self.wallet.add_known_notes(
            notes
                .into_iter()
                .filter(|known| known.note.public_key != *PK0),
        );
    }

    pub fn add_leadership_notes(&mut self, notes: impl IntoIterator<Item = KnownNote>) {
        self.leadership_wallet.add_known_notes(notes);
        self.update_state();
    }

    pub const fn leadership_wallet(&self) -> &Wallet {
        &self.leadership_wallet
    }

    pub const fn leadership_keys(&self) -> &HashMap<LeadershipPublicKeys, KeyId> {
        &self.leadership_keys
    }

    pub fn apply_block(&mut self, block: &WalletBlock) -> Result<(), WalletError> {
        let mut wallet = self.wallet.clone();
        let mut leadership_wallet = self.leadership_wallet.clone();
        wallet.apply_block(block)?;
        leadership_wallet.apply_block(block)?;
        self.wallet = wallet;
        self.leadership_wallet = leadership_wallet;
        // Note reservations are released after `pending_note_expiry_blocks`
        // (see `evict_expired`), not on spent. The recovery state, the LIB's
        // wallet state, stays as it is.
        Ok(())
    }

    pub fn advance_lib(&mut self, new_lib: HeaderId, new_immutable_blocks_count: u64) {
        // The wallet must already hold a `WalletState` for `new_lib`, otherwise
        // `update_state` cannot persist the recovery state. The service
        // backfills up to `new_lib` before calling us; if it still
        // isn't present (e.g. the block is not yet in storage) we skip
        // advancing rather than panicking, keeping `self.lib` pointing
        // at a block whose state exists. A later block application or
        // LIB update advances the LIB once the gap is filled.
        if !self.wallet.has_processed_block(new_lib) {
            warn!(
                target: wallet::SERVICE,
                ?new_lib,
                current_lib = ?self.lib,
                "Skipping LIB advance: wallet has not applied the new LIB block yet"
            );
            return;
        }

        self.lib = new_lib;
        self.wallet.prune_states(new_lib);
        // The notes final at the new LIB are never adopted again.
        self.wallet
            .forget_final_known_notes(new_lib)
            .expect("WalletState at LIB must exist");
        self.leadership_wallet.prune_states(new_lib);
        self.pending_notes
            .evict_expired(new_immutable_blocks_count, self.pending_note_expiry_blocks);
        self.update_state();
    }

    pub const fn wallet(&self) -> &Wallet {
        &self.wallet
    }

    /// Fund `tx_builder` from the wallet's notes at `tip`, excluding notes
    /// already reserved for in-flight transactions. `priority_fee` reserves
    /// its percentage of the final mandatory fee, including both execution and
    /// storage cost, within its maximum fee. Only the unused reserve becomes an
    /// effective priority tip, and the percentage is not capped at 100. A
    /// funding that fails releases its own reservations, such as a split.
    /// A builder whose funding is already reserved cannot be funded again.
    #[expect(
        clippy::too_many_arguments,
        reason = "thin passthrough to `Wallet::fund_tx` plus the reservations"
    )]
    pub fn fund_tx(
        &mut self,
        tip: HeaderId,
        tx_builder: &MantleTxBuilder,
        change_pk: ZkPublicKey,
        change_nonce: Fr,
        funding_pks: impl IntoIterator<Item = impl Borrow<ZkPublicKey>>,
        context: &OpsContext,
        priority_fee: impl Into<PriorityFee>,
    ) -> Result<MantleTxBuilder, WalletError> {
        if self.has_reserved_funding(tx_builder) {
            return Err(WalletError::InputReserved);
        }
        if self
            .pending_notes
            .notes
            .iter()
            .any(|(&(set, nullifier), (owner, _, _))| {
                !tx_builder.owns_reservation(*owner)
                    && tx_builder
                        .nullifiers_consumed_in(set)
                        .any(|input| input == nullifier)
            })
        {
            self.release_transaction_notes(tx_builder);
            return Err(WalletError::InputReserved);
        }
        let funded = self.wallet.fund_tx(
            tip,
            tx_builder,
            change_pk,
            change_nonce,
            funding_pks,
            context,
            &self.pending_notes.nullifiers(NoteSetIndex::Ledger),
            priority_fee,
        );
        if funded.is_err() {
            self.release_transaction_notes(tx_builder);
        }
        funded
    }

    // A Transfer giving `owner` a note of exactly `value` and the rest back to
    // `owner`, from the wallet's notes at `tip` that neither `tx_builder` nor
    // an in flight transaction spends.
    #[expect(
        clippy::too_many_arguments,
        reason = "thin passthrough to `WalletState::exact_note_split` plus the reservations"
    )]
    pub fn exact_note_split(
        &self,
        tip: HeaderId,
        tx_builder: &MantleTxBuilder,
        value: Value,
        owner: ZkPublicKey,
        nonce: Fr,
        change_nonce: Fr,
        funding_pks: impl IntoIterator<Item = impl Borrow<ZkPublicKey>>,
    ) -> Result<(TransferOp, ZkTransferOpening), WalletError> {
        let excluded = tx_builder
            .nullifiers_consumed_in(NoteSetIndex::Ledger)
            .chain(self.pending_notes.nullifiers(NoteSetIndex::Ledger))
            .collect::<HashSet<_>>();
        self.wallet.spend_state(tip)?.exact_note_split(
            value,
            owner,
            nonce,
            owner,
            change_nonce,
            funding_pks,
            &excluded,
        )
    }

    pub fn note_set_spend(
        &self,
        tip: HeaderId,
        spend: NoteSetSpend,
    ) -> Result<(InputNullifiers, ZkTransferOpening, Fr), WalletError> {
        let note_set = spend.note_set;
        self.wallet.spend_state(tip)?.note_set_spend(
            spend,
            self.wallet.known_notes(),
            &self.pending_notes.nullifiers(note_set),
        )
    }

    pub fn is_pending_note(&self, note_set: NoteSetIndex, nullifier: &NoteNullifier) -> bool {
        self.pending_notes
            .notes
            .contains_key(&(note_set, *nullifier))
    }

    pub fn has_reserved_funding(&self, builder: &MantleTxBuilder) -> bool {
        builder.funding_inputs().iter().any(|input| {
            self.pending_notes
                .notes
                .contains_key(&(NoteSetIndex::Ledger, input.nullifier))
        }) || self
            .pending_notes
            .notes
            .iter()
            .any(|(&(set, nullifier), (_, _, claimed))| {
                *claimed
                    && builder
                        .nullifiers_consumed_in(set)
                        .any(|input| input == nullifier)
            })
    }

    pub fn release_transaction_notes(&mut self, builder: &MantleTxBuilder) {
        let entries = self
            .pending_notes
            .notes
            .iter()
            .filter_map(|(&(set, nullifier), (existing, _, _))| {
                (builder.owns_reservation(*existing)
                    && builder
                        .nullifiers_consumed_in(set)
                        .any(|input| input == nullifier))
                .then_some((set, nullifier, *existing))
            })
            .collect::<Vec<_>>();
        for (set, nullifier, owner) in entries {
            self.pending_notes.release(set, [nullifier], owner);
        }
    }

    pub fn reserve_transaction_notes(&mut self, builder: &MantleTxBuilder, owner: [u8; 32]) {
        for (set, nullifier) in builder.note_set_nullifiers_consumed() {
            self.pending_notes
                .notes
                .entry((set, nullifier))
                .and_modify(|(existing, _, claimed)| {
                    if builder.owns_reservation(*existing) {
                        *existing = owner;
                        *claimed = true;
                    }
                })
                .or_insert((owner, 0, true));
        }
    }

    pub fn reserve_pending_notes(
        &mut self,
        nullifiers: impl IntoIterator<Item = NoteNullifier>,
        owner: [u8; 32],
    ) {
        self.pending_notes
            .reserve(NoteSetIndex::Ledger, nullifiers, owner);
    }

    pub fn release_pending_notes(
        &mut self,
        nullifiers: impl IntoIterator<Item = NoteNullifier>,
        owner: Option<[u8; 32]>,
    ) {
        if let Some(owner) = owner {
            self.pending_notes
                .release(NoteSetIndex::Ledger, nullifiers, owner);
        }
    }

    pub fn reserve_pending_notes_in(
        &mut self,
        note_set: NoteSetIndex,
        nullifiers: impl IntoIterator<Item = NoteNullifier>,
        owner: [u8; 32],
    ) {
        self.pending_notes.reserve(note_set, nullifiers, owner);
    }

    pub fn release_pending_notes_in(
        &mut self,
        note_set: NoteSetIndex,
        nullifiers: impl IntoIterator<Item = NoteNullifier>,
        owner: [u8; 32],
    ) {
        self.pending_notes.release(note_set, nullifiers, owner);
    }

    fn update_state(&self) {
        if !self.persist_enabled {
            return;
        }
        self.updater.update(Some(self.snapshot()));
    }

    pub fn snapshot(&self) -> RecoveryState {
        let lib_wallet_state = self
            .wallet()
            .wallet_state_at(self.lib)
            .expect("WalletState at LIB must exist");
        RecoveryState {
            recovery_generation: self.recovery_generation,
            lib_wallet_state: Some((self.lib, lib_wallet_state)),
            known_notes: self.wallet.known_notes().clone(),
            leadership_lib_wallet_state: Some((
                self.lib,
                self.leadership_wallet
                    .wallet_state_at(self.lib)
                    .expect("Leadership state at LIB must exist"),
            )),
            leadership_known_notes: self.leadership_wallet.known_notes().clone(),
            configured_leadership_keys: self.configured_leadership_keys.clone(),
            next_reward_key_index: self.next_reward_key_index,
        }
    }
}

fn known_keys(settings: &WalletServiceSettings) -> impl Iterator<Item = (ZkPublicKey, KeyId)> {
    settings
        .known_keys
        .iter()
        .filter(|(_, pk)| **pk != *PK0)
        .map(|(key_id, pk)| (*pk, key_id.clone()))
}

#[cfg(test)]
mod tests {
    use core::{num::NonZero, time::Duration};

    use lb_core::{era::ForkDigest, mantle::notes::NoteCommitment};
    use lb_groth16::Field as _;

    use super::*;
    use crate::{DEFAULT_FEE_MARGIN_PERCENT, ServiceEraSchedule, tests::GAS_PROFILE};

    const EXPIRY_BLOCKS: u64 = 4;

    /// A schedule of one era, with epochs of 100 slots and a `k` of 5.
    fn era_schedule() -> ServiceEraSchedule {
        use lb_time::era::{EraEntriesAfterGenesis, EraSchedule, EraTiming};
        use time::OffsetDateTime;

        use crate::WalletEraSettings;

        let timing = EraTiming {
            slot_duration: Duration::from_secs(1),
            epoch_length: NonZero::new(100).unwrap(),
        };
        EraSchedule::new(
            OffsetDateTime::UNIX_EPOCH,
            timing,
            EraEntriesAfterGenesis::empty(),
        )
        .unwrap()
        .map(|_| WalletEraSettings {
            epoch_length: timing.epoch_length,
            security_param: NonZero::new(5).unwrap(),
            fork_digest: ForkDigest::new([0x11; 32]),
            gas_profile: GAS_PROFILE,
        })
    }

    #[test]
    fn recovery_state_keeps_the_known_notes_and_the_reward_key_index() {
        use lb_binary_codec::bincode::{DeserializeOp as _, SerializeOp as _};
        let note = KnownNote {
            note: Note::new(10, Fr::ONE, ZkPublicKey::new(Fr::from(2u64))),
            note_set: NoteSetIndex::Ledger,
            nullifier: NoteNullifier(Fr::from(3u64)),
        };
        let state = RecoveryState {
            recovery_generation: 0,
            lib_wallet_state: None,
            known_notes: KnownNotes::from_iter([(note.id(), note)]),
            leadership_lib_wallet_state: None,
            leadership_known_notes: KnownNotes::new_sync(),
            configured_leadership_keys: HashMap::new(),
            next_reward_key_index: 4,
        };
        let decoded = RecoveryState::from_bytes(&state.to_bytes().unwrap()).unwrap();
        assert_eq!(decoded.known_notes, state.known_notes);
        assert_eq!(decoded.next_reward_key_index, 4);
    }

    #[test]
    #[expect(
        clippy::too_many_lines,
        reason = "The lifecycle covers both forks and recovery"
    )]
    fn leadership_notes_follow_spends_forks_and_recovery_without_becoming_spendable() {
        use std::sync::Arc;

        use lb_binary_codec::bincode::{DeserializeOp as _, SerializeOp as _};
        use lb_core::mantle::note_set::NoteSetUpdate;
        use lb_key_management_system_service::keys::{LeadershipKey, secured_key::SecuredKey as _};
        use lb_services_utils::overwatch::RecoveryData;
        use overwatch::services::state::ServiceState as _;

        let key = LeadershipKey::new(Fr::from(7u64), Fr::from(9u64));
        let public = key.as_public_key();
        let note = Note {
            stake_public_key: public.staking_public_key,
            ..Note::new(10, Fr::ONE, public.nullifier_public_key)
        };
        let known = KnownNote {
            note,
            note_set: NoteSetIndex::Ledger,
            nullifier: NoteNullifier(Fr::from(3u64)),
        };
        let service_known = KnownNote {
            note_set: NoteSetIndex::Sdp,
            ..known
        };
        let settings = WalletServiceSettings {
            historical_recovery: Vec::new(),
            historical_recovery_withdrawals: Vec::new(),
            known_keys: HashMap::new(),
            leadership_keys: HashMap::from([("leader".into(), public)]),
            voucher_master_key_id: "voucher-master".into(),
            recovery_data: RecoveryData::default(),
            pending_note_expiry_blocks: 10,
            fee_margin_percent: DEFAULT_FEE_MARGIN_PERCENT,
            era_schedule: era_schedule(),
        };
        let genesis = HeaderId::from([0; 32]);
        let (sender, receiver) = tokio::sync::watch::channel(None);
        let updater = StateUpdater::new(Arc::new(sender));
        let mut state = ServiceState::from_genesis(
            RecoveryState::from_settings(&settings).unwrap(),
            &settings,
            genesis,
            &[note],
            &[note],
            [],
            [known, service_known],
            &updater,
        )
        .unwrap();
        assert!(
            state
                .wallet()
                .balance(genesis, note.public_key)
                .unwrap()
                .is_none()
        );
        assert_eq!(
            state
                .leadership_wallet()
                .wallet_state_at(genesis)
                .unwrap()
                .aged_notes()
                .unwrap()
                .len(),
            2
        );
        let block = |id, nullifiers| WalletBlock {
            id: HeaderId::from([id; 32]),
            parent: genesis,
            epoch: 0.into(),
            updates: vec![NoteSetUpdate {
                note_set: NoteSetIndex::Ledger,
                nullifiers,
                commitments: vec![],
            }],
            revealed: vec![],
            withdrawals: vec![],
        };
        let spent = block(1, vec![known.nullifier]);
        let fork = block(2, vec![]);
        state.apply_block(&spent).unwrap();
        state.apply_block(&fork).unwrap();
        assert_eq!(
            state
                .leadership_wallet()
                .wallet_state_at(spent.id)
                .unwrap()
                .aged_notes()
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            state
                .leadership_wallet()
                .wallet_state_at(fork.id)
                .unwrap()
                .aged_notes()
                .unwrap()
                .len(),
            2
        );
        state.advance_lib(fork.id, 1);
        let saved = receiver.borrow().clone().unwrap();
        let later_note = KnownNote {
            note: Note {
                nonce: Fr::from(42u64),
                ..note
            },
            ..known
        };
        let mut replay_after_opening = saved.clone();
        replay_after_opening.restore_leadership_records(
            &KnownNotes::from_iter([(later_note.id(), later_note)]),
            true,
        );
        assert!(
            replay_after_opening
                .take_lib_wallet_state(&settings)
                .is_none()
        );
        assert!(
            replay_after_opening
                .leadership_known_notes()
                .contains_key(&later_note.id())
        );
        let mut changed_settings = settings.clone();
        changed_settings.leadership_keys = HashMap::from([("replacement".into(), public)]);
        let mut replay = saved.clone();
        assert!(replay.take_lib_wallet_state(&changed_settings).is_none());
        assert!(
            replay
                .leadership_known_notes()
                .contains_key(&lb_wallet::NoteId::new(
                    NoteSetIndex::Ledger,
                    note.commitment()
                ))
        );
        let restored = RecoveryState::from_bytes(&saved.to_bytes().unwrap()).unwrap();
        let mut restored = restored;
        let (lib, wallet_state) = restored.take_lib_wallet_state(&settings).unwrap();
        let restored =
            ServiceState::from_lib_wallet_state(restored, lib, wallet_state, &settings, &updater);
        assert_eq!(
            restored
                .leadership_wallet()
                .wallet_state_at(lib)
                .unwrap()
                .aged_notes()
                .unwrap()
                .len(),
            2
        );
        let restored_state = restored.leadership_wallet().wallet_state_at(lib).unwrap();
        for (owned, path) in restored_state.aged_notes().unwrap() {
            assert_eq!(owned.nullifier, known.nullifier);
            assert!(path.verify::<lb_core::crypto::ZkHasher>(
                note.commitment().0,
                restored_state.root_of(owned.note_set)
            ));
        }
        assert!(
            restored
                .wallet()
                .balance(lib, note.public_key)
                .unwrap()
                .is_none()
        );
        assert_eq!(restored.leadership_keys().get(&public).unwrap(), "leader");
    }

    #[test]
    fn newly_created_leadership_note_ages_before_selection() {
        use std::sync::Arc;

        use lb_core::mantle::note_set::NoteSetUpdate;
        use lb_key_management_system_service::keys::{LeadershipKey, secured_key::SecuredKey as _};
        use lb_services_utils::overwatch::RecoveryData;
        use overwatch::services::state::ServiceState as _;

        let public = LeadershipKey::new(7u64.into(), 9u64.into()).as_public_key();
        let note = Note {
            stake_public_key: public.staking_public_key,
            ..Note::new(10, Fr::ONE, public.nullifier_public_key)
        };
        let known = KnownNote {
            note,
            note_set: NoteSetIndex::Ledger,
            nullifier: NoteNullifier(Fr::from(3u64)),
        };
        let settings = WalletServiceSettings {
            historical_recovery: Vec::new(),
            historical_recovery_withdrawals: Vec::new(),
            known_keys: HashMap::new(),
            leadership_keys: HashMap::from([("leader".into(), public)]),
            voucher_master_key_id: "voucher-master".into(),
            recovery_data: RecoveryData::default(),
            pending_note_expiry_blocks: 10,
            fee_margin_percent: DEFAULT_FEE_MARGIN_PERCENT,
            era_schedule: era_schedule(),
        };
        let genesis = HeaderId::from([0; 32]);
        let (sender, _receiver) = tokio::sync::watch::channel(None);
        let updater = StateUpdater::new(Arc::new(sender));
        let mut state = ServiceState::from_genesis(
            RecoveryState::from_settings(&settings).unwrap(),
            &settings,
            genesis,
            &[],
            &[],
            [],
            [],
            &updater,
        )
        .unwrap();
        let mut late_import = state.replay_copy();
        state.add_leadership_notes([known]);
        let child = WalletBlock {
            id: HeaderId::from([1; 32]),
            parent: genesis,
            epoch: 0.into(),
            updates: vec![NoteSetUpdate {
                note_set: NoteSetIndex::Ledger,
                nullifiers: vec![],
                commitments: vec![note.commitment()],
            }],
            revealed: vec![],
            withdrawals: vec![],
        };
        state.apply_block(&child).unwrap();
        late_import.apply_block(&child).unwrap();
        assert_eq!(
            late_import
                .leadership_wallet()
                .wallet_state_at(child.id)
                .unwrap()
                .aged_notes_at(2.into())
                .unwrap()
                .len(),
            0
        );
        late_import.add_leadership_notes([known]);
        late_import.rewind_to_genesis(genesis, &[], &[]).unwrap();
        late_import.apply_block(&child).unwrap();
        assert_eq!(
            late_import
                .leadership_wallet()
                .wallet_state_at(child.id)
                .unwrap()
                .aged_notes_at(2.into())
                .unwrap()
                .len(),
            1
        );
        let tracked = state.leadership_wallet().wallet_state_at(child.id).unwrap();
        assert_eq!(tracked.aged_notes_at(1.into()).unwrap().len(), 0);
        assert_eq!(tracked.aged_notes_at(2.into()).unwrap().len(), 1);
        assert!(
            state
                .wallet()
                .balance(child.id, note.public_key)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn pending_note_does_not_expire_without_lib_progress() {
        let nullifier = NoteNullifier(Fr::ONE);
        let mut pending_notes = PendingNotes::default();

        pending_notes.reserve(NoteSetIndex::Ledger, [nullifier], [1; 32]);
        pending_notes.evict_expired(0, EXPIRY_BLOCKS);

        assert!(
            pending_notes
                .nullifiers(NoteSetIndex::Ledger)
                .contains(&nullifier)
        );
    }

    #[test]
    fn pending_note_expires_after_enough_lib_progress() {
        let nullifier = NoteNullifier(Fr::ONE);
        let mut pending_notes = PendingNotes::default();

        pending_notes.reserve(NoteSetIndex::Ledger, [nullifier], [1; 32]);
        pending_notes.evict_expired(EXPIRY_BLOCKS - 1, EXPIRY_BLOCKS);
        assert!(
            pending_notes
                .nullifiers(NoteSetIndex::Ledger)
                .contains(&nullifier)
        );

        pending_notes.evict_expired(1, EXPIRY_BLOCKS);
        assert!(
            !pending_notes
                .nullifiers(NoteSetIndex::Ledger)
                .contains(&nullifier)
        );
    }

    #[test]
    fn released_pending_note_becomes_available() {
        let nullifier = NoteNullifier(Fr::ONE);
        let mut pending_notes = PendingNotes::default();

        pending_notes.reserve(NoteSetIndex::Ledger, [nullifier], [1; 32]);
        pending_notes.release(NoteSetIndex::Ledger, [nullifier], [1; 32]);

        assert!(pending_notes.nullifiers(NoteSetIndex::Ledger).is_empty());
    }

    #[test]
    fn reservations_with_the_same_nullifier_are_independent_between_sets() {
        let mut pending = PendingNotes::default();
        let nullifier = NoteNullifier(Fr::ONE);
        pending.reserve(NoteSetIndex::Ledger, [nullifier], [1; 32]);
        assert!(!pending.nullifiers(NoteSetIndex::Sdp).contains(&nullifier));
        pending.reserve(NoteSetIndex::Sdp, [nullifier], [2; 32]);
        pending.release(NoteSetIndex::Ledger, [nullifier], [2; 32]);
        assert!(
            pending
                .nullifiers(NoteSetIndex::Ledger)
                .contains(&nullifier)
        );
        pending.release(NoteSetIndex::Ledger, [nullifier], [1; 32]);
        assert!(pending.nullifiers(NoteSetIndex::Sdp).contains(&nullifier));
    }

    #[test]
    #[expect(
        clippy::too_many_lines,
        reason = "Exercises funding failures and reservations across note sets"
    )]
    fn a_failed_funding_releases_the_notes_of_its_builder_alone() {
        use std::sync::Arc;

        use lb_core::mantle::{
            Op,
            ops::{
                channel::{ChannelId, inscribe::InscriptionOp, withdraw::ChannelWithdrawOp},
                sdp::SDPWithdrawOp,
            },
            transactions::{GasPrices, builder::FundingNote, tx_list::ops::OpsGasContext},
        };
        use lb_services_utils::overwatch::RecoveryData;

        let settings = WalletServiceSettings {
            historical_recovery: Vec::new(),
            historical_recovery_withdrawals: Vec::new(),
            known_keys: HashMap::new(),
            leadership_keys: HashMap::new(),
            voucher_master_key_id: "voucher-master".into(),
            recovery_data: RecoveryData::default(),
            pending_note_expiry_blocks: 10,
            fee_margin_percent: DEFAULT_FEE_MARGIN_PERCENT,
            era_schedule: era_schedule(),
        };
        let recovery = RecoveryState {
            recovery_generation: 0,
            lib_wallet_state: None,
            known_notes: KnownNotes::new_sync(),
            leadership_lib_wallet_state: None,
            leadership_known_notes: KnownNotes::new_sync(),
            configured_leadership_keys: HashMap::new(),
            next_reward_key_index: 0,
        };
        let genesis = HeaderId::from([0; 32]);
        let (sender, _receiver) = tokio::sync::watch::channel(None);
        let updater = StateUpdater::new(Arc::new(sender));
        let mut state =
            ServiceState::from_genesis(recovery, &settings, genesis, &[], &[], [], [], &updater)
                .unwrap();

        let split_input = NoteNullifier(Fr::ONE);
        let in_flight = NoteNullifier(Fr::from(2u64));
        state.reserve_pending_notes([split_input, in_flight], [1; 32]);
        let tx_builder = MantleTxBuilder::new()
            .with_reservation_id([1; 32])
            .push_op(Op::Transfer(TransferOp::new(
                [split_input].into(),
                [NoteCommitment(Fr::ONE)].into(),
                Fr::ONE,
                0,
            )))
            .unwrap();
        let funding_pk = ZkPublicKey::new(Fr::ONE);
        let context = OpsContext {
            gas_context: OpsGasContext::new(HashMap::new(), GasPrices::new(1, 1), GAS_PROFILE),
        };

        state
            .fund_tx(
                genesis,
                &tx_builder,
                funding_pk,
                Fr::ONE,
                [funding_pk],
                &context,
                0,
            )
            .unwrap_err();

        assert_eq!(
            state.pending_notes.nullifiers(NoteSetIndex::Ledger),
            HashSet::from([in_flight])
        );

        // An SDP spend with the same nullifier must not release a Ledger
        // reservation when funding fails.
        let sdp = MantleTxBuilder::new()
            .push_op(Op::SDPWithdraw(SDPWithdrawOp {
                service_note_nullifier: in_flight,
                ..SDPWithdrawOp::sample()
            }))
            .unwrap();
        state
            .fund_tx(
                genesis,
                &sdp,
                funding_pk,
                Fr::ONE,
                [funding_pk],
                &context,
                0,
            )
            .unwrap_err();
        assert!(state.is_pending_note(NoteSetIndex::Ledger, &in_flight));
        let previously_funded = MantleTxBuilder::new()
            .with_reservation_id([1; 32])
            .extend_funding_inputs(
                Fr::ONE,
                [FundingNote {
                    note: Note::new(1, Fr::ONE, funding_pk),
                    nullifier: in_flight,
                }],
            )
            .unwrap();
        assert!(matches!(
            state.fund_tx(
                genesis,
                &previously_funded,
                funding_pk,
                Fr::ONE,
                [funding_pk],
                &context,
                0
            ),
            Err(WalletError::InputReserved)
        ));
        assert!(state.is_pending_note(NoteSetIndex::Ledger, &in_flight));
        let channel = ChannelId::from([7; 32]);
        let channel_set = NoteSetIndex::Channel(channel);
        state.reserve_pending_notes_in(NoteSetIndex::Sdp, [in_flight], [2; 32]);
        state.reserve_pending_notes_in(channel_set, [in_flight], [1; 32]);
        let withdrawal = MantleTxBuilder::new()
            .with_reservation_id([1; 32])
            .push_op(Op::ChannelWithdraw(ChannelWithdrawOp {
                channel_id: channel,
                inputs: [in_flight].into(),
                ..ChannelWithdrawOp::sample()
            }))
            .unwrap()
            .push_op(Op::SDPWithdraw(SDPWithdrawOp {
                service_note_nullifier: in_flight,
                ..SDPWithdrawOp::sample()
            }))
            .unwrap();
        state.release_transaction_notes(&withdrawal);
        assert!(!state.is_pending_note(channel_set, &in_flight));
        assert!(state.is_pending_note(NoteSetIndex::Ledger, &in_flight));
        assert!(state.is_pending_note(NoteSetIndex::Sdp, &in_flight));
        let mut inscription = InscriptionOp::sample_with_step();
        inscription.channel_id = channel;
        let step_nullifier = *inscription
            .steps()
            .next()
            .unwrap()
            .inputs()
            .first()
            .unwrap();
        state.reserve_pending_notes_in(channel_set, [step_nullifier], [3; 32]);
        state.reserve_pending_notes_in(NoteSetIndex::Sdp, [step_nullifier], [4; 32]);
        let step_builder = MantleTxBuilder::new()
            .with_additional_reservation_id([3; 32])
            .push_op(Op::ChannelInscribe(inscription))
            .unwrap();
        assert!(!state.has_reserved_funding(&step_builder));
        state.reserve_transaction_notes(&step_builder, [5; 32]);
        // Retrying the original builder must not release the first request's
        // reservation, even though that builder never received funding inputs.
        assert!(state.has_reserved_funding(&step_builder));
        assert!(matches!(
            state.fund_tx(
                genesis,
                &step_builder,
                funding_pk,
                Fr::ONE,
                [funding_pk],
                &context,
                0
            ),
            Err(WalletError::InputReserved)
        ));
        assert!(state.is_pending_note(channel_set, &step_nullifier));
        // A later failure involving the original builder cannot release the
        // reservation transferred to the funded transaction.
        state.release_transaction_notes(&step_builder);
        assert!(state.is_pending_note(channel_set, &step_nullifier));
        state.release_transaction_notes(&step_builder.with_reservation_id([5; 32]));
        assert!(!state.is_pending_note(channel_set, &step_nullifier));
        assert!(state.is_pending_note(NoteSetIndex::Sdp, &step_nullifier));
        let conflicting = MantleTxBuilder::new()
            .with_reservation_id([2; 32])
            .push_op(Op::Transfer(TransferOp::new(
                [in_flight].into(),
                [NoteCommitment(Fr::ONE)].into(),
                Fr::ONE,
                0,
            )))
            .unwrap();
        assert!(matches!(
            state.fund_tx(
                genesis,
                &conflicting,
                funding_pk,
                Fr::ONE,
                [funding_pk],
                &context,
                0
            ),
            Err(WalletError::InputReserved)
        ));
        assert!(state.is_pending_note(NoteSetIndex::Ledger, &in_flight));
    }

    /// Regression test for the wallet-service crash seen the first time a
    /// bootstrapping node switches to Online:
    ///
    /// During bootstrap the engine never advances the LIB, so no `LibUpdate`
    /// is broadcast and the wallet's `state.lib` stays at its starting point.
    /// `switch_to_online()` then jumps the chain LIB to the tip's k-th
    /// ancestor without broadcasting a `LibUpdate`. The first block processed
    /// afterwards broadcasts a `LibUpdate` whose `new_lib` can be a block the
    /// wallet has not applied yet (its frontier lags during IBD).
    ///
    /// The service backfills up to `new_lib` before advancing (see
    /// `handle_lib_update`). This test covers the `advance_lib` safety net for
    /// the case the block is still missing: it must skip the advance and keep
    /// the current LIB rather than panicking with
    /// "`WalletState` at LIB must exist: `UnknownBlock(...)`".
    #[test]
    fn lib_update_for_unapplied_block_skips_advance() {
        use std::sync::Arc;

        use lb_services_utils::overwatch::RecoveryData;

        let settings = WalletServiceSettings {
            historical_recovery: Vec::new(),
            historical_recovery_withdrawals: Vec::new(),
            known_keys: HashMap::new(),
            leadership_keys: HashMap::new(),
            voucher_master_key_id: "voucher-master".into(),
            recovery_data: RecoveryData::default(),
            pending_note_expiry_blocks: 10,
            fee_margin_percent: DEFAULT_FEE_MARGIN_PERCENT,
            era_schedule: era_schedule(),
        };

        // Fresh wallet: initialized from the genesis note sets.
        let recovery = RecoveryState {
            recovery_generation: 0,
            lib_wallet_state: None,
            known_notes: KnownNotes::new_sync(),
            leadership_lib_wallet_state: None,
            leadership_known_notes: KnownNotes::new_sync(),
            configured_leadership_keys: HashMap::new(),
            next_reward_key_index: 0,
        };

        let genesis = HeaderId::from([0; 32]);
        let (sender, _receiver) = tokio::sync::watch::channel(None);
        let updater = StateUpdater::new(Arc::new(sender));

        let mut state =
            ServiceState::from_genesis(recovery, &settings, genesis, &[], &[], [], [], &updater)
                .unwrap();

        // The first post-online LibUpdate delivers a new_lib the wallet never
        // applied (the silent bootstrap->online LIB jump outran the wallet).
        let unseen_lib = HeaderId::from([42; 32]);
        state.advance_lib(unseen_lib, 1);

        // No panic: the advance is skipped and the LIB stays at a block whose
        // WalletState exists.
        assert_eq!(state.lib(), genesis);
    }
}
