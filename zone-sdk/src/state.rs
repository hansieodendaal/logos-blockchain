use std::collections::{BTreeMap, HashMap, HashSet};

use lb_common_http_client::Slot;
use lb_core::{
    header::HeaderId,
    mantle::{SignedMantleTx, ops::channel::MsgId, tx::TxHash},
};
use rpds::HashTrieSetSync;

/// Transaction status in the lifecycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TxStatus {
    /// Not yet on canonical chain, needs resubmitting.
    Pending,
    /// On canonical chain between LIB and tip.
    Safe,
    /// At or below LIB, permanent.
    Finalized,
    /// Unknown transaction.
    Unknown,
}

impl TxStatus {
    #[must_use]
    pub const fn is_on_chain(&self) -> bool {
        matches!(self, Self::Safe | Self::Finalized)
    }
}

/// Result of resolving the latest inscription chain tip.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChainTipResolution {
    /// A deterministic chain tip was found.
    Determinate(MsgId),
    /// No inscriptions are known yet.
    NoInscriptions,
    /// Competing branches are present with no deterministic winner yet.
    Ambiguous,
}

/// Transaction state tracker.
pub struct TxState {
    /// All transactions being tracked, kept until finalized.
    pending: HashMap<TxHash, SignedMantleTx>,
    /// Per-block cumulative safe sets.
    block_states: BTreeMap<HeaderId, HashTrieSetSync<TxHash>>,
    /// Block parent relationships for pruning.
    parent_map: HashMap<HeaderId, HeaderId>,
    /// Finalized transactions.
    finalized: HashSet<TxHash>,
    /// Current LIB for pruning.
    current_lib: HeaderId,
    /// Inscriptions tracked per block header (allows multiple inscriptions per
    /// block)
    parent_msg_id_map: HashMap<HeaderId, Vec<MsgIdState>>,
    /// Last finalized inscription state retained when older blocks are pruned.
    /// This preserves enough lineage to continue building the correct chain.
    last_finalized_msg_state: Option<MsgIdState>,
}

/// Relationship between `HeaderId`, `TxHash` and `MsgId`
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct MsgIdState {
    /// The header ID of the block containing this inscription
    header_id: HeaderId,
    /// The slot of the block containing this inscription
    slot: Slot,
    /// The `TxHash` of the transaction that was included in the block
    tx_hash: TxHash,
    /// The `MsgId` of the parent message
    parent_id: MsgId,
    /// The `MsgId` of this message
    this_id: MsgId,
}

impl MsgIdState {
    #[must_use]
    pub const fn new(
        header_id: HeaderId,
        slot: Slot,
        tx_hash: TxHash,
        parent_id: MsgId,
        this_id: MsgId,
    ) -> Self {
        Self {
            header_id,
            slot,
            tx_hash,
            parent_id,
            this_id,
        }
    }

    #[must_use]
    pub const fn header_id(&self) -> HeaderId {
        self.header_id
    }

    #[must_use]
    pub const fn slot(&self) -> Slot {
        self.slot
    }

    #[must_use]
    pub const fn tx_hash(&self) -> TxHash {
        self.tx_hash
    }

    #[must_use]
    pub const fn parent_id(&self) -> MsgId {
        self.parent_id
    }

    #[must_use]
    pub const fn this_id(&self) -> MsgId {
        self.this_id
    }
}

impl TxState {
    #[must_use]
    pub fn new(lib: HeaderId) -> Self {
        let mut block_states = BTreeMap::new();
        block_states.insert(lib, HashTrieSetSync::new_sync());
        Self {
            pending: HashMap::new(),
            block_states,
            parent_map: HashMap::new(),
            finalized: HashSet::new(),
            current_lib: lib,
            parent_msg_id_map: HashMap::new(),
            last_finalized_msg_state: None,
        }
    }

    /// Submit a transaction for tracking.
    pub fn submit(&mut self, tx_hash: TxHash, signed_tx: SignedMantleTx) {
        self.pending.insert(tx_hash, signed_tx);
    }

    // Order MsgIdState values into parent-linked chain(s).
    // A chain starts at a root inscription (parent_id == MsgId::root())
    // or at an orphan relative to this batch (its parent_id does not appear
    // as any this_id in the provided states).
    fn order_msg_id_states(states: &[MsgIdState]) -> Vec<Vec<MsgIdState>> {
        const fn all_assigned(all: usize, assigned: &[MsgIdState]) -> bool {
            assigned.len() >= all
        }

        let mut ordered = Vec::new();
        let mut roots = Vec::new();
        let mut assigned = Vec::new();

        // First find the root inscription(s), those that have parent_id ==
        // MsgId::root() or whose parent_id is not referenced as a this_id in
        // any other inscription (orphans).
        for state in states {
            if state.parent_id() == MsgId::root()
                || !states.iter().any(|s| s.this_id() == state.parent_id())
            {
                roots.push(*state);
            }
        }

        for root in roots {
            ordered.push(vec![root]);
            assigned.push(root);
        }

        let mut cyclic = 0;
        while !all_assigned(states.len(), &assigned) && cyclic < states.len() {
            let mut progress = false;

            for chain in &mut ordered {
                let Some(mut last) = chain.last().copied() else {
                    continue;
                };

                for state in states {
                    if assigned.contains(state) {
                        continue; // Already ordered
                    }

                    if state.parent_id() == last.this_id() {
                        chain.push(*state);
                        assigned.push(*state);
                        last = *state;
                        progress = true;
                    }
                }
            }

            if progress {
                cyclic = 0;
            } else {
                cyclic += 1;
            }
        }

        ordered
    }

    /// Process a new block.
    pub fn process_block(
        &mut self,
        block_id: HeaderId,
        parent_id: HeaderId,
        lib: HeaderId,
        our_txs: &[MsgIdState],
    ) {
        // Store parent relationship for pruning
        self.parent_map.insert(block_id, parent_id);

        // Build cumulative safe set from parent.
        // If parent state is missing (e.g., first event after subscribe is a snapshot
        // where we receive a block whose parent we never saw), start with an empty set.
        // This is conservative: txs might show as "pending" when they should be "safe",
        // but they'll be correctly detected when seen in subsequent blocks.
        let mut safe_set = self
            .block_states
            .get(&parent_id)
            .cloned()
            .unwrap_or_default();

        for msg_state in our_txs {
            if self.pending.contains_key(&msg_state.tx_hash) {
                safe_set = safe_set.insert(msg_state.tx_hash);
            }
        }

        // Store inscriptions for this block
        if !our_txs.is_empty() {
            let ordered = Self::order_msg_id_states(our_txs);
            debug_assert!(
                ordered.len() <= 1,
                "expected at most one inscription chain per block, got {}",
                ordered.len()
            );

            if let Some(chain) = ordered.into_iter().next() {
                self.parent_msg_id_map.insert(block_id, chain);
            }
        }

        self.block_states.insert(block_id, safe_set);

        // When lib advances: finalize txs and prune
        if lib != self.current_lib {
            // Finalize txs in all blocks from new lib back to old lib (inclusive).
            // We may not have state for all intermediate blocks if we missed events,
            // so we skip blocks we don't know about.
            let mut block_opt = Some(lib);
            while let Some(block) = block_opt {
                if let Some(block_safe) = self.block_states.get(&block) {
                    for tx_hash in block_safe.iter() {
                        if self.pending.remove(tx_hash).is_some() {
                            self.finalized.insert(*tx_hash);
                        }
                    }
                }

                if block == self.current_lib {
                    break;
                }

                block_opt = self.parent_map.get(&block).copied();
            }

            // Prune finalized ancestors of new lib (but not lib itself)
            // Keep parent/inscription lineage so we can reconstruct chain ancestry
            // after clean restarts and around shallow reorg windows.
            let mut prune_cursor = self.parent_map.get(&lib).copied();
            let mut pruned_headers = Vec::new();

            while let Some(b) = prune_cursor {
                pruned_headers.push(b);
                self.block_states.remove(&b);
                prune_cursor = self.parent_map.get(&b).copied();
            }

            let pruned_msg_id_set = pruned_headers
                .iter()
                .filter_map(|header_id| self.parent_msg_id_map.get(header_id))
                .flatten()
                .copied()
                .collect::<Vec<_>>();

            let ordered_msg_id_states = Self::order_msg_id_states(&pruned_msg_id_set);

            // Determine which header(s) to keep below LIB: the last block in each chain.
            let kept_headers = ordered_msg_id_states
                .iter()
                .filter_map(|chain| chain.last().map(MsgIdState::header_id))
                .collect::<HashSet<_>>();

            // Remove all other pruned below-LIB headers from parent_msg_id_map.
            for header_id in &pruned_headers {
                if !kept_headers.contains(header_id) {
                    self.parent_msg_id_map.remove(header_id);
                }
            }

            // Refresh the finalized fallback from the retained below-LIB blocks.
            self.last_finalized_msg_state = kept_headers
                .iter()
                .filter_map(|header_id| self.parent_msg_id_map.get(header_id))
                .flatten()
                .copied()
                .last();

            self.prune_orphans(lib);
            self.current_lib = lib;
        }
    }

    /// Remove orphaned block-state snapshots whose parent snapshot was pruned.
    /// Keep block/inscription lineage maps for deterministic parent
    /// reconstruction.
    fn prune_orphans(&mut self, lib: HeaderId) {
        loop {
            let orphans: Vec<_> = self
                .parent_map
                .iter()
                .filter_map(|(id, parent)| {
                    if *id == lib {
                        return None; // lib is root
                    }
                    let parent_is_lib = *parent == lib;
                    let parent_snapshot_exists = self.block_states.contains_key(parent);
                    (!parent_is_lib && !parent_snapshot_exists).then_some(*id)
                })
                .collect();

            if orphans.is_empty() {
                break;
            }

            for orphan in orphans {
                self.block_states.remove(&orphan);
                // Keep parent_msg_id_map lineage, but remove orphan edges so pruning converges.
                self.parent_map.remove(&orphan);
            }
        }
    }

    #[must_use]
    pub fn status(&self, tx_hash: &TxHash, tip: HeaderId) -> TxStatus {
        if self.finalized.contains(tx_hash) {
            return TxStatus::Finalized;
        }

        if let Some(safe_set) = self.block_states.get(&tip)
            && safe_set.contains(tx_hash)
        {
            return TxStatus::Safe;
        }

        if self.pending.contains_key(tx_hash) {
            return TxStatus::Pending;
        }

        TxStatus::Unknown
    }

    /// Pending txs for resubmission (not safe at tip).
    pub fn pending_txs(&self, tip: HeaderId) -> impl Iterator<Item = (&TxHash, &SignedMantleTx)> {
        let safe = self
            .block_states
            .get(&tip)
            .cloned()
            .unwrap_or_else(HashTrieSetSync::new_sync);
        self.pending
            .iter()
            .filter(move |(hash, _)| !safe.contains(hash))
    }

    /// Number of pending transactions.
    #[must_use]
    pub fn pending_count(&self) -> usize {
        self.pending.len()
    }

    /// Number of finalized transactions.
    #[must_use]
    pub fn finalized_count(&self) -> usize {
        self.finalized.len()
    }

    /// Check if we have state for a block.
    #[must_use]
    pub fn has_block(&self, block_id: &HeaderId) -> bool {
        self.block_states.contains_key(block_id)
    }

    /// Current LIB.
    #[must_use]
    pub const fn lib(&self) -> HeaderId {
        self.current_lib
    }

    /// Remove a pending transaction, returning it if it existed.
    ///
    /// Also scrubs the tx hash from every block safe set so that
    /// [`status`](Self::status) immediately reflects the removal.
    pub fn remove_pending(&mut self, tx_hash: &TxHash) -> Option<SignedMantleTx> {
        let tx = self.pending.remove(tx_hash)?;
        for safe_set in self.block_states.values_mut() {
            *safe_set = safe_set.remove(tx_hash);
        }
        Some(tx)
    }

    /// All pending transactions (for checkpoint serialization).
    pub fn all_pending_txs(&self) -> impl Iterator<Item = (&TxHash, &SignedMantleTx)> {
        self.pending.iter()
    }

    #[must_use]
    pub fn is_tx_pending(&self, tx_hash: &TxHash) -> bool {
        self.pending.contains_key(tx_hash)
    }

    /// Returns true if the transaction hash has been observed in any on-chain
    /// inscription that this state has processed, even if the current tip's
    /// cumulative safe set has not yet been rebuilt to include it.
    #[must_use]
    pub fn is_tx_observed_on_chain(&self, tx_hash: &TxHash) -> bool {
        if self.finalized.contains(tx_hash) {
            return true;
        }

        if self
            .block_states
            .values()
            .any(|safe_set| safe_set.contains(tx_hash))
        {
            return true;
        }

        self.parent_msg_id_map
            .values()
            .flatten()
            .any(|msg_state| msg_state.tx_hash() == *tx_hash)
    }

    #[must_use]
    pub fn is_msg_id_used_as_parent_on_chain(&self, msg_id: MsgId) -> bool {
        self.parent_msg_id_map
            .values()
            .flatten()
            .any(|msg_state| msg_state.parent_id() == msg_id)
            || self
                .last_finalized_msg_state
                .is_some_and(|s| s.parent_id() == msg_id)
    }

    fn all_known_inscriptions(&self) -> Vec<MsgIdState> {
        let mut all = Vec::new();
        for states in self.parent_msg_id_map.values() {
            all.extend(states.iter().copied());
        }
        if let Some(finalized) = self.last_finalized_msg_state
            && !all.contains(&finalized)
        {
            all.push(finalized);
        }
        all
    }

    /// Resolve the latest known inscription chain tip.
    ///
    /// If two competing branches have equal best length, the tip is ambiguous
    /// until a subsequent inscription extends one branch.
    #[must_use]
    pub fn resolve_inscription_chain_tip(&self) -> ChainTipResolution {
        let all = self.all_known_inscriptions();

        if all.is_empty() {
            return ChainTipResolution::NoInscriptions;
        }

        let chains = Self::order_msg_id_states(&all);

        if chains.is_empty() {
            return ChainTipResolution::NoInscriptions;
        }

        let max_len = chains.iter().map(Vec::len).max().unwrap_or(0);

        let best: Vec<&Vec<MsgIdState>> = chains.iter().filter(|c| c.len() == max_len).collect();

        if best.len() > 1 {
            return ChainTipResolution::Ambiguous;
        }

        best[0]
            .last()
            .map_or(ChainTipResolution::NoInscriptions, |state| {
                ChainTipResolution::Determinate(state.this_id())
            })
    }

    /// Get the latest confirmed parent message ID from blocks that have been
    /// processed. Returns `None` if there are no inscriptions or the latest
    /// tip is ambiguous.
    #[must_use]
    pub fn latest_confirmed_parent_id(&self) -> Option<MsgId> {
        match self.resolve_inscription_chain_tip() {
            ChainTipResolution::Determinate(this_id) => self
                .all_known_inscriptions()
                .into_iter()
                .find(|s| s.this_id() == this_id)
                .map(|s| s.parent_id()),
            ChainTipResolution::NoInscriptions | ChainTipResolution::Ambiguous => None,
        }
    }

    /// Get the latest chain tip message ID from processed blocks.
    /// Returns `None` when no inscriptions are known or the tip is ambiguous.
    #[must_use]
    pub fn latest_inscriptions_tip_id(&self) -> Option<MsgId> {
        match self.resolve_inscription_chain_tip() {
            ChainTipResolution::Determinate(this_id) => Some(this_id),
            ChainTipResolution::NoInscriptions | ChainTipResolution::Ambiguous => None,
        }
    }

    /// Get the mapping of block headers to their included inscriptions
    /// (`MsgIdState`).
    #[must_use]
    pub fn parent_msg_id_map(&self) -> HashMap<HeaderId, Vec<MsgIdState>> {
        self.parent_msg_id_map.clone()
    }
}

#[cfg(test)]
mod tests {
    use lb_core::mantle::{MantleTx, Transaction as _, ledger::Tx as LedgerTx};

    use super::*;

    fn header_id(n: u8) -> HeaderId {
        let mut bytes = [0u8; 32];
        bytes[0] = n;
        HeaderId::from(bytes)
    }

    fn make_dummy_tx(data: u8) -> SignedMantleTx {
        let ledger_tx = LedgerTx::new(vec![], vec![]);
        let mantle_tx = MantleTx {
            ops: vec![],
            ledger_tx,
            storage_gas_price: 0,
            execution_gas_price: data.into(),
        };
        SignedMantleTx {
            ops_proofs: vec![],
            ledger_tx_proof: lb_key_management_system_service::keys::ZkKey::multi_sign(
                &[],
                mantle_tx.hash().as_ref(),
            )
            .expect("empty multi-sign"),
            mantle_tx,
        }
    }

    fn empty_our_txs() -> Vec<MsgIdState> {
        Vec::new()
    }

    fn our_txs_with_hash(header_id: HeaderId, slot: Slot, hash: TxHash) -> Vec<MsgIdState> {
        // Derive a distinct this_id from the tx_hash bytes so parent_id != this_id,
        // preventing a self-loop in the inscription children map.
        let bytes: [u8; 32] = hash.into();
        let this_id = MsgId::from(bytes);
        vec![MsgIdState::new(
            header_id,
            slot,
            hash,
            MsgId::root(),
            this_id,
        )]
    }

    #[test]
    fn submit_and_query_pending() {
        let genesis = header_id(0);
        let mut state = TxState::new(genesis);
        let tx = make_dummy_tx(1);
        let hash = tx.mantle_tx.hash();

        state.submit(hash, tx);
        assert_eq!(state.pending_count(), 1);
        assert_eq!(state.status(&hash, genesis), TxStatus::Pending);
    }

    #[test]
    fn block_promotes_to_safe() {
        let genesis = header_id(0);
        let b1 = header_id(1);
        let mut state = TxState::new(genesis);

        let tx = make_dummy_tx(1);
        let hash = tx.mantle_tx.hash();
        state.submit(hash, tx);

        // Process block containing our tx, lib stays at genesis
        state.process_block(
            b1,
            genesis,
            genesis,
            &our_txs_with_hash(header_id(1), Slot::new(123), hash),
        );

        assert_eq!(state.status(&hash, b1), TxStatus::Safe);
        assert_eq!(state.status(&hash, genesis), TxStatus::Pending);
    }

    #[test]
    fn lib_advance_finalizes() {
        let genesis = header_id(0);
        let b1 = header_id(1);
        let b2 = header_id(2);
        let mut state = TxState::new(genesis);

        let tx = make_dummy_tx(1);
        let hash = tx.mantle_tx.hash();
        state.submit(hash, tx);

        // b1 with our tx
        state.process_block(
            b1,
            genesis,
            genesis,
            &our_txs_with_hash(header_id(1), Slot::new(123), hash),
        );
        assert_eq!(state.status(&hash, b1), TxStatus::Safe);

        // b2, lib advances to b1
        state.process_block(b2, b1, b1, &empty_our_txs());
        assert_eq!(state.status(&hash, b2), TxStatus::Finalized);
        assert_eq!(state.finalized_count(), 1);
    }

    #[test]
    fn pending_txs_excludes_safe() {
        let genesis = header_id(0);
        let b1 = header_id(1);
        let mut state = TxState::new(genesis);

        let tx1 = make_dummy_tx(1);
        let tx2 = make_dummy_tx(2);
        let hash1 = tx1.mantle_tx.hash();
        let hash2 = tx2.mantle_tx.hash();

        state.submit(hash1, tx1);
        state.submit(hash2, tx2);

        // b1 contains only tx1
        state.process_block(
            b1,
            genesis,
            genesis,
            &our_txs_with_hash(header_id(1), Slot::new(123), hash1),
        );

        // pending_txs at b1 should only return tx2
        let pending: Vec<_> = state.pending_txs(b1).map(|(h, _)| *h).collect();
        assert_eq!(pending.len(), 1);
        assert!(pending.contains(&hash2));
    }

    #[test]
    fn reorg_changes_safe_status() {
        // G -> b1 (has tx)
        //   -> b2 (no tx)
        let genesis = header_id(0);
        let b1 = header_id(1);
        let b2 = header_id(2);
        let mut state = TxState::new(genesis);

        let tx = make_dummy_tx(1);
        let hash = tx.mantle_tx.hash();
        state.submit(hash, tx);

        // b1 has our tx
        state.process_block(
            b1,
            genesis,
            genesis,
            &our_txs_with_hash(header_id(1), Slot::new(123), hash),
        );
        assert_eq!(state.status(&hash, b1), TxStatus::Safe);

        // b2 forks from genesis, no tx
        state.process_block(b2, genesis, genesis, &empty_our_txs());

        // At b2 tip, tx is not safe (different branch)
        assert_eq!(state.status(&hash, b2), TxStatus::Pending);
        // At b1 tip, tx is still safe
        assert_eq!(state.status(&hash, b1), TxStatus::Safe);
    }

    #[test]
    fn lib_advance_prunes_ancestors_and_orphans() {
        // Chain: genesis <- a1 <- a2 <- a3 (lib) <- a4 <- a5 <- a6
        //                    |
        //                   b1 <- b2 (fork from a1)
        let genesis = header_id(0);
        let a1 = header_id(1);
        let a2 = header_id(2);
        let a3 = header_id(3);
        let a4 = header_id(4);
        let a5 = header_id(5);
        let a6 = header_id(6);
        let b1 = header_id(10);
        let b2 = header_id(11);

        let mut state = TxState::new(genesis);

        // Build main chain up to a1
        state.process_block(a1, genesis, genesis, &empty_our_txs());

        // Build fork from a1 (before lib advances past a1)
        state.process_block(b1, a1, genesis, &empty_our_txs());
        state.process_block(b2, b1, genesis, &empty_our_txs());

        // Verify fork blocks exist before lib advances
        assert!(state.block_states.contains_key(&b1));
        assert!(state.block_states.contains_key(&b2));

        // Continue main chain, lib advances to a3
        state.process_block(a2, a1, genesis, &empty_our_txs());
        state.process_block(a3, a2, a3, &empty_our_txs()); // lib advances to a3

        // After lib advances to a3:
        // - genesis, a1, a2 should be pruned (ancestors up to and including old lib)
        // - b1, b2 should be GC'd (orphans - their ancestor a1 was pruned)
        // - a3 (new lib) should exist

        assert!(
            !state.block_states.contains_key(&genesis),
            "genesis (old lib) should be pruned"
        );
        assert!(!state.block_states.contains_key(&a1), "a1 should be pruned");
        assert!(!state.block_states.contains_key(&a2), "a2 should be pruned");
        assert!(
            !state.block_states.contains_key(&b1),
            "orphan b1 should be pruned"
        );
        assert!(
            !state.block_states.contains_key(&b2),
            "orphan b2 should be pruned"
        );

        assert!(state.block_states.contains_key(&a3), "lib should exist");

        // Continue and verify pruning continues working
        state.process_block(a4, a3, a3, &empty_our_txs());
        state.process_block(a5, a4, a5, &empty_our_txs()); // lib advances to a5
        state.process_block(a6, a5, a5, &empty_our_txs());

        assert!(
            !state.block_states.contains_key(&a3),
            "old lib should be pruned"
        );
        assert!(!state.block_states.contains_key(&a4), "a4 should be pruned");
        assert!(state.block_states.contains_key(&a5), "new lib should exist");
        assert!(state.block_states.contains_key(&a6), "tip should exist");
    }

    #[test]
    fn multi_block_lib_advance_finalizes_intermediate() {
        // When LIB advances multiple blocks at once, all intermediate txs must finalize
        // genesis <- b1 (tx1) <- b2 (tx2) <- b3
        //                                     ^
        //                                    LIB jumps here
        let genesis = header_id(0);
        let b1 = header_id(1);
        let b2 = header_id(2);
        let b3 = header_id(3);
        let mut state = TxState::new(genesis);

        let tx1 = make_dummy_tx(1);
        let tx2 = make_dummy_tx(2);
        let hash1 = tx1.mantle_tx.hash();
        let hash2 = tx2.mantle_tx.hash();

        state.submit(hash1, tx1);
        state.submit(hash2, tx2);

        // b1 has tx1
        state.process_block(
            b1,
            genesis,
            genesis,
            &our_txs_with_hash(header_id(1), Slot::new(123), hash1),
        );
        // b2 has tx2
        state.process_block(
            b2,
            b1,
            genesis,
            &our_txs_with_hash(header_id(2), Slot::new(234), hash2),
        );
        // b3, lib jumps from genesis to b2 (skipping b1)
        state.process_block(b3, b2, b2, &empty_our_txs());

        // Both tx1 (in b1) and tx2 (in b2) should be finalized
        assert_eq!(state.status(&hash1, b3), TxStatus::Finalized);
        assert_eq!(state.status(&hash2, b3), TxStatus::Finalized);
        assert_eq!(state.finalized_count(), 2);
    }

    #[test]
    fn keeps_finalized_history_when_pruned_below_lib() {
        // Ensure we keep enough inscription history below LIB to keep building
        // correct parent relationships even when no inscriptions are above LIB.
        let genesis = header_id(0);
        let b1 = header_id(1);
        let b2 = header_id(2);
        let mut state = TxState::new(genesis);

        let tx1 = make_dummy_tx(1);
        let hash1 = tx1.mantle_tx.hash();
        state.submit(hash1, tx1);

        let parent_1 = MsgId::root();
        let this_1 = MsgId::from([9u8; 32]);
        state.process_block(
            b1,
            genesis,
            genesis,
            &[MsgIdState::new(b1, Slot::new(123), hash1, parent_1, this_1)],
        );

        // Advance LIB to b2 with no inscriptions. This prunes b1 from maps.
        state.process_block(b2, b1, b2, &empty_our_txs());

        // We still retain finalized inscription lineage for future parent selection.
        assert_eq!(state.latest_inscriptions_tip_id(), Some(this_1));
        assert_eq!(state.latest_confirmed_parent_id(), Some(parent_1));
    }

    #[test]
    fn competing_children_are_ambiguous_until_one_branch_extends() {
        let genesis = header_id(0);
        let block_1 = header_id(1);
        let block_2 = header_id(2);
        let block_3 = header_id(3);
        let mut state = TxState::new(genesis);

        let this_pid_a = MsgId::from([21u8; 32]);
        let this_pid_b = MsgId::from([22u8; 32]);

        let tx_a = make_dummy_tx(21);
        let tx_b = make_dummy_tx(22);

        state.process_block(
            block_1,
            genesis,
            genesis,
            &[MsgIdState::new(
                header_id(1),
                Slot::new(123),
                tx_a.mantle_tx.hash(),
                MsgId::root(),
                this_pid_a,
            )],
        );
        state.process_block(
            block_2,
            genesis,
            genesis,
            &[MsgIdState::new(
                header_id(2),
                Slot::new(123),
                tx_b.mantle_tx.hash(),
                MsgId::root(),
                this_pid_b,
            )],
        );

        assert_eq!(
            state.resolve_inscription_chain_tip(),
            ChainTipResolution::Ambiguous,
            "two children of the same parent are ambiguous without a tie-break"
        );

        let this_pid_b_child = MsgId::from([23u8; 32]);
        let tx_b_child = make_dummy_tx(23);
        state.process_block(
            block_3,
            block_2,
            genesis,
            &[MsgIdState::new(
                header_id(3),
                Slot::new(234),
                tx_b_child.mantle_tx.hash(),
                this_pid_b,
                this_pid_b_child,
            )],
        );

        assert_eq!(
            state.resolve_inscription_chain_tip(),
            ChainTipResolution::Determinate(this_pid_b_child)
        );
    }

    #[test]
    fn remove_pending_scrubs_safe_sets_and_status_becomes_unknown() {
        let genesis = header_id(0);
        let b1 = header_id(1);
        let mut state = TxState::new(genesis);

        let tx = make_dummy_tx(1);
        let hash = tx.mantle_tx.hash();
        state.submit(hash, tx);

        state.process_block(
            b1,
            genesis,
            genesis,
            &our_txs_with_hash(b1, Slot::new(123), hash),
        );
        assert_eq!(state.status(&hash, b1), TxStatus::Safe);

        let removed = state.remove_pending(&hash);
        assert!(removed.is_some());
        assert_eq!(state.status(&hash, b1), TxStatus::Unknown);
        assert!(!state.is_tx_pending(&hash));
    }

    #[test]
    fn process_block_keeps_multiple_inscriptions_from_same_block_in_order() {
        let genesis = header_id(0);
        let b1 = header_id(1);
        let mut state = TxState::new(genesis);

        let tx1 = make_dummy_tx(41);
        let tx2 = make_dummy_tx(42);
        let tx3 = make_dummy_tx(43);

        let this_1 = MsgId::from([41u8; 32]);
        let this_2 = MsgId::from([42u8; 32]);
        let this_3 = MsgId::from([43u8; 32]);

        state.process_block(
            b1,
            genesis,
            genesis,
            &[
                MsgIdState::new(
                    b1,
                    Slot::new(100),
                    tx1.mantle_tx.hash(),
                    MsgId::root(),
                    this_1,
                ),
                MsgIdState::new(b1, Slot::new(100), tx2.mantle_tx.hash(), this_1, this_2),
                MsgIdState::new(b1, Slot::new(100), tx3.mantle_tx.hash(), this_2, this_3),
            ],
        );

        let map = state.parent_msg_id_map();
        let chain = map.get(&b1).expect("block chain should be stored");
        assert_eq!(chain.len(), 3);
        assert_eq!(chain[0].this_id(), this_1);
        assert_eq!(chain[1].this_id(), this_2);
        assert_eq!(chain[2].this_id(), this_3);
        assert_eq!(state.latest_inscriptions_tip_id(), Some(this_3));
    }

    #[test]
    fn latest_tip_and_parent_are_none_when_no_inscriptions_exist() {
        let genesis = header_id(0);
        let state = TxState::new(genesis);

        assert_eq!(state.latest_inscriptions_tip_id(), None);
        assert_eq!(state.latest_confirmed_parent_id(), None);
        assert_eq!(
            state.resolve_inscription_chain_tip(),
            ChainTipResolution::NoInscriptions
        );
    }

    #[test]
    fn latest_tip_and_parent_follow_linear_chain() {
        let genesis = header_id(0);
        let b1 = header_id(1);
        let b2 = header_id(2);
        let mut state = TxState::new(genesis);

        let this_1 = MsgId::from([31u8; 32]);
        let this_2 = MsgId::from([32u8; 32]);

        state.process_block(
            b1,
            genesis,
            genesis,
            &[MsgIdState::new(
                b1,
                Slot::new(101),
                make_dummy_tx(31).mantle_tx.hash(),
                MsgId::root(),
                this_1,
            )],
        );
        state.process_block(
            b2,
            b1,
            genesis,
            &[MsgIdState::new(
                b2,
                Slot::new(102),
                make_dummy_tx(32).mantle_tx.hash(),
                this_1,
                this_2,
            )],
        );

        assert_eq!(
            state.resolve_inscription_chain_tip(),
            ChainTipResolution::Determinate(this_2)
        );
        assert_eq!(state.latest_inscriptions_tip_id(), Some(this_2));
        assert_eq!(state.latest_confirmed_parent_id(), Some(this_1));
    }

    #[test]
    fn process_block_keeps_only_tail_below_lib_for_each_chain() {
        let genesis = header_id(0);
        let a1 = header_id(1);
        let a2 = header_id(2);
        let a3 = header_id(3);
        let mut state = TxState::new(genesis);

        let tx1 = make_dummy_tx(51);
        let tx2 = make_dummy_tx(52);

        let this_1 = MsgId::from([51u8; 32]);
        let this_2 = MsgId::from([52u8; 32]);

        state.process_block(
            a1,
            genesis,
            genesis,
            &[MsgIdState::new(
                a1,
                Slot::new(100),
                tx1.mantle_tx.hash(),
                MsgId::root(),
                this_1,
            )],
        );
        state.process_block(
            a2,
            a1,
            genesis,
            &[MsgIdState::new(
                a2,
                Slot::new(101),
                tx2.mantle_tx.hash(),
                this_1,
                this_2,
            )],
        );

        state.process_block(a3, a2, a3, &empty_our_txs());

        let map = state.parent_msg_id_map();
        assert!(
            !map.contains_key(&a1),
            "older below-lib lineage should be dropped when superseded"
        );
        assert!(
            map.contains_key(&a2),
            "tail below-lib lineage should be retained"
        );
        assert_eq!(state.latest_inscriptions_tip_id(), Some(this_2));
    }

    #[test]
    fn resolve_chain_tip_prefers_longest_chain() {
        let genesis = header_id(0);
        let b1 = header_id(1);
        let b2 = header_id(2);
        let b3 = header_id(3);
        let mut state = TxState::new(genesis);

        let a1 = MsgId::from([61u8; 32]);
        let a2 = MsgId::from([62u8; 32]);
        let b1_msg = MsgId::from([63u8; 32]);

        state.process_block(
            b1,
            genesis,
            genesis,
            &[MsgIdState::new(
                b1,
                Slot::new(100),
                make_dummy_tx(61).mantle_tx.hash(),
                MsgId::root(),
                a1,
            )],
        );
        state.process_block(
            b2,
            b1,
            genesis,
            &[MsgIdState::new(
                b2,
                Slot::new(101),
                make_dummy_tx(62).mantle_tx.hash(),
                a1,
                a2,
            )],
        );
        state.process_block(
            b3,
            genesis,
            genesis,
            &[MsgIdState::new(
                b3,
                Slot::new(102),
                make_dummy_tx(63).mantle_tx.hash(),
                MsgId::root(),
                b1_msg,
            )],
        );

        assert_eq!(
            state.resolve_inscription_chain_tip(),
            ChainTipResolution::Determinate(a2)
        );
    }
}
