use core::num::NonZeroU32;
use std::{
    fmt::{self, Display, Formatter},
    time::Duration,
};

use futures::poll;
use lb_binary_codec::{
    bincode::{DeserializeOp as _, SerializeOp as _},
    canonical::BinaryEncode as _,
};
use lb_chain_service::{ConsensusMsg, CryptarchiaInfo, PhaseTag, Query, Slot, State};
use lb_core::{
    block::{BlockTransactions, UncleHeaders},
    era::AcceptedForkDigests,
    events::{BlockEvent, Event},
    mantle::{
        GenesisParameters, GenesisTime,
        gas::Gas,
        note_set::NoteSetUpdate,
        notes::derive_note_nullifier,
        ops::{
            channel::{
                MsgId,
                inscribe::{Inscription, InscriptionOp, MiniBlocks},
            },
            sdp::{SDPActiveOp, SDPDeclareOp},
            transfer::TransferOp,
        },
        transactions::{GasPrices, tx_list::ops::OpsGasContext},
    },
    proofs::leader_proof::Groth16LeaderProof,
    sdp::ServiceParameters,
};
use lb_cryptarchia_engine::EpochConfig;
use lb_groth16::{AdditiveGroup as _, CompressedGroth16Proof, Field as _};
use lb_key_management_system_service::{
    KMSService, keys::UnverifiedEd25519PublicKey, message::KMSMessage,
};
use lb_ledger::{
    config::{BlendPoWConfig, ModulusShift, PoWConfig, RewardPoWConfig},
    mantle::sdp::{ServiceRewardsParameters, rewards},
};
use lb_storage_service::api::requests::StorageMsg;
use lb_time::era::{Era, EraEntriesAfterGenesis, EraTiming};
use lb_utils::math::{NonNegativeRatio, PositiveRatio, ProperFraction};
use overwatch::services::{
    relay::{InboundRelay, OutboundRelay, Relay},
    state::{NoOperator, NoState, ServiceState as _},
};
use time::OffsetDateTime;
use tokio::{
    sync::{mpsc, oneshot::error::TryRecvError, watch},
    time::{sleep, timeout},
};

use super::*;
#[path = "tests/recovery.rs"]
mod recovery;

#[test]
fn op_proof_pairs_each_operation_with_its_proof_type() {
    let zk = ZkTransferProof::new(CompressedGroth16Proof::from_bytes(&[1u8; 128]));
    let signature = Ed25519Signature::from_bytes(&[2u8; 64]);
    let transfer = TransferOp::sample();
    let declare = SDPDeclareOp::sample();
    let active = SDPActiveOp::sample();
    assert_eq!(
        op_proof(&OpRef::Transfer(&transfer), Some(zk), None).unwrap(),
        OpProof::ZkTransfer(zk)
    );
    assert_eq!(
        op_proof(&OpRef::SDPDeclare(&declare), Some(zk), Some(signature)).unwrap(),
        OpProof::ZkAndEd25519Sigs(ZkAndEd25519Proof::new(zk, signature))
    );
    assert_eq!(
        op_proof(&OpRef::SDPActive(&active), None, Some(signature)).unwrap(),
        OpProof::Ed25519Sig(signature)
    );
    assert!(op_proof(&OpRef::Transfer(&transfer), None, None).is_err());
}

#[test]
fn historical_recovery_requires_a_current_membership_proof() {
    let owner = ZkPublicKey::new(Fr::from(2u64));
    let committed = Note::new(25, Fr::from(1u64), owner);
    let fabricated = Note::new(25, Fr::from(2u64), owner);
    let (mmr, path) = CommitmentMmr::new()
        .push_with_paths(committed.commitment(), &mut [])
        .unwrap();
    assert!(valid_historical_path(&mmr, committed, &path));
    assert!(!valid_historical_path(&mmr, fabricated, &path));
    let (later, _) = mmr
        .push_with_paths(fabricated.commitment(), &mut [])
        .unwrap();
    assert!(!valid_historical_path(&later, committed, &path));
}

#[tokio::test]
async fn fabricated_historical_imports_do_not_start_replay() {
    let key = ZkKey::new(Fr::from(7u64));
    let committed = known_note(&key, 100, 1).note;
    let fabricated = Note {
        nonce: Fr::from(2u64),
        ..committed
    };
    let (mmr, path) = CommitmentMmr::new()
        .push_with_paths(committed.commitment(), &mut [])
        .unwrap();
    let ledger = LedgerState::from_notes([committed], &ledger_config(), &genesis_parameters());
    assert!(valid_historical_path(&mmr, committed, &path));
    let (sender, mut requests) = mpsc::channel(8);
    let chain = CryptarchiaServiceApi::<DummyCryptarchia>::new(OutboundRelay::new(sender));
    let unexpected = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let observed = Arc::clone(&unexpected);
    tokio::spawn(async move {
        while let Some(message) = requests.recv().await {
            match message {
                ConsensusMsg::Query(Query::Info { reply_channel }) => {
                    let genesis = HeaderId::from([0; 32]);
                    drop(reply_channel.send(ChainServiceInfo {
                        cryptarchia_info: CryptarchiaInfo {
                            lib: genesis,
                            lib_slot: Slot::from(0),
                            tip: genesis,
                            slot: Slot::from(0),
                            height: 0,
                            state: State::Online,
                        },
                        phase: PhaseTag::Following,
                    }));
                }
                ConsensusMsg::Query(Query::GetLedgerState { reply_channel, .. }) => {
                    drop(reply_channel.send(Some(ledger.clone())));
                }
                _ => {
                    observed.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                }
            }
        }
    });
    let settings = wallet_settings(committed.public_key);
    let (updates, _receiver) = watch::channel(None);
    let updater = StateUpdater::new(Arc::new(updates));
    let mut state = wallet_state(&settings, &[], &updater);
    let (storage, mut writes) = storage_api();
    let kms = kms_api(key);
    for attempt in 0..3 {
        let result = TestWallet::recover_note(
            fabricated,
            NoteSetIndex::Ledger,
            path.clone(),
            &mut state,
            &storage,
            &chain,
            &kms,
            &era_schedule(),
        )
        .await;
        if attempt == 0 {
            assert!(matches!(
                result,
                Err(WalletServiceError::InvalidHistoricalPath)
            ));
        } else {
            assert!(matches!(
                result,
                Err(WalletServiceError::HistoricalReplayRateLimited)
            ));
        }
    }
    assert!(writes.try_recv().is_err());
    assert_eq!(unexpected.load(std::sync::atomic::Ordering::SeqCst), 0);
    assert!(state.wallet().known_notes().is_empty());
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "exercises replay, failure and deduplication"
)]
async fn confirmed_genesis_note_is_recovered_with_a_verified_path() {
    use lb_core::{
        block::genesis::GenesisBlockBuilder,
        mantle::{ops::channel::NULL_CHANNEL_ID, transactions::GenesisAllocation},
    };

    let key = ZkKey::new(Fr::from(7u64));
    let genesis = GenesisBlockBuilder::new()
        .add_note(GenesisAllocation {
            value: 100,
            public_key: key.to_public_key(),
        })
        .set_inscription(InscriptionOp {
            channel_id: NULL_CHANNEL_ID,
            inscription: Inscription::new_unchecked(genesis_parameters().encode_to_vec()),
            parent: MsgId::root(),
            signer: UnverifiedEd25519PublicKey::from_bytes(&[0; 32]).unwrap(),
            mini_blocks: MiniBlocks::default(),
        })
        .build()
        .unwrap();
    let notes = genesis.genesis_tx().initial_notes();
    let note = notes[0];
    let id = genesis.header().id();
    let (mmr, path) = CommitmentMmr::new()
        .push_with_paths(note.commitment(), &mut [])
        .unwrap();
    let ledger = LedgerState::from_notes(
        notes.iter().copied(),
        &ledger_config(),
        &genesis_parameters(),
    );
    assert!(valid_historical_path(&mmr, note, &path));
    let (sender, mut requests) = mpsc::channel(8);
    let chain = CryptarchiaServiceApi::<DummyCryptarchia>::new(OutboundRelay::new(sender));
    let replay_count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let observed = Arc::clone(&replay_count);
    let fail_genesis = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let fail_query = Arc::clone(&fail_genesis);
    tokio::spawn(async move {
        while let Some(message) = requests.recv().await {
            match message {
                ConsensusMsg::Query(Query::Info { reply_channel }) => {
                    drop(reply_channel.send(ChainServiceInfo {
                        cryptarchia_info: CryptarchiaInfo {
                            lib: id,
                            lib_slot: 0.into(),
                            tip: id,
                            slot: 0.into(),
                            height: 0,
                            state: State::Online,
                        },
                        phase: PhaseTag::Following,
                    }));
                }
                ConsensusMsg::Query(Query::GetLedgerState { reply_channel, .. }) => {
                    drop(reply_channel.send(Some(ledger.clone())));
                }
                ConsensusMsg::Query(Query::GetGenesisBlock { reply_channel }) => {
                    observed.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    if !fail_query.load(std::sync::atomic::Ordering::SeqCst) {
                        drop(reply_channel.send(genesis.clone()));
                    }
                }
                ConsensusMsg::Query(Query::GetHeaders { reply_channel, .. }) => {
                    drop(reply_channel.send(Box::pin(futures::stream::iter([Ok(id)]))));
                }
                _ => panic!("unexpected chain request"),
            }
        }
    });
    let (sender, mut requests) = mpsc::channel(8);
    let storage = StorageApi::<TestTx>::new(OutboundRelay::new(sender));
    tokio::spawn(async move {
        let mut records = HashMap::new();
        while let Some(request) = requests.recv().await {
            match request {
                StorageMsg::BulkStoreConfirmed {
                    items,
                    reply_channel,
                } => {
                    records.extend(items);
                    drop(reply_channel.send(Ok(())));
                }
                StorageMsg::LoadPrefix {
                    prefix,
                    reply_channel,
                    ..
                } => {
                    drop(
                        reply_channel.send(
                            records
                                .iter()
                                .filter(|(key, _)| key.starts_with(&prefix))
                                .map(|(_, value)| value.clone())
                                .collect(),
                        ),
                    );
                }
                StorageMsg::Load { key, reply_channel } => {
                    drop(reply_channel.send(records.get(&key).cloned()));
                }
                _ => panic!("unexpected storage request"),
            }
        }
    });
    let settings = wallet_settings(note.public_key);
    let (updates, _receiver) = watch::channel(None);
    let updater = StateUpdater::new(Arc::new(updates));
    let mut state = ServiceState::from_genesis(
        RecoveryState::from_settings(&settings).unwrap(),
        &settings,
        id,
        &notes,
        &[],
        [],
        [],
        &updater,
    )
    .unwrap();
    let kms = kms_api(key);
    assert!(
        state
            .wallet()
            .wallet_state_at(id)
            .unwrap()
            .owned_note(NoteSetIndex::Ledger, &note.commitment())
            .unwrap()
            .is_none()
    );
    let mut failed = state.replay_copy();
    assert!(
        TestWallet::recover_note(
            note,
            NoteSetIndex::Ledger,
            path.clone(),
            &mut failed,
            &storage,
            &chain,
            &kms,
            &era_schedule()
        )
        .await
        .is_err()
    );
    assert!(
        failed
            .wallet()
            .wallet_state_at(id)
            .unwrap()
            .owned_note(NoteSetIndex::Ledger, &note.commitment())
            .unwrap()
            .is_none()
    );
    assert!(storage.wallet_requires_historical_replay().await.unwrap());
    assert!(
        storage
            .load_wallet_known_notes()
            .await
            .unwrap()
            .contains_key(&NoteId::new(NoteSetIndex::Ledger, note.commitment()))
    );
    assert!(matches!(
        TestWallet::recover_note(
            note,
            NoteSetIndex::Ledger,
            path.clone(),
            &mut failed,
            &storage,
            &chain,
            &kms,
            &era_schedule()
        )
        .await,
        Err(WalletServiceError::HistoricalReplayRateLimited)
    ));
    fail_genesis.store(false, std::sync::atomic::Ordering::SeqCst);
    TestWallet::recover_note(
        note,
        NoteSetIndex::Ledger,
        path.clone(),
        &mut state,
        &storage,
        &chain,
        &kms,
        &era_schedule(),
    )
    .await
    .unwrap();
    let wallet = state.wallet().wallet_state_at(id).unwrap();
    assert!(
        wallet
            .owned_note(NoteSetIndex::Ledger, &note.commitment())
            .unwrap()
            .is_some()
    );
    assert_eq!(replay_count.load(std::sync::atomic::Ordering::SeqCst), 2);
    assert!(storage.wallet_requires_historical_replay().await.unwrap());
    assert!(matches!(
        TestWallet::recover_note(
            note,
            NoteSetIndex::Ledger,
            path,
            &mut state,
            &storage,
            &chain,
            &kms,
            &era_schedule(),
        )
        .await,
        Err(WalletServiceError::HistoricalReplayRateLimited)
    ));
    assert_eq!(replay_count.load(std::sync::atomic::Ordering::SeqCst), 2);
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "exercises late recovery across forks, LIB and restart"
)]
async fn late_replay_preserves_forks_spending_and_reservations_after_restart() {
    use lb_core::{
        block::genesis::GenesisBlockBuilder,
        mantle::{ops::channel::NULL_CHANNEL_ID, transactions::GenesisAllocation},
    };

    let key = ZkKey::new(Fr::from(7u64));
    let genesis = GenesisBlockBuilder::new()
        .add_note(GenesisAllocation {
            value: 100,
            public_key: key.to_public_key(),
        })
        .set_inscription(InscriptionOp {
            channel_id: NULL_CHANNEL_ID,
            inscription: Inscription::new_unchecked(genesis_parameters().encode_to_vec()),
            parent: MsgId::root(),
            signer: UnverifiedEd25519PublicKey::from_bytes(&[0; 32]).unwrap(),
            mini_blocks: MiniBlocks::default(),
        })
        .build()
        .unwrap();
    let notes = genesis.genesis_tx().initial_notes();
    let note = notes[0];
    let nullifier = derive_note_nullifier(&note.commitment(), &key.nullifier_key());
    let genesis_id = genesis.header().id();
    let common = create_block(genesis_id, 1);
    let common_id = common.header().id();
    let spent = create_block(common_id, 2);
    let spent_id = spent.header().id();
    let fork = create_block(common_id, 3);
    let fork_id = fork.header().id();
    let spend_events = Events::from(Event::Block(BlockEvent::NoteSetUpdated(NoteSetUpdate {
        note_set: NoteSetIndex::Ledger,
        nullifiers: vec![nullifier],
        commitments: vec![],
    })));
    let events = HashMap::from([
        (common_id, Bytes::try_from(Events::new()).unwrap()),
        (spent_id, Bytes::try_from(spend_events).unwrap()),
        (fork_id, Bytes::try_from(Events::new()).unwrap()),
    ]);
    let blocks = [common, spent, fork]
        .into_iter()
        .map(|block| (block.header().id(), Bytes::from(block.encode_to_vec())))
        .collect::<HashMap<_, _>>();
    let (sender, mut requests) = mpsc::channel(8);
    let storage = StorageApi::<TestTx>::new(OutboundRelay::new(sender));
    tokio::spawn(async move {
        let mut records = HashMap::new();
        while let Some(request) = requests.recv().await {
            match request {
                StorageMsg::BulkStoreConfirmed {
                    items,
                    reply_channel,
                } => {
                    records.extend(items);
                    drop(reply_channel.send(Ok(())));
                }
                StorageMsg::LoadPrefix {
                    prefix,
                    reply_channel,
                    ..
                } => {
                    drop(
                        reply_channel.send(
                            records
                                .iter()
                                .filter(|(key, _)| key.starts_with(&prefix))
                                .map(|(_, value)| value.clone())
                                .collect(),
                        ),
                    );
                }
                StorageMsg::Load { key, reply_channel } => {
                    drop(reply_channel.send(records.get(&key).cloned()));
                }
                StorageMsg::GetBlock {
                    header_id,
                    response_tx,
                } => {
                    drop(response_tx.send(blocks.get(&header_id).cloned()));
                }
                StorageMsg::GetBlockEvents {
                    header_id,
                    response_tx,
                } => {
                    drop(response_tx.send(events.get(&header_id).cloned()));
                }
                _ => panic!("unexpected storage request"),
            }
        }
    });
    let ledger = LedgerState::from_notes(
        notes.iter().copied(),
        &ledger_config(),
        &genesis_parameters(),
    );
    let (sender, mut requests) = mpsc::channel(8);
    let chain = CryptarchiaServiceApi::<DummyCryptarchia>::new(OutboundRelay::new(sender));
    tokio::spawn(async move {
        while let Some(message) = requests.recv().await {
            match message {
                ConsensusMsg::Query(Query::Info { reply_channel }) => {
                    drop(reply_channel.send(ChainServiceInfo {
                        cryptarchia_info: CryptarchiaInfo {
                            lib: common_id,
                            lib_slot: 1.into(),
                            tip: spent_id,
                            slot: 2.into(),
                            height: 2,
                            state: State::Online,
                        },
                        phase: PhaseTag::Following,
                    }));
                }
                ConsensusMsg::Query(Query::GetLedgerState { reply_channel, .. }) => {
                    drop(reply_channel.send(Some(ledger.clone())));
                }
                ConsensusMsg::Query(Query::GetGenesisBlock { reply_channel }) => {
                    drop(reply_channel.send(genesis.clone()));
                }
                ConsensusMsg::Query(Query::GetHeaders {
                    from_descendant,
                    to_ancestor,
                    reply_channel,
                }) => {
                    let descendant = from_descendant.unwrap();
                    let mut headers = vec![descendant];
                    if descendant != common_id && descendant != genesis_id {
                        headers.push(common_id);
                    }
                    if descendant != genesis_id {
                        headers.push(genesis_id);
                    }
                    if let Some(index) = headers.iter().position(|id| Some(*id) == to_ancestor) {
                        headers.truncate(index + 1);
                    }
                    drop(
                        reply_channel
                            .send(Box::pin(futures::stream::iter(headers.into_iter().map(Ok)))),
                    );
                }
                _ => panic!("unexpected chain request"),
            }
        }
    });
    let settings = wallet_settings(note.public_key);
    let (updates, checkpoints) = watch::channel(None);
    let updater = StateUpdater::new(Arc::new(updates));
    let mut state = ServiceState::from_genesis(
        RecoveryState::from_settings(&settings).unwrap(),
        &settings,
        genesis_id,
        &notes,
        &[],
        [],
        [],
        &updater,
    )
    .unwrap();
    for (id, parent, nullifiers) in [
        (common_id, genesis_id, vec![]),
        (spent_id, common_id, vec![nullifier]),
        (fork_id, common_id, vec![]),
    ] {
        state
            .apply_block(&WalletBlock {
                id,
                parent,
                epoch: 0.into(),
                updates: vec![NoteSetUpdate {
                    note_set: NoteSetIndex::Ledger,
                    nullifiers,
                    commitments: vec![],
                }],
                revealed: vec![],
                withdrawals: vec![],
            })
            .unwrap();
    }
    state.advance_lib(common_id, 1);
    state.reserve_pending_notes_in(NoteSetIndex::Ledger, [nullifier], [3; 32]);
    let (_, path) = CommitmentMmr::new()
        .push_with_paths(note.commitment(), &mut [])
        .unwrap();
    let kms = kms_api(key);
    TestWallet::recover_note(
        note,
        NoteSetIndex::Ledger,
        path,
        &mut state,
        &storage,
        &chain,
        &kms,
        &settings.era_schedule,
    )
    .await
    .unwrap();
    assert!(state.is_pending_note(NoteSetIndex::Ledger, &nullifier));
    let check_forks = |state: &ServiceState<'_>| {
        let spent = state.wallet().wallet_state_at(spent_id).unwrap();
        assert!(
            spent
                .owned_note(NoteSetIndex::Ledger, &note.commitment())
                .unwrap()
                .is_none()
        );
        let fork = state.wallet().wallet_state_at(fork_id).unwrap();
        let owned = fork
            .owned_note(NoteSetIndex::Ledger, &note.commitment())
            .unwrap()
            .unwrap();
        assert_eq!(owned.nullifier, nullifier);
        assert!(
            owned
                .path
                .verify::<ZkHasher>(note.commitment().0, fork.root_of(NoteSetIndex::Ledger))
        );
    };
    check_forks(&state);
    let checkpoint = checkpoints.borrow().clone().unwrap().to_bytes().unwrap();
    let mut restored = TestWallet::start_state(
        RecoveryState::from_bytes(&checkpoint).unwrap(),
        &settings,
        &updater,
        &chain,
        &kms,
        &storage,
    )
    .await
    .unwrap();
    TestWallet::backfill_missing_blocks(
        spent_id,
        &mut restored,
        &storage,
        &chain,
        &kms,
        &settings.era_schedule,
    )
    .await
    .unwrap();
    TestWallet::backfill_missing_blocks(
        fork_id,
        &mut restored,
        &storage,
        &chain,
        &kms,
        &settings.era_schedule,
    )
    .await
    .unwrap();
    restored.advance_lib(common_id, 1);
    check_forks(&restored);
}

#[test]
fn leader_selection_excludes_notes_without_a_staking_key() {
    let owner = ZkPublicKey::new(Fr::from(2u64));
    let staked = Note::new(25, Fr::from(1u64), owner);
    let unstaked = Note {
        nonce: Fr::from(2u64),
        stake_public_key: ZkPublicKey::zero(),
        ..staked
    };
    let known_notes = [staked, unstaked]
        .into_iter()
        .zip([1u64, 2])
        .map(|(note, nullifier)| {
            (
                NoteId::new(NoteSetIndex::Ledger, note.commitment()),
                KnownNote {
                    note,
                    note_set: NoteSetIndex::Ledger,
                    nullifier: NoteNullifier(Fr::from(nullifier)),
                },
            )
        })
        .collect();
    let genesis = HeaderId::from([0; 32]);
    let wallet = Wallet::from_genesis(
        [(owner, "owner".into())],
        known_notes,
        genesis,
        &[staked, unstaked],
        &[],
    )
    .unwrap();
    let aged = wallet
        .wallet_state_at(genesis)
        .unwrap()
        .aged_notes()
        .unwrap();
    let keys = HashMap::from([(
        LeadershipPublicKeys {
            nullifier_public_key: owner,
            staking_public_key: owner,
        },
        "owner".into(),
    )]);
    let selected = select_leader_notes(&keys, aged);
    assert_eq!(selected.len(), 1);
    assert_eq!(selected[0].note.note, staked);
    assert_eq!(wallet.balance(genesis, owner).unwrap().unwrap().balance, 50);
}

#[tokio::test]
async fn leadership_only_wallet_and_kms_build_a_verifiable_proof_with_distinct_keys() {
    assert_leadership_proof(Fr::from(7u64), Fr::from(0u64), NoteSetIndex::Ledger, false).await;
}

#[test]
fn sdp_reservation_does_not_exclude_or_release_the_matching_ledger_note() {
    use lb_core::mantle::ops::sdp::SDPWithdrawOp;

    let key = ZkKey::new(Fr::from(7u64));
    let ledger = known_note(&key, 100, 1);
    let sdp = KnownNote {
        note_set: NoteSetIndex::Sdp,
        ..ledger
    };
    let settings = wallet_settings(ledger.note.public_key);
    let (updates, _receiver) = watch::channel(None);
    let updater = StateUpdater::new(Arc::new(updates));
    let genesis = HeaderId::from([0; 32]);
    let mut state = ServiceState::from_genesis(
        RecoveryState::from_settings(&settings).unwrap(),
        &settings,
        genesis,
        &[ledger.note],
        &[sdp.note],
        [ledger, sdp],
        [],
        &updater,
    )
    .unwrap();
    let owner = [1; 32];
    state.reserve_pending_notes_in(NoteSetIndex::Sdp, [sdp.nullifier], owner);
    assert!(!state.is_pending_note(NoteSetIndex::Ledger, &ledger.nullifier));
    let builder = MantleTxBuilder::new()
        .with_reservation_id(owner)
        .push_op(Op::SDPWithdraw(SDPWithdrawOp {
            service_note_nullifier: sdp.nullifier,
            ..SDPWithdrawOp::sample()
        }))
        .unwrap()
        .add_ledger_output(Note::new(50, Fr::from(3u64), ledger.note.public_key))
        .unwrap();
    let funded = state
        .fund_tx(
            genesis,
            &builder,
            ledger.note.public_key,
            Fr::from(4u64),
            [ledger.note.public_key],
            &free_context(),
            0,
        )
        .unwrap();
    assert_eq!(funded.funding_inputs()[0].nullifier, ledger.nullifier);
    state.reserve_transaction_notes(&funded, owner);
    state.release_pending_notes_in(NoteSetIndex::Sdp, [sdp.nullifier], owner);
    assert!(state.is_pending_note(NoteSetIndex::Ledger, &ledger.nullifier));
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "Covers imports, discovery and spending separation"
)]
async fn importing_a_leadership_only_opening_does_not_add_spending_authority() {
    use lb_key_management_system_service::{
        backend::preload::PreloadKMSBackendSettings,
        keys::{LeadershipKey, kdf_public_key},
    };
    let nf_sk = Fr::from(7u64);
    let staking_sk = Fr::from(19u64);
    let public = LeadershipPublicKeys {
        nullifier_public_key: kdf_public_key(&nf_sk),
        staking_public_key: kdf_public_key(&staking_sk),
    };
    let note = Note {
        stake_public_key: public.staking_public_key,
        ..Note::new(12, Fr::from(11u64), public.nullifier_public_key)
    };
    let id = "leader".to_owned();
    let backend = KmsBackend::new(PreloadKMSBackendSettings {
        keys: HashMap::from([(
            id.clone(),
            Key::Leadership(LeadershipKey::new(nf_sk, staking_sk)),
        )]),
    });
    let (sender, mut receiver) = mpsc::channel(2);
    tokio::spawn(async move {
        while let Some(message) = receiver.recv().await {
            if let KMSMessage::Execute {
                key_id,
                operator,
                reply_channel,
            } = message
            {
                drop(reply_channel.send(backend.execute(&key_id, operator).await));
            }
        }
    });
    let kms = KmsServiceApi::<TestKms, TestRuntimeServiceId>::new(OutboundRelay::new(sender));
    let mut settings = wallet_settings(public.nullifier_public_key);
    settings.known_keys.clear();
    settings.leadership_keys.insert(id, public);
    let genesis = HeaderId::from([0; 32]);
    let (updates, _receiver) = watch::channel(None);
    let updater = StateUpdater::new(Arc::new(updates));
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
    let (storage, mut writes) = storage_api();
    let (failed, _) = complete_opening_write(
        TestWallet::import_note(note, NoteSetIndex::Ledger, &mut state, &storage, &kms),
        &mut writes,
        Err("write failed".into()),
    )
    .await;
    assert!(matches!(failed, Err(WalletServiceError::PersistedState(_))));
    assert!(state.leadership_wallet().known_notes().is_empty());
    tokio::spawn(async move {
        while let Some(request) = writes.recv().await {
            if let StorageMsg::BulkStoreConfirmed { reply_channel, .. } = request {
                drop(reply_channel.send(Ok(())));
            } else {
                panic!("unexpected storage request");
            }
        }
    });
    TestWallet::import_note(note, NoteSetIndex::Ledger, &mut state, &storage, &kms)
        .await
        .unwrap();
    TestWallet::import_note(note, NoteSetIndex::Sdp, &mut state, &storage, &kms)
        .await
        .unwrap();
    let channel = NoteSetIndex::Channel(ChannelId::from([8; 32]));
    TestWallet::import_note(note, channel, &mut state, &storage, &kms)
        .await
        .unwrap();
    // Registration has no chain-service dependency, including for repeat
    // requests. The next block discovers all three memberships normally.
    TestWallet::import_note(note, channel, &mut state, &storage, &kms)
        .await
        .unwrap();
    assert!(
        !state
            .wallet()
            .known_notes()
            .contains_key(&NoteId::new(NoteSetIndex::Ledger, note.commitment()))
    );
    assert!(
        state
            .leadership_wallet()
            .known_notes()
            .contains_key(&NoteId::new(NoteSetIndex::Ledger, note.commitment()))
    );
    let child = WalletBlock {
        id: HeaderId::from([1; 32]),
        parent: genesis,
        epoch: Epoch::new(0),
        updates: [NoteSetIndex::Ledger, NoteSetIndex::Sdp, channel]
            .into_iter()
            .map(|note_set| NoteSetUpdate {
                note_set,
                nullifiers: vec![],
                commitments: vec![note.commitment()],
            })
            .collect(),
        revealed: vec![(NoteSetIndex::Channel(ChannelId::from([8; 32])), note)],
        withdrawals: vec![],
    };
    TestWallet::add_revealed_notes(&child, &mut state, &storage, &kms)
        .await
        .unwrap();
    state.apply_block(&child).unwrap();
    assert!(
        state
            .wallet()
            .balance(child.id, note.public_key)
            .unwrap()
            .is_none()
    );
    assert_eq!(
        state
            .leadership_wallet()
            .wallet_state_at(child.id)
            .unwrap()
            .aged_notes_at(Epoch::new(2))
            .unwrap()
            .len(),
        3
    );
}

#[tokio::test]
async fn delegated_sdp_service_note_builds_a_verifiable_leadership_proof() {
    assert_leadership_proof(Fr::from(7u64), Fr::from(0u64), NoteSetIndex::Sdp, false).await;
}

#[tokio::test]
async fn program_leadership_uses_nf_sk0_without_spending_authority() {
    let nf_sk0 = ZkKey::zero().nullifier_key();
    assert_eq!(
        lb_key_management_system_service::keys::kdf_public_key(&nf_sk0),
        *PK0
    );
    assert_leadership_proof(
        nf_sk0,
        Fr::from(42u64),
        NoteSetIndex::Channel(ChannelId::from([8; 32])),
        false,
    )
    .await;
}

async fn assert_leadership_proof(nf_sk: Fr, program: Fr, note_set: NoteSetIndex, collision: bool) {
    assert_leadership_proof_with_recovery(nf_sk, program, note_set, collision, false).await;
}

#[expect(
    clippy::too_many_lines,
    clippy::cognitive_complexity,
    clippy::option_if_let_else,
    reason = "proof verification includes wallet, KMS and recovery"
)]
async fn assert_leadership_proof_with_recovery(
    nf_sk: Fr,
    program: Fr,
    note_set: NoteSetIndex,
    collision: bool,
    recovered: bool,
) {
    use lb_core::{
        mantle::note_set::NoteSets,
        proofs::leader_proof::{LeaderPublic, pol_wallet_inputs},
    };
    use lb_key_management_system_service::{
        backend::preload::PreloadKMSBackendSettings,
        keys::{LeadershipKey, kdf_public_key},
        operators::zk::leader::{BuildPrivateInputsWithLeaderKey, CheckLotteryWinning},
    };

    let staking_sk = Fr::from(19u64);
    let public = LeadershipPublicKeys {
        nullifier_public_key: kdf_public_key(&nf_sk),
        staking_public_key: kdf_public_key(&staking_sk),
    };
    let note = if note_set == NoteSetIndex::Sdp {
        let mut declaration = SDPDeclareOp::sample();
        declaration.note_pk = public.nullifier_public_key;
        declaration.note_stake_pk = public.staking_public_key;
        declaration.amount = 1000;
        declaration.service_note()
    } else {
        Note {
            stake_public_key: public.staking_public_key,
            program,
            ..Note::new(1000, Fr::from(11u64), public.nullifier_public_key)
        }
    };
    let key_id = "leadership".to_owned();
    let backend = KmsBackend::new(PreloadKMSBackendSettings {
        keys: HashMap::from([(
            key_id.clone(),
            Key::Leadership(LeadershipKey::new(nf_sk, staking_sk)),
        )]),
    });
    let (sender, mut receiver) = mpsc::channel(8);
    tokio::spawn(async move {
        while let Some(message) = receiver.recv().await {
            if let KMSMessage::Execute {
                key_id,
                operator,
                reply_channel,
            } = message
            {
                let result = backend.execute(&key_id, operator).await;
                drop(reply_channel.send(result));
            }
        }
    });
    let kms = KmsServiceApi::<TestKms, TestRuntimeServiceId>::new(OutboundRelay::new(sender));
    let known = TestWallet::leadership_known_note(&kms, key_id.clone(), note, note_set)
        .await
        .unwrap();
    let mut known_notes = vec![known];
    if collision {
        known_notes.push(
            TestWallet::leadership_known_note(&kms, key_id.clone(), note, NoteSetIndex::Ledger)
                .await
                .unwrap(),
        );
    }
    let genesis = HeaderId::from([0; 32]);
    let (updates, _receiver) = watch::channel(None);
    let updater = StateUpdater::new(Arc::new(updates));
    let mut settings = wallet_settings(public.nullifier_public_key);
    settings.known_keys.clear();
    settings.leadership_keys.insert(key_id.clone(), public);
    let ledger_notes = if note_set == NoteSetIndex::Ledger || collision {
        vec![note]
    } else {
        vec![]
    };
    let filler = Note::new(5, Fr::from(99u64), public.nullifier_public_key);
    let service_notes = if note_set == NoteSetIndex::Sdp && collision {
        vec![filler, note]
    } else if note_set == NoteSetIndex::Sdp {
        vec![note]
    } else {
        vec![]
    };
    let mut state = ServiceState::from_genesis(
        RecoveryState::from_settings(&settings).unwrap(),
        &settings,
        genesis,
        &ledger_notes,
        &service_notes,
        [],
        known_notes,
        &updater,
    )
    .unwrap();
    let mut ledger_prefix = Vec::new();
    let recovered_tip = if recovered {
        settings
            .historical_recovery
            .push(RecoveryOpening { note, note_set });
        let fixture = recovery::Fixture::new(note, note_set, Duration::ZERO);
        ledger_prefix = fixture.genesis_notes.clone();
        state = fixture
            .reconstruct(
                &settings,
                RecoveryState::from_settings(&settings).unwrap(),
                &kms,
                &updater,
            )
            .await;
        TestWallet::complete_startup_recovery(&mut state, fixture.tip, &fixture.storage)
            .await
            .unwrap();
        Some(fixture.tip)
    } else {
        None
    };
    let tip = if let Some(tip) = recovered_tip {
        tip
    } else if collision {
        assert_eq!(
            select_leader_notes(
                state.leadership_keys(),
                state
                    .leadership_wallet()
                    .wallet_state_at(genesis)
                    .unwrap()
                    .aged_notes()
                    .unwrap()
            )
            .len(),
            2
        );
        let block = WalletBlock {
            id: HeaderId::from([1; 32]),
            parent: genesis,
            epoch: Epoch::new(0),
            updates: vec![NoteSetUpdate {
                note_set: NoteSetIndex::Ledger,
                nullifiers: vec![known.nullifier],
                commitments: vec![],
            }],
            revealed: vec![],
            withdrawals: vec![],
        };
        state.apply_block(&block).unwrap();
        block.id
    } else if let NoteSetIndex::Channel(_) = note_set {
        let block = WalletBlock {
            id: HeaderId::from([1; 32]),
            parent: genesis,
            epoch: Epoch::new(0),
            updates: vec![NoteSetUpdate {
                note_set,
                nullifiers: vec![],
                commitments: vec![note.commitment()],
            }],
            revealed: vec![],
            withdrawals: vec![],
        };
        state.apply_block(&block).unwrap();
        block.id
    } else {
        genesis
    };
    assert!(
        state
            .wallet()
            .balance(tip, note.public_key)
            .unwrap()
            .is_none()
    );
    let aged = state
        .leadership_wallet()
        .wallet_state_at(tip)
        .unwrap()
        .aged_notes_at(
            if recovered || matches!(note_set, NoteSetIndex::Channel(_)) {
                Epoch::new(2)
            } else {
                Epoch::new(0)
            },
        )
        .unwrap();
    let selected = select_leader_notes(state.leadership_keys(), aged);
    assert_eq!(selected.len(), 1);

    let mut sets = NoteSets::new();
    if !ledger_prefix.is_empty() {
        let ledger = sets
            .ledger()
            .clone()
            .execute_adding(
                &ledger_prefix
                    .iter()
                    .map(Note::commitment)
                    .collect::<Vec<_>>(),
            )
            .unwrap();
        sets = sets.update_ledger(ledger);
    }
    let sets = match note_set {
        NoteSetIndex::Ledger => {
            let ledger = sets
                .ledger()
                .clone()
                .execute_adding(&[note.commitment()])
                .unwrap();
            sets.update_ledger(ledger)
        }
        NoteSetIndex::Sdp => {
            let sdp = sets
                .sdp()
                .clone()
                .execute_adding(
                    &service_notes
                        .iter()
                        .map(Note::commitment)
                        .collect::<Vec<_>>(),
                )
                .unwrap();
            let sets = sets.update_sdp(sdp);
            if collision {
                let ledger = sets
                    .ledger()
                    .clone()
                    .execute_adding(&[note.commitment()])
                    .unwrap();
                sets.update_ledger(ledger)
            } else {
                sets
            }
        }
        NoteSetIndex::Channel(channel_id) => {
            let sets = sets.with_channel(channel_id);
            let channel = sets
                .channel(&channel_id)
                .unwrap()
                .clone()
                .execute_adding(&[note.commitment()])
                .unwrap();
            sets.update_channel(channel)
        }
    }
    .end_block(1, 1024)
    .unwrap();
    if collision {
        let candidates = select_leader_notes(
            state.leadership_keys(),
            state
                .leadership_wallet()
                .wallet_state_at(genesis)
                .unwrap()
                .aged_notes()
                .unwrap(),
        );
        for candidate in &candidates {
            assert!(
                pol_wallet_inputs(
                    &note,
                    candidate.note.note_set,
                    &candidate.note.nullifier,
                    &candidate.aged_path,
                    &sets,
                    &sets
                )
                .is_ok()
            );
        }
        assert!(
            pol_wallet_inputs(
                &note,
                NoteSetIndex::Ledger,
                &known.nullifier,
                &candidates
                    .iter()
                    .find(|candidate| candidate.note.note_set == NoteSetIndex::Sdp)
                    .unwrap()
                    .aged_path,
                &sets,
                &sets
            )
            .is_err()
        );
    }
    let latest = if collision {
        let ledger = sets
            .ledger()
            .clone()
            .execute_spending(&[known.nullifier])
            .unwrap();
        sets.clone()
            .update_ledger(ledger)
            .end_block(2, 1024)
            .unwrap()
    } else {
        sets.clone()
    };
    let inputs = LeaderPublic::new(
        sets.eligible_root(),
        latest.eligible_root(),
        Fr::from(3u64),
        5,
        -Fr::from(1u64),
        Fr::from(0u64),
    );
    let (tx, rx) = oneshot::channel();
    Box::pin(kms.execute(
        key_id.clone(),
        KeyOperators::Leadership(Box::new(CheckLotteryWinning::new(tx, note, inputs))),
    ))
    .await
    .unwrap();
    assert!(rx.await.unwrap());
    let mut wallet = pol_wallet_inputs(
        &note,
        note_set,
        &known.nullifier,
        &selected[0].aged_path,
        &sets,
        &latest,
    )
    .unwrap();
    wallet.reward_public_key = *public.nullifier_public_key.as_fr();
    wallet.reward_nonce = Fr::from(23u64);
    let (tx, rx) = oneshot::channel();
    Box::pin(kms.execute(
        key_id,
        KeyOperators::Leadership(Box::new(BuildPrivateInputsWithLeaderKey::new(
            tx, inputs, wallet, 0, note,
        ))),
    ))
    .await
    .unwrap();
    let private = rx.await.unwrap();
    assert_eq!(private.input().wallet.nullifier_key, nf_sk);
    assert_eq!(private.input().wallet.staking_secret_key, staking_sk);
    let proof = Groth16LeaderProof::prove(private).unwrap();
    assert!(proof.verify(&inputs, Fr::from(0u64)));
}

#[tokio::test]
async fn identical_memberships_survive_one_set_spending_and_prove_leadership() {
    assert_leadership_proof(Fr::from(7u64), Fr::from(0u64), NoteSetIndex::Sdp, true).await;
}

#[tokio::test]
async fn unavailable_events_leave_a_notified_block_for_backfill() {
    let known_note = KnownNote {
        note: Note::new(25, Fr::from(1u64), ZkPublicKey::new(Fr::from(2u64))),
        note_set: NoteSetIndex::Ledger,
        nullifier: NoteNullifier(Fr::from(3u64)),
    };
    let settings = wallet_settings(known_note.note.public_key);
    let genesis = HeaderId::from([0; 32]);
    let (sender, _receiver) = watch::channel(None);
    let updater = StateUpdater::new(Arc::new(sender));
    let mut state = wallet_for_backfill(&settings, genesis, known_note, &updater);

    let block = create_block(genesis, 1);
    let block_id = block.header().id();
    let child = create_block(block_id, 2);
    let child_id = child.header().id();
    let commitment = known_note.note.commitment();
    let events = Events::from(Event::Block(BlockEvent::NoteSetUpdated(NoteSetUpdate {
        note_set: NoteSetIndex::Ledger,
        nullifiers: vec![],
        commitments: vec![commitment],
    })));
    let storage = storage_for_backfill([block, child], events, None);
    let cryptarchia = chain_for_backfill(genesis, block_id, child_id);
    let (kms_sender, _) = mpsc::channel(1);
    let kms = KmsServiceApi::new(OutboundRelay::new(kms_sender));

    TestWallet::handle_new_block(
        block_id,
        &mut state,
        &storage,
        &cryptarchia,
        &kms,
        &settings.era_schedule,
    )
    .await;
    assert!(!state.wallet().has_processed_block(block_id));

    // The next notification backfills the missing parent, including its
    // events.
    TestWallet::handle_new_block(
        child_id,
        &mut state,
        &storage,
        &cryptarchia,
        &kms,
        &settings.era_schedule,
    )
    .await;

    assert!(state.wallet().has_processed_block(block_id));
    assert!(state.wallet().has_processed_block(child_id));
    let wallet_state = state.wallet().wallet_state_at(child_id).unwrap();
    assert_single_note_state(&wallet_state, known_note);
}

#[tokio::test]
async fn undecodable_events_stop_backfill_until_they_can_be_loaded() {
    let known_note = KnownNote {
        note: Note::new(25, Fr::from(1u64), ZkPublicKey::new(Fr::from(2u64))),
        note_set: NoteSetIndex::Ledger,
        nullifier: NoteNullifier(Fr::from(3u64)),
    };
    let settings = wallet_settings(known_note.note.public_key);
    let genesis = HeaderId::from([0; 32]);
    let (sender, _receiver) = watch::channel(None);
    let updater = StateUpdater::new(Arc::new(sender));
    let mut state = wallet_for_backfill(&settings, genesis, known_note, &updater);

    let block = create_block(genesis, 1);
    let block_id = block.header().id();
    let child = create_block(block_id, 2);
    let child_id = child.header().id();
    let commitment = known_note.note.commitment();
    let events = Events::from(Event::Block(BlockEvent::NoteSetUpdated(NoteSetUpdate {
        note_set: NoteSetIndex::Ledger,
        nullifiers: vec![],
        commitments: vec![commitment],
    })));
    let storage = storage_for_backfill(
        [block, child],
        events,
        Some(Bytes::from_static(b"invalid events")),
    );
    let cryptarchia = chain_for_backfill(genesis, block_id, child_id);
    let (kms_sender, _) = mpsc::channel(1);
    let kms = KmsServiceApi::new(OutboundRelay::new(kms_sender));

    let result = TestWallet::backfill_missing_blocks(
        child_id,
        &mut state,
        &storage,
        &cryptarchia,
        &kms,
        &settings.era_schedule,
    )
    .await;
    assert!(matches!(
        result,
        Err(WalletServiceError::BlockEventsUnavailable(id)) if id == block_id
    ));
    assert!(!state.wallet().has_processed_block(block_id));
    assert!(!state.wallet().has_processed_block(child_id));

    TestWallet::backfill_missing_blocks(
        child_id,
        &mut state,
        &storage,
        &cryptarchia,
        &kms,
        &settings.era_schedule,
    )
    .await
    .unwrap();

    assert!(state.wallet().has_processed_block(block_id));
    assert!(state.wallet().has_processed_block(child_id));
    let wallet_state = state.wallet().wallet_state_at(child_id).unwrap();
    assert_single_note_state(&wallet_state, known_note);
}

#[tokio::test]
async fn change_waits_for_storage_and_is_spendable_after_restore() {
    // A 100-value input pays 40 to another key and returns 60 in change.
    let key = ZkKey::new(Fr::from(7u64));
    let input = known_note(&key, 100, 1);
    let settings = wallet_settings(input.note.public_key);

    let (checkpoint_sender, checkpoint_updates) = watch::channel(None);
    let updater = StateUpdater::new(Arc::new(checkpoint_sender));
    let mut state = wallet_state(&settings, &[input], &updater);

    let owner = input.note.public_key;
    let nonce = Fr::from(2u64);
    let tx = MantleTxBuilder::new()
        .add_ledger_output(Note::new(
            40,
            Fr::from(3u64),
            ZkPublicKey::new(Fr::from(4u64)),
        ))
        .unwrap();
    let funded = state
        .fund_tx(
            HeaderId::from([0; 32]),
            &tx,
            owner,
            nonce,
            [owner],
            &free_context(),
            0,
        )
        .unwrap();
    let (storage, mut writes) = storage_api();
    let kms = kms_api(key);

    // A failed atomic write must not register the change in memory.
    let expected_change = Note::new(60, nonce, owner);
    let (failed, failed_records) = complete_opening_write(
        TestWallet::add_change_note(&funded, owner, nonce, &mut state, &storage, &kms),
        &mut writes,
        Err("write failed".into()),
    )
    .await;
    assert!(matches!(failed, Err(WalletServiceError::PersistedState(_))));
    assert_eq!(failed_records.len(), 0);
    assert!(!state.wallet().known_notes().contains_key(&NoteId::new(
        NoteSetIndex::Ledger,
        expected_change.commitment()
    )));

    // Retry the save, then rebuild the wallet and spend the change.
    let (saved, records) = complete_opening_write(
        TestWallet::add_change_note(&funded, owner, nonce, &mut state, &storage, &kms),
        &mut writes,
        Ok(()),
    )
    .await;
    saved.unwrap();
    assert_eq!(
        state.wallet().known_notes()
            [&NoteId::new(NoteSetIndex::Ledger, expected_change.commitment())]
            .note,
        expected_change
    );
    assert!(checkpoint_updates.borrow().is_none());
    assert_restored_notes_are_spendable(&settings, input, &records, &funded, &kms).await;
}

#[tokio::test]
async fn split_response_waits_for_an_atomic_save_and_a_failed_write_can_be_retried() {
    let key = ZkKey::new(Fr::from(7u64));
    let input = known_note(&key, 100, 1);
    let settings = wallet_settings(input.note.public_key);

    let (checkpoint_sender, _checkpoint_updates) = watch::channel(None);
    let updater = StateUpdater::new(Arc::new(checkpoint_sender));
    let mut state = wallet_state(&settings, &[input], &updater);

    let kms = kms_api(key);

    // The entire split save fails; neither output is registered.
    let (failed, failed_records) = split_with_storage_result(
        &mut state,
        &settings,
        input.note.public_key,
        &kms,
        Err("write failed".into()),
    )
    .await;
    assert!(matches!(failed, Err(WalletServiceError::PersistedState(_))));
    assert_eq!(failed_records.len(), 0);
    assert_eq!(state.wallet().known_notes().size(), 1); // Only the original input.

    // Retry on the same wallet. Both outputs must be saved before it replies.
    let (succeeded, saved) =
        split_with_storage_result(&mut state, &settings, input.note.public_key, &kms, Ok(())).await;
    let (tx, exact) = succeeded.unwrap();
    assert_eq!(exact.note.value, 25);

    // Restore with the successful save and spend the split outputs.
    assert_restored_notes_are_spendable(&settings, input, &saved, &tx, &kms).await;
}

#[tokio::test]
async fn leader_reward_waits_for_storage_before_becoming_known() {
    let key = ZkKey::new(Fr::from(7u64));
    let input = known_note(&key, 100, 1);
    let settings = wallet_settings(input.note.public_key);

    let (checkpoint_sender, _checkpoint_updates) = watch::channel(None);
    let updater = StateUpdater::new(Arc::new(checkpoint_sender));
    let mut state = wallet_state(&settings, &[input], &updater);

    let (storage, mut writes) = storage_api();
    let kms = kms_api(key);

    let reward = Note::new(20, Fr::from(9u64), input.note.public_key);
    let (tracked, records) = complete_opening_write(
        TestWallet::track_leader_reward(reward, &mut state, &storage, &kms),
        &mut writes,
        Ok(()),
    )
    .await;
    tracked.unwrap();
    let stored = storage_with_records(records)
        .load_wallet_known_notes()
        .await
        .unwrap();
    assert_eq!(
        stored[&NoteId::new(NoteSetIndex::Ledger, reward.commitment())].note,
        reward
    );
    assert_eq!(
        state
            .wallet()
            .known_notes()
            .get(&NoteId::new(NoteSetIndex::Ledger, reward.commitment())),
        stored.get(&NoteId::new(NoteSetIndex::Ledger, reward.commitment()))
    );
}

#[tokio::test]
async fn imported_note_waits_for_storage_before_becoming_known() {
    let key = ZkKey::new(Fr::from(7u64));
    let input = known_note(&key, 100, 1);
    let settings = wallet_settings(input.note.public_key);

    let (checkpoint_sender, _checkpoint_updates) = watch::channel(None);
    let updater = StateUpdater::new(Arc::new(checkpoint_sender));
    let mut state = wallet_state(&settings, &[input], &updater);

    let (storage, mut writes) = storage_api();
    let kms = kms_api(key);

    let note = Note::new(20, Fr::from(8u64), input.note.public_key);

    let (imported, records) = complete_opening_write(
        TestWallet::import_note(note, NoteSetIndex::Ledger, &mut state, &storage, &kms),
        &mut writes,
        Ok(()),
    )
    .await;
    let imported = imported.unwrap();
    let stored = storage_with_records(records)
        .load_wallet_known_notes()
        .await
        .unwrap();
    assert_eq!(stored.get(&imported.id()), Some(&imported));
    assert_eq!(
        state.wallet().known_notes().get(&imported.id()),
        Some(&imported)
    );
}

#[tokio::test]
async fn replay_restores_finalized_spending_openings_before_rewinding() {
    let key = ZkKey::new(Fr::from(7u64));
    let input = known_note(&key, 100, 1);
    let settings = wallet_settings(input.note.public_key);
    let (checkpoint_sender, _checkpoint_updates) = watch::channel(None);
    let updater = StateUpdater::new(Arc::new(checkpoint_sender));
    let mut state = wallet_state(&settings, &[input], &updater);
    let (storage, mut writes) = storage_api();
    let (saved, records) = complete_opening_write(
        state.store_known_notes(&storage, &[input]),
        &mut writes,
        Ok(()),
    )
    .await;
    saved.unwrap();

    let genesis = HeaderId::from([0; 32]);
    state.advance_lib(genesis, 0);
    assert!(!state.wallet().known_notes().contains_key(&input.id()));

    let mut staged = state.replay_copy();
    staged
        .restore_replay_openings(&storage_with_records(records))
        .await
        .unwrap();
    staged
        .rewind_to_genesis(genesis, &[input.note], &[])
        .unwrap();
    assert_eq!(
        staged
            .wallet()
            .balance(genesis, input.note.public_key)
            .unwrap()
            .unwrap()
            .balance,
        input.note.value
    );
}

#[tokio::test]
async fn replay_restores_finalized_leadership_openings_without_spending_authority() {
    use lb_key_management_system_service::keys::LeadershipKey;

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
    let mut settings = wallet_settings(public.nullifier_public_key);
    settings.known_keys.clear();
    settings.leadership_keys.insert("leader".into(), public);
    let genesis = HeaderId::from([0; 32]);
    let (sender, _receiver) = watch::channel(None);
    let updater = StateUpdater::new(Arc::new(sender));
    let mut state = ServiceState::from_genesis(
        RecoveryState::from_settings(&settings).unwrap(),
        &settings,
        genesis,
        &[note],
        &[],
        [],
        [],
        &updater,
    )
    .unwrap();
    let (storage, mut writes) = storage_api();
    let (saved, records) = complete_opening_write(
        async {
            storage
                .store_wallet_leadership_notes(&[known])
                .await
                .map_err(WalletServiceError::PersistedState)
        },
        &mut writes,
        Ok(()),
    )
    .await;
    saved.unwrap();
    state.advance_lib(genesis, 0);
    assert!(
        !state
            .leadership_wallet()
            .known_notes()
            .contains_key(&known.id())
    );
    let mut staged = state.replay_copy();
    staged
        .restore_replay_openings(&storage_with_records(records))
        .await
        .unwrap();
    staged.rewind_to_genesis(genesis, &[note], &[]).unwrap();
    assert_eq!(
        staged
            .leadership_wallet()
            .wallet_state_at(genesis)
            .unwrap()
            .aged_notes()
            .unwrap()
            .len(),
        1
    );
    assert!(
        staged
            .wallet()
            .balance(genesis, note.public_key)
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn imported_note_batch_waits_for_each_durable_opening() {
    let key = ZkKey::new(Fr::from(7u64));
    let input = known_note(&key, 100, 1);
    let settings = wallet_settings(input.note.public_key);
    let (checkpoint_sender, _checkpoint_updates) = watch::channel(None);
    let updater = StateUpdater::new(Arc::new(checkpoint_sender));
    let mut state = wallet_state(&settings, &[input], &updater);
    let (storage, mut writes) = storage_api();
    let kms = kms_api(key);
    let notes = [
        Note::new(20, Fr::from(8u64), input.note.public_key),
        Note::new(30, Fr::from(9u64), input.note.public_key),
    ];
    let mut operation = Box::pin(TestWallet::register_notes(
        notes
            .into_iter()
            .map(|note| (note, NoteSetIndex::Ledger))
            .collect(),
        &mut state,
        &storage,
        &kms,
    ));
    let mut records = HashMap::new();
    for _ in notes {
        let message = tokio::select! {
            _ = &mut operation => panic!("returned before all opening saves"),
            message = writes.recv() => message.unwrap(),
        };
        let StorageMsg::BulkStoreConfirmed {
            items,
            reply_channel,
        } = message
        else {
            panic!("expected an atomic opening save");
        };
        assert!(poll!(operation.as_mut()).is_pending());
        records.extend(items);
        reply_channel.send(Ok(())).unwrap();
    }
    let imported = operation.await.unwrap();
    let restored = storage_with_records(records)
        .load_wallet_known_notes()
        .await
        .unwrap();
    assert_eq!(imported.len(), notes.len());
    for known in imported {
        assert_eq!(restored.get(&known.id()), Some(&known));
        assert_eq!(state.wallet().known_notes().get(&known.id()), Some(&known));
    }
}

#[tokio::test]
async fn reward_keys_wait_for_the_saved_count_and_are_registered_again_on_startup() {
    let key = ZkKey::new(Fr::from(7u64));
    let settings = wallet_settings(key.to_public_key());

    // Save the wallet before any reward key has been generated.
    let (checkpoint_sender, checkpoint_updates) = watch::channel(None);
    let updater = StateUpdater::new(Arc::new(checkpoint_sender));
    let mut state = wallet_state(&settings, &[], &updater);
    state.advance_lib(HeaderId::from([0; 32]), 0);
    let checkpoint_bytes = checkpoint_updates
        .borrow()
        .clone()
        .unwrap()
        .to_bytes()
        .unwrap();

    // Keep storage replies under the test's control.
    let (storage, mut writes) = storage_api();
    let cryptarchia = chain_api();
    let signer = TestSigner::new(kms_api(key.clone()));
    let signings = FuturesUnordered::new();

    let mut saved_count_record = None;

    // A failed write uses index 0 without returning a key. The next two keys
    // are returned only after their counts are saved, including the skipped
    // index.
    for (expected_count, write_result) in
        [(1, Err("write failed".into())), (2, Ok(())), (3, Ok(()))]
    {
        let write_succeeds = write_result.is_ok();
        let (resp_tx, mut response) = oneshot::channel();
        let era = settings.era_schedule.get(Era::genesis()).unwrap();
        let mut handle = Box::pin(TestWallet::handle_wallet_message(
            WalletMsg::GenerateRewardKey { resp_tx },
            &mut state,
            &settings.voucher_master_key_id,
            &storage,
            &cryptarchia,
            &signer,
            &signings,
            &settings.era_schedule,
            era.parameters,
        ));
        let message = tokio::select! {
            () = &mut handle => panic!("responded before the key count was written"),
            message = writes.recv() => message.unwrap(),
        };
        let StorageMsg::StoreConfirmed {
            key,
            value,
            reply_channel,
        } = message
        else {
            panic!("expected a confirmed reward-key count write");
        };
        assert_eq!(key, Bytes::from_static(b"wallet/reward_key_count"));
        assert_eq!(u64::from_bytes(&value).unwrap(), expected_count);
        assert!(poll!(handle.as_mut()).is_pending());
        assert!(matches!(response.try_recv(), Err(TryRecvError::Empty)));
        if write_succeeds {
            saved_count_record = Some((key, value));
        }
        reply_channel.send(write_result).unwrap();
        handle.await;
        let result = response.await.unwrap();
        if write_succeeds {
            assert!(state.wallet().known_keys().contains_key(&result.unwrap()));
        } else {
            assert!(matches!(result, Err(WalletServiceError::PersistedState(_))));
        }
    }

    let expected_keys = state.wallet().known_keys().clone();
    let storage = storage_with_records(HashMap::from([saved_count_record.unwrap()]));
    let fresh_kms = kms_api(key);
    let restored = TestWallet::start_state(
        RecoveryState::from_bytes(&checkpoint_bytes).unwrap(),
        &settings,
        &updater,
        &cryptarchia,
        &fresh_kms,
        &storage,
    )
    .await
    .unwrap();
    assert_eq!(restored.reward_key_count(), 3);
    assert_eq!(restored.wallet().known_keys(), &expected_keys);
}

// Storage acknowledgements and recovery assertions.

// The tests control only the batch acknowledgement. Keys and values belong to
// the storage API and are kept opaque until a restore reads them through it.
async fn complete_opening_write<Output>(
    operation: impl Future<Output = Result<Output, WalletServiceError>>,
    writes: &mut mpsc::Receiver<StorageMsg>,
    write_result: Result<(), String>,
) -> (Result<Output, WalletServiceError>, HashMap<Bytes, Bytes>) {
    let mut operation = Box::pin(operation);
    let message = tokio::select! {
        _ = &mut operation => panic!("returned before the opening save"),
        message = writes.recv() => message.unwrap(),
    };
    let StorageMsg::BulkStoreConfirmed {
        items,
        reply_channel,
    } = message
    else {
        panic!("expected an atomic opening save");
    };
    assert!(poll!(operation.as_mut()).is_pending());
    let saved = if write_result.is_ok() {
        items
    } else {
        HashMap::new()
    };
    reply_channel.send(write_result).unwrap();
    (operation.await, saved)
}

// Sends a real PrepareExactNote request. Its reply must wait for the one
// atomic save containing the openings and their ownership information.
async fn split_with_storage_result(
    state: &mut ServiceState<'_>,
    settings: &WalletServiceSettings,
    owner: ZkPublicKey,
    kms: &KmsServiceApi<TestKms, TestRuntimeServiceId>,
    write_result: Result<(), String>,
) -> (
    Result<(MantleTxBuilder, KnownNote), WalletServiceError>,
    HashMap<Bytes, Bytes>,
) {
    let (storage, mut writes) = storage_api();
    let cryptarchia = chain_api();
    let signer = TestSigner::new(kms.clone());
    let signings = FuturesUnordered::new();
    let era = settings.era_schedule.get(Era::genesis()).unwrap();
    let (resp_tx, mut response) = oneshot::channel();
    let mut handle = Box::pin(TestWallet::handle_wallet_message(
        WalletMsg::PrepareExactNote {
            tip: Some(HeaderId::from([0; 32])),
            tx_builder: MantleTxBuilder::new(),
            value: 25,
            owner,
            funding_pks: vec![owner],
            resp_tx,
        },
        state,
        &settings.voucher_master_key_id,
        &storage,
        &cryptarchia,
        &signer,
        &signings,
        &settings.era_schedule,
        era.parameters,
    ));
    let message = tokio::select! {
        () = &mut handle => panic!("responded before saving the split"),
        message = writes.recv() => message.unwrap(),
    };
    let StorageMsg::BulkStoreConfirmed {
        items,
        reply_channel,
    } = message
    else {
        panic!("expected an atomic split save");
    };
    assert!(poll!(handle.as_mut()).is_pending());
    assert!(matches!(response.try_recv(), Err(TryRecvError::Empty)));
    let saved = if write_result.is_ok() {
        items
    } else {
        HashMap::new()
    };
    reply_channel.send(write_result).unwrap();
    handle.await;
    (response.await.unwrap().map(|reply| reply.response), saved)
}

#[expect(
    clippy::too_many_lines,
    reason = "Checks both checkpoint and genesis recovery through spending"
)]
async fn assert_restored_notes_are_spendable(
    settings: &WalletServiceSettings,
    input: KnownNote,
    saved: &HashMap<Bytes, Bytes>,
    tx: &MantleTxBuilder,
    kms: &KmsServiceApi<TestKms, TestRuntimeServiceId>,
) {
    let (checkpoint_sender, checkpoint_updates) = watch::channel(None);
    let updater = StateUpdater::new(Arc::new(checkpoint_sender));
    let mut before = wallet_state(settings, &[input], &updater);
    before.advance_lib(HeaderId::from([0; 32]), 0);
    let checkpoint_bytes = checkpoint_updates
        .borrow()
        .clone()
        .unwrap()
        .to_bytes()
        .unwrap();
    drop(before);

    let (storage, mut writes) = storage_api();
    let (saved_input, mut records) = complete_opening_write(
        async {
            storage
                .store_wallet_known_notes(&[input])
                .await
                .map_err(WalletServiceError::PersistedState)
        },
        &mut writes,
        Ok(()),
    )
    .await;
    saved_input.unwrap();
    records.extend(saved.clone());
    let storage = storage_with_records(records);

    let cryptarchia = chain_api();

    // Restore once through the service's checkpoint startup, and once from
    // genesis.
    let from_checkpoint = TestWallet::start_state(
        RecoveryState::from_bytes(&checkpoint_bytes).unwrap(),
        settings,
        &updater,
        &cryptarchia,
        kms,
        &storage,
    )
    .await
    .unwrap();
    let mut recovery = RecoveryState::from_settings(settings).unwrap();
    recovery.restore_spending_records(storage.load_wallet_known_notes().await.unwrap(), 0);
    let from_genesis = ServiceState::from_genesis(
        recovery,
        settings,
        HeaderId::from([0; 32]),
        &[input.note],
        &[],
        [],
        [],
        &updater,
    )
    .unwrap();
    for mut restored in [from_checkpoint, from_genesis] {
        let block_id = HeaderId::from([1; 32]);
        let outputs = tx
            .openings()
            .into_values()
            .flat_map(|opening| opening.outputs)
            .collect::<Vec<_>>();
        restored
            .apply_block(&WalletBlock {
                id: block_id,
                parent: HeaderId::from([0; 32]),
                epoch: Epoch::new(0),
                revealed: vec![],
                withdrawals: vec![],
                updates: vec![NoteSetUpdate {
                    note_set: NoteSetIndex::Ledger,
                    nullifiers: vec![input.nullifier],
                    commitments: outputs.iter().map(Note::commitment).collect(),
                }],
            })
            .unwrap();
        let state = restored.wallet().wallet_state_at(block_id).unwrap();
        let owned_outputs = outputs
            .iter()
            .filter(|note| note.public_key == input.note.public_key)
            .collect::<Vec<_>>();
        assert_eq!(state.notes.size(), owned_outputs.len());
        for note in owned_outputs {
            let owned = state
                .notes
                .get(&NoteId::new(NoteSetIndex::Ledger, note.commitment()))
                .unwrap();
            assert_eq!(owned.note, *note);
            assert_eq!(
                owned.nullifier,
                restored.wallet().known_notes()
                    [&NoteId::new(NoteSetIndex::Ledger, note.commitment())]
                    .nullifier
            );
        }
        let funded = state
            .fund_tx(
                &MantleTxBuilder::new()
                    .add_ledger_output(Note::new(1, Fr::from(99u64), input.note.public_key))
                    .unwrap(),
                input.note.public_key,
                Fr::from(100u64),
                [input.note.public_key],
                &free_context(),
                &HashSet::new(),
                0,
            )
            .unwrap();
        assert_ne!(funded.funding_inputs(), []);
    }
}

// Service stubs: only the messages these tests use.

type TestTx = SignedOps<Preverified, StandardMode>;
type TestKms = KMSService<KmsBackend, TestRuntimeServiceId>;
type TestWallet = WalletService<TestKms, DummyCryptarchia, TestTx, (), TestRuntimeServiceId>;

struct DummyCryptarchia;

impl ServiceData for DummyCryptarchia {
    type Settings = ();
    type State = NoState<Self::Settings>;
    type StateOperator = NoOperator<Self::State>;
    type Message = ConsensusMsg<TestTx>;
}

#[derive(Clone, Copy, Debug)]
struct TestRuntimeServiceId;

impl AsServiceId<DummyCryptarchia> for TestRuntimeServiceId {
    const SERVICE_ID: Self = Self;
}

impl AsServiceId<TestKms> for TestRuntimeServiceId {
    const SERVICE_ID: Self = Self;
}

impl Display for TestRuntimeServiceId {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "TestRuntimeServiceId")
    }
}

pub fn storage_with_records(records: HashMap<Bytes, Bytes>) -> StorageApi<TestTx> {
    let (sender, mut requests) = mpsc::channel(1);
    tokio::spawn(async move {
        while let Some(request) = requests.recv().await {
            match request {
                StorageMsg::Load { key, reply_channel } => {
                    drop(reply_channel.send(records.get(&key).cloned()));
                }
                StorageMsg::LoadPrefix {
                    prefix,
                    reply_channel,
                    ..
                } => {
                    let values = records
                        .iter()
                        .filter(|(key, _)| key.starts_with(&prefix))
                        .map(|(_, value)| value.clone())
                        .collect();
                    drop(reply_channel.send(values));
                }
                _ => panic!("unexpected storage request"),
            }
        }
    });
    StorageApi::new(OutboundRelay::new(sender))
}

fn storage_api() -> (StorageApi<TestTx>, mpsc::Receiver<StorageMsg>) {
    let (sender, receiver) = mpsc::channel(1);
    (StorageApi::new(OutboundRelay::new(sender)), receiver)
}

fn chain_api() -> CryptarchiaServiceApi<DummyCryptarchia> {
    let (sender, mut receiver) = mpsc::channel(1);
    tokio::spawn(async move {
        while let Some(ConsensusMsg::Query(Query::Info { reply_channel })) = receiver.recv().await {
            let genesis = HeaderId::from([0; 32]);
            reply_channel
                .send(ChainServiceInfo {
                    cryptarchia_info: CryptarchiaInfo {
                        lib: genesis,
                        lib_slot: Slot::from(0),
                        tip: genesis,
                        slot: Slot::from(0),
                        height: 0,
                        state: State::Online,
                    },
                    phase: PhaseTag::Following,
                })
                .unwrap();
        }
    });
    CryptarchiaServiceApi::new(OutboundRelay::new(sender))
}

fn kms_api(key: ZkKey) -> KmsServiceApi<TestKms, TestRuntimeServiceId> {
    kms_api_with_keys(HashMap::from([
        ("owner".to_owned(), Key::Zk(key.clone())),
        ("voucher-master".to_owned(), Key::Zk(key)),
    ]))
}

fn kms_api_with_keys(
    mut keys: HashMap<String, Key>,
) -> KmsServiceApi<TestKms, TestRuntimeServiceId> {
    let (sender, mut receiver) = mpsc::channel::<KMSMessage<KmsBackend>>(1);
    tokio::spawn(async move {
        while let Some(message) = receiver.recv().await {
            match message {
                KMSMessage::Execute {
                    key_id,
                    operator,
                    reply_channel,
                } => {
                    reply_channel
                        .send(keys[&key_id].execute(operator).await.map_err(Into::into))
                        .unwrap();
                }
                KMSMessage::Register {
                    key_id,
                    key_type,
                    reply_channel,
                } => {
                    let public_key = key_type.as_public_key();
                    keys.insert(key_id.clone(), key_type);
                    reply_channel.send(Ok((key_id, public_key))).unwrap();
                }
                KMSMessage::PublicKey {
                    key_id,
                    reply_channel,
                } => {
                    reply_channel
                        .send(Ok(keys[&key_id].as_public_key()))
                        .unwrap();
                }
                KMSMessage::RegisterDefaultLeadership {
                    source,
                    destination,
                    reply_channel,
                } => {
                    let Key::Zk(key) = &keys[&source] else {
                        panic!("expected spending key");
                    };
                    let nf = key.nullifier_key();
                    let key = Key::Leadership(
                        lb_key_management_system_service::keys::LeadershipKey::new(nf, nf),
                    );
                    let public = key.as_public_key();
                    keys.insert(destination.clone(), key);
                    reply_channel.send(Ok((destination, public))).unwrap();
                }
                KMSMessage::Sign { .. } => panic!("unexpected KMS request"),
            }
        }
    });
    KmsServiceApi::new(OutboundRelay::new(sender))
}

// Shared note, wallet and era data.

pub fn known_note(key: &ZkKey, value: Value, nonce: u64) -> KnownNote {
    let note = Note::new(value, Fr::from(nonce), key.to_public_key());
    KnownNote {
        note,
        note_set: NoteSetIndex::Ledger,
        nullifier: derive_note_nullifier(&note.commitment(), &key.nullifier_key()),
    }
}

fn wallet_settings(owner: ZkPublicKey) -> WalletServiceSettings {
    WalletServiceSettings {
        historical_recovery: Vec::new(),
        historical_recovery_withdrawals: Vec::new(),
        known_keys: HashMap::from([("owner".into(), owner)]),
        leadership_keys: HashMap::new(),
        voucher_master_key_id: "voucher-master".into(),
        recovery_data: RecoveryData::default(),
        pending_note_expiry_blocks: 10,
        fee_margin_percent: DEFAULT_FEE_MARGIN_PERCENT,
        era_schedule: era_schedule(),
    }
}

fn wallet_state<'u>(
    settings: &WalletServiceSettings,
    notes: &[KnownNote],
    updater: &'u StateUpdater<Option<RecoveryState>>,
) -> ServiceState<'u> {
    let genesis_notes = notes.iter().map(|note| note.note).collect::<Vec<_>>();
    ServiceState::from_genesis(
        RecoveryState::from_settings(settings).unwrap(),
        settings,
        HeaderId::from([0; 32]),
        &genesis_notes,
        &[],
        notes.iter().copied(),
        [],
        updater,
    )
    .unwrap()
}

fn free_context() -> OpsContext {
    OpsContext {
        gas_context: OpsGasContext::new(HashMap::new(), GasPrices::new(0, 0), GAS_PROFILE),
    }
}

/// A schedule of one era, with epochs of 100 slots and a `k` of 5.
pub fn era_schedule() -> ServiceEraSchedule {
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

/// The costs of the deployments' current gas profile.
pub const GAS_PROFILE: GasProfile = GasProfile {
    transfer: Gas::new(10),
    program_register: Gas::new(15114),
    channel_config: Gas::new(24),
    channel_inscribe: Gas::new(26),
    channel_deposit: Gas::new(26),
    channel_withdraw: Gas::new(33),
    sdp_declare: Gas::new(1693),
    sdp_withdraw: Gas::new(0),
    sdp_active: Gas::new(15117),
    claim_pow_reward: Gas::new(1506),
    kilobyte: Gas::new(30),
    note_set: Gas::new(8961),
    proof: Gas::new(4177),
    root: Gas::new(2332),
    buffered: Gas::new(60),
    nullifier: Gas::new(1758),
    commitment: Gas::new(58),
    signature: Gas::new(557),
    user_step: Gas::new(28),
    program_deposit: Gas::new(144),
    program_step: Gas::new(13528),
    opening: Gas::new(470),
};

fn wallet_for_backfill<'u>(
    settings: &WalletServiceSettings,
    genesis: HeaderId,
    known_note: KnownNote,
    updater: &'u StateUpdater<Option<RecoveryState>>,
) -> ServiceState<'u> {
    ServiceState::from_genesis(
        RecoveryState::from_settings(settings).unwrap(),
        settings,
        genesis,
        &[],
        &[],
        [known_note],
        [],
        updater,
    )
    .unwrap()
}

fn assert_single_note_state(state: &WalletState, known_note: KnownNote) {
    let commitment = known_note.note.commitment();
    assert_eq!(state.notes.size(), 1);
    let owned_note = state
        .notes
        .get(&NoteId::new(known_note.note_set, commitment))
        .unwrap();
    assert_eq!(owned_note.note, known_note.note);
    assert_eq!(owned_note.nullifier, known_note.nullifier);
    assert_eq!(
        state.balance(known_note.note.public_key).unwrap().balance,
        known_note.note.value,
    );
    let expected = CommitmentMmr::new().push(commitment).unwrap();
    assert_eq!(
        state
            .commitments
            .get(&NoteSetIndex::Ledger)
            .unwrap()
            .frontier_root(),
        expected.frontier_root(),
    );
}

fn create_block(parent: HeaderId, slot: u64) -> Block<TestTx> {
    Block::create(
        parent,
        Slot::new(slot),
        UncleHeaders::empty(),
        Groth16LeaderProof::genesis(),
        BlockTransactions::empty(),
        u32::MAX,
    )
    .unwrap()
}

fn storage_for_backfill(
    blocks: [Block<TestTx>; 2],
    block_events: Events,
    first_event_response: Option<Bytes>,
) -> StorageApi<TestTx> {
    let [block, child] = blocks;
    let block_id = block.header().id();
    let child_id = child.header().id();
    let blocks = HashMap::from([
        (block_id, Bytes::from(block.encode_to_vec())),
        (child_id, Bytes::from(child.encode_to_vec())),
    ]);
    let events = HashMap::from([
        (block_id, Bytes::try_from(block_events).unwrap()),
        (child_id, Bytes::try_from(Events::new()).unwrap()),
    ]);
    let (msg_sender, mut msg_receiver) = mpsc::channel(1);
    tokio::spawn(async move {
        let mut fail_first_event_read = true;
        while let Some(msg) = msg_receiver.recv().await {
            match msg {
                StorageMsg::GetBlock {
                    header_id,
                    response_tx,
                } => response_tx.send(blocks.get(&header_id).cloned()).unwrap(),
                StorageMsg::GetBlockEvents {
                    header_id,
                    response_tx,
                } => {
                    let response = if header_id == block_id && fail_first_event_read {
                        fail_first_event_read = false;
                        first_event_response.clone()
                    } else {
                        events.get(&header_id).cloned()
                    };
                    response_tx.send(response).unwrap();
                }
                _ => panic!("Unexpected storage request"),
            }
        }
    });
    StorageApi::new(OutboundRelay::new(msg_sender))
}

fn chain_for_backfill(
    genesis: HeaderId,
    block: HeaderId,
    child: HeaderId,
) -> CryptarchiaServiceApi<DummyCryptarchia> {
    let (msg_sender, mut msg_receiver) = mpsc::channel(1);
    tokio::spawn(async move {
        while let Some(msg) = msg_receiver.recv().await {
            let ConsensusMsg::Query(Query::GetHeaders {
                from_descendant,
                to_ancestor,
                reply_channel,
            }) = msg
            else {
                panic!("Unexpected chain query");
            };
            assert_eq!(from_descendant, Some(child));
            assert_eq!(to_ancestor, Some(genesis));
            let headers = futures::stream::iter([Ok(child), Ok(block), Ok(genesis)]);
            assert!(reply_channel.send(Box::pin(headers)).is_ok());
        }
    });
    CryptarchiaServiceApi::new(OutboundRelay::new(msg_sender))
}

// Signings, which run off the service loop.

/// Long enough for a task that isn't held back to have got this far.
const SETTLE_TIME: Duration = Duration::from_millis(100);
const REPLY_TIMEOUT: Duration = Duration::from_secs(10);

type TestSigner = TxSigner<TestKms, TestRuntimeServiceId>;

#[tokio::test]
async fn a_signing_in_flight_leaves_the_wallet_free() {
    let updater = updater();
    let mut state = wallet_state(&wallet_settings(ZkPublicKey::new(Fr::ONE)), &[], &updater);
    let chain = chain();
    let (signer, _unserved_kms) = signer();
    let signings = FuturesUnordered::new();

    let (msg, _signed) = sign_message();
    handle(msg, &mut state, &chain, &signer, &signings).await;

    let (resp_tx, known_addresses) = oneshot::channel();
    handle(
        WalletMsg::GetKnownAddresses { resp_tx },
        &mut state,
        &chain,
        &signer,
        &signings,
    )
    .await;
    assert!(known_addresses.await.unwrap().is_ok());
}

#[tokio::test]
async fn signings_past_the_limit_wait_for_a_permit() {
    let updater = updater();
    let mut state = wallet_state(&wallet_settings(ZkPublicKey::new(Fr::ONE)), &[], &updater);
    let chain = chain();
    let (signer, mut kms) = signer();
    let mut signings = FuturesUnordered::new();

    for _ in 0..=MAX_CONCURRENT_SIGNINGS {
        let (msg, _signed) = sign_message();
        handle(msg, &mut state, &chain, &signer, &signings).await;
    }
    // The service loop's part.
    let _polling = tokio::spawn(async move { while signings.next().await.is_some() {} });

    let mut requests = Vec::new();
    for _ in 0..MAX_CONCURRENT_SIGNINGS {
        requests.push(timeout(REPLY_TIMEOUT, kms.recv()).await.unwrap());
    }
    sleep(SETTLE_TIME).await;
    assert!(timeout(SETTLE_TIME, kms.recv()).await.is_err());

    // Failing one frees its permit for the signing that was waiting.
    drop(requests.pop());
    assert!(timeout(REPLY_TIMEOUT, kms.recv()).await.unwrap().is_some());
}

#[tokio::test]
async fn a_failed_signing_gives_its_reservations_back() {
    let (signer, _unserved_kms) = signer();
    let (resp_tx, response) = oneshot::channel();

    let unused = TestWallet::signing(
        unsignable_job(),
        tip(),
        reserved_builder(),
        &signer,
        resp_tx,
    )
    .await;

    assert!(response.await.unwrap().is_err());
    assert_returned_reservation(unused);
}

#[tokio::test]
async fn an_undelivered_transaction_gives_its_reservations_back() {
    let (signer, _unserved_kms) = signer();
    let (resp_tx, response) = oneshot::channel();
    drop(response);

    let unused = TestWallet::signing(
        signing_job(MantleTxBuilder::new()),
        tip(),
        reserved_builder(),
        &signer,
        resp_tx,
    )
    .await;

    assert_returned_reservation(unused);
}

#[tokio::test]
async fn a_delivered_transaction_keeps_its_reservations() {
    let (signer, _unserved_kms) = signer();
    let (resp_tx, response) = oneshot::channel();

    let unused = TestWallet::signing(
        signing_job(MantleTxBuilder::new()),
        tip(),
        reserved_builder(),
        &signer,
        resp_tx,
    )
    .await;

    assert!(response.await.unwrap().is_ok());
    assert!(unused.is_none());
}

/// The genesis parameters of a test chain: the supply cap and the genesis gas
/// prices of the deployments.
fn genesis_parameters() -> GenesisParameters {
    GenesisParameters {
        chain_id: "test".to_owned().try_into().unwrap(),
        start_time: GenesisTime::new(0),
        epoch_nonce: Fr::ZERO,
        max_supply: 10_000_000_000_000_000_000,
        pow_reward_pool: 0,
        rewards_reserve: 1_000_000_000_000_000_000,
        storage_gas_price: NonZero::new(1_000_000_000).unwrap(),
        execution_gas_price: NonZero::new(1_000_000_000).unwrap(),
    }
}

fn ledger_config() -> lb_ledger::Config {
    let mut service_params = HashMap::new();
    service_params.insert(
        lb_core::sdp::ServiceType::BlendNetwork,
        ServiceParameters {
            inactivity_period: 2.try_into().unwrap(),
        },
    );
    let epoch_config = EpochConfig {
        epoch_stake_distribution_stabilization: 3.try_into().unwrap(),
        epoch_period_nonce_buffer: 3.try_into().unwrap(),
        epoch_period_nonce_stabilization: 4.try_into().unwrap(),
    };
    let consensus_config = lb_cryptarchia_engine::Config::new(
        5.try_into().unwrap(),
        ProperFraction::new(1, 10.try_into().unwrap()).unwrap(),
        1.0.try_into().unwrap(),
        12,
        4,
    );
    let epoch_length = epoch_config.epoch_length(consensus_config.base_period_length());

    lb_ledger::Config {
        epoch_config,
        consensus_config,
        sdp_config: lb_ledger::mantle::sdp::Config {
            service_params: Arc::new(service_params),
            service_rewards_params: ServiceRewardsParameters {
                blend: rewards::blend::RewardsParameters {
                    rounds_per_epoch: epoch_length.try_into().unwrap(),
                    message_frequency_per_round: PositiveRatio::new(1, NonZeroU32::MIN).unwrap(),
                    cover_replication_factor: 0,
                    num_blend_layers: 3.try_into().unwrap(),
                    minimum_network_size: 1.try_into().unwrap(),
                    data_replication_factor: 0,
                    activity_threshold_sensitivity: 1,
                },
            },
            min_stake: lb_core::sdp::MinStake { threshold: 1 },
        },
        max_transactions: 1024,
        max_transactions_size: 2 * 1024 * 1024,
        max_op_data_size: 2 * 1024 * 1024 * 7 / 8,
        root_window: 1024,
        channel_withdraw_delay: 86_400.into(),
        execution_market: lb_ledger::config::ExecutionMarketConfig {
            max_gas_per_block: lb_ledger::config::MaxGasPerBlock::new(3_137_798).unwrap(),
            fee_adjustment_rate: ProperFraction::new(1, 8.try_into().unwrap()).unwrap(),
            ema_smoothing_factor: ProperFraction::new(9, 10.try_into().unwrap()).unwrap(),
        },
        storage_market: lb_ledger::config::StorageMarketConfig {
            max_adjustment_factor: ProperFraction::new(1, 8.try_into().unwrap()).unwrap(),
            ema_smoothing_factor: ProperFraction::new(1, 2.try_into().unwrap()).unwrap(),
        },
        block_rewards: lb_ledger::config::BlockRewardsConfig::new(
            lb_ledger::config::BlockRewardsParameters {
                max_emission_rate: ProperFraction::new(1, 100.try_into().unwrap()).unwrap(),
                min_emission_rate: NonNegativeRatio::new(0, 1.try_into().unwrap()),
                stake_target: ProperFraction::new(3, 10.try_into().unwrap()).unwrap(),
                pooled_fees_target: NonNegativeRatio::new(1, 1.try_into().unwrap()),
                stake_deviation_weight: NonNegativeRatio::new(1, 4.try_into().unwrap()),
                pooled_fees_weight: NonNegativeRatio::new(1, 1.try_into().unwrap()),
                pooled_fees_window: 120.try_into().unwrap(),
                blend_share: ProperFraction::new(6, 10.try_into().unwrap()).unwrap(),
            },
            10_000_000_000_000_000_000,
            1.try_into().unwrap(),
            ProperFraction::new(1, 30.try_into().unwrap()).unwrap(),
        )
        .unwrap(),
        gas_profile: GAS_PROFILE,
        pow_config: PoWConfig {
            blend: BlendPoWConfig {
                base_difficulty: ModulusShift::new::<19>(),
                damping_den_offset: 0,
                damping_num: 1.try_into().unwrap(),
                max_step: 1.try_into().unwrap(),
                target_transactions_per_block: 1.try_into().unwrap(),
            },
            reward: RewardPoWConfig {
                minimum_difficulty: ModulusShift::new::<26>(),
                ema_smoothing_factor: 9,
                ema_smoothing_precision: 10.try_into().unwrap(),
                target_claims_per_block: 100.try_into().unwrap(),
                rate_num: 0,
                rate_den: NonZero::<u64>::MIN,
                pow_share: 0,
                share_den: NonZero::<u64>::MIN,
                slot_window: 100.try_into().unwrap(),
            },
        },
        accepted_fork_digests: AcceptedForkDigests::new(vec![fork_digest()]),
    }
}

/// The ledger of a chain with no notes.
fn ledger() -> LedgerState {
    LedgerState::from_notes([], &ledger_config(), &genesis_parameters())
}

fn fork_digest() -> ForkDigest {
    ForkDigest::new([0x11; 32])
}

fn tip() -> HeaderId {
    HeaderId::from([0; 32])
}

fn updater() -> StateUpdater<Option<RecoveryState>> {
    StateUpdater::new(Arc::new(watch::channel(None).0))
}

/// A signer, and the KMS relay that nothing serves unless the test does.
fn signer() -> (TestSigner, InboundRelay<KMSMessage<KmsBackend>>) {
    let Relay {
        inbound_relay,
        outbound_relay,
        ..
    } = Relay::new(16);
    (
        TestSigner::new(KmsServiceApi::new(outbound_relay)),
        inbound_relay,
    )
}

fn closed_relay<Message>() -> OutboundRelay<Message> {
    let Relay { outbound_relay, .. } = Relay::new(1);
    outbound_relay
}

/// A chain that answers what the ledger is at any block with [`ledger`], and
/// nothing else.
fn chain() -> CryptarchiaServiceApi<DummyCryptarchia> {
    let Relay {
        mut inbound_relay,
        outbound_relay,
        ..
    } = Relay::new(16);
    tokio::spawn(async move {
        while let Some(msg) = inbound_relay.recv().await {
            if let ConsensusMsg::Query(Query::GetLedgerState { reply_channel, .. }) = msg {
                assert!(reply_channel.send(Some(ledger())).is_ok());
            }
        }
    });
    CryptarchiaServiceApi::new(outbound_relay)
}

/// Handles `msg`, which leaves any signing it starts in `signings`.
async fn handle(
    msg: WalletMsg,
    state: &mut ServiceState<'_>,
    chain: &CryptarchiaServiceApi<DummyCryptarchia>,
    signer: &TestSigner,
    signings: &FuturesUnordered<PendingSigning>,
) {
    let voucher_master_key_id = "voucher-master".to_owned();
    let storage = StorageApi::new(closed_relay());
    let era_schedule = era_schedule();
    let era_settings = era_schedule.get(Era::genesis()).unwrap().parameters;
    let handled = Box::pin(TestWallet::handle_wallet_message(
        msg,
        state,
        &voucher_master_key_id,
        &storage,
        chain,
        signer,
        signings,
        &era_schedule,
        era_settings,
    ));
    timeout(REPLY_TIMEOUT, handled)
        .await
        .expect("The handler should not wait for a signature.");
}

/// An inscription, which its signer signs through the KMS.
fn inscription() -> Op {
    let signer = Ed25519Key::from_bytes(&[1; 32]).public_key();
    Op::ChannelInscribe(InscriptionOp {
        channel_id: ChannelId::from([1; 32]),
        inscription: Inscription::new_unchecked(Vec::new()),
        parent: MsgId::root(),
        signer: UnverifiedEd25519PublicKey::from_bytes(signer.as_bytes()).unwrap(),
        mini_blocks: MiniBlocks::default(),
    })
}

fn sign_message() -> (
    WalletMsg,
    oneshot::Receiver<Result<TipResponse<TestTx>, WalletServiceError>>,
) {
    let (resp_tx, resp_rx) = oneshot::channel();
    let msg = WalletMsg::SignTx {
        tip: Some(tip()),
        tx_builder: MantleTxBuilder::new().push_op(inscription()).unwrap(),
        resp_tx,
    };
    (msg, resp_rx)
}

fn signing_job(tx_builder: MantleTxBuilder) -> TxSigningRequest {
    TxSigningRequest {
        ops: tx_builder.build(fork_digest()).unwrap(),
        channel_multi_sig_proofs: HashMap::new(),
        witnesses: Vec::new(),
        ledger: ledger(),
    }
}

/// A transfer with no witness to prove it with, which signing can only refuse.
fn unsignable_job() -> TxSigningRequest {
    signing_job(
        MantleTxBuilder::new()
            .push_op(Op::Transfer(TransferOp::sample()))
            .unwrap(),
    )
}

/// The note funding a signing's transaction.
fn reserved_builder() -> MantleTxBuilder {
    MantleTxBuilder::new()
        .with_reservation_id([7; 32])
        .push_op(Op::Transfer(TransferOp::new(
            [NoteNullifier(Fr::ONE)].into(),
            [NoteCommitment(Fr::ONE)].into(),
            Fr::ONE,
            0,
        )))
        .unwrap()
}

fn assert_returned_reservation(release: Option<PendingRelease>) {
    let Some(PendingRelease::Transaction(builder)) = release else {
        panic!("expected the transaction reservation");
    };
    assert_eq!(builder.reservation_id(), Some([7; 32]));
    assert_eq!(
        builder
            .nullifiers_consumed_in(NoteSetIndex::Ledger)
            .collect::<Vec<_>>(),
        vec![NoteNullifier(Fr::ONE)]
    );
}
