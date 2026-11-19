use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use lb_core::{
    block::genesis::GenesisBlockBuilder,
    mantle::{ops::channel::NULL_CHANNEL_ID, transactions::GenesisAllocation},
};
use lb_storage_service::{
    backend::StorageBackend as _,
    rocksdb::{RocksBackend, RocksBackendSettings},
};

use super::*;

#[path = "recovery/transitions.rs"]
mod transitions;

pub(super) struct Fixture {
    pub storage: StorageApi<TestTx>,
    pub chain: CryptarchiaServiceApi<DummyCryptarchia>,
    pub tip: HeaderId,
    pub genesis: HeaderId,
    pub genesis_notes: Vec<Note>,
    pub genesis_reads: Arc<AtomicUsize>,
    pub fail_completion: Arc<AtomicBool>,
    pub fail_admission: Arc<AtomicBool>,
    storage_worker: tokio::task::JoinHandle<()>,
}

impl Fixture {
    pub fn new(note: Note, note_set: NoteSetIndex, read_delay: Duration) -> Self {
        Self::with_backend(note, note_set, read_delay, None)
    }

    fn with_backend(
        note: Note,
        note_set: NoteSetIndex,
        read_delay: Duration,
        backend: Option<RocksBackend>,
    ) -> Self {
        Self::with_history(note, note_set, read_delay, backend, None)
    }

    fn with_history(
        note: Note,
        note_set: NoteSetIndex,
        read_delay: Duration,
        backend: Option<RocksBackend>,
        spent: Option<SpentNullifier>,
    ) -> Self {
        Self::with_branch(note, note_set, read_delay, backend, spent, 1)
    }

    #[expect(
        clippy::too_many_lines,
        reason = "controlled persistent storage and chain fixture"
    )]
    fn with_branch(
        note: Note,
        note_set: NoteSetIndex,
        read_delay: Duration,
        backend: Option<RocksBackend>,
        spent: Option<SpentNullifier>,
        first_slot: u64,
    ) -> Self {
        let genesis = GenesisBlockBuilder::new()
            .add_note(GenesisAllocation {
                value: 1,
                public_key: ZkPublicKey::new(Fr::from(42u64)),
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
        let genesis_notes = genesis.genesis_tx().initial_notes();
        let genesis_id = genesis.header().id();
        let append = create_block(genesis_id, first_slot);
        let appended = append.header().id();
        let mut chain_blocks = vec![append];
        let mut parent = appended;
        // The private note is old enough both to lead and to fund a spend.
        for slot in [
            first_slot + 1,
            first_slot + 2,
            first_slot + 3,
            first_slot + 199,
        ] {
            let block = create_block(parent, slot);
            parent = block.header().id();
            chain_blocks.push(block);
        }
        let tip = parent;
        let headers = chain_blocks
            .iter()
            .rev()
            .map(|block| block.header().id())
            .chain([genesis_id])
            .collect::<Vec<_>>();
        let blocks = chain_blocks
            .into_iter()
            .map(|block| (block.header().id(), Bytes::from(block.encode_to_vec())))
            .collect::<HashMap<_, _>>();
        let mut events = blocks
            .keys()
            .map(|id| (*id, Bytes::try_from(Events::new()).unwrap()))
            .collect::<HashMap<_, _>>();
        events.extend([
            (
                appended,
                Bytes::try_from(Events::from(Event::Block(BlockEvent::NoteSetUpdated(
                    NoteSetUpdate {
                        note_set,
                        commitments: vec![note.commitment()],
                        nullifiers: vec![],
                    },
                ))))
                .unwrap(),
            ),
            (
                tip,
                Bytes::try_from(spent.map_or_else(Events::new, |spent| {
                    Events::from(Event::Block(BlockEvent::NoteSetUpdated(NoteSetUpdate {
                        note_set: spent.note_set,
                        commitments: vec![],
                        nullifiers: vec![spent.nullifier],
                    })))
                }))
                .unwrap(),
            ),
        ]);
        let fail_completion = Arc::new(AtomicBool::new(false));
        let fail_admission = Arc::new(AtomicBool::new(false));
        let fail = Arc::clone(&fail_completion);
        let fail_request = Arc::clone(&fail_admission);
        let (sender, mut requests) = mpsc::channel(8);
        let storage = StorageApi::new(OutboundRelay::new(sender));
        let storage_worker = tokio::spawn(async move {
            let mut records = HashMap::new();
            while let Some(request) = requests.recv().await {
                match request {
                    StorageMsg::BulkStoreConfirmed {
                        items,
                        reply_channel,
                    } => {
                        if fail_request.load(Ordering::SeqCst) {
                            drop(
                                reply_channel
                                    .send(Err("injected request amendment failure".into())),
                            );
                            continue;
                        }
                        if let Some(backend) = &backend {
                            backend.bulk_store(items).unwrap();
                        } else {
                            records.extend(items);
                        }
                        drop(reply_channel.send(Ok(())));
                    }
                    StorageMsg::StoreConfirmed {
                        key,
                        value,
                        reply_channel,
                    } => {
                        if fail.load(Ordering::SeqCst) {
                            drop(reply_channel.send(Err("injected checkpoint failure".into())));
                        } else {
                            if let Some(backend) = &backend {
                                backend.store(key, value).unwrap();
                            } else {
                                records.insert(key, value);
                            }
                            drop(reply_channel.send(Ok(())));
                        }
                    }
                    StorageMsg::Load { key, reply_channel } => {
                        let value = backend.as_ref().map_or_else(
                            || records.get(&key).cloned(),
                            |backend| backend.load(&key).unwrap(),
                        );
                        drop(reply_channel.send(value));
                    }
                    StorageMsg::LoadPrefix {
                        prefix,
                        reply_channel,
                        ..
                    } => {
                        if let Some(backend) = &backend {
                            drop(
                                reply_channel
                                    .send(backend.load_prefix(&prefix, None, None, None).unwrap()),
                            );
                        } else {
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
                    }
                    StorageMsg::GetBlock {
                        header_id,
                        response_tx,
                    } => {
                        sleep(read_delay).await;
                        drop(response_tx.send(blocks.get(&header_id).cloned()));
                    }
                    StorageMsg::GetBlockEvents {
                        header_id,
                        response_tx,
                    } => {
                        drop(response_tx.send(events.get(&header_id).cloned()));
                    }
                    _ => panic!("unexpected storage message"),
                }
            }
        });
        let genesis_reads = Arc::new(AtomicUsize::new(0));
        let reads = Arc::clone(&genesis_reads);
        let (sender, mut requests) = mpsc::channel(8);
        let chain = CryptarchiaServiceApi::new(OutboundRelay::new(sender));
        tokio::spawn(async move {
            while let Some(message) = requests.recv().await {
                match message {
                    ConsensusMsg::Query(Query::GetGenesisBlock { reply_channel }) => {
                        reads.fetch_add(1, Ordering::SeqCst);
                        drop(reply_channel.send(genesis.clone()));
                    }
                    ConsensusMsg::Query(Query::GetHeaders {
                        reply_channel,
                        to_ancestor,
                        ..
                    }) => {
                        let mut headers = headers.clone();
                        if let Some(index) = headers.iter().position(|id| Some(*id) == to_ancestor)
                        {
                            headers.truncate(index + 1);
                        }
                        drop(
                            reply_channel
                                .send(Box::pin(futures::stream::iter(headers.into_iter().map(Ok)))),
                        );
                    }
                    ConsensusMsg::Query(Query::Info { reply_channel }) => {
                        drop(reply_channel.send(ChainServiceInfo {
                            cryptarchia_info: CryptarchiaInfo {
                                lib: tip,
                                lib_slot: 200.into(),
                                tip,
                                slot: 200.into(),
                                height: 5,
                                state: State::Online,
                            },
                            phase: PhaseTag::Following,
                        }));
                    }
                    _ => panic!("startup recovery must not require external membership proofs"),
                }
            }
        });
        Self {
            storage,
            chain,
            tip,
            genesis: genesis_id,
            genesis_notes,
            genesis_reads,
            fail_completion,
            fail_admission,
            storage_worker,
        }
    }

    async fn close(self) {
        drop(self.storage);
        self.storage_worker.await.unwrap();
    }

    pub async fn reconstruct<'a>(
        &self,
        settings: &WalletServiceSettings,
        checkpoint: RecoveryState,
        kms: &KmsServiceApi<TestKms, TestRuntimeServiceId>,
        updater: &'a StateUpdater<Option<RecoveryState>>,
    ) -> ServiceState<'a> {
        let mut state = TestWallet::start_state(
            checkpoint,
            settings,
            updater,
            &self.chain,
            kms,
            &self.storage,
        )
        .await
        .unwrap();
        TestWallet::backfill_missing_blocks(
            self.tip,
            &mut state,
            &self.storage,
            &self.chain,
            kms,
            &settings.era_schedule,
        )
        .await
        .unwrap();
        state.advance_lib(self.tip, 0);
        state
    }
}

#[tokio::test]
async fn post_genesis_sdp_recovery_proves_leadership_without_spending() {
    assert_leadership_proof_with_recovery(7u64.into(), 0u64.into(), NoteSetIndex::Sdp, false, true)
        .await;
}

#[tokio::test]
async fn post_genesis_pk0_program_recovery_proves_leadership_without_spending() {
    assert_leadership_proof_with_recovery(
        ZkKey::zero().nullifier_key(),
        13u64.into(),
        NoteSetIndex::Channel(ChannelId::from([8; 32])),
        false,
        true,
    )
    .await;
}

#[tokio::test]
async fn post_genesis_recovery_keeps_spent_memberships_ineligible_after_checkpoint() {
    for note_set in [
        NoteSetIndex::Ledger,
        NoteSetIndex::Sdp,
        NoteSetIndex::Channel(ChannelId::from([8; 32])),
    ] {
        let key = ZkKey::new(7u64.into());
        let known = KnownNote {
            note_set,
            ..known_note(&key, 100, 7)
        };
        let spent = SpentNullifier {
            note_set,
            nullifier: known.nullifier,
        };
        let fixture =
            Fixture::with_history(known.note, note_set, Duration::ZERO, None, Some(spent));
        let mut settings = wallet_settings(known.note.public_key);
        let kms = kms_api(key.clone());
        let (_, public) = kms
            .register_default_leadership("owner".into(), "delegated".into())
            .await
            .unwrap();
        let PublicKeyEncoding::Leadership(public) = public else {
            panic!("typed leadership credentials");
        };
        settings.leadership_keys.insert("delegated".into(), public);
        settings.historical_recovery.push(RecoveryOpening {
            note: known.note,
            note_set,
        });
        let updater = updater();
        let initial = RecoveryState::from_settings(&settings).unwrap();
        let mut state = fixture
            .reconstruct(&settings, initial.clone(), &kms, &updater)
            .await;
        TestWallet::complete_startup_recovery(&mut state, fixture.tip, &fixture.storage)
            .await
            .unwrap();
        drop(state);
        let restored = fixture
            .reconstruct(&settings, initial, &kms, &updater)
            .await;
        for wallet in [restored.wallet(), restored.leadership_wallet()] {
            let snapshot = wallet.wallet_state_at(fixture.tip).unwrap();
            assert!(snapshot.spent.contains(&spent));
            assert!(!snapshot.notes.contains_key(&known.id()));
            assert_eq!(snapshot.aged_notes().unwrap().len(), 0);
        }
        assert_eq!(restored.recovery_generation, 2);
    }
}

#[tokio::test]
async fn startup_recovery_retries_failed_completion_and_uses_the_completed_checkpoint() {
    let key = ZkKey::new(Fr::from(7u64));
    let known = known_note(&key, 100, 7);
    let fixture = Fixture::new(known.note, NoteSetIndex::Ledger, Duration::ZERO);
    let mut settings = wallet_settings(known.note.public_key);
    settings.historical_recovery.push(RecoveryOpening {
        note: known.note,
        note_set: NoteSetIndex::Ledger,
    });
    let kms = kms_api(key);
    let updater = updater();
    let empty = RecoveryState::from_settings(&settings).unwrap();
    let mut state = fixture
        .reconstruct(&settings, empty.clone(), &kms, &updater)
        .await;
    // Duplicate pending requests need neither another KMS derivation nor a
    // history query, and the completed generation below remains one.
    settings
        .historical_recovery
        .push(settings.historical_recovery[0]);
    let reads_before_duplicate = fixture.genesis_reads.load(Ordering::SeqCst);
    let (_, pending) = TestWallet::prepare_startup_recovery(
        empty.clone(),
        &settings,
        &fixture.storage,
        &KmsServiceApi::new(closed_relay()),
    )
    .await
    .unwrap();
    assert!(pending);
    assert_eq!(
        fixture.genesis_reads.load(Ordering::SeqCst),
        reads_before_duplicate
    );
    fixture.fail_completion.store(true, Ordering::SeqCst);
    assert!(
        TestWallet::complete_startup_recovery(&mut state, fixture.tip, &fixture.storage)
            .await
            .is_err()
    );
    assert_eq!(state.recovery_generation, 0);
    drop(state);
    fixture.fail_completion.store(false, Ordering::SeqCst);
    let mut state = fixture
        .reconstruct(&settings, empty.clone(), &kms, &updater)
        .await;
    TestWallet::complete_startup_recovery(&mut state, fixture.tip, &fixture.storage)
        .await
        .unwrap();
    assert_eq!(state.recovery_generation, 1);
    let reads = fixture.genesis_reads.load(Ordering::SeqCst);
    drop(state);
    // Even an obsolete asynchronous snapshot must use the confirmed checkpoint.
    let restored = fixture.reconstruct(&settings, empty, &kms, &updater).await;
    assert_eq!(fixture.genesis_reads.load(Ordering::SeqCst), reads);
    assert_eq!(
        restored
            .wallet()
            .balance(fixture.tip, known.note.public_key)
            .unwrap()
            .unwrap()
            .balance,
        100
    );
}

#[tokio::test]
async fn a_later_missing_membership_remains_pending_without_undoing_the_completed_checkpoint() {
    let key = ZkKey::new(7u64.into());
    let known = known_note(&key, 100, 7);
    let fixture = Fixture::new(known.note, NoteSetIndex::Ledger, Duration::ZERO);
    let mut settings = wallet_settings(known.note.public_key);
    settings.historical_recovery.push(RecoveryOpening {
        note: known.note,
        note_set: NoteSetIndex::Ledger,
    });
    let kms = kms_api(key.clone());
    let updater = updater();
    let initial = RecoveryState::from_settings(&settings).unwrap();
    let mut state = fixture
        .reconstruct(&settings, initial.clone(), &kms, &updater)
        .await;
    TestWallet::complete_startup_recovery(&mut state, fixture.tip, &fixture.storage)
        .await
        .unwrap();
    drop(state);
    settings.historical_recovery.push(RecoveryOpening {
        note: known_note(&key, 100, 8).note,
        note_set: NoteSetIndex::Ledger,
    });
    let mut state = fixture
        .reconstruct(&settings, initial.clone(), &kms, &updater)
        .await;
    assert!(matches!(
        TestWallet::complete_startup_recovery(&mut state, fixture.tip, &fixture.storage).await,
        Err(WalletServiceError::RecoveryNoteNotFound(_))
    ));
    let (saved, pending) =
        TestWallet::prepare_startup_recovery(initial, &settings, &fixture.storage, &kms)
            .await
            .unwrap();
    assert_eq!(saved.recovery_generation, 1);
    assert!(pending);
}

#[tokio::test(start_paused = true)]
async fn startup_recovery_completes_beyond_the_former_whole_request_deadline() {
    let key = ZkKey::new(Fr::from(7u64));
    let known = known_note(&key, 100, 7);
    let fixture = Fixture::new(known.note, NoteSetIndex::Ledger, Duration::from_secs(31));
    let mut settings = wallet_settings(known.note.public_key);
    settings.historical_recovery.push(RecoveryOpening {
        note: known.note,
        note_set: NoteSetIndex::Ledger,
    });
    let kms = kms_api(key);
    let updater = updater();
    let started = tokio::time::Instant::now();
    let mut state = fixture
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
    assert!(started.elapsed() >= Duration::from_secs(62));
    assert_eq!(state.recovery_generation, 1);
}

#[tokio::test]
async fn invalid_authority_is_rejected_before_admitting_durable_recovery() {
    let key = ZkKey::new(7u64.into());
    let known = known_note(&key, 100, 7);
    let fixture = Fixture::new(known.note, NoteSetIndex::Ledger, Duration::ZERO);
    let mut settings = wallet_settings(ZkPublicKey::zero());
    settings.historical_recovery.push(RecoveryOpening {
        note: known.note,
        note_set: NoteSetIndex::Ledger,
    });
    let kms = kms_api(key);
    for _ in 0..3 {
        assert!(matches!(
            TestWallet::prepare_startup_recovery(
                RecoveryState::from_settings(&settings).unwrap(),
                &settings,
                &fixture.storage,
                &kms
            )
            .await,
            Err(WalletServiceError::UnknownKey(_))
        ));
    }
    assert_eq!(fixture.genesis_reads.load(Ordering::SeqCst), 0);
    assert!(
        fixture
            .storage
            .load_wallet_known_notes()
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        fixture
            .storage
            .load_wallet_leadership_notes()
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn public_recovery_requests_cannot_query_history_or_reserve_maintenance() {
    let key = ZkKey::new(7u64.into());
    let known = known_note(&key, 100, 7);
    let settings = wallet_settings(known.note.public_key);
    let updater = updater();
    let mut state = wallet_state(&settings, &[], &updater);
    let chain = CryptarchiaServiceApi::new(closed_relay());
    let (signer, _kms) = signer();
    let signings = FuturesUnordered::new();
    let (_, path) = CommitmentMmr::new()
        .push_with_paths(known.note.commitment(), &mut [])
        .unwrap();
    for _ in 0..3 {
        let (resp_tx, response) = oneshot::channel();
        handle(
            WalletMsg::RecoverNote {
                note: known.note,
                note_set: NoteSetIndex::Ledger,
                path: path.clone(),
                resp_tx,
            },
            &mut state,
            &chain,
            &signer,
            &signings,
        )
        .await;
        assert!(matches!(
            response.await.unwrap(),
            Err(WalletServiceError::RecoveryRequiresRestart)
        ));
    }
    assert!(state.wallet().known_notes().is_empty());
}

#[tokio::test]
async fn interrupted_startup_recovery_reopens_its_durable_request_and_checkpoint() {
    let directory = tempfile::tempdir().unwrap();
    let db = RocksBackendSettings {
        db_path: directory.path().into(),
        read_only: false,
        column_family: None,
    };
    let key = ZkKey::new(7u64.into());
    let known = known_note(&key, 100, 7);
    let mut settings = wallet_settings(known.note.public_key);
    settings.historical_recovery.push(RecoveryOpening {
        note: known.note,
        note_set: NoteSetIndex::Ledger,
    });
    let kms = kms_api(key);
    let updater = updater();
    let initial = RecoveryState::from_settings(&settings).unwrap();
    let fixture = Fixture::with_backend(
        known.note,
        NoteSetIndex::Ledger,
        Duration::ZERO,
        Some(RocksBackend::new(db.clone()).unwrap()),
    );
    let interrupted = TestWallet::start_state(
        initial.clone(),
        &settings,
        &updater,
        &fixture.chain,
        &kms,
        &fixture.storage,
    )
    .await
    .unwrap();
    assert_eq!(interrupted.recovery_generation, 0);
    drop(interrupted);
    fixture.close().await;
    let fixture = Fixture::with_backend(
        known.note,
        NoteSetIndex::Ledger,
        Duration::ZERO,
        Some(RocksBackend::new(db.clone()).unwrap()),
    );
    let mut recovered = fixture
        .reconstruct(&settings, initial.clone(), &kms, &updater)
        .await;
    TestWallet::complete_startup_recovery(&mut recovered, fixture.tip, &fixture.storage)
        .await
        .unwrap();
    drop(recovered);
    fixture.close().await;
    let fixture = Fixture::with_backend(
        known.note,
        NoteSetIndex::Ledger,
        Duration::ZERO,
        Some(RocksBackend::new(db).unwrap()),
    );
    let recovered = fixture
        .reconstruct(&settings, initial, &kms, &updater)
        .await;
    assert_eq!(recovered.recovery_generation, 1);
    assert_eq!(fixture.genesis_reads.load(Ordering::SeqCst), 0);
    assert_eq!(
        recovered
            .wallet()
            .balance(fixture.tip, known.note.public_key)
            .unwrap()
            .unwrap()
            .balance,
        100
    );
    drop(recovered);
    fixture.close().await;
}

#[tokio::test(start_paused = true)]
async fn an_unresponsive_history_read_fails_without_completing_recovery() {
    let key = ZkKey::new(7u64.into());
    let known = known_note(&key, 100, 7);
    let fixture = Fixture::new(known.note, NoteSetIndex::Ledger, Duration::from_secs(46));
    let mut settings = wallet_settings(known.note.public_key);
    settings.historical_recovery.push(RecoveryOpening {
        note: known.note,
        note_set: NoteSetIndex::Ledger,
    });
    let kms = kms_api(key);
    let updater = updater();
    let initial = RecoveryState::from_settings(&settings).unwrap();
    let mut state = TestWallet::start_state(
        initial.clone(),
        &settings,
        &updater,
        &fixture.chain,
        &kms,
        &fixture.storage,
    )
    .await
    .unwrap();
    assert!(matches!(
        TestWallet::backfill_missing_blocks(
            fixture.tip,
            &mut state,
            &fixture.storage,
            &fixture.chain,
            &kms,
            &settings.era_schedule
        )
        .await,
        Err(WalletServiceError::HistoryQueryTimeout)
    ));
    assert_eq!(state.recovery_generation, 0);
    let (_, pending) =
        TestWallet::prepare_startup_recovery(initial, &settings, &fixture.storage, &kms)
            .await
            .unwrap();
    assert!(pending);
}
