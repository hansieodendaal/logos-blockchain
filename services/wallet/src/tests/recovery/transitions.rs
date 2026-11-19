use lb_core::{
    mantle::note_set::NoteSets,
    proofs::leader_proof::{LeaderPublic, pol_wallet_inputs},
};
use lb_key_management_system_service::{
    keys::{LeadershipKey, kdf_public_key},
    operators::zk::leader::{BuildPrivateInputsWithLeaderKey, CheckLotteryWinning},
};

use super::*;

fn private_note(key: &ZkKey) -> Note {
    Note {
        stake_public_key: kdf_public_key(&Fr::from(19u64)),
        ..Note::new(1000, 11u64.into(), key.to_public_key())
    }
}

fn request(note: Note, note_set: NoteSetIndex) -> RecoveryOpening {
    RecoveryOpening { note, note_set }
}

fn database(directory: &tempfile::TempDir) -> RocksBackendSettings {
    RocksBackendSettings {
        db_path: directory.path().into(),
        read_only: false,
        column_family: None,
    }
}

async fn register_leadership(
    kms: &KmsServiceApi<TestKms, TestRuntimeServiceId>,
    key: &ZkKey,
) -> LeadershipPublicKeys {
    let (_, public) = kms
        .register(
            "leadership".into(),
            Key::Leadership(LeadershipKey::new(key.nullifier_key(), 19u64.into())),
        )
        .await
        .unwrap();
    let PublicKeyEncoding::Leadership(public) = public else {
        panic!("typed leadership credentials");
    };
    public
}

fn assert_spending(state: &ServiceState<'_>, tip: HeaderId, note: Note) {
    assert_eq!(
        state
            .wallet()
            .balance(tip, note.public_key)
            .unwrap()
            .unwrap()
            .balance,
        note.value
    );
    assert!(
        state
            .wallet()
            .spend_state(tip)
            .unwrap()
            .notes
            .contains_key(&NoteId::new(NoteSetIndex::Ledger, note.commitment()))
    );
}

async fn verify_leadership(
    fixture: &Fixture,
    state: &ServiceState<'_>,
    kms: &KmsServiceApi<TestKms, TestRuntimeServiceId>,
    note: Note,
    nf_sk: Fr,
) {
    let selected = select_leader_notes(
        state.leadership_keys(),
        state
            .leadership_wallet()
            .wallet_state_at(fixture.tip)
            .unwrap()
            .aged_notes_at(Epoch::new(2))
            .unwrap(),
    );
    assert_eq!(selected.len(), 1);
    let mut sets = NoteSets::new();
    let commitments = fixture
        .genesis_notes
        .iter()
        .chain([&note])
        .map(Note::commitment)
        .collect::<Vec<_>>();
    let ledger = sets.ledger().clone().execute_adding(&commitments).unwrap();
    sets = sets.update_ledger(ledger).end_block(1, 1024).unwrap();
    let inputs = LeaderPublic::new(
        sets.eligible_root(),
        sets.eligible_root(),
        3u64.into(),
        5,
        -Fr::from(1u64),
        Fr::ZERO,
    );
    let (tx, rx) = oneshot::channel();
    Box::pin(kms.execute(
        selected[0].key_id.clone(),
        KeyOperators::Leadership(Box::new(CheckLotteryWinning::new(tx, note, inputs))),
    ))
    .await
    .unwrap();
    assert!(rx.await.unwrap());
    let mut wallet = pol_wallet_inputs(
        &note,
        NoteSetIndex::Ledger,
        &selected[0].note.nullifier,
        &selected[0].aged_path,
        &sets,
        &sets,
    )
    .unwrap();
    wallet.reward_public_key = *note.public_key.as_fr();
    wallet.reward_nonce = 23u64.into();
    let (tx, rx) = oneshot::channel();
    Box::pin(kms.execute(
        selected[0].key_id.clone(),
        KeyOperators::Leadership(Box::new(BuildPrivateInputsWithLeaderKey::new(
            tx, inputs, wallet, 0, note,
        ))),
    ))
    .await
    .unwrap();
    let private = rx.await.unwrap();
    assert_eq!(private.input().wallet.nullifier_key, nf_sk);
    assert_eq!(private.input().wallet.staking_secret_key, Fr::from(19u64));
    assert!(
        Groth16LeaderProof::prove(private)
            .unwrap()
            .verify(&inputs, Fr::ZERO)
    );
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "authority expansion, proof verification and durable restarts"
)]
async fn adding_leadership_to_spending_recovery_proves_and_survives_restart() {
    let directory = tempfile::tempdir().unwrap();
    let db = database(&directory);
    let key = ZkKey::new(7u64.into());
    let note = private_note(&key);
    let id = NoteId::new(NoteSetIndex::Ledger, note.commitment());
    let fixture = Fixture::with_backend(
        note,
        NoteSetIndex::Ledger,
        Duration::ZERO,
        Some(RocksBackend::new(db.clone()).unwrap()),
    );
    let kms = kms_api(key.clone());
    let updater = updater();
    let mut settings = wallet_settings(note.public_key);
    settings.historical_recovery = vec![request(note, NoteSetIndex::Ledger)];
    let initial = RecoveryState::from_settings(&settings).unwrap();
    let mut spending = fixture
        .reconstruct(&settings, initial.clone(), &kms, &updater)
        .await;
    TestWallet::complete_startup_recovery(&mut spending, fixture.tip, &fixture.storage)
        .await
        .unwrap();
    assert_spending(&spending, fixture.tip, note);
    assert!(
        !spending
            .leadership_wallet()
            .wallet_state_at(fixture.tip)
            .unwrap()
            .notes
            .contains_key(&id)
    );
    let old_checkpoint = spending.snapshot();
    let public = register_leadership(&kms, &key).await;
    settings.leadership_keys.insert("leadership".into(), public);
    let (_, pending) = TestWallet::prepare_startup_recovery(
        old_checkpoint.clone(),
        &settings,
        &fixture.storage,
        &kms,
    )
    .await
    .unwrap();
    assert!(pending);
    assert!(
        fixture
            .storage
            .load_wallet_leadership_notes()
            .await
            .unwrap()
            .contains_key(&id)
    );
    // The old spending checkpoint cannot complete the new leadership
    // requirement.
    assert!(matches!(
        TestWallet::complete_startup_recovery(&mut spending, fixture.tip, &fixture.storage).await,
        Err(WalletServiceError::RecoveryNoteNotFound(_))
    ));
    assert_eq!(spending.recovery_generation, 1);
    drop(spending);
    fixture.close().await;
    // Crash after the confirmed amendment, before its completing checkpoint.
    let fixture = Fixture::with_backend(
        note,
        NoteSetIndex::Ledger,
        Duration::ZERO,
        Some(RocksBackend::new(db.clone()).unwrap()),
    );
    let mut expanded = fixture
        .reconstruct(&settings, old_checkpoint.clone(), &kms, &updater)
        .await;
    assert_spending(&expanded, fixture.tip, note);
    assert!(
        expanded
            .leadership_wallet()
            .wallet_state_at(fixture.tip)
            .unwrap()
            .notes
            .contains_key(&id)
    );
    fixture.fail_completion.store(true, Ordering::SeqCst);
    assert!(
        TestWallet::complete_startup_recovery(&mut expanded, fixture.tip, &fixture.storage)
            .await
            .is_err()
    );
    assert_eq!(expanded.recovery_generation, 1);
    drop(expanded);
    fixture.close().await;
    let fixture = Fixture::with_backend(
        note,
        NoteSetIndex::Ledger,
        Duration::ZERO,
        Some(RocksBackend::new(db.clone()).unwrap()),
    );
    let mut expanded = fixture
        .reconstruct(&settings, old_checkpoint.clone(), &kms, &updater)
        .await;
    TestWallet::complete_startup_recovery(&mut expanded, fixture.tip, &fixture.storage)
        .await
        .unwrap();
    assert_eq!(expanded.recovery_generation, 2);
    verify_leadership(&fixture, &expanded, &kms, note, key.nullifier_key()).await;
    drop(expanded);
    fixture.close().await;
    let fixture = Fixture::with_backend(
        note,
        NoteSetIndex::Ledger,
        Duration::ZERO,
        Some(RocksBackend::new(db).unwrap()),
    );
    // Both an obsolete snapshot and repeated configuration use expanded
    // coverage.
    let restored = fixture
        .reconstruct(&settings, old_checkpoint, &kms, &updater)
        .await;
    assert_eq!(restored.recovery_generation, 2);
    assert_eq!(fixture.genesis_reads.load(Ordering::SeqCst), 0);
    assert_spending(&restored, fixture.tip, note);
    assert!(
        restored
            .leadership_wallet()
            .wallet_state_at(fixture.tip)
            .unwrap()
            .notes
            .contains_key(&id)
    );
    drop(restored);
    fixture.close().await;
}

#[tokio::test]
async fn adding_spending_to_leadership_recovery_preserves_both_namespaces() {
    let key = ZkKey::new(7u64.into());
    let note = private_note(&key);
    let id = NoteId::new(NoteSetIndex::Ledger, note.commitment());
    let fixture = Fixture::new(note, NoteSetIndex::Ledger, Duration::ZERO);
    let kms = kms_api_with_keys(HashMap::from([(
        "leadership".into(),
        Key::Leadership(LeadershipKey::new(key.nullifier_key(), 19u64.into())),
    )]));
    let updater = updater();
    let mut settings = wallet_settings(note.public_key);
    settings.known_keys.clear();
    settings.leadership_keys.insert(
        "leadership".into(),
        LeadershipPublicKeys {
            nullifier_public_key: note.public_key,
            staking_public_key: note.stake_public_key,
        },
    );
    settings.historical_recovery = vec![request(note, NoteSetIndex::Ledger)];
    let initial = RecoveryState::from_settings(&settings).unwrap();
    let mut leader = fixture
        .reconstruct(&settings, initial, &kms, &updater)
        .await;
    TestWallet::complete_startup_recovery(&mut leader, fixture.tip, &fixture.storage)
        .await
        .unwrap();
    assert!(
        leader
            .wallet()
            .balance(fixture.tip, note.public_key)
            .unwrap()
            .is_none()
    );
    assert!(
        leader
            .wallet()
            .spend_state(fixture.tip)
            .unwrap()
            .notes
            .is_empty()
    );
    let before = leader.snapshot();
    drop(leader);
    kms.register("owner".into(), Key::Zk(key)).await.unwrap();
    settings.known_keys.insert("owner".into(), note.public_key);
    let mut expanded = fixture
        .reconstruct(&settings, before.clone(), &kms, &updater)
        .await;
    assert_eq!(expanded.recovery_generation, 1);
    assert!(
        fixture
            .storage
            .load_wallet_known_notes()
            .await
            .unwrap()
            .contains_key(&id)
    );
    TestWallet::complete_startup_recovery(&mut expanded, fixture.tip, &fixture.storage)
        .await
        .unwrap();
    assert_spending(&expanded, fixture.tip, note);
    assert!(
        expanded
            .leadership_wallet()
            .wallet_state_at(fixture.tip)
            .unwrap()
            .notes
            .contains_key(&id)
    );
    drop(expanded);
    let reads = fixture.genesis_reads.load(Ordering::SeqCst);
    let restored = fixture.reconstruct(&settings, before, &kms, &updater).await;
    assert_eq!(fixture.genesis_reads.load(Ordering::SeqCst), reads);
    assert_eq!(restored.recovery_generation, 2);
    assert_spending(&restored, fixture.tip, note);
}

#[tokio::test]
async fn mismatched_leadership_cannot_admit_an_authority_expansion() {
    let key = ZkKey::new(7u64.into());
    let note = private_note(&key);
    let fixture = Fixture::new(note, NoteSetIndex::Ledger, Duration::ZERO);
    let kms = kms_api(key);
    let updater = updater();
    let mut settings = wallet_settings(note.public_key);
    settings.historical_recovery = vec![request(note, NoteSetIndex::Ledger)];
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
    let checkpoint = state.snapshot();
    drop(state);
    kms.register(
        "leadership".into(),
        Key::Leadership(LeadershipKey::new(99u64.into(), 19u64.into())),
    )
    .await
    .unwrap();
    settings.leadership_keys.insert(
        "leadership".into(),
        LeadershipPublicKeys {
            nullifier_public_key: note.public_key,
            staking_public_key: note.stake_public_key,
        },
    );
    assert!(
        TestWallet::prepare_startup_recovery(checkpoint.clone(), &settings, &fixture.storage, &kms)
            .await
            .is_err()
    );
    assert!(
        fixture
            .storage
            .load_wallet_leadership_notes()
            .await
            .unwrap()
            .is_empty()
    );
    settings.leadership_keys.clear();
    let (saved, pending) =
        TestWallet::prepare_startup_recovery(checkpoint, &settings, &fixture.storage, &kms)
            .await
            .unwrap();
    assert!(!pending);
    assert_eq!(saved.recovery_generation, 1);
}

#[tokio::test]
async fn mismatched_spending_cannot_admit_an_authority_expansion() {
    let key = ZkKey::new(7u64.into());
    let note = private_note(&key);
    let fixture = Fixture::new(note, NoteSetIndex::Ledger, Duration::ZERO);
    let kms = kms_api(key.clone());
    let updater = updater();
    let mut settings = wallet_settings(note.public_key);
    settings.known_keys.clear();
    settings
        .leadership_keys
        .insert("leadership".into(), register_leadership(&kms, &key).await);
    settings.historical_recovery = vec![request(note, NoteSetIndex::Ledger)];
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
    let checkpoint = state.snapshot();
    drop(state);
    kms.register("owner".into(), Key::Zk(ZkKey::new(99u64.into())))
        .await
        .unwrap();
    settings.known_keys.insert("owner".into(), note.public_key);
    assert!(matches!(
        TestWallet::prepare_startup_recovery(checkpoint.clone(), &settings, &fixture.storage, &kms)
            .await,
        Err(WalletServiceError::UnknownKey(_))
    ));
    assert!(
        fixture
            .storage
            .load_wallet_known_notes()
            .await
            .unwrap()
            .is_empty()
    );
    settings.known_keys.clear();
    let (saved, pending) =
        TestWallet::prepare_startup_recovery(checkpoint, &settings, &fixture.storage, &kms)
            .await
            .unwrap();
    assert!(!pending);
    assert_eq!(saved.recovery_generation, 1);
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "withdrawal validation, persistence failure and durable restart"
)]
async fn withdrawing_failed_work_is_durable_and_reuses_the_completed_checkpoint() {
    let directory = tempfile::tempdir().unwrap();
    let db = database(&directory);
    let key = ZkKey::new(7u64.into());
    let note = private_note(&key);
    let fixture = Fixture::with_backend(
        note,
        NoteSetIndex::Ledger,
        Duration::ZERO,
        Some(RocksBackend::new(db.clone()).unwrap()),
    );
    let kms = kms_api(key);
    let updater = updater();
    let mut settings = wallet_settings(note.public_key);
    settings.historical_recovery = vec![request(note, NoteSetIndex::Ledger)];
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
    let checkpoint = state.snapshot();
    drop(state);
    settings.historical_recovery = vec![request(note, NoteSetIndex::Sdp)];
    let mut invalid = fixture
        .reconstruct(&settings, checkpoint.clone(), &kms, &updater)
        .await;
    assert!(matches!(
        TestWallet::complete_startup_recovery(&mut invalid, fixture.tip, &fixture.storage).await,
        Err(WalletServiceError::RecoveryNoteNotFound(id)) if id.note_set == NoteSetIndex::Sdp
    ));
    drop(invalid);
    settings.historical_recovery_withdrawals = vec![2];
    assert!(matches!(
        TestWallet::prepare_startup_recovery(checkpoint.clone(), &settings, &fixture.storage, &kms)
            .await,
        Err(WalletServiceError::RecoveryWithdrawalConflict(_))
    ));
    settings.historical_recovery_withdrawals.clear();
    settings.historical_recovery.clear();
    // Omission is not cancellation of the durable pending request (ID 2).
    assert!(
        TestWallet::prepare_startup_recovery(checkpoint.clone(), &settings, &fixture.storage, &kms)
            .await
            .unwrap()
            .1
    );
    settings.historical_recovery_withdrawals = vec![2, 999];
    assert!(matches!(
        TestWallet::prepare_startup_recovery(checkpoint.clone(), &settings, &fixture.storage, &kms)
            .await,
        Err(WalletServiceError::RecoveryWithdrawalNotPending(999))
    ));
    settings.historical_recovery_withdrawals.clear();
    assert!(
        TestWallet::prepare_startup_recovery(checkpoint.clone(), &settings, &fixture.storage, &kms)
            .await
            .unwrap()
            .1
    );
    settings.historical_recovery_withdrawals = vec![2];
    fixture.fail_admission.store(true, Ordering::SeqCst);
    assert!(
        TestWallet::prepare_startup_recovery(checkpoint.clone(), &settings, &fixture.storage, &kms)
            .await
            .is_err()
    );
    fixture.fail_admission.store(false, Ordering::SeqCst);
    settings.historical_recovery_withdrawals.clear();
    fixture.close().await;
    let fixture = Fixture::with_backend(
        note,
        NoteSetIndex::Ledger,
        Duration::ZERO,
        Some(RocksBackend::new(db.clone()).unwrap()),
    );
    assert!(
        TestWallet::prepare_startup_recovery(checkpoint.clone(), &settings, &fixture.storage, &kms)
            .await
            .unwrap()
            .1
    );
    settings.historical_recovery_withdrawals = vec![2];
    let (restored, pending) =
        TestWallet::prepare_startup_recovery(checkpoint.clone(), &settings, &fixture.storage, &kms)
            .await
            .unwrap();
    assert!(!pending);
    assert_eq!(restored.recovery_generation, 1);
    // Withdrawal cannot be confused with reconstruction, or redo itself.
    assert!(
        !TestWallet::prepare_startup_recovery(
            checkpoint.clone(),
            &settings,
            &fixture.storage,
            &kms
        )
        .await
        .unwrap()
        .1
    );
    fixture.close().await;
    let fixture = Fixture::with_backend(
        note,
        NoteSetIndex::Ledger,
        Duration::ZERO,
        Some(RocksBackend::new(db).unwrap()),
    );
    settings.historical_recovery_withdrawals.clear();
    let mut restored = fixture
        .reconstruct(
            &settings,
            RecoveryState::from_settings(&settings).unwrap(),
            &kms,
            &updater,
        )
        .await;
    TestWallet::complete_startup_recovery(&mut restored, fixture.tip, &fixture.storage)
        .await
        .unwrap();
    assert_eq!(restored.recovery_generation, 1);
    assert_eq!(fixture.genesis_reads.load(Ordering::SeqCst), 0);
    assert_spending(&restored, fixture.tip, note);
    assert!(
        fixture
            .storage
            .load_wallet_known_notes()
            .await
            .unwrap()
            .contains_key(&NoteId::new(NoteSetIndex::Sdp, note.commitment()))
    );
    settings.historical_recovery_withdrawals = vec![1];
    assert!(matches!(
        TestWallet::prepare_startup_recovery(checkpoint, &settings, &fixture.storage, &kms).await,
        Err(WalletServiceError::RecoveryWithdrawalNotPending(1))
    ));
    drop(restored);
    fixture.close().await;
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "atomic correction preserves unrelated work across durable restart"
)]
async fn correcting_one_request_preserves_unrelated_pending_work_across_restart() {
    let directory = tempfile::tempdir().unwrap();
    let db = database(&directory);
    let key = ZkKey::new(7u64.into());
    let note = private_note(&key);
    let channel = NoteSetIndex::Channel(ChannelId::from([8; 32]));
    let fixture = Fixture::with_backend(
        note,
        NoteSetIndex::Ledger,
        Duration::ZERO,
        Some(RocksBackend::new(db.clone()).unwrap()),
    );
    let kms = kms_api(key.clone());
    let updater = updater();
    let mut settings = wallet_settings(note.public_key);
    settings.historical_recovery = vec![request(note, NoteSetIndex::Ledger)];
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
    let checkpoint = state.snapshot();
    drop(state);
    // ID 2 is the mistaken SDP membership; ID 3 is unrelated pending work.
    settings.historical_recovery = vec![request(note, NoteSetIndex::Sdp), request(note, channel)];
    let mut invalid = fixture
        .reconstruct(&settings, checkpoint.clone(), &kms, &updater)
        .await;
    assert!(matches!(
        TestWallet::complete_startup_recovery(&mut invalid, fixture.tip, &fixture.storage).await,
        Err(WalletServiceError::RecoveryNoteNotFound(id)) if id.note_set == NoteSetIndex::Sdp
    ));
    drop(invalid);
    let public = register_leadership(&kms, &key).await;
    settings.leadership_keys.insert("leadership".into(), public);
    settings.historical_recovery = vec![request(note, NoteSetIndex::Ledger)];
    settings.historical_recovery_withdrawals = vec![2];
    fixture.fail_admission.store(true, Ordering::SeqCst);
    assert!(
        TestWallet::prepare_startup_recovery(checkpoint.clone(), &settings, &fixture.storage, &kms)
            .await
            .is_err()
    );
    assert!(
        fixture
            .storage
            .load_wallet_leadership_notes()
            .await
            .unwrap()
            .is_empty()
    );
    fixture.fail_admission.store(false, Ordering::SeqCst);
    fixture.close().await;
    let fixture = Fixture::with_backend(
        note,
        NoteSetIndex::Ledger,
        Duration::ZERO,
        Some(RocksBackend::new(db.clone()).unwrap()),
    );
    let mut unchanged = settings.clone();
    unchanged.historical_recovery.clear();
    unchanged.historical_recovery_withdrawals.clear();
    unchanged.leadership_keys.clear();
    let mut before_amendment = fixture
        .reconstruct(&unchanged, checkpoint.clone(), &kms, &updater)
        .await;
    assert!(matches!(
        TestWallet::complete_startup_recovery(&mut before_amendment, fixture.tip, &fixture.storage).await,
        Err(WalletServiceError::RecoveryNoteNotFound(id)) if id.note_set == NoteSetIndex::Sdp
    ));
    drop(before_amendment);
    assert!(
        TestWallet::prepare_startup_recovery(checkpoint.clone(), &settings, &fixture.storage, &kms)
            .await
            .unwrap()
            .1
    );
    fixture.close().await;
    let fixture = Fixture::with_backend(
        note,
        NoteSetIndex::Ledger,
        Duration::ZERO,
        Some(RocksBackend::new(db.clone()).unwrap()),
    );
    settings.historical_recovery.clear();
    settings.historical_recovery_withdrawals.clear();
    let mut restored = fixture
        .reconstruct(&settings, checkpoint.clone(), &kms, &updater)
        .await;
    // The corrected SDP request is gone; the unrelated channel request is not.
    assert!(matches!(
        TestWallet::complete_startup_recovery(&mut restored, fixture.tip, &fixture.storage).await,
        Err(WalletServiceError::RecoveryNoteNotFound(id)) if id.note_set == channel
    ));
    assert_eq!(restored.recovery_generation, 1);
    drop(restored);
    settings.historical_recovery_withdrawals = vec![3];
    let mut corrected = fixture
        .reconstruct(&settings, checkpoint.clone(), &kms, &updater)
        .await;
    TestWallet::complete_startup_recovery(&mut corrected, fixture.tip, &fixture.storage)
        .await
        .unwrap();
    assert_eq!(corrected.recovery_generation, 6);
    assert_spending(&corrected, fixture.tip, note);
    assert!(
        corrected
            .leadership_wallet()
            .wallet_state_at(fixture.tip)
            .unwrap()
            .notes
            .contains_key(&NoteId::new(NoteSetIndex::Ledger, note.commitment()))
    );
    drop(corrected);
    fixture.close().await;
    let fixture = Fixture::with_backend(
        note,
        NoteSetIndex::Ledger,
        Duration::ZERO,
        Some(RocksBackend::new(db).unwrap()),
    );
    let restored = fixture
        .reconstruct(&settings, checkpoint, &kms, &updater)
        .await;
    assert_eq!(restored.recovery_generation, 6);
    assert_eq!(fixture.genesis_reads.load(Ordering::SeqCst), 0);
    assert_spending(&restored, fixture.tip, note);
    drop(restored);
    fixture.close().await;
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "mutable fork recovery and confirmed checkpoint restart"
)]
async fn an_orphaned_completed_membership_does_not_block_recovery_on_the_new_fork() {
    let directory = tempfile::tempdir().unwrap();
    let db = database(&directory);
    let key = ZkKey::new(7u64.into());
    let orphaned = private_note(&key);
    let replacement = Note {
        nonce: 12u64.into(),
        value: 200,
        ..orphaned
    };
    let orphaned_id = NoteId::new(NoteSetIndex::Ledger, orphaned.commitment());
    let fixture = Fixture::with_backend(
        orphaned,
        NoteSetIndex::Ledger,
        Duration::ZERO,
        Some(RocksBackend::new(db.clone()).unwrap()),
    );
    let kms = kms_api(key.clone());
    let updater = updater();
    let mut settings = wallet_settings(orphaned.public_key);
    settings
        .leadership_keys
        .insert("leadership".into(), register_leadership(&kms, &key).await);
    settings.historical_recovery = vec![request(orphaned, NoteSetIndex::Ledger)];
    let mut state = TestWallet::start_state(
        RecoveryState::from_settings(&settings).unwrap(),
        &settings,
        &updater,
        &fixture.chain,
        &kms,
        &fixture.storage,
    )
    .await
    .unwrap();
    TestWallet::backfill_missing_blocks(
        fixture.tip,
        &mut state,
        &fixture.storage,
        &fixture.chain,
        &kms,
        &settings.era_schedule,
    )
    .await
    .unwrap();
    assert_eq!(state.lib(), fixture.genesis);
    TestWallet::complete_startup_recovery(&mut state, fixture.tip, &fixture.storage)
        .await
        .unwrap();
    let checkpoint = state.snapshot();
    let genesis = fixture.genesis;
    let old_tip = fixture.tip;
    drop(state);
    fixture.close().await;
    // A distinct child of the same genesis replaces the mutable fork.
    let fixture = Fixture::with_branch(
        replacement,
        NoteSetIndex::Ledger,
        Duration::ZERO,
        Some(RocksBackend::new(db.clone()).unwrap()),
        None,
        2,
    );
    assert_eq!(fixture.genesis, genesis);
    assert_ne!(fixture.tip, old_tip);
    settings.historical_recovery = vec![request(replacement, NoteSetIndex::Ledger)];
    let mut state = fixture
        .reconstruct(&settings, checkpoint.clone(), &kms, &updater)
        .await;
    for wallet in [state.wallet(), state.leadership_wallet()] {
        assert!(
            !wallet
                .wallet_state_at(fixture.tip)
                .unwrap()
                .notes
                .contains_key(&orphaned_id)
        );
    }
    TestWallet::complete_startup_recovery(&mut state, fixture.tip, &fixture.storage)
        .await
        .unwrap();
    assert_eq!(state.recovery_generation, 4);
    assert_spending(&state, fixture.tip, replacement);
    let leaders = select_leader_notes(
        state.leadership_keys(),
        state
            .leadership_wallet()
            .wallet_state_at(fixture.tip)
            .unwrap()
            .aged_notes()
            .unwrap(),
    );
    assert_eq!(leaders.len(), 1);
    assert_eq!(leaders[0].note.note, replacement);
    assert!(
        fixture
            .storage
            .load_wallet_leadership_notes()
            .await
            .unwrap()
            .contains_key(&orphaned_id)
    );
    drop(state);
    fixture.close().await;
    let fixture = Fixture::with_branch(
        replacement,
        NoteSetIndex::Ledger,
        Duration::ZERO,
        Some(RocksBackend::new(db).unwrap()),
        None,
        2,
    );
    let state = fixture
        .reconstruct(&settings, checkpoint, &kms, &updater)
        .await;
    assert_eq!(fixture.genesis_reads.load(Ordering::SeqCst), 0);
    assert!(
        !state
            .leadership_wallet()
            .wallet_state_at(fixture.tip)
            .unwrap()
            .notes
            .contains_key(&orphaned_id)
    );
    drop(state);
    fixture.close().await;
}
