use std::sync::{
    Mutex,
    atomic::{AtomicBool, AtomicUsize},
};

use axum::{
    Json, Router,
    extract::{Path, State},
    routing::{get, post},
};
use lb_core::{
    era::ForkDigest,
    mantle::{
        Note, SignedOps,
        ledger::verification_mode::StandardMode,
        transactions::{Ops, TxList, states::Unverified},
    },
};
use lb_groth16::Fr;
use serde_json::Value;

use super::*;

struct Node {
    stall: AtomicBool,
    confirm: AtomicBool,
    immutable: AtomicBool,
    canonical: AtomicBool,
    canonical_checks: AtomicUsize,
    preparations: AtomicUsize,
    reads: AtomicUsize,
    submissions: Mutex<Vec<Value>>,
    depth: u8,
    delay: Duration,
}

fn wallet() -> WalletBalanceResponseBody {
    WalletBalanceResponseBody {
        tip: HeaderId::from([1; 32]),
        balance: 100,
        notes: HashMap::new(),
        pending_notes: HashSet::new(),
        address: ZkPublicKey::new(Fr::from(2u64)),
    }
}

fn prepared(index: u8) -> WalletPrepareTransferResponseBody {
    let ops = Ops::sample().iter().cloned().collect::<Vec<_>>();
    let transaction = SignedOps::<Unverified, StandardMode>::from_ops_with_sample_proofs(Ops::new(
        ForkDigest::new([index; 32]),
        TxList::new_unchecked(ops),
    ));
    WalletPrepareTransferResponseBody {
        hash: transaction.hash(),
        transaction,
        recipient_note: Note::new(
            10,
            Fr::from(u64::from(index)),
            ZkPublicKey::new(Fr::from(3u64)),
        ),
    }
}

async fn node(depth: u8, delay: Duration) -> (Url, Arc<Node>) {
    let node = Arc::new(Node {
        stall: AtomicBool::new(false),
        confirm: AtomicBool::new(false),
        immutable: AtomicBool::new(true),
        canonical: AtomicBool::new(true),
        canonical_checks: AtomicUsize::new(0),
        preparations: AtomicUsize::new(0),
        reads: AtomicUsize::new(0),
        submissions: Mutex::new(vec![]),
        depth,
        delay,
    });
    let app = Router::new()
        .route("/wallet/transactions/prepare-transfer-funds", post(async |State(node): State<Arc<Node>>| {
            Json(serde_json::to_value(prepared(node.preparations.fetch_add(1, Ordering::SeqCst) as u8 + 1)).unwrap())
        }))
        .route("/wallet/{pk}/balance", get(async || Json(serde_json::to_value(wallet()).unwrap())))
        .route("/mempool/add/tx", post(async |State(node): State<Arc<Node>>, Json(tx): Json<Value>| {
            node.submissions.lock().unwrap().push(tx);
            if node.stall.load(Ordering::SeqCst) { std::future::pending::<()>().await; }
            Json(())
        }))
        .route("/cryptarchia/info", get(async |State(node): State<Arc<Node>>| {
            let tip = HeaderId::from([node.depth; 32]);
            Json(serde_json::to_value(lb_common_http_client::ChainServiceInfo {
                cryptarchia_info: lb_common_http_client::CryptarchiaInfo { lib: if node.immutable.load(Ordering::SeqCst) { tip } else { HeaderId::from([0; 32]) }, lib_slot: 1.into(),
                    tip, slot: 1.into(), height: u64::from(node.depth), state: lb_common_http_client::State::Online },
                phase: lb_common_http_client::PhaseTag::Following }).unwrap())
        }))
        .route("/cryptarchia/headers", get(async |State(node): State<Arc<Node>>| {
            node.canonical_checks.fetch_add(1, Ordering::SeqCst);
            Json(if node.canonical.load(Ordering::SeqCst) {
                // Only the pinned tip is in this bounded response; the
                // payment's older block is deliberately absent.
                vec![HeaderId::from([node.depth; 32])]
            } else {
                vec![HeaderId::from([99; 32])]
            })
        }))
        .route("/cryptarchia/blocks/{id}", get(async |State(node): State<Arc<Node>>, Path(id): Path<String>| {
            node.reads.fetch_add(1, Ordering::SeqCst);
            tokio::time::sleep(node.delay).await;
            let index = hex::decode(id.trim_start_matches("0x")).unwrap()[0];
            let txs = if index == 1 && node.confirm.load(Ordering::SeqCst) { node.submissions.lock().unwrap().clone() } else { vec![] };
            Json(serde_json::json!({ "header": { "id": HeaderId::from([index; 32]),
                "parent_block": HeaderId::from([index - 1; 32]), "slot": lb_common_http_client::Slot::from(1),
                "body_root": lb_core::header::ContentId::from([0; 32]),
                "proof_of_leadership": lb_core::proofs::leader_proof::Groth16LeaderProof::genesis() },
                "uncle_headers": [], "transactions": txs }))
        })).with_state(Arc::clone(&node));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/", listener.local_addr().unwrap())
        .parse()
        .unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (url, node)
}

async fn until(mut condition: impl FnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while !condition() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn stalled_initial_publication_keeps_original_payment_and_closes_admission() {
    let (url, node) = node(1, Duration::ZERO).await;
    node.stall.store(true, Ordering::SeqCst);
    let directory = tempfile::tempdir().unwrap();
    let mut faucet =
        Faucet::new(url, wallet().address, 10, &directory.path().join("state")).unwrap();
    faucet.request_timeout = Duration::from_millis(30);
    let failure = faucet
        .transfer_to_pk(prepared(1).recipient_note.public_key, [1; 32], &wallet())
        .await
        .err()
        .unwrap();
    assert!(failure.may_be_submitted);
    assert_eq!(faucet.status.get(), FaucetReadiness::NodeUnavailable);
    let saved = faucet.store.load_pending().unwrap().unwrap();
    node.stall.store(false, Ordering::SeqCst);
    let faucet = Arc::new(faucet);
    let running = tokio::spawn({
        let faucet = Arc::clone(&faucet);
        async move {
            faucet
                .resume_pending_with_policy(Duration::from_millis(50), Duration::from_millis(5))
                .await
        }
    });
    until(|| node.submissions.lock().unwrap().len() >= 2).await;
    node.confirm.store(true, Ordering::SeqCst);
    running.await.unwrap().unwrap();
    assert_eq!(
        faucet
            .store
            .completed(&hex::encode([1; 32]))
            .unwrap()
            .unwrap(),
        saved
    );
    assert!(
        node.submissions
            .lock()
            .unwrap()
            .iter()
            .all(|tx| *tx == saved["prepared"]["transaction"])
    );
    assert_eq!(node.preparations.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn deep_confirmation_progress_survives_the_readiness_deadline() {
    let (url, node) = node(12, Duration::from_millis(900)).await;
    let directory = tempfile::tempdir().unwrap();
    let mut faucet =
        Faucet::new(url, wallet().address, 10, &directory.path().join("state")).unwrap();
    faucet.request_timeout = Duration::from_secs(2);
    faucet
        .transfer_to_pk(prepared(1).recipient_note.public_key, [1; 32], &wallet())
        .await
        .ok()
        .unwrap();
    node.confirm.store(true, Ordering::SeqCst);
    let faucet = Arc::new(faucet);
    let running = tokio::spawn({
        let faucet = Arc::clone(&faucet);
        async move {
            faucet
                .resume_pending_with_policy(Duration::from_millis(50), Duration::from_millis(5))
                .await
        }
    });
    until(|| faucet.status.get() == FaucetReadiness::InterventionRequired).await;
    assert!(!running.is_finished());
    running.await.unwrap().unwrap();
    assert_eq!(node.reads.load(Ordering::SeqCst), 12);
    assert!(
        faucet
            .store
            .completed(&hex::encode([1; 32]))
            .unwrap()
            .is_some()
    );
}

#[tokio::test]
async fn mutable_confirmation_revalidates_the_current_canonical_chain() {
    let (url, node) = node(8, Duration::ZERO).await;
    node.immutable.store(false, Ordering::SeqCst);
    node.canonical.store(false, Ordering::SeqCst);
    let directory = tempfile::tempdir().unwrap();
    let faucet =
        Arc::new(Faucet::new(url, wallet().address, 10, &directory.path().join("state")).unwrap());
    faucet
        .transfer_to_pk(prepared(1).recipient_note.public_key, [1; 32], &wallet())
        .await
        .ok()
        .unwrap();
    let saved = faucet.store.load_pending().unwrap().unwrap();
    node.confirm.store(true, Ordering::SeqCst);
    let running = tokio::spawn({
        let faucet = Arc::clone(&faucet);
        async move {
            faucet
                .resume_pending_with_policy(Duration::from_millis(50), Duration::from_millis(5))
                .await
        }
    });
    until(|| node.canonical_checks.load(Ordering::SeqCst) > 0).await;
    assert_eq!(faucet.store.load_pending().unwrap().unwrap(), saved);
    assert!(!running.is_finished());
    node.canonical.store(true, Ordering::SeqCst);
    running.await.unwrap().unwrap();
    assert_eq!(
        faucet
            .store
            .completed(&hex::encode([1; 32]))
            .unwrap()
            .unwrap(),
        saved
    );
    assert_eq!(node.preparations.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn restarted_worker_reconciles_archives_and_processes_the_next_http_payment() {
    use crate::server::{FaucetServerState, faucet_app};
    let (url, node) = node(1, Duration::ZERO).await;
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("state");
    let faucet = Arc::new(Faucet::new(url.clone(), wallet().address, 10, &path).unwrap());
    faucet
        .transfer_to_pk(prepared(1).recipient_note.public_key, [1; 32], &wallet())
        .await
        .ok()
        .unwrap();
    let saved = faucet.store.load_pending().unwrap().unwrap();
    let (sender, receiver) = mpsc::channel(2);
    let running = tokio::spawn(run_worker(Arc::clone(&faucet), receiver));
    until(|| node.submissions.lock().unwrap().len() >= 2).await;
    running.abort();
    drop(running.await);
    drop(sender);
    drop(faucet);
    let faucet = Arc::new(Faucet::new(url, wallet().address, 10, &path).unwrap());
    let (sender, receiver) = mpsc::channel(2);
    let app = faucet_app(Arc::new(FaucetServerState::new(
        sender,
        Duration::ZERO,
        faucet.status(),
    )));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let target = format!(
        "http://{}/faucet/{}",
        listener.local_addr().unwrap(),
        hex::encode(lb_groth16::fr_to_bytes(
            prepared(1).recipient_note.public_key.as_fr()
        ))
    );
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    let running = tokio::spawn(run_worker(Arc::clone(&faucet), receiver));
    let client = reqwest::Client::new();
    assert_eq!(
        client
            .post(&target)
            .header("Idempotency-Key", hex::encode([2; 32]))
            .send()
            .await
            .unwrap()
            .status(),
        reqwest::StatusCode::SERVICE_UNAVAILABLE
    );
    assert_eq!(faucet.store.load_pending().unwrap().unwrap(), saved);
    node.confirm.store(true, Ordering::SeqCst);
    until(|| faucet.status.get() == FaucetReadiness::Ready).await;
    assert_eq!(
        faucet
            .store
            .completed(&hex::encode([1; 32]))
            .unwrap()
            .unwrap(),
        saved
    );
    assert_eq!(
        client
            .post(&target)
            .header("Idempotency-Key", hex::encode([2; 32]))
            .send()
            .await
            .unwrap()
            .status(),
        reqwest::StatusCode::ACCEPTED
    );
    until(|| {
        faucet
            .store
            .completed(&hex::encode([2; 32]))
            .unwrap()
            .is_some()
    })
    .await;
    assert_eq!(node.preparations.load(Ordering::SeqCst), 2);
    assert!(faucet.store.load_pending().unwrap().is_none());
    running.abort();
    drop(running.await);
    server.abort();
}
