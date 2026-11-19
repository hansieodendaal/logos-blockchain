use std::{
    collections::{HashMap, HashSet},
    hash::Hash,
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicU8, Ordering},
    },
    time::Duration,
};

use lb_common_http_client::{CommonHttpClient, Error as HttpError};
use lb_core::{
    header::HeaderId,
    mantle::{traits::Hashable as _, transactions::hash::TxHash},
};
use lb_http_api_common::bodies::wallet::{
    balance::WalletBalanceResponseBody,
    transfer_funds::{WalletPrepareTransferResponseBody, WalletTransferFundsRequestBody},
};
use lb_key_management_system_keys::keys::ZkPublicKey;
use lb_wallet::SPEND_ROOT_DEPTH;
use reqwest::Url;
use tokio::sync::mpsc;

use crate::durable::PaymentStore;
#[cfg(test)]
#[path = "faucet/recovery_tests.rs"]
mod recovery_tests;

/// How often the worker polls the faucet balance while waiting for a
/// submitted drip to be included in a block.
const CONFIRMATION_POLL_INTERVAL: Duration = Duration::from_secs(1);
/// How long the worker waits for a submitted drip to appear at the tip before
/// taking it as dropped. Generous compared to expected block times.
const CONFIRMATION_TIMEOUT: Duration = Duration::from_mins(2);
/// How many blocks below the tip a drip must be to count as confirmed, or at
/// the LIB if that is shallower: the wallet spends the change note the next
/// drip needs from that depth on.
const CONFIRMATION_DEPTH: usize = SPEND_ROOT_DEPTH;
/// Pause between transfer attempts for the same drip request that has not been
/// submitted yet, so transient node errors are not retried in a tight loop.
const RETRY_DELAY: Duration = Duration::from_secs(5);
/// Transfer attempts per drip request before it is dropped. Until an attempt is
/// submitted, one failing request stalls the queue for at most
/// `MAX_TRANSFER_ATTEMPTS * RETRY_DELAY` plus the time the requests take, on
/// top of the wait for the notes that can fund it to become spendable. Once one
/// is, each attempt waits for confirmation until the balance has been back
/// at its value before the drip for `CONFIRMATION_TIMEOUT`, so a drip seen at
/// the tip holds the queue for as long as it stays there short of the
/// confirmation depth.
const MAX_TRANSFER_ATTEMPTS: u32 = 5;
/// Stop accepting new work when a saved payment has not confirmed in this
/// period. The original transaction remains in storage and is retried.
const RECOVERY_BLOCK_AFTER: Duration = Duration::from_mins(2);
const RECOVERY_REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum FaucetReadiness {
    Ready,
    AwaitingConfirmation,
    NodeUnavailable,
    InterventionRequired,
}

#[derive(Debug)]
pub struct FaucetStatus(AtomicU8);

impl FaucetStatus {
    #[must_use]
    pub const fn new(status: FaucetReadiness) -> Self {
        Self(AtomicU8::new(status as u8))
    }

    pub fn set(&self, status: FaucetReadiness) {
        self.0.store(status as u8, Ordering::SeqCst);
    }

    #[must_use]
    pub fn get(&self) -> FaucetReadiness {
        match self.0.load(Ordering::SeqCst) {
            0 => FaucetReadiness::Ready,
            1 => FaucetReadiness::AwaitingConfirmation,
            2 => FaucetReadiness::NodeUnavailable,
            _ => FaucetReadiness::InterventionRequired,
        }
    }
}

/// A transfer that did not report success.
pub struct TransferFailure {
    reason: String,
    /// The node may have accepted the transaction all the same.
    may_be_submitted: bool,
    storage_failed: bool,
}

/// A caller-chosen id identifies one payment across retries and restarts.
#[derive(Clone, Copy, Debug)]
pub struct DripRequest {
    pub recipient: ZkPublicKey,
    pub id: [u8; 32],
}

/// Whether a transfer failing with `error` may still have reached the mempool:
/// the node's answer was lost, or is not one the transfer handler gives, as
/// with a timeout. A refused connection never reached it, and an internal
/// server error is the handler refusing the transfer.
fn may_be_submitted(error: &HttpError) -> bool {
    match error {
        HttpError::Request(error) => !error.is_connect(),
        HttpError::UnexpectedResponse(_) => true,
        HttpError::Server(_) | HttpError::Client(_) | HttpError::Url(_) => false,
    }
}

pub struct Faucet {
    request_timeout: Duration,
    faucet_pk: ZkPublicKey,
    drip_amount: u64,
    http_client: CommonHttpClient,
    base_url: Url,
    store: PaymentStore,
    status: Arc<FaucetStatus>,
}

impl Faucet {
    pub fn new(
        base_url: Url,
        faucet_pk: ZkPublicKey,
        drip_amount: u64,
        state_dir: &Path,
    ) -> Result<Self, String> {
        let http_client = CommonHttpClient::new(None);
        let store = PaymentStore::open(state_dir).map_err(|error| error.to_string())?;

        let status = if store
            .load_pending()
            .map_err(|error| error.to_string())?
            .is_some()
        {
            // A restart loses the age of the pending attempt. Keep admission
            // closed until it has been reconciled, regardless of its age.
            FaucetReadiness::InterventionRequired
        } else {
            FaucetReadiness::Ready
        };
        Ok(Self {
            request_timeout: RECOVERY_REQUEST_TIMEOUT,
            faucet_pk,
            drip_amount,
            http_client,
            base_url,
            store,
            status: Arc::new(FaucetStatus::new(status)),
        })
    }

    #[must_use]
    pub fn status(&self) -> Arc<FaucetStatus> {
        Arc::clone(&self.status)
    }

    /// The notes of the faucet at the tip, and that tip.
    pub async fn wallet_at_tip(&self) -> Result<WalletBalanceResponseBody, String> {
        self.http_client
            .get_wallet_balance(self.base_url.clone(), self.faucet_pk, None)
            .await
            .map_err(|e| format!("Failed to fetch faucet balance: {e}"))
    }

    /// The notes of the faucet at the tip, once the drip they can pay is not
    /// held back by notes no spend can use yet. Those become spendable as
    /// the chain grows, so waiting uses up none of the transfer attempts.
    async fn wallet_past_pending_notes(&self) -> Result<WalletBalanceResponseBody, String> {
        let mut told = false;
        loop {
            let wallet = self.wallet_at_tip().await?;
            if !pending_notes_hold_the_drip(&wallet.notes, &wallet.pending_notes, self.drip_amount)
            {
                return Ok(wallet);
            }
            if !told {
                eprintln!("Waiting for pending notes to become spendable before the next drip");
                told = true;
            }
            tokio::time::sleep(CONFIRMATION_POLL_INTERVAL).await;
        }
    }

    /// The faucet balance `CONFIRMATION_DEPTH` blocks below the tip, or at the
    /// LIB if that is shallower.
    async fn confirmed_balance(&self) -> Result<u64, String> {
        let info = self
            .http_client
            .consensus_info(self.base_url.clone())
            .await
            .map_err(|e| format!("Failed to fetch the chain tip: {e}"))?
            .cryptarchia_info;
        let mut block = info.tip;
        for _ in 0..CONFIRMATION_DEPTH {
            if block == info.lib {
                break;
            }
            block = self
                .http_client
                .get_block_by_id(self.base_url.clone(), block)
                .await
                .map_err(|e| format!("Failed to fetch block {block}: {e}"))?
                .ok_or_else(|| format!("Block {block} is not on the node"))?
                .header
                .parent_block;
        }
        self.http_client
            .get_wallet_balance(self.base_url.clone(), self.faucet_pk, Some(block))
            .await
            .map(|info| info.balance)
            .map_err(|e| format!("Failed to fetch faucet balance at {block}: {e}"))
    }

    /// Transfers to `recipient_pk` from the notes of `wallet`, at its tip.
    pub async fn transfer_to_pk(
        &self,
        recipient_pk: ZkPublicKey,
        request_id: [u8; 32],
        wallet: &WalletBalanceResponseBody,
    ) -> Result<WalletPrepareTransferResponseBody, TransferFailure> {
        match self.store.load_pending() {
            Ok(None) => (),
            Ok(Some(_)) => {
                return Err(TransferFailure {
                    reason: "A faucet payment is already pending".into(),
                    may_be_submitted: false,
                    storage_failed: false,
                });
            }
            Err(error) => {
                return Err(TransferFailure {
                    reason: format!("Cannot read pending faucet payment: {error}"),
                    may_be_submitted: false,
                    storage_failed: true,
                });
            }
        }
        let current_balance = wallet.balance;

        let amount_to_send = std::cmp::min(current_balance, self.drip_amount);
        if amount_to_send == 0 {
            return Err(TransferFailure {
                reason: format!(
                    "Balance too low to drip (Current: {current_balance}, ask for direct transfter in discord)"
                ),
                may_be_submitted: false,
                storage_failed: false,
            });
        }

        println!("Dripping {amount_to_send} units to {recipient_pk:?}");

        let body = WalletTransferFundsRequestBody {
            tip: Some(wallet.tip),
            change_public_key: self.faucet_pk,
            funding_public_keys: vec![self.faucet_pk],
            recipient_public_key: recipient_pk,
            amount: amount_to_send,
        };

        let response = self
            .http_client
            .prepare_transfer_funds(self.base_url.clone(), body)
            .await
            .map_err(|e| TransferFailure {
                may_be_submitted: false,
                storage_failed: false,
                reason: format!("Faucet transfer preparation failed: {e}"),
            })?;

        let payment = serde_json::json!({"request_id": hex::encode(request_id), "recipient": recipient_pk, "prepared": response, "start_tip": wallet.tip, "faucet_pk": self.faucet_pk});
        self.store
            .save_pending(&payment)
            .map_err(|error| TransferFailure {
                may_be_submitted: false,
                storage_failed: true,
                reason: format!("Cannot persist prepared faucet transfer: {error}"),
            })?;
        self.status.set(FaucetReadiness::AwaitingConfirmation);

        tokio::time::timeout(
            self.request_timeout,
            self.http_client
                .post_transaction(self.base_url.clone(), response.transaction.clone()),
        )
        .await
        .map_err(|_| {
            self.status.set(FaucetReadiness::NodeUnavailable);
            TransferFailure {
                may_be_submitted: true,
                storage_failed: false,
                reason:
                    "Initial faucet submission timed out; the saved transaction remains pending"
                        .into(),
            }
        })?
        .map_err(|e| TransferFailure {
            may_be_submitted: may_be_submitted(&e),
            storage_failed: false,
            reason: format!("Faucet transfer failed: {e}"),
        })?;

        Ok(response)
    }

    // Search the canonical chain from tip through the original preparation
    // point. A confirmation belongs to this payment only if its hash appears.
    async fn payment_confirmed(&self, hash: TxHash, start: HeaderId) -> Result<bool, String> {
        let info = tokio::time::timeout(
            self.request_timeout,
            self.http_client.consensus_info(self.base_url.clone()),
        )
        .await
        .map_err(|_| "Timed out reading chain information")?
        .map_err(|e| e.to_string())?
        .cryptarchia_info;
        let mut id = info.tip;
        let mut depth = 0usize;
        let mut immutable = false;
        loop {
            immutable |= id == info.lib;
            let block = tokio::time::timeout(
                self.request_timeout,
                self.http_client.get_block_by_id(self.base_url.clone(), id),
            )
            .await
            .map_err(|_| "Timed out reading a confirmation block")?
            .map_err(|e| e.to_string())?
            .ok_or_else(|| format!("Missing block {id}"))?;
            if block.transactions.iter().any(|tx| tx.hash() == hash) {
                if immutable {
                    return Ok(true);
                }
                if depth < CONFIRMATION_DEPTH {
                    return Ok(false);
                }
                // Revalidate the pinned tip: its ancestry already established
                // the payment's depth. The payment itself may be deeper than
                // the bounded canonical-headers response can reach.
                let headers = tokio::time::timeout(
                    self.request_timeout,
                    self.http_client
                        .canonical_headers(self.base_url.clone(), info.tip),
                )
                .await
                .map_err(|_| "Timed out validating canonical confirmation")?
                .map_err(|e| e.to_string())?;
                return Ok(headers.contains(&info.tip));
            }
            if id == start
                || block.header.parent_block == id
                || block.header.parent_block == HeaderId::from([0; 32])
            {
                return Ok(false);
            }
            id = block.header.parent_block;
            depth = depth.saturating_add(1);
        }
    }

    async fn resume_pending(&self) -> Result<(), String> {
        self.resume_pending_with_policy(RECOVERY_BLOCK_AFTER, CONFIRMATION_POLL_INTERVAL)
            .await
    }

    async fn resume_pending_with_policy(
        &self,
        block_after: Duration,
        retry_delay: Duration,
    ) -> Result<(), String> {
        let Some(record) = self.store.load_pending().map_err(|e| e.to_string())? else {
            self.status.set(FaucetReadiness::Ready);
            return Ok(());
        };
        let response: WalletPrepareTransferResponseBody =
            serde_json::from_value(record["prepared"].clone()).map_err(|e| e.to_string())?;
        let start: HeaderId =
            serde_json::from_value(record["start_tip"].clone()).map_err(|e| e.to_string())?;
        let key: ZkPublicKey =
            serde_json::from_value(record["faucet_pk"].clone()).map_err(|e| e.to_string())?;
        let recipient: ZkPublicKey =
            serde_json::from_value(record["recipient"].clone()).map_err(|e| e.to_string())?;
        let request_id = record["request_id"]
            .as_str()
            .ok_or("pending payment has no request id")?;
        // Validate the id before attempting publication.
        self.store
            .completed(request_id)
            .map_err(|e| e.to_string())?;
        if key != self.faucet_pk
            || response.recipient_note.public_key != recipient
            || response.transaction.hash() != response.hash
        {
            return Err("pending faucet payment does not match its stored identity".into());
        }
        let started = tokio::time::Instant::now();
        let mut blocked = self.status.get() == FaucetReadiness::InterventionRequired;
        loop {
            blocked |= started.elapsed() >= block_after;
            if blocked {
                self.status.set(FaucetReadiness::InterventionRequired);
            }
            let confirmation = self.payment_confirmed(response.hash, start);
            tokio::pin!(confirmation);
            let result = if blocked {
                confirmation.await
            } else {
                tokio::select! {
                    result = &mut confirmation => result,
                    () = tokio::time::sleep(block_after.saturating_sub(started.elapsed())) => {
                        blocked = true;
                        self.status.set(FaucetReadiness::InterventionRequired);
                        confirmation.await
                    }
                }
            };
            match result {
                Ok(true) => {
                    self.store.complete(request_id).map_err(|e| e.to_string())?;
                    self.status.set(FaucetReadiness::Ready);
                    return Ok(());
                }
                Ok(false) => {
                    if !blocked {
                        self.status.set(FaucetReadiness::AwaitingConfirmation);
                    }
                }
                Err(error) => {
                    if !blocked {
                        self.status.set(FaucetReadiness::NodeUnavailable);
                    }
                    eprintln!("Cannot reconcile pending payment: {error}");
                }
            }
            match tokio::time::timeout(
                self.request_timeout,
                self.http_client
                    .post_transaction(self.base_url.clone(), response.transaction.clone()),
            )
            .await
            {
                Ok(Ok(())) => (),
                Ok(Err(error)) => eprintln!("Pending faucet submission not acknowledged: {error}"),
                Err(_) => eprintln!("Pending faucet submission timed out"),
            }
            tokio::time::sleep(retry_delay).await;
        }
    }

    /// Waits until the faucet balance differs from `balance_before` at the
    /// confirmation depth, which signals that the submitted drip is unlikely to
    /// be lost to a fork and the change note is spendable again. Returns false
    /// if the balance at the tip has been `balance_before` for
    /// `CONFIRMATION_TIMEOUT`.
    async fn wait_for_confirmation(&self, balance_before: u64) -> bool {
        let mut deadline = tokio::time::Instant::now() + CONFIRMATION_TIMEOUT;
        while tokio::time::Instant::now() < deadline {
            if let Ok(wallet) = self.wallet_at_tip().await
                && wallet.balance != balance_before
            {
                if let Ok(balance) = self.confirmed_balance().await
                    && balance != balance_before
                {
                    return true;
                }
                deadline = tokio::time::Instant::now() + CONFIRMATION_TIMEOUT;
            }
            tokio::time::sleep(CONFIRMATION_POLL_INTERVAL).await;
        }
        eprintln!("Timed out waiting for drip confirmation");
        false
    }
}

/// Drains queued drip requests one at a time.
///
/// Each drip waits for on-chain confirmation before the next starts. The
/// faucet wallet holds a single note, so concurrent transfers would conflict
/// on the same input; serializing on confirmation guarantees each drip funds
/// from the confirmed change of the previous one.
pub async fn run_worker(faucet: Arc<Faucet>, mut requests: mpsc::Receiver<DripRequest>) {
    if let Err(error) = faucet.resume_pending().await {
        faucet.status.set(FaucetReadiness::InterventionRequired);
        eprintln!("Cannot recover faucet payment: {error}");
        return;
    }
    while let Some(request) = requests.recv().await {
        match faucet.store.completed(&hex::encode(request.id)) {
            Ok(Some(record)) => {
                let matches = serde_json::from_value::<ZkPublicKey>(record["recipient"].clone())
                    .is_ok_and(|key| key == request.recipient);
                if !matches {
                    eprintln!("Faucet request id belongs to another recipient");
                }
                continue;
            }
            Ok(None) => (),
            Err(error) => {
                faucet.status.set(FaucetReadiness::InterventionRequired);
                eprintln!("Cannot check completed faucet payment: {error}");
                return;
            }
        }
        if let Err(error) = drip_with_retries(&faucet, request).await {
            faucet.status.set(FaucetReadiness::InterventionRequired);
            eprintln!("Cannot recover faucet payment: {error}");
            return;
        }
    }
}

/// Submits the drip until it is confirmed. A submitted drip that does not show
/// up may have been dropped from the mempools, for example when the note it
/// spends was lost to a fork, so it is submitted again, as long as that cannot
/// pay the recipient twice. A transfer whose answer was lost counts as
/// submitted, since it may land.
async fn drip_with_retries(faucet: &Faucet, request: DripRequest) -> Result<(), String> {
    let recipient_pk = request.recipient;
    let mut first_attempt = None;
    for attempt in 1..=MAX_TRANSFER_ATTEMPTS {
        match submit_drip(faucet, request, first_attempt.as_ref()).await {
            Ok(wallet) => {
                first_attempt.get_or_insert(wallet);
            }
            Err(failed) => {
                if failed.storage_failed {
                    return Err(failed.reason);
                }
                eprintln!(
                    "Drip attempt {attempt}/{MAX_TRANSFER_ATTEMPTS} failed for {recipient_pk:?}: {}",
                    failed.reason
                );
                if let Some(wallet) = failed.maybe_submitted_from {
                    first_attempt.get_or_insert(*wallet);
                }
            }
        }
        if faucet
            .store
            .load_pending()
            .map_err(|e| e.to_string())?
            .is_some()
        {
            return faucet.resume_pending().await;
        }
        // A failed attempt may be a note still reserved for the first one,
        // whose transaction can land in the meantime.
        match &first_attempt {
            Some(first) => {
                if faucet.wait_for_confirmation(first.balance).await {
                    return Ok(());
                }
            }
            None => tokio::time::sleep(RETRY_DELAY).await,
        }
    }
    eprintln!("Dropping drip request for {recipient_pk:?} after {MAX_TRANSFER_ATTEMPTS} attempts");
    Ok(())
}

/// An attempt at a drip that did not report success.
struct FailedAttempt {
    reason: String,
    /// The notes it was funded from, if its transaction may land all the same.
    maybe_submitted_from: Option<Box<WalletBalanceResponseBody>>,
    storage_failed: bool,
}

impl From<String> for FailedAttempt {
    fn from(reason: String) -> Self {
        Self {
            reason,
            maybe_submitted_from: None,
            storage_failed: false,
        }
    }
}

/// Submits the drip from the notes at the tip, and returns them. The transfer
/// is funded at that tip, so that the notes checked are the ones it spends.
async fn submit_drip(
    faucet: &Faucet,
    request: DripRequest,
    first_attempt: Option<&WalletBalanceResponseBody>,
) -> Result<WalletBalanceResponseBody, FailedAttempt> {
    let recipient_pk = request.recipient;
    let wallet = match first_attempt {
        Some(_) => faucet.wallet_at_tip().await?,
        None => faucet.wallet_past_pending_notes().await?,
    };
    if let Some(first) = first_attempt
        && !conflicts_with_first_attempt(&first.notes, &wallet.notes)
    {
        return Err("the first attempt can still land next to a new one"
            .to_owned()
            .into());
    }
    let response = match faucet
        .transfer_to_pk(recipient_pk, request.id, &wallet)
        .await
    {
        Ok(response) => response,
        Err(failure) => {
            return Err(FailedAttempt {
                reason: failure.reason,
                maybe_submitted_from: failure.may_be_submitted.then(|| Box::new(wallet)),
                storage_failed: failure.storage_failed,
            });
        }
    };
    println!(
        "Drip submitted for {recipient_pk:?}, hash: {:?}",
        response.hash
    );
    Ok(wallet)
}

/// Whether a transfer funded from `held_now` cannot land next to the first
/// attempt, funded from `funded_first_attempt`: both then spend the same note,
/// the only one the faucet had and still has. With several notes, the wallet
/// may fund each attempt from another one, and with the note gone the first
/// attempt has landed and its change would fund a second payment.
fn conflicts_with_first_attempt<Note: Eq + Hash>(
    funded_first_attempt: &HashMap<Note, u64>,
    held_now: &HashMap<Note, u64>,
) -> bool {
    funded_first_attempt.len() == 1 && funded_first_attempt == held_now
}

/// Whether the spendable notes fall short of the drip `notes` can pay, so that
/// the rest of it sits in `pending`.
fn pending_notes_hold_the_drip<Note: Eq + Hash>(
    notes: &HashMap<Note, u64>,
    pending: &HashSet<Note>,
    drip_amount: u64,
) -> bool {
    let total: u64 = notes.values().sum();
    let spendable: u64 = notes
        .iter()
        .filter(|(note, _)| !pending.contains(*note))
        .map(|(_, value)| value)
        .sum();
    spendable < total.min(drip_amount)
}

#[cfg(test)]
mod tests {
    use std::{path::PathBuf, sync::Mutex};

    use serde_json::Value;

    use super::*;

    async fn mock_node(
        fail_path: Option<PathBuf>,
    ) -> (
        Url,
        Arc<Mutex<Vec<Value>>>,
        WalletPrepareTransferResponseBody,
        Arc<AtomicU8>,
    ) {
        use axum::{
            Json, Router,
            extract::State,
            http::StatusCode,
            routing::{get, post},
        };
        use lb_core::mantle::{
            Note, SignedOps, ledger::verification_mode::StandardMode,
            transactions::states::Unverified,
        };
        use lb_groth16::Fr;

        let transaction = SignedOps::<Unverified, StandardMode>::sample();
        let recipient_note = Note::new(10, Fr::from(11u64), ZkPublicKey::new(Fr::from(3u64)));
        let response = WalletPrepareTransferResponseBody {
            hash: transaction.hash(),
            transaction,
            recipient_note,
        };
        let prepared = serde_json::to_value(&response).unwrap();
        let chain_state = Arc::new(AtomicU8::new(0));
        let info_state = Arc::clone(&chain_state);
        let block_state = Arc::clone(&chain_state);
        let block_transaction = response.transaction.clone();
        let submissions = Arc::new(Mutex::new(Vec::new()));
        let recorded = Arc::clone(&submissions);
        let app = Router::new()
            .route(
                "/wallet/transactions/prepare-transfer-funds",
                post(move || {
                    let prepared = prepared.clone();
                    let fail_path = fail_path.clone();
                    async move {
                        if let Some(path) = fail_path {
                            std::fs::write(path, b"collision").unwrap();
                        }
                        Json(prepared)
                    }
                }),
            )
            .route(
                "/mempool/add/tx",
                post(
                    async |State(recorded): State<Arc<Mutex<Vec<Value>>>>,
                           Json(tx): Json<Value>| {
                        recorded.lock().unwrap().push(tx.clone());
                        // The server has accepted the transaction. Terminating
                        // the handler closes the connection before its reply.
                        assert!(
                            tx.is_null(),
                            "simulate an accepted submission with a lost response"
                        );
                        StatusCode::OK
                    },
                ),
            )
            .route("/cryptarchia/info", get(move || {
                let state = Arc::clone(&info_state);
                async move {
                    if state.load(Ordering::SeqCst) == 0 {
                        return (StatusCode::SERVICE_UNAVAILABLE, Json(Value::Null));
                    }
                    let tip = HeaderId::from([1; 32]);
                    (StatusCode::OK, Json(serde_json::to_value(lb_common_http_client::ChainServiceInfo {
                        cryptarchia_info: lb_common_http_client::CryptarchiaInfo {
                            lib: tip, lib_slot: 0.into(), tip, slot: 0.into(), height: 0,
                            state: lb_common_http_client::State::Online,
                        },
                        phase: lb_common_http_client::PhaseTag::Following,
                    }).unwrap()))
                }
            }))
            .route("/cryptarchia/blocks/{id}", get(move || {
                let state = Arc::clone(&block_state);
                let tx = block_transaction.clone();
                async move {
                    let transactions = if state.load(Ordering::SeqCst) == 2 { vec![tx] } else { vec![] };
                    Json(serde_json::json!({
                        "header": {
                            "id": HeaderId::from([1; 32]),
                            "parent_block": HeaderId::from([0; 32]),
                            "slot": lb_common_http_client::Slot::from(0),
                            "body_root": lb_core::header::ContentId::from([0; 32]),
                            "proof_of_leadership": lb_core::proofs::leader_proof::Groth16LeaderProof::genesis(),
                        },
                        "uncle_headers": [],
                        "transactions": transactions,
                    }))
                }
            }))
            .with_state(recorded);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/", listener.local_addr().unwrap())
            .parse()
            .unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        (url, submissions, response, chain_state)
    }

    fn mock_wallet() -> WalletBalanceResponseBody {
        use lb_groth16::Fr;
        WalletBalanceResponseBody {
            tip: HeaderId::from([1; 32]),
            balance: 100,
            notes: HashMap::new(),
            pending_notes: HashSet::new(),
            address: ZkPublicKey::new(Fr::from(2u64)),
        }
    }

    #[tokio::test]
    async fn failed_persistence_prevents_publication() {
        let directory = tempfile::tempdir().unwrap();
        let (url, submissions, prepared, _) =
            mock_node(Some(directory.path().join("state/pending.json"))).await;
        let faucet = Faucet::new(
            url,
            mock_wallet().address,
            10,
            &directory.path().join("state"),
        )
        .unwrap();
        let Err(error) = faucet
            .transfer_to_pk(prepared.recipient_note.public_key, [1; 32], &mock_wallet())
            .await
        else {
            panic!("persistence failure must prevent submission");
        };
        assert!(error.storage_failed);
        assert!(error.reason.contains("Cannot persist"));
        assert!(submissions.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn ambiguous_submission_retries_the_saved_payment_after_restart() {
        let (url, submissions, prepared, _) = mock_node(None).await;
        let directory = tempfile::tempdir().unwrap();
        let state_dir = directory.path().join("state");
        let faucet = Faucet::new(url.clone(), mock_wallet().address, 10, &state_dir).unwrap();
        let Err(error) = faucet
            .transfer_to_pk(prepared.recipient_note.public_key, [1; 32], &mock_wallet())
            .await
        else {
            panic!("the mock loses the submission response");
        };
        assert!(error.may_be_submitted);
        assert!(!error.storage_failed);
        let saved = faucet.store.load_pending().unwrap().unwrap();
        assert_eq!(saved["prepared"], serde_json::to_value(&prepared).unwrap());
        drop(faucet);

        let faucet = Arc::new(Faucet::new(url, mock_wallet().address, 10, &state_dir).unwrap());
        assert_eq!(faucet.store.load_pending().unwrap().unwrap(), saved);
        assert!(
            faucet
                .transfer_to_pk(prepared.recipient_note.public_key, [1; 32], &mock_wallet())
                .await
                .is_err()
        );
        assert_eq!(submissions.lock().unwrap().len(), 1);
        let (requests, receiver) = mpsc::channel(1);
        requests
            .send(DripRequest {
                recipient: prepared.recipient_note.public_key,
                id: [1; 32],
            })
            .await
            .unwrap();
        let running = tokio::spawn(run_worker(Arc::clone(&faucet), receiver));
        tokio::time::timeout(Duration::from_secs(5), async {
            while submissions.lock().unwrap().len() < 2 {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap();
        running.abort();
        let all_same = submissions
            .lock()
            .unwrap()
            .iter()
            .all(|tx| *tx == serde_json::to_value(&prepared.transaction).unwrap());
        assert!(all_same);
        assert_eq!(faucet.store.load_pending().unwrap().unwrap(), saved);
        faucet.store.complete(&hex::encode([1; 32])).unwrap();
        let submission_count = submissions.lock().unwrap().len();
        let (requests, receiver) = mpsc::channel(1);
        requests
            .send(DripRequest {
                recipient: prepared.recipient_note.public_key,
                id: [1; 32],
            })
            .await
            .unwrap();
        drop(requests);
        run_worker(Arc::clone(&faucet), receiver).await;
        assert_eq!(submissions.lock().unwrap().len(), submission_count);
    }

    #[tokio::test]
    async fn unresolved_payment_blocks_and_survives_restart_without_replacement() {
        let (url, submissions, prepared, _) = mock_node(None).await;
        let directory = tempfile::tempdir().unwrap();
        let state_dir = directory.path().join("state");
        let faucet =
            Arc::new(Faucet::new(url.clone(), mock_wallet().address, 10, &state_dir).unwrap());
        assert!(
            faucet
                .transfer_to_pk(prepared.recipient_note.public_key, [2; 32], &mock_wallet())
                .await
                .is_err()
        );
        let saved = faucet.store.load_pending().unwrap().unwrap();
        let worker = tokio::spawn({
            let faucet = Arc::clone(&faucet);
            async move {
                faucet
                    .resume_pending_with_policy(
                        Duration::from_millis(60),
                        Duration::from_millis(10),
                    )
                    .await
            }
        });
        tokio::time::timeout(Duration::from_secs(5), async {
            while faucet.status.get() != FaucetReadiness::InterventionRequired {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        worker.abort();
        drop(worker.await);
        assert_eq!(faucet.store.load_pending().unwrap().unwrap(), saved);
        assert!(submissions.lock().unwrap().len() >= 2);
        assert!(
            submissions
                .lock()
                .unwrap()
                .iter()
                .all(|tx| *tx == serde_json::to_value(&prepared.transaction).unwrap())
        );
        drop(faucet);
        let restarted = Faucet::new(url, mock_wallet().address, 10, &state_dir).unwrap();
        assert_eq!(
            restarted.status.get(),
            FaucetReadiness::InterventionRequired
        );
        assert_eq!(restarted.store.load_pending().unwrap().unwrap(), saved);
        assert_eq!(saved["prepared"], serde_json::to_value(&prepared).unwrap());
        assert!(
            restarted
                .transfer_to_pk(prepared.recipient_note.public_key, [3; 32], &mock_wallet())
                .await
                .is_err()
        );
        assert_eq!(restarted.store.load_pending().unwrap().unwrap(), saved);
    }

    #[tokio::test]
    async fn transient_unavailability_recovers_and_completes_the_original_payment() {
        let (url, submissions, prepared, chain) = mock_node(None).await;
        let directory = tempfile::tempdir().unwrap();
        let faucet = Arc::new(
            Faucet::new(
                url,
                mock_wallet().address,
                10,
                &directory.path().join("state"),
            )
            .unwrap(),
        );
        assert!(
            faucet
                .transfer_to_pk(prepared.recipient_note.public_key, [4; 32], &mock_wallet())
                .await
                .is_err()
        );
        let saved = faucet.store.load_pending().unwrap().unwrap();
        let worker = tokio::spawn({
            let faucet = Arc::clone(&faucet);
            async move {
                faucet
                    .resume_pending_with_policy(Duration::from_secs(5), Duration::from_millis(10))
                    .await
            }
        });
        tokio::time::timeout(Duration::from_secs(2), async {
            while faucet.status.get() != FaucetReadiness::NodeUnavailable {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        chain.store(1, Ordering::SeqCst);
        tokio::time::timeout(Duration::from_secs(2), async {
            while faucet.status.get() != FaucetReadiness::AwaitingConfirmation {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        assert_eq!(faucet.store.load_pending().unwrap().unwrap(), saved);
        chain.store(2, Ordering::SeqCst);
        tokio::time::timeout(Duration::from_secs(2), worker)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(faucet.status.get(), FaucetReadiness::Ready);
        assert!(faucet.store.load_pending().unwrap().is_none());
        assert_eq!(
            faucet
                .store
                .completed(&hex::encode([4; 32]))
                .unwrap()
                .unwrap(),
            saved
        );
        assert!(
            submissions
                .lock()
                .unwrap()
                .iter()
                .all(|tx| *tx == serde_json::to_value(&prepared.transaction).unwrap())
        );
    }

    #[test]
    fn only_an_answer_from_the_handler_settles_a_transfer_as_not_submitted() {
        assert!(!may_be_submitted(&HttpError::Server("refused".to_owned())));
        // such as the 408 of the node's timeout, while the handler may have
        // added the transaction to the mempool
        assert!(may_be_submitted(&HttpError::UnexpectedResponse(
            "[408]".to_owned()
        )));
    }

    #[test]
    fn a_drip_waits_only_for_pending_notes_that_hold_it() {
        let notes = HashMap::from([("spendable", 40), ("young", 100)]);
        let pending = HashSet::from(["young"]);
        assert!(pending_notes_hold_the_drip(&notes, &pending, 50));
        // what is spendable pays the drip, whatever else is pending
        assert!(!pending_notes_hold_the_drip(&notes, &pending, 40));
        // nothing is pending: waiting would change nothing, however short the
        // funds
        assert!(!pending_notes_hold_the_drip(&notes, &HashSet::new(), 500));
    }

    #[test]
    fn only_the_single_note_of_the_first_attempt_conflicts_with_it() {
        let single = HashMap::from([("n", 100)]);
        assert!(conflicts_with_first_attempt(&single, &single));
        // the first attempt landed: this note is its change, a second payment
        let change = HashMap::from([("c", 49)]);
        assert!(!conflicts_with_first_attempt(&single, &change));
        // another note can fund a transfer that spends nothing of the first
        // attempt
        let topped_up = HashMap::from([("n", 100), ("t", 500)]);
        assert!(!conflicts_with_first_attempt(&single, &topped_up));
        // the first attempt may have spent the note that is not funding this
        // one
        assert!(!conflicts_with_first_attempt(&topped_up, &topped_up));
    }
}
