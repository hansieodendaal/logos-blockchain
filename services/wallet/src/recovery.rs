//! Operator-controlled startup recovery. No HTTP request can schedule a scan.

use super::*;

const REQUESTS: &[u8] = b"wallet/startup_recovery_requests";
const CHECKPOINT: &[u8] = b"wallet/startup_recovery_checkpoint";

#[derive(Clone, Copy, Debug, Serialize, serde::Deserialize)]
pub struct RecoveryOpening {
    pub note: Note,
    pub note_set: NoteSetIndex,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, serde::Deserialize)]
enum Authority {
    Spending,
    Leadership,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, serde::Deserialize)]
enum Disposition {
    Requested,
    Withdrawn,
}

#[derive(Serialize, serde::Deserialize)]
struct Request {
    id: u64,
    known: KnownNote,
    authority: Authority,
    disposition: Disposition,
}

#[derive(Default, Serialize, serde::Deserialize)]
struct Requests {
    revision: u64,
    entries: Vec<Request>,
}

impl Requests {
    fn next_revision(&mut self) -> Result<u64, WalletServiceError> {
        self.revision = self
            .revision
            .checked_add(1)
            .ok_or(WalletServiceError::RecoveryRevisionOverflow)?;
        Ok(self.revision)
    }

    fn pending<'a>(&'a self, covered: &'a HashSet<u64>) -> impl Iterator<Item = &'a Request> {
        self.entries.iter().filter(|request| {
            request.disposition == Disposition::Requested && !covered.contains(&request.id)
        })
    }
}

/// Coverage and the state justifying it are one confirmed record. Ordinary
/// asynchronous snapshots cannot change either request disposition or coverage.
#[derive(Serialize, serde::Deserialize)]
struct Checkpoint {
    state: RecoveryState,
    covered: HashSet<u64>,
}

impl<Kms, Cryptarchia, Tx, TimeBackend, RuntimeServiceId>
    WalletService<Kms, Cryptarchia, Tx, TimeBackend, RuntimeServiceId>
where
    Kms: KmsServiceData<Backend = KmsBackend> + 'static,
    Tx: SignedMantleTx<Preverified, StandardMode>
        + Send
        + Sync
        + Clone
        + Eq
        + Serialize
        + DeserializeOwned
        + BinaryDecode<Context = u32>
        + StorageSize
        + 'static,
    Cryptarchia: CryptarchiaServiceData<Tx = Tx> + Send + 'static,
    TimeBackend: 'static,
    RuntimeServiceId: AsServiceId<Cryptarchia>
        + AsServiceId<Kms>
        + std::fmt::Debug
        + std::fmt::Display
        + Send
        + Sync
        + 'static,
{
    /// Configuration is the admission boundary. Validate every credential
    /// before atomically persisting any new request or opening.
    #[expect(
        clippy::too_many_lines,
        clippy::cognitive_complexity,
        reason = "validate and atomically admit authority amendments before startup replay"
    )]
    pub(super) async fn prepare_startup_recovery(
        mut initial: RecoveryState,
        settings: &WalletServiceSettings,
        storage: &StorageApi<Tx>,
        kms: &KmsServiceApi<Kms, RuntimeServiceId>,
    ) -> Result<(RecoveryState, bool), WalletServiceError> {
        let mut requests: Requests = storage
            .load(Bytes::from_static(REQUESTS))
            .await
            .map_err(WalletServiceError::PersistedState)?
            .unwrap_or_default();
        let checkpoint = storage
            .load::<Checkpoint>(Bytes::from_static(CHECKPOINT))
            .await
            .map_err(WalletServiceError::PersistedState)?;
        let covered = checkpoint
            .as_ref()
            .map_or_else(HashSet::new, |saved| saved.covered.clone());
        let previous_revision = requests.revision;
        for id in &settings.historical_recovery_withdrawals {
            let request = requests
                .entries
                .iter_mut()
                .find(|request| request.id == *id)
                .ok_or(WalletServiceError::RecoveryWithdrawalNotPending(*id))?;
            if covered.contains(id) {
                return Err(WalletServiceError::RecoveryWithdrawalNotPending(*id));
            }
            if request.disposition != Disposition::Withdrawn {
                request.disposition = Disposition::Withdrawn;
                requests.next_revision()?;
            }
        }
        let mut spending = Vec::new();
        let mut leadership = Vec::new();
        for opening in &settings.historical_recovery {
            let RecoveryOpening { note, note_set } = *opening;
            let public = LeadershipPublicKeys {
                nullifier_public_key: note.public_key,
                staking_public_key: note.stake_public_key,
            };
            let spend = settings
                .known_keys
                .iter()
                .find(|(_, pk)| **pk == note.public_key && **pk != *PK0);
            let leader = settings
                .leadership_keys
                .iter()
                .find(|(_, keys)| **keys == public && note.stake_public_key != ZkPublicKey::zero());
            if spend.is_none() && leader.is_none() {
                return Err(WalletServiceError::UnknownKey(note.public_key));
            }
            for (authority, key) in [
                (Authority::Spending, spend.map(|(id, _)| id)),
                (Authority::Leadership, leader.map(|(id, _)| id)),
            ] {
                let Some(key_id) = key else {
                    continue;
                };
                let note_id = NoteId::new(note_set, note.commitment());
                if requests.entries.iter().any(|request| {
                    request.known.id() == note_id
                        && request.authority == authority
                        && settings
                            .historical_recovery_withdrawals
                            .contains(&request.id)
                }) {
                    return Err(WalletServiceError::RecoveryWithdrawalConflict(note_id));
                }
                if requests.entries.iter().any(|request| {
                    request.known.id() == note_id
                        && request.authority == authority
                        && request.disposition == Disposition::Requested
                }) {
                    continue;
                }
                let known = match authority {
                    Authority::Spending => {
                        let public = Box::pin(kms.public_key(key_id.clone()))
                            .await
                            .map_err(WalletServiceError::KmsApi)?;
                        if public != PublicKeyEncoding::Zk(note.public_key) {
                            return Err(WalletServiceError::UnknownKey(note.public_key));
                        }
                        let known = Self::known_note(kms, key_id.clone(), note, note_set).await?;
                        spending.push(known);
                        known
                    }
                    Authority::Leadership => {
                        let known =
                            Self::leadership_known_note(kms, key_id.clone(), note, note_set)
                                .await?;
                        leadership.push(known);
                        known
                    }
                };
                let id = requests.next_revision()?;
                requests.entries.push(Request {
                    id,
                    known,
                    authority,
                    disposition: Disposition::Requested,
                });
            }
        }
        if requests.revision != previous_revision {
            storage
                .store_wallet_recovery_openings(
                    &spending,
                    &leadership,
                    Bytes::from_static(REQUESTS),
                    &requests,
                )
                .await
                .map_err(WalletServiceError::PersistedState)?;
        }
        for id in &settings.historical_recovery_withdrawals {
            info!(target: LOG_TARGET, request_id = id, "Wallet historical recovery request withdrawal confirmed");
        }
        // The asynchronous recovery writer may still contain an older state.
        // A separate confirmed checkpoint wins until that writer catches up.
        if let Some(saved) = checkpoint
            && saved.state.recovery_generation > initial.recovery_generation
        {
            initial = saved.state;
        }
        let pending = requests.pending(&covered).next().is_some();
        for request in requests.pending(&covered) {
            info!(target: LOG_TARGET, request_id = request.id, authority = ?request.authority,
                note_id = ?request.known.id(), "Wallet historical recovery request pending");
        }
        if pending {
            info!(target: LOG_TARGET, revision = requests.revision, "Starting wallet historical recovery");
        }
        Ok((initial, pending))
    }

    pub(super) async fn complete_startup_recovery(
        state: &mut ServiceState<'_>,
        tip: HeaderId,
        storage: &StorageApi<Tx>,
    ) -> Result<(), WalletServiceError> {
        let requests: Requests = storage
            .load(Bytes::from_static(REQUESTS))
            .await
            .map_err(WalletServiceError::PersistedState)?
            .unwrap_or_default();
        let mut covered = storage
            .load::<Checkpoint>(Bytes::from_static(CHECKPOINT))
            .await
            .map_err(WalletServiceError::PersistedState)?
            .map_or_else(HashSet::new, |saved| saved.covered);
        if requests.pending(&covered).next().is_none() {
            return Ok(());
        }
        let spending = state.wallet().wallet_state_at(tip)?;
        let leadership = state.leadership_wallet().wallet_state_at(tip)?;
        for request in requests.pending(&covered) {
            let snapshot = match request.authority {
                Authority::Spending => &spending,
                Authority::Leadership => &leadership,
            };
            let known = &request.known;
            let observed = snapshot.notes.get(&known.id()).is_some_and(|owned| {
                owned.note == known.note && owned.nullifier == known.nullifier
            }) || snapshot.spent.contains(&SpentNullifier {
                note_set: known.note_set,
                nullifier: known.nullifier,
            });
            if !observed {
                return Err(WalletServiceError::RecoveryNoteNotFound(known.id()));
            }
        }
        covered.extend(
            requests
                .entries
                .iter()
                .filter(|request| request.disposition == Disposition::Requested)
                .map(|request| request.id),
        );
        let mut checkpoint = Checkpoint {
            state: state.snapshot(),
            covered,
        };
        checkpoint.state.recovery_generation = requests.revision;
        storage
            .store_confirmed(Bytes::from_static(CHECKPOINT), &checkpoint)
            .await
            .map_err(WalletServiceError::PersistedState)?;
        state.recovery_generation = checkpoint.state.recovery_generation;
        info!(target: LOG_TARGET, generation = state.recovery_generation, "Wallet historical recovery checkpoint confirmed");
        state.persist_after_replay();
        Ok(())
    }
}
