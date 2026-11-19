# Historical wallet recovery

Ordinary `POST /wallet/notes/import` registers a full opening durably for
future inclusion. It does not query chain history and does not claim to recover
an already included note. Supplying `historical_path` returns an explicit error.

Historical recovery is an **operator-controlled startup maintenance operation**.
It requires the retained blocks and note-set events from genesis. It does not
require an externally generated membership proof.

1. Keep the complete recipient/service/program opening obtained when the note
   was prepared. Do not publish a prepared payment until its opening is saved.
2. Stop the node. Configure the relevant KMS credentials. For leadership, use a
   `LeadershipKey` with the nullifier and staking secrets and map its two public
   keys in `wallet.leadership_keys`. Spending credentials are optional. Program
   notes use `ZkKey::zero().nullifier_key()` as the nullifier secret, not zero.
3. Add the opening to `wallet.historical_recovery`. Each entry contains `note`
   (the complete serialized `Note`, unchanged from the prepared response) and
   `note_set` (the serialized `NoteSetIndex`: Ledger, Sdp, or its specific channel).
   For example, a Ledger entry has this shape:

   ```yaml
   wallet:
     historical_recovery:
       - note_set: Ledger
         note: # insert the complete serialized opening here
   ```

4. Restart the node with the same persistent storage. The wallet validates its
   configured authority, saves new requests and both opening namespaces in a
   confirmed atomic write, then reconstructs the note sets. Wallet HTTP routes
   return `503` while the wallet is starting; leadership and wallet-dependent
   services wait for readiness. This is maintenance, not concurrent online replay.
5. Wait for wallet readiness and the log message
   `Wallet historical recovery checkpoint confirmed`. Spent/withdrawn and
   immature notes retain their status; inclusion does not make them eligible.
   Leadership-only notes never enter spending balances or funding candidates.

## Authority coverage and restart

Requests identify a note-set membership **and an authority namespace**:
spending or leadership. A note configured for both has separate requirements.
Adding a legitimate authority later saves its missing opening and makes its
reconstruction pending, even if the other authority was already recovered.
Completion checks each required wallet separately. Leadership never creates
spending authority, and PK0 never receives a spending requirement.

Repeating an unchanged, completed requirement does not schedule reconstruction.
Completed configuration entries may remain or be removed. A confirmed LIB
checkpoint stores both wallet states and the exact request IDs it covers in one
write. An obsolete asynchronous snapshot cannot undo that coverage. Openings
above LIB are rediscovered during ordinary startup backfill from that checkpoint.

Completed coverage records successful reconstruction at that time, not a promise
that the membership will remain canonical forever. A later reorg can remove it
or change its eligibility. Such a completed request does not obstruct recovery
of other notes, and does not restore an orphaned membership.

## Correct or withdraw pending work

Removing an entry from `historical_recovery` alone **does not cancel** its durable
pending requirements. This preserves interrupted recovery across restart.

1. Stop the node and find the pending requirement in the wallet log:
   `Wallet historical recovery request pending`. Each line gives `request_id`,
   `authority`, and `note_id` (note set and commitment). These stable IDs are also
   logged when resuming previously persisted work, before reconstruction begins.
2. Remove the incorrect opening from `historical_recovery`. Add its pending
   request ID to `wallet.historical_recovery_withdrawals`. If the opening required
   both authorities, withdraw both pending IDs when cancelling the whole request;
   withdrawing one ID preserves the other requirement.
3. For a correction, add the valid replacement opening and note set to
   `historical_recovery`, with its legitimate configured credentials. For example:

   ```yaml
   wallet:
     historical_recovery_withdrawals: [2] # intended pending ID from the log
     historical_recovery:
       - note_set: Ledger
         note: # insert the corrected complete opening
   ```

4. Restart with the same persistent state. Request dispositions, replacement
   requirements, and their new authority openings are saved in one confirmed
   atomic batch. Failed validation or persistence applies none of this batch.
   Unrelated pending work and existing opening records remain intact.
5. Wait for `Wallet historical recovery request withdrawal confirmed` for the
   targeted IDs. Withdrawal is distinct from successful reconstruction. If all
   remaining requirements already have confirmed coverage, the existing usable
   checkpoint is reused without replay or a false completion claim. Otherwise
   the wallet stays in maintenance until the remaining recovery completes.

Repeated withdrawal of the same withdrawn ID is idempotent. Unknown or completed
IDs are rejected; completed coverage cannot be withdrawn through this control.
Requesting and withdrawing the same authority requirement in one configuration
is rejected. After acknowledgement, withdrawal entries may be removed: the
disposition is durable. Remove the acknowledged withdrawal entry before explicitly
requesting that same authority again; the renewed request receives a fresh ID.

If the process stops before confirmed completion, restart retries the durable
request. There is no whole-reconstruction time limit. Unavailable blocks/events,
an opening not found in the reconstructed history, or a persistence failure are
reported as errors and keep the wallet unavailable. No partial reconstruction is
reported as completed. Restore the required retained data or correct the
operator's configuration using the explicit procedure above before retrying;
this feature does not recreate pruned history. Reservations remain ephemeral
under the existing restart contract.
