# WP2C queued-request baseline investigation

Status: **inconclusive** for both IBD and runtime orphan recovery. No WP2C
real-node scenario has been run. The findings below are source inspection and a
focused characterization test, not proof of either system-level outcome.

## Gate 0: invariant and baseline

- Experimental branch: `ho_eras_cucumber_feature`, HEAD
  `58b232dc1eeca5fd838871d31f6679fb91bf6cef`. Upstream `master` observed at
  `0c0c99bee1cabbe607486e8014888b6372774a69`. PR #4026's merge commit
  `035118cfbd788d7f9102c5398552b9c52704d589` is an ancestor of HEAD.
- [Issue #3902](https://github.com/logos-blockchain/logos-blockchain/issues/3902)
  requests recovery for queued F0 downloads after both peers rotate. The
  [RFC's transition-period rules](https://github.com/logos-co/logos-lips/blob/Bedrock-RFC-Eras/docs/blockchain/raw/bedrock-eras.md#era-transition-period)
  reserve old identifiers for in-flight work and forbid new old-era requests.
- A selected request must be queued on the requester under F0, remain unpolled
  until the requester and selected provider have rotated to F1, then attempt
  the captured F0 protocol before transition expiry. The actual failure must
  reach the appropriate downloader and recovery must occur autonomously.
- Disable tip polling and uncontrolled publication on the requester; constrain
  peer selection and distinguish initial IBD from runtime orphan download.
  An independent barrier must establish both participants' rotations before
  release. A test-side re-enqueue, republication, restart, or unrelated download
  cannot establish recovery.
- `SyncProtocolState::request_blocks` captures the protocol and inserts the
  future into `sending_blocks_requests`. `OldEra::new` retains ongoing
  requests while dropping the incoming-stream listener. The selected future
  calls `open_stream` when first polled; an opening error drops its reply sender.
  The existing requester-start observation occurs after successful opening and
  therefore cannot establish this queued or failed-opening lifecycle.

A focused two-swarm test queues one request before rotating both behaviours,
then observes the reply channel closing after the swarms resume polling:
`rtk cargo nextest run -p logos-blockchain-cryptarchia-sync --lib queued_old_era_request_loses_reply_after_both_swarms_rotate`
(one passed, 26 skipped). It does not record the precise stream-opening error,
exercise the production downloaders, or prove a real-node recovery outcome.

## IBD

The required invariant is autonomous replacement under F1 and completion of
IBD, with convergence, finalized ancestry and health independently checked.
Source inspection shows an outer IBD loop that re-collects unsynced tips after a
round delay. Whether it recovers from this exact stale-protocol opening failure
has not been established. No selected request, opening outcome, F1 replacement,
or real-node evidence path exists yet. Result: **inconclusive**.

## Runtime orphan

The required invariant is autonomous recovery of the original missing-ancestry
orphan without a test-side trigger. Source inspection shows that a dequeued
orphan enters `Requesting`, and an error starting the download sets the state to
`Idle`. The feature-independent characterization test
`runtime_orphan_request_start_failure_after_rotation_leaves_downloader_idle`
exercises that state transition using a mock adapter with no response. It does
not establish that a real old-protocol refusal reaches the same path or that
other node mechanisms cannot recover the orphan. Result: **inconclusive**.

The focused test passed with:
`rtk cargo nextest run -p logos-blockchain-chain-network-service --lib runtime_orphan_request_start_failure_after_rotation_leaves_downloader_idle`
(one passed, 45 skipped). This test asserts current behavior, not the desired
recovery invariant, and must be revised if a production correction is made.

If a decisive run confirms the hypothesis, investigate retaining the original
request context across the `Requesting` state and scheduling a bounded retry
only for the transition-related failure. The production correction and its
permanent tests require separate review; no correction is included here.

## Remaining Gate 1 and Gate 2 work

Implement an explicitly configured, feature-gated hold around the selected
future in the actual pending collection. Preserve the future's captured F0
protocol and observe queue insertion, release, first poll, and real opening
result. Record requester and provider rotations locally and establish the
cross-node order through orchestration, not clocks. Extend lifecycle verification
to reject missing, duplicate, ambiguous and contradictory events. Add both
scenarios with independent finality, convergence, and health checks, and run
each exactly once with the native runner and retries disabled. Preserve typed
evidence and classify each outcome independently before proposing a fix.
