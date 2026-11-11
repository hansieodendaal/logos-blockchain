# Era transition invariant catalogue

## Status definitions

- **Implemented**: a regression or verifier exists in this worktree.
- **Verified**: a real-node run produced all required evidence.
- **Failed**: a recorded execution violated the invariant or contradicted the expected behavior.
- **Not yet tested**: no decisive regression has established the result.

A two-node, no-retry recovery run on 2026-11-12 passed the scenario and typed evidence verification. Its scenario-local evidence and per-node journals record the observed lifecycle. Missing events remain unverified, not proof that an event did not happen.

## Issue #3902 PR 1: expiry, IBD recovery, and partial progress

| Invariant | Status | Required independent evidence |
| --- | --- | --- |
| Participants and topology are explicit | Verified | Cucumber participant table; exactly one provider and requester; any observers declared; names, runtime names/directories, peer IDs, and started topology agree. |
| Requester opens the original request under F₀ before activation | Verified | Requester outbound request uses the F₀ fork digest and precedes its F₀→F₁ rotation; provider accepts the matching peer, target, requester tip, and LIB context before its rotation. |
| Provider and requester evidence refer to one unambiguous request | Verified | Unique matching lifecycle; matching peer identities, protocol, request context, and target; reused context or duplicate candidates fail verification. |
| Request enters established `Downloading` state | Verified | Requester independently receives and decodes response index 1, the IBD downloader enters `Downloading` for the request target, and the block is applied before expiry. |
| Provider successfully sends the first response and requester receives it | Verified | Provider send event and requester receive event share request context, F₀, peer identities, target, and response index. Provider response events currently do not contain the block ID; requester decode/apply records it. The test does not claim both sides observed the same block ID. |
| Provider hold is one-shot and leaves more response work pending | Verified | Provider-local event order shows the configured number of successful sends before hold activation and `a typed hold event emitted only when an additional block is pending`; the hold remains pending without completing or sending `NoMoreBlocks`; subsequent F₁ response completes. |
| Original F₀ work remains active through grace | Verified | Both nodes record F₀→F₁ rotation; provider reports one pending response at rotation and expiry; request has not completed before expiry. |
| Transition expiry cancels unfinished provider work | Verified | Provider expiry event precedes cancellation of the correlated held future; response/request capacity is later reused by the F₁ request. Effective response and inbound-stream timeouts exceed observed grace. |
| Requester independently detects interruption | Verified | Requester-side stream error for the same F₀ request, after receiving one response; an earlier response timeout fails the assertion. Provider cancellation alone is insufficient. |
| IBD automatically retries under F₁ | Verified | Requester initiates an F₁ request after interruption; provider accepts the same F₁ context from the declared peer; provider and requester observe normal replacement-stream completion; no manual re-enqueue or restart. |
| Interrupted synchronization work is resolved | Verified | The original target remains retrievable and occurs in the requester's final canonical ancestry. A replacement request may carry an advanced target or requester tip. |
| Partial progress survives recovery | Verified | First requester-decoded block ID is applied before interruption, remains retrievable afterward, and is an ancestor of both the replacement requester's tip and the final canonical tip. |
| IBD completes and nodes remain healthy | Verified | Unique IBD completion follows the accepted F₁ request and normal stream completion; all declared participants answer consensus and network APIs and remain up until ordinary cleanup. |
| Canonical histories converge | Verified | Fresh consensus snapshots and ancestry inspection record all participant tips, common ancestor, minimum height, and a bounded height difference. |
| Finality continuity is maintained | Verified | The captured non-genesis pre-boundary finalized anchor remains on every declared participant's post-boundary LIB ancestry. |
| Gossip cannot satisfy the recovery assertion | Verified | Each participant explicitly starts with proposal publication disabled and reports the disabled state before and after the recovery window; requester tip polling is disabled. |
| Structured evidence is complete | Verified | Scenario evidence references each node's typed journal and sequence; malformed, missing, ambiguous, or incomplete observations fail. |

Within-node order uses a single monotonically increasing journal sequence.
Across nodes, matching peer identities, protocols, request contexts, local
request IDs and response indices establish relationships without comparing
clocks. Provider expiry and requester interruption are independently observed;
their cross-process wall-clock order is not claimed. Local monotonic elapsed
time is used only to compare provider grace with configured timeouts. The
provider does not observe the first block ID in its response, so no cross-node
block-ID equality is claimed.

## Later #3902 cases

These are **Not yet tested** and remain outside PR 1.

| Planned case | Evidence required |
| --- | --- |
| Old-era request completes during grace | Accepted F₀ request completes before expiry; requester consumes the target; no replacement is required. |
| Interruption before the first block | Accepted F₀ request is held before response 1; expiry cancels it; requester detects interruption and its recovery is independently established. |
| Ordinary orphan synchronization recovers after a stream error | Non-IBD orphan download records the error, requeues without outside intervention, and converges under the active protocol. |
| Outstanding `GetTip` request crosses expiry | Request identity, F₀/F₁ protocol, expiry, requester observation, and successful replacement tip query. |
| Multiple concurrent requests cross expiry | Independent request identities/lifecycles; expiry neither strands streams nor leaves provider capacity occupied. |
| F₀ work queued but first polled after rotation | Queueing and stream-open evidence establish which protocol is selected after rotation. |
| New F₀ request rejection | Covered separately by issue #3854; do not infer from PR 1. |
| Clock-skew-specific transition behavior | Covered separately by issue #3869; do not infer from synchronized-clock tests. |

## Evidence interpretation

Each execution is independent and records its effective configuration and scenario attempt directory. Reports distinguish established behavior, conditional conclusions, and unverified behavior. Convergence alone does not prove automatic recovery; eventual health does not prove that no request was stuck; provider-side cancellation does not prove requester detection.
