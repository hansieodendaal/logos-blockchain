# Era-transition invariant catalogue

## Status meanings

- **Verified by reported real-node run**: the named scenario was reported to
  pass with retries disabled and typed evidence verification. This catalogue
  records that report; it does not claim a new execution by this documentation
  change.
- **Verified by focused test**: a focused unit/verifier test exercises the
  stated behavior, without establishing the complete behavior in a real node.
- **Implemented, not independently verified**: code or an assertion exists,
  but decisive evidence is not available here.
- **Partially covered**: a regression establishes a specific case but not the
  broader behavior named in the row.
- **Not yet tested**: no decisive regression establishes the behavior.
- **Outside this experiment**: another issue or workstream owns the coverage.

The four existing real-node scenarios were reported passing individually with
retries disabled and typed evidence verification. The catalogue keeps each
claim within the topology and lifecycle that its scenario actually exercises.
Missing or contradictory observations fail verification; they are not
evidence that an event did not occur.

## Demonstrated scenarios

| Scenario | Status | What the regression establishes |
| --- | --- | --- |
| WP1 — `IBD recovers after an in-flight old-era block request expires` | Verified by reported real-node run | A requester starts an F0 IBD request before activation, receives and applies partial progress, remains unfinished through transition grace, observes expiry interruption, automatically replaces the request under F1, completes IBD, retains valid progress on canonical ancestry, and converges with finality continuity and healthy participants. |
| WP2A — `An unselected F0 request completes while a selected concurrent request expires` | Verified by reported real-node run | Two distinguishable F0 requests overlap at one provider. The selected requester is held after partial progress and automatically recovers after expiry; the unselected requester completes normally before expiry. This does not establish every concurrent-expiry or same-context collision case. |
| WP2B — `A runtime orphan download recovers after its F0 request expires` | Verified by reported real-node run | After initial IBD, a genuine missing-ancestry block enters the normal orphan downloader. The active F0 ancestry request makes partial progress, is interrupted at expiry, and autonomously continues under F1. The original orphan is resolved without a test-side re-enqueue, republish, restart, or alternate recovery trigger. |
| A2–A4 — `Explicit interactions isolate the selected request across multiple providers and requesters` | Verified by reported real-node run | Two providers and two requesters participate in separately declared selected and control interactions. The selected request is faulted and recovers; the independent control request completes normally. This demonstrates isolation for that topology, not all provider/requester topologies or every same-peer request collision. |

## Shared contracts established by the framework

| Invariant | Status | Evidence and limit |
| --- | --- | --- |
| Participant and interaction bindings are explicit | Verified by reported real-node runs | Participant declarations bind named nodes to runtime directories and peer IDs. Interaction declarations bind provider/requester pairs; exactly one interaction is selected for fault control, and exactly one participant is the canonical reference. Missing, duplicate, conflicting, or invalid role bindings fail validation. |
| Selected request is correlated to the provider-local hold identity | Verified by reported real-node runs; selector edge cases verified by focused tests | Configured provider/requester peer, protocol, target, requester tip, latest immutable block, and additional-context fields are conjunctive filters. The one-shot runtime control is claimed by the first eligible matching stream; it does not census candidates before the claim. The provider hold event identifies the actual local request faulted, and lifecycle matching maps it to the requester using directional peers and full context without comparing local IDs. Identical observable contexts cannot be selected semantically; the verifier rejects ambiguous evidence, so the scenario fails closed. |
| Request IDs are local to their emitting node | Verified by focused tests and reported multi-interaction run | The verifier binds each ID to its node and correlates participants using directional peer IDs and complete relevant request context. Request context alone is not globally unique, and matching IDs across peers are never required. |
| Per-node observation order is authoritative | Verified by reported real-node runs | Typed journal sequence numbers establish local order. Wall-clock timestamps are diagnostic only and do not establish cross-process causality. Local monotonic elapsed time may measure local durations. |
| Evidence collection and verification fail closed | Verified by focused tests and reported real-node runs | Missing journals, malformed or incomplete records, sequence gaps, missing lifecycle events, ambiguous matches, and contradictory outcomes fail. Human-readable logs are diagnostic only. |
| Configuration, runtime controls, health, convergence, and finality are checked | Verified by reported real-node runs | Effective configuration and capabilities are captured; runtime publication state and hold observations are checked; canonical state, finality ancestry, and participant health are independently asserted for the scenario topology. |

## #3902 coverage boundaries

| Case | Status | Coverage boundary |
| --- | --- | --- |
| Selected F0 request expires after partial progress and recovers under F1 | Verified by reported real-node runs | WP1, WP2A, and A2–A4 cover IBD; WP2B covers normal runtime orphan ancestry recovery. Their lifecycle assertions remain distinct. |
| Another concurrent request completes normally while the selected request is held | Verified for the declared scenarios | WP2A uses one provider and two requesters; A2–A4 uses two provider/requester interactions. Neither proves all simultaneous expiry patterns. |
| Successful old-era response service during transition grace | Not yet tested | Existing regressions keep the selected F0 request unfinished through grace and cancel it at expiry; they do not establish successful F0 completion during grace. |
| Interruption before the first response block | Not yet tested | WP1 and WP2A preserve one response before the hold; WP2B preserves multiple blocks. No regression establishes the zero-progress interruption case. |
| Same-peer competing requests with different observable contexts, end to end | Partially covered | Focused selector/verifier tests distinguish contexts and reject identical-context ambiguity. Existing real-node scenarios demonstrate peer-targeted selection and competing traffic, but not every same-peer request competition topology. |
| Identical observable requests distinguished by an external/global request identity | Not supported | The wire protocol provides no such identity in this framework. Identical candidates fail closed instead of relying on arrival order or inventing a cross-node ID. |
| F0 work queued but first polled after rotation | Not yet tested | No scenario currently establishes queue insertion, delayed polling, and selected protocol. |
| Outstanding `GetTip` request crosses expiry and recovers | Not yet tested | No scenario exercises this request lifecycle. |
| New F0 request rejection after rotation | Outside this experiment | Tracked separately by issue [#3854](https://github.com/logos-blockchain/logos-blockchain/issues/3854); do not infer it from these recovery cases. |
| Clock-skew-specific transition behavior | Outside this experiment | Tracked separately by issue [#3869](https://github.com/logos-blockchain/logos-blockchain/issues/3869); synchronized test runs do not establish it. |

## Evidence interpretation

Within-node event order is established by the journal sequence. Across nodes,
the verifier matches protocol identities and request context with the correct
peer direction; timestamps do not prove that one process's event preceded
another's. Provider response observations record request context and response
index, but not the response block ID. The requester independently records its
decoded/applied block identity, so the framework does not claim provider-side
block identity equality.

Convergence alone does not prove automatic recovery. Eventual health does not
prove that no request remained stranded. Provider cancellation alone does not
prove requester interruption. Each scenario requires its own selected-request
lifecycle and recovery evidence in addition to chain-state checks.
