# RFC 439 era-transition experiments

This directory documents a long-lived experimental testing facility for RFC 439
era transitions and related issue [#3902](https://github.com/logos-blockchain/logos-blockchain/issues/3902).
It records what the current regressions establish, how to run them from the
repository, and which protocol cases remain uncovered.

## Current regressions

The Cucumber feature contains four real-node scenarios:

1. **WP1 — IBD recovery:** an accepted F0 request makes partial progress, stays
   unfinished through transition grace, expires, and is automatically replaced
   under F1. The scenario checks retained progress, IBD completion, convergence,
   finality continuity, and participant health.
2. **WP2A — Concurrent requests:** a selected request is held while an
   unselected request completes normally. At expiry, the selected requester
   recovers under F1. This covers the declared pair of concurrent requests, not
   every possible concurrency pattern.
3. **WP2B — Runtime orphan recovery:** after initial IBD, a genuine block with
   missing ancestry starts the normal orphan downloader. Its F0 request is
   interrupted at expiry and autonomously replaced under F1, resolving the
   original orphan without a test-side retrigger.
4. **A2–A4 — Multiple interactions:** two providers and two requesters create a
   selected faulted interaction and an independent control interaction. The
   control traffic completes normally while the selected interaction recovers.

The feature file states each scenario's topology and distinguishing invariant.
The invariant catalogue describes the limits of the evidence and the remaining
#3902 cases.

## Architecture and evidence

The `testing-era-boundaries` Cargo feature gates experimental observation and
provider-response-control code. Enabling the feature makes those mechanisms
available; it does not activate a fault. A per-node runtime control must be
configured for a hold to operate. Without that configuration, provider traffic
uses the ordinary response path. The tagged tests use the dedicated
`release-era-boundary-recovery` node profile, and the runner rejects an
incompatible `LOGOS_BLOCKCHAIN_NODE_BIN` override for that profile.

Scenario declarations describe **participants** and their runtime node/peer
bindings separately from **interactions**, which identify a provider/requester
pair and the one interaction selected for fault control. The provider hold
applies all configured provider/requester identities, protocol, target,
requester-tip, latest-immutable, and additional-context filters conjunctively.
Its one-shot claim is made by the first eligible stream that matches those
filters. If simultaneous streams have identical observable context, the
selector cannot choose a semantic request identity; the lifecycle verifier
rejects ambiguous evidence, so such a run cannot pass. A configured selector
excludes nonmatching traffic, but the implementation does not perform a
pre-claim census of matching streams.

Feature-gated producers write typed NDJSON observations to one journal per
node. A local request ID belongs only to its emitting node. Cross-node matching
uses directional peer identities and complete relevant request context; it
never assumes IDs match across nodes or that context is globally unique.
Monotonic journal sequence numbers establish order within a node. Wall-clock
timestamps are diagnostic and do not establish cross-node causality. Provider
response observations currently omit block IDs, so the evidence does not claim
provider/requester block-ID equality. Human-readable logs are diagnostic, not
authoritative evidence. Missing, malformed, ambiguous, or contradictory typed
evidence fails verification.

The runtime orphan recovery scenario retains the WP2B production correction
that resumes orphan ancestry recovery after an interrupted stream. This is
production behavior exercised by the regression, not an experimental fault
control. Review and extraction of that correction into a separate upstream
change are deferred; the test facility does not redesign era activation,
expiry, synchronization, consensus, fork choice, or block validation.

## Branch and evidence policy

The experimental branch is intended to remain long-lived and regularly
incorporate upstream changes. Keep experimental integration hooks narrow and
rebase-friendly. Suitable permanent production corrections and tests should be
extracted into separate issue-specific changes when reviewed. Do not wholesale
merge the experimental facility upstream without that review.

Each attempt writes evidence in Cucumber's existing per-scenario artifact
tree. Evidence references participant journals and sequence numbers; chain
state APIs independently establish retained blocks and ancestry. Cross-node
request correlation establishes protocol relationships, not relative ordering
from process clocks. Artifact preservation follows the existing manual policy.

## Directory contents

- [INVARIANTS.md](INVARIANTS.md) maps demonstrated coverage and limitations.
- [RUN_SCENARIOS.md](RUN_SCENARIOS.md) documents repository-supported build,
  run, artifact, and diagnostic procedures.
