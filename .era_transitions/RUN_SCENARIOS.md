# Run era-transition scenarios

Run from the repository root. Use the repository's Cucumber wrappers; they
select the correct test invocation and set up the shared Logos environment.
For blockchain Cucumber runs, `--node-binary auto` lets the framework build
and select the node executable with the profile's features.

## PR 1 expiry and recovery regression

The focused scenario is named **IBD recovers after an in-flight old-era block
request expires** and tagged `@era_boundary_recovery`. Run it serially with
retries disabled:

```bash
.agents/cucumber_scripts/rtk-cucumber-scenario.sh \
  'IBD recovers after an in-flight old-era block request expires' \
  --features testing-era-boundaries \
  --node-binary auto \
  --concurrency 1 \
  --retries 0
```

The recovery profile builds `logos-blockchain-node` with
`testing-era-boundaries` under the
`release-era-boundary-recovery` profile. To build that exact node executable
manually for inspection:

```bash
cargo build --locked --profile release-era-boundary-recovery \
  -p logos-blockchain-node \
  --features testing,testing-era-boundaries
```

The expected executable is
`target/release-era-boundary-recovery/logos-blockchain-node`. During a named
Cucumber run, let the wrapper and framework select the binary. The scenario
rejects `LOGOS_BLOCKCHAIN_NODE_BIN` overrides for the recovery profile.
The feature makes the runtime controls available; it does not activate them.
The scenario explicitly configures and verifies publication suppression, then
enables it again. Runtime hold observations establish that the hold operated.

## Visible scenario configuration

The feature file declares participants and roles, the canonical-reference
node, the era schedule, provider hold count, IBD peer, disabled requester tip
polling, proposal-publication suppression, and long peer-response and inbound
stream timeouts. Effective `node.yaml` and `deployment.yaml` files are
captured per participant. At run end, evidence checks confirm these timeouts
exceeded the observed transition grace, the request held one outstanding
provider response at expiry, and the requester saw a stream error rather than
a response timeout.

The scenario uses two real nodes in the current case. Additional observer
participants can be declared and started through the shared participant
steps; later experiments can increase cluster capacity and reuse the same
role, attribution, evidence, and convergence logic.

## Evidence locations and contents

Raw runtime artifacts remain under Cucumber's existing tree:

```text
tests/cucumber_tests/temp/cucumber_artefacts/
  Era boundary recovery/
    IBD_recovers_after_an_in-flight_old-era_block_request_expires/
      attempt_1/
```

The attempt directory contains each node's `era_observations.ndjson`, runtime
logs and effective configuration, plus `era_boundary_recovery_evidence.ndjson`
and the shared `era_boundaries_evidence.ndjson`. The typed journals are read
incrementally and records are attributed to their registered node. Their
per-node sequence numbers establish local order; corruption, sequence gaps,
truncation and incomplete final records fail evidence collection. Human-readable
logs are retained for diagnosis and do not determine recovery assertions.

The provider's send event records request context and response index but not a
block ID. The requester records its decoded/applied block ID. Those
observations are correlated by the original request, peer, protocol, target,
and response index; the test does not claim a cross-node block-ID comparison.
Request keys are request context, not globally unique identities. Ambiguous
matching lifecycles fail.

## Artifact handling

Artifact preservation is handled manually. No experiment code archives,
renames, rotates or deletes historical attempts.

## Independent acceptance evidence

A passing scenario must have all of these independently recorded:

- Original requester F₀ request and matching provider acceptance before both
  participants rotate.
- Provider send and requester receive/decode/apply for response index 1;
  requester IBD `Downloading` state for the same target.
- Provider hold after one successful send with more response work pending;
  one response still pending at provider rotation and expiry, with both nodes
  independently reporting their F₁ rotations.
- Provider transition expiry before provider hold cancellation; the requester
  independently reports a stream interruption rather than a response timeout.
- An automatically initiated F₁ replacement and matching provider acceptance,
  followed by normal provider and requester stream completion and IBD
  completion.
- The first block remains available and belongs to the replacement requester's
  tip ancestry; the original synchronization target remains in final canonical
  ancestry.
- Fresh canonical convergence evidence and LIB ancestry continuity for every
  participant, plus health observations until ordinary cleanup.
- Effective configuration and runtime state evidence of proposal suppression
  for all declared participants, followed by restoration.

Per-node sequence numbers establish local order. Cross-node request and
response identities establish protocol relationships; timestamps do not prove
cross-process ordering. The provider does not observe the response block ID,
and no cross-node block-ID comparison is claimed.

## Diagnose failures

Classify the earliest missing or contradictory evidence: participant/table
validation, missing journal or runtime identity, request rejection, wrong fork digest, no first response, no
requester `Downloading` state, hold not activated, ordinary timeout before
expiry, missed activation, no outstanding work during grace, cancellation
failure, requester interruption failure, F₁ replacement missing/rejected,
request completion failure, IBD failure, partial-progress loss, original
target unresolved, convergence failure, finality discontinuity, participant
health failure, or evidence corruption/incompleteness.

Inspect only that attempt's runtime directories, effective YAML files,
timeline, and journals. Preserve the attempt manually before a diagnostic
rerun using the procedure above. A single passing run is not evidence of
determinism; report each execution separately with its own scenario evidence.
