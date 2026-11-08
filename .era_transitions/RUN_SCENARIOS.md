# Run era-transition scenarios

Run these commands from the repository root. They use the tracked Cargo
Cucumber runner and require no personal wrapper, alias, or file outside the
checkout.

## Prerequisites and binary selection

- Use the Rust toolchain selected by the repository's `rust-toolchain.toml`.
- Start from a clean build environment with the normal native dependencies
  required to build the workspace and node executable.
- Keep `LOGOS_BLOCKCHAIN_NODE_BIN` unset. The era-boundary Cucumber runner
  selects and builds the `release-era-boundary-recovery` node profile with
  `testing` and `testing-era-boundaries` enabled. The scenario rejects an
  incompatible executable override.
- Run with local networking and resources available for the Cucumber-managed
  real nodes. The scenarios declare no Blend providers.

The Cucumber test package needs both the `cucumber` and
`testing-era-boundaries` Cargo features. The Cargo runner handles selection of
the feature-compatible node profile; no wrapper-specific `--node-binary`,
`--concurrency`, or `--retries` arguments are passed to Cargo.

## Run one scenario

This focused command runs WP1 with retries disabled and one scenario at a time:

```bash
unset LOGOS_BLOCKCHAIN_NODE_BIN
export CUCUMBER_RETRIES=0
export MAX_CUCUMBER_CONCURRENT_SCENARIOS=1
cargo test --locked \
  -p logos-blockchain-tests \
  --features cucumber,testing-era-boundaries \
  --test cucumber \
  -- --name "IBD recovers after an in-flight old-era block request expires"
```

## Run all four scenarios sequentially

Run the following from the repository root in one shell. Each Cargo command
finishes before the next begins; a nonzero result should be investigated before
continuing.

```bash
unset LOGOS_BLOCKCHAIN_NODE_BIN
export CUCUMBER_RETRIES=0
export MAX_CUCUMBER_CONCURRENT_SCENARIOS=1
set -e

cargo test --locked -p logos-blockchain-tests --features cucumber,testing-era-boundaries --test cucumber -- --name "IBD recovers after an in-flight old-era block request expires"
cargo test --locked -p logos-blockchain-tests --features cucumber,testing-era-boundaries --test cucumber -- --name "An unselected F0 request completes while a selected concurrent request expires"
cargo test --locked -p logos-blockchain-tests --features cucumber,testing-era-boundaries --test cucumber -- --name "A runtime orphan download recovers after its F0 request expires"
cargo test --locked -p logos-blockchain-tests --features cucumber,testing-era-boundaries --test cucumber -- --name "Explicit interactions isolate the selected request across multiple providers and requesters"
```

`CUCUMBER_RETRIES=0` disables scenario retries. The runner reads
`MAX_CUCUMBER_CONCURRENT_SCENARIOS=1` to keep scenario execution serial. Cargo's
`-- --name "..."` filters to the named Gherkin scenario; confirm the output
reports one scenario and nonzero steps, since a zero-scenario run is not
validation.

## Node profile and configuration

The tagged scenarios use the `release-era-boundary-recovery` node profile with
the `testing` and `testing-era-boundaries` node features. The feature gates the
test controls and observations; it does not itself activate a response hold.
The scenario configures and verifies per-node runtime state, including
publication controls, and restores publication during cleanup.

To build that profile explicitly for inspection (the Cucumber runner normally
selects it automatically):

```bash
cargo build --locked \
  --profile release-era-boundary-recovery \
  -p logos-blockchain-node \
  --features testing,testing-era-boundaries
```

The expected executable is
`target/release-era-boundary-recovery/logos-blockchain-node`.

## Artifacts and evidence

The default scenario artifact tree is:

```text
tests/cucumber_tests/temp/cucumber_artefacts/
  Era boundary recovery/
    <scenario_name_with_underscores>/
      attempt_1/
```

An attempt contains per-node `era_observations.ndjson` journals, runtime logs,
effective `node.yaml` and `deployment.yaml` configuration, and scenario-level
`era_boundary_recovery_evidence.ndjson` and
`era_boundaries_evidence.ndjson` records. The per-node journal sequence
establishes local order. The verifier rejects missing or malformed journals,
sequence gaps, truncated records, missing lifecycle events, ambiguous request
matches, and contradictory outcomes. Human-readable logs aid diagnosis but do
not establish acceptance.

Request IDs are local to their node. Cross-node correlation uses directional
peer identities, protocol, target and complete relevant request context; the
context itself is not globally unique. Cross-process timestamps do not
establish causality. Provider response observations do not include a block ID,
so evidence does not claim provider/requester block-ID equality.

## Diagnose failures

Start with the failed Cucumber step and the earliest failed or missing
structured assertion in that attempt's
`era_boundary_recovery_evidence.ndjson`. Follow its journal references by node
and sequence in `era_observations.ndjson`; use runtime configuration and logs
to explain what happened. Check the relevant lifecycle specifically: participant
and interaction binding, F0 request and provider acceptance, partial progress,
hold activation, activation versus transition expiry, cancellation and
requester interruption, F1 replacement and completion, retained ancestry,
convergence, finality, health, and evidence completeness.

Preserve attempts manually before a diagnostic rerun. The framework does not
archive, rename, rotate, or delete historical evidence. One passing execution
does not establish repeatability. A passing scenario proves only its declared
topology and assertions; in particular, the concurrent-request case does not
prove all concurrent-expiry behaviors, and request context cannot distinguish
requests with identical observable fields. The one-shot hold claims the first
eligible stream matching its configured fields; it does not census matching
streams before claiming. The typed lifecycle verifier rejects ambiguous
evidence, so a collision cannot count as a passing result.
