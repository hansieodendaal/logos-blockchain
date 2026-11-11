# Era transition experiments

This directory is the durable index for experimental system tests of RFC 439
era transitions. It begins with issue #3902 and is intended to support later
transition investigations without tying the experiments to one scenario.

## Architecture

The `testing-era-boundaries` Cargo feature adds narrowly scoped event
instrumentation and the provider response hold. It is disabled by default.
When enabled without a runtime control, chain-sync follows its ordinary
response path. A configured control is enabled per node through that node's
effective deployment configuration. The hold affects a real provider block
response stream after successful sends, is one-shot, and is cancelled by
dropping the old-era request future at transition expiry.

The experiment uses real nodes, normal fork-digest protocol identifiers, the
production IBD path, normal block validation, and the existing Cucumber
scenario artifact tree. The Cucumber participant table assigns provider and
requester roles, and the framework associates each journal with the registered
node and peer. Feature-gated producers write typed events directly to a
per-node NDJSON journal. Cucumber reads each journal incrementally and rejects
malformed records, sequence gaps, truncation, and incomplete final records.
Within a node the journal sequence establishes order. Cross-node correlation
uses peers, protocol, request context, local request IDs, response index and
observed block identity; wall-clock times do not establish causality. A request
context is not a globally unique ID. Human-readable logs are diagnostic only.

The tagged scenario uses the dedicated `release-era-boundary-recovery` profile
and rejects incompatible `LOGOS_BLOCKCHAIN_NODE_BIN` overrides. Runtime hold
observations and the existing publication-state API establish the effective
controls. The `testing-era-boundaries` feature enables the controls but does
not itself suppress publication; the scenario configures suppression per node,
verifies it, and restores publication afterward.

## RFC and issue scope

The experiments follow [RFC 439, Bedrock Eras](https://github.com/logos-co/logos-lips/blob/Bedrock-RFC-Eras/docs/blockchain/raw/bedrock-eras.md).
The scenario records its specification reference. The PR 1
regression establishes expiry interruption, automatic IBD recovery under the
new fork digest, partial-progress retention, canonical convergence, finality
continuity, and participant health. It does not change consensus, fork choice,
wire format, or block validation.

The initial issue is [#3902](https://github.com/logos-blockchain/logos-blockchain/issues/3902).
Further experiments may cover other RFC 439 transitions. Their Cucumber
features and Rust instrumentation remain in the repository's existing test
and production locations; this directory holds documentation, invariant
tracking, runbooks, tools when needed, and analytical reports.

## Branch and evidence policy

The experimental branch is permanent and is not intended for wholesale merge
upstream. Extract suitable production corrections and permanent tests into
separate issue-specific branches. Leave changes uncommitted until the branch
owner reviews them.

Keep journals and scenario evidence in Cucumber's existing per-attempt
directory. Evidence records reference the participant journal and sequence;
chain-state APIs independently establish retained blocks and ancestry. Missing
or ambiguous observations are unverified and fail the scenario. Protocol
evidence links matching requests and responses; it does not prove the relative
wall-clock order of otherwise unrelated events on different nodes. Artifact
preservation is handled manually.

## Directory contents

- [INVARIANTS.md](INVARIANTS.md) maps the #3902 checklist to regressions and
  evidence requirements.
- [RUN_SCENARIOS.md](RUN_SCENARIOS.md) documents reproducible builds, Cucumber
  commands, artifact handling, and diagnostics.
- `tools/` and `reports/` are reserved for concrete utilities and analytical
  reports and will be created only when there is content to place in them.
