# Run security-audit scenarios

Run these commands from the repository root. `security_audit_v1.feature` covers
invariant 1. Future individual invariants use `security_audit_v2.feature`,
`security_audit_v3.feature`, and so on; combined-invariant experiments use
`security_audit_mixed_1.feature` and subsequent numbered names. The
`@security_audit` tag selects the audit suite, and `@security_audit_v1` selects
invariant 1.

## Invariant index

- [Invariant 1: Same-parent, same-slot sibling proposals](#invariant-1-same-parent-same-slot-sibling-proposals)

## Invariant 1: Same-parent, same-slot sibling proposals

The [V1 scenarios](../tests/cucumber_tests/features/security_audit_v1.feature)
exercise the [attack invariants](ATTACK_INVARIANTS.md).

### Build and select the node

```bash
cargo build --locked --release -p logos-blockchain-node --features testing,security-audit
sha256sum target/release/logos-blockchain-node
export LOGOS_BLOCKCHAIN_NODE_BIN="$PWD/target/release/logos-blockchain-node"
export CUCUMBER_RETRIES=0 MAX_CUCUMBER_CONCURRENT_SCENARIOS=1
```

If Cargo uses a different target directory, select its release executable
instead. The environment variable selects this prebuilt node; Cargo features
passed to the test crate cannot alter an existing executable. Retain the build
command, source revision, executable path and SHA-256 with the results. Reports
record the selected executable's path and hash, but do not independently
establish all of its compiled features.

### Run the scenarios

Use `--features cucumber,security-audit` for the test crate. The Cargo options
come before `--`; Cucumber selection options come after it. Run the following
five names one at a time, in order, with the command below. Run the sustained
attack before the control. `--name` is a Cucumber name filter; verify that each
invocation actually executed the intended single scenario.

```bash
cargo test --locked -p logos-blockchain-tests --test cucumber \
  --features cucumber,security-audit -- \
  --name 'Honest peers validate valid same-slot sibling blocks'
```

1. `Honest peers validate valid same-slot sibling blocks`
2. `Honest peers validate nonempty same-slot sibling blocks`
3. `Honest nodes converge and finalize after nonempty sibling equivocation`
4. `Honest peers recover after sustained nonempty sibling equivocation`
5. `Honest peers under equivalent workload without sibling equivocation`

Replace the name in the command for each invocation. For the last scenario,
set `SECURITY_AUDIT_MATCHED_ATTACK_REPORT` to the **current run's** sustained
attack report:

```bash
export SECURITY_AUDIT_MATCHED_ATTACK_REPORT="$PWD/tests/cucumber_tests/temp/cucumber_artefacts/Security audit/Honest_peers_recover_after_sustained_nonempty_sibling_equivocation/attempt_1/security-audit-sustained-evidence/active-observations.json"
cargo test --locked -p logos-blockchain-tests --test cucumber \
  --features cucumber,security-audit -- \
  --name 'Honest peers under equivalent workload without sibling equivocation'
```

Confirm
that report identifies a verified positive canonical block count and retains
the matching configuration and binary provenance. The control checks the
participant roles, canonical reference, configured initial-peer topology,
Blend providers, readiness deadline, active timeout, sampling interval and
permitted tip skew as well as the
consensus and workload settings. Missing configuration is unverified. The control checks those
properties before observing its own chain. A report with unavailable ancestry
or an unverified count is not a valid matching target.

For other invariant or mixed features, Cucumber's `--tags` filter can select
their tags; run dependent scenarios separately in the order their feature
specifies. Do not run the matched control before its attack report exists.

### Preserve and interpret evidence

The runner writes scenario artifacts under
`tests/cucumber_tests/temp/cucumber_artefacts/<feature>/<scenario>/attempt_1/`.
They include node logs, generated configuration and audit JSON reports. Keep
the reports and complete node runtime directories for passes and failures. Set
no `CUCUMBER_REMOVE_ARTEFACTS_IF_SUCCESSFUL` value when evidence is needed.

The runner appends JUnit output to
`tests/cucumber_tests/temp/cucumber-output-junit.xml`. After **each** invocation,
move that file to a uniquely named file under the security-audit artifacts
directory before starting the next one. Otherwise multiple XML documents may
be appended to the same file. Preserve any existing JUnit file before the
first run. Before retrying a scenario, move its existing scenario artifact
directory to a unique archive path so the next attempt cannot mix old logs
with new evidence. Retain the original report paths and hashes when archiving;
paths embedded in a moved report refer to their original locations.

Record the exit status, individual JUnit result, source revision, executable
hash, effective configuration and report paths. A pass requires the evidence
and acceptance checks in the relevant scenario, not merely a process that
started. Insufficient stochastic exposure, missing ancestry or body evidence,
and unavailable matching reports are distinct from observed consensus or
finality conflicts. Preserve and report failures and unverified evidence; do
not infer rejection, canonical selection or security guarantees from missing
observations.

The brief finality scenario stops the producer when an honest node receives a
selected sibling through Blend. Its deadline is measured from opportunity
selection and includes shutdown. If reception is not observed by the deadline,
the producer is stopped for cleanup and the trigger is reported as unverified.
The `shutdown-trigger.json` artifact records the event and timing evidence.
