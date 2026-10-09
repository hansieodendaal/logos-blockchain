# Security audit log analysis

`summarize.py` reads producer and receiver logs and writes a JSON report plus a
CSV timing table. It uses only the Python standard library and does not copy or
rewrite input logs.

## Run

Pass each log as `NODE=PATH`. Supply one or more half-open UTC/offset ISO-8601
windows with `--window START END`; without windows, all parsed events are used.
Windows should not overlap when comparing sample counts. Set
`--expected-siblings` to validate the configured count. When logs include
honest nodes with a different setting, use `--producer-node` to scope that
check to the producer's `NODE=PATH` label. The output directory is created if
needed.

```bash
python3 .security_audit_tools/summarize.py \
  --log NODE_1=/path/to/producer.log \
  --log NODE_2=/path/to/honest-peer.log \
  --window 2026-10-11T10:00:00Z 2026-10-11T10:02:00Z \
  --expected-siblings 4 \
  --producer-node NODE_1 \
  --commit-sha 0123456789abcdef \
  --binary-sha256 <sha256> \
  --config-sha256 <sha256> \
  --feature security-audit,testing \
  --scenario 'Honest peers validate valid same-slot sibling blocks' \
  --node-identity NODE_1=<producer-peer-id> \
  --node-identity NODE_2=<receiver-peer-id> \
  --output-dir /tmp/security-audit-summary
```

The report contains each leadership opportunity, its timing fields, sibling
identity events and receiver results grouped by node. `timings.csv` contains
minimum, median and maximum values with a sample count for each timing metric.
Anomalies are written to JSON and produce exit status 1 unless
`--allow-anomalies` is passed. The parser reports missing/incomplete timing and
identity records, duplicate IDs or indexes, parent/slot mismatches, relay
enqueue failures, and inconsistent event counts.

## Evidence semantics

- `relay_enqueued` records only that the producer's local Blend relay accepted
  the enqueue request. It is never treated as proof of peer reception.
- Receiver events are correlated within the selected measurement window by
  sibling ID, receiving node, producer parent ID and slot. Each receiver has
  independent Blend reception, Chain Network receipt, reconstruction,
  application and storage fields. Parent/slot mismatches are reported and
  excluded from the matching receiver path.
- `accepted_after_blend_reception` requires one honest node to show a
  timestamp-ordered sequence of Blend decapsulation, Chain Network proposal
  receipt, reconstruction and successful consensus application. Equal
  timestamps establish order only when the events are in the same file and
  their line positions are increasing. Missing timestamps, ambiguous ties,
  reverse order, and stages split across peers cannot produce a successful
  path. Partial observations remain `not_observed`; they are not treated as
  confirmed rejection.
- The aggregate sibling outcome is derived from complete per-node results.
  Aggregate stage-presence fields remain for compatibility, but must not be
  used alone to claim an accepted Blend delivery. `accepted_after_blend_reception`
  is also exposed in the summary.
- A stored block does not prove that it arrived through Blend. Establishing
  that path requires a matching receiver Blend-decapsulation record as well as
  later processing evidence. `accepted` does not imply canonical selection;
  canonical status is shown only if an explicit storage event records it.
- The analyzer correlates sibling identity and leadership records by producer
  node, parent ID and slot, then verifies the ordinary ID shared by the sibling
  identity records. Receiver events are correlated by window, sibling ID,
  receiving node, parent ID and slot. An accepted stage on one peer cannot be
  combined with Blend reception on another peer.

These logs cannot establish that an unobserved peer never received a proposal;
they establish only which stages were observed in the supplied logs and time
windows. They also do not prove network transmission from a local enqueue
boolean.

The earlier release-node 0/1/3 measurements were replayed from the original
producer logs on the originating host. The analyzer reproduced the counts and
medians exactly:

| Configured siblings | Winning slots | Median additional processing |
| ---: | ---: | ---: |
| 0 | 10 | 0 ms |
| 1 | 9 | 0.552253 ms |
| 3 | 4 | 1.2610905 ms |

Those original logs live outside the repository at
`/tmp/logos-audit-runtime-82a6465-r9wjjlz3/count-{0,1,3}-node-0.log` on the
originating machine; they are not part of the tracked fixture. Replay one
producer log at a time with the matching `--expected-siblings` to preserve the
original sample count. Add honest-node logs when receiver evidence is needed;
`--producer-node NODE_1` scopes expected-count validation while receiver events
remain correlated by node.

## Provenance

Pass provenance explicitly when known: commit SHA, release binary SHA-256,
configuration SHA-256, enabled feature set, scenario name and each node's
identity. Missing provenance stays `null` in `measurements.json`; the tool does
not guess it from filenames or Git state.

## Tests

```bash
python3 -m unittest discover -s .security_audit_tools/tests -v
```

The sanitized fixture covers zero, one and three sibling opportunities and a
complete ordered one-peer receiver path. Synthetic tests cover reverse event
order, split stages across peers, missing reconstruction, parent/slot
mismatches, repeated stages, measurement-window isolation, duplicate/missing
identities, failed local enqueue, incomplete records, summary statistics and
machine-readable output.
