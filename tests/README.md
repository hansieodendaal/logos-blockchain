# Tests

## Tests Debugging Setup

This document provides instructions for setting up and using the testing environment, including how to start the Docker 
setup, run tests with a feature flag, and access the Grafana dashboard.

## Prerequisites

### Using Docker

Ensure that the following are installed on your system:
- [Docker](https://docs.docker.com/get-docker/)
- [Docker Compose](https://docs.docker.com/compose/install/)

### Using Rust `cargo test`

Integration tests involving nodes run the binaries directly by spawning. Local test runs build the node binary through
the testing framework before nodes are started. CI keeps using the binary built by the workflow before the test step.

## Setup and Usage (using Docker)

### 1. Start `compose.debug.yml`

To start the services defined in `compose.debug.yml` using Docker Compose, run the following command:

```bash
docker-compose -f compose.debug.yml up -d
```

This command will:
    Use the configuration specified in compose.debug.yml.
    Start all services in detached mode (-d), allowing the terminal to be used for other commands.

To stop the services, you can run:
```
docker compose -f compose.debug.yml down   # compose filename needs to be the same
```

### 2. Access the Grafana Dashboard
> It's important that the test is performed after the docker compose is started

Once the Docker setup is running, you can access the Grafana dashboard to view metrics and logs:
    Open a browser and navigate to http://localhost:9091.

Use "Explore" tab to select data source: "Loki", "Tempo", "Prometheus". Prometheus source is unusable at the moment in 
local setup.

- Loki - to kickstart your query, select "host" as label filter, and "nomo-0" or other nodes as value, this will show 
- all logs for selected host.
- Tempo - to kickstart your query, enter "{}" as TraceQL query to see all traces.


## Setup and Usage (using `cargo test`)

Where tests involve spawning node binaries locally, the testing framework uses
`LOGOS_BLOCKCHAIN_NODE_BIN` when it is set:

```bash
LOGOS_BLOCKCHAIN_NODE_BIN=/path/to/logos-blockchain-node cargo test ...
```

If `LOGOS_BLOCKCHAIN_NODE_BIN` is not set, local runs build the testing-featured
release binary automatically:

```bash
cargo build --locked --release -p logos-blockchain-node --features testing
```

CI expects `target/release/logos-blockchain-node` to already exist, or
`LOGOS_BLOCKCHAIN_NODE_BIN` to point at the prebuilt binary. It does not build
node binaries inside the test process.

## Security-audit sibling production

The security-audit branch can build an audit-capable node binary separately:

    cargo build --release --target-dir target/security-audit -p logos-blockchain-node --features security-audit

For a local test run, set LOGOS_BLOCKCHAIN_NODE_BIN to that binary. A node
configuration can enable sibling production with:

    cryptarchia:
      leader:
        security_audit:
          sibling_blocks_per_leadership: 3

Omitting the setting or using 0 leaves the audit hook disabled. Local
TopologyConfig builders can select a binary per node with
with_node_binary_override(index, path) and set the sibling count per node
with with_sibling_blocks_per_leadership(index, count). A zero count is
omitted from serialized config, so an ordinary node binary can share a
topology with audit-capable nodes.

Compose and Kubernetes can set the sibling count independently per node and
use their existing cluster-wide node-image setting to run the audit image.
Their current runners select one node image for the whole cluster; per-node
production/audit image mixing is available in local deployments through the
binary-path override, but needs runner/chart support for Compose and Kubernetes.

For a winning slot, the producer freezes the parent ledger state, uncle set,
and mempool snapshot while it builds every candidate. It then self-applies and
reconciles the ordinary candidate as usual. Extra siblings are published
through Blend as competing proposals and are not inserted into the producer's
local chain, so they cannot become same-slot parents or change its local tip.

### 1. Run a specific test

_**MacOS or Linux**_

```bash
cargo test --test test_cryptarchia_happy_path  two_nodes_happy -- --no-capture
```

_**Windows (PowerShell)**_

```pwsh
cargo test --test test_cryptarchia_happy_path two_nodes_happy -- --no-capture
```

### 2. Run Tests with Debug Feature Flag

To execute the test suite with the debug feature flag, use the following command:

_**MacOS or Linux**_

```bash
cargo test -p logos-blockchain-tests -F debug disseminate_and_retrieve
```

_**Windows (PowerShell)**_

```pwsh
cargo test -p logos-blockchain-tests -F debug disseminate_and_retrieve
```

`-F debug`: Enables the debug feature flag for the integration tests, allowing for extra debug output or specific
debug-only code paths to be enabled during the tests.
To modify the tracing configuration when using `-F debug` flag go to `tests/src/topology/configs/tracing.rs`. If debug
flag is not used, logs will be written into each nodes temporary directory.

### E2E Artifact Location (nextest / cargo test)

Manual-cluster E2E tests create per-run directories (node configs, state, and default node log files)
under the OS temporary directory by default.

You can override this root directory with:

```text
E2E_TESTS_BASE_DIR_OVERRIDE=/absolute/or/relative/path
```

You can force manual-cluster scenario directories to be kept even when tests pass:

```text
E2E_KEEP_LOGS=true
```

(`true`, `1`, `yes` are accepted.)

For testing-framework tempdir behavior, you can also keep temp run directories with:

```text
TF_KEEP_LOGS=1
```

Optional stable node log output location:

```text
LOGOS_BLOCKCHAIN_LOG_DIR=/path/to/node-log-files
```

## Running Cucumber tests

Local Cucumber tests use the same testing-framework binary provider as the direct e2e tests, so the node binary is built
before local nodes are started. In CI, Cucumber uses the release binary built by the workflow before the Cucumber suite.

Filtering based on tags can be done using the `--tags` option. For example, to run all tests tagged with `@normal_ci`, 
use the following command:
```bash
cargo test --release --features cucumber --test cucumber -- --tags "@normal_ci"
```

Filtering based on test names can be done using the `--name` option. For example, to run a specific test named
"Idle smoke", use the following command:

```bash
cargo test --release --features cucumber --test cucumber -- --name "Idle smoke"
```

For more information on running Cucumber tests, refer to https://github.com/cucumber-rs/cucumber or 
https://cucumber.io/docs.
