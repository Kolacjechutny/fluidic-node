# Fluidic — Continuous-Wave State Engine (Research Prototype)

A Rust/Tokio reference implementation of the amended Fluidic architecture:

- **Number Theoretic Transform (NTT)** aggregation for *commutative*,
  state-independent operations: liquidity-pool balance shifts, continuous
  micro-payment streams, and data-throughput routing.
- **Vector-clock DAG ordering** for *state-dependent* operations such as unique
  balance exhaustion, guaranteeing strict causal consistency before any wave
  synthesis occurs.
- **Metabolic decay engine** for continuous time-based value burn.
- **TCP gossip mesh** for local sandboxed oscillator networks.

> **Status:** Permissionless testnet implementation. Anyone can run a `mesh_node`
> from source or a published container image. The core state engine, HTTP/WebSocket
> API, Ed25519-signed stateful shifts, React dApp, and TypeScript SDK are real.

## Run a node in one command

### Docker (recommended)

```bash
docker run -d --name fluidic-node \
  --restart unless-stopped \
  -p 8080:8080 -p 7000:7000 \
  -e OSCILLATOR_ID=12345 \
  -e PEERS="34.56.159.76:7000" \
  -e FLUIDIC_DATA_DIR=/data \
  -v "$HOME/fluidic-data:/data" \
  us-central1-docker.pkg.dev/project-934c3e12-e0e7-4811-810/fluidic/mesh-node:latest
```

> Use a **unique numeric** `OSCILLATOR_ID` (e.g. `12345`, not `node-1`). The identity is
deterministic, so two nodes with the same ID share a keypair and will slash each other.
> Mount `/data` so your snapshot and identity survive container restarts.

Or with Docker Compose:

```bash
wget https://raw.githubusercontent.com/Fluidic-Foundation/Fluidic-FVM/main/docker-compose.yml
docker compose up -d
```

### One-liner installer (source)

```bash
curl -sSL https://raw.githubusercontent.com/Fluidic-Foundation/Fluidic-FVM/main/scripts/run-node.sh | bash
```

The installer tries Docker first; if the image is unavailable it installs Rust,
clones the repo, builds the release binary, and starts the node.

### Manual build

```bash
git clone https://github.com/Fluidic-Foundation/Fluidic-FVM.git
cd fluidic
cargo build --release --bin mesh_node
OSCILLATOR_ID=12345 API_PORT=8080 BIND_ADDR=0.0.0.0:7000 \
  PEERS="34.56.159.76:7000" \
  ./target/release/mesh_node
```

### Environment variables

| Variable | Default | Purpose |
|----------|---------|---------|
| `OSCILLATOR_ID` | `0` | Unique numeric node identity. Use a random number; do not reuse `node-1` across machines |
| `API_PORT` | `8080` | HTTP/WebSocket API port |
| `BIND_ADDR` | `0.0.0.0:7000` | TCP gossip bind address |
| `PEERS` | `''` | Comma-separated list of gossip peers to dial |
| `SYNTHESIS_INTERVAL_MS` | `1000` | How often a synthesis tick runs |

### What happens when it starts

1. A deterministic Ed25519 keypair is derived from `OSCILLATOR_ID`.
2. The node seeds a genesis balance for its own operator account.
3. It locks that balance as stake, so the node is immediately eligible to
   produce BFT synthesis certificates.
4. It opens the API server and joins the gossip mesh via `PEERS`.
5. Every `SYNTHESIS_INTERVAL_MS` it runs a synthesis tick: burns metabolic
   value, finalizes stateful/commutative/EVM shifts, signs a certificate, and
   gossips it to peers.

Your node is online when you see `API server listening on 0.0.0.0:8080`.
Point a browser or the SDK at `http://localhost:8080`.

## Architecture

```
src/
├── crypto/        Ed25519 keypairs, signed phase-shifts
├── field/         State wave-field, account frequency coordinates
├── consensus/     NTT engine, vector-clock DAG, oscillator synthesis, mesh simulation
├── network/       Async in-process gossip, TCP gossip, zero-copy ring buffers
├── value/         Metabolic decay, continuous streams, spectrum band allocation
└── bin/           mesh_node — containerized oscillator node
```

## Build

```bash
cargo build --release
```

## Test

Unit and integration tests:

```bash
cargo test
```

10,000 overlapping transaction benchmark:

```bash
cargo test --test bench -- --nocapture
```

100,000 concurrent NTT stress test:

```bash
cargo test --test ntt_stress -- --nocapture
```

Metabolic decay overhead invariant (< 1%):

```bash
cargo test --test metabolic_overhead -- --nocapture
```

## Benchmark

Metabolic decay Criterion benchmark:

```bash
cargo bench --bench metabolic_bench
```


## Local Sandboxed Mesh

Build and run the mesh node binary:

```bash
cargo run --release --bin mesh_node
```

Or deploy with Docker Compose:

```bash
cd docker
./partition_test.sh
```

The partition test spins up six oscillator containers, disconnects two of them
(~33%) for 15 seconds, then reconnects them and verifies that the surviving
nodes continued to synthesize the wave-field.

> **Note:** Docker must be available and the current user must have permission
> to access the Docker daemon. The partition test was validated syntactically
> but could not be executed in this environment due to daemon permissions.

Run the node with the API server:

```bash
cargo run --release --bin mesh_node -- --api-port 8080
```

API endpoints:

- `GET  /api/state` — live pool reserves, price, throughput, pool account IDs
- `GET  /api/account/:id/balance` — WAVE/USDC balances for a registered account
- `POST /api/account/register` — register an Ed25519 pubkey, returns derived WAVE/USDC accounts and seeds a faucet
- `POST /api/shift/stateful` — submit a signed `StatefulShift` to be synthesized (returns the shift hash)
- `GET  /api/shift/:hash/status` — finality status: `unknown`, `accepted`, `finalized`, or `rejected`
- `GET  /api/ws` — WebSocket feed of pool state updates

A shift becomes **finalized** after surviving `FINALIZATION_DEPTH` synthesis ticks
without a conflicting double-spend being accepted into the DAG.


## Key Design Decisions

1. **NTT-only for commutative ops.** The NTT is used to batch-sum deltas and
   verify that frequency-domain synthesis matches sequential aggregation. It
   does not replace causal ordering.
2. **DAG for stateful ops.** Every stateful phase-shift carries a vector clock
   and predecessor hashes. The oscillator topologically orders the DAG and
   rejects overdrafts, enforcing the conservation law.
3. **No blocks, no mempool.** Shifts are ingested as a continuous stream and
   synthesized in periodic windows. However, causal ordering for stateful
   operations is explicit and deterministic.
4. **Metabolic decay is integer-only.** Burn is computed as
   `rate_per_second * elapsed_ns / 1_000_000_000` using `u128` fixed-point
   arithmetic, so there is no floating-point drift.

## License

MIT OR Apache-2.0
