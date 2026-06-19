# Fluidic Node

Minimal source for running a Fluidic oscillator node.

A node joins the gossip mesh, ingests signed stateful/commutative/EVM shifts,
and runs periodic synthesis ticks to finalize state and sign BFT certificates.

## One command to run a node

### Docker

```bash
docker run -d --name fluidic-node \
  -p 8080:8080 -p 7000:7000 \
  -e OSCILLATOR_ID=my-node \
  -e PEERS="testnet-seed.fluidic.foundation:7000" \
  ghcr.io/kolacjechutny/fluidic-node:latest
```

Or with Docker Compose:

```bash
wget https://raw.githubusercontent.com/Kolacjechutny/fluidic-node/main/docker-compose.yml
docker compose up -d
```

### Source installer

```bash
curl -sSL https://raw.githubusercontent.com/Kolacjechutny/fluidic-node/main/scripts/run-node.sh | bash
```

The installer tries Docker first; if that fails it installs Rust, clones this
repo, builds the release binary, and starts the node.

### Manual build

```bash
git clone https://github.com/Kolacjechutny/fluidic-node.git
cd fluidic-node
cargo build --release --bin mesh_node
OSCILLATOR_ID=my-node PEERS="testnet-seed.fluidic.foundation:7000" \
  ./target/release/mesh_node
```

## Configuration

| Variable | Default | Purpose |
|----------|---------|---------|
| `OSCILLATOR_ID` | `0` | Node identity. Same ID always yields the same operator account. |
| `API_PORT` | `8080` | HTTP/WebSocket API port |
| `BIND_ADDR` | `0.0.0.0:7000` | TCP gossip bind address |
| `PEERS` | `''` | Comma-separated gossip seed peers |
| `SYNTHESIS_INTERVAL_MS` | `1000` | Synthesis tick interval |

## How it works

1. **Identity** — Derives a deterministic Ed25519 keypair from `OSCILLATOR_ID`.
2. **Genesis stake** — Seeds a genesis balance for its own operator account and
   stakes it, so the node can produce synthesis certificates immediately.
3. **Joins the mesh** — Opens the API and dials the peers in `PEERS`.
4. **Synthesizes** — Every tick: burns metabolic value, finalizes shifts,
   computes Merkle roots, signs a certificate, and gossips it.

The node is online when the log shows the API listening on `0.0.0.0:8080`.
Use the API at `http://localhost:8080`.

## Testnet

To join the public testnet, set `PEERS` to a live seed address. The testnet
exposes its API through `https://api.testnet.fluidic.foundation`, but joining
as a peer requires a reachable gossip seed on port `7000`.
