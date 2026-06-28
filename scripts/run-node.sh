#!/usr/bin/env bash
set -euo pipefail

# One-command Fluidic node runner.
# Usage:
#   curl -sSL https://raw.githubusercontent.com/Fluidic-Foundation/Fluidic-FVM/main/scripts/run-node.sh | bash
#   PEERS="1.2.3.4:7000,5.6.7.8:7000" ./run-node.sh

REPO="https://github.com/Fluidic-Foundation/Fluidic-FVM.git"
FLUIDIC_DIR="${FLUIDIC_DIR:-$HOME/.fluidic}"
SRC_DIR="$FLUIDIC_DIR/src"
DATA_DIR="$FLUIDIC_DIR/data"
IMAGE="us-central1-docker.pkg.dev/project-934c3e12-e0e7-4811-810/fluidic/mesh-node:latest"

API_PORT="${API_PORT:-8080}"
GOSSIP_PORT="${GOSSIP_PORT:-7000}"
# Default to a persisted random numeric ID so repeated runs keep the same
# identity, and different machines never collide on the default.
DEFAULT_ID_FILE="$DATA_DIR/.oscillator-id"
if [ -z "${OSCILLATOR_ID:-}" ]; then
  if [ -f "$DEFAULT_ID_FILE" ]; then
    OSCILLATOR_ID="$(cat "$DEFAULT_ID_FILE")"
  else
    OSCILLATOR_ID="$(shuf -i 1-4294967295 -n 1)"
    mkdir -p "$DATA_DIR"
    echo "$OSCILLATOR_ID" > "$DEFAULT_ID_FILE"
  fi
fi
# Default to the Fluidic testnet gossip seed so a one-liner joins the mesh.
PEERS="${PEERS:-34.56.159.76:7000}"

check_docker() {
  command -v docker >/dev/null 2>&1
}

run_docker() {
  echo "==> Starting Fluidic node from container image..."
  docker run -d \
    --name fluidic-node \
    --restart unless-stopped \
    -p "$API_PORT:8080" \
    -p "$GOSSIP_PORT:7000" \
    -e OSCILLATOR_ID="$OSCILLATOR_ID" \
    -e API_PORT=8080 \
    -e BIND_ADDR="0.0.0.0:7000" \
    -e PEERS="$PEERS" \
    -e SYNTHESIS_INTERVAL_MS="${SYNTHESIS_INTERVAL_MS:-1000}" \
    -e FLUIDIC_DATA_DIR=/data \
    -v "$DATA_DIR:/data" \
    "$IMAGE"
}

ensure_rust() {
  if ! command -v cargo >/dev/null 2>&1; then
    echo "==> Rust not found. Installing rustup..."
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
    # shellcheck source=/dev/null
    source "$HOME/.cargo/env"
  fi
}

run_source() {
  echo "==> Building Fluidic node from source..."
  mkdir -p "$FLUIDIC_DIR" "$DATA_DIR"
  if [ ! -d "$SRC_DIR/.git" ]; then
    git clone --depth 1 "$REPO" "$SRC_DIR"
  else
    git -C "$SRC_DIR" pull --ff-only
  fi
  cd "$SRC_DIR"
  cargo build --release --bin mesh_node
  echo "==> Starting node..."
  exec ./target/release/mesh_node
}

main() {
  echo "Fluidic node launcher"
  echo "  API port:  $API_PORT"
  echo "  Gossip port: $GOSSIP_PORT"
  echo "  Node ID:   $OSCILLATOR_ID"
  [ -n "$PEERS" ] && echo "  Peers:     $PEERS"

  mkdir -p "$DATA_DIR"

  if check_docker; then
    if docker pull "$IMAGE" >/dev/null 2>&1; then
      run_docker
      echo ""
      echo "Node is running in Docker."
      echo "API:    http://localhost:$API_PORT"
      echo "Gossip: localhost:$GOSSIP_PORT"
      echo "Stop:   docker stop fluidic-node"
      exit 0
    else
      echo "==> Could not pull container image (it may be private). Falling back to source build."
    fi
  fi

  ensure_rust
  run_source
}

main "$@"
