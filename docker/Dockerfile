# Build stage
FROM rust:1.92-slim AS builder
WORKDIR /usr/src/fluidic

# Install build dependencies.
RUN apt-get update && apt-get install -y pkg-config libssl-dev && rm -rf /var/lib/apt/lists/*

# Copy source and build the node.
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --release --bin mesh_node

# Runtime stage
FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y ca-certificates iputils-ping && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=builder /usr/src/fluidic/target/release/mesh_node /app/mesh_node

ENV API_PORT=8080
ENV BIND_ADDR=0.0.0.0:7000
ENV OSCILLATOR_ID=0
ENV SYNTHESIS_INTERVAL_MS=1000
ENV PEERS=""

EXPOSE 8080 7000
ENTRYPOINT ["/app/mesh_node"]
