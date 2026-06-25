# Build stage
FROM rust:1.92-slim AS builder
WORKDIR /usr/src/fluidic

# Install build dependencies.
RUN apt-get update && apt-get install -y pkg-config libssl-dev && rm -rf /var/lib/apt/lists/*

# Cache dependencies by copying manifests first.
COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY benches ./benches
RUN cargo build --release --bin mesh_node

# Runtime stage
FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y ca-certificates iputils-ping && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=builder /usr/src/fluidic/target/release/mesh_node /app/mesh_node
EXPOSE 7000
ENTRYPOINT ["/app/mesh_node"]
