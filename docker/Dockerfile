# ── OpenDuck multi-stage build ───────────────────────────────────────────────
# Produces three lightweight images:
#   openduck:gateway  — gRPC gateway (auth, routing, plan splitting)
#   openduck:worker   — embedded DuckDB worker (gRPC, Arrow IPC)
#   openduck:fuse     — FUSE mount (differential storage as a file)
#
# Build:
#   docker build --target gateway -t openduck:gateway .
#   docker build --target worker  -t openduck:worker  .
#   docker build --target fuse    -t openduck:fuse    .
#
# Or build all with docker compose (see docker/docker-compose.full.yml)

# ── Builder ─────────────────────────────────────────────────────────────────
FROM rust:1.82-bookworm AS builder

RUN apt-get update && apt-get install -y \
    cmake protobuf-compiler libfuse3-dev pkg-config \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY crates crates
COPY proto proto

# Build all workspace binaries in release mode
RUN cargo build --release \
    --bin openduck \
    --bin openduck-fuse \
    && strip /src/target/release/openduck \
    && strip /src/target/release/openduck-fuse

# ── Gateway ─────────────────────────────────────────────────────────────────
FROM debian:bookworm-slim AS gateway

RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*

COPY --from=builder /src/target/release/openduck /usr/local/bin/openduck

ENV OPENDUCK_TOKEN=""
ENV OPENDUCK_WORKER_ADDRS="http://worker:9898"

EXPOSE 7878

ENTRYPOINT ["openduck", "gateway"]

# ── Worker ──────────────────────────────────────────────────────────────────
FROM debian:bookworm-slim AS worker

RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*

COPY --from=builder /src/target/release/openduck /usr/local/bin/openduck

ENV OPENDUCK_TOKEN=""

EXPOSE 9898

ENTRYPOINT ["openduck", "worker"]

# ── FUSE ────────────────────────────────────────────────────────────────────
FROM debian:bookworm-slim AS fuse

RUN apt-get update && apt-get install -y \
    ca-certificates fuse3 libfuse3-3 \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /src/target/release/openduck-fuse /usr/local/bin/openduck-fuse

ENV DATABASE_URL=""

ENTRYPOINT ["openduck-fuse"]
