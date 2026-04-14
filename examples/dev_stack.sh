#!/usr/bin/env bash
# OpenDuck development stack — starts all services locally.
#
# Usage:
#   bash examples/dev_stack.sh
#
# Prerequisites:
#   - Docker (for Postgres + MinIO)
#   - Rust toolchain (cargo)
#
# Ctrl-C stops everything.

set -euo pipefail
cd "$(dirname "$0")/.."

export DATABASE_URL=postgres://openduck:openduck@localhost:5433/openduck_meta
export OPENDUCK_TOKEN=dev-token
export OPENDUCK_WORKER_LISTEN=127.0.0.1:9898
export OPENDUCK_WORKER_ADDRS=http://127.0.0.1:9898

echo "==> Starting Docker services (Postgres + MinIO)..."
docker compose -f docker/docker-compose.yml up -d

echo "==> Waiting for Postgres to be ready..."
until pg_isready -h localhost -p 5433 -U openduck -q 2>/dev/null; do
  sleep 1
done

echo "==> Running migrations..."
for f in crates/diff-metadata/migrations/*.sql; do
  psql "$DATABASE_URL" -f "$f" 2>&1 | grep -v "already exists" || true
done

echo "==> Building workspace..."
cargo build --workspace

echo ""
echo "==> Starting worker (PID will be printed)..."
cargo run --bin openduck-worker &
WORKER_PID=$!
sleep 2

echo "==> Starting gateway (PID will be printed)..."
cargo run --bin openduck-gateway &
GATEWAY_PID=$!
sleep 1

echo ""
echo "========================================"
echo "  OpenDuck dev stack is running!"
echo ""
echo "  Worker:   $OPENDUCK_WORKER_LISTEN"
echo "  Gateway:  0.0.0.0:7878"
echo "  Postgres: localhost:5433"
echo "  MinIO:    localhost:9000 (console: 9001)"
echo "  Token:    $OPENDUCK_TOKEN"
echo ""
echo "  Try:"
echo "    cargo run --example grpc_roundtrip"
echo "    cargo run --example hybrid_plan"
echo ""
echo "  Press Ctrl-C to stop all services."
echo "========================================"

cleanup() {
  echo ""
  echo "==> Stopping services..."
  kill "$WORKER_PID" "$GATEWAY_PID" 2>/dev/null || true
  wait "$WORKER_PID" "$GATEWAY_PID" 2>/dev/null || true
  echo "==> Services stopped. Docker containers are still running."
  echo "    Run 'docker compose -f docker/docker-compose.yml down' to stop them."
}
trap cleanup EXIT

wait
