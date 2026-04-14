#!/usr/bin/env bash
set -euo pipefail

# Linux-only validation:
# 1) boot Postgres
# 2) run migrations
# 3) mount rw FUSE
# 4) write/checkpoint with DuckDB
# 5) seal + mount snapshot ro
# 6) compare hashes

if [[ "$(uname -s)" != "Linux" ]]; then
  echo "This script requires Linux (FUSE3)." >&2
  exit 2
fi

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

DB_NAME="${DB_NAME:-e2e_db}"
DATA_DIR="${DATA_DIR:-$ROOT_DIR/.e2e-data}"
RW_MOUNT="${RW_MOUNT:-$ROOT_DIR/.e2e-mnt-rw}"
RO_MOUNT="${RO_MOUNT:-$ROOT_DIR/.e2e-mnt-ro}"
DATABASE_URL="${DATABASE_URL:-postgres://openduck:openduck@localhost:5433/openduck_meta}"
LEASE_HOLDER="${LEASE_HOLDER:-e2e-script}"

mkdir -p "$DATA_DIR" "$RW_MOUNT" "$RO_MOUNT"

docker compose -f docker/docker-compose.yml up -d

for f in $(ls crates/diff-metadata/migrations/*.sql | sort); do
  psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f "$f"
done

cargo build -p diff-fuse -p diff-metadata >/dev/null

target/debug/openduck-fuse \
  --db "$DB_NAME" \
  --postgres "$DATABASE_URL" \
  --data-dir "$DATA_DIR" \
  --mountpoint "$RW_MOUNT" \
  --lease-holder "$LEASE_HOLDER" &
FUSE_PID=$!
sleep 2

duckdb "$RW_MOUNT/database.duckdb" <<'SQL'
CREATE TABLE IF NOT EXISTS t AS SELECT * FROM range(1000);
INSERT INTO t SELECT * FROM range(1000, 2000);
CHECKPOINT;
SQL

SNAP_ID="$(psql "$DATABASE_URL" -tA -c "SELECT current_tip_snapshot_id FROM openduck_db WHERE name = '$DB_NAME'")"
if [[ -z "$SNAP_ID" ]]; then
  echo "No snapshot id found for db $DB_NAME" >&2
  kill "$FUSE_PID" || true
  exit 1
fi

target/debug/openduck-fuse \
  --db "$DB_NAME" \
  --postgres "$DATABASE_URL" \
  --data-dir "$DATA_DIR" \
  --mountpoint "$RO_MOUNT" \
  --snapshot "$SNAP_ID" &
RO_FUSE_PID=$!
sleep 2

RW_HASH="$(sha256sum "$RW_MOUNT/database.duckdb" | awk '{print $1}')"
RO_HASH="$(sha256sum "$RO_MOUNT/database.duckdb" | awk '{print $1}')"
echo "rw hash: $RW_HASH"
echo "ro hash: $RO_HASH"

kill "$RO_FUSE_PID" || true
kill "$FUSE_PID" || true

if [[ "$RW_HASH" != "$RO_HASH" ]]; then
  echo "Hash mismatch between rw and snapshot ro mounts" >&2
  exit 1
fi

echo "E2E success: snapshot hash matched"
