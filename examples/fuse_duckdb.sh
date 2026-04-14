#!/usr/bin/env bash
# ── FUSE Mount + DuckDB End-to-End ──────────────────────────────────────────
#
# Mount an OpenDuck differential-storage-backed file via FUSE,
# then use DuckDB against it like a normal database file.
#
# Requires:
#   - Linux with FUSE3 (sudo apt install libfuse3-dev)
#   - Postgres running (docker compose -f docker/docker-compose.yml up -d)
#   - Migrations applied (see below)
#   - cargo build --workspace
#
# Usage:
#   bash examples/fuse_duckdb.sh

set -euo pipefail

DB_NAME="fuse_demo_$$"
DATA_DIR="/tmp/openduck-fuse-data-$$"
MOUNTPOINT="/tmp/openduck-fuse-mount-$$"
DATABASE_URL="${DATABASE_URL:-postgres://openduck:openduck@localhost:5433/openduck_meta}"

echo "=== OpenDuck FUSE + DuckDB Example ==="
echo ""
echo "Database:   $DB_NAME"
echo "Data dir:   $DATA_DIR"
echo "Mountpoint: $MOUNTPOINT"
echo "Postgres:   $DATABASE_URL"
echo ""

# ── 1. Ensure Postgres migrations are applied ───────────────────────────────
echo "── 1. Applying migrations ──"
for f in $(ls crates/diff-metadata/migrations/*.sql 2>/dev/null | sort); do
    psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f "$f" -q 2>/dev/null || true
done
echo "   Done."
echo ""

# ── 2. Create data directory ────────────────────────────────────────────────
echo "── 2. Creating directories ──"
mkdir -p "$DATA_DIR" "$MOUNTPOINT"
echo "   $DATA_DIR"
echo "   $MOUNTPOINT"
echo ""

# ── 3. Mount via FUSE ───────────────────────────────────────────────────────
echo "── 3. Mounting FUSE filesystem ──"
echo "   openduck-fuse --db $DB_NAME --postgres \$DATABASE_URL --data-dir $DATA_DIR --mountpoint $MOUNTPOINT"
cargo run -p diff-fuse --bin openduck-fuse -- \
    --db "$DB_NAME" \
    --postgres "$DATABASE_URL" \
    --data-dir "$DATA_DIR" \
    --mountpoint "$MOUNTPOINT" \
    --lease-holder "example-$$" &
FUSE_PID=$!

# Wait for mount to be ready
sleep 2
echo "   Mounted. PID=$FUSE_PID"
echo "   ls $MOUNTPOINT:"
ls -la "$MOUNTPOINT/"
echo ""

# ── 4. Use DuckDB against the mounted file ──────────────────────────────────
echo "── 4. DuckDB on FUSE-mounted file ──"
DB_FILE="$MOUNTPOINT/database.duckdb"
echo "   Opening: $DB_FILE"
echo ""

# Create tables and insert data
echo "   Creating tables and inserting data..."
duckdb "$DB_FILE" <<'SQL'
CREATE TABLE users (id INTEGER, name VARCHAR, email VARCHAR);
INSERT INTO users VALUES
    (1, 'Alice', 'alice@example.com'),
    (2, 'Bob', 'bob@example.com'),
    (3, 'Carol', 'carol@example.com');

CREATE TABLE events (id INTEGER, user_id INTEGER, action VARCHAR, ts TIMESTAMP);
INSERT INTO events VALUES
    (1, 1, 'login', '2025-01-15 10:00:00'),
    (2, 1, 'purchase', '2025-01-15 10:05:00'),
    (3, 2, 'login', '2025-01-15 11:00:00'),
    (4, 3, 'signup', '2025-01-15 12:00:00');

CHECKPOINT;
SQL

echo "   Done. Data written through FUSE to differential storage."
echo ""

# Query it back
echo "   Querying users:"
duckdb "$DB_FILE" -c "SELECT * FROM users;"

echo ""
echo "   Join query:"
duckdb "$DB_FILE" -c "
    SELECT u.name, COUNT(*) AS actions, MAX(e.ts) AS last_seen
    FROM users u
    JOIN events e ON u.id = e.user_id
    GROUP BY u.name
    ORDER BY actions DESC;
"

# ── 5. What's happening under the hood ──────────────────────────────────────
echo ""
echo "── 5. What happened under the hood ──"
echo ""
echo "   DuckDB sees $DB_FILE as a normal file."
echo "   But every read/write goes through FUSE → OpenDuck's StorageBackend:"
echo ""
echo "   DuckDB write(offset, data)"
echo "     → FUSE write() callback"
echo "       → StorageBackend.write(logical_offset, data)"
echo "         → appends to active layer"
echo "         → records extent in Postgres"
echo ""
echo "   DuckDB read(offset, len)"
echo "     → FUSE read() callback"
echo "       → StorageBackend.read(LogicalRange, ReadContext)"
echo "         → resolves extents newest-first across layers"
echo "         → returns bytes (zeros for unwritten holes)"
echo ""
echo "   CHECKPOINT → flush/fsync → backend.seal()"
echo "     → active layer sealed (immutable)"
echo "     → new snapshot row in Postgres"
echo "     → fresh active layer opened"

# ── 6. Cleanup ──────────────────────────────────────────────────────────────
echo ""
echo "── 6. Cleanup ──"
fusermount3 -u "$MOUNTPOINT" 2>/dev/null || fusermount -u "$MOUNTPOINT" 2>/dev/null || true
wait "$FUSE_PID" 2>/dev/null || true
rm -rf "$DATA_DIR" "$MOUNTPOINT"
echo "   Unmounted and cleaned up."
echo ""
echo "=== Done ==="
