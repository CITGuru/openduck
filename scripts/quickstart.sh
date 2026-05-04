#!/usr/bin/env bash
# OpenDuck quickstart — clone-to-first-hybrid-query in one command.
#
# Usage:
#   ./scripts/quickstart.sh [flags]
#
# Flags:
#   --with-extension      Also build the C++ DuckDB extension (slow: 15-25 min cold).
#   --skip-docker         Don't start docker compose; assume Postgres is already
#                         running at $DATABASE_URL (default: postgres://openduck:openduck@localhost:5433/openduck_meta).
#   --keep                Leave the gateway/worker running after the script exits.
#   --no-color            Disable ANSI colour codes.
#   -h, --help            Show this help.
#
# What it does:
#   1. Detects platform (macOS arm64 / Linux x86_64) and verifies prereqs.
#   2. Brings up Postgres + MinIO via docker compose (skip with --skip-docker).
#   3. Runs metadata migrations.
#   4. Builds the Rust workspace (excluding diff-fuse on non-Linux).
#   5. Starts the worker + gateway in the background.
#   6. Smoke-checks the gateway with `openduck status`.
#   7. Runs `cargo run --example hybrid_execution`, which prints an annotated
#      hybrid plan ([LOCAL]/[REMOTE] = the [L]/[R] annotations) and proves an
#      end-to-end LOCAL+REMOTE join returns the expected result.
#
# Idempotent: re-running picks up the existing docker stack, reuses the cargo
# target/, and stops any prior worker/gateway started by this script via
# .openduck-quickstart.pids before relaunching.

set -euo pipefail

# ── repo root ────────────────────────────────────────────────────────────────
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_ROOT"

# ── flags ────────────────────────────────────────────────────────────────────
WITH_EXTENSION=0
SKIP_DOCKER=0
KEEP_RUNNING=0
USE_COLOR=1

while [[ $# -gt 0 ]]; do
  case "$1" in
    --with-extension) WITH_EXTENSION=1 ;;
    --skip-docker)    SKIP_DOCKER=1 ;;
    --keep)           KEEP_RUNNING=1 ;;
    --no-color)       USE_COLOR=0 ;;
    -h|--help)
      sed -n '2,28p' "$0" | sed 's/^# \{0,1\}//'
      exit 0
      ;;
    *)
      echo "unknown flag: $1 (try --help)" >&2
      exit 2
      ;;
  esac
  shift
done

if [[ ! -t 1 ]]; then USE_COLOR=0; fi
if [[ "$USE_COLOR" -eq 1 ]]; then
  C_BOLD=$'\033[1m'; C_DIM=$'\033[2m'
  C_RED=$'\033[31m'; C_GREEN=$'\033[32m'
  C_YELLOW=$'\033[33m'; C_CYAN=$'\033[36m'
  C_RESET=$'\033[0m'
else
  C_BOLD=""; C_DIM=""; C_RED=""; C_GREEN=""; C_YELLOW=""; C_CYAN=""; C_RESET=""
fi

step()  { printf '\n%s==>%s %s%s%s\n' "$C_CYAN" "$C_RESET" "$C_BOLD" "$*" "$C_RESET"; }
info()  { printf '   %s\n' "$*"; }
ok()    { printf '   %s✓%s %s\n' "$C_GREEN" "$C_RESET" "$*"; }
warn()  { printf '   %s!%s %s\n' "$C_YELLOW" "$C_RESET" "$*"; }
fail()  { printf '%s✗%s %s\n' "$C_RED" "$C_RESET" "$*" >&2; }
hint()  { printf '   %shint:%s %s\n' "$C_DIM" "$C_RESET" "$*"; }

die() { fail "$1"; shift || true; for h in "$@"; do hint "$h"; done; exit 1; }

# ── env defaults (mirror examples/dev_stack.sh) ──────────────────────────────
export DATABASE_URL="${DATABASE_URL:-postgres://openduck:openduck@localhost:5433/openduck_meta}"
export OPENDUCK_TOKEN="${OPENDUCK_TOKEN:-quickstart-token}"
export OPENDUCK_WORKER_LISTEN="${OPENDUCK_WORKER_LISTEN:-127.0.0.1:9898}"
export OPENDUCK_WORKER_ADDRS="${OPENDUCK_WORKER_ADDRS:-http://127.0.0.1:9898}"
export OPENDUCK_DB_NAME="${OPENDUCK_DB_NAME:-mydb}"
export OPENDUCK_DATA_DIR="${OPENDUCK_DATA_DIR:-$REPO_ROOT/.openduck-data}"

GATEWAY_LISTEN="${GATEWAY_LISTEN:-0.0.0.0:7878}"
GATEWAY_ENDPOINT="${GATEWAY_ENDPOINT:-http://127.0.0.1:7878}"
PG_HOST="${PG_HOST:-localhost}"
PG_PORT="${PG_PORT:-5433}"
PG_USER="${PG_USER:-openduck}"

PIDFILE="$REPO_ROOT/.openduck-quickstart.pids"
LOG_DIR="$REPO_ROOT/.openduck-quickstart-logs"
mkdir -p "$LOG_DIR" "$OPENDUCK_DATA_DIR"

# ── prior-run cleanup (idempotency) ──────────────────────────────────────────
stop_prior_pids() {
  if [[ -f "$PIDFILE" ]]; then
    while IFS= read -r pid; do
      [[ -z "$pid" ]] && continue
      if kill -0 "$pid" 2>/dev/null; then
        info "stopping prior process $pid"
        kill "$pid" 2>/dev/null || true
      fi
    done < "$PIDFILE"
    rm -f "$PIDFILE"
  fi
}

# ── final cleanup (always-on, unless --keep) ─────────────────────────────────
cleanup() {
  local rc=$?
  if [[ "$KEEP_RUNNING" -eq 1 ]]; then
    if [[ -s "$PIDFILE" ]]; then
      step "Leaving stack running (--keep)"
      info "gateway: $GATEWAY_ENDPOINT  worker: $OPENDUCK_WORKER_LISTEN"
      info "stop later with: kill \$(cat $PIDFILE) && rm $PIDFILE"
      [[ "$SKIP_DOCKER" -eq 0 ]] && \
        info "stop docker with: docker compose -f docker/docker-compose.yml down"
    fi
  else
    if [[ -s "$PIDFILE" ]]; then
      step "Stopping background services"
      stop_prior_pids
      ok "stopped"
    fi
  fi
  exit "$rc"
}
trap cleanup EXIT INT TERM

# ── platform detection ──────────────────────────────────────────────────────
step "Detecting platform"
OS_NAME="$(uname -s)"
OS_ARCH="$(uname -m)"
case "$OS_NAME" in
  Darwin) PLATFORM="macos-$OS_ARCH" ;;
  Linux)  PLATFORM="linux-$OS_ARCH" ;;
  *)      die "unsupported platform: $OS_NAME (supported: Darwin, Linux)" \
              "if you're on Windows, use WSL2 + Linux instructions" ;;
esac
info "platform: $PLATFORM"

# diff-fuse only builds on Linux. Exclude it elsewhere.
WORKSPACE_ARGS=("--workspace")
if [[ "$OS_NAME" != "Linux" ]]; then
  WORKSPACE_ARGS+=("--exclude" "diff-fuse")
fi

# ── prereq checks ───────────────────────────────────────────────────────────
step "Checking prereqs"

require_cmd() {
  local cmd="$1"; shift
  if ! command -v "$cmd" >/dev/null 2>&1; then
    fail "missing required tool: $cmd"
    for h in "$@"; do hint "$h"; done
    exit 1
  fi
}

require_cmd cargo \
  "install Rust via https://rustup.rs (curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh)" \
  "then re-run this script"

require_cmd protoc \
  "macOS: brew install protobuf" \
  "Ubuntu/Debian: sudo apt-get install -y protobuf-compiler" \
  "see https://grpc.io/docs/protoc-installation/"

if [[ "$SKIP_DOCKER" -eq 0 ]]; then
  if ! command -v docker >/dev/null 2>&1; then
    die "docker not found, and --skip-docker was not passed" \
        "install Docker Desktop (macOS) or 'sudo apt-get install -y docker.io' (Linux)" \
        "or pass --skip-docker if you have Postgres already on $PG_HOST:$PG_PORT"
  fi
  if ! docker info >/dev/null 2>&1; then
    die "docker is installed but the daemon isn't reachable" \
        "start Docker Desktop, or 'sudo systemctl start docker' on Linux" \
        "verify with: docker info"
  fi
  if ! docker compose version >/dev/null 2>&1; then
    die "docker compose v2 plugin missing" \
        "install via: docker plugin install docker/compose, or use Docker Desktop"
  fi
fi

require_cmd pg_isready \
  "macOS: brew install libpq && brew link --force libpq" \
  "Ubuntu/Debian: sudo apt-get install -y postgresql-client"

require_cmd psql \
  "macOS: brew install libpq && brew link --force libpq" \
  "Ubuntu/Debian: sudo apt-get install -y postgresql-client"

# Linux-only: FUSE dev headers (needed only if we end up building diff-fuse).
if [[ "$OS_NAME" == "Linux" ]]; then
  if ! pkg-config --exists fuse3 2>/dev/null; then
    warn "libfuse3 dev headers not detected; diff-fuse may fail to build"
    hint "install: sudo apt-get install -y libfuse3-dev pkg-config"
  fi
fi

# Port-collision pre-check (only when we're going to start docker / services).
check_port_free() {
  local port="$1" who="$2"
  if command -v lsof >/dev/null 2>&1 && lsof -nP -iTCP:"$port" -sTCP:LISTEN >/dev/null 2>&1; then
    local owner
    owner=$(lsof -nP -iTCP:"$port" -sTCP:LISTEN -F c 2>/dev/null | awk '/^c/ {sub(/^c/,""); print; exit}')
    die "port $port already in use (needed by $who, current holder: ${owner:-unknown})" \
        "free the port (kill the process, or 'docker compose -f docker/docker-compose.yml down')" \
        "or override with the matching env var (PG_PORT, GATEWAY_LISTEN, OPENDUCK_WORKER_LISTEN)"
  fi
}

GATEWAY_PORT="${GATEWAY_LISTEN##*:}"
WORKER_PORT="${OPENDUCK_WORKER_LISTEN##*:}"

# Note: we deliberately skip a pre-flight port check on $PG_PORT.
# `docker compose up -d` is idempotent — if our compose container already
# holds 5433, that's the desired state, not a collision. If something
# *other* than our compose service holds it, `docker compose up` itself
# will fail with a clear "port is already allocated" error.
#
# We will stop our own previous worker/gateway from PIDFILE before re-checking.
stop_prior_pids
check_port_free "$GATEWAY_PORT" "openduck-gateway"
check_port_free "$WORKER_PORT" "openduck-worker"

ok "prereqs satisfied"

# ── docker stack ────────────────────────────────────────────────────────────
if [[ "$SKIP_DOCKER" -eq 0 ]]; then
  step "Starting Postgres + MinIO (docker compose)"
  if ! docker compose -f docker/docker-compose.yml up -d 2> "$LOG_DIR/docker-up.log"; then
    fail "docker compose up failed"
    sed 's/^/   /' "$LOG_DIR/docker-up.log" >&2
    hint "see $LOG_DIR/docker-up.log for the full error"
    exit 1
  fi
  ok "containers up"
else
  step "Skipping docker (--skip-docker); assuming Postgres at $PG_HOST:$PG_PORT"
fi

step "Waiting for Postgres at $PG_HOST:$PG_PORT"
deadline=$(( $(date +%s) + 60 ))
until pg_isready -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" -q 2>/dev/null; do
  if (( $(date +%s) >= deadline )); then
    die "Postgres did not become ready within 60s" \
        "check 'docker compose -f docker/docker-compose.yml logs postgres'"
  fi
  sleep 1
done
ok "Postgres ready"

# ── migrations ──────────────────────────────────────────────────────────────
step "Running diff-metadata migrations"
shopt -s nullglob
migrations=( crates/diff-metadata/migrations/*.sql )
shopt -u nullglob
if [[ "${#migrations[@]}" -eq 0 ]]; then
  die "no migrations found in crates/diff-metadata/migrations" \
      "did the repo finish cloning? (this directory should not be empty)"
fi
for f in "${migrations[@]}"; do
  # We accept "already exists" because re-runs are expected (idempotency goal).
  if ! out=$(psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f "$f" 2>&1); then
    if echo "$out" | grep -qiE 'already exists|duplicate'; then
      info "skipped (already applied): $(basename "$f")"
    else
      fail "migration failed: $(basename "$f")"
      echo "$out" | sed 's/^/   /' >&2
      exit 1
    fi
  else
    ok "applied: $(basename "$f")"
  fi
done

# ── build workspace ─────────────────────────────────────────────────────────
step "Building Rust workspace (this can take 5-15 min on a cold cache)"
build_log="$LOG_DIR/cargo-build.log"
if ! cargo build "${WORKSPACE_ARGS[@]}" 2>&1 | tee "$build_log"; then
  fail "cargo build failed"
  hint "full log at $build_log"
  exit 1
fi
ok "workspace built"

# Optional: C++ DuckDB extension (slow; off by default).
if [[ "$WITH_EXTENSION" -eq 1 ]]; then
  step "Building C++ DuckDB extension (15-25 min cold)"
  if ! command -v make >/dev/null 2>&1; then
    die "make not found; can't build extension" \
        "macOS: install Xcode Command Line Tools (xcode-select --install)" \
        "Ubuntu/Debian: sudo apt-get install -y build-essential"
  fi
  ext_log="$LOG_DIR/extension-build.log"
  ( cd extensions/openduck && make ) 2>&1 | tee "$ext_log" || {
    fail "extension build failed"
    hint "full log at $ext_log"
    hint "see extensions/openduck/README.md for vcpkg/bison prereqs"
    exit 1
  }
  ok "extension built"
else
  info "skipping C++ extension build (pass --with-extension to enable)"
fi

# ── start worker + gateway ──────────────────────────────────────────────────
step "Starting worker on $OPENDUCK_WORKER_LISTEN"
: > "$PIDFILE"
"$REPO_ROOT/target/debug/openduck-worker" \
  > "$LOG_DIR/worker.log" 2>&1 &
WORKER_PID=$!
echo "$WORKER_PID" >> "$PIDFILE"
info "worker pid=$WORKER_PID  log=$LOG_DIR/worker.log"

# Wait for worker port. Probe via /dev/tcp (bash builtin, works without `nc`).
WORKER_HOST="${OPENDUCK_WORKER_LISTEN%:*}"
wait_port() {
  local host="$1" port="$2" pid="$3" name="$4" log="$5" timeout="${6:-30}"
  local deadline=$(( $(date +%s) + timeout ))
  while ! (exec 3<>"/dev/tcp/$host/$port") 2>/dev/null; do
    exec 3>&- 3<&- || true
    if ! kill -0 "$pid" 2>/dev/null; then
      fail "$name exited before becoming ready"
      sed 's/^/   /' "$log" >&2 || true
      exit 1
    fi
    if (( $(date +%s) >= deadline )); then
      die "$name did not bind $host:$port within ${timeout}s" "see $log"
    fi
    sleep 0.5
  done
  exec 3>&- 3<&- || true
}

wait_port "$WORKER_HOST" "$WORKER_PORT" "$WORKER_PID" "worker" "$LOG_DIR/worker.log" 30
ok "worker accepting connections"

step "Starting gateway on $GATEWAY_LISTEN"
"$REPO_ROOT/target/debug/openduck-gateway" \
  > "$LOG_DIR/gateway.log" 2>&1 &
GATEWAY_PID=$!
echo "$GATEWAY_PID" >> "$PIDFILE"
info "gateway pid=$GATEWAY_PID  log=$LOG_DIR/gateway.log"

wait_port "127.0.0.1" "$GATEWAY_PORT" "$GATEWAY_PID" "gateway" "$LOG_DIR/gateway.log" 30
ok "gateway accepting connections"

# ── smoke check ─────────────────────────────────────────────────────────────
step "Smoke check: openduck status"
if ! "$REPO_ROOT/target/debug/openduck" status --endpoint "$GATEWAY_ENDPOINT" \
     >"$LOG_DIR/status.log" 2>&1; then
  fail "openduck status failed against $GATEWAY_ENDPOINT"
  sed 's/^/   /' "$LOG_DIR/status.log" >&2
  exit 1
fi
sed 's/^/   /' "$LOG_DIR/status.log"
ok "gateway responsive"

# ── hybrid query: prints [LOCAL]/[REMOTE] annotations ───────────────────────
step "Running hybrid query (prints annotated [LOCAL]/[REMOTE] plan)"
hybrid_log="$LOG_DIR/hybrid.log"
if ! cargo run --quiet --example hybrid_execution 2>&1 | tee "$hybrid_log"; then
  fail "hybrid_execution example failed"
  hint "full log at $hybrid_log"
  exit 1
fi

# Verify the annotations actually showed up.
if ! grep -q '\[LOCAL\]' "$hybrid_log" || ! grep -q '\[REMOTE\]' "$hybrid_log"; then
  fail "hybrid example ran but didn't emit [LOCAL]/[REMOTE] annotations"
  hint "see $hybrid_log"
  exit 1
fi
ok "hybrid plan annotations present"

# ── done ────────────────────────────────────────────────────────────────────
step "Quickstart complete"
cat <<EOF
   $C_BOLD${C_GREEN}OpenDuck is running.$C_RESET

   Endpoints:
     gateway   $GATEWAY_ENDPOINT   (token: $OPENDUCK_TOKEN)
     worker    $OPENDUCK_WORKER_LISTEN
     postgres  postgres://$PG_USER:***@$PG_HOST:$PG_PORT/openduck_meta

   Try a query:
     ./target/debug/openduck query --endpoint $GATEWAY_ENDPOINT \\
       "CREATE TABLE users(id INTEGER, name VARCHAR); \\
        INSERT INTO users VALUES (1,'Ada'),(2,'Grace'); \\
        SELECT * FROM users;"

   Logs:           $LOG_DIR/
   Pidfile:        $PIDFILE
EOF
if [[ "$KEEP_RUNNING" -eq 0 ]]; then
  echo "   Stack will shut down on exit (re-run with --keep to leave it up)."
fi
