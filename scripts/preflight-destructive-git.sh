#!/usr/bin/env bash
# Mandatory pre-flight check for any destructive git operation.
#
# REQUIRED before any of:
#   git checkout <branch>     git switch <branch>      git reset --hard
#   git rebase                git clean                git pull --rebase
#   git worktree remove
#
# These ops will silently delete UNTRACKED files at top level. `git stash`
# without `--include-untracked` does NOT save them. This script either
# refuses (clean check) or stashes with --include-untracked.
#
# History: see POWA-224 / POWA-227. apps/landing/ was eliminated by a
# rebase + checkout taken from a working tree containing an untracked
# top-level directory.
#
# Usage:
#   scripts/preflight-destructive-git.sh check
#       Exit 0 if `git status --porcelain` is empty. Exit 1 (and print the
#       dirty entries) otherwise. Use this when you intend to refuse to
#       proceed unless the tree is already clean.
#
#   scripts/preflight-destructive-git.sh stash <op-label>
#       If the working tree is clean, exits 0 with status "clean".
#       Otherwise runs `git stash push --include-untracked -m
#       "<run-id>: pre-<op-label>"`, verifies the stash entry was created,
#       prints the stash ref + list, and exits 0.
#       Use this when you want to proceed but preserve current state.
#
#   scripts/preflight-destructive-git.sh self-test
#       Runs the built-in self-test (creates a sandbox repo with an
#       untracked top-level dir, exercises `check` and `stash`, and
#       confirms behaviour).
#
# Run id: read from PAPERCLIP_RUN_ID env var, falls back to a timestamp.

set -euo pipefail

run_id="${PAPERCLIP_RUN_ID:-local-$(date -u +%Y%m%dT%H%M%SZ)}"

usage() {
  sed -n '2,32p' "$0"
}

require_in_repo() {
  if ! git rev-parse --git-dir >/dev/null 2>&1; then
    echo "preflight-destructive-git: not inside a git repository" >&2
    exit 2
  fi
}

cmd_check() {
  require_in_repo
  local porcelain
  porcelain="$(git status --porcelain)"
  if [[ -z "$porcelain" ]]; then
    echo "preflight-destructive-git: clean (no tracked changes, no untracked files)."
    return 0
  fi
  cat >&2 <<EOF
preflight-destructive-git: REFUSE — working tree is not clean.
The pending destructive operation would silently destroy these entries
(untracked entries marked '??' are most at risk):

$porcelain

Action: either commit/discard the changes deliberately, or run
        scripts/preflight-destructive-git.sh stash <op-label>
        to stash with --include-untracked before proceeding.
EOF
  return 1
}

cmd_stash() {
  require_in_repo
  local op_label="${1:-unspecified}"
  local porcelain
  porcelain="$(git status --porcelain)"
  if [[ -z "$porcelain" ]]; then
    echo "preflight-destructive-git: clean (no stash needed)."
    return 0
  fi

  local message="${run_id}: pre-${op_label}"
  echo "preflight-destructive-git: stashing with --include-untracked..."
  echo "  message: ${message}"

  # Capture stash list size before/after to confirm the stash was created.
  local before_count after_count
  before_count="$(git stash list | wc -l | tr -d ' ')"
  git stash push --include-untracked -m "$message" >/dev/null
  after_count="$(git stash list | wc -l | tr -d ' ')"

  if [[ "$after_count" -le "$before_count" ]]; then
    echo "preflight-destructive-git: stash failed — stash list did not grow." >&2
    echo "Refusing to proceed; restore the working tree manually." >&2
    return 1
  fi

  # Re-confirm working tree is now clean. If anything still shows up, the
  # caller MUST stop — otherwise the destructive op will eat it.
  local residual
  residual="$(git status --porcelain)"
  if [[ -n "$residual" ]]; then
    cat >&2 <<EOF
preflight-destructive-git: stash created but working tree is STILL dirty:

$residual

Refusing to proceed. Investigate and re-run.
EOF
    return 1
  fi

  echo "preflight-destructive-git: stash ok. Top of stack:"
  git stash list | head -1
  echo "preflight-destructive-git: recover later with: git stash pop"
  return 0
}

cmd_self_test() {
  local script_path
  script_path="$(cd "$(dirname "$0")" && pwd)/$(basename "$0")"

  local tmp
  tmp="$(mktemp -d -t preflight-selftest.XXXXXX)"
  trap "rm -rf '$tmp'" EXIT

  local home="$tmp/home"
  mkdir -p "$home"
  cat > "$home/.gitconfig" <<'EOF'
[user]
email = preflight-selftest@example.com
name = Preflight Self Test
[init]
defaultBranch = main
EOF

  local repo="$tmp/repo"
  mkdir -p "$repo"
  (
    cd "$repo"
    export HOME="$home"
    export GIT_CONFIG_GLOBAL="$home/.gitconfig"
    git init -q -b main
    git commit --allow-empty -q -m "init"

    # Plant an untracked top-level dir, like apps/landing/.
    mkdir -p apps/landing
    echo "<html>do not delete</html>" > apps/landing/index.html

    echo "self-test: 'check' on dirty tree must REFUSE..."
    local rc=0
    bash "$script_path" check >"$tmp/check.out" 2>"$tmp/check.err" </dev/null \
      || rc=$?
    if [[ $rc -eq 0 ]]; then
      echo "self-test FAILED: 'check' returned 0 on dirty tree." >&2
      cat "$tmp/check.err" >&2
      return 1
    fi
    # `git status --porcelain` collapses untracked dirs to the top-level
    # entry by default — that is exactly the silent-deletion blast radius
    # POWA-227 protects against, so matching '?? apps/' is the right check.
    if ! grep -qE '^\?\? apps/' "$tmp/check.err"; then
      echo "self-test FAILED: 'check' error did not flag untracked apps/ dir." >&2
      cat "$tmp/check.err" >&2
      return 1
    fi
    echo "self-test: 'check' refused as expected (rc=$rc)."

    echo "self-test: 'stash switch_main' on dirty tree must stash with --include-untracked..."
    bash "$script_path" stash switch_main >"$tmp/stash.out" 2>"$tmp/stash.err"
    if ! git stash list | grep -q "pre-switch_main"; then
      echo "self-test FAILED: stash entry not found." >&2
      cat "$tmp/stash.err" >&2
      return 1
    fi
    if [[ -e apps/landing/index.html ]]; then
      echo "self-test FAILED: untracked file still present after stash." >&2
      return 1
    fi
    echo "self-test: stash captured untracked apps/landing (rc=$?)."

    echo "self-test: 'check' on now-clean tree must pass..."
    bash "$script_path" check >"$tmp/check2.out" 2>&1
    echo "self-test: clean check ok."

    echo "self-test: stash pop must restore apps/landing..."
    git stash pop --quiet
    if [[ ! -e apps/landing/index.html ]]; then
      echo "self-test FAILED: apps/landing not restored after stash pop." >&2
      return 1
    fi
    echo "self-test: apps/landing recovered after pop."
  )

  echo "self-test: ok."
  return 0
}

main() {
  if [[ $# -eq 0 ]]; then
    usage >&2
    exit 2
  fi
  local sub="$1"
  shift || true
  case "$sub" in
    check)      cmd_check "$@" ;;
    stash)      cmd_stash "$@" ;;
    self-test)  cmd_self_test "$@" ;;
    -h|--help)  usage; exit 0 ;;
    *)
      echo "preflight-destructive-git: unknown subcommand '$sub'" >&2
      usage >&2
      exit 2
      ;;
  esac
}

main "$@"
