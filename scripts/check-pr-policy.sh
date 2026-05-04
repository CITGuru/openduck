#!/usr/bin/env bash
# Validates the current branch against the Powa code-organization policy:
#   1. Branch name matches (feat|bugfix|docs|tests|work)/<descriptor>.
#   2. No commit on the branch carries a Co-Authored-By: trailer.
#   3. Every commit author email matches the configured global git user.email
#      (no per-repo identity overrides; commits must be under the global identity).
# See CONTRIBUTING.md for the policy.

set -euo pipefail

BASE_REF="origin/main"
STRIP=0
BRANCH_OVERRIDE=""
SELF_TEST=0

usage() {
  cat <<'EOF'
Usage: scripts/check-pr-policy.sh [--strip] [--base <ref>] [--branch <name>] [--self-test]

Validates the current branch name against
  ^(feat|bugfix|docs|tests|work)/[A-Za-z0-9._-]+$
and rejects:
  - any commit (vs --base, default origin/main) whose message contains a
    Co-Authored-By: trailer.
  - any commit whose author email differs from `git config --global user.email`.

  --strip          Rewrite branch history to drop Co-Authored-By: trailers.
  --base <ref>     Compare against this ref (default origin/main; falls back
                   to local main if origin/main is unavailable).
  --branch <name>  Validate this branch name instead of the current one
                   (skips trailer + author checks).
  --self-test      Run the script's built-in self-test (exits 0 on pass).
  -h, --help       Show this help.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --strip) STRIP=1; shift ;;
    --base) BASE_REF="${2:?--base needs a value}"; shift 2 ;;
    --branch) BRANCH_OVERRIDE="${2:?--branch needs a value}"; shift 2 ;;
    --self-test) SELF_TEST=1; shift ;;
    -h|--help) usage; exit 0 ;;
    *) echo "unknown arg: $1" >&2; usage >&2; exit 2 ;;
  esac
done

regex='^(feat|bugfix|docs|tests|work)/[A-Za-z0-9._-]+$'

run_self_test() {
  # Plant a throwaway repo with a fake-author commit and verify this script
  # rejects it on the author-identity check.
  local script_path
  script_path="$(cd "$(dirname "$0")" && pwd)/$(basename "$0")"

  local tmp
  tmp="$(mktemp -d -t powa-pr-policy-selftest.XXXXXX)"
  # Expand $tmp NOW so cleanup still works after the local var goes out of scope.
  trap "rm -rf '$tmp'" EXIT

  local fake_home="$tmp/home"
  mkdir -p "$fake_home"
  cat > "$fake_home/.gitconfig" <<'EOF'
[user]
email = real@example.com
name = Real User
[init]
defaultBranch = main
EOF

  local repo="$tmp/repo"
  mkdir -p "$repo"
  (
    cd "$repo"
    export HOME="$fake_home"
    export GIT_CONFIG_GLOBAL="$fake_home/.gitconfig"
    git init -q -b main
    git commit --allow-empty -q -m "init"
    git checkout -q -b feat/self_test
    GIT_AUTHOR_NAME="CTO Test" GIT_AUTHOR_EMAIL="cto-test@local" \
    GIT_COMMITTER_NAME="CTO Test" GIT_COMMITTER_EMAIL="cto-test@local" \
      git commit --allow-empty -q -m "feat: bad author commit"
  )

  echo "self-test: expecting failure on fake-author commit (cto-test@local)..."
  local rc=0
  (cd "$repo" && HOME="$fake_home" GIT_CONFIG_GLOBAL="$fake_home/.gitconfig" \
    bash "$script_path" --base main >"$tmp/out" 2>"$tmp/err" </dev/null) \
    || rc=$?

  if [[ $rc -eq 0 ]]; then
    echo "self-test FAILED: script exited 0 but should have rejected fake-author commit." >&2
    cat "$tmp/out" >&2
    cat "$tmp/err" >&2
    return 1
  fi
  if ! grep -q "cto-test@local" "$tmp/err"; then
    echo "self-test FAILED: error output did not mention cto-test@local." >&2
    cat "$tmp/err" >&2
    return 1
  fi
  echo "self-test: ok (script rejected fake-author commit; rc=$rc)."
  return 0
}

if [[ $SELF_TEST -eq 1 ]]; then
  run_self_test
  exit $?
fi

if [[ -n "$BRANCH_OVERRIDE" ]]; then
  branch="$BRANCH_OVERRIDE"
else
  branch="$(git symbolic-ref --quiet --short HEAD || true)"
  if [[ -z "$branch" ]]; then
    echo "error: not on a branch (detached HEAD?)" >&2
    exit 1
  fi
fi

if ! [[ "$branch" =~ $regex ]]; then
  cat >&2 <<EOF
error: branch name "$branch" does not match required format.
Required: (feat|bugfix|docs|tests|work)/<descriptor>
Allowed descriptor chars: letters, digits, '.', '_', '-'.
Examples: feat/hybrid_plan_split, bugfix/invariant_number, docs/quickstart_links.
EOF
  exit 1
fi

# If only validating an external branch name, stop here.
if [[ -n "$BRANCH_OVERRIDE" ]]; then
  echo "ok: branch name '$branch' matches policy."
  exit 0
fi

if ! git rev-parse --verify --quiet "$BASE_REF" >/dev/null; then
  if git rev-parse --verify --quiet "main" >/dev/null; then
    BASE_REF="main"
  else
    echo "error: base ref '$BASE_REF' not found and no local 'main'." >&2
    exit 1
  fi
fi

merge_base="$(git merge-base HEAD "$BASE_REF")"

scan_trailer_violations() {
  # Print one SHA per line for commits whose message has a Co-Authored-By: trailer.
  local sha
  for sha in $(git rev-list --reverse "$merge_base..HEAD"); do
    if git log -1 --format=%B "$sha" | grep -iqE '^Co-Authored-By:'; then
      printf '%s\n' "$sha"
    fi
  done
}

scan_author_violations() {
  # Print "<sha>\t<author-email>" for commits whose author email != $1 (expected email).
  local expected="$1"
  local sha author_email
  for sha in $(git rev-list --reverse "$merge_base..HEAD"); do
    author_email="$(git log -1 --format='%ae' "$sha")"
    if [[ "$author_email" != "$expected" ]]; then
      printf '%s\t%s\n' "$sha" "$author_email"
    fi
  done
}

trailer_violations="$(scan_trailer_violations)"
trailer_count=0
if [[ -n "$trailer_violations" ]]; then
  trailer_count=$(printf '%s\n' "$trailer_violations" | wc -l | tr -d ' ')
fi

if [[ "$trailer_count" -ne 0 ]]; then
  echo "error: $trailer_count commit(s) contain Co-Authored-By: trailers:" >&2
  while IFS= read -r sha; do
    [[ -z "$sha" ]] && continue
    subject="$(git log -1 --format=%s "$sha")"
    echo "  $sha  $subject" >&2
  done <<EOF
$trailer_violations
EOF

  if [[ $STRIP -eq 0 ]]; then
    cat >&2 <<EOF

To fix:
  - Re-run with --strip to rewrite branch history automatically, or
  - 'git rebase -i $merge_base' and amend each commit to remove the trailer.
EOF
    exit 1
  fi

  echo "stripping Co-Authored-By: trailers from $merge_base..HEAD..."
  export FILTER_BRANCH_SQUELCH_WARNING=1
  git filter-branch -f \
    --msg-filter "sed '/^[Cc]o-[Aa]uthored-[Bb]y:/d'" \
    -- "$merge_base..HEAD" >/dev/null

  trailer_violations="$(scan_trailer_violations)"
  trailer_count=0
  if [[ -n "$trailer_violations" ]]; then
    trailer_count=$(printf '%s\n' "$trailer_violations" | wc -l | tr -d ' ')
  fi
  if [[ "$trailer_count" -ne 0 ]]; then
    echo "error: trailers still present after strip; check filter output." >&2
    exit 1
  fi
  echo "ok: trailers stripped on '$branch'. If the branch is already pushed,"
  echo "    you'll need to force-push: git push --force-with-lease origin $branch"
fi

# --- Author identity check ------------------------------------------------
expected_email="$(git config --global user.email || true)"
if [[ -z "$expected_email" ]]; then
  echo "error: no global git user.email configured. Set one with:" >&2
  echo "       git config --global user.email <you@example.com>" >&2
  exit 1
fi

# Per-repo overrides mask the global identity. Reject them outright.
local_user_email="$(git config --local --get user.email || true)"
local_user_name="$(git config --local --get user.name || true)"
if [[ -n "$local_user_email" || -n "$local_user_name" ]]; then
  cat >&2 <<EOF
error: per-repo git identity override detected in $(git rev-parse --git-dir)/config:
       user.email=$local_user_email
       user.name=$local_user_name
Per-repo identity overrides are forbidden. Remove the [user] block:
       git config --local --unset user.email
       git config --local --unset user.name
       git config --local --remove-section user || true
EOF
  exit 1
fi

author_violations="$(scan_author_violations "$expected_email")"
author_count=0
if [[ -n "$author_violations" ]]; then
  author_count=$(printf '%s\n' "$author_violations" | wc -l | tr -d ' ')
fi

if [[ "$author_count" -ne 0 ]]; then
  echo "error: $author_count commit(s) authored under an email != global user.email ($expected_email):" >&2
  while IFS=$'\t' read -r sha email; do
    [[ -z "$sha" ]] && continue
    subject="$(git log -1 --format=%s "$sha")"
    echo "  $sha  <$email>  $subject" >&2
  done <<EOF
$author_violations
EOF
  cat >&2 <<EOF

To fix, on this branch:
  git rebase $merge_base --exec 'git commit --amend --no-edit --reset-author'
  git push --force-with-lease origin $branch
EOF
  exit 1
fi

echo "ok: branch '$branch' passes pre-PR checks (name, no Co-Authored-By trailers, author == $expected_email)."
