#!/usr/bin/env bash
# Validates the current branch name against the Powa code-organization policy
# and rejects any Co-Authored-By: trailers on commits ahead of the base ref.
# See CONTRIBUTING.md for the policy.

set -euo pipefail

BASE_REF="origin/main"
STRIP=0
BRANCH_OVERRIDE=""

usage() {
  cat <<'EOF'
Usage: scripts/check-pr-policy.sh [--strip] [--base <ref>] [--branch <name>]

Validates the current branch name against
  ^(feat|bugfix|docs|tests|work)/[A-Za-z0-9._-]+$
and rejects any commit on the branch (vs --base, default origin/main) whose
message contains a Co-Authored-By: trailer.

  --strip          Rewrite branch history to drop Co-Authored-By: trailers.
  --base <ref>     Compare against this ref (default origin/main; falls back
                   to local main if origin/main is unavailable).
  --branch <name>  Validate this branch name instead of the current one
                   (skips the trailer check).
  -h, --help       Show this help.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --strip) STRIP=1; shift ;;
    --base) BASE_REF="${2:?--base needs a value}"; shift 2 ;;
    --branch) BRANCH_OVERRIDE="${2:?--branch needs a value}"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "unknown arg: $1" >&2; usage >&2; exit 2 ;;
  esac
done

regex='^(feat|bugfix|docs|tests|work)/[A-Za-z0-9._-]+$'

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

scan_violations() {
  # Print one SHA per line for commits whose message has a Co-Authored-By: trailer.
  local sha
  for sha in $(git rev-list --reverse "$merge_base..HEAD"); do
    if git log -1 --format=%B "$sha" | grep -iqE '^Co-Authored-By:'; then
      printf '%s\n' "$sha"
    fi
  done
}

violations_text="$(scan_violations)"
violations_count=0
if [[ -n "$violations_text" ]]; then
  violations_count=$(printf '%s\n' "$violations_text" | wc -l | tr -d ' ')
fi

if [[ "$violations_count" -eq 0 ]]; then
  echo "ok: branch '$branch' is valid; no Co-Authored-By trailers found vs $BASE_REF."
  exit 0
fi

echo "error: $violations_count commit(s) contain Co-Authored-By: trailers:" >&2
while IFS= read -r sha; do
  [[ -z "$sha" ]] && continue
  subject="$(git log -1 --format=%s "$sha")"
  echo "  $sha  $subject" >&2
done <<EOF
$violations_text
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

violations_text="$(scan_violations)"
violations_count=0
if [[ -n "$violations_text" ]]; then
  violations_count=$(printf '%s\n' "$violations_text" | wc -l | tr -d ' ')
fi

if [[ "$violations_count" -ne 0 ]]; then
  echo "error: trailers still present after strip; check filter output." >&2
  exit 1
fi

echo "ok: trailers stripped on '$branch'. If the branch is already pushed,"
echo "    you'll need to force-push: git push --force-with-lease origin $branch"
