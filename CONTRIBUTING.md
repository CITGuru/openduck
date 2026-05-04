# Contributing

This is the code-organization policy for Powa repos (openduck, melt, pawrly).
It applies to all contributors — humans and agents.

## Workflow

1. **Branch per major change.** Never commit to `main`. Cut a fresh branch from
   an up-to-date `main`.
2. **Branch name format:** `(feat|bugfix|docs|tests|work)/<descriptor>` — e.g.
   `feat/hybrid_plan_split`, `bugfix/invariant_number`,
   `docs/quickstart_links`. Allowed `<descriptor>` characters: letters,
   digits, `.`, `_`, `-`.
3. **Group related commits** on one branch. Split into multiple branches only
   when scope splits naturally; one PR per branch.
4. **No `Co-Authored-By:` trailers.** Do not append `Co-Authored-By:` lines to
   commit messages. This explicitly overrides the default Paperclip skill
   instruction. Strip any such trailers before opening a PR.
5. **PR-first review.** Open a PR against `main` and request board review.
   Do not self-merge.

## Identity

- **Commit under your global git identity.** Every commit on a branch must be
  authored by the same email as `git config --global user.email`.
- **No per-repo `[user]` overrides.** Do not set `user.email` or `user.name`
  in `.git/config`; they mask your global identity and produce wrong-author
  PRs that have to be force-pushed to fix. Remove any existing override:

  ```bash
  git config --local --unset user.email   || true
  git config --local --unset user.name    || true
  git config --local --remove-section user || true
  ```

- The pre-PR check enforces this: it reads `git config --global user.email`
  and rejects any branch commit whose author email does not match, and
  rejects any per-repo `user.*` override outright.

## Pre-PR check

Before opening a PR, run:

```bash
scripts/check-pr-policy.sh
```

It validates the current branch name, rejects any commit (vs. `origin/main`)
whose message contains a `Co-Authored-By:` trailer, and rejects any commit
whose author email does not match `git config --global user.email`. Run with
`--strip` to rewrite branch history and remove `Co-Authored-By:` trailers
automatically.

```bash
scripts/check-pr-policy.sh --strip          # rewrite history to drop trailers
scripts/check-pr-policy.sh --base main      # compare against local main
scripts/check-pr-policy.sh --branch feat/x  # validate a name without checking out
scripts/check-pr-policy.sh --self-test      # built-in test of the author check
```

## Examples

| OK                       | Not OK                |
| ------------------------ | --------------------- |
| `feat/hybrid_plan_split` | `hybrid-plan-split`   |
| `bugfix/invariant_number`| `fix/INV-12`          |
| `docs/quickstart_links`  | `docs add quickstart` |
| `tests/grpc_smoke`       | `add-tests`           |
| `work/spike_hot_path`    | `wip/foo`             |
