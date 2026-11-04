# Expertise fork of Bifrost: the `expertise-main` branch

This file is about our fork only. It is not part of upstream Bifrost.

## What `expertise-main` is

`expertise-main` is the official branch of our fork. It plays the same role for us that `main` plays for upstream (`maximhq/bifrost`).

- It holds upstream Bifrost plus every change we made on top of it.
- It is the only branch we deploy from.
- It is the only branch that other work is merged into.

The branch is named after its role, not after what it contains or after an upstream version. Its content changes with time. Its role does not.

## Remotes

| Remote | URL | Use |
|---|---|---|
| `origin` | `git@github.com:felipealmeida/bifrost.git` | Our fork. We push here. |
| `upstream` | `https://github.com/maximhq/bifrost.git` | Upstream. We only fetch from it. |

`origin/main` is a plain mirror of `upstream/main`. Never commit to it and never merge our work into it.

## Rules

1. Every change to Bifrost ends up in `expertise-main`. A change that is not in `expertise-main` is not part of our Bifrost.
2. Start new work from `expertise-main`. Use a short-lived branch (for example `fix/stream-timeout`).
3. When the work is done and tested, merge the branch into `expertise-main` and push it to `origin`.
4. Do not deploy from a feature branch. Build the deploy image from a commit that is already in `expertise-main`.
5. Do not rewrite the history of `expertise-main` (no force push, no rebase of published commits). Other people and the deploy build depend on the commit IDs.
6. Run the build and the tests of the packages you touched before you merge. At minimum, build `core` and run the tests of the changed packages.

## Normal workflow

```sh
git fetch origin
git switch -c fix/my-change origin/expertise-main
# work, commit, test
git switch expertise-main
git pull --ff-only origin expertise-main
git merge --no-ff fix/my-change
git push origin expertise-main
```

## Taking in upstream changes

We follow upstream on purpose, not automatically. When we need a newer upstream version or a specific upstream fix, prepare it with a controlled rebase. Keep local corrections and features that still make sense. Adapt them when upstream changed the surrounding code. Do not silently drop them.

This procedure prepares a rebased candidate. It does not authorize rewriting published `expertise-main`.

1. Fetch and pin the upstream input. Record the upstream remote, tag or commit, and its resolved commit ID.
2. Record the current `expertise-main` tip and create a recovery reference before replaying anything.
3. Create a dedicated, non-shared `sync/rebase-upstream-<tag-or-date>` branch from that recorded tip.
4. Make a complete disposition record for our delta, including merge resolutions: retain, adapt, superseded upstream, or intentionally drop. Every dropped or skipped patch needs evidence. Removing a non-equivalent local behavior needs explicit user approval.
5. Rebase the candidate's local delta onto the pinned upstream input. Resolve every conflict by comparing upstream behavior with the local patch's intent. Pay special attention to `core`, telemetry, routing, configstore, and streaming paths.
6. Review the old-to-new patch correspondence, conflict resolutions, final tree diff, and all commits proposed for publication. Run the relevant builds and regression tests, including retained custom behavior. Record unavailable checks as unavailable.
7. Prepare the final integration on a second temporary branch based on the recorded `expertise-main` tip. Test that integration result too and compare its tree with the accepted rebased candidate. If `expertise-main` moved, repeat this step.

```sh
git fetch upstream
git switch -c sync/rebase-upstream-<tag-or-date> <recorded-expertise-main>
# inventory local patches and pin the upstream commit
# rebase the candidate local delta onto <pinned-upstream-commit>
# resolve conflicts, review patch dispositions, build, and test
# prepare and test integration from <recorded-expertise-main>
```

A rebased candidate normally cannot fast-forward `expertise-main`, because replaying patches changes their commit IDs. Fast-forward only when ancestry permits. Otherwise, publishing it requires a separately reviewed history-preserving merge into `expertise-main`; that merge retains the old published history as well as the rebased candidate. If a linear replacement history is required, stop and request a separate policy decision. This procedure does not authorize replacing `expertise-main`.

Never force-push `expertise-main` or another published integration branch. A force update is permitted only for a dedicated, non-shared temporary sync branch, with explicit user approval, an exact remote/ref, and `--force-with-lease` tied to the previously observed remote commit. If the lease fails, stop and investigate.

Known conflict areas, from a trial upstream integration on 2026-10-22: `framework/configstore/migrations.go` and `plugins/routing/main.go`. Read both sides before you resolve them.

## Where we started

`expertise-main` was created on 2026-10-22 from the branch `local/v2.2.3-patched`. That branch is built on the upstream tag `transports/v2.2.3` (common ancestor `411d62b28`). The older branch name `local/v2.2.3-patched` is kept only so old references still work. Do not add new work to it.

## Related branches

- `grafana-inference-int`, `grafana-inference-t2`, `grafana-inference-t3`: earlier feature branches. Their content is already in `expertise-main`. Do not use them for new work.
