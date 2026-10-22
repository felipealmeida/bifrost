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

We follow upstream on purpose, not automatically. Do this when we want a newer upstream version or a specific upstream fix.

```sh
git fetch upstream
git switch -c sync/upstream-<tag-or-date> expertise-main
git merge upstream/main        # or a release tag, for example transports/v2.2.4
# resolve conflicts, build, run the tests
git switch expertise-main
git merge --no-ff sync/upstream-<tag-or-date>
git push origin expertise-main
```

Known conflict areas, from a trial merge of `upstream/main` on 2026-10-22: `framework/configstore/migrations.go` and `plugins/routing/main.go`. Read both sides before you resolve them.

Merge upstream into our branch. Do not rebase our branch onto upstream.

## Where we started

`expertise-main` was created on 2026-10-22 from the branch `local/v2.2.3-patched`. That branch is built on the upstream tag `transports/v2.2.3` (common ancestor `411d62b28`). The older branch name `local/v2.2.3-patched` is kept only so old references still work. Do not add new work to it.

## Related branches

- `grafana-inference-int`, `grafana-inference-t2`, `grafana-inference-t3`: earlier feature branches. Their content is already in `expertise-main`. Do not use them for new work.
