# Rebase candidate report: upstream `transports/v2.4.0`

Date: 2026-11-04

## Scope and safety

This candidate was prepared in a new worktree:

```text
/home/felipe/dev/bifrost-wt/rebase-v2.4.0
branch: sync/rebase-upstream-v2.4.0
```

It is based on upstream `transports/v2.4.0` at `e869c3f9adc96ea53c38694a21bb04a8e1085487`.

The recorded pre-rebase branch tip was `expertise-main` at `a33b06b22f6ad2d0c517d549934d7273cc72afaa`. This candidate did not change that branch, push any ref, deploy an image, or change production.

## Candidate head

The completed replay head was `5f50c0be52ea0d5b68c639f5b0b7c363590c30f5`. Post-replay follow-up commits (detailed in "Post-review corrections") bring the current head to `50ed9c4922225986cc56babb4edc4976a3d69fc6`:

```text
5f50c0be5  replay head (rebase onto transports/v2.4.0)
89138b55b  test(core): adapt enqueue hook test to TTFT API
60daa6c1a  docs: use controlled upstream rebases
ff7a5ec9e  docs: record v2.4.0 rebase candidate
e653707d9  test(core): align idle timeout coverage with first byte
5f7d5fe3d  build: retain local framework and telemetry modules
50ed9c492  docs: record rebase follow-up evidence   <- current head
```

`89138b55b` adds the required zero `firstTokenTimeout` argument to the pre-enqueue post-hook stream test. Zero disables TTFT and matches the no-fallback primary path that the test models.

## Replay dispositions and decisions

No local capability was silently dropped. All replay units were retained or adapted. The most significant decisions follow.

| Original commit | Candidate commit | Disposition | Decision |
|---|---|---|---|
| `f1228be11` | `446b92141` | Adapted | Preserve a cloned pre-dispatch fallback list while retaining upstream inferred-fallback caller-fault suppression, routing logs, cancellation handling, and `AllowFallbacks` policy. |
| `c6d192209` | `930acd8c0` | Adapted | Keep request-time custom-provider fallback parsing, including provider-empty entries, together with upstream warning/skip behavior, model inheritance, key pins, and TTFT behavior. |
| `25c20022a` | `3d045a628` | Adapted | Keep idle token-gap timeouts, SSE comment handling, empty-stream fallback, and cleanup. Retain upstream TTFT deadlines and separate local rejected-stream cleanup from upstream `StreamAttemptAbort`. |
| `a0e0d106c` | `a8bb50889` | Adapted | Keep fork `ai_bifrost_*` fallback counters while retaining upstream idempotent `bifrost_active_requests`, A2A, relay, and overhead accounting. |
| `ffd60285c` and dependent telemetry chain | `a2ffeb9d4` through `16cdf9334` | Adapted | Preserve fork `ai_inference_*` lifecycle metrics, immutable async-finalization snapshots, local reason attribution, synchronized attempts, and model labels alongside upstream telemetry. The initial lifecycle commit had expected intermediate failures; dependent snapshot/finalization commits resolved them before final validation. |
| `3a76a84cb` | `8c503de85` | Adapted | Preserve configurable session affinity: `fallback_allowed`, `primary_only`, and `disabled`. In `primary_only`, a fallback clears the failed primary binding but is not recorded as preferred. Preserve upstream direct-key protection. |
| `287fead2f` | `ea93b85b7` | Adapted | Preserve idempotent configstore migration, `fallback_allowed` default, save-time validation/defaulting, and compatible routing-rule hashing. |
| `ce5dc47a0` | `44a0f32b0` | Adapted | Apply affinity mode through the routing engine and HTTP API while retaining upstream TTFT target deadline, provider selection, fallback key pins, and direct-key behavior. |
| `71b879422` | `afa746da4` | Retained | Preserve the pre-enqueue post-hook fix: queue-full, provider-closing, nil request, and caller-cancelled paths execute terminal post hooks so fork inflight accounting can release. |

Other retained replayed capabilities include Qwen KV session headers, OpenAI error-envelope handling, provider response buffering, retry-only fallback coverage, stream first-byte bounds, token-gap idle timeouts, storage-failure fallback coverage, routing/configstore local-module build integration, and fork branch documentation.

## Complete replay ledger (original `expertise-main` → candidate)

Exhaustive 1:1 mapping of all 32 replayed units. Original SHAs are from pre-rebase `expertise-main` (`transports/v2.2.3..a33b06b22`); candidate SHAs are from `transports/v2.4.0..5f50c0be5`. Pairs match by subject. Disposition: **Adapted** = patch reconciled with upstream changes; **Retained** = replayed cleanly (feature/test/doc/build, no content rewrite); **Retained (merge)** = merge commit re-created by `--rebase-merges`.

| # | Original | Candidate | Subject | Disposition |
|---|---|---|---|---|
| 1 | `c8a8df4cc` | `63b877760` | fix(openai): increase response read buffer | Retained |
| 2 | `f1228be11` | `446b92141` | fix(core): preserve fallbacks across provider dispatch | Adapted |
| 3 | `a7c09e286` | `c4f0b40c3` | feat(openai): forward client session headers to qwen-kv-router as x-kv-session-id | Retained |
| 4 | `c6d192209` | `930acd8c0` | fix(routing): resolve custom-provider fallbacks at request time | Adapted |
| 5 | `2a8b10d59` | `6e6c70cc7` | fix(openai): treat explicit error envelopes in 200 responses as errors | Retained |
| 6 | `25c20022a` | `3d045a628` | fix(core): make stream fallbacks robust to idle timeouts, SSE comments and empty streams | Adapted |
| 7 | `59302ea8e` | `eac092fcd` | build(transports): build the local plugins/routing module | Retained |
| 8 | `ffd60285c` | `a2ffeb9d4` | feat(telemetry): add inference lifecycle metrics (ai_inference_*) with finalization guard | Adapted |
| 9 | `a0e0d106c` | `a8bb50889` | feat(telemetry): observe real fallback transitions, retries and chain results | Adapted |
| 10 | `2b154bcdb` | `3e1adf41f` | fix(telemetry): capture fallback index and outcome before the async finalization | Adapted |
| 11 | `a7805f219` | `4dfd2d215` | fix(core): make normalizeFallbackReason a package-level function | Adapted |
| 12 | `2965fd9fb` | `f9d4d609a` | test(telemetry): add retry-only fallback test and gofmt fallback telemetry | Retained |
| 13 | `184bbbb31` | `ce3eea983` | Merge grafana-inference-t3 into t2: combine inference lifecycle and fallback telemetry | Retained (merge) |
| 14 | `acfdcf911` | `4250634f1` | fix(telemetry): use one inference predicate so token counting is not a fallback terminal | Adapted |
| 15 | `9afaa77d4` | `7ff928709` | feat(telemetry): carry exact local cause into fallback transition local_reason | Adapted |
| 16 | `5a4943913` | `75966ae7d` | fix(telemetry): keep local_reason when raw response capture is off | Adapted |
| 17 | `8efd19446` | `59ee6dcea` | test(telemetry): drive local_reason through the real fallback loop | Retained |
| 18 | `b1cf05158` | `b6e61c4c4` | fix(telemetry): classify refused connections as transport local_reason | Adapted |
| 19 | `ba8e3b901` | `e8c617304` | fix(telemetry): synchronize inference attempt tracking | Adapted |
| 20 | `1742d0e7e` | `021b210a1` | fix(core): report the last executed fallback origin | Adapted |
| 21 | `91439aa16` | `5bb612fb4` | style(telemetry): gofmt main_test.go | Retained |
| 22 | `4f28807c4` | `16cdf9334` | fix(telemetry): keep luna and terra model labels in the inference whitelist | Adapted |
| 23 | `4100918cb` | `5fb37e3e8` | test(core): cover storage failure HTTP fallback | Retained |
| 24 | `3a76a84cb` | `8c503de85` | feat(core): add configurable session affinity | Adapted |
| 25 | `287fead2f` | `ea93b85b7` | feat(configstore): persist session affinity mode | Adapted |
| 26 | `ce5dc47a0` | `44a0f32b0` | feat(routing): apply session affinity to bifrost requests | Adapted |
| 27 | `4a29953b0` | `da84ffb82` | fix(providers): stream idle timeout measures gaps between tokens only | Retained |
| 28 | `f5cc60cfd` | `6072af2ce` | fix(providers): bound the wait for the first stream byte to 13 minutes | Retained |
| 29 | `dc8615556` | `f9e733d1f` | Merge branch 'grafana-inference-int' into local/v2.2.3-patched | Retained (merge) |
| 30 | `84763920d` | `eaf298823` | docs: define expertise-main as the official branch of our fork | Retained |
| 31 | `71b879422` | `afa746da4` | fix(core): run post hooks when a request fails before it is enqueued | Retained |
| 32 | `a33b06b22` | `5f50c0be5` | Merge branch 'fix/telemetry-inflight-leak' into expertise-main | Retained (merge) |

Two sibling pairs replayed in swapped order under `--rebase-merges` (content preserved, only the linear position inside the merge changed):

- Telemetry merge `184bbbb31` → `ce3eea983`: the `observe real fallback transitions` unit (`a0e0d106c` → `a8bb50889`) now precedes the `inference lifecycle metrics` unit (`ffd60285c` → `a2ffeb9d4`); in the original the lifecycle unit came first.
- `grafana-inference-int` merge `dc8615556` → `f9e733d1f`: the `storage failure HTTP fallback` unit (`4100918cb` → `5fb37e3e8`) now follows the `configurable session affinity` unit (`3a76a84cb` → `8c503de85`); in the original the storage-failure unit came first.

No unit was dropped. To re-derive this table: `git log --reverse --format='%h %s' transports/v2.2.3..a33b06b22` (original) and `git log --reverse --format='%h %s' transports/v2.4.0..5f50c0be5` (candidate), joined by subject.

## Fork delta: old vs new patch-equivalence

The fork's own footprint is `expertise-main` over its old base `transports/v2.2.3` (56 files, +5361/-339) versus the candidate over the new base `transports/v2.4.0` (57 files, +5568/-327; the extra file is this report). Comparing per-file +/- line counts between those two deltas:

- **40 of 56 files carried over byte-identically** — the fork's patch applied onto v2.4.0 with no reconciliation at all.
- The 16 with differing deltas break into three causes:

| File | Old delta (+/-) | New delta (+/-) | Cause |
|---|---|---|---|
| `plugins/routing/rules/engine.go` | +15/-12 | +3/-0 | Same functional change (`SessionAffinityMode` field, assignment, `ctx.SetValue`). The old delta also carried gofmt re-alignment of neighboring struct fields (adding the longest field name shifted alignment); that churn does not occur on the v2.4.0 layout. |
| `framework/configstore/migrations.go` | +19/-7 | +13/-0 | Same functional change (`add_routing_session_affinity_mode` migration registration and function). The old delta also carried gofmt re-alignment of an unrelated anonymous struct (3 lines each way) that is absent on v2.4.0. |
| `plugins/routing/main.go` | +5/-0 | +3/-0 | Smaller delta; not individually analyzed beyond line counts. |
| `core/bifrost.go` | +213/-69 | +219/-70 | Minor reconciliation with upstream fallback/dispatch changes. |
| `core/bifrost_test.go` | +82/-45 | +84/-46 | Minor reconciliation. |
| `core/providers/openai/openai.go` | +32/-13 | +33/-14 | Minor reconciliation. |
| `core/providers/utils/stream.go` | +55/-2 | +55/-3 | Minor reconciliation. |
| `core/providers/utils/utils.go` | +236/-70 | +234/-72 | Minor reconciliation. |
| `plugins/telemetry/main.go` | +261/-1 | +262/-1 | +1 line reconciliation. |
| `transports/bifrost-http/handlers/routing_test.go` | +42/-0 | +39/-1 | Minor reconciliation. |
| `core/streamfallback_matrix_test.go` | +465/-0 | +473/-0 | +8 lines from first-byte test alignment (`e653707d9`). |
| `core/streamfallback_timeouts_test.go` | +480/-0 | +486/-0 | +6 lines from first-byte test alignment (`e653707d9`). |
| `transports/go.mod` | +4/-0 | +7/-0 | +3 lines from the build fix (`5f7d5fe3d`). |
| `transports/Dockerfile` | +5/-0 | +8/-0 | +3 lines from the build fix (`5f7d5fe3d`). |
| `transports/Dockerfile.redhat` | +5/-0 | +8/-0 | +3 lines from the build fix (`5f7d5fe3d`). |
| `FORK.md` | +69/-0 | +80/-0 | +11 lines from the controlled-rebase policy (`60daa6c1a`). |
| `REBASE_V2.4.0_REPORT.md` | +0/-0 | +188/-0 | New file (this report). |

Only `engine.go` and `migrations.go` were diffed line by line (the shrink there is formatting churn, not upstream absorption of fork logic). The other minor-reconciliation rows are judged from line counts alone and were not individually reviewed. To re-derive: `git diff --numstat transports/v2.2.3 a33b06b22` vs `git diff --numstat transports/v2.4.0 <candidate-head>`, joined by path.

## Focused validation that passed

All runs used Docker `golang:1.27.3-bookworm`, candidate source mounted read-only, a temporary external Go workspace/cache, and local modules linked through `go.work`.

| Scope | Evidence |
|---|---|
| Core fallback policy | `go test ./ -run "Fallback|Inferred" -count=1` passed after `446b92141`. |
| Routing custom fallback resolution | `go test /src/plugins/routing -run TestApplyRoutingRules -count=1` passed after `930acd8c0`. |
| Streaming/fallback robustness | Core/provider utility/OpenAI/Azure focused stream, idle, SSE, fallback, attempt-abort, and first-token tests passed after `3d045a628`. |
| Telemetry lifecycle | Full telemetry package and core fallback/retry/cancel/first-token tests passed after the complete dependent telemetry chain at `16cdf9334`. |
| Session affinity | Core, routing, routing rules, transport handlers, configstore, and core affinity-focused tests passed after `44a0f32b0`. |
| Pre-enqueue post hooks / inflight release | `go test . -run '^TestEnqueueFailureRunsPostHooks$' -count=1` passed after `89138b55b`; all six queue/provider/caller-gone stream and non-stream subtests passed. |
| Qwen KV headers | Focused `TestSetKVSessionHeaders` test passed. |
| Static checks | `go vet` passed for core, telemetry, routing, and framework/configstore. |
| Full module suites | Full telemetry, routing, and framework/configstore suites passed. |

## Checks not fully green

At the current head the candidate passes the core, telemetry, routing, and configstore suites plus their `go vet` scopes, and the non-workspace module graph resolves the four local modules. The stream/idle suites, which initially failed on a pre-existing first-byte/idle semantic mismatch, are resolved by the first-byte contract decision below. The only remaining non-green items are test-prerequisite and upstream-timing issues, none attributable to the rebase.

### Core stream/idle suites: resolved by the first-byte contract decision

The five stream/idle families originally failed identically on candidate `89138b55b` and pre-rebase `a33b06b22` (67 passing, 13 failing subtests): `stall_headers`/`keepalive_forever` in `TestStreamFailureMatrix`; silent-after-headers and comments-only cases in `TestTimeoutPrimaryLimits`; `TestTimeoutIdleClosesPrimaryConnection`/`TestTimeoutCommentsOnlyClosesPrimaryConnection`; and all eight `TestTimeoutChainUsesEachProvidersOwnLimits` cases. The shared mechanism: `idleTimeoutReader` starts the 13-minute `DefaultStreamFirstByteTimeout` at response headers and switches to the configured token-gap idle timeout only after the first useful (non-comment) SSE frame; the old tests expected the configured idle timeout to begin at headers. That mismatch is pre-existing, not a rebase regression.

Decision made: keep the first-byte-window production behavior and realign the tests to it (`e653707d9`). The old idle/comment tests were renamed to `TestTimeoutIdleAfterRoleClosesPrimaryConnection` and `TestTimeoutCommentsAfterRoleClosesPrimaryConnection`, and a `TestTimeoutCommentsOnlyCallerCancel` case was added; headers and comments stay inside the first-byte window and the token-gap idle timer starts on the first useful frame. Re-run at the current head, all five families pass (see "Reproducible evidence") and the full core suite shows zero stream/idle failures.

### ListAllModels slow pagination: upstream test fragility

`TestListAllModelsIncludesEveryPageOfASlowLargeProvider` does not exist in pre-rebase `a33b06b22`. It is an upstream v2.4.0 test from PR #8767. Candidate code and test are byte-identical to pure upstream v2.4.0 for this path.

The test failed 5/5 on both candidate and pure upstream v2.4.0 in Docker Go 1.27.3. The mock sleeps 300 ms per page but sets a 500 ms deadline, leaving only 200 ms scheduler/HTTP overhead. The result is `context deadline exceeded` at about 0.5-0.7 seconds. No fork code changes `ListAllModels`; candidate changes to `core/bifrost.go` are outside this function.

No candidate fix is justified. If desired, report or fix upstream test timing headroom.

### Generated prerequisites: built, gates now run

Both prerequisites were generated in the worktree (all outputs are gitignored, so the tree stays clean):

- MCP example servers: 10 Go binaries (`examples/mcps/*/bin`) and 3 TypeScript builds (`examples/mcps/*/dist`), using the same per-directory commands as `make setup-mcp-tests`.
- UI: `npm ci && npm run build` in `ui/` (which runs `copy-build` into `transports/bifrost-http/ui/`), with Node 22.

The harness image `golang:1.27.3-bookworm` has no Node, and `go test` spawns `node dist/index.js` for the STDIO tests. Assets alone are therefore not enough: the test container needs Node on `PATH`. A derived image (`golang:1.27.3-bookworm` plus Node 22) was used for the runs that need it. The first run with assets but without Node still failed 22 tests (`unstable` MCP clients), which is why Node matters.

### Not run

- Race detector was not run.
- PostgreSQL configstore migration execution was not run; SQLite idempotency coverage passed.
- Live-provider checks were not run. The release image was built later and production was switched to it; see "Release image and production switch".

## Post-review corrections

The candidate head now includes these follow-up commits:

| Commit | Change | Evidence |
|---|---|---|
| `89138b55b` | Adapt `TestEnqueueFailureRunsPostHooks` to upstream `tryStreamRequest` TTFT argument. | All six queue-full, provider-closing, and caller-cancelled stream/non-stream subtests pass with Docker Go 1.27.3. |
| `60daa6c1a` | Replace the fork policy's upstream-merge instruction with controlled rebase preparation. | `FORK.md` requires a temporary candidate branch, patch disposition ledger, retained/adapted local features, and no rewrite of published `expertise-main`. |
| `e653707d9` | Align stream timeout coverage to the selected first-byte contract. | Headers and comments remain in the first-byte phase; useful role/data frames begin token-gap idle coverage. The affected matrix/timeout families pass. |
| `5f7d5fe3d` | Retain local framework and telemetry in non-workspace transport builds. | `GOWORK=off go list -mod=readonly -m -json all` resolves local `core`, `framework`, `plugins/routing`, and `plugins/telemetry` replacements. |

The release Dockerfiles now copy those four sibling module directories before `go mod download` and keep `GOWORK=off`. This avoids silently replacing session-affinity schema/persistence or fork telemetry with published module versions.

## Reproducible evidence

The candidate test harness is `/home/felipe/.cache/bifrost-rebase-v240-verify/run.sh`. It uses Docker `golang:1.27.3-bookworm`, source mounted read-only, an external `go.work`, and isolated cache directories. All gates below were re-run fresh against the current head `e0cf9ddfc` (log files `*-fresh.log` / `*-fresh.rc` under `/home/felipe/.cache/bifrost-rebase-v240-verify/logs/`).

| Gate | Command scope | Fresh result at `e0cf9ddfc` |
|---|---|---|
| Core post-hook/inflight | `go test . -run '^TestEnqueueFailureRunsPostHooks$' -count=1` | Pass; six subtests. |
| Stream first-byte contract | `go test . -run` over the five families named below, `-count=1 -v` (exact regex in `core-fb-fresh.log`) | Pass; `TestStreamFailureMatrix` 9.67s, `TestTimeoutPrimaryLimits` 0.00s, `TestTimeoutIdleAfterRoleClosesPrimaryConnection` 2.01s, `TestTimeoutCommentsAfterRoleClosesPrimaryConnection` 2.01s, `TestTimeoutChainUsesEachProvidersOwnLimits` 0.00s. |
| Qwen KV headers | `go test ./providers/openai/ -run '^TestSetKVSessionHeaders'` | Pass. |
| Core full suite, no assets (`core-full-fresh`) | `go test ./... -count=1` | 40 failing: 39 MCP/STDIO tests (assets missing) plus `ListAllModels`; zero stream/idle failures. |
| Core full suite, assets and Node (`core-full-node`) | `go test ./... -count=1` with MCP assets built and Node 22 | `core/internal/mcptests` passes (`ok`, 177s). Two core failures remain, see below. |
| MCP tests alone (`mcp-cand-alone`) | `go test ./internal/mcptests/ -count=1`, `GOWORK=off` | Pass (`ok`, 144s), 13 of 13 servers built. |
| `bifrost-http` (`bifhttp-full-assets`, `bifhttp-vet-assets`) | `go test ./bifrost-http/... -count=1` and `go vet ./bifrost-http/...` with UI built | Both exit 0. Root package compiles with the embedded UI. |
| Telemetry | `go test ./...` in `plugins/telemetry` | Pass. |
| Routing | `go test ./...` in `plugins/routing` | Pass. |
| Configstore | `go test ./configstore/...` in `framework` | Pass. |
| Static checks | `go vet ./...` for core, telemetry, routing, configstore | Pass. |
| Production module provenance | `GOWORK=off go list -mod=readonly -m -json all` in `transports` | Pass; exactly four local replaces — `core`→`…/core`, `framework`→`…/framework`, `plugins/routing`→`…/plugins/routing`, `plugins/telemetry`→`…/plugins/telemetry` — while the untouched plugins resolve to published versions. |

## Remaining validation gaps

- `TestListAllModelsIncludesEveryPageOfASlowLargeProvider` still fails. It is an upstream v2.4.0 test with 200 ms of timing headroom, and it failed 5 of 5 on pure upstream as well. Not a candidate defect.
- `TestStreamingPostHookSpans_SiblingsWithRealDuration` failed once in the full run (`slow posthook span lasts 57.9ms, want about 30ms`). It is a wall-clock assertion. In isolation it passed 8 of 8 on the candidate and 8 of 8 on pure upstream `v2.4.0`, so I treat it as load-sensitive, not as a regression. I did not repeat the full run to prove it flaky, so this is a judgment, not a measurement.
- Race tests and PostgreSQL configstore migration execution were not run. Production uses SQLite, so the PostgreSQL path is not exercised by the switch either.

## Release image and production switch

### Image build

The two earlier image builds hung in `apk upgrade`. The default Docker bridge cannot reach `dl-cdn.alpinelinux.org`: `apk update` hung there and finished in about 11 s with `--network host`. From the host, `curl -4` to the mirror works and `curl -6` is refused at once. I did not prove the exact cause of the bridge failure; the host-network workaround is what was verified.

Build command: `docker build --network host -f transports/Dockerfile` at head `95bf7907e`, run detached with `setsid nohup` (script `build-image.sh`). Result: exit 0, tag `bifrost:v2.4.0-rebase-candidate-95bf7907e`, 106 MB. The banner shows `vunknown` because the Dockerfile received no `VERSION`.

Checks on the image:

- `go version -m` on `/app/main` shows `core`, `framework`, `plugins/routing`, `plugins/telemetry` replaced with `../…` (local code); the other plugins come from published versions. This is the image-level proof of the Dockerfile `COPY` change.
- Boot on an empty data directory: `/health` returns ok, the embedded UI is served, plugins `telemetry` and `routing` report active, `routing_rules.session_affinity_mode` exists, and a second boot on the same database succeeds.

### Migration rehearsal on a copy of production data

Production data was copied with the SQLite backup API (`config.db` 40 MB in 0.3 s, `logs.db` 20 GB in 169 s) and the new image was started on the copy with `--network none`. Bootstrap took 7.6 s. The v2.4.0 image applied its pending migrations: `config.db` ended with 306 migration rows and `logs.db` with 108, `session_affinity_mode` and `ttft_timeout_ms` exist, and `PRAGMA quick_check` on `config.db` returned `ok`. `quick_check` was skipped on the 20 GB `logs.db`. Upstream v2.2.3 to v2.4.0 adds 75 `configstore` and 16 `logstore` migration ids.

### Production switch

Done with `prod-swap.sh`: stop, cold backup, start the new image, wait for health, automatic rollback (restore the databases and the old image) if anything fails. Stop and start are in one script, as `docs/bifrost-litellm.md` requires.

- Window: 11:18:09 to 11:20:33 UTC, about 2 min 24 s, almost all of it the 21.5 GB backup.
- Backup: `~/.local/state/bifrost/backups/pre-v2.4.0-20261105T111809Z` (21,538,388,536 bytes, matched the source size). The previous unit is saved next to the live unit as `ai-terminal-bifrost.service.bak-pre-v2.4.0-*`.
- Old image kept: `bifrost:v2.2.3-gapfix-aiinf-a33b06b`.
- After the switch: unit active, 0 restarts, `/health` ok, `/v1/models` 200, no error-level log lines except provider and DNS warnings.
- `logs.db` after the switch (11:20 to 12:58 UTC): 93 success and 21 error rows. Before (09:40 to 11:18 UTC): 80 success and 24 error. The errors after the switch are provider 429s and `qwen-kv-router` 504/503 (`prefill_save_failed`, `all slots busy`). I did not compare them to the same errors before the switch in detail.
- Rollback caveat: rolling back the image alone is not safe once the databases are migrated. Use the backup directory above.

### `/metrics` 403 regression: fixed

v2.4.0 adds `SetupLockMiddleware`. When dashboard auth is not active and no setup token is set, it rejects every route that is not in `apiSystemWhitelistedRoutes` or the configured `whitelisted_routes`. `/metrics` was in neither list, so after the first switch it answered 403, the metrics proxy on `:9103` answered 502, and Prometheus `up{job="bifrost"}` fell to 0. v2.2.3 had no such lock and served `/metrics` openly.

Fix `0c730986f` adds the exact path `/metrics` to `apiSystemWhitelistedRoutes`, with a test (`TestSetupLockMiddleware_MetricsStaysOpen`) that fails before the change (401) and passes after, and checks that `/metrics/extra`, `/metricsx` and `/api/metrics` stay locked. A new image `bifrost:v2.4.0-rebase-candidate-0c730986f` was built with `VERSION=2.4.0-expertise`, so `/api/version` now reports `v2.4.0-expertise` instead of `vunknown`.

Second production switch (image only, no database change, about 38 s from restart to health): `/metrics` 200 directly and through the proxy, Prometheus `up{job="bifrost"}` back to 1, `/api/providers` still 403. The rollback of this step is the previous image only; the databases were not touched. Series seen: `ai_inference_dispatches_total`, `executions_total`, `inflight`, `output_tokens_total`, `requests_total`, and `bifrost_active_requests`. No `ai_bifrost_fallback_*` or `retry_attempts` series were present, because the 7 requests since the restart had no fallback; the code registers them as labeled counters, so I expect them to appear on the first fallback, but I have not observed that yet.

## Review recommendation

The candidate is ready for review, with the open items below. Evidence at the current head `e0cf9ddfc`:

- The rebase onto `transports/v2.4.0` is complete with no local capability dropped — all 32 replayed units were retained or adapted (full ledger above), and the two `--rebase-merges` sibling reorders are documented.
- The first-byte stream-timeout contract is decided and covered by passing tests; the full core suite shows zero stream/idle failures.
- Core, telemetry, routing, and configstore suites and their `go vet` scopes pass; the non-workspace module graph resolves the four local modules, so the release image preserves fork framework and telemetry.
- With the MCP and UI assets built and Node on `PATH`, the MCP suite and the `bifrost-http` tests and vet pass. The only red core test that reproduces is the upstream `ListAllModels` timing test. One wall-clock test (`StreamingPostHookSpans`) failed once under load and passes in isolation on both candidate and pure upstream.
- Not proven: race tests and PostgreSQL migrations. The release image now exists and production runs it (next section), and the `/metrics` regression is fixed.

Before publishing: review the candidate delta against `expertise-main` and this report, and confirm that the `ai_bifrost_*` fallback series appear on the first real fallback. Do not publish until that review is signed off. The candidate branch did not change `expertise-main`, push any ref, deploy an image, or touch production.
