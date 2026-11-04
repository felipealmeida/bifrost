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

### Generated prerequisites absent

- Core MCP integration tests need generated Node/Go example assets under `examples/mcps/*/dist` and `examples/mcps/*/bin`. They were absent. This caused MCP test failures unrelated to the candidate.
- The root `transports/bifrost-http` package and its vet command require the gitignored generated directory `transports/bifrost-http/ui/` for `//go:embed all:ui`. It was absent. The `bifrost-http` library subpackages passed, but root package/vet cannot be claimed green until the UI build prerequisite exists.

### Not run

- Race detector was not run.
- PostgreSQL configstore migration execution was not run; SQLite idempotency coverage passed.
- Live-provider, production, image build, and deployment checks were intentionally not run.

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
| Core full suite | `go test ./... -count=1` | 40 failing, all `TestAgent_*` / `TestCodeMode_STDIO_*` (missing generated MCP assets) plus `TestListAllModelsIncludesEveryPageOfASlowLargeProvider` (upstream timing); zero stream/idle failures. |
| Telemetry | `go test ./...` in `plugins/telemetry` | Pass. |
| Routing | `go test ./...` in `plugins/routing` | Pass. |
| Configstore | `go test ./configstore/...` in `framework` | Pass. |
| Static checks | `go vet ./...` for core, telemetry, routing, configstore | Pass. |
| Production module provenance | `GOWORK=off go list -mod=readonly -m -json all` in `transports` | Pass; exactly four local replaces — `core`→`…/core`, `framework`→`…/framework`, `plugins/routing`→`…/plugins/routing`, `plugins/telemetry`→`…/plugins/telemetry` — while the untouched plugins resolve to published versions. |

## Remaining validation gaps

- The root `transports/bifrost-http` package and its `go vet` check still need the gitignored generated `transports/bifrost-http/ui/` assets for `//go:embed all:ui`; its library subpackages pass.
- Core MCP integration tests (`TestAgent_*`, `TestCodeMode_STDIO_*`, `TestToolsToExecute_*`, `TestRefreshClientTools_*`, `TestHealthCheckSTDIOServer*`) still need generated example Node/Go assets under `examples/mcps/*/dist` and `examples/mcps/*/bin`; 39 such tests fail until those assets are built. These exercise upstream MCP agent/tool-calling infrastructure the fork did not modify, so the failures are a test-environment prerequisite gap, not a rebase regression.
- `TestListAllModelsIncludesEveryPageOfASlowLargeProvider` is an upstream v2.4.0 test that fails on timing headroom on both candidate and pure upstream; it is not a candidate defect.
- Race tests, PostgreSQL configstore migration execution, release-image builds, live-provider tests, deployment, and runtime metric scraping were not run.

## Review recommendation

The candidate is ready for promotion to the fork's main branch, subject to the generated-asset prerequisites above. Evidence at the current head `e0cf9ddfc`:

- The rebase onto `transports/v2.4.0` is complete with no local capability dropped — all 32 replayed units were retained or adapted (full ledger above), and the two `--rebase-merges` sibling reorders are documented.
- The first-byte stream-timeout contract is decided and covered by passing tests; the full core suite shows zero stream/idle failures.
- Core, telemetry, routing, and configstore suites and their `go vet` scopes pass; the non-workspace module graph resolves the four local modules, so the release image preserves fork framework and telemetry.
- The only non-green core tests are the 39 generated-MCP-asset integration tests (upstream infrastructure the fork did not touch) and one upstream-timing `ListAllModels` test — neither a rebase regression.

Before publishing: build the generated MCP/UI assets so those gates run green (or record the asset gap as a known upstream test-environment limitation), then review the candidate delta against `expertise-main` and this report. Do not publish until that delta review is signed off. This candidate did not change `expertise-main`, push any ref, deploy an image, or touch production.
