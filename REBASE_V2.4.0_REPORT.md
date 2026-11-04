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

The candidate is reviewable but not fully validated for production promotion yet. The following failures are not attributed to the rebase.

### Core stream/idle suites: pre-existing failures

The five stream/idle test families were compared under matched Docker Go 1.27.3 conditions between candidate `89138b55b` and pre-rebase `a33b06b22`.

Both have the same result: 67 passing and 13 failing subtests. The failures are identical:

- `TestStreamFailureMatrix`: `stall_headers` and `keepalive_forever` wait 20 seconds with zero fallback hits.
- `TestTimeoutPrimaryLimits`: silent-after-headers and comments-only cases wait 20 seconds; slow first data arrives after 5 seconds without fallback.
- `TestTimeoutIdleClosesPrimaryConnection` and `TestTimeoutCommentsOnlyClosesPrimaryConnection`: wait 20 seconds with zero fallback hits.
- `TestTimeoutChainUsesEachProvidersOwnLimits`: all eight cases wait about 30 seconds and report only the local provider before caller context expiry.

The shared mechanism is identical in both trees: `idleTimeoutReader` starts the 13-minute `DefaultStreamFirstByteTimeout` at response headers and only switches to configured idle timeout after the first non-comment SSE byte. The tests instead expect configured idle timeout from headers. This is a pre-existing semantic mismatch, not a rebase regression.

Decision required later: change production to make configured idle begin at headers, or change the tests to match the first-byte-window design. Do not promote based on these tests until that decision is made and validated.

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

The candidate test harness is `/home/felipe/.cache/bifrost-rebase-v240-verify/run.sh`. It uses Docker `golang:1.27.3-bookworm`, source mounted read-only, an external `go.work`, and isolated cache directories. Relevant logs are under `/home/felipe/.cache/bifrost-rebase-v240-verify/logs/`.

| Gate | Command scope | Result |
|---|---|---|
| Core post-hook/inflight | `go test . -run '^TestEnqueueFailureRunsPostHooks$' -count=1` | Pass; six subtests. |
| Stream first-byte contract | `go test . -run '^(TestStreamFailureMatrix|TestTimeoutPrimaryLimits|TestTimeoutIdleAfterRoleClosesPrimaryConnection|TestTimeoutCommentsAfterRoleClosesPrimaryConnection|TestTimeoutChainUsesEachProvidersOwnLimits)$' -count=1` | Pass. |
| Qwen KV headers | `go test ./providers/openai/ -run '^TestSetKVSessionHeaders'` | Pass. |
| Telemetry | `go test ./...` in `plugins/telemetry` | Pass. |
| Routing | `go test ./...` in `plugins/routing` | Pass. |
| Configstore | `go test ./configstore/...` in `framework` | Pass. |
| Static checks | `go vet` for core, telemetry, routing, and configstore scopes | Pass. |
| Production module provenance | `GOWORK=off go list -mod=readonly -m -json all` in `transports` | Pass; four expected local replacements. |

## Remaining validation gaps

- The root `bifrost-http` package and its vet check still need generated `transports/bifrost-http/ui/` assets for `//go:embed`.
- Core MCP tests still need generated example Node/Go assets under `examples/mcps/*/dist` and `examples/mcps/*/bin`.
- Race tests, PostgreSQL migration execution, release-image builds, live-provider tests, deployment, and runtime metric scraping were not run.

## Review recommendation

Keep the candidate in review until the generated MCP/UI prerequisites allow the remaining full build gates to run. The rebase is complete; local features were retained/adapted, the selected first-byte contract is covered by tests, and the non-workspace module graph now preserves fork framework and telemetry features. Review the candidate delta against `expertise-main` and this report before publishing any branch.
