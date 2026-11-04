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

The completed replay head was `5f50c0be52ea0d5b68c639f5b0b7c363590c30f5`. A post-replay test adaptation is committed as `89138b55b5385c4c4c88886fde2ee5c65baad405`:

```text
test(core): adapt enqueue hook test to TTFT API
```

It adds the required zero `firstTokenTimeout` argument to the pre-enqueue post-hook stream test. Zero disables TTFT and matches the no-fallback primary path that the test models.

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

## Review recommendation

Hold this candidate for review rather than promotion. The rebase is complete, local features were retained/adapted, and targeted capability tests pass. Before integration or production deployment, decide the stream idle contract, provide generated MCP/UI test prerequisites, and rerun the affected full gates. Review the candidate delta against `expertise-main` and this report before publishing any branch.
