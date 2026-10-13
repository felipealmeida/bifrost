package telemetry

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	bifrost "github.com/maximhq/bifrost/core"
	schemas "github.com/maximhq/bifrost/core/schemas"
	"github.com/prometheus/client_golang/prometheus"
)

// newTestPlugin builds a PrometheusPlugin on a fresh registry with no pricing manager (cost
// skipped) and no custom labels, so each test's counters start at zero and are unambiguous.
func newTestPlugin(t *testing.T) *PrometheusPlugin {
	t.Helper()
	p, err := Init(&Config{}, nil, bifrost.NewDefaultLogger(schemas.LogLevelError))
	if err != nil {
		t.Fatalf("Init: %v", err)
	}
	return p
}

// newHookContext returns a BifrostContext primed the way the plugin pipeline primes it before
// PostLLMHook: PreLLMHook has run, so startTimeKey and activeRequestTypeKey are set. Without
// startTimeKey, PostLLMHook logs a warning and records nothing.
func newHookContext(reqType schemas.RequestType) *schemas.BifrostContext {
	ctx := schemas.NewBifrostContext(context.Background(), time.Now().Add(time.Minute))
	ctx.SetValue(startTimeKey, time.Now())
	ctx.SetValue(activeRequestTypeKey, reqType)
	return ctx
}

// counterTotal gathers the named counter family from the registry and sums every series'
// value. Summing over labels keeps the assertion independent of the exact label ordering.
func counterTotal(t *testing.T, reg *prometheus.Registry, name string) float64 {
	t.Helper()
	fams, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	for _, mf := range fams {
		if mf.GetName() != name {
			continue
		}
		var sum float64
		for _, m := range mf.GetMetric() {
			sum += m.GetCounter().GetValue()
		}
		return sum
	}
	return 0
}

// waitForCounter polls until the named counter reaches want (PostLLMHook records tokens in a
// background goroutine, so the write is not synchronous with the hook returning).
func waitForCounter(t *testing.T, reg *prometheus.Registry, name string, want float64) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		if got := counterTotal(t, reg, name); got == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("counter %s = %v, want %v (timed out)", name, counterTotal(t, reg, name), want)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// usageCase describes one usage-bearing response type and the input/output tokens the plugin
// must record for it.
type usageCase struct {
	name     string
	reqType  schemas.RequestType
	response *schemas.BifrostResponse
	wantIn   float64
	wantOut  float64
}

// tokenUsageCases enumerates every non-streaming, usage-bearing response type that the logging
// plugin records token usage for in plugins/logging/operations.go (applyNonStreamingOutputToEntry).
// Telemetry MUST record tokens for the same set, or Bifrost logs will report usage that never
// reaches the Prometheus counters (and therefore Grafana) — the Grafana-vs-logs mismatch.
//
// When logging learns a new usage-bearing response type, add it here AND to the switch in
// PostLLMHook; this list is the contract between the two plugins.
func tokenUsageCases() []usageCase {
	return []usageCase{
		{
			name:    "chat",
			reqType: schemas.ChatCompletionRequest,
			response: &schemas.BifrostResponse{ChatResponse: &schemas.BifrostChatResponse{
				Usage: &schemas.BifrostLLMUsage{PromptTokens: 11, CompletionTokens: 7, TotalTokens: 18},
			}},
			wantIn: 11, wantOut: 7,
		},
		{
			name:    "text_completion",
			reqType: schemas.TextCompletionRequest,
			response: &schemas.BifrostResponse{TextCompletionResponse: &schemas.BifrostTextCompletionResponse{
				Usage: &schemas.BifrostLLMUsage{PromptTokens: 5, CompletionTokens: 3, TotalTokens: 8},
			}},
			wantIn: 5, wantOut: 3,
		},
		{
			name:    "responses",
			reqType: schemas.ResponsesRequest,
			response: &schemas.BifrostResponse{ResponsesResponse: &schemas.BifrostResponsesResponse{
				Usage: &schemas.ResponsesResponseUsage{InputTokens: 9, OutputTokens: 4, TotalTokens: 13},
			}},
			wantIn: 9, wantOut: 4,
		},
		{
			name:    "embedding",
			reqType: schemas.EmbeddingRequest,
			response: &schemas.BifrostResponse{EmbeddingResponse: &schemas.BifrostEmbeddingResponse{
				Usage: &schemas.BifrostLLMUsage{PromptTokens: 6, CompletionTokens: 0, TotalTokens: 6},
			}},
			wantIn: 6, wantOut: 0,
		},
		// --- The three below regressed the Grafana-vs-logs parity before the fix. ---
		{
			name:    "compaction",
			reqType: schemas.CompactionRequest,
			response: &schemas.BifrostResponse{CompactionResponse: &schemas.BifrostCompactionResponse{
				Usage: &schemas.ResponsesResponseUsage{InputTokens: 20, OutputTokens: 8, TotalTokens: 28},
			}},
			wantIn: 20, wantOut: 8,
		},
		{
			name:    "image_generation",
			reqType: schemas.ImageGenerationRequest,
			response: &schemas.BifrostResponse{ImageGenerationResponse: &schemas.BifrostImageGenerationResponse{
				Usage: &schemas.ImageUsage{InputTokens: 15, OutputTokens: 2, TotalTokens: 17},
			}},
			wantIn: 15, wantOut: 2,
		},
		{
			name:    "passthrough",
			reqType: schemas.PassthroughRequest,
			response: &schemas.BifrostResponse{PassthroughResponse: &schemas.BifrostPassthroughResponse{
				PassthroughUsage: &schemas.BifrostPassthroughUsage{
					LLMUsage: &schemas.BifrostLLMUsage{PromptTokens: 30, CompletionTokens: 12, TotalTokens: 42},
				},
			}},
			wantIn: 30, wantOut: 12,
		},
	}
}

// TestTokenExtractionParityWithLogging is the regression guard for the customer-reported
// Grafana-vs-Bifrost-logs usage mismatch: it drives PostLLMHook with one response per
// usage-bearing type that logging records, and asserts bifrost_input_tokens_total /
// bifrost_output_tokens_total reflect the exact token counts. A response type that logging
// records but PostLLMHook's switch omits records zero here and fails the test.
func TestTokenExtractionParityWithLogging(t *testing.T) {
	for _, tc := range tokenUsageCases() {
		t.Run(tc.name, func(t *testing.T) {
			p := newTestPlugin(t)
			tc.response.PopulateExtraFields(tc.reqType, schemas.ModelProvider("openai"), "test-model", "test-model")

			ctx := newHookContext(tc.reqType)
			if _, _, err := p.PostLLMHook(ctx, tc.response, nil); err != nil {
				t.Fatalf("PostLLMHook: %v", err)
			}

			waitForCounter(t, p.registry, "bifrost_input_tokens_total", tc.wantIn)
			waitForCounter(t, p.registry, "bifrost_output_tokens_total", tc.wantOut)
		})
	}
}

// TestPostLLMHookRequiresStartTime asserts the documented early-return: without startTimeKey in
// context (PreLLMHook never ran) PostLLMHook records nothing rather than panicking or recording
// with a bogus latency.
func TestPostLLMHookRequiresStartTime(t *testing.T) {
	p := newTestPlugin(t)
	resp := &schemas.BifrostResponse{ChatResponse: &schemas.BifrostChatResponse{
		Usage: &schemas.BifrostLLMUsage{PromptTokens: 5, CompletionTokens: 5, TotalTokens: 10},
	}}
	resp.PopulateExtraFields(schemas.ChatCompletionRequest, "openai", "m", "m")

	ctx := schemas.NewBifrostContext(context.Background(), time.Now().Add(time.Minute))
	// Deliberately omit startTimeKey.
	if _, _, err := p.PostLLMHook(ctx, resp, nil); err != nil {
		t.Fatalf("PostLLMHook: %v", err)
	}
	// Give any (incorrectly-spawned) goroutine a chance to write before asserting zero.
	time.Sleep(50 * time.Millisecond)
	if got := counterTotal(t, p.registry, "bifrost_input_tokens_total"); got != 0 {
		t.Errorf("input tokens = %v, want 0 (no start time -> no recording)", got)
	}
}

// TestMetricsEnabledGating covers the pull-gateway (/metrics scrape) on/off switch: default-on
// when the config omits the field (back-compat), and honoring an explicit value.
// TestRoutingEmbeddingCounters: a response stamped with routing metadata increments
// bifrost_routing_embedding_requests_total regardless of the count_toward_budgets
// flag — the flag only controls budget folding (CalculateCost), never telemetry.
// Cost stays unrecorded here because the test plugin has no pricing manager;
// the routing embedding cost math is covered in modelcatalog's datasheet tests.
func TestRoutingEmbeddingCounters(t *testing.T) {
	for _, countTowardBudgets := range []bool{false, true} {
		p := newTestPlugin(t)

		provider := "openai"
		model := "text-embedding-3-small"
		inputTokens := 42
		resp := &schemas.BifrostResponse{ChatResponse: &schemas.BifrostChatResponse{
			Usage: &schemas.BifrostLLMUsage{PromptTokens: 11, CompletionTokens: 7, TotalTokens: 18},
			ExtraFields: schemas.BifrostResponseExtraFields{
				RequestType: schemas.ChatCompletionRequest,
				RoutingMetadata: &schemas.BifrostRoutingMetadata{
					Calls: []schemas.BifrostRoutingCall{{
						ProviderUsed:       &provider,
						ModelUsed:          &model,
						InputTokens:        &inputTokens,
						CountTowardBudgets: countTowardBudgets,
					}},
				},
			},
		}}
		resp.PopulateExtraFields(schemas.ChatCompletionRequest, schemas.ModelProvider(provider), model, model)

		ctx := newHookContext(schemas.ChatCompletionRequest)
		if _, _, err := p.PostLLMHook(ctx, resp, nil); err != nil {
			t.Fatalf("PostLLMHook (flag=%v): %v", countTowardBudgets, err)
		}

		waitForCounter(t, p.registry, "bifrost_routing_embedding_requests_total", 1)
		if got := counterTotalWithLabel(t, p.registry, "bifrost_routing_embedding_requests_total", "phase", "request"); got != 1 {
			t.Fatalf("request-phase requests counter = %v, want 1", got)
		}
		if got := counterTotal(t, p.registry, "bifrost_routing_embedding_cost_total"); got != 0 {
			t.Fatalf("cost counter without pricing manager = %v, want 0", got)
		}
	}
}

// TestRoutingCountersRecordBothSemanticAndLLMCalls pins the fix for the bug
// where a request that classified via semantic and then fell back to the llm
// classifier lost the embedding's telemetry: both calls in one routing metadata record
// stamp must each increment their own counter family, not just the last one
// written.
func TestRoutingCountersRecordBothSemanticAndLLMCalls(t *testing.T) {
	p := newTestPlugin(t)

	embedProvider, embedModel, embedTokens := "openai", "text-embedding-3-small", 42
	llmProvider, llmModel, llmInput, llmOutput := "anthropic", "claude-haiku-4-5", 30, 5
	resp := &schemas.BifrostResponse{ChatResponse: &schemas.BifrostChatResponse{
		Usage: &schemas.BifrostLLMUsage{PromptTokens: 11, CompletionTokens: 7, TotalTokens: 18},
		ExtraFields: schemas.BifrostResponseExtraFields{
			RequestType: schemas.ChatCompletionRequest,
			RoutingMetadata: &schemas.BifrostRoutingMetadata{
				Calls: []schemas.BifrostRoutingCall{
					{ProviderUsed: &embedProvider, ModelUsed: &embedModel, InputTokens: &embedTokens},
					{ProviderUsed: &llmProvider, ModelUsed: &llmModel, InputTokens: &llmInput, OutputTokens: &llmOutput},
				},
			},
		},
	}}
	resp.PopulateExtraFields(schemas.ChatCompletionRequest, schemas.ModelProvider(embedProvider), embedModel, embedModel)

	ctx := newHookContext(schemas.ChatCompletionRequest)
	if _, _, err := p.PostLLMHook(ctx, resp, nil); err != nil {
		t.Fatalf("PostLLMHook: %v", err)
	}

	waitForCounter(t, p.registry, "bifrost_routing_llm_requests_total", 1)
	if got := counterTotalWithLabel(t, p.registry, "bifrost_routing_embedding_requests_total", "phase", "request"); got != 1 {
		t.Fatalf("embedding requests counter = %v, want 1", got)
	}
	if got := counterTotal(t, p.registry, "bifrost_routing_llm_requests_total"); got != 1 {
		t.Fatalf("llm requests counter = %v, want 1 (must not be shadowed by the embed call)", got)
	}
}

// TestObserveWarmupRoutingEmbedding: warmup embeds report through the direct
// observer method (no request/response exists for them) and land under
// phase="warmup", separate from the request-phase series. Cost stays
// unrecorded without a pricing manager, same as the request phase.
func TestObserveWarmupRoutingEmbedding(t *testing.T) {
	p := newTestPlugin(t)

	p.ObserveWarmupRoutingEmbedding("openai", "text-embedding-3-small", 7)
	p.ObserveWarmupRoutingEmbedding("openai", "text-embedding-3-small", 9)

	if got := counterTotalWithLabel(t, p.registry, "bifrost_routing_embedding_requests_total", "phase", "warmup"); got != 2 {
		t.Fatalf("warmup-phase requests counter = %v, want 2", got)
	}
	if got := counterTotalWithLabel(t, p.registry, "bifrost_routing_embedding_requests_total", "phase", "request"); got != 0 {
		t.Fatalf("request-phase requests counter = %v, want 0 (warmup must not leak into it)", got)
	}
	if got := counterTotal(t, p.registry, "bifrost_routing_embedding_cost_total"); got != 0 {
		t.Fatalf("cost counter without pricing manager = %v, want 0", got)
	}
}

// counterTotalWithLabel sums every series of the named counter family whose
// labels include name=value.
func gaugeTotal(t *testing.T, reg *prometheus.Registry, name string) float64 {
	t.Helper()
	fams, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	for _, mf := range fams {
		if mf.GetName() == name {
			var sum float64
			for _, metric := range mf.GetMetric() {
				sum += metric.GetGauge().GetValue()
			}
			return sum
		}
	}
	return 0
}

func counterTotalWithLabel(t *testing.T, reg *prometheus.Registry, name, labelName, labelValue string) float64 {
	t.Helper()
	fams, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	var sum float64
	for _, mf := range fams {
		if mf.GetName() != name {
			continue
		}
		for _, m := range mf.GetMetric() {
			for _, lp := range m.GetLabel() {
				if lp.GetName() == labelName && lp.GetValue() == labelValue {
					sum += m.GetCounter().GetValue()
					break
				}
			}
		}
	}
	return sum
}

// TestRoutingEmbeddingCountersAbsentWithoutStamp: responses without routing metadata
// (no routing embed ran) must not touch the routing counters.
func TestRoutingEmbeddingCountersAbsentWithoutStamp(t *testing.T) {
	p := newTestPlugin(t)

	resp := &schemas.BifrostResponse{ChatResponse: &schemas.BifrostChatResponse{
		Usage: &schemas.BifrostLLMUsage{PromptTokens: 11, CompletionTokens: 7, TotalTokens: 18},
	}}
	resp.PopulateExtraFields(schemas.ChatCompletionRequest, "openai", "test-model", "test-model")

	ctx := newHookContext(schemas.ChatCompletionRequest)
	if _, _, err := p.PostLLMHook(ctx, resp, nil); err != nil {
		t.Fatalf("PostLLMHook: %v", err)
	}

	// Token counters record after the routing check in the same goroutine, so
	// once they land we know the routing check already ran without recording.
	waitForCounter(t, p.registry, "bifrost_input_tokens_total", 11)
	if got := counterTotal(t, p.registry, "bifrost_routing_embedding_requests_total"); got != 0 {
		t.Fatalf("routing requests counter = %v, want 0", got)
	}
}

func TestMetricsEnabledGating(t *testing.T) {
	cases := []struct {
		name string
		set  *bool
		want bool
	}{
		{"default omitted -> on", nil, true},
		{"explicit true", boolPtr(true), true},
		{"explicit false", boolPtr(false), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p, err := Init(&Config{MetricsEnabled: tc.set}, nil, bifrost.NewDefaultLogger(schemas.LogLevelError))
			if err != nil {
				t.Fatalf("Init: %v", err)
			}
			if got := p.IsMetricsEnabled(); got != tc.want {
				t.Errorf("IsMetricsEnabled() = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestMarshalConfigForStorageKeepsToggles guards the hand-maintained storage whitelist
// (Config.MarshalForStorage's configStorage struct): a toggle added to Config must be
// added there too, or it is silently dropped on save and the UI reverts it. Every *bool
// field on Config is enumerated by reflection so a new toggle cannot escape this test,
// which is how overhead_breakdown_enabled and later user_labels_enabled both regressed.
func TestMarshalConfigForStorageKeepsToggles(t *testing.T) {
	p := newTestPlugin(t)
	ct := reflect.TypeOf(Config{})
	for i := 0; i < ct.NumField(); i++ {
		f := ct.Field(i)
		if f.Type.Kind() != reflect.Ptr || f.Type.Elem().Kind() != reflect.Bool {
			continue
		}
		key := strings.Split(f.Tag.Get("json"), ",")[0]
		if key == "" || key == "-" {
			t.Fatalf("Config.%s is a toggle with no json tag", f.Name)
		}
		// Both values: omitempty would hide a dropped field if only false were sent.
		for _, want := range []bool{true, false} {
			out, err := p.MarshalConfigForStorage(map[string]any{key: want})
			if err != nil {
				t.Fatalf("MarshalConfigForStorage(%s=%v): %v", key, want, err)
			}
			got, ok := out[key].(bool)
			if !ok {
				t.Errorf("%s dropped by storage whitelist (Config.%s): got %v (%T), want %v", key, f.Name, out[key], out[key], want)
				continue
			}
			if got != want {
				t.Errorf("%s = %v after storage round-trip, want %v", key, got, want)
			}
		}
	}
}

// TestCleanupIdempotent guards the shutdown crash: the plugin is registered under several
// types, so Cleanup runs more than once. With the overhead sweeper enabled, a second call
// must not double-close its stop channel. Also covers the disabled path (no sweeper).
func TestCleanupIdempotent(t *testing.T) {
	log := bifrost.NewDefaultLogger(schemas.LogLevelError)
	for _, enabled := range []bool{true, false} {
		p, err := Init(&Config{OverheadBreakdownEnabled: boolPtr(enabled)}, nil, log)
		if err != nil {
			t.Fatalf("Init(enabled=%v): %v", enabled, err)
		}
		if err := p.Cleanup(); err != nil {
			t.Fatalf("Cleanup 1 (enabled=%v): %v", enabled, err)
		}
		// Second call must not panic on a closed channel.
		if err := p.Cleanup(); err != nil {
			t.Fatalf("Cleanup 2 (enabled=%v): %v", enabled, err)
		}
	}
}

// TestGetMetricsGathererCombinesRegistries asserts the /metrics scrape gatherer exposes both
// Bifrost metrics (from p.registry) and the Go/process runtime collectors (from p.systemRegistry).
func TestGetMetricsGathererCombinesRegistries(t *testing.T) {
	p := newTestPlugin(t)
	// Record one Bifrost metric so its family is present in the gather output.
	resp := &schemas.BifrostResponse{ChatResponse: &schemas.BifrostChatResponse{
		Usage: &schemas.BifrostLLMUsage{PromptTokens: 1, CompletionTokens: 1, TotalTokens: 2},
	}}
	resp.PopulateExtraFields(schemas.ChatCompletionRequest, "openai", "m", "m")
	ctx := newHookContext(schemas.ChatCompletionRequest)
	if _, _, err := p.PostLLMHook(ctx, resp, nil); err != nil {
		t.Fatalf("PostLLMHook: %v", err)
	}
	waitForCounter(t, p.registry, "bifrost_input_tokens_total", 1)

	fams, err := p.GetMetricsGatherer().Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	present := map[string]bool{}
	for _, mf := range fams {
		present[mf.GetName()] = true
	}
	if !present["bifrost_input_tokens_total"] {
		t.Error("/metrics gatherer missing Bifrost metric bifrost_input_tokens_total")
	}
	if !present["go_goroutines"] {
		t.Error("/metrics gatherer missing Go runtime collector go_goroutines (systemRegistry not combined)")
	}
}

// TestPushGatewayLifecycle covers the push-gateway config plumbing: defaults are applied, the
// running flag toggles, and re-enabling replaces the previous pusher cleanly.
func TestPushGatewayLifecycle(t *testing.T) {
	p := newTestPlugin(t)
	if p.IsPushGatewayRunning() {
		t.Fatal("push gateway should not be running before EnablePushGateway")
	}

	cfg := &PushGatewayConfig{
		Enabled:        true,
		PushGatewayURL: schemas.NewSecretVar("http://127.0.0.1:0"), // never actually reached in this test
	}
	if err := p.EnablePushGateway(cfg); err != nil {
		t.Fatalf("EnablePushGateway: %v", err)
	}
	defer p.DisablePushGateway()

	if !p.IsPushGatewayRunning() {
		t.Error("push gateway should be running after EnablePushGateway")
	}
	got := p.GetPushGatewayConfig()
	if got.JobName != "bifrost" {
		t.Errorf("default JobName = %q, want bifrost", got.JobName)
	}
	if got.PushInterval != 15 {
		t.Errorf("default PushInterval = %d, want 15", got.PushInterval)
	}
	if got.InstanceID == "" {
		t.Error("default InstanceID should be the hostname, got empty")
	}

	// Re-enable must stop the previous loop and start a new one without leaking / hanging.
	if err := p.EnablePushGateway(cfg); err != nil {
		t.Fatalf("re-EnablePushGateway: %v", err)
	}
	if !p.IsPushGatewayRunning() {
		t.Error("push gateway should still be running after re-enable")
	}

	p.DisablePushGateway()
	if p.IsPushGatewayRunning() {
		t.Error("push gateway should be stopped after DisablePushGateway")
	}
}

// TestPushGatewayPushesBifrostButNotRuntimeCollectors stands up a fake push gateway and asserts
// the initial push carries Bifrost metrics but NOT the Go/process runtime collectors — the
// documented reason those live in a separate registry (they would collide with the gateway's own
// go_/process_ series). This exercises the real push path end-to-end without a live gateway.
func TestPushGatewayPushesBifrostButNotRuntimeCollectors(t *testing.T) {
	p := newTestPlugin(t)

	// Record a Bifrost metric so the push has a non-trivial payload.
	resp := &schemas.BifrostResponse{ChatResponse: &schemas.BifrostChatResponse{
		Usage: &schemas.BifrostLLMUsage{PromptTokens: 3, CompletionTokens: 2, TotalTokens: 5},
	}}
	resp.PopulateExtraFields(schemas.ChatCompletionRequest, "openai", "m", "m")
	if _, _, err := p.PostLLMHook(newHookContext(schemas.ChatCompletionRequest), resp, nil); err != nil {
		t.Fatalf("PostLLMHook: %v", err)
	}
	waitForCounter(t, p.registry, "bifrost_input_tokens_total", 3)

	bodies := make(chan []byte, 4)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		select {
		case bodies <- body:
		default:
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	cfg := &PushGatewayConfig{
		Enabled:        true,
		PushGatewayURL: schemas.NewSecretVar(srv.URL),
		PushInterval:   3600, // long, so only the immediate initial push fires during the test
	}
	if err := p.EnablePushGateway(cfg); err != nil {
		t.Fatalf("EnablePushGateway: %v", err)
	}
	defer p.DisablePushGateway()

	select {
	case body := <-bodies:
		if !bytes.Contains(body, []byte("bifrost_input_tokens_total")) {
			t.Error("pushed payload missing Bifrost metric bifrost_input_tokens_total")
		}
		if bytes.Contains(body, []byte("go_goroutines")) || bytes.Contains(body, []byte("process_cpu_seconds_total")) {
			t.Error("pushed payload unexpectedly contains Go/process runtime collectors (should be push-excluded)")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("fake push gateway received no push within 5s")
	}
}

func boolPtr(b bool) *bool { return &b }

// TestApplyCustomLabels covers applyCustomLabels' resolution behavior: values
// sourced from x-bf-dim-* dimensions, values from a direct typed context key,
// and dimension precedence when both are present. Header-level exclusion of the
// removed x-bf-prom-* prefix is enforced upstream in the HTTP transport, not here.
func TestApplyCustomLabels(t *testing.T) {
	tests := []struct {
		name         string
		customLabels []string
		dimensions   map[string]string
		typedKeys    map[string]string // set via ctx.SetValue(BifrostContextKey(k), v)
		want         map[string]string
	}{
		{
			name:         "resolves from dimensions",
			customLabels: []string{"environment"},
			dimensions:   map[string]string{"environment": "production"},
			want:         map[string]string{"environment": "production"},
		},
		{
			name:         "resolves from direct typed context key",
			customLabels: []string{"tenant"},
			typedKeys:    map[string]string{"tenant": "acme"},
			want:         map[string]string{"tenant": "acme"},
		},
		{
			name:         "dimension takes precedence over typed key",
			customLabels: []string{"region"},
			dimensions:   map[string]string{"region": "us-east-1"},
			typedKeys:    map[string]string{"region": "eu-west-1"},
			want:         map[string]string{"region": "us-east-1"},
		},
		{
			name:         "label absent from all sources is not emitted",
			customLabels: []string{"missing"},
			want:         map[string]string{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := schemas.NewBifrostContext(context.Background(), time.Now().Add(time.Minute))
			if tt.dimensions != nil {
				ctx.SetValue(schemas.BifrostContextKeyDimensions, tt.dimensions)
			}
			for k, v := range tt.typedKeys {
				ctx.SetValue(schemas.BifrostContextKey(k), v)
			}

			p := &PrometheusPlugin{customLabels: tt.customLabels}
			got := map[string]string{}
			p.applyCustomLabels(ctx, got)

			if len(got) != len(tt.want) {
				t.Fatalf("label count = %d, want %d (got %v)", len(got), len(tt.want), got)
			}
			for k, v := range tt.want {
				if got[k] != v {
					t.Errorf("label %q = %q, want %q", k, got[k], v)
				}
			}
		})
	}
}

// TestSpliceLabelValues pins the final label-value ordering produced for the latency,
// error, and cache-hit metrics: default labels first, then the metric's extra values
// (is_success / status_code+error_type / cache_type), then the custom labels. Metric
// vectors match values to label names purely by position, so any reordering here
// silently attributes values to the wrong labels.
func TestSpliceLabelValues(t *testing.T) {
	defaults := []string{"openai", "gpt-4o", "chat"} // provider, model, method
	custom := []string{"team-a", "env-prod"}         // custom dimension labels
	base := append(append([]string{}, defaults...), custom...)

	tests := []struct {
		name   string
		in     []string
		extras []string
		want   []string
	}{
		{
			name:   "latency is_success between defaults and custom labels",
			in:     base,
			extras: []string{"true"},
			want:   []string{"openai", "gpt-4o", "chat", "true", "team-a", "env-prod"},
		},
		{
			name:   "error status_code and error_type keep their order",
			in:     base,
			extras: []string{"429", "rate_limit"},
			want:   []string{"openai", "gpt-4o", "chat", "429", "rate_limit", "team-a", "env-prod"},
		},
		{
			name:   "cache_type between defaults and custom labels",
			in:     base,
			extras: []string{"semantic"},
			want:   []string{"openai", "gpt-4o", "chat", "semantic", "team-a", "env-prod"},
		},
		{
			name:   "no custom labels appends extras at the end",
			in:     defaults,
			extras: []string{"true"},
			want:   []string{"openai", "gpt-4o", "chat", "true"},
		},
		{
			name:   "no extras returns the input unchanged",
			in:     base,
			extras: nil,
			want:   []string{"openai", "gpt-4o", "chat", "team-a", "env-prod"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			before := slices.Clone(tt.in)
			got := spliceLabelValues(tt.in, len(defaults), tt.extras...)
			if !slices.Equal(got, tt.want) {
				t.Errorf("spliceLabelValues() = %v, want %v", got, tt.want)
			}
			if !slices.Equal(tt.in, before) {
				t.Errorf("input mutated: %v, was %v", tt.in, before)
			}
		})
	}
}

// TestInferenceTelemetrySimpleSuccess: simple non-streaming success increments requests once.
func TestInferenceTelemetrySimpleSuccess(t *testing.T) {
	p := newTestPlugin(t)
	resp := &schemas.BifrostResponse{ChatResponse: &schemas.BifrostChatResponse{
		Usage: &schemas.BifrostLLMUsage{PromptTokens: 10, CompletionTokens: 5, TotalTokens: 15},
	}}
	resp.PopulateExtraFields(schemas.ChatCompletionRequest, "qwen-kv-router", "ai-server1-local", "ai-server1-local")

	ctx := newHookContext(schemas.ChatCompletionRequest)
	p.PreLLMHook(ctx, &schemas.BifrostRequest{RequestType: schemas.ChatCompletionRequest, ChatRequest: &schemas.BifrostChatRequest{Provider: "qwen-kv-router", Model: "ai-server1-local"}})
	if _, _, err := p.PostLLMHook(ctx, resp, nil); err != nil {
		t.Fatalf("PostLLMHook: %v", err)
	}

	waitForCounter(t, p.registry, "ai_inference_requests_total", 1)
	waitForCounter(t, p.registry, "ai_inference_executions_total", 1)
	if got := counterTotal(t, p.registry, "ai_inference_dispatches_total"); got != 1 {
		t.Errorf("dispatches = %v, want 1", got)
	}
	if got := gaugeTotal(t, p.registry, "ai_inference_inflight"); got != 0 {
		t.Errorf("inflight = %v, want 0", got)
	}
	if got := counterTotalWithLabel(t, p.registry, "ai_inference_requests_total", "outcome", "success"); got != 1 {
		t.Errorf("success outcome = %v, want 1", got)
	}
}

// TestInferenceTelemetryStreamOpenNotSuccess: stream open (not final) does not count success.
func TestInferenceTelemetryStreamOpenNotSuccess(t *testing.T) {
	p := newTestPlugin(t)
	resp := &schemas.BifrostResponse{ChatResponse: &schemas.BifrostChatResponse{
		Usage: &schemas.BifrostLLMUsage{PromptTokens: 10, CompletionTokens: 1, TotalTokens: 11},
	}}
	resp.PopulateExtraFields(schemas.ChatCompletionRequest, "qwen-kv-router", "ai-server1-local", "ai-server1-local")

	ctx := newHookContext(schemas.ChatCompletionStreamRequest)
	resp.PopulateExtraFields(schemas.ChatCompletionStreamRequest, "qwen-kv-router", "ai-server1-local", "ai-server1-local")
	ctx.SetValue(schemas.BifrostContextKeyStreamEndIndicator, false)

	if _, _, err := p.PostLLMHook(ctx, resp, nil); err != nil {
		t.Fatalf("PostLLMHook: %v", err)
	}

	time.Sleep(100 * time.Millisecond)
	if got := counterTotal(t, p.registry, "ai_inference_requests_total"); got != 0 {
		t.Errorf("stream open requests = %v, want 0 (intermediate chunks don't count)", got)
	}
}

// TestInferenceTelemetryStreamTerminalCountsOnce: stream terminal counts once, not per chunk.
func TestInferenceTelemetryStreamTerminalCountsOnce(t *testing.T) {
	p := newTestPlugin(t)
	provider := "qwen-kv-router"
	model := "ai-server1-local"
	ctx := newHookContext(schemas.ChatCompletionStreamRequest)

	for _, isFinal := range []bool{false, true} {
		resp := &schemas.BifrostResponse{ChatResponse: &schemas.BifrostChatResponse{
			Usage: &schemas.BifrostLLMUsage{PromptTokens: 10, CompletionTokens: 5, TotalTokens: 15},
		}}
		resp.PopulateExtraFields(schemas.ChatCompletionStreamRequest, schemas.ModelProvider(provider), model, model)
		ctx.SetValue(schemas.BifrostContextKeyStreamEndIndicator, isFinal)

		if _, _, err := p.PostLLMHook(ctx, resp, nil); err != nil {
			t.Fatalf("PostLLMHook (isFinal=%v): %v", isFinal, err)
		}
	}

	waitForCounter(t, p.registry, "ai_inference_requests_total", 1)
	if got := counterTotalWithLabel(t, p.registry, "ai_inference_requests_total", "outcome", "success"); got != 1 {
		t.Errorf("success outcome after 2 chunks (1 final) = %v, want 1", got)
	}
}

// TestInferenceTelemetryDisconnectReleasesInflightCancelled: disconnect releases inflight and counts cancelled.
func TestInferenceTelemetryDisconnectReleasesInflightCancelled(t *testing.T) {
	p := newTestPlugin(t)
	// Disconnect/client context cancelled = no response, error with cancelled marker
	bifrostErr := &schemas.BifrostError{
		Error:       &schemas.ErrorField{Message: "context cancelled"},
		StatusCode:  nil,
		ExtraFields: schemas.BifrostErrorExtraFields{},
	}
	// Core populates provider/model on the ctx-done error (newBifrostCtxDoneError + PopulateExtraFields).
	bifrostErr.PopulateExtraFields(schemas.ChatCompletionRequest, "qwen-kv-router", "ai-server1-local", "ai-server1-local")

	ctx := newHookContext(schemas.ChatCompletionRequest)
	p.PreLLMHook(ctx, &schemas.BifrostRequest{RequestType: schemas.ChatCompletionRequest, ChatRequest: &schemas.BifrostChatRequest{Provider: "qwen-kv-router", Model: "ai-server1-local"}})
	if got := gaugeTotal(t, p.registry, "ai_inference_inflight"); got != 1 {
		t.Fatalf("inflight after dispatch = %v, want 1", got)
	}
	ctx.Cancel()
	if _, _, err := p.PostLLMHook(ctx, nil, bifrostErr); err != nil {
		t.Fatalf("PostLLMHook: %v", err)
	}

	waitForCounter(t, p.registry, "ai_inference_requests_total", 1)
	if got := counterTotalWithLabel(t, p.registry, "ai_inference_requests_total", "outcome", "cancelled"); got != 1 {
		t.Errorf("cancelled outcome (disconnect) = %v, want 1", got)
	}
	waitForCounter(t, p.registry, "ai_inference_executions_total", 1)
	if got := gaugeTotal(t, p.registry, "ai_inference_inflight"); got != 0 {
		t.Errorf("inflight after disconnect = %v, want 0", got)
	}
}

// TestInferenceTelemetryErrorAfterFirstChunk: error after first chunk counts error once.
func TestInferenceTelemetryErrorAfterFirstChunk(t *testing.T) {
	p := newTestPlugin(t)

	resp := &schemas.BifrostResponse{ChatResponse: &schemas.BifrostChatResponse{
		Usage: &schemas.BifrostLLMUsage{PromptTokens: 10, CompletionTokens: 1, TotalTokens: 11},
	}}
	resp.PopulateExtraFields(schemas.ChatCompletionRequest, "qwen-kv-router", "ai-server1-local", "ai-server1-local")

	ctx := newHookContext(schemas.ChatCompletionRequest)
	ctx.SetValue(schemas.BifrostContextKeyStreamEndIndicator, true) // Final chunk with error

	bifrostErr := &schemas.BifrostError{
		Error:      &schemas.ErrorField{Message: "stream error"},
		StatusCode: ptrInt(500),
	}

	if _, _, err := p.PostLLMHook(ctx, resp, bifrostErr); err != nil {
		t.Fatalf("PostLLMHook: %v", err)
	}

	waitForCounter(t, p.registry, "ai_inference_requests_total", 1)
	if got := counterTotalWithLabel(t, p.registry, "ai_inference_requests_total", "outcome", "error"); got != 1 {
		t.Errorf("error outcome (after first chunk) = %v, want 1", got)
	}
}

// TestInferenceTelemetryDuplicateFinalizationCountsOnce: duplicate finalization guard ensures idempotence.
func TestInferenceTelemetryDuplicateFinalizationCountsOnce(t *testing.T) {
	p := newTestPlugin(t)
	resp := &schemas.BifrostResponse{ChatResponse: &schemas.BifrostChatResponse{
		Usage: &schemas.BifrostLLMUsage{PromptTokens: 10, CompletionTokens: 5, TotalTokens: 15},
	}}
	resp.PopulateExtraFields(schemas.ChatCompletionRequest, "qwen-kv-router", "ai-server1-local", "ai-server1-local")

	ctx := newHookContext(schemas.ChatCompletionRequest)
	ctx.SetValue(schemas.BifrostContextKeyStreamEndIndicator, true)

	// Call PostLLMHook twice with the same context (simulating duplicate terminal call)
	for i := 0; i < 2; i++ {
		if _, _, err := p.PostLLMHook(ctx, resp, nil); err != nil {
			t.Fatalf("PostLLMHook (attempt %d): %v", i+1, err)
		}
	}

	waitForCounter(t, p.registry, "ai_inference_requests_total", 1)
	if got := counterTotal(t, p.registry, "ai_inference_requests_total"); got != 1 {
		t.Errorf("after duplicate finalization = %v, want 1 (once-guard should prevent double-count)", got)
	}
}

// TestInferenceTelemetryPreDispatchFailureNotExecution: pre-dispatch rejection (no provider) is not an execution.
func TestInferenceTelemetryPreDispatchFailureNotExecution(t *testing.T) {
	p := newTestPlugin(t)

	ctx := newHookContext(schemas.ChatCompletionRequest)
	// Omit provider entirely (pre-dispatch rejection)
	if _, _, err := p.PostLLMHook(ctx, nil, nil); err != nil {
		t.Fatalf("PostLLMHook: %v", err)
	}

	time.Sleep(50 * time.Millisecond)
	if got := counterTotal(t, p.registry, "ai_inference_executions_total"); got != 0 {
		t.Errorf("pre-dispatch executions = %v, want 0", got)
	}
	if got := counterTotal(t, p.registry, "ai_inference_requests_total"); got != 0 {
		t.Errorf("pre-dispatch requests = %v, want 0", got)
	}
}

// TestInferenceTelemetryNonInferenceRequestsNotCounted: non-inference request types are excluded.
func TestInferenceTelemetryNonInferenceRequestsNotCounted(t *testing.T) {
	for _, reqType := range []schemas.RequestType{schemas.CountTokensRequest, schemas.ListModelsRequest} {
		t.Run(string(reqType), func(t *testing.T) {
			p := newTestPlugin(t)
			resp := &schemas.BifrostResponse{ChatResponse: &schemas.BifrostChatResponse{
				Usage: &schemas.BifrostLLMUsage{PromptTokens: 5, CompletionTokens: 0, TotalTokens: 5},
			}}
			resp.PopulateExtraFields(reqType, "qwen-kv-router", "ai-server1-local", "ai-server1-local")

			ctx := newHookContext(reqType)
			if _, _, err := p.PostLLMHook(ctx, resp, nil); err != nil {
				t.Fatalf("PostLLMHook: %v", err)
			}

			time.Sleep(50 * time.Millisecond)
			if got := counterTotal(t, p.registry, "ai_inference_requests_total"); got != 0 {
				t.Errorf("%s inference requests = %v, want 0", reqType, got)
			}
		})
	}
}

// TestInferenceTelemetryMissingUsageUnknownCounter: missing usage goes to unknown counter, not 0 on main counter.
func TestInferenceTelemetryMissingUsageUnknownCounter(t *testing.T) {
	p := newTestPlugin(t)
	// Response with no usage
	resp := &schemas.BifrostResponse{ChatResponse: &schemas.BifrostChatResponse{
		Usage: nil, // No usage
	}}
	resp.PopulateExtraFields(schemas.ChatCompletionRequest, "qwen-kv-router", "ai-server1-local", "ai-server1-local")

	ctx := newHookContext(schemas.ChatCompletionRequest)
	if _, _, err := p.PostLLMHook(ctx, resp, nil); err != nil {
		t.Fatalf("PostLLMHook: %v", err)
	}

	waitForCounter(t, p.registry, "ai_inference_output_tokens_unknown_total", 1)
	if got := counterTotal(t, p.registry, "ai_inference_output_tokens_total"); got != 0 {
		t.Errorf("output tokens with missing usage = %v, want 0 (not counted as 0, tracked separately)", got)
	}
}

// TestInferenceTelemetryLabelBounding: arbitrary model strings are bounded or normalized.
func TestInferenceTelemetryLabelBounding(t *testing.T) {
	p := newTestPlugin(t)
	resp := &schemas.BifrostResponse{ChatResponse: &schemas.BifrostChatResponse{
		Usage: &schemas.BifrostLLMUsage{PromptTokens: 10, CompletionTokens: 5, TotalTokens: 15},
	}}
	// Use a model that gets normalized by PostLLMHook's schemas.NormalizeModelName
	resp.PopulateExtraFields(schemas.ChatCompletionRequest, "qwen-kv-router", "ai-server1-local", "ai-server1-local")

	ctx := newHookContext(schemas.ChatCompletionRequest)
	if _, _, err := p.PostLLMHook(ctx, resp, nil); err != nil {
		t.Fatalf("PostLLMHook: %v", err)
	}

	waitForCounter(t, p.registry, "ai_inference_requests_total", 1)
	// Verify the model label is not "unknown" (bounded, normalized)
	if got := counterTotalWithLabel(t, p.registry, "ai_inference_executions_total", "model", "unknown"); got > 0 {
		t.Errorf("unknown model labels present = %v, want 0 (model should be normalized)", got)
	}
}

func ptrInt(v int) *int { return &v }

func TestInferenceTelemetryFallbackFinalization(t *testing.T) {
	p := newTestPlugin(t)
	request := &schemas.BifrostRequest{
		RequestType: schemas.ChatCompletionRequest,
		ChatRequest: &schemas.BifrostChatRequest{
			Provider:  "qwen-kv-router",
			Model:     "ai-server1-local",
			Fallbacks: []schemas.Fallback{{Provider: "teamclaude-canary", Model: "claude-haiku-4-5"}},
		},
	}
	ctx := newHookContext(schemas.ChatCompletionRequest)

	p.PreLLMHook(ctx, request)
	primary := &schemas.BifrostResponse{ChatResponse: &schemas.BifrostChatResponse{}}
	primary.PopulateExtraFields(schemas.ChatCompletionRequest, "qwen-kv-router", "ai-server1-local", "ai-server1-local")
	primaryErr := &schemas.BifrostError{Error: &schemas.ErrorField{Message: "primary failed"}, StatusCode: ptrInt(500)}
	if _, _, err := p.PostLLMHook(ctx, primary, primaryErr); err != nil {
		t.Fatalf("primary PostLLMHook: %v", err)
	}

	ctx.SetValue(schemas.BifrostContextKeyFallbackIndex, 1)
	p.PreLLMHook(ctx, request)
	fallback := &schemas.BifrostResponse{ChatResponse: &schemas.BifrostChatResponse{}}
	fallback.PopulateExtraFields(schemas.ChatCompletionRequest, "teamclaude-canary", "claude-haiku-4-5", "claude-haiku-4-5")
	if _, _, err := p.PostLLMHook(ctx, fallback, nil); err != nil {
		t.Fatalf("fallback PostLLMHook: %v", err)
	}

	waitForCounter(t, p.registry, "ai_inference_requests_total", 1)
	if got := counterTotalWithLabel(t, p.registry, "ai_inference_requests_total", "outcome", "success"); got != 1 {
		t.Errorf("fallback success outcome = %v, want 1", got)
	}
	if got := counterTotalWithLabel(t, p.registry, "ai_inference_requests_total", "outcome", "error"); got != 0 {
		t.Errorf("primary error outcome = %v, want 0", got)
	}
}

func TestInferenceTelemetryFallbackExhaustedFinalization(t *testing.T) {
	p := newTestPlugin(t)
	request := &schemas.BifrostRequest{
		RequestType: schemas.ChatCompletionRequest,
		ChatRequest: &schemas.BifrostChatRequest{
			Provider:  "qwen-kv-router",
			Model:     "ai-server1-local",
			Fallbacks: []schemas.Fallback{{Provider: "teamclaude-canary", Model: "claude-haiku-4-5"}},
		},
	}
	ctx := newHookContext(schemas.ChatCompletionRequest)
	ctx.SetValue(schemas.BifrostContextKeyFallbackIndex, 1)
	p.PreLLMHook(ctx, request)
	attempt := &schemas.BifrostResponse{ChatResponse: &schemas.BifrostChatResponse{}}
	attempt.PopulateExtraFields(schemas.ChatCompletionRequest, "teamclaude-canary", "claude-haiku-4-5", "claude-haiku-4-5")
	attemptErr := &schemas.BifrostError{Error: &schemas.ErrorField{Message: "fallback failed"}, StatusCode: ptrInt(500)}
	if _, _, err := p.PostLLMHook(ctx, attempt, attemptErr); err != nil {
		t.Fatalf("fallback PostLLMHook: %v", err)
	}

	waitForCounter(t, p.registry, "ai_inference_requests_total", 1)
	if got := counterTotalWithLabel(t, p.registry, "ai_inference_requests_total", "outcome", "error"); got != 1 {
		t.Errorf("exhausted fallback error outcome = %v, want 1", got)
	}
}
