package telemetry

import (
	"context"
	"testing"
	"time"

	schemas "github.com/maximhq/bifrost/core/schemas"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

func newFallbackObserverForTest(t *testing.T, totalFallbacks int) (*PrometheusPlugin, schemas.FallbackObserver) {
	t.Helper()
	plugin := newTestPlugin(t)
	ctx := schemas.NewBifrostContext(context.Background(), time.Now().Add(time.Minute))
	req := &schemas.BifrostRequest{RequestType: schemas.ChatCompletionRequest, ChatRequest: &schemas.BifrostChatRequest{Model: "claude-opus-5-5", Fallbacks: make([]schemas.Fallback, totalFallbacks)}}
	if _, _, err := plugin.PreLLMHook(ctx, req); err != nil {
		t.Fatalf("PreLLMHook: %v", err)
	}
	observer, ok := ctx.Value(schemas.BifrostContextKeyFallbackObserver).(schemas.FallbackObserver)
	if !ok || observer == nil {
		t.Fatal("PreLLMHook did not install a fallback observer")
	}
	return plugin, observer
}

func fallbackTerminal(t *testing.T, observer schemas.FallbackObserver, outcome string, index int) {
	t.Helper()
	observer.(*fallbackObserver).OnRequestTerminal(outcome, index)
}

func TestFallbackTelemetryABC(t *testing.T) {
	plugin, observer := newFallbackObserverForTest(t, 2)
	observer.OnFallbackTransition("openai", "claude-opus-5-5", "anthropic", "claude-sonnet-5-5", "capacity", "")
	observer.OnFallbackTransition("anthropic", "claude-sonnet-5-5", "local", "ai-server1-local", "rate_limit", "")
	fallbackTerminal(t, observer, "error", 2)

	if got := testutil.ToFloat64(plugin.FallbackRequestsTotal.WithLabelValues("chat_completion")); got != 1 {
		t.Fatalf("requests = %v, want 1", got)
	}
	if got := testutil.ToFloat64(plugin.FallbackTransitionsTotal.WithLabelValues("openai", "claude-opus-5-5", "anthropic", "claude-sonnet-5-5", "capacity", "none")); got != 1 {
		t.Fatalf("A->B = %v, want 1", got)
	}
	if got := testutil.ToFloat64(plugin.FallbackTransitionsTotal.WithLabelValues("anthropic", "claude-sonnet-5-5", "local", "ai-server1-local", "rate_limit", "none")); got != 1 {
		t.Fatalf("B->C = %v, want 1", got)
	}
	if got := testutil.ToFloat64(plugin.FallbackResultsTotal.WithLabelValues("chat_completion", "error")); got != 1 {
		t.Fatalf("results = %v, want 1", got)
	}
}

func TestFallbackTelemetrySuccessBeforeLast(t *testing.T) {
	plugin, observer := newFallbackObserverForTest(t, 2)
	observer.OnFallbackTransition("openai", "claude-opus-5-5", "anthropic", "claude-sonnet-5-5", "capacity", "")
	fallbackTerminal(t, observer, "success", 1)
	fallbackTerminal(t, observer, "success", 1)
	if got := testutil.ToFloat64(plugin.FallbackResultsTotal.WithLabelValues("chat_completion", "success")); got != 1 {
		t.Fatalf("results = %v, want 1", got)
	}
}

func TestFallbackTelemetryStreamErrorAfterChunks(t *testing.T) {
	plugin, observer := newFallbackObserverForTest(t, 1)
	observer.OnFallbackTransition("openai", "claude-opus-5-5", "anthropic", "claude-sonnet-5-5", "provider_error", "")
	fallbackTerminal(t, observer, "error", 1)
	if got := testutil.ToFloat64(plugin.FallbackResultsTotal.WithLabelValues("chat_completion", "error")); got != 1 {
		t.Fatalf("error results = %v, want 1", got)
	}
	if got := testutil.ToFloat64(plugin.FallbackResultsTotal.WithLabelValues("chat_completion", "success")); got != 0 {
		t.Fatalf("false success results = %v, want 0", got)
	}
}

func TestFallbackTelemetryEarlyHaltAcceptedUndercount(t *testing.T) {
	plugin, observer := newFallbackObserverForTest(t, 2)
	observer.OnFallbackTransition("openai", "claude-opus-5-5", "anthropic", "claude-sonnet-5-5", "provider_error", "")
	fallbackTerminal(t, observer, "error", 1)
	if got := testutil.ToFloat64(plugin.FallbackResultsTotal.WithLabelValues("chat_completion", "error")); got != 0 {
		t.Fatalf("early-halt results = %v, want 0", got)
	}
}

func TestFallbackTelemetryRetryAndBoundedLabels(t *testing.T) {
	plugin, observer := newFallbackObserverForTest(t, 1)
	observer.OnRetryAttempt("openai", "arbitrary-model", "timeout")
	observer.OnFallbackTransition("openai", "arbitrary-model", "anthropic", "arbitrary-model", "free text", "")
	if got := testutil.ToFloat64(plugin.RetryAttemptsTotal.WithLabelValues("openai", "unknown", "timeout")); got != 1 {
		t.Fatalf("retry = %v, want 1", got)
	}
	if got := testutil.ToFloat64(plugin.FallbackTransitionsTotal.WithLabelValues("openai", "unknown", "anthropic", "unknown", "unknown", "none")); got != 1 {
		t.Fatalf("bounded transition = %v, want 1", got)
	}
}

func TestFallbackTelemetryRetryOnlyDoesNotCountFallback(t *testing.T) {
	plugin, observer := newFallbackObserverForTest(t, 1)
	observer.OnRetryAttempt("openai", "claude-opus-5-5", "timeout")
	observer.OnRetryAttempt("openai", "claude-opus-5-5", "timeout")
	fallbackTerminal(t, observer, "error", 1)
	if got := testutil.ToFloat64(plugin.RetryAttemptsTotal.WithLabelValues("openai", "claude-opus-5-5", "timeout")); got != 2 {
		t.Fatalf("retry = %v, want 2", got)
	}
	if got := testutil.ToFloat64(plugin.FallbackRequestsTotal.WithLabelValues("chat_completion")); got != 0 {
		t.Fatalf("requests = %v, want 0", got)
	}
	if got := testutil.ToFloat64(plugin.FallbackResultsTotal.WithLabelValues("chat_completion", "error")); got != 0 {
		t.Fatalf("results = %v, want 0", got)
	}
	if got := testutil.CollectAndCount(plugin.FallbackTransitionsTotal); got != 0 {
		t.Fatalf("transitions = %v, want 0", got)
	}
}

func TestFallbackTelemetryCountTokensIsNotInference(t *testing.T) {
	if isInferenceRequest(schemas.CountTokensRequest) {
		t.Fatal("CountTokensRequest must not be treated as inference")
	}
	if isInferenceRequest(schemas.ListModelsRequest) {
		t.Fatal("ListModelsRequest must not be treated as inference")
	}
	if !isInferenceRequest(schemas.ChatCompletionRequest) {
		t.Fatal("ChatCompletionRequest must be treated as inference")
	}
}

func TestNormalizeFallbackModelPreservesAllowedChatGPTLabels(t *testing.T) {
	for _, model := range []string{
		"bifrost-chatgpt-gpt-6-astra",
		"bifrost-chatgpt-gpt-5.6-luna",
		"bifrost-chatgpt-gpt-5.6-terra",
	} {
		t.Run(model, func(t *testing.T) {
			if got := normalizeFallbackModel(model); got != model {
				t.Errorf("normalizeFallbackModel(%q) = %q, want exact model label", model, got)
			}
		})
	}
}

func TestNormalizeFallbackModelBoundsUnsupportedLabels(t *testing.T) {
	if got := normalizeFallbackModel("unsupported-chatgpt-model"); got != "unknown" {
		t.Errorf("normalizeFallbackModel(unsupported) = %q, want unknown", got)
	}
}

func TestLocalFallbackReasonLabels(t *testing.T) {
	cases := []struct {
		name, from, localReason, want string
	}{
		{"backend_busy", "qwen-kv-router", "backend_busy", "backend_busy"},
		{"prefill_down", "qwen-kv-router", "prefill_down", "prefill_down"},
		{"template_mismatch", "qwen-kv-router", "template_mismatch", "template_mismatch"},
		{"transport", "qwen-kv-router", "transport", "transport"},
		{"cloud_with_body_reason", "teamclaude-canary", "backend_busy", "none"},
		{"cloud_with_transport", "chatgpt-xdpnet", "transport", "none"},
		{"local_empty", "qwen-kv-router", "", "unknown"},
		{"local_arbitrary", "qwen-kv-router", "free text from body", "unknown"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			plugin, observer := newFallbackObserverForTest(t, 1)
			observer.OnFallbackTransition(tc.from, "claude-opus-5-5", "anthropic", "claude-sonnet-5-5", "capacity", tc.localReason)
			if got := testutil.ToFloat64(plugin.FallbackTransitionsTotal.WithLabelValues(tc.from, "claude-opus-5-5", "anthropic", "claude-sonnet-5-5", "capacity", tc.want)); got != 1 {
				t.Fatalf("local_reason=%q series = %v, want 1", tc.want, got)
			}
			if got := testutil.CollectAndCount(plugin.FallbackTransitionsTotal); got != 1 {
				t.Fatalf("series = %v, want 1", got)
			}
		})
	}
}

func TestLocalFallbackReasonBoundedSeries(t *testing.T) {
	plugin, observer := newFallbackObserverForTest(t, 1)
	for i := 0; i < 20; i++ {
		observer.OnFallbackTransition("qwen-kv-router", "claude-opus-5-5", "anthropic", "claude-sonnet-5-5", "capacity", "arbitrary-"+string(rune('a'+i)))
		observer.OnFallbackTransition("teamclaude-canary", "claude-opus-5-5", "anthropic", "claude-sonnet-5-5", "capacity", "backend_busy")
	}
	if got := testutil.CollectAndCount(plugin.FallbackTransitionsTotal); got != 2 {
		t.Fatalf("series = %v, want 2 (unknown + none)", got)
	}
}
