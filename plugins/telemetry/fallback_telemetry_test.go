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
	observer.OnFallbackTransition("openai", "claude-opus-5-5", "anthropic", "claude-sonnet-5-5", "capacity")
	observer.OnFallbackTransition("anthropic", "claude-sonnet-5-5", "local", "ai-server1-local", "rate_limit")
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
	observer.OnFallbackTransition("openai", "claude-opus-5-5", "anthropic", "claude-sonnet-5-5", "capacity")
	fallbackTerminal(t, observer, "success", 1)
	fallbackTerminal(t, observer, "success", 1)
	if got := testutil.ToFloat64(plugin.FallbackResultsTotal.WithLabelValues("chat_completion", "success")); got != 1 {
		t.Fatalf("results = %v, want 1", got)
	}
}

func TestFallbackTelemetryStreamErrorAfterChunks(t *testing.T) {
	plugin, observer := newFallbackObserverForTest(t, 1)
	observer.OnFallbackTransition("openai", "claude-opus-5-5", "anthropic", "claude-sonnet-5-5", "provider_error")
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
	observer.OnFallbackTransition("openai", "claude-opus-5-5", "anthropic", "claude-sonnet-5-5", "provider_error")
	fallbackTerminal(t, observer, "error", 1)
	if got := testutil.ToFloat64(plugin.FallbackResultsTotal.WithLabelValues("chat_completion", "error")); got != 0 {
		t.Fatalf("early-halt results = %v, want 0", got)
	}
}

func TestFallbackTelemetryRetryAndBoundedLabels(t *testing.T) {
	plugin, observer := newFallbackObserverForTest(t, 1)
	observer.OnRetryAttempt("openai", "arbitrary-model", "timeout")
	observer.OnFallbackTransition("openai", "arbitrary-model", "anthropic", "arbitrary-model", "free text")
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
