package telemetry

import (
	"sync"
	"testing"

	schemas "github.com/maximhq/bifrost/core/schemas"
)

// Core advances BifrostContextKeyFallbackIndex and runs PreLLMHook for attempt i+1 while the
// async PostLLMHook goroutine of attempt i is still releasing its dispatched-attempt marker.
// Both touch the per-request tracker stored in the context. This test replays that sequence
// on one context; under -race the unguarded map[int]bool shows up as a data race (and
// without -race as "fatal error: concurrent map writes").
func TestInferenceDispatchedAttemptsConcurrentHooks(t *testing.T) {
	const attempts = 400
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

	for i := 0; i < attempts; i++ {
		ctx.SetValue(schemas.BifrostContextKeyFallbackIndex, i)
		p.PreLLMHook(ctx, request)
		resp := &schemas.BifrostResponse{ChatResponse: &schemas.BifrostChatResponse{}}
		resp.PopulateExtraFields(schemas.ChatCompletionRequest, "qwen-kv-router", "ai-server1-local", "ai-server1-local")
		attemptErr := &schemas.BifrostError{Error: &schemas.ErrorField{Message: "503"}, StatusCode: ptrInt(503)}
		if _, _, err := p.PostLLMHook(ctx, resp, attemptErr); err != nil {
			t.Fatalf("PostLLMHook attempt %d: %v", i, err)
		}
	}

	waitForCounter(t, p.registry, "ai_inference_executions_total", attempts)
	// Every dispatched attempt must have released its executing slot exactly once.
	if got := gaugeTotal(t, p.registry, "ai_inference_inflight"); got != 0 {
		t.Fatalf("ai_inference_inflight = %v after all attempts finished, want 0", got)
	}
}

// Many requests share no context, but the per-context helpers must also tolerate two
// goroutines creating the same context state at once (the hooks of different attempts
// run on different goroutines).
func TestInferenceContextStateCreationIsRaceFree(t *testing.T) {
	ctx := newHookContext(schemas.ChatCompletionRequest)
	var wg sync.WaitGroup
	onceSeen := make([]*sync.Once, 64)
	attemptOnceSeen := make([]*sync.Once, 64)
	trackers := make([]*dispatchedAttempts, 64)
	for g := 0; g < 64; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			onceSeen[g] = getOrCreateFinalizationOnce(ctx)
			attemptOnceSeen[g] = getOrCreateAttemptFinalizationOnce(ctx, g%4)
			trackers[g] = getOrCreateDispatchedAttempts(ctx)
			trackers[g].mark(g)
			if !trackers[g].takeIfMarked(g) || trackers[g].takeIfMarked(g) {
				t.Errorf("attempt %d must release exactly once", g)
			}
		}(g)
	}
	wg.Wait()
	for g := 1; g < len(onceSeen); g++ {
		if trackers[g] != trackers[0] || attemptOnceSeen[g] != attemptOnceSeen[g%4] {
			t.Errorf("goroutine %d got different per-context state", g)
		}
		if onceSeen[g] != onceSeen[0] {
			t.Fatalf("goroutine %d got a different finalization Once than goroutine 0", g)
		}
	}
}
