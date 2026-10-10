package bifrost

import (
	"context"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	schemas "github.com/maximhq/bifrost/core/schemas"
)

// An upstream that sends role-only frames forever never goes idle and never
// closes, so the startup buffer overflows with no output. The failed attempt
// must be torn down (upstream connection closed) and the fallback must answer
// within a bounded time instead of waiting for a producer that never ends.
func TestStreamEndlessRoleOnlyOverflowFallsBack(t *testing.T) {
	const bound = 5 * time.Second
	var sent, clientClosed atomic.Int32
	handlerDone := make(chan struct{})
	endlessRole := func(stop <-chan struct{}) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			defer close(handlerDone)
			matrixSSEHeaders(w)
			tick := time.NewTicker(5 * time.Millisecond)
			defer tick.Stop()
			for {
				select {
				case <-stop:
					return
				case <-r.Context().Done():
					clientClosed.Add(1)
					return
				case <-tick.C:
					matrixWrite(w, matrixData(matrixRole))
					sent.Add(1)
				}
			}
		}
	}

	// Given: a primary that streams role-only frames forever, with an Anthropic fallback.
	stop := make(chan struct{})
	h := newMatrixHarness(t, endlessRole(stop), stop)

	// When: a streaming chat request is issued and drained.
	type outcome struct {
		content string
		errs    []string
		syncErr *schemas.BifrostError
	}
	done := make(chan outcome, 1)
	start := time.Now()
	go func() {
		ctx := schemas.NewBifrostContext(context.Background(), time.Now().Add(time.Minute))
		stream, err := h.client.ChatCompletionStreamRequest(ctx, matrixRequest())
		if err != nil {
			done <- outcome{syncErr: err}
			return
		}
		content, errs := drainChatStream(stream)
		done <- outcome{content: content, errs: errs}
	}()
	var out outcome
	select {
	case out = <-done:
	case <-time.After(bound):
		t.Fatalf("no answer within %v: fallback hits %d, upstream frames %d, upstream closed %d",
			bound, h.fallbackHits.Load(), sent.Load(), clientClosed.Load())
	}
	elapsed := time.Since(start)

	// Then: the fallback answered once, nothing from the primary was replayed,
	// and the primary's connection was closed by Bifrost (not by the test).
	if out.syncErr != nil {
		t.Fatalf("synchronous error: %s", out.syncErr.Error.Message)
	}
	if got := h.fallbackHits.Load(); got != 1 {
		t.Errorf("fallback hits = %d, want 1", got)
	}
	if out.content != "hello" || len(out.errs) != 0 {
		t.Errorf("content = %q errs = %v, want only the fallback answer", out.content, out.errs)
	}
	// The server notices the closed connection asynchronously; the stop channel
	// is closed only at cleanup, so an exit here means Bifrost hung up.
	select {
	case <-handlerDone:
	case <-time.After(time.Second):
		t.Fatalf("upstream handler still running after the answer (%d frames sent)", sent.Load())
	}
	if clientClosed.Load() != 1 {
		t.Errorf("upstream handler exited without seeing the client close")
	}
	frames := sent.Load()
	t.Logf("elapsed=%v upstream frames=%d", elapsed.Round(time.Millisecond), frames)
}
