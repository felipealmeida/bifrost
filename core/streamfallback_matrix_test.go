package bifrost

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	schemas "github.com/maximhq/bifrost/core/schemas"
)

// Failure-shape matrix for a STREAMING chat request to an OpenAI-style primary
// (the local Qwen router, lpcache) with an Anthropic fallback.
//
// Rule under test: if the primary fails BEFORE the first real
// content/finish/usage chunk, Bifrost falls back and the client sees only the
// fallback answer with zero error chunks. If content was already emitted,
// Bifrost must not replay: the client sees content then an error chunk and the
// fallback is never hit.
//
// The "200 arrives too early" case is keepalive_then_error: lpcache sends 200
// plus SSE headers and ": prefill" comments while it prefills, then reports the
// failure as an SSE error event inside the 200.

const (
	matrixRole      = `{"choices":[{"delta":{"role":"assistant","content":null}}]}`
	matrixHel       = `{"id":"c1","object":"chat.completion.chunk","choices":[{"index":0,"delta":{"content":"Hel"}}]}`
	matrixLo        = `{"id":"c1","object":"chat.completion.chunk","choices":[{"index":0,"delta":{"content":"lo"}}]}`
	matrixFinish    = `{"id":"c1","object":"chat.completion.chunk","choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}`
	matrixUsage     = `{"id":"c1","object":"chat.completion.chunk","choices":[],"usage":{"prompt_tokens":10,"completion_tokens":2,"total_tokens":12}}`
	matrixErrObj    = `{"error":{"message":"handoff_destination_unavailable","type":"unavailable_error","code":503}}`
	matrixErrString = `{"error":"handoff_destination_unavailable"}`
	matrixReasoning = `{"id":"c1","object":"chat.completion.chunk","choices":[{"index":0,"delta":{"reasoning_content":"thinking"}}]}`
	matrixToolCall  = `{"id":"c1","object":"chat.completion.chunk","choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"id":"call_1","type":"function","function":{"name":"bash","arguments":""}}]}}]}`

	matrixCallBound = 20 * time.Second
	matrixStallSec  = 1 // stream idle timeout used by the test account
)

func matrixData(payload string) string { return "data: " + payload + "\n\n" }

const (
	matrixDone    = "data: [DONE]\n\n"
	matrixComment = ": prefill\n\n"
)

func matrixWrite(w http.ResponseWriter, s string) {
	_, _ = io.WriteString(w, s)
	if fl, ok := w.(http.Flusher); ok {
		fl.Flush()
	}
}

func matrixSSEHeaders(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.WriteHeader(http.StatusOK)
	if fl, ok := w.(http.Flusher); ok {
		fl.Flush()
	}
}

// matrixSend serves the parts over a 200 SSE response and ends the body cleanly.
func matrixSend(parts ...string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		matrixSSEHeaders(w)
		for _, p := range parts {
			matrixWrite(w, p)
		}
	}
}

// matrixAbort serves the parts as raw chunked HTTP/1.1 and then closes the TCP
// connection WITHOUT the terminating zero-length chunk.
func matrixAbort(parts ...string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		conn, buf, err := w.(http.Hijacker).Hijack()
		if err != nil {
			return
		}
		defer conn.Close()
		_, _ = buf.WriteString("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n")
		for _, p := range parts {
			_, _ = fmt.Fprintf(buf, "%x\r\n%s\r\n", len(p), p)
		}
		_ = buf.Flush()
		time.Sleep(50 * time.Millisecond)
	}
}

// matrixStall serves the parts, then goes silent until the test releases it.
func matrixStall(stop <-chan struct{}, parts ...string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		matrixSSEHeaders(w)
		for _, p := range parts {
			matrixWrite(w, p)
		}
		select {
		case <-stop:
		case <-r.Context().Done():
		}
	}
}

// matrixKeepalives writes n ": prefill" comments spaced by gap, then the parts.
func matrixKeepalives(n int, gap time.Duration, parts ...string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		matrixSSEHeaders(w)
		for range n {
			matrixWrite(w, matrixComment)
			time.Sleep(gap)
		}
		for _, p := range parts {
			matrixWrite(w, p)
		}
	}
}

// matrixCommentsForever serves the parts, then a ": prefill" comment every gap
// until the test releases it or the client goes away.
func matrixCommentsForever(stop <-chan struct{}, gap time.Duration, parts ...string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		matrixSSEHeaders(w)
		for _, p := range parts {
			matrixWrite(w, p)
		}
		for {
			select {
			case <-stop:
				return
			case <-r.Context().Done():
				return
			case <-time.After(gap):
				matrixWrite(w, matrixComment)
			}
		}
	}
}

func matrixHTTPError(status int, body, retryAfter string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if retryAfter != "" {
			w.Header().Set("Retry-After", retryAfter)
		}
		w.WriteHeader(status)
		_, _ = io.WriteString(w, body)
	}
}

func matrixStatus(status int, retryAfter string) http.HandlerFunc {
	return matrixHTTPError(status, fmt.Sprintf(`{"error":"upstream failure %d"}`, status), retryAfter)
}

var matrixOKStream = []string{
	matrixData(matrixRole), matrixData(matrixHel), matrixData(matrixLo),
	matrixData(matrixFinish), matrixData(matrixUsage), matrixDone,
}

type failureMode struct {
	name         string
	serve        func(stop <-chan struct{}) http.HandlerFunc
	fallbackHits int32
	content      string
	contentIsPfx bool // content is only a prefix of what the client saw
	errs         int  // exact number of error chunks; -1 means at least one
}

func serve(h http.HandlerFunc) func(<-chan struct{}) http.HandlerFunc {
	return func(<-chan struct{}) http.HandlerFunc { return h }
}

// fallsBack builds a Group P row: fallback serves "hello", no error chunks.
func fallsBack(name string, f func(stop <-chan struct{}) http.HandlerFunc) failureMode {
	return failureMode{name: name, serve: f, fallbackHits: 1, content: "hello"}
}

var failureModes = []failureMode{
	// GROUP P: must fall back. Failure arrives before any real content.
	fallsBack("http_503", serve(matrixStatus(503, "1"))),
	fallsBack("http_500", serve(matrixStatus(500, ""))),
	fallsBack("http_502", serve(matrixStatus(502, ""))),
	fallsBack("http_504", serve(matrixStatus(504, ""))),
	fallsBack("http_429", serve(matrixStatus(429, "1"))),
	fallsBack("http_400", serve(matrixStatus(400, ""))),
	fallsBack("http_413", serve(matrixStatus(413, ""))),
	fallsBack("http_409", serve(matrixStatus(409, ""))),
	fallsBack("http_503_obj", serve(matrixHTTPError(503, matrixErrObj, "1"))),
	fallsBack("json200_error", serve(matrixHTTPError(200, `{"object":"error","message":"boom","type":"server_error","code":503}`, ""))),
	fallsBack("non_sse_200", serve(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		_, _ = io.WriteString(w, "garbage, not an event stream")
	})),
	fallsBack("empty_200", serve(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Content-Length", "0")
		w.WriteHeader(http.StatusOK)
	})),
	fallsBack("close_early", serve(matrixSend())),
	fallsBack("abort_early", serve(matrixAbort())),
	fallsBack("close_after_role", serve(matrixSend(matrixData(matrixRole)))),
	fallsBack("abort_after_role", serve(matrixAbort(matrixData(matrixRole)))),
	fallsBack("sse_error_first", serve(matrixSend(matrixData(matrixErrObj), matrixDone))),
	fallsBack("sse_error_string_first", serve(matrixSend(matrixData(matrixErrString), matrixDone))),
	fallsBack("sse_error_string_after_role", serve(matrixSend(matrixData(matrixRole), matrixData(matrixErrString), matrixDone))),
	fallsBack("sse_error_malformed", serve(matrixSend(matrixData(`{"error":[]}`), matrixDone))),
	fallsBack("sse_error_code_only", serve(matrixSend(matrixData(`{"error":{"code":503}}`), matrixDone))),
	fallsBack("sse_error_type_only", serve(matrixSend(matrixData(`{"error":{"type":"unavailable"}}`), matrixDone))),
	fallsBack("sse_error_after_role", serve(matrixSend(matrixData(matrixRole), matrixData(matrixErrObj), matrixDone))),
	// The "200 arrives too early" case: 200 + keepalive comments, then the error event.
	fallsBack("keepalive_then_error", serve(matrixKeepalives(1, 100*time.Millisecond, matrixData(matrixErrObj), matrixDone))),
	fallsBack("keepalive3_then_error", serve(matrixKeepalives(3, 100*time.Millisecond, matrixData(matrixErrObj), matrixDone))),
	fallsBack("stall_headers", func(stop <-chan struct{}) http.HandlerFunc { return matrixStall(stop) }),
	fallsBack("stall_after_role", func(stop <-chan struct{}) http.HandlerFunc { return matrixStall(stop, matrixData(matrixRole)) }),
	// SSE comments are not activity: comment-only keepalives cannot hold the idle timer open.
	fallsBack("keepalive_forever", func(stop <-chan struct{}) http.HandlerFunc {
		return matrixCommentsForever(stop, 100*time.Millisecond)
	}),
	fallsBack("comments_forever_after_role", func(stop <-chan struct{}) http.HandlerFunc {
		return matrixCommentsForever(stop, 100*time.Millisecond, matrixData(matrixRole))
	}),

	// GROUP K: healthy primary, must NOT fall back.
	{name: "ok", serve: serve(matrixSend(matrixOKStream...)), content: "Hello"},
	// Comments do not reset idle, so the first data must come within the 1 s window (~0.6 s here).
	{name: "keepalive_then_ok", serve: serve(matrixKeepalives(3, 200*time.Millisecond, matrixOKStream...)), content: "Hello"},

	// GROUP N: content already emitted. No replay: error chunk after content.
	{name: "sse_string_error_mid", serve: serve(matrixSend(matrixData(matrixHel), matrixComment, matrixData(matrixErrString), matrixDone)), content: "Hel", errs: 1},
	{name: "sse_string_error_after_tool", serve: serve(matrixSend(matrixData(matrixToolCall), matrixData(matrixErrString), matrixDone)), errs: 1},
	{name: "sse_null_error", serve: serve(matrixSend(matrixData(`{"error":null,"choices":[{"delta":{"content":"Hello"}}]}`), matrixData(matrixFinish), matrixDone)), content: "Hello"},
	{name: "close_mid", serve: serve(matrixSend(matrixData(matrixRole), matrixData(matrixHel), matrixData(matrixLo))), content: "Hel", contentIsPfx: true, errs: -1},
	{name: "abort_mid", serve: serve(matrixAbort(matrixData(matrixRole), matrixData(matrixHel), matrixData(matrixLo))), content: "Hel", contentIsPfx: true, errs: -1},
	{name: "sse_error_mid", serve: serve(matrixSend(matrixData(matrixRole), matrixData(matrixHel), matrixData(matrixErrObj), matrixDone)), content: "Hel", contentIsPfx: true, errs: -1},
	{name: "stall_after_chunk", serve: func(stop <-chan struct{}) http.HandlerFunc {
		return matrixStall(stop, matrixData(matrixRole), matrixData(matrixHel))
	}, content: "Hel", contentIsPfx: true, errs: -1},
	{name: "comments_forever_after_content", serve: func(stop <-chan struct{}) http.HandlerFunc {
		return matrixCommentsForever(stop, 100*time.Millisecond, matrixData(matrixRole), matrixData(matrixHel))
	}, content: "Hel", errs: 1},
	// NEW: pinned from observation, see the report. Reasoning / tool-call deltas.
	{name: "reasoning_then_error", serve: serve(matrixSend(matrixData(matrixReasoning), matrixData(matrixErrObj), matrixDone)), fallbackHits: 0, content: "", contentIsPfx: true, errs: -1},
	{name: "toolcall_then_error", serve: serve(matrixSend(matrixData(matrixToolCall), matrixData(matrixErrObj), matrixDone)), fallbackHits: 0, content: "", contentIsPfx: true, errs: -1},

	// GROUP E: the stream ends with no output (stop or no finish_reason). Usage,
	// even with completion_tokens > 0, is not output. Must fall back.
	fallsBack("immediate_done", serve(matrixSend(matrixDone))),
	fallsBack("role_finish_done", serve(matrixSend(matrixData(matrixRole), matrixData(matrixFinish), matrixDone))),
	fallsBack("role_usage_done", serve(matrixSend(matrixData(matrixRole), matrixData(matrixUsage), matrixDone))),
	fallsBack("role_finish_tokens_done", serve(matrixSend(matrixData(matrixRole), matrixData(matrixFinish), matrixData(matrixUsage), matrixDone))),
	fallsBack("role_finish_eof", serve(matrixSend(matrixData(matrixRole), matrixData(matrixFinish)))),
	fallsBack("role_finish_parked", func(stop <-chan struct{}) http.HandlerFunc {
		return matrixStall(stop, matrixData(matrixRole), matrixData(matrixFinish))
	}),
	fallsBack("role_finish_comments", func(stop <-chan struct{}) http.HandlerFunc {
		return matrixCommentsForever(stop, 100*time.Millisecond, matrixData(matrixRole), matrixData(matrixFinish))
	}),
	fallsBack("empty_content_finish_done", serve(matrixSend(matrixData(matrixRole), matrixData(`{"choices":[{"index":0,"delta":{"content":""},"finish_reason":"stop"}]}`), matrixDone))),
	// Startup frames past the buffer bound fail before output instead of committing.
	fallsBack("role_flood_overflow", serve(matrixSend(append(slices.Repeat([]string{matrixData(matrixRole)}, 100), matrixDone)...))),

	// GROUP L: legitimate outcomes without text content. Must NOT fall back.
	{name: "refusal_stop", serve: serve(matrixSend(matrixData(matrixRole), matrixData(`{"choices":[{"index":0,"delta":{"refusal":"I can't help with that."}}]}`), matrixData(matrixFinish), matrixDone))},
	{name: "reasoning_only_stop", serve: serve(matrixSend(matrixData(matrixRole), matrixData(matrixReasoning), matrixData(matrixFinish), matrixDone))},
	{name: "tool_call_finish", serve: serve(matrixSend(matrixData(matrixRole), matrixData(matrixToolCall), matrixData(`{"choices":[{"index":0,"delta":{},"finish_reason":"tool_calls"}]}`), matrixDone))},
	{name: "content_filter_finish", serve: serve(matrixSend(matrixData(matrixRole), matrixData(`{"choices":[{"index":0,"delta":{},"finish_reason":"content_filter"}]}`), matrixDone))},
	{name: "length_finish", serve: serve(matrixSend(matrixData(matrixRole), matrixData(`{"choices":[{"index":0,"delta":{},"finish_reason":"length"}]}`), matrixData(matrixUsage), matrixDone))},
	{name: "multi_choice_second_has_content", serve: serve(matrixSend(matrixData(`{"choices":[{"index":0,"delta":{"role":"assistant"}},{"index":1,"delta":{"content":"x"}}]}`), matrixData(matrixFinish), matrixDone)), content: "x"},
	{name: "role_then_content_after_metadata", serve: serve(matrixSend(matrixData(matrixRole), matrixData(matrixRole), matrixData(matrixHel), matrixData(matrixFinish), matrixDone)), content: "Hel"},
}

// matrixHarness is one primary + fake Anthropic fallback behind a Bifrost client.
type matrixHarness struct {
	client       *Bifrost
	fallbackHits atomic.Int32
	stop         chan struct{}
}

func newMatrixHarness(t *testing.T, primary http.HandlerFunc, stop chan struct{}) *matrixHarness {
	t.Helper()
	h := &matrixHarness{stop: stop}
	primarySrv := httptest.NewServer(primary)
	t.Cleanup(primarySrv.Close)
	fallbackSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h.fallbackHits.Add(1)
		body, _ := io.ReadAll(r.Body)
		if strings.Contains(string(body), `"stream":true`) {
			anthropicMessagesHandler()(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"id":"msg_1","type":"message","role":"assistant","content":[{"type":"text","text":"hello"}],"model":"claude-3-5-haiku-20241022","stop_reason":"end_turn","usage":{"input_tokens":10,"output_tokens":2}}`)
	}))
	t.Cleanup(fallbackSrv.Close)

	account := NewMockAccount()
	account.AddProviderWithBaseURL(schemas.OpenAI, 1, 1, primarySrv.URL)
	account.AddProviderWithBaseURL(schemas.Anthropic, 1, 1, fallbackSrv.URL)
	// StreamIdleTimeoutInSeconds is the per-provider stream idle timer; 1 s keeps stalls short.
	account.configs[schemas.OpenAI].NetworkConfig.StreamIdleTimeoutInSeconds = matrixStallSec
	account.configs[schemas.OpenAI].NetworkConfig.MaxRetries = 0
	account.configs[schemas.Anthropic].NetworkConfig.MaxRetries = 0
	account.SetKeysForProvider(schemas.OpenAI, []schemas.Key{
		{ID: "primary-key", Value: *schemas.NewSecretVar("sk-primary"), Models: schemas.WhiteList{"*"}, Weight: 100},
	})
	account.SetKeysForProvider(schemas.Anthropic, []schemas.Key{
		{ID: "fallback-key", Value: *schemas.NewSecretVar("sk-fallback"), Models: schemas.WhiteList{"*"}, Weight: 100},
	})
	h.client = newStreamTestClient(t, account)
	// Runs before client.Shutdown and the server closes (cleanups are LIFO):
	// releases stalled handlers so nothing waits on them.
	t.Cleanup(func() { close(stop) })
	return h
}

func matrixRequest() *schemas.BifrostChatRequest {
	return &schemas.BifrostChatRequest{
		Provider: schemas.OpenAI,
		Model:    "gpt-4o-mini",
		Input: []schemas.ChatMessage{
			{Role: schemas.ChatMessageRoleUser, Content: &schemas.ChatMessageContent{ContentStr: schemas.Ptr("hi")}},
		},
		Fallbacks: []schemas.Fallback{{Provider: schemas.Anthropic, Model: "claude-3-5-haiku-20241022"}},
	}
}

type streamOutcome struct {
	content string
	errs    []string
	syncErr *schemas.BifrostError
}

// runStream issues the streaming request and drains it, within matrixCallBound.
func runStream(t *testing.T, h *matrixHarness) streamOutcome {
	t.Helper()
	done := make(chan streamOutcome, 1)
	go func() {
		ctx := schemas.NewBifrostContext(context.Background(), time.Now().Add(matrixCallBound+10*time.Second))
		stream, err := h.client.ChatCompletionStreamRequest(ctx, matrixRequest())
		if err != nil {
			done <- streamOutcome{syncErr: err}
			return
		}
		content, errs := drainChatStream(stream)
		done <- streamOutcome{content: content, errs: errs}
	}()
	select {
	case out := <-done:
		return out
	case <-time.After(matrixCallBound):
		t.Fatalf("call did not return within %v (fallback hits %d)", matrixCallBound, h.fallbackHits.Load())
		return streamOutcome{}
	}
}

func TestStreamFailureMatrix(t *testing.T) {
	for _, m := range failureModes {
		t.Run(m.name, func(t *testing.T) {
			stop := make(chan struct{})
			h := newMatrixHarness(t, m.serve(stop), stop)
			start := time.Now()
			out := runStream(t, h)
			t.Logf("elapsed=%v fallbackHits=%d content=%q errs=%v syncErr=%v",
				time.Since(start).Round(time.Millisecond), h.fallbackHits.Load(), out.content, out.errs, out.syncErr != nil)
			if out.syncErr != nil {
				t.Fatalf("stream returned a synchronous error: %s", out.syncErr.Error.Message)
			}
			var problems []string
			if got := h.fallbackHits.Load(); got != m.fallbackHits {
				problems = append(problems, fmt.Sprintf("fallback hits = %d, want %d", got, m.fallbackHits))
			}
			if m.contentIsPfx {
				if !strings.HasPrefix(out.content, m.content) {
					problems = append(problems, fmt.Sprintf("content = %q, want prefix %q", out.content, m.content))
				}
			} else if out.content != m.content {
				problems = append(problems, fmt.Sprintf("content = %q, want %q", out.content, m.content))
			}
			switch {
			case m.errs < 0 && len(out.errs) == 0:
				problems = append(problems, "want at least 1 error chunk, got none")
			case m.errs >= 0 && len(out.errs) != m.errs:
				problems = append(problems, fmt.Sprintf("error chunks = %v, want %d", out.errs, m.errs))
			}
			for _, p := range problems {
				t.Error(p)
			}
		})
	}
}

// Non-stream subset (stream=false).
func TestNonStreamFailureMatrix(t *testing.T) {
	cases := []struct {
		name         string
		serve        http.HandlerFunc
		fallbackHits int32
		content      string
	}{
		{name: "http_503", serve: matrixStatus(503, "1"), fallbackHits: 1, content: "hello"},
		{name: "empty_200", serve: func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("Content-Length", "0")
			w.WriteHeader(http.StatusOK)
		}, fallbackHits: 1, content: "hello"},
		{name: "close_mid", serve: func(w http.ResponseWriter, r *http.Request) {
			conn, buf, err := w.(http.Hijacker).Hijack()
			if err != nil {
				return
			}
			defer conn.Close()
			_, _ = buf.WriteString("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 500\r\nConnection: close\r\n\r\n{\"id\":\"c1\",\"choices\":[{\"mess")
			_ = buf.Flush()
		}, fallbackHits: 1, content: "hello"},
		{name: "json200_error", serve: matrixHTTPError(200, `{"object":"error","message":"boom","type":"server_error","code":503}`, ""), fallbackHits: 1, content: "hello"},
		{name: "json200_string", serve: matrixHTTPError(200, matrixErrString, ""), fallbackHits: 1, content: "hello"},
		{name: "json200_object", serve: matrixHTTPError(200, matrixErrObj, ""), fallbackHits: 1, content: "hello"},
		{name: "json200_malformed", serve: matrixHTTPError(200, `{"error":true}`, ""), fallbackHits: 1, content: "hello"},
	}
	for _, c := range cases {
		t.Run("nonstream_"+c.name, func(t *testing.T) {
			stop := make(chan struct{})
			h := newMatrixHarness(t, c.serve, stop)
			type result struct {
				resp *schemas.BifrostChatResponse
				err  *schemas.BifrostError
			}
			done := make(chan result, 1)
			go func() {
				ctx := schemas.NewBifrostContext(context.Background(), time.Now().Add(matrixCallBound+10*time.Second))
				resp, err := h.client.ChatCompletionRequest(ctx, matrixRequest())
				done <- result{resp, err}
			}()
			var res result
			select {
			case res = <-done:
			case <-time.After(matrixCallBound):
				t.Fatalf("call did not return within %v", matrixCallBound)
			}
			var content string
			if res.resp != nil {
				for _, ch := range res.resp.Choices {
					if ch.ChatNonStreamResponseChoice != nil && ch.Message != nil && ch.Message.Content != nil && ch.Message.Content.ContentStr != nil {
						content += *ch.Message.Content.ContentStr
					}
				}
			}
			t.Logf("fallbackHits=%d content=%q err=%v", h.fallbackHits.Load(), content, res.err != nil)
			if got := h.fallbackHits.Load(); got != c.fallbackHits {
				t.Errorf("fallback hits = %d, want %d", got, c.fallbackHits)
			}
			if content != c.content {
				t.Errorf("content = %q, want %q", content, c.content)
			}
			if c.fallbackHits == 1 && res.err != nil {
				t.Errorf("unexpected error: %s", res.err.Error.Message)
			}
		})
	}
}
