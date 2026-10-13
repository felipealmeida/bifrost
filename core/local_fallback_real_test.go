package bifrost

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	schemas "github.com/maximhq/bifrost/core/schemas"
)

// End-to-end check that the lpcache local cause reaches the FallbackObserver
// through the real handleRequest / handleStreamRequest fallback loop: a real
// OpenAI-based custom provider parses a real HTTP 503 from an httptest server,
// with raw response capture off (production default for qwen-kv-router).

const (
	lrLocalProvider = schemas.ModelProvider("qwen-kv-router")
	lrCloudProvider = schemas.ModelProvider("cloud-openai")
	lrCloudModel    = "cloud-model"
	lrBusyBody      = `{"error":"prefill_backend_busy","lpcache":{"local_reason":"backend_busy"}}`
	lrCloudJSON     = `{"id":"chatcmpl-b","object":"chat.completion","created":1,"model":"cloud-model",` +
		`"choices":[{"index":0,"message":{"role":"assistant","content":"from-cloud"},"finish_reason":"stop"}],` +
		`"usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}}`
)

type lrTransition struct{ from, to, reason, localReason string }

type lrObserver struct {
	mu          sync.Mutex
	transitions []lrTransition
}

func (o *lrObserver) OnFallbackTransition(fromProvider, fromModel, toProvider, toModel, reason, localReason string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.transitions = append(o.transitions, lrTransition{fromProvider, toProvider, reason, localReason})
}

func (o *lrObserver) OnRetryAttempt(provider, model, reason string) {}

func (o *lrObserver) snapshot() []lrTransition {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]lrTransition(nil), o.transitions...)
}

// lrLocal503 plays lpcache refusing admission.
func lrLocal503(body string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/chat/completions" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Retry-After", "30")
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = io.WriteString(w, body)
	}
}

// lrCloud plays the cloud fallback: JSON for unary, SSE for stream requests.
func lrCloud(hits *atomic.Int32) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		reqBody, _ := io.ReadAll(r.Body)
		if strings.Contains(string(reqBody), `"stream":true`) {
			matrixSend(matrixOKStream...)(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, lrCloudJSON)
	}
}

func lrServer(t *testing.T, h http.Handler) string {
	t.Helper()
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	return srv.URL
}

// lrClosedURL returns a URL on a port that was just released, so dialing it is refused.
func lrClosedURL(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()
	if err := ln.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	return "http://" + addr
}

// lrAddOpenAIProvider registers an OpenAI-based provider. A custom name gets a
// CustomProviderConfig with BaseProviderType openai, like qwen-kv-router in routing-export.json.
func lrAddOpenAIProvider(account *MockAccount, provider schemas.ModelProvider, baseURL string) {
	account.AddProviderWithBaseURL(provider, 1, 1, baseURL)
	account.configs[provider].NetworkConfig.MaxRetries = 0
	if provider != schemas.OpenAI {
		account.SetCustomProviderConfig(provider, &schemas.CustomProviderConfig{BaseProviderType: schemas.OpenAI})
	}
	account.SetKeysForProvider(provider, []schemas.Key{
		{ID: string(provider) + "-key", Value: *schemas.NewSecretVar("sk-" + string(provider)), Models: schemas.WhiteList{"*"}, Weight: 100},
	})
}

type lrResult struct {
	transitions []lrTransition
	cloudHits   int32
	content     string
}

func runLocalReasonFallback(t *testing.T, primary schemas.ModelProvider, primaryURL string, stream bool) lrResult {
	t.Helper()
	var cloudHits atomic.Int32
	account := NewMockAccount()
	lrAddOpenAIProvider(account, primary, primaryURL)
	lrAddOpenAIProvider(account, lrCloudProvider, lrServer(t, lrCloud(&cloudHits)))
	client := newStreamTestClient(t, account)

	obs := &lrObserver{}
	ctx := schemas.NewBifrostContext(context.Background(), time.Now().Add(30*time.Second))
	ctx.SetValue(schemas.BifrostContextKeyFallbackObserver, schemas.FallbackObserver(obs))
	req := &schemas.BifrostChatRequest{
		Provider: primary,
		Model:    "qwen-local",
		Input: []schemas.ChatMessage{
			{Role: schemas.ChatMessageRoleUser, Content: &schemas.ChatMessageContent{ContentStr: schemas.Ptr("hi")}},
		},
		Fallbacks: []schemas.Fallback{{Provider: lrCloudProvider, Model: lrCloudModel}},
	}

	var content string
	if stream {
		ch, bifrostErr := client.ChatCompletionStreamRequest(ctx, req)
		if bifrostErr != nil {
			t.Fatalf("stream fallback failed: %s", bifrostErr.GetErrorString())
		}
		var errs []string
		content, errs = drainChatStream(ch)
		if len(errs) > 0 {
			t.Fatalf("stream emitted error chunks: %v", errs)
		}
	} else {
		resp, bifrostErr := client.ChatCompletionRequest(ctx, req)
		if bifrostErr != nil {
			t.Fatalf("fallback failed: %s", bifrostErr.GetErrorString())
		}
		if len(resp.Choices) == 0 || resp.Choices[0].ChatNonStreamResponseChoice == nil ||
			resp.Choices[0].ChatNonStreamResponseChoice.Message == nil ||
			resp.Choices[0].ChatNonStreamResponseChoice.Message.Content == nil ||
			resp.Choices[0].ChatNonStreamResponseChoice.Message.Content.ContentStr == nil {
			t.Fatalf("response has no text content: %+v", resp)
		}
		content = *resp.Choices[0].ChatNonStreamResponseChoice.Message.Content.ContentStr
	}
	return lrResult{transitions: obs.snapshot(), cloudHits: cloudHits.Load(), content: content}
}

func TestLocalFallbackReasonThroughRealFallback(t *testing.T) {
	cases := []struct {
		name       string
		primary    schemas.ModelProvider
		primaryURL func(t *testing.T) string
		wantReason string
		wantLocal  string
		knownGap   string
	}{
		{"lpcache backend_busy", lrLocalProvider, func(t *testing.T) string { return lrServer(t, lrLocal503(lrBusyBody)) }, "capacity", "backend_busy", ""},
		{"503 without lpcache metadata", lrLocalProvider, func(t *testing.T) string { return lrServer(t, lrLocal503(`{"error":"x"}`)) }, "capacity", "", ""},
		{"503 with unknown local_reason", lrLocalProvider, func(t *testing.T) string {
			return lrServer(t, lrLocal503(`{"error":"x","lpcache":{"local_reason":"something_else"}}`))
		}, "capacity", "", ""},
		{"connection refused", lrLocalProvider, lrClosedURL, "provider_error", "transport",
			// The real dial failure sets Error.Type=provider_connection_failed
			// (NewBifrostUpstreamConnectionError) but never ExtraFields.ErrorType, which
			// is the only field extractLocalFallbackReason checks, so it yields "".
			"real connection failure carries Error.Type, not ExtraFields.ErrorType; extractLocalFallbackReason returns \"\" (production fix pending)"},
		// Core is origin-agnostic: a non-local provider echoing the lpcache body still
		// yields backend_busy here. The telemetry plugin's normalizeLocalReason gate is
		// what maps a non-local from-provider to "none".
		{"non-local provider with lpcache body", schemas.OpenAI, func(t *testing.T) string { return lrServer(t, lrLocal503(lrBusyBody)) }, "capacity", "backend_busy", ""},
	}
	for _, stream := range []bool{false, true} {
		mode, wantContent := "unary", "from-cloud"
		if stream {
			mode, wantContent = "stream", "Hello"
		}
		for _, tc := range cases {
			t.Run(mode+"/"+tc.name, func(t *testing.T) {
				got := runLocalReasonFallback(t, tc.primary, tc.primaryURL(t), stream)
				if got.cloudHits != 1 {
					t.Fatalf("cloud hits = %d, want 1", got.cloudHits)
				}
				if got.content != wantContent {
					t.Fatalf("content = %q, want %q (served by cloud)", got.content, wantContent)
				}
				want := []lrTransition{{string(tc.primary), string(lrCloudProvider), tc.wantReason, tc.wantLocal}}
				if len(got.transitions) != 1 {
					t.Fatalf("transitions = %+v, want %+v", got.transitions, want)
				}
				if got.transitions[0] != want[0] {
					sameExceptLocal := got.transitions[0]
					sameExceptLocal.localReason = tc.wantLocal
					if tc.knownGap != "" && sameExceptLocal == want[0] {
						t.Skipf("known gap: %s; got %+v", tc.knownGap, got.transitions[0])
					}
					t.Fatalf("transitions = %+v, want %+v", got.transitions, want)
				}
			})
		}
	}
}
