package bifrost

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"sync/atomic"

	schemas "github.com/maximhq/bifrost/core/schemas"
)

// pluginRouter is a PreRequestHook-only routing plugin: it resolves the empty
// provider / "local-fast" request to a primary plus one fallback, the way the
// production routing plugin does.
type pluginRouter struct {
	fallbacks []schemas.Fallback
}

func (p *pluginRouter) GetName() string { return "test-plugin-router" }
func (p *pluginRouter) Cleanup() error  { return nil }
func (p *pluginRouter) PreRequestHook(_ *schemas.BifrostContext, req *schemas.BifrostRequest) error {
	req.SetProvider(schemas.OpenAI)
	req.SetModel("gpt-4o-mini")
	req.SetFallbacks(p.fallbacks)
	return nil
}
func (p *pluginRouter) PreLLMHook(_ *schemas.BifrostContext, req *schemas.BifrostRequest) (*schemas.BifrostRequest, *schemas.LLMPluginShortCircuit, error) {
	return req, nil, nil
}
func (p *pluginRouter) PostLLMHook(_ *schemas.BifrostContext, resp *schemas.BifrostResponse, bifrostErr *schemas.BifrostError) (*schemas.BifrostResponse, *schemas.BifrostError, error) {
	return resp, bifrostErr, nil
}

// A streaming request that arrives with no provider, is routed by a
// PreRequestHook plugin, and whose primary answers 503 must still reach the
// fallback, with or without session affinity active.
func TestStreamFallbackPluginRoutedSession(t *testing.T) {
	for _, tc := range []struct {
		name      string
		sessionID string
	}{
		{"with_session", "sess-1"},
		{"without_session", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			primary := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusServiceUnavailable)
				_, _ = fmt.Fprint(w, `{"error":"all slots busy, please retry later"}`)
			}))
			defer primary.Close()
			var fallbackHits atomic.Int32
			fallback := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				fallbackHits.Add(1)
				anthropicMessagesHandler()(w, r)
			}))
			defer fallback.Close()

			account := NewMockAccount()
			account.AddProviderWithBaseURL(schemas.OpenAI, 1, 1, primary.URL)
			account.AddProviderWithBaseURL(schemas.Anthropic, 1, 1, fallback.URL)
			account.configs[schemas.OpenAI].NetworkConfig.MaxRetries = 0
			account.configs[schemas.Anthropic].NetworkConfig.MaxRetries = 0
			account.SetKeysForProvider(schemas.OpenAI, []schemas.Key{
				{ID: "primary-key", Value: *schemas.NewSecretVar("sk-primary"), Models: schemas.WhiteList{"*"}, Weight: 100},
			})
			account.SetKeysForProvider(schemas.Anthropic, []schemas.Key{
				{ID: "fallback-key", Value: *schemas.NewSecretVar("sk-fallback"), Models: schemas.WhiteList{"*"}, Weight: 100},
			})
			client, err := Init(context.Background(), schemas.BifrostConfig{
				Account: account,
				Logger:  NewDefaultLogger(schemas.LogLevelError),
				LLMPlugins: []schemas.LLMPlugin{&pluginRouter{
					fallbacks: []schemas.Fallback{{Provider: schemas.Anthropic, Model: "claude-3-5-haiku-20241022"}},
				}},
			})
			if err != nil {
				t.Fatalf("failed to initialize bifrost: %v", err)
			}
			t.Cleanup(client.Shutdown)

			ctx := schemas.NewBifrostContext(context.Background(), time.Now().Add(30*time.Second))
			if tc.sessionID != "" {
				ctx.SetValue(schemas.BifrostContextKeySessionID, tc.sessionID)
			}
			if got := schemas.IsSessionAffinityActive(ctx); got != (tc.sessionID != "") {
				t.Fatalf("IsSessionAffinityActive = %v, want %v", got, tc.sessionID != "")
			}

			stream, bifrostErr := client.ChatCompletionStreamRequest(ctx, &schemas.BifrostChatRequest{
				Model: "local-fast",
				Input: []schemas.ChatMessage{
					{Role: schemas.ChatMessageRoleUser, Content: &schemas.ChatMessageContent{ContentStr: schemas.Ptr("hi")}},
				},
			})
			if bifrostErr != nil {
				t.Fatalf("fallback stream failed (fallback server hit %d time(s)): %s", fallbackHits.Load(), bifrostErr.Error.Message)
			}
			content, errs := drainChatStream(stream)
			if got := fallbackHits.Load(); got != 1 {
				t.Fatalf("fallback server hits = %d, want 1", got)
			}
			if len(errs) > 0 {
				t.Fatalf("fallback stream emitted error chunks: %v", errs)
			}
			if content != "hello" {
				t.Fatalf("fallback stream content = %q, want %q", content, "hello")
			}
		})
	}
}
