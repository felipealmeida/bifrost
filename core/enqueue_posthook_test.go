package bifrost

import (
	"context"
	"sync/atomic"
	"testing"

	schemas "github.com/maximhq/bifrost/core/schemas"
)

// inflightPlugin models a plugin that holds a resource from PreLLMHook until the
// terminal PostLLMHook (the telemetry inflight gauge). For streams the terminal
// post hook is the one that sees the stream-end indicator.
type inflightPlugin struct {
	stream   bool
	pre      atomic.Int64
	released atomic.Int64
}

func (p *inflightPlugin) GetName() string { return "inflight-plugin" }
func (p *inflightPlugin) Cleanup() error  { return nil }
func (p *inflightPlugin) PreRequestHook(_ *schemas.BifrostContext, _ *schemas.BifrostRequest) error {
	return nil
}
func (p *inflightPlugin) PreLLMHook(_ *schemas.BifrostContext, req *schemas.BifrostRequest) (*schemas.BifrostRequest, *schemas.LLMPluginShortCircuit, error) {
	p.pre.Add(1)
	return req, nil, nil
}
func (p *inflightPlugin) PostLLMHook(ctx *schemas.BifrostContext, resp *schemas.BifrostResponse, bifrostErr *schemas.BifrostError) (*schemas.BifrostResponse, *schemas.BifrostError, error) {
	if !p.stream || IsFinalChunk(ctx) {
		p.released.Add(1)
	}
	return resp, bifrostErr, nil
}

// TestEnqueueFailureRunsPostHooks locks the contract that a request whose PreLLMHook
// ran must reach the terminal PostLLMHook even when it fails before it is enqueued
// (queue full, provider shutting down, caller gone). Otherwise a plugin that holds a
// resource between the two hooks, such as the telemetry inflight gauge, leaks it.
func TestEnqueueFailureRunsPostHooks(t *testing.T) {
	cases := []struct {
		name   string
		stream bool
		setup  func(pq *ProviderQueue, b *Bifrost) (*schemas.BifrostContext, context.CancelFunc)
	}{
		{"queue full", false, setupQueueFull},
		{"queue full stream", true, setupQueueFull},
		{"provider closing", false, setupClosing},
		{"provider closing stream", true, setupClosing},
		{"caller gone while queue full", false, setupCallerGone},
		{"caller gone while queue full stream", true, setupCallerGone},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			plugin := &inflightPlugin{stream: tc.stream}
			account := NewMockAccount()
			account.AddProvider(schemas.OpenAI, 1, 1)
			account.SetKeysForProvider(schemas.OpenAI, []schemas.Key{{
				ID: "k", Value: *schemas.NewSecretVar("sk-test"), Models: schemas.WhiteList{"*"}, Weight: 100,
			}})
			client, err := Init(context.Background(), schemas.BifrostConfig{
				Account: account, Logger: NewNoOpLogger(), LLMPlugins: []schemas.LLMPlugin{plugin},
			})
			if err != nil {
				t.Fatalf("Init failed: %v", err)
			}
			t.Cleanup(client.Shutdown)

			// Swap in a queue with no worker. Restore the real one before Shutdown
			// (cleanups run last-in first-out) so Shutdown can stop its worker.
			orig, _ := client.requestQueues.Load(schemas.OpenAI)
			pq := &ProviderQueue{queue: make(chan *ChannelMessage), done: make(chan struct{})}
			client.requestQueues.Store(schemas.OpenAI, pq)
			t.Cleanup(func() { client.requestQueues.Store(schemas.OpenAI, orig) })

			ctx, cancel := tc.setup(pq, client)
			defer cancel()

			chat := &schemas.BifrostChatRequest{
				Provider: schemas.OpenAI,
				Model:    "gpt-4o-mini",
				Input: []schemas.ChatMessage{{
					Role:    schemas.ChatMessageRoleUser,
					Content: &schemas.ChatMessageContent{ContentStr: new("hi")},
				}},
			}
			var bifrostErr *schemas.BifrostError
			if tc.stream {
				_, bifrostErr = client.tryStreamRequest(ctx, &schemas.BifrostRequest{RequestType: schemas.ChatCompletionStreamRequest, ChatRequest: chat})
			} else {
				_, bifrostErr = client.tryRequest(ctx, &schemas.BifrostRequest{RequestType: schemas.ChatCompletionRequest, ChatRequest: chat})
			}
			if bifrostErr == nil {
				t.Fatal("expected an enqueue error, got nil")
			}
			if got := plugin.pre.Load(); got != 1 {
				t.Fatalf("PreLLMHook ran %d times, want 1", got)
			}
			if got := plugin.released.Load(); got != 1 {
				t.Fatalf("terminal PostLLMHook ran %d times after an enqueue failure, want 1 (resource held since PreLLMHook leaks)", got)
			}
		})
	}
}

func setupQueueFull(_ *ProviderQueue, b *Bifrost) (*schemas.BifrostContext, context.CancelFunc) {
	b.dropExcessRequests.Store(true)
	return schemas.NewBifrostContextWithCancel(context.Background())
}

func setupClosing(pq *ProviderQueue, _ *Bifrost) (*schemas.BifrostContext, context.CancelFunc) {
	pq.signalClosing()
	return schemas.NewBifrostContextWithCancel(context.Background())
}

func setupCallerGone(_ *ProviderQueue, b *Bifrost) (*schemas.BifrostContext, context.CancelFunc) {
	b.dropExcessRequests.Store(false)
	ctx, cancel := schemas.NewBifrostContextWithCancel(context.Background())
	cancel()
	return ctx, cancel
}
