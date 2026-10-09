package openai

import (
	"context"
	"testing"

	"github.com/maximhq/bifrost/core/schemas"
	"github.com/valyala/fasthttp"
)

func TestSetKVSessionHeaders(t *testing.T) {
	both := map[string]string{
		"x-session-id":        "ses_abc",
		"x-parent-session-id": "ses_parent",
	}

	tests := []struct {
		name         string
		provider     schemas.ModelProvider
		clientHdrs   map[string]string
		noHeadersCtx bool
		preset       map[string]string
		wantSession  string
		wantParent   string
	}{
		{
			name:        "kv router with both headers",
			provider:    "qwen-kv-router",
			clientHdrs:  both,
			wantSession: "ses_abc",
			wantParent:  "ses_parent",
		},
		{
			name:        "kv router with only session id",
			provider:    "qwen-kv-router",
			clientHdrs:  map[string]string{"x-session-id": "ses_abc"},
			wantSession: "ses_abc",
		},
		{
			name:       "openai provider does not leak",
			provider:   "openai",
			clientHdrs: both,
		},
		{
			name:       "teamclaude provider does not leak",
			provider:   "teamclaude-canary",
			clientHdrs: both,
		},
		{
			name:         "no request headers in ctx",
			provider:     "qwen-kv-router",
			noHeadersCtx: true,
		},
		{
			name:     "empty values are not set",
			provider: "qwen-kv-router",
			clientHdrs: map[string]string{
				"x-session-id":        "",
				"x-parent-session-id": "",
			},
		},
		{
			name:        "pre-existing value is overwritten",
			provider:    "qwen-kv-router",
			clientHdrs:  both,
			preset:      map[string]string{"x-kv-session-id": "stale", "x-kv-parent-session-id": "stale"},
			wantSession: "ses_abc",
			wantParent:  "ses_parent",
		},
		{
			name:        "pre-existing value kept when source is empty",
			provider:    "qwen-kv-router",
			clientHdrs:  map[string]string{"x-session-id": ""},
			preset:      map[string]string{"x-kv-session-id": "keep"},
			wantSession: "keep",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			if !tc.noHeadersCtx {
				ctx = context.WithValue(ctx, schemas.BifrostContextKeyRequestHeaders, tc.clientHdrs)
			}
			req := fasthttp.AcquireRequest()
			defer fasthttp.ReleaseRequest(req)
			for k, v := range tc.preset {
				req.Header.Set(k, v)
			}

			setKVSessionHeaders(ctx, req, tc.provider)

			if got := string(req.Header.Peek("x-kv-session-id")); got != tc.wantSession {
				t.Errorf("x-kv-session-id = %q; want %q", got, tc.wantSession)
			}
			if got := string(req.Header.Peek("x-kv-parent-session-id")); got != tc.wantParent {
				t.Errorf("x-kv-parent-session-id = %q; want %q", got, tc.wantParent)
			}
		})
	}
}

func TestSetKVSessionHeadersWithBifrostContext(t *testing.T) {
	ctx := schemas.NewBifrostContext(context.Background(), schemas.NoDeadline)
	ctx.SetValue(schemas.BifrostContextKeyRequestHeaders, map[string]string{
		"x-session-id": "ses_abc",
	})
	req := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(req)

	setKVSessionHeaders(ctx, req, kvRouterProviderName)

	if got := string(req.Header.Peek("x-kv-session-id")); got != "ses_abc" {
		t.Errorf("x-kv-session-id = %q; want %q", got, "ses_abc")
	}
}
