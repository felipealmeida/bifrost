package openai

import (
	"net"
	"strings"
	"testing"

	"github.com/maximhq/bifrost/core/schemas"
	"github.com/valyala/fasthttp"
)

func TestOpenAIClientsReadLargeResponseHeaders(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	server := &fasthttp.Server{Handler: func(ctx *fasthttp.RequestCtx) {
		ctx.Response.Header.Set("X-Large-Response-Header", strings.Repeat("x", 8*1024))
		ctx.SetStatusCode(fasthttp.StatusOK)
		ctx.SetBodyString("ok")
	}}
	go func() {
		_ = server.Serve(listener)
	}()
	t.Cleanup(func() {
		_ = server.Shutdown()
	})

	provider := NewOpenAIProvider(&schemas.ProviderConfig{
		NetworkConfig: schemas.NetworkConfig{AllowPrivateNetwork: true},
	}, nil)
	clients := []struct {
		name   string
		client *fasthttp.Client
	}{
		{name: "unary", client: provider.client},
		{name: "streaming", client: provider.streamingClient},
	}

	for _, testCase := range clients {
		t.Run(testCase.name, func(t *testing.T) {
			request := fasthttp.AcquireRequest()
			response := fasthttp.AcquireResponse()
			defer fasthttp.ReleaseRequest(request)
			defer fasthttp.ReleaseResponse(response)

			request.SetRequestURI("http://" + listener.Addr().String())
			request.Header.SetMethod(fasthttp.MethodGet)
			if err := testCase.client.Do(request, response); err != nil {
				t.Fatalf("request with an 8 KiB response header failed: %v", err)
			}
			if got := len(response.Header.Peek("X-Large-Response-Header")); got != 8*1024 {
				t.Fatalf("large response header length = %d; want %d", got, 8*1024)
			}
		})
	}
}
