package openai

import (
	"testing"

	"github.com/maximhq/bifrost/core/schemas"
)

func TestNewOpenAIProviderSupportsLargeResponseHeaders(t *testing.T) {
	provider := NewOpenAIProvider(&schemas.ProviderConfig{}, nil)
	const minimumReadBufferSize = 64 * 1024

	if provider.client.ReadBufferSize < minimumReadBufferSize {
		t.Fatalf("unary client ReadBufferSize = %d; want at least %d", provider.client.ReadBufferSize, minimumReadBufferSize)
	}
	if provider.streamingClient.ReadBufferSize < minimumReadBufferSize {
		t.Fatalf("streaming client ReadBufferSize = %d; want at least %d", provider.streamingClient.ReadBufferSize, minimumReadBufferSize)
	}
}
