package bifrost

import (
	"fmt"
	"io"
	"net/http"
	"sync/atomic"
	"testing"
)

// Exercise real provider HTTP parsing and fallback dispatch, not fabricated
// BifrostError values. Both upstreams are loopback mocks with synthetic keys.
func TestStorageFailureHTTPFallback(t *testing.T) {
	for _, status := range []int{502, 504} {
		for _, streaming := range []bool{false, true} {
			t.Run(fmt.Sprintf("status_%d/stream_%t", status, streaming), func(t *testing.T) {
				var localHits atomic.Int32
				local := lrServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.URL.Path != "/v1/chat/completions" {
						t.Errorf("unexpected local path: %s", r.URL.Path)
						http.NotFound(w, r)
						return
					}
					localHits.Add(1)
					w.Header().Set("Content-Type", "application/json")
					w.WriteHeader(status)
					body := `{"error":"prefill_save_failed"}`
					_, _ = io.WriteString(w, body)
				}))
				got := runLocalReasonFallback(t, lrLocalProvider, local, streaming)
				wantContent := "from-cloud"
				if streaming {
					wantContent = "Hello"
				}
				if localHits.Load() != 1 || got.cloudHits != 1 {
					t.Fatalf("local hits=%d cloud hits=%d; want one each", localHits.Load(), got.cloudHits)
				}
				if got.content != wantContent {
					t.Fatalf("content=%q, want mock cloud content %q", got.content, wantContent)
				}
				if len(got.transitions) != 1 || got.transitions[0].from != string(lrLocalProvider) || got.transitions[0].to != string(lrCloudProvider) {
					t.Fatalf("unexpected fallback transitions: %+v", got.transitions)
				}
			})
		}
	}
}
