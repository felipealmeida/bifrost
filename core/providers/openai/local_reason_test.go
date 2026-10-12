package openai

import (
	"context"
	"strings"
	"testing"
	"time"

	providerUtils "github.com/maximhq/bifrost/core/providers/utils"
	"github.com/maximhq/bifrost/core/schemas"
	"github.com/valyala/fasthttp"
)

func TestLocalFallbackReasonRealPathParseAndEnrich(t *testing.T) {
	cases := []struct {
		name, body, want string
	}{
		{"backend_busy", `{"error":"prefill_backend_busy","lpcache":{"local_reason":"backend_busy"}}`, "backend_busy"},
		{"prefill_down", `{"error":"prefill_down","lpcache":{"local_reason":"prefill_down"}}`, "prefill_down"},
		{"unknown enum", `{"error":"prefill_backend_busy","lpcache":{"local_reason":"disk_full"}}`, ""},
		{"no lpcache", `{"error":"x"}`, ""},
		{"html body", `<html><body>503 Service Unavailable</body></html>`, ""},
		{"empty body", ``, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var resp fasthttp.Response
			resp.SetStatusCode(fasthttp.StatusServiceUnavailable)
			resp.SetBodyString(tc.body)

			ctx := schemas.NewBifrostContext(context.Background(), time.Now().Add(time.Minute))
			ctx.SetValue(schemas.BifrostContextKeyCaptureRawResponse, false)
			err := providerUtils.EnrichError(ctx, ParseOpenAIError(&resp), []byte(`{}`), nil, false, false, 0)

			if err.ExtraFields.RawResponse != nil {
				t.Fatalf("RawResponse = %v, want nil when capture is off", err.ExtraFields.RawResponse)
			}
			if err.ExtraFields.LocalReason != tc.want {
				t.Fatalf("LocalReason = %q, want %q", err.ExtraFields.LocalReason, tc.want)
			}
		})
	}
}

func TestLocalFallbackReasonRealPathNotSerialised(t *testing.T) {
	err := &schemas.BifrostError{ExtraFields: schemas.BifrostErrorExtraFields{LocalReason: "backend_busy"}}
	b, marshalErr := schemas.Marshal(err)
	if marshalErr != nil {
		t.Fatalf("marshal: %v", marshalErr)
	}
	if got := string(b); strings.Contains(got, "backend_busy") || strings.Contains(got, "LocalReason") {
		t.Fatalf("LocalReason leaked into JSON: %s", got)
	}
}
