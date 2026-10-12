package bifrost

import (
	"context"
	"testing"
	"time"

	"github.com/maximhq/bifrost/core/providers/openai"
	providerUtils "github.com/maximhq/bifrost/core/providers/utils"
	schemas "github.com/maximhq/bifrost/core/schemas"
	"github.com/valyala/fasthttp"
)

func localReasonErr(raw interface{}) *schemas.BifrostError {
	return &schemas.BifrostError{ExtraFields: schemas.BifrostErrorExtraFields{RawResponse: raw}}
}

func lpcacheBody(reason interface{}) map[string]interface{} {
	return map[string]interface{}{
		"error":   "prefill_backend_busy",
		"lpcache": map[string]interface{}{"local_reason": reason},
	}
}

func TestLocalFallbackReasonExtract(t *testing.T) {
	connFailed := &schemas.BifrostError{ExtraFields: schemas.BifrostErrorExtraFields{ErrorType: schemas.ErrorTypeProviderConnectionFailed}}
	connFailedWithBody := localReasonErr(lpcacheBody("prefill_down"))
	connFailedWithBody.ExtraFields.ErrorType = schemas.ErrorTypeProviderConnectionFailed

	cases := []struct {
		name string
		err  *schemas.BifrostError
		want string
	}{
		{"backend_busy", localReasonErr(lpcacheBody("backend_busy")), "backend_busy"},
		{"prefill_down", localReasonErr(lpcacheBody("prefill_down")), "prefill_down"},
		{"template_mismatch", localReasonErr(lpcacheBody("template_mismatch")), "template_mismatch"},
		{"absent metadata", localReasonErr(nil), ""},
		{"no lpcache key", localReasonErr(map[string]interface{}{"error": "x"}), ""},
		{"unknown enum", localReasonErr(lpcacheBody("something_else")), ""},
		{"transport is not accepted from body", localReasonErr(lpcacheBody("transport")), ""},
		{"raw response is a string", localReasonErr("prefill_backend_busy"), ""},
		{"lpcache is not an object", localReasonErr(map[string]interface{}{"lpcache": "backend_busy"}), ""},
		{"local_reason is a number", localReasonErr(lpcacheBody(42)), ""},
		{"local_reason is missing", localReasonErr(map[string]interface{}{"lpcache": map[string]interface{}{}}), ""},
		{"nil error", nil, ""},
		{"connection failed", connFailed, "transport"},
		{"body reason wins over connection failed", connFailedWithBody, "prefill_down"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := extractLocalFallbackReason(tc.err); got != tc.want {
				t.Fatalf("extractLocalFallbackReason = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestLocalFallbackReasonFrom503(t *testing.T) {
	status := 503
	newErr := func(raw interface{}) *schemas.BifrostError {
		err := localReasonErr(raw)
		err.StatusCode = &status
		return err
	}

	cases := []struct {
		name       string
		err        *schemas.BifrostError
		wantReason string
		wantLocal  string
	}{
		{"lpcache busy body", newErr(lpcacheBody("backend_busy")), "capacity", "backend_busy"},
		{"missing metadata", newErr(nil), "capacity", ""},
		{"malformed string body", newErr("not json object"), "capacity", ""},
		{"malformed lpcache", newErr(map[string]interface{}{"lpcache": 7}), "capacity", ""},
		{"status alone does not imply a local cause", newErr(map[string]interface{}{"error": "overloaded"}), "capacity", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := normalizeFallbackReason(tc.err); got != tc.wantReason {
				t.Fatalf("normalizeFallbackReason = %q, want %q", got, tc.wantReason)
			}
			if got := extractLocalFallbackReason(tc.err); got != tc.wantLocal {
				t.Fatalf("extractLocalFallbackReason = %q, want %q", got, tc.wantLocal)
			}
		})
	}
}

func TestLocalFallbackReasonRealPathEnrichedError(t *testing.T) {
	var resp fasthttp.Response
	resp.SetStatusCode(fasthttp.StatusServiceUnavailable)
	resp.SetBodyString(`{"error":"prefill_backend_busy","lpcache":{"local_reason":"backend_busy"}}`)

	ctx := schemas.NewBifrostContext(context.Background(), time.Now().Add(time.Minute))
	ctx.SetValue(schemas.BifrostContextKeyCaptureRawResponse, false)
	err := providerUtils.EnrichError(ctx, openai.ParseOpenAIError(&resp), []byte(`{}`), resp.Body(), false, false, 0)

	if err.ExtraFields.RawResponse != nil {
		t.Fatalf("RawResponse = %v, want nil when capture is off", err.ExtraFields.RawResponse)
	}
	if got := normalizeFallbackReason(err); got != "capacity" {
		t.Fatalf("normalizeFallbackReason = %q, want capacity", got)
	}
	if got := extractLocalFallbackReason(err); got != "backend_busy" {
		t.Fatalf("extractLocalFallbackReason = %q, want backend_busy", got)
	}
}
