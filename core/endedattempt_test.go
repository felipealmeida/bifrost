package bifrost

import (
	"context"
	"testing"
	"time"

	providerUtils "github.com/maximhq/bifrost/core/providers/utils"
	"github.com/maximhq/bifrost/core/schemas"
)

// cancelledError is the context.Canceled a provider call returns when the caller leaves mid-request:
// not built by Bifrost, no status, but typed request_cancelled like Bifrost's own 499.
func cancelledError() *schemas.BifrostError {
	return &schemas.BifrostError{
		IsBifrostError: false,
		Error:          &schemas.ErrorField{Type: Ptr(schemas.RequestCancelled), Message: "request cancelled", Error: context.Canceled},
	}
}

// expiredContextError is the 504 newBifrostCtxDoneError builds for a request whose deadline passed.
func expiredContextError(t *testing.T) *schemas.BifrostError {
	t.Helper()
	ctx := schemas.NewBifrostContext(context.Background(), time.Now().Add(-time.Second))
	<-ctx.Done()
	return newBifrostCtxDoneError(ctx, "during the request")
}

// requireEndedAttempt fails the test unless the one record on the trail carries class, status and
// failReason ("" wants none).
func requireEndedAttempt(t *testing.T, ctx *schemas.BifrostContext, class schemas.FailureClass, status int, failReason string) {
	t.Helper()
	got := attemptTrail(t, ctx, 1)[0]
	if got.FailureClass != class || got.StatusCode == nil || *got.StatusCode != status {
		t.Fatalf("attempt class=%q status=%v, want %q %d (%+v)", got.FailureClass, got.StatusCode, class, status, got)
	}
	switch {
	case failReason == "" && got.FailReason != nil:
		t.Fatalf("attempt fail_reason=%q, want none", *got.FailReason)
	case failReason != "" && (got.FailReason == nil || *got.FailReason != failReason):
		t.Fatalf("attempt fail_reason=%v, want %q", got.FailReason, failReason)
	}
}

// An attempt Bifrost ended is labelled on the trail though the retry loop stops on it before it
// classifies: a timeout (the provider's or the request's deadline) is timeout 504 with
// request_timed_out, and a cancellation, Bifrost's 499 or a provider call's context.Canceled, is
// cancelled 499 with no fail_reason, since a fail_reason says the key failed.
func TestExecuteRequestWithRetries_StampsTheAttemptBifrostEnded(t *testing.T) {
	cases := []struct {
		name       string
		err        func(t *testing.T) *schemas.BifrostError
		class      schemas.FailureClass
		status     int
		failReason string
	}{
		{name: "provider timeout", err: func(*testing.T) *schemas.BifrostError {
			return providerUtils.NewBifrostTimeoutError(schemas.ErrProviderRequestTimedOut, context.DeadlineExceeded)
		}, class: schemas.FailureClassTimeout, status: 504, failReason: schemas.RequestTimedOut},
		{name: "request deadline", err: expiredContextError, class: schemas.FailureClassTimeout, status: 504, failReason: schemas.RequestTimedOut},
		{name: "caller cancel", err: func(*testing.T) *schemas.BifrostError {
			return createBifrostError("request cancelled by caller", Ptr(499), Ptr(schemas.RequestCancelled), true)
		}, class: schemas.FailureClassCancelled, status: 499},
		{name: "context.Canceled from the provider call", err: func(*testing.T) *schemas.BifrostError { return cancelledError() }, class: schemas.FailureClassCancelled, status: 499},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := rotationTestContext()
			calls := 0
			handler := func(schemas.Key) (string, *schemas.BifrostError) {
				calls++
				return "", tc.err(t)
			}
			_, err := executeRequestWithRetries(ctx, createTestConfig(2, time.Millisecond, time.Millisecond), handler,
				poolKeyProvider([]schemas.Key{rotationKeyA, rotationKeyB}),
				schemas.ChatCompletionRequest, schemas.OpenAI, "gpt-4o", nil, NewDefaultLogger(schemas.LogLevelError))
			if err == nil {
				t.Fatal("the attempt Bifrost ended was served")
			}
			if calls != 1 {
				t.Fatalf("the attempt was tried %d times, want once: Bifrost's endings are not retried", calls)
			}
			requireEndedAttempt(t, ctx, tc.class, tc.status, tc.failReason)
		})
	}
}

// A cancellation during the backoff after a refusal ends the request before another attempt is
// dispatched: the refused attempt keeps its own class and reason, and no record is added for the
// attempt that never ran.
func TestExecuteRequestWithRetries_CancelDuringBackoffKeepsTheRefusal(t *testing.T) {
	parent, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx := schemas.NewBifrostContext(parent, schemas.NoDeadline)
	ctx.SetValue(schemas.BifrostContextKeyTracer, &schemas.NoOpTracer{})
	handler := func(schemas.Key) (string, *schemas.BifrostError) {
		// The caller leaves while the retry waits out its backoff.
		cancel()
		return "", providerError(429, "requests", "rate_limit_exceeded", "Rate limit reached for requests")
	}
	_, err := executeRequestWithRetries(ctx, createTestConfig(2, time.Second, time.Second), handler,
		poolKeyProvider([]schemas.Key{rotationKeyA, rotationKeyB}),
		schemas.ChatCompletionRequest, schemas.OpenAI, "gpt-4o", nil, NewDefaultLogger(schemas.LogLevelError))
	if err == nil || err.Error == nil || err.Error.Type == nil || *err.Error.Type != schemas.RequestCancelled {
		t.Fatalf("want the request to end as cancelled, got %+v", err)
	}
	got := attemptTrail(t, ctx, 1)[0]
	if got.FailureClass != schemas.FailureClassRateLimit || got.FailReason == nil || *got.FailReason != "rate_limit_error" {
		t.Fatalf("the refused attempt should keep rate_limit / rate_limit_error, got %+v", got)
	}
}

// A TTFT miss keeps the ttft_timeout settleAttemptAbort wrote and gains the timeout class and its
// status. A record already classified is left alone, and an error Bifrost did not end is not stamped.
func TestStampEndedAttempt(t *testing.T) {
	trailWith := func(record schemas.KeyAttemptRecord) *schemas.BifrostContext {
		ctx := rotationTestContext()
		ctx.SetValue(schemas.BifrostContextKeyAttemptTrail, []schemas.KeyAttemptRecord{record})
		return ctx
	}
	t.Run("a TTFT miss keeps its reason", func(t *testing.T) {
		ctx := trailWith(schemas.KeyAttemptRecord{KeyID: rotationKeyA.ID, FailReason: Ptr(schemas.FirstTokenTimeoutErrorCode)})
		stampEndedAttempt(ctx, providerUtils.NewFirstTokenTimeoutError(time.Second))
		requireEndedAttempt(t, ctx, schemas.FailureClassTimeout, 504, schemas.FirstTokenTimeoutErrorCode)
	})
	t.Run("a classified record is left alone", func(t *testing.T) {
		ctx := trailWith(schemas.KeyAttemptRecord{KeyID: rotationKeyA.ID, FailureClass: schemas.FailureClassRateLimit, StatusCode: Ptr(429), FailReason: Ptr("rate_limit_error")})
		stampEndedAttempt(ctx, cancelledError())
		requireEndedAttempt(t, ctx, schemas.FailureClassRateLimit, 429, "rate_limit_error")
	})
	t.Run("another Bifrost error is not stamped", func(t *testing.T) {
		ctx := trailWith(schemas.KeyAttemptRecord{KeyID: rotationKeyA.ID})
		stampEndedAttempt(ctx, createBifrostError("tracer not found in context", nil, nil, true))
		if got := attemptTrail(t, ctx, 1)[0]; got.FailureClass != "" || got.StatusCode != nil || got.FailReason != nil {
			t.Fatalf("an internal error was stamped: %+v", got)
		}
	})
}
