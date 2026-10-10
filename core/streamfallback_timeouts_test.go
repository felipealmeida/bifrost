package bifrost

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	schemas "github.com/maximhq/bifrost/core/schemas"
)

// Timeout design under test (scaled down: prod local primary is idle 300 s /
// request 330 s, cloud providers 45-60 s / 120-240 s).
//
//   - The future lpcache router sends neither ": prefill" comments nor early
//     200 headers. Two timers then bound a silent primary:
//   - DefaultRequestTimeoutInSeconds: the wait for response HEADERS only. A
//     healthy streaming body is not capped by it.
//   - StreamIdleTimeoutInSeconds: starts when the response body reader exists
//     (headers received) and is reset by any non-comment SSE byte, including
//     role-only chunks. SSE comment lines (": prefill") and blank lines do NOT
//     reset it. It is both the "time to first real data" limit and the
//     mid-stream stall limit once headers are out.
//   - Before the first content/finish/usage chunk a failure falls back; after
//     it, the client only sees an error chunk (no replay).
//
// Reuses helpers from streamfallback_matrix_test.go and streamfallback_test.go.

var (
	timeoutPrimaryIdleSec    = 2
	timeoutPrimaryRequestSec = 3
)

var tmOK = strings.Join(matrixOKStream, "")

func tmContent(s string) string {
	return matrixData(fmt.Sprintf(`{"id":"c1","object":"chat.completion.chunk","choices":[{"index":0,"delta":{"content":%q}}]}`, s))
}

// tmStep is one scripted action of a primary response: wait, then optionally
// send the SSE headers, then optionally write data.
type tmStep struct {
	wait    time.Duration
	headers bool
	data    string
}

// tmScript plays the steps, then either goes silent until released (hold) or
// ends the body cleanly.
func tmScript(hold bool, steps ...tmStep) func(<-chan struct{}) http.HandlerFunc {
	return func(stop <-chan struct{}) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			for _, s := range steps {
				if s.wait > 0 {
					select {
					case <-time.After(s.wait):
					case <-stop:
						return
					case <-r.Context().Done():
						return
					}
				}
				if s.headers {
					matrixSSEHeaders(w)
				}
				if s.data != "" {
					matrixWrite(w, s.data)
				}
			}
			if hold {
				select {
				case <-stop:
				case <-r.Context().Done():
				}
			}
		}
	}
}

// tmComments sends the SSE headers and parts, then a ": prefill" comment every
// gap seconds until released.
func tmComments(gap float64, parts ...string) func(<-chan struct{}) http.HandlerFunc {
	return func(stop <-chan struct{}) http.HandlerFunc {
		return matrixCommentsForever(stop, s(gap), parts...)
	}
}

// newTimeoutHarness is newMatrixHarness with the primary timers of this file.
func newTimeoutHarness(t *testing.T, primary http.HandlerFunc, stop chan struct{}) *matrixHarness {
	t.Helper()
	h := &matrixHarness{stop: stop}
	primarySrv := httptest.NewServer(primary)
	t.Cleanup(primarySrv.Close)
	fallbackSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h.fallbackHits.Add(1)
		_, _ = io.Copy(io.Discard, r.Body)
		anthropicMessagesHandler()(w, r)
	}))
	t.Cleanup(fallbackSrv.Close)

	account := NewMockAccount()
	account.AddProviderWithBaseURL(schemas.OpenAI, 1, 1, primarySrv.URL)
	account.AddProviderWithBaseURL(schemas.Anthropic, 1, 1, fallbackSrv.URL)
	account.configs[schemas.OpenAI].NetworkConfig.StreamIdleTimeoutInSeconds = timeoutPrimaryIdleSec
	account.configs[schemas.OpenAI].NetworkConfig.DefaultRequestTimeoutInSeconds = timeoutPrimaryRequestSec
	account.configs[schemas.OpenAI].NetworkConfig.MaxRetries = 0
	account.configs[schemas.Anthropic].NetworkConfig.MaxRetries = 0
	account.SetKeysForProvider(schemas.OpenAI, []schemas.Key{
		{ID: "primary-key", Value: *schemas.NewSecretVar("sk-primary"), Models: schemas.WhiteList{"*"}, Weight: 100},
	})
	account.SetKeysForProvider(schemas.Anthropic, []schemas.Key{
		{ID: "fallback-key", Value: *schemas.NewSecretVar("sk-fallback"), Models: schemas.WhiteList{"*"}, Weight: 100},
	})
	h.client = newStreamTestClient(t, account)
	t.Cleanup(func() { close(stop) }) // runs before client.Shutdown and server Close (LIFO)
	return h
}

type timeoutCase struct {
	name         string
	serve        func(stop <-chan struct{}) http.HandlerFunc
	fallbackHits int32
	content      string // exact
	errs         int    // exact number of error chunks
	minElapsed   time.Duration
	maxElapsed   time.Duration
}

func s(d float64) time.Duration { return time.Duration(d * float64(time.Second)) }

var timeoutCases = []timeoutCase{
	// Silence once headers are out: the idle timer (2 s) is the limit; no content yet, so fall back.
	{name: "silent_after_headers_falls_back", serve: tmScript(true, tmStep{headers: true}),
		fallbackHits: 1, content: "hello", minElapsed: s(1.8), maxElapsed: s(5)},
	{name: "silent_after_role_falls_back", serve: tmScript(true, tmStep{headers: true, data: matrixData(matrixRole)}),
		fallbackHits: 1, content: "hello", minElapsed: s(1.8), maxElapsed: s(5)},
	// Server accepts and sends nothing: only the request timeout (3 s) can bound this.
	{name: "no_headers_at_all_falls_back_via_request_timeout", serve: tmScript(true),
		fallbackHits: 1, content: "hello", minElapsed: s(2.8), maxElapsed: s(6)},
	// Headers at 1 s and 1.6 s of role-only data, real data at 5 s. Every byte resets
	// idle, so the limit is last byte (1.6 s) + idle (2 s) = ~3.6 s, not first byte + idle.
	{name: "preamble_bytes_reset_idle_then_stall_falls_back", serve: tmScript(false,
		tmStep{wait: s(1), headers: true, data: matrixData(matrixRole)},
		tmStep{wait: s(0.6), data: matrixData(matrixRole)},
		tmStep{wait: s(3.4), data: tmOK}),
		fallbackHits: 1, content: "hello", minElapsed: s(3.3), maxElapsed: s(4.9)},
	// Early headers, first data 1.6 s later: under the 2 s idle limit, no fallback.
	{name: "slow_first_data_just_under_idle_does_not_fall_back", serve: tmScript(false,
		tmStep{headers: true}, tmStep{wait: s(1.6), data: tmOK}),
		fallbackHits: 0, content: "Hello", minElapsed: s(1.5), maxElapsed: s(5)},
	// Early headers, first data 5 s later (2.5x idle): idle fires at ~2 s; late data is not delivered.
	{name: "slow_first_data_over_idle_falls_back", serve: tmScript(false,
		tmStep{headers: true}, tmStep{wait: s(5), data: tmOK}),
		fallbackHits: 1, content: "hello", minElapsed: s(1.8), maxElapsed: s(4.5)},
	// No early headers: the 2 s idle timer does not exist yet. Headers at 1 s are under both limits.
	{name: "late_headers_1s_does_not_fall_back", serve: tmScript(false,
		tmStep{wait: s(1), headers: true, data: tmOK}),
		fallbackHits: 0, content: "Hello", minElapsed: s(0.9), maxElapsed: s(5)},
	// No early headers, headers at 2.5 s: above idle (2 s) but under request (3 s). Idle does not
	// apply before headers, so the primary wins. Time-to-first-data is bounded by the REQUEST
	// timeout, not idle, for a router that sends no early headers.
	{name: "late_headers_between_idle_and_request_does_not_fall_back", serve: tmScript(false,
		tmStep{wait: s(2.5), headers: true, data: tmOK}),
		fallbackHits: 0, content: "Hello", minElapsed: s(2.4), maxElapsed: s(6)},
	// Chunks every 1 s (< idle 2 s) for ~8 s (> request 3 s, > 3x idle): healthy, not capped.
	{name: "trickle_longer_than_request_timeout_is_not_capped", serve: tmScript(false,
		tmStep{headers: true, data: matrixData(matrixRole)},
		tmStep{wait: s(1), data: tmContent("a")}, tmStep{wait: s(1), data: tmContent("b")},
		tmStep{wait: s(1), data: tmContent("c")}, tmStep{wait: s(1), data: tmContent("d")},
		tmStep{wait: s(1), data: tmContent("e")}, tmStep{wait: s(1), data: tmContent("f")},
		tmStep{wait: s(1), data: tmContent("g")},
		tmStep{wait: s(1), data: matrixData(matrixFinish) + matrixData(matrixUsage) + matrixDone}),
		fallbackHits: 0, content: "abcdefg", minElapsed: s(7.5), maxElapsed: s(14)},
	// Content already out, then silence: idle fires, no replay, exactly one error chunk.
	{name: "stall_after_content_is_not_replayed", serve: tmScript(true,
		tmStep{headers: true, data: matrixData(matrixRole) + matrixData(matrixHel)}),
		fallbackHits: 0, content: "Hel", errs: 1, minElapsed: s(1.8), maxElapsed: s(5)},
	// Comments every 0.5 s and nothing else: comments do not reset idle, so fall back at ~2 s.
	{name: "comments_only_fall_back_at_idle", serve: tmComments(0.5),
		fallbackHits: 1, content: "hello", minElapsed: s(1.8), maxElapsed: s(4)},
	// Role chunk, then only comments: idle counts from the role chunk, fall back at ~2 s.
	{name: "comments_after_role_fall_back_at_idle", serve: tmComments(0.5, matrixData(matrixRole)),
		fallbackHits: 1, content: "hello", minElapsed: s(1.8), maxElapsed: s(4)},
	// Content, then only comments: idle fires at ~2 s, no replay, exactly one error chunk.
	{name: "comments_after_content_error_without_replay", serve: tmComments(0.5, matrixData(matrixRole)+matrixData(matrixHel)),
		fallbackHits: 0, content: "Hel", errs: 1, minElapsed: s(1.8), maxElapsed: s(4)},
	// Comments between real chunks (data every 1 s, CRLF framing) for ~6 s: healthy, not cut.
	{name: "data_with_comments_long_stream_is_not_cut", serve: tmScript(false,
		tmStep{headers: true, data: matrixData(matrixRole)},
		tmStep{wait: s(0.5), data: matrixComment}, tmStep{wait: s(0.5), data: "data: " + `{"id":"c1","object":"chat.completion.chunk","choices":[{"index":0,"delta":{"content":"a"}}]}` + "\r\n\r\n"},
		tmStep{wait: s(0.5), data: ":\r\n\r\n"}, tmStep{wait: s(0.5), data: tmContent("b")},
		tmStep{wait: s(0.5), data: matrixComment}, tmStep{wait: s(0.5), data: tmContent("c")},
		tmStep{wait: s(0.5), data: matrixComment}, tmStep{wait: s(0.5), data: tmContent("d")},
		tmStep{wait: s(0.5), data: matrixComment}, tmStep{wait: s(0.5), data: tmContent("e")},
		tmStep{wait: s(0.5), data: matrixComment}, tmStep{wait: s(0.5), data: matrixData(matrixFinish) + matrixData(matrixUsage) + matrixDone}),
		fallbackHits: 0, content: "abcde", minElapsed: s(5.5), maxElapsed: s(10)},
	// A comment does not commit output: a string error still triggers fallback.
	{name: "sse_error_string_after_comment_falls_back", serve: tmScript(false,
		tmStep{headers: true, data: matrixComment}, tmStep{wait: s(0.1), data: matrixData(matrixErrString) + matrixDone}),
		fallbackHits: 1, content: "hello"},
}

func TestTimeoutPrimaryLimits(t *testing.T) {
	for _, c := range timeoutCases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			stop := make(chan struct{})
			h := newTimeoutHarness(t, c.serve(stop), stop)
			start := time.Now()
			out := runStream(t, h)
			elapsed := time.Since(start)
			t.Logf("elapsed=%v fallbackHits=%d content=%q errs=%v syncErr=%v",
				elapsed.Round(time.Millisecond), h.fallbackHits.Load(), out.content, out.errs, out.syncErr != nil)
			var problems []string
			if out.syncErr != nil {
				problems = append(problems, "synchronous error: "+out.syncErr.Error.Message)
			}
			if got := h.fallbackHits.Load(); got != c.fallbackHits {
				problems = append(problems, fmt.Sprintf("fallback hits = %d, want %d", got, c.fallbackHits))
			}
			if out.content != c.content {
				problems = append(problems, fmt.Sprintf("content = %q, want %q", out.content, c.content))
			}
			if len(out.errs) != c.errs {
				problems = append(problems, fmt.Sprintf("error chunks = %v, want %d", out.errs, c.errs))
			}
			if c.maxElapsed > 0 && (elapsed < c.minElapsed || elapsed > c.maxElapsed) {
				problems = append(problems, fmt.Sprintf("elapsed = %v, want within [%v, %v]", elapsed, c.minElapsed, c.maxElapsed))
			}
			for _, p := range problems {
				t.Error(p)
			}
		})
	}
}

// The idle timer must actually close the primary connection, not just stop
// listening: a late first chunk can then never reach the client.
func TestTimeoutIdleClosesPrimaryConnection(t *testing.T) {
	gone := make(chan struct{})
	stop := make(chan struct{})
	h := newTimeoutHarness(t, func(w http.ResponseWriter, r *http.Request) {
		matrixSSEHeaders(w)
		select {
		case <-r.Context().Done():
			close(gone)
		case <-stop:
		}
	}, stop)
	out := runStream(t, h)
	if out.content != "hello" || len(out.errs) != 0 || h.fallbackHits.Load() != 1 {
		t.Fatalf("content=%q errs=%v hits=%d, want fallback answer", out.content, out.errs, h.fallbackHits.Load())
	}
	select {
	case <-gone:
	case <-time.After(3 * time.Second):
		t.Fatal("primary connection was still open 3 s after the idle timeout fired")
	}
}

// Comment-only keepalives must not keep the primary connection open: when idle
// fires, the primary sees its request context end (connection closed).
func TestTimeoutCommentsOnlyClosesPrimaryConnection(t *testing.T) {
	gone := make(chan struct{})
	stop := make(chan struct{})
	h := newTimeoutHarness(t, func(w http.ResponseWriter, r *http.Request) {
		defer close(gone)
		matrixCommentsForever(stop, 500*time.Millisecond)(w, r)
	}, stop)
	out := runStream(t, h)
	if out.content != "hello" || len(out.errs) != 0 || h.fallbackHits.Load() != 1 {
		t.Fatalf("content=%q errs=%v hits=%d, want fallback answer", out.content, out.errs, h.fallbackHits.Load())
	}
	select {
	case <-gone:
	case <-time.After(3 * time.Second):
		t.Fatal("primary connection was still open 3 s after the idle timeout fired")
	}
}

// Caller cancel during a comment-only stream (before idle fires) ends the call
// promptly, without falling back.
func TestTimeoutCommentsOnlyCallerCancel(t *testing.T) {
	stop := make(chan struct{})
	started := make(chan struct{})
	gone := make(chan struct{})
	h := newTimeoutHarness(t, func(w http.ResponseWriter, r *http.Request) {
		matrixSSEHeaders(w)
		matrixWrite(w, matrixComment)
		close(started)
		defer close(gone)
		select {
		case <-r.Context().Done():
		case <-stop:
		}
	}, stop)
	ctx, cancel := schemas.NewBifrostContextWithCancel(context.Background())
	defer cancel()
	returned := make(chan struct{})
	go func() {
		defer close(returned)
		stream, err := h.client.ChatCompletionStreamRequest(ctx, matrixRequest())
		if err == nil && stream != nil {
			drainChatStream(stream)
		}
	}()
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("primary did not start")
	}
	cancel()
	select {
	case <-returned:
	case <-time.After(3 * time.Second):
		t.Fatal("call did not return within 3 s after context cancel")
	}
	if got := h.fallbackHits.Load(); got != 0 {
		t.Errorf("fallback hits = %d after caller cancel, want 0", got)
	}
	select {
	case <-gone:
	case <-time.After(3 * time.Second):
		t.Fatal("primary connection stayed open after caller cancel")
	}
}

type hitLog struct {
	mu    sync.Mutex
	order []string
}

func (l *hitLog) add(name string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.order = append(l.order, name)
}

func (l *hitLog) get() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.order...)
}

// idleChain is a local (OpenAI) -> cloud (Anthropic) -> second (Groq) chain.
// Local goes silent after headers. Cloud either goes silent after headers or
// answers after cloudDelay. Second always answers at once.
type idleChain struct {
	name       string
	localIdle  int           // StreamIdleTimeoutInSeconds; 0 = provider default
	cloudIdle  int           // StreamIdleTimeoutInSeconds; 0 = provider default
	callerIdle any           // value put on the ctx by the SDK caller; nil = not set
	cloudDelay time.Duration // 0 = cloud stays silent after headers
	wantHits   string
	minElapsed time.Duration
	maxElapsed time.Duration
}

func runIdleChain(t *testing.T, c idleChain) {
	t.Helper()
	var hits hitLog
	stop := make(chan struct{})
	wait := func(r *http.Request, d time.Duration) bool {
		select {
		case <-time.After(d):
			return true
		case <-stop:
		case <-r.Context().Done():
		}
		return false
	}
	local := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.add("local")
		matrixSSEHeaders(w)
		wait(r, time.Hour)
	}))
	t.Cleanup(local.Close)
	cloud := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.add("cloud")
		_, _ = io.Copy(io.Discard, r.Body)
		matrixSSEHeaders(w)
		if c.cloudDelay == 0 {
			wait(r, time.Hour)
			return
		}
		if wait(r, c.cloudDelay) {
			anthropicMessagesHandler()(w, r)
		}
	}))
	t.Cleanup(cloud.Close)
	second := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.add("second")
		matrixSend(matrixData(matrixRole), tmContent("hello"), matrixData(matrixFinish), matrixData(matrixUsage), matrixDone)(w, r)
	}))
	t.Cleanup(second.Close)

	account := NewMockAccount()
	account.AddProviderWithBaseURL(schemas.OpenAI, 1, 1, local.URL)
	account.AddProviderWithBaseURL(schemas.Anthropic, 1, 1, cloud.URL)
	account.AddProviderWithBaseURL(schemas.Groq, 1, 1, second.URL)
	account.configs[schemas.OpenAI].NetworkConfig.StreamIdleTimeoutInSeconds = c.localIdle
	account.configs[schemas.Anthropic].NetworkConfig.StreamIdleTimeoutInSeconds = c.cloudIdle
	for _, p := range []schemas.ModelProvider{schemas.OpenAI, schemas.Anthropic, schemas.Groq} {
		account.configs[p].NetworkConfig.MaxRetries = 0
		account.SetKeysForProvider(p, []schemas.Key{
			{ID: string(p) + "-key", Value: *schemas.NewSecretVar("sk-test"), Models: schemas.WhiteList{"*"}, Weight: 100},
		})
	}
	client := newStreamTestClient(t, account)
	t.Cleanup(func() { close(stop) })

	req := matrixRequest()
	req.Fallbacks = []schemas.Fallback{
		{Provider: schemas.Anthropic, Model: "claude-3-5-haiku-20241022"},
		{Provider: schemas.Groq, Model: "llama-3.1-8b-instant"},
	}
	ctx := schemas.NewBifrostContext(context.Background(), time.Now().Add(30*time.Second))
	if c.callerIdle != nil {
		ctx.SetValue(schemas.BifrostContextKeyStreamIdleTimeout, c.callerIdle)
	}
	start := time.Now()
	stream, err := client.ChatCompletionStreamRequest(ctx, req)
	if err != nil {
		t.Fatalf("chain failed (hits %v): %s", hits.get(), err.Error.Message)
	}
	content, errs := drainChatStream(stream)
	elapsed := time.Since(start)
	t.Logf("elapsed=%v hits=%v content=%q errs=%v", elapsed.Round(time.Millisecond), hits.get(), content, errs)
	if got := strings.Join(hits.get(), ","); got != c.wantHits {
		t.Errorf("hit order = %s, want %s", got, c.wantHits)
	}
	if content != "hello" || len(errs) != 0 {
		t.Errorf("content = %q errs = %v, want \"hello\" and no errors", content, errs)
	}
	if elapsed < c.minElapsed || elapsed > c.maxElapsed {
		t.Errorf("elapsed = %v, want within [%v, %v]", elapsed, c.minElapsed, c.maxElapsed)
	}
}

// Each fallback must apply its own provider's idle limit unless the SDK caller
// set a positive one. Leak signatures are in each case's comment.
func TestTimeoutChainUsesEachProvidersOwnLimits(t *testing.T) {
	cases := []idleChain{
		// 3 + 1 = 4 s. Local leaking into cloud gives 6 s.
		{name: "local3_cloud1", localIdle: 3, cloudIdle: 1,
			wantHits: "local,cloud,second", minElapsed: s(3.8), maxElapsed: s(5.5)},
		// 1 + 3 = 4 s. Local leaking into cloud gives 2 s.
		{name: "local1_cloud3", localIdle: 1, cloudIdle: 3,
			wantHits: "local,cloud,second", minElapsed: s(3.8), maxElapsed: s(5.5)},
		// Local idle 2 s, cloud idle 5 s answers after 3 s: ~5 s, served by cloud.
		// Local leaking into cloud cuts cloud at 2 s and reaches second at ~4 s.
		{name: "local2_cloud5_slow3_served_by_cloud", localIdle: 2, cloudIdle: 5, cloudDelay: s(3),
			wantHits: "local,cloud", minElapsed: s(4.8), maxElapsed: s(6.5)},
		// Cloud has no configured idle: the default (120 s) applies, so a 3 s first byte is fine.
		{name: "local2_cloud_default_slow3_served_by_cloud", localIdle: 2, cloudDelay: s(3),
			wantHits: "local,cloud", minElapsed: s(4.8), maxElapsed: s(6.5)},
		// Explicit SDK caller value 2 s beats both provider configs (1 s): 2 + 2 = 4 s.
		// Caller value dropped on fallback gives 3 s; ignored everywhere gives 2 s.
		{name: "caller2_overrides_local1_cloud1", localIdle: 1, cloudIdle: 1, callerIdle: s(2),
			wantHits: "local,cloud,second", minElapsed: s(3.8), maxElapsed: s(5.5)},
		// Zero, negative and wrong-type caller values are ignored: same as local3_cloud1.
		{name: "caller_zero_is_ignored", localIdle: 3, cloudIdle: 1, callerIdle: time.Duration(0),
			wantHits: "local,cloud,second", minElapsed: s(3.8), maxElapsed: s(5.5)},
		{name: "caller_negative_is_ignored", localIdle: 3, cloudIdle: 1, callerIdle: -s(5),
			wantHits: "local,cloud,second", minElapsed: s(3.8), maxElapsed: s(5.5)},
		{name: "caller_wrong_type_is_ignored", localIdle: 3, cloudIdle: 1, callerIdle: 7,
			wantHits: "local,cloud,second", minElapsed: s(3.8), maxElapsed: s(5.5)},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			runIdleChain(t, c)
		})
	}
}
