package utils

import (
	"context"
	"errors"
	"fmt"
	"io"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/maximhq/bifrost/core/schemas"
	"github.com/valyala/fasthttp"
)

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

// syncedPanicBody is a controlled io.ReadCloser used to deterministically
// reproduce the race in SetupStreamCancellation.
//
// Read blocks until Close() is called, then panics — simulating what fasthttp's
// streaming body does when the underlying TCP connection is force-closed.
//
// Close() triggers the panic (by closing panicTrigger) and then blocks on
// allowReturn until the test signals it. This keeps SetupStreamCancellation
// stuck inside Close() so BifrostContextKeyConnectionClosed is guaranteed to
// be unset when idleTimeoutReader.Read's recover block runs — the exact race
// window that exists in the current code.
type syncedPanicBody struct {
	panicTrigger chan struct{}
	allowReturn  chan struct{}
	closeOnce    sync.Once
	closes       atomic.Int32
}

// closeCount reports how many times the stream was closed, so a test can assert
// that nothing closed it rather than only that a read stayed blocked.
func (s *syncedPanicBody) closeCount() int { return int(s.closes.Load()) }

func newSyncedPanicBody() *syncedPanicBody {
	return &syncedPanicBody{
		panicTrigger: make(chan struct{}),
		allowReturn:  make(chan struct{}),
	}
}

func (s *syncedPanicBody) Read(_ []byte) (int, error) {
	<-s.panicTrigger
	panic("use of closed network connection")
}

func (s *syncedPanicBody) Close() error {
	s.closes.Add(1)
	s.closeOnce.Do(func() { close(s.panicTrigger) })
	<-s.allowReturn
	return nil
}

var errPanicReader = errors.New("panic reader called")

type panicReader struct{}

func (panicReader) Read([]byte) (int, error) {
	panic(errPanicReader)
}

// panicAfterOnceReader returns one byte on the first read, then panics like a
// fasthttp streaming body whose connection was force-closed. It exists to arm
// the idle timer (the first read is useful) before the panic path is tested.
type panicAfterOnceReader struct{ done atomic.Bool }

func (r *panicAfterOnceReader) Read(p []byte) (int, error) {
	if r.done.CompareAndSwap(false, true) {
		return copy(p, []byte("x")), nil
	}
	panic("use of closed network connection")
}

// timerPanicCloser mimics fasthttp's streaming body when the underlying
// connection has already been released to / reused from the pool: CloseWithError
// nil-derefs in (*HostClient).CloseConn and panics. It implements
// streamCloserWithError (not io.Closer) so closeBodyStream takes the
// CloseWithError branch — the path the idle timer hits.
//
// called is closed the instant CloseWithError is entered (just before the
// panic), so a test can deterministically wait for the guarded path to be
// exercised rather than rely on a fixed sleep — which a slow runner could
// outrun, letting cleanup stop the timer before it ever fired and passing the
// test without touching the recover.
type timerPanicCloser struct {
	called chan struct{}
}

func newTimerPanicCloser() *timerPanicCloser {
	return &timerPanicCloser{called: make(chan struct{})}
}

func (*timerPanicCloser) Read([]byte) (int, error) { return 0, io.EOF }

func (c *timerPanicCloser) CloseWithError(error) error {
	close(c.called)
	panic("simulated fasthttp CloseConn nil-deref")
}

// captureLogger records Debug messages so a test can assert that a recovered
// panic value is logged (not silently swallowed). It embeds noopLogger to
// satisfy the rest of the schemas.Logger interface.
type captureLogger struct {
	noopLogger
	mu   sync.Mutex
	msgs []string
}

func (c *captureLogger) Debug(format string, args ...any) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.msgs = append(c.msgs, fmt.Sprintf(format, args...))
}

func (c *captureLogger) contains(sub string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, m := range c.msgs {
		if strings.Contains(m, sub) {
			return true
		}
	}
	return false
}

type blockingCloserSpy struct {
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func newBlockingCloserSpy() *blockingCloserSpy {
	return &blockingCloserSpy{
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
}

func (c *blockingCloserSpy) Read([]byte) (int, error) { return 0, io.EOF }

func (c *blockingCloserSpy) Close() error {
	c.once.Do(func() { close(c.started) })
	<-c.release
	return nil
}

func (c *blockingCloserSpy) unblock() {
	close(c.release)
}

// readCloserSpy implements io.ReadCloser and records how many times Close() was called.
type readCloserSpy struct {
	mu     sync.Mutex
	closed int
}

func (c *readCloserSpy) Read([]byte) (int, error) { return 0, io.EOF }

func (c *readCloserSpy) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closed++
	return nil
}

func (c *readCloserSpy) closeCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closed
}

// zeroThenBlockReader returns (0, nil) on the first read, then blocks forever.
type zeroThenBlockReader struct {
	first  atomic.Bool
	pipeRd *io.PipeReader
}

func (r *zeroThenBlockReader) Read(p []byte) (int, error) {
	if r.first.CompareAndSwap(false, true) {
		return 0, nil // zero-byte read
	}
	// block until pipe is closed
	return r.pipeRd.Read(p)
}

// ---------------------------------------------------------------------------
// tests
// ---------------------------------------------------------------------------

func TestIdleTimeoutReader_NormalRead(t *testing.T) {
	t.Parallel()
	pr, pw := io.Pipe()
	defer pr.Close()

	// Use pr as bodyStream — closing pr unblocks reads.
	wrapped, cleanup := NewIdleTimeoutReader(pr, pr, 500*time.Millisecond, nil)
	defer cleanup()

	// Writer sends 5 chunks quickly.
	go func() {
		for i := 0; i < 5; i++ {
			time.Sleep(10 * time.Millisecond)
			pw.Write([]byte("chunk"))
		}
		pw.Close()
	}()

	buf := make([]byte, 64)
	var totalBytes int
	for {
		n, err := wrapped.Read(buf)
		totalBytes += n
		if err != nil {
			if err != io.EOF {
				t.Fatalf("unexpected error: %v", err)
			}
			break
		}
	}

	if totalBytes != 5*len("chunk") {
		t.Fatalf("expected %d bytes, got %d", 5*len("chunk"), totalBytes)
	}
}

// TestIdleTimeoutReader_SilenceBeforeFirstByteDoesNotFire replaces the old
// TestIdleTimeoutReader_TimeoutClosesStream, which asserted that a stream with
// zero bytes is killed after the timeout. The idle window measures the gap
// between useful chunks only: silence while waiting for the first token (upstream
// prefill) must not fire. Fails on the old code, which armed the timer at
// construction and closed the body after `timeout` of zero bytes.
func TestIdleTimeoutReader_SilenceBeforeFirstByteDoesNotFire(t *testing.T) {
	t.Parallel()
	const timeout = 50 * time.Millisecond
	pr, pw := io.Pipe()
	defer pw.Close()

	wrapped, cleanup := NewIdleTimeoutReader(pr, pr, timeout, nil)
	defer cleanup()

	// Silent for 3x the window (prefill), then two bytes within one window.
	go func() {
		time.Sleep(3 * timeout)
		if _, err := pw.Write([]byte("tok1")); err != nil {
			return
		}
		time.Sleep(timeout / 2)
		if _, err := pw.Write([]byte("tok2")); err != nil {
			return
		}
		pw.Close()
	}()

	buf := make([]byte, 64)
	var got []byte
	for {
		n, err := wrapped.Read(buf)
		got = append(got, buf[:n]...)
		if err != nil {
			if errors.Is(err, ErrStreamIdleTimeout) {
				t.Fatalf("stream closed with ErrStreamIdleTimeout after %d bytes; the first-token wait must not count against the idle window (got %q)", len(got), got)
			}
			if err != io.EOF {
				t.Fatalf("unexpected error: %v", err)
			}
			break
		}
	}
	if string(got) != "tok1tok2" {
		t.Fatalf("got %q, want %q", got, "tok1tok2")
	}
}

// TestIdleTimeoutReader_FiresAfterFirstByteThenSilence: the first byte arrives
// only after 2x the window — the old code had already closed the stream before
// it, so the byte was never delivered. After the byte, silence beyond the
// window must close the stream with ErrStreamIdleTimeout.
func TestIdleTimeoutReader_FiresAfterFirstByteThenSilence(t *testing.T) {
	t.Parallel()
	const timeout = 50 * time.Millisecond
	pr, pw := io.Pipe()
	defer pw.Close()

	wrapped, cleanup := NewIdleTimeoutReader(pr, pr, timeout, nil)
	defer cleanup()

	go func() {
		time.Sleep(2 * timeout)
		if _, err := pw.Write([]byte("tok")); err != nil {
			return
		}
		// Never closes pw: only the idle timeout can end the stream.
	}()

	buf := make([]byte, 64)
	var got []byte
	for {
		n, err := wrapped.Read(buf)
		got = append(got, buf[:n]...)
		if err != nil {
			if !errors.Is(err, ErrStreamIdleTimeout) {
				t.Fatalf("expected ErrStreamIdleTimeout after the silence following the first byte, got %v (bytes %q)", err, got)
			}
			break
		}
	}
	if string(got) != "tok" {
		t.Fatalf("first byte not delivered before the stream was closed: got %q, want %q", got, "tok")
	}
}

// TestIdleTimeoutReader_GapResetNeverFires: bytes spaced 0.6x the timeout apart
// for 5 intervals never fire. The first byte starts just past one full window,
// where the old (construction-armed) code had already fired and swallowed it.
func TestIdleTimeoutReader_GapResetNeverFires(t *testing.T) {
	t.Parallel()
	const timeout = 50 * time.Millisecond
	const chunks = 6
	pr, pw := io.Pipe()
	defer pw.Close()

	wrapped, cleanup := NewIdleTimeoutReader(pr, pr, timeout, nil)
	defer cleanup()

	go func() {
		start := time.Now()
		for i := 0; i < chunks; i++ {
			at := time.Duration(float64(timeout) * (1.05 + 0.6*float64(i)))
			if d := at - time.Since(start); d > 0 {
				time.Sleep(d)
			}
			if _, err := pw.Write([]byte("t")); err != nil {
				return
			}
		}
		pw.Close()
	}()

	buf := make([]byte, 64)
	var got []byte
	for {
		n, err := wrapped.Read(buf)
		got = append(got, buf[:n]...)
		if err != nil {
			if errors.Is(err, ErrStreamIdleTimeout) {
				t.Fatalf("stream closed with ErrStreamIdleTimeout after %d bytes; every gap was 0.6x the timeout", len(got))
			}
			if err != io.EOF {
				t.Fatalf("unexpected error: %v", err)
			}
			break
		}
	}
	if len(got) != chunks {
		t.Fatalf("got %d bytes, want %d (each byte must reset the timer)", len(got), chunks)
	}
}

func TestIdleTimeoutReader_TimeoutAfterPartialData(t *testing.T) {
	t.Parallel()
	pr, pw := io.Pipe()

	// 200ms idle timeout.
	wrapped, cleanup := NewIdleTimeoutReader(pr, pr, 200*time.Millisecond, nil)
	defer cleanup()

	// Writer sends 3 chunks then stops.
	go func() {
		for i := 0; i < 3; i++ {
			time.Sleep(20 * time.Millisecond)
			pw.Write([]byte("data"))
		}
		// stop writing — idle timeout should fire after 200ms and close pr
	}()

	buf := make([]byte, 64)
	chunksRead := 0
	for {
		n, err := wrapped.Read(buf)
		if n > 0 {
			chunksRead++
		}
		if err != nil {
			break
		}
	}

	if chunksRead != 3 {
		t.Fatalf("expected 3 chunks before timeout, got %d", chunksRead)
	}

	pw.Close()
}

func TestIdleTimeoutReader_ResetOnData(t *testing.T) {
	t.Parallel()
	pr, pw := io.Pipe()

	// 200ms timeout, but data arrives every 150ms — should never timeout.
	wrapped, cleanup := NewIdleTimeoutReader(pr, pr, 200*time.Millisecond, nil)
	defer cleanup()

	go func() {
		for i := 0; i < 5; i++ {
			time.Sleep(150 * time.Millisecond)
			pw.Write([]byte("ok"))
		}
		pw.Close()
	}()

	buf := make([]byte, 64)
	chunksRead := 0
	for {
		n, err := wrapped.Read(buf)
		if n > 0 {
			chunksRead++
		}
		if err != nil {
			if err != io.EOF {
				t.Fatalf("expected EOF after all chunks, got: %v", err)
			}
			break
		}
	}

	if chunksRead != 5 {
		t.Fatalf("expected 5 chunks (timer should reset), got %d", chunksRead)
	}
}

func TestIdleTimeoutReader_CleanupStopsTimer(t *testing.T) {
	t.Parallel()
	pr, pw := io.Pipe()
	defer pr.Close()
	defer pw.Close()

	spy := &readCloserSpy{}

	wrapped, cleanup := NewIdleTimeoutReader(strings.NewReader("x"), spy, 100*time.Millisecond, nil)
	// Arm the timer, then call cleanup immediately — timer should be stopped.
	if n, err := wrapped.Read(make([]byte, 8)); err != nil || n == 0 {
		t.Fatalf("arming read: n=%d err=%v, want one byte", n, err)
	}
	cleanup()

	// Wait well past the timeout duration.
	time.Sleep(250 * time.Millisecond)

	if spy.closeCount() != 0 {
		t.Fatalf("expected closer to NOT be called after cleanup, but was called %d times", spy.closeCount())
	}
}

func TestIdleTimeoutReader_DoubleCloseIsSafe(t *testing.T) {
	t.Parallel()
	spy := &readCloserSpy{}

	// strings.NewReader("x") provides the one useful byte that arms the
	// timer (the reader is not read in the old construction-armed design,
	// but the timer there fired on zero data).
	// Use spy as bodyStream — it implements both io.Reader and io.Closer.
	wrapped, cleanup := NewIdleTimeoutReader(strings.NewReader("x"), spy, 50*time.Millisecond, nil)
	defer cleanup()

	// Arm the timer with one useful read.
	if n, err := wrapped.Read(make([]byte, 8)); err != nil || n == 0 {
		t.Fatalf("arming read: n=%d err=%v, want one byte", n, err)
	}

	// Let the timer fire (closes spy via sync.Once).
	time.Sleep(100 * time.Millisecond)

	// Manually close again — should not panic.
	spy.Close()

	// sync.Once ensures the idle timer's close ran exactly once.
	// The manual close above adds another, so total should be 2
	// (the once.Do protects the timer path, not external callers).
	// The key guarantee: no panic.
	if spy.closeCount() < 1 {
		t.Fatal("expected at least one close call")
	}
}

// TestIdleTimeoutReader_ZeroBytesDoNotArmTimer: a zero-byte read must not arm
// the timer, so a stream that only ever returns (0, nil) before its first data
// byte cannot be killed. The old assertion here was the opposite: zero bytes
// did not *reset* the construction-armed timer, so the stream timed out.
func TestIdleTimeoutReader_ZeroBytesDoNotArmTimer(t *testing.T) {
	t.Parallel()
	pr, pw := io.Pipe()
	defer pw.Close()

	zr := &zeroThenBlockReader{pipeRd: pr}
	wrapped, cleanup := NewIdleTimeoutReader(zr, pr, 100*time.Millisecond, nil)
	defer cleanup()

	done := make(chan error, 1)
	go func() {
		buf := make([]byte, 64)
		// First read returns (0, nil); the next read blocks until the pipe
		// is closed (the test closes it, after proving nothing fired).
		for {
			_, err := wrapped.Read(buf)
			if err != nil {
				done <- err
				return
			}
		}
	}()

	// Zero bytes must not arm the timer: nothing may fire for 5x the window.
	select {
	case err := <-done:
		t.Fatalf("timer fired on zero-byte reads: read returned %v", err)
	case <-time.After(500 * time.Millisecond):
	}

	pw.Close()
	select {
	case err := <-done:
		if errors.Is(err, ErrStreamIdleTimeout) {
			t.Fatalf("stream reported ErrStreamIdleTimeout though the pipe was closed normally: %v", err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("read did not unblock after the pipe was closed")
	}
}

func TestIdleTimeoutReader_ErrorFromClosedPipe(t *testing.T) {
	t.Parallel()
	pr, pw := io.Pipe()
	defer pw.Close()

	// Use pr as bodyStream — closing pr makes Read return io.ErrClosedPipe.
	wrapped, cleanup := NewIdleTimeoutReader(pr, pr, 50*time.Millisecond, nil)
	defer cleanup()

	buf := make([]byte, 64)
	// One useful byte arms the timer; then the test closes the read end.
	go func() {
		time.Sleep(10 * time.Millisecond)
		if _, err := pw.Write([]byte("x")); err != nil {
			return
		}
	}()
	if n, err := wrapped.Read(buf); err != nil || n == 0 {
		t.Fatalf("first read: n=%d err=%v, want one byte", n, err)
	}
	pr.Close()
	_, err := wrapped.Read(buf)
	if err == nil {
		t.Fatal("expected error from closed pipe")
	}
	// The error should indicate the pipe was closed.
	if !errors.Is(err, io.ErrClosedPipe) && !errors.Is(err, io.EOF) {
		// Some implementations return io.ErrClosedPipe, others EOF.
		t.Logf("got error: %v (acceptable)", err)
	}
}

func TestIdleTimeoutReader_NilContextDoesNotPanic(t *testing.T) {
	t.Parallel()
	wrapped, cleanup := NewIdleTimeoutReader(&readCloserSpy{}, &readCloserSpy{}, time.Second, nil)
	defer cleanup()

	_, err := wrapped.Read(make([]byte, 1))
	if err != io.EOF {
		t.Fatalf("expected EOF, got %v", err)
	}
}

func TestIdleTimeoutReader_ClosedContextReturnsError(t *testing.T) {
	t.Parallel()
	ctx := schemas.NewBifrostContext(context.Background(), time.Time{})
	ctx.SetValue(schemas.BifrostContextKeyConnectionClosed, true)
	wrapped, cleanup := NewIdleTimeoutReader(panicReader{}, &readCloserSpy{}, time.Second, ctx)
	defer cleanup()

	_, err := wrapped.Read(make([]byte, 1))
	if !errors.Is(err, ErrStreamClosed) {
		t.Fatalf("expected ErrStreamClosed, got %v", err)
	}
}

func TestIdleTimeoutReader_RecoversReadPanicAfterTimeout(t *testing.T) {
	t.Parallel()
	// panicAfterOnceReader returns one byte (arming the timer) and then
	// panics on the next read, like a force-closed fasthttp connection.
	wrapped, cleanup := NewIdleTimeoutReader(&panicAfterOnceReader{}, &readCloserSpy{}, 10*time.Millisecond, nil)
	defer cleanup()

	if n, err := wrapped.Read(make([]byte, 8)); err != nil || n == 0 {
		t.Fatalf("arming read: n=%d err=%v, want one byte", n, err)
	}
	time.Sleep(50 * time.Millisecond)

	_, err := wrapped.Read(make([]byte, 1))
	if !errors.Is(err, ErrStreamIdleTimeout) {
		t.Fatalf("expected ErrStreamIdleTimeout, got %v", err)
	}
}

// TestIdleTimeoutReader_RecoversCloseStreamPanicOnTimerFire verifies that a
// panic raised by closeBodyStream WHEN THE IDLE TIMER FIRES — e.g. fasthttp's
// CloseWithError nil-dereffing in (*HostClient).CloseConn because the stream's
// connection was already released to / reused from the pool (an orphaned timer
// on a completed stream) — is recovered inside the timer goroutine and does not
// crash the process.
//
// This is the timer-callback counterpart to RecoversReadPanicAfterTimeout
// (#3677), which guarded the Read() path but not the AfterFunc's own
// closeBodyStream call. Without the recover in the AfterFunc, the panic runs in
// the timer goroutine, is unrecoverable by callers, and takes the whole process
// down (observed crashing a router under sustained streaming load).
func TestIdleTimeoutReader_RecoversCloseStreamPanicOnTimerFire(t *testing.T) {
	t.Parallel()
	body := newTimerPanicCloser()
	// The reader supplies one useful byte so the timer arms; without it the
	// lazy timer would never start and the callback would never run.
	wrapped, cleanup := NewIdleTimeoutReader(strings.NewReader("x"), body, 10*time.Millisecond, nil)
	if n, err := wrapped.Read(make([]byte, 8)); err != nil || n == 0 {
		t.Fatalf("arming read: n=%d err=%v, want one byte", n, err)
	}

	// Wait until the timer goroutine has actually entered CloseWithError (about to
	// panic). This proves the guarded path was exercised; a fixed sleep could be
	// outrun by a slow runner, letting cleanup stop the timer before it fired and
	// passing the test without ever touching the recover.
	select {
	case <-body.called:
	case <-time.After(2 * time.Second):
		t.Fatal("idle timer never fired CloseWithError within 2s")
	}

	// cleanup() blocks on timerDone, closed only after the (now-recovered) timer
	// callback returns — so reaching the end proves the panic was recovered in the
	// timer goroutine. Without the recover, the process would already have crashed.
	cleanup()
}

// TestIdleTimeoutReader_LogsRecoveredTimerPanic verifies that the recovered
// panic value is logged (not silently swallowed), so an unexpected future
// panic on this path leaves a forensic trace. Not parallel: it swaps the
// package-global logger and restores it via t.Cleanup before parallel tests
// resume.
func TestIdleTimeoutReader_LogsRecoveredTimerPanic(t *testing.T) {
	capLog := &captureLogger{}
	prev := getLogger()
	SetLogger(capLog)
	t.Cleanup(func() { SetLogger(prev) })

	body := newTimerPanicCloser()
	wrapped, cleanup := NewIdleTimeoutReader(strings.NewReader("x"), body, 10*time.Millisecond, nil)
	if n, err := wrapped.Read(make([]byte, 8)); err != nil || n == 0 {
		t.Fatalf("arming read: n=%d err=%v, want one byte", n, err)
	}
	select {
	case <-body.called:
	case <-time.After(2 * time.Second):
		t.Fatal("idle timer never fired CloseWithError within 2s")
	}
	cleanup() // blocks until the timer callback (recover + log) has returned

	if !capLog.contains("idle-timeout timer") || !capLog.contains("nil-deref") {
		t.Fatalf("expected recovered panic to be logged; got %v", capLog.msgs)
	}
}

func TestIdleTimeoutReader_CleanupWaitsForRunningTimerCallback(t *testing.T) {
	t.Parallel()
	body := newBlockingCloserSpy()
	wrapped, cleanup := NewIdleTimeoutReader(strings.NewReader("x"), body, 10*time.Millisecond, nil)
	if n, err := wrapped.Read(make([]byte, 8)); err != nil || n == 0 {
		t.Fatalf("arming read: n=%d err=%v, want one byte", n, err)
	}

	select {
	case <-body.started:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("expected idle timeout callback to start closing the body")
	}

	cleanupDone := make(chan struct{})
	go func() {
		cleanup()
		close(cleanupDone)
	}()

	select {
	case <-cleanupDone:
		t.Fatal("cleanup returned before the running timer callback finished")
	case <-time.After(50 * time.Millisecond):
	}

	body.unblock()

	select {
	case <-cleanupDone:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("cleanup did not return after timer callback finished")
	}
}

// TestSetupStreamCancellation_NoPanicOnCancelledContext verifies that
// cancelling a context unblocks a read that is already in flight, and that the
// reader surfaces ErrStreamClosed rather than letting a panic escape.
//
// The body's Close() blocks on allowReturn, so the flag is guaranteed to be
// unset when idleTimeoutReader.Read's recover block runs - the worst-case
// ordering.
//
// Closing under an active reader is only safe on a fasthttp carrying upstream
// commit fb3b29e (#2353), where clientStreamBody waits on its readLock before
// pooling the *requestStream and the connection's *bufio.Reader. See
// TestStreamCloseUnderActiveReaderIsSafe, which fails under -race on any
// fasthttp without that fix, and maximhq/bifrost#6143 for what it cost.
func TestSetupStreamCancellation_NoPanicOnCancelledContext(t *testing.T) {
	t.Parallel()

	body := newSyncedPanicBody()
	goCtx, cancel := context.WithCancel(context.Background())
	bifrostCtx := schemas.NewBifrostContext(goCtx, time.Time{})

	reader, cleanupReader := NewIdleTimeoutReader(body, body, time.Minute, bifrostCtx)
	// Defers run LIFO: allowReturn first, then stopCancel, then cleanupReader.
	// This order lets Close() return before we wait for the goroutine.
	defer cleanupReader()
	stopCancel := SetupStreamCancellation(bifrostCtx, body, getLogger())
	defer stopCancel()
	defer close(body.allowReturn)

	type readResult struct {
		err      error
		panicked any
	}
	resultCh := make(chan readResult, 1)
	go func() {
		var res readResult
		defer func() {
			if r := recover(); r != nil {
				res.panicked = r
			}
			resultCh <- res
		}()
		buf := make([]byte, 1)
		_, res.err = reader.Read(buf)
	}()

	cancel()

	select {
	case res := <-resultCh:
		if res.panicked != nil {
			t.Errorf("Read re-panicked instead of reporting a closed stream: %v", res.panicked)
			return
		}
		if !errors.Is(res.err, ErrStreamClosed) {
			t.Errorf("expected ErrStreamClosed, got: %v", res.err)
		}
	case <-time.After(2 * time.Second):
		t.Error("Read goroutine did not unblock after context cancellation")
	}
}

// TestSetupStreamCancellation_RecoversCloseStreamPanic verifies that a panic
// raised by closeBodyStream inside the cancellation goroutine (fasthttp's
// CloseWithError nil-dereffing in (*HostClient).CloseConn once the connection is
// gone) is recovered and logged rather than taking the process down. This is the
// cancellation-goroutine counterpart to
// TestIdleTimeoutReader_RecoversCloseStreamPanicOnTimerFire. Not parallel: it
// swaps the package-global logger.
func TestSetupStreamCancellation_RecoversCloseStreamPanic(t *testing.T) {
	capLog := &captureLogger{}
	prev := getLogger()
	SetLogger(capLog)
	t.Cleanup(func() { SetLogger(prev) })

	body := newTimerPanicCloser()
	goCtx, cancel := context.WithCancel(context.Background())
	bifrostCtx := schemas.NewBifrostContext(goCtx, time.Time{})

	stopCancel := SetupStreamCancellation(bifrostCtx, body, getLogger())
	cancel()
	select {
	case <-body.called:
	case <-time.After(2 * time.Second):
		t.Fatal("cancellation goroutine never called CloseWithError within 2s")
	}
	stopCancel() // blocks until the goroutine (recover + log) has returned

	if !capLog.contains("stream cancellation") || !capLog.contains("nil-deref") {
		t.Fatalf("expected recovered panic to be logged; got %v", capLog.msgs)
	}
}

// TestSetupStreamCancellation_ClosesExactlyOnce guards the ownership claim: the
// cancellation goroutine, the idle-timeout timer and ReleaseStreamingResponse
// all close the same stream, and only one of them may drive it.
func TestSetupStreamCancellation_ClosesExactlyOnce(t *testing.T) {
	t.Parallel()

	body := newSyncedPanicBody()
	close(body.allowReturn) // let Close() return immediately
	goCtx, cancel := context.WithCancel(context.Background())
	bifrostCtx := schemas.NewBifrostContext(goCtx, time.Time{})

	stopCancel := SetupStreamCancellation(bifrostCtx, body, getLogger())
	cancel()
	// BifrostContext propagates its parent's cancellation through a watcher
	// goroutine, so cancel() alone does not guarantee ctx.Err() is set yet.
	// Waiting here keeps the assertion about close ownership rather than about
	// how fast that propagation happens.
	<-bifrostCtx.Done()
	stopCancel()

	if got := body.closeCount(); got != 1 {
		t.Errorf("body stream closed %d times, want exactly 1", got)
	}
}

// countingStreamCloser models fasthttp's non-idempotent streaming body handle:
// it implements streamCloserWithError (CloseWithError) and io.Reader, counting
// closes and reads. The whole point of the atomic-claim fix is that the
// underlying close runs at most once across the cancellation goroutine, the
// idle-timeout timer, and ReleaseStreamingResponse — a second close would
// re-run fasthttp's releaseRequestStream and double-Put the pooled struct.
type countingStreamCloser struct {
	closeCount atomic.Int32
	readCount  atomic.Int32
}

func (c *countingStreamCloser) Read([]byte) (int, error) {
	c.readCount.Add(1)
	return 0, io.EOF
}

func (c *countingStreamCloser) CloseWithError(error) error {
	c.closeCount.Add(1)
	return nil
}

// TestStreamClose_ExactlyOnceAcrossOwners drives the idle-timeout timer and the
// cancellation goroutine concurrently against one shared stream + context and
// asserts the underlying CloseWithError fires exactly once. Before the fix the
// two owners used a racy Value-then-SetValue guard and could both close, which
// double-released the pooled fasthttp requestStream and poisoned another
// request (the `slice bounds out of range [:-680]` crash). Run with -race.
func TestStreamClose_ExactlyOnceAcrossOwners(t *testing.T) {
	t.Parallel()

	for i := 0; i < 100; i++ {
		body := &countingStreamCloser{}
		goCtx, cancel := context.WithCancel(context.Background())
		ctx := schemas.NewBifrostContext(goCtx, time.Time{})

		// Tiny idle timeout so the timer races the cancellation.
		wrapped, cleanup := NewIdleTimeoutReader(strings.NewReader("x"), body, time.Millisecond, ctx)
		if n, err := wrapped.Read(make([]byte, 8)); err != nil || n == 0 {
			t.Fatalf("iteration %d: arming read: n=%d err=%v, want one byte", i, n, err)
		}
		stop := SetupStreamCancellation(ctx, body, getLogger())

		go cancel() // fire cancellation concurrently with the idle timer

		// Wait (bounded) until at least one owner has performed the close, rather
		// than relying on a fixed sleep that can miss both close paths on slow CI.
		deadline := time.Now().Add(2 * time.Second)
		for body.closeCount.Load() == 0 && time.Now().Before(deadline) {
			time.Sleep(50 * time.Microsecond)
		}
		stop()
		cleanup()

		if got := body.closeCount.Load(); got != 1 {
			t.Fatalf("iteration %d: CloseWithError called %d times, want exactly 1", i, got)
		}
	}
}

// TestConnectionClosedClaim_SerializesOwners verifies the primitive the fix
// relies on: GetAndSetValue on BifrostContextKeyConnectionClosed is an atomic
// compare-and-claim, so exactly one of many concurrent owners observes the
// prior `false` and is permitted to perform the underlying close.
func TestConnectionClosedClaim_SerializesOwners(t *testing.T) {
	t.Parallel()

	const owners = 64
	ctx := schemas.NewBifrostContext(context.Background(), time.Time{})

	var winners atomic.Int32
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < owners; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			if prev, _ := ctx.GetAndSetValue(schemas.BifrostContextKeyConnectionClosed, true).(bool); !prev {
				winners.Add(1)
			}
		}()
	}
	close(start)
	wg.Wait()

	if got := winners.Load(); got != 1 {
		t.Fatalf("expected exactly 1 owner to win the close claim, got %d", got)
	}
}

// TestReleaseStreamingResponse_SkipsWhenAlreadyClaimed asserts that once a
// cancel/timeout owner has claimed the close, ReleaseStreamingResponse neither
// drains nor releases the body stream — preventing fasthttp.ReleaseResponse
// from triggering a second releaseRequestStream on an already-relinquished
// pooled struct.
func TestReleaseStreamingResponse_SkipsWhenAlreadyClaimed(t *testing.T) {
	t.Parallel()

	ctx := schemas.NewBifrostContext(context.Background(), time.Time{})
	// A prior owner already claimed + closed the stream.
	ctx.GetAndSetValue(schemas.BifrostContextKeyConnectionClosed, true)

	body := &countingStreamCloser{}
	resp := fasthttp.AcquireResponse()
	resp.SetBodyStream(body, -1)

	ReleaseStreamingResponse(ctx, resp)

	if got := body.readCount.Load(); got != 0 {
		t.Fatalf("ReleaseStreamingResponse drained the body despite a prior close claim (read %d times)", got)
	}
	if got := body.closeCount.Load(); got != 0 {
		t.Fatalf("ReleaseStreamingResponse closed an already-claimed stream (%d times)", got)
	}

	// The claim was lost, so resp was intentionally not released (the
	// production GC-leak trade-off). Release it here to keep the test clean.
	fasthttp.ReleaseResponse(resp)
}

// TestReleaseStreamingResponse_NoBodyDoesNotClaimConnection covers the
// pre-response error path: fasthttp.Do can fail before installing a body stream.
// Releasing that empty response must not mark the request context as
// connection-closed, because retries/fallbacks reuse the same BifrostContext.
func TestReleaseStreamingResponse_NoBodyDoesNotClaimConnection(t *testing.T) {
	t.Parallel()

	ctx := schemas.NewBifrostContext(context.Background(), time.Time{})
	resp := fasthttp.AcquireResponse()

	ReleaseStreamingResponse(ctx, resp)

	if closed, _ := ctx.Value(schemas.BifrostContextKeyConnectionClosed).(bool); closed {
		t.Fatal("ReleaseStreamingResponse claimed connection ownership without a body stream")
	}
}

type abortCloser struct{ closed chan error }

func (c *abortCloser) Read([]byte) (int, error) { return 0, io.EOF }
func (c *abortCloser) CloseWithError(err error) error {
	c.closed <- err
	return nil
}

func TestSetupStreamCancellation_AttemptAbort(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		claimed   bool
		wantClose bool
	}{
		{name: "abort closes the attempt body", wantClose: true},
		{name: "abort skips a body another owner closed", claimed: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			// Given: a live context carrying this attempt's abort channel.
			ctx := schemas.NewBifrostContext(context.Background(), time.Time{})
			abort := make(chan struct{})
			ctx.SetValue(schemas.BifrostContextKeyStreamAttemptAbort, abort)
			if tt.claimed {
				ctx.SetValue(schemas.BifrostContextKeyConnectionClosed, true)
			}
			body := &abortCloser{closed: make(chan error, 1)}
			stop := SetupStreamCancellation(ctx, body, getLogger())
			// The next attempt's channel must not affect this one.
			ctx.SetValue(schemas.BifrostContextKeyStreamAttemptAbort, make(chan struct{}))

			// When: core rejects the attempt.
			close(abort)

			// Then: the body is closed exactly when nobody else owns the close.
			if tt.wantClose {
				select {
				case err := <-body.closed:
					if !errors.Is(err, errStreamAttemptAborted) {
						t.Fatalf("close error = %v, want errStreamAttemptAborted", err)
					}
				case <-time.After(2 * time.Second):
					t.Fatal("abort did not close the attempt body")
				}
			}
			stop()
			select {
			case err := <-body.closed:
				t.Fatalf("unexpected extra close: %v", err)
			default:
			}
			if closed, _ := ctx.Value(schemas.BifrostContextKeyConnectionClosed).(bool); !closed {
				t.Fatal("abort left the close unclaimed")
			}
		})
	}
}

func runWithin(t *testing.T, d time.Duration, name string, f func()) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		defer close(done)
		f()
	}()
	select {
	case <-done:
	case <-time.After(d):
		t.Fatalf("%s did not return within %v (deadlock)", name, d)
	}
}

func TestIdleTimeoutReader_CleanupNeverArmedReturnsPromptly(t *testing.T) {
	t.Parallel()
	spy := &readCloserSpy{}
	_, cleanup := NewIdleTimeoutReader(strings.NewReader("x"), spy, 20*time.Millisecond, nil)
	time.Sleep(80 * time.Millisecond)
	runWithin(t, time.Second, "cleanup on a never-armed reader", cleanup)
	runWithin(t, time.Second, "second cleanup", cleanup)

	if spy.closeCount() != 0 {
		t.Fatalf("body closed %d times for a reader that was never armed", spy.closeCount())
	}
}

func TestIdleTimeoutReader_CleanupNeverArmedLeavesNoGoroutine(t *testing.T) {
	spy := &readCloserSpy{}
	before := runtime.NumGoroutine()
	for i := 0; i < 50; i++ {
		_, cleanup := NewIdleTimeoutReader(strings.NewReader("x"), spy, 10*time.Millisecond, nil)
		cleanup()
	}
	time.Sleep(60 * time.Millisecond)
	if after := runtime.NumGoroutine(); after > before+2 {
		t.Fatalf("goroutines grew from %d to %d", before, after)
	}
	if spy.closeCount() != 0 {
		t.Fatalf("body closed %d times for never-armed readers", spy.closeCount())
	}
}

func TestIdleTimeoutReader_ReadAfterCleanupDoesNotArm(t *testing.T) {
	t.Parallel()
	spy := &readCloserSpy{}
	wrapped, cleanup := NewIdleTimeoutReader(strings.NewReader("xy"), spy, 10*time.Millisecond, nil)
	cleanup()
	if n, err := wrapped.Read(make([]byte, 1)); err != nil || n != 1 {
		t.Fatalf("read after cleanup: n=%d err=%v", n, err)
	}
	time.Sleep(60 * time.Millisecond)
	if spy.closeCount() != 0 {
		t.Fatalf("a read after cleanup armed a timer that closed the body %d times", spy.closeCount())
	}
}

func TestIdleTimeoutReader_CleanupAfterArmedAndFired(t *testing.T) {
	t.Parallel()
	spy := &readCloserSpy{}
	wrapped, cleanup := NewIdleTimeoutReader(strings.NewReader("x"), spy, 10*time.Millisecond, nil)
	if n, err := wrapped.Read(make([]byte, 8)); err != nil || n != 1 {
		t.Fatalf("arming read: n=%d err=%v", n, err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for spy.closeCount() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if spy.closeCount() != 1 {
		t.Fatalf("expected the idle timer to close the body once, got %d", spy.closeCount())
	}
	runWithin(t, time.Second, "cleanup after the timer fired", cleanup)
}

func TestIdleTimeoutReader_CleanupAfterArmedNotFired(t *testing.T) {
	t.Parallel()
	spy := &readCloserSpy{}
	wrapped, cleanup := NewIdleTimeoutReader(strings.NewReader("x"), spy, time.Hour, nil)
	if n, err := wrapped.Read(make([]byte, 8)); err != nil || n != 1 {
		t.Fatalf("arming read: n=%d err=%v", n, err)
	}
	runWithin(t, time.Second, "cleanup on an armed, unfired reader", cleanup)
	if spy.closeCount() != 0 {
		t.Fatalf("cleanup closed the body %d times", spy.closeCount())
	}
}
