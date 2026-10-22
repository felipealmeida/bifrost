package utils

import (
	"bytes"
	"errors"
	"io"
	"strings"
	"testing"
	"testing/synctest"
	"time"
)

// Each case feeds chunks in order to one sseActivity; want[i] is whether chunk i
// must reset the idle timer.
func TestSSEActivity_Observe(t *testing.T) {
	t.Parallel()
	long := "data: " + strings.Repeat("x", 64*1024) + "\n\n"
	cases := []struct {
		name   string
		chunks []string
		want   []bool
	}{
		{"comment_only", []string{": prefill\n\n"}, []bool{false}},
		{"blank_lines_only", []string{"\n\n\r\n\r\r"}, []bool{false}},
		{"data_line", []string{"data: {}\n\n"}, []bool{true}},
		{"event_line_counts", []string{"event: ping\n"}, []bool{true}},
		{"comment_then_data_same_read", []string{": prefill\n\ndata: {}\n\n"}, []bool{true}},
		{"data_then_comment_reads", []string{"data: {}\n\n", ": prefill\n\n", ": prefill\n\n"}, []bool{true, false, false}},
		{"split_before_colon", []string{"\n", ":", " prefill\n"}, []bool{false, false, false}},
		{"split_after_colon", []string{":", " prefill", "\n\n"}, []bool{false, false, false}},
		{"split_comment_then_data", []string{": pre", "fill\n\n", "da", "ta: {}\n"}, []bool{false, false, true, true}},
		{"comment_tail_then_data_head_same_read", []string{": pre", "fill\n\nda"}, []bool{false, true}},
		{"split_crlf_then_comment", []string{": a\r", "\n:", " b\r\n\r\n"}, []bool{false, false, false}},
		{"split_crlf_then_data", []string{": a\r", "\nd", "ata\r\n"}, []bool{false, true, true}},
		{"lone_cr_ends_line", []string{": a\rdata\r"}, []bool{true}},
		{"data_partial_line_counts_promptly", []string{"da", "ta: {\"choices\"", ":[]}", "\n\n"}, []bool{true, true, true, false}},
		{"colon_inside_data_is_not_comment", []string{"data", ": x\n"}, []bool{true, true}},
		{"long_data_single_read", []string{long}, []bool{true}},
		{"long_data_mid_line_read", []string{long[:10], long[10:40000], long[40000:]}, []bool{true, true, true}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			s := &sseActivity{lineStart: true}
			for i, chunk := range c.chunks {
				if got := s.observe([]byte(chunk)); got != c.want[i] {
					t.Fatalf("chunk %d %q: active = %v, want %v", i, chunk, got, c.want[i])
				}
			}
		})
	}
}

// After the first data line arms the timer, comments every 30 ms with a 150 ms
// idle window: the SSE reader times out and returns ErrStreamIdleTimeout; the
// plain reader would have been kept alive. (Before the first data line,
// comments neither arm nor fire; see CommentsBeforeFirstDataDoNotArm.)
func TestSSEIdleTimeoutReader_CommentsDoNotResetIdle(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		pr, pw := io.Pipe()
		defer pw.Close()
		wrapped, cleanup := NewSSEIdleTimeoutReader(pr, pr, 150*time.Millisecond, nil)
		defer cleanup()
		writerDone := make(chan struct{})
		defer func() { <-writerDone }()
		go func() {
			defer close(writerDone)
			if _, err := pw.Write([]byte("data: x\n")); err != nil {
				return
			}
			for {
				if _, err := pw.Write([]byte(": prefill\n\n")); err != nil {
					return
				}
				time.Sleep(30 * time.Millisecond)
			}
		}()
		start := time.Now()
		buf := make([]byte, 64)
		var err error
		for err == nil {
			_, err = wrapped.Read(buf)
		}
		if !errors.Is(err, ErrStreamIdleTimeout) {
			t.Fatalf("err = %v, want ErrStreamIdleTimeout", err)
		}
		if elapsed := time.Since(start); elapsed > time.Second {
			t.Fatalf("timed out after %v, want ~150ms", elapsed)
		}
	})
}

// Data every 50 ms interleaved with comments, under a 150 ms window, for
// longer than several windows: never times out, and the bytes are unchanged.
func TestSSEIdleTimeoutReader_DataResetsAndBytesPreserved(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		pr, pw := io.Pipe()
		wrapped, cleanup := NewSSEIdleTimeoutReader(pr, pr, 150*time.Millisecond, nil)
		defer cleanup()
		var sent bytes.Buffer
		parts := []string{": prefill\r\n\r\n", "data: {\"a\":1}\r\n\r\n", ": x\n\n", "data: {\"b\":2}\n\n"}
		go func() {
			for i := range 16 { // ~800 ms total, > 5 idle windows
				p := parts[i%len(parts)]
				sent.WriteString(p)
				_, _ = pw.Write([]byte(p))
				time.Sleep(25 * time.Millisecond)
			}
			pw.Close()
		}()
		got, err := io.ReadAll(wrapped)
		if err != nil {
			t.Fatalf("ReadAll error: %v", err)
		}
		if !bytes.Equal(got, sent.Bytes()) {
			t.Fatalf("bytes changed:\n got %q\nwant %q", got, sent.Bytes())
		}
	})
}

func TestSSEIdleTimeoutReader_CommentsBeforeFirstDataDoNotArm(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		const timeout = 150 * time.Millisecond
		pr, pw := io.Pipe()
		defer pw.Close()
		wrapped, cleanup := NewSSEIdleTimeoutReader(pr, pr, timeout, nil)
		defer cleanup()
		// Long enough not to interfere: this test is about the idle window.
		shortenFirstByte(wrapped, 10*time.Second)

		go func() {
			start := time.Now()
			for time.Since(start) < 3*timeout {
				if _, err := pw.Write([]byte(": ping\n\n")); err != nil {
					return
				}
				time.Sleep(30 * time.Millisecond)
			}
			_, _ = pw.Write([]byte("data: x\n"))
		}()

		buf := make([]byte, 64)
		var got bytes.Buffer
		for !strings.Contains(got.String(), "data: x\n") {
			n, err := wrapped.Read(buf)
			got.Write(buf[:n])
			if err != nil {
				t.Fatalf("stream closed before the first data line (comments must not arm or fire): %v", err)
			}
		}

		start := time.Now()
		var err error
		for err == nil {
			_, err = wrapped.Read(buf)
		}
		if !errors.Is(err, ErrStreamIdleTimeout) {
			t.Fatalf("err = %v, want ErrStreamIdleTimeout after silence following the first data line", err)
		}
		if elapsed := time.Since(start); elapsed < timeout {
			t.Fatalf("fired after %v, want >= %v", elapsed, timeout)
		}
	})
}

// Comment-only keepalives do not end the first-byte window, so the first-byte
// limit stays absolute: the stream is closed with ErrStreamFirstByteTimeout
// even though bytes keep arriving.
func TestSSEIdleTimeoutReader_CommentsDoNotEndFirstByteWindow(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		const firstByte = 300 * time.Millisecond
		pr, pw := io.Pipe()
		defer pw.Close()
		wrapped, cleanup := NewSSEIdleTimeoutReader(pr, pr, 100*time.Millisecond, nil)
		defer cleanup()
		shortenFirstByte(wrapped, firstByte)

		writerDone := make(chan struct{})
		defer func() { <-writerDone }()
		go func() {
			defer close(writerDone)
			for {
				if _, err := pw.Write([]byte(": ping\n\n")); err != nil {
					return
				}
				time.Sleep(30 * time.Millisecond)
			}
		}()

		start := time.Now()
		_, err := readUntilErr(wrapped)
		if !errors.Is(err, ErrStreamFirstByteTimeout) {
			t.Fatalf("err = %v, want ErrStreamFirstByteTimeout", err)
		}
		if errors.Is(err, ErrStreamIdleTimeout) {
			t.Fatalf("first-byte timeout matched ErrStreamIdleTimeout: %v", err)
		}
		if elapsed := time.Since(start); elapsed < firstByte {
			t.Fatalf("fired after %v, want >= %v", elapsed, firstByte)
		}
	})
}
