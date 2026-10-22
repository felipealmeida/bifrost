package utils

import (
	"io"
	"time"

	schemas "github.com/maximhq/bifrost/core/schemas"
)

// NewSSEIdleTimeoutReader is NewIdleTimeoutReader for a text/event-stream body.
// Only bytes of non-comment SSE lines end the first-byte window and reset the
// idle timer. Comment lines (starting with ':') and blank lines do not, so an
// upstream that sends only keepalive comments cannot hold the stream open
// forever: it is closed at the first-byte limit, or at the idle limit once data
// has started. The bytes returned to
// the caller are not changed. Timer lifecycle, close ownership and error
// translation are the same as NewIdleTimeoutReader.
func NewSSEIdleTimeoutReader(reader io.Reader, bodyStream io.Reader, timeout time.Duration, ctx *schemas.BifrostContext) (io.Reader, func()) {
	wrapped, cleanup := NewIdleTimeoutReader(reader, bodyStream, timeout, ctx)
	wrapped.(*idleTimeoutReader).sse = &sseActivity{lineStart: true}
	return wrapped, cleanup
}

// sseActivity classifies SSE bytes line by line. The state survives arbitrary
// read boundaries, including a split "\r\n" and a line split right after ':'.
type sseActivity struct {
	lineStart bool // the next byte starts a new line
	comment   bool // the current line is a comment
	afterCR   bool // the previous byte was '\r'; a following '\n' is part of that line end
}

// observe consumes p and reports whether it contained a byte of a non-comment line.
func (s *sseActivity) observe(p []byte) bool {
	active := false
	for _, b := range p {
		if s.afterCR {
			s.afterCR = false
			if b == '\n' {
				continue
			}
		}
		switch b {
		case '\r':
			s.lineStart, s.afterCR = true, true
		case '\n':
			s.lineStart = true
		default:
			if s.lineStart {
				s.lineStart = false
				s.comment = b == ':'
			}
			if !s.comment {
				active = true
			}
		}
	}
	return active
}
