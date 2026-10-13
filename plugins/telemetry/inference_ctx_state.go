package telemetry

import (
	"sync"

	schemas "github.com/maximhq/bifrost/core/schemas"
)

// Per-request inference state is stored in the BifrostContext and shared between the
// synchronous hooks (PreLLMHook of attempt i+1) and the async goroutine that PostLLMHook
// starts for attempt i. Every container below is therefore guarded by its own mutex, and
// creation in the context is serialised by inferenceCtxStateMu so that two goroutines can
// never install two different containers for one request.
var inferenceCtxStateMu sync.Mutex

// dispatchedAttempts tracks which fallback attempts incremented the executing gauge and
// have not released it yet.
type dispatchedAttempts struct {
	mu sync.Mutex
	m  map[int]bool
}

func (d *dispatchedAttempts) mark(idx int) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.m == nil {
		d.m = make(map[int]bool)
	}
	d.m[idx] = true
}

// takeIfMarked reports whether idx was marked and clears the mark in the same critical
// section, so exactly one caller releases a given attempt.
func (d *dispatchedAttempts) takeIfMarked(idx int) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	if !d.m[idx] {
		return false
	}
	delete(d.m, idx)
	return true
}

func getOrCreateDispatchedAttempts(ctx *schemas.BifrostContext) *dispatchedAttempts {
	inferenceCtxStateMu.Lock()
	defer inferenceCtxStateMu.Unlock()
	if existing, ok := ctx.Value(inferenceDispatchedAttemptsKey).(*dispatchedAttempts); ok && existing != nil {
		return existing
	}
	attempts := &dispatchedAttempts{}
	ctx.SetValue(inferenceDispatchedAttemptsKey, attempts)
	return attempts
}

// attemptFinalizations holds one sync.Once per fallback attempt.
type attemptFinalizations struct {
	mu sync.Mutex
	m  map[int]*sync.Once
}

func (a *attemptFinalizations) get(attempt int) *sync.Once {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.m == nil {
		a.m = make(map[int]*sync.Once)
	}
	if once := a.m[attempt]; once != nil {
		return once
	}
	once := &sync.Once{}
	a.m[attempt] = once
	return once
}

func getOrCreateAttemptFinalizations(ctx *schemas.BifrostContext) *attemptFinalizations {
	inferenceCtxStateMu.Lock()
	defer inferenceCtxStateMu.Unlock()
	if existing, ok := ctx.Value(inferenceAttemptFinalizationKey).(*attemptFinalizations); ok && existing != nil {
		return existing
	}
	attempts := &attemptFinalizations{}
	ctx.SetValue(inferenceAttemptFinalizationKey, attempts)
	return attempts
}
