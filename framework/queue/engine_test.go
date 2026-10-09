package queue

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	bifrost "github.com/maximhq/bifrost/core"
	"github.com/maximhq/bifrost/core/schemas"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fastOpts keeps engine tests quick: tight polling and millisecond backoff.
func fastOpts(extra ...SubscribeOption) []SubscribeOption {
	return append([]SubscribeOption{
		WithPollInterval(20 * time.Millisecond),
		WithLeaseDuration(400 * time.Millisecond),
		WithBackoff(time.Millisecond, 5*time.Millisecond),
	}, extra...)
}

func newTestQueue(t *testing.T) (*storeQueue, *memoryStore) {
	t.Helper()
	store := newMemoryStore()
	q, err := NewStoreQueue(store, EngineConfig{}, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = q.Close(context.Background()) })
	return q.(*storeQueue), store
}

func publish(t *testing.T, q Queue, msgs ...*Message) {
	t.Helper()
	require.NoError(t, q.Publish(context.Background(), msgs...))
}

func subscribe(t *testing.T, q Queue, topic, group string, h Handler, opts ...SubscribeOption) Subscription {
	t.Helper()
	sub, err := q.Subscribe(context.Background(), topic, group, h, fastOpts(opts...)...)
	require.NoError(t, err)
	t.Cleanup(func() { _ = sub.Close(context.Background()) })
	return sub
}

func waitStats(t *testing.T, q Queue, topic, group string, want Stats) {
	t.Helper()
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		st, err := q.Stats(context.Background(), topic, group)
		assert.NoError(c, err)
		assert.Equal(c, want, st)
	}, 5*time.Second, 10*time.Millisecond)
}

// engineBackend returns n stores that share one backend, one per simulated
// instance. The engine contract runs the same cases over every backend.
type engineBackend func(t *testing.T, n int) []Store

// engineQueues starts one engine per store, closed when the test ends.
func engineQueues(t *testing.T, stores []Store) []*storeQueue {
	t.Helper()
	out := make([]*storeQueue, len(stores))
	for i, s := range stores {
		q, err := NewStoreQueue(s, EngineConfig{}, nil)
		require.NoError(t, err)
		t.Cleanup(func() { _ = q.Close(context.Background()) })
		out[i] = q.(*storeQueue)
	}
	return out
}

// runEngineContract checks engine behaviour end to end over a real backend:
// retries, dead-lettering, panics, lease heartbeats and lease loss,
// shutdown, and publish wakeups.
func runEngineContract(t *testing.T, backend engineBackend) {
	cases := []struct {
		name string
		fn   func(t *testing.T, backend engineBackend)
	}{
		{"PublishSubscribe", engineContractPublishSubscribe},
		{"RetriesThenDeadLetters", engineContractRetriesThenDeadLetters},
		{"PermanentSkipsRetries", engineContractPermanentSkipsRetries},
		{"RecoversPanics", engineContractRecoversPanics},
		{"HeartbeatKeepsLongHandlersExclusive", engineContractHeartbeatKeepsLongHandlersExclusive},
		{"LeaseLossCancelsHandler", engineContractLeaseLossCancelsHandler},
		{"AttemptsSpentByLostLeasesDeadLetterWithoutRunning", engineContractAttemptsSpentByLostLeases},
		{"CloseDrainsInFlight", engineContractCloseDrainsInFlight},
		{"CloseDeadlineKeepsRunningLease", engineContractCloseDeadlineKeepsRunningLease},
		{"CloseReleasesUnstartedWithoutSpendingAttempts", engineContractCloseReleasesUnstarted},
		{"WakeDeliversBeforePoll", engineContractWakeDeliversBeforePoll},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) { tc.fn(t, backend) })
	}
}

func TestMemoryEngineContract(t *testing.T) {
	runEngineContract(t, func(t *testing.T, n int) []Store {
		store := newMemoryStore()
		t.Cleanup(func() { _ = store.Close(context.Background()) })
		out := make([]Store, n)
		for i := range out {
			out[i] = sharedStore{store}
		}
		return out
	})
}

func engineContractPublishSubscribe(t *testing.T, backend engineBackend) {
	q := engineQueues(t, backend(t, 1))[0]
	topic := uniqueTopic(t)
	var mu sync.Mutex
	got := map[string][]string{}
	handler := func(group string) Handler {
		return func(ctx context.Context, d *Delivery) error {
			mu.Lock()
			defer mu.Unlock()
			got[group] = append(got[group], string(d.Payload))
			assert.Equal(t, DeliveryID(d.ID, group), d.DeliveryID)
			assert.Equal(t, 1, d.Attempt)
			return nil
		}
	}
	subscribe(t, q, topic, "billing", handler("billing"))
	subscribe(t, q, topic, "shipping", handler("shipping"))

	m := &Message{Topic: topic, Payload: []byte("o1"), Headers: map[string]string{"h": "v"}}
	publish(t, q, m, &Message{Topic: topic, Payload: []byte("o2")})
	assert.NotEmpty(t, m.ID, "Publish fills the ID")
	assert.False(t, m.PublishedAt.IsZero(), "Publish fills the publish time")

	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(got["billing"]) == 2 && len(got["shipping"]) == 2
	}, 10*time.Second, 10*time.Millisecond)
	waitStats(t, q, topic, "billing", Stats{})
	waitStats(t, q, topic, "shipping", Stats{})
}

func engineContractRetriesThenDeadLetters(t *testing.T, backend engineBackend) {
	q := engineQueues(t, backend(t, 1))[0]
	topic := uniqueTopic(t)
	dlqTopic := topic + ".dlq"
	var attempts []int
	var mu sync.Mutex
	subscribe(t, q, topic, "workers", func(ctx context.Context, d *Delivery) error {
		mu.Lock()
		attempts = append(attempts, d.Attempt)
		mu.Unlock()
		return errors.New("still failing")
	}, WithMaxAttempts(3), WithDeadLetterTopic(dlqTopic))

	dlq := make(chan *Delivery, 1)
	subscribe(t, q, dlqTopic, "inspect", func(ctx context.Context, d *Delivery) error {
		dlq <- d
		return nil
	})

	m := &Message{Topic: topic, Key: "k", Payload: []byte("p"), Headers: map[string]string{"orig": "1"}}
	publish(t, q, m)

	select {
	case d := <-dlq:
		assert.Equal(t, []byte("p"), d.Payload)
		assert.Equal(t, "k", d.Key)
		assert.Equal(t, "1", d.Headers["orig"])
		assert.Equal(t, topic, d.Headers[HeaderDeadLetterSourceTopic])
		assert.Equal(t, "workers", d.Headers[HeaderDeadLetterGroup])
		assert.Equal(t, DeliveryID(m.ID, "workers"), d.Headers[HeaderDeadLetterDeliveryID])
		assert.Equal(t, m.ID, d.Headers[HeaderDeadLetterMessageID])
		assert.Equal(t, "3", d.Headers[HeaderDeadLetterAttempts])
		assert.Equal(t, "still failing", d.Headers[HeaderDeadLetterError])
		assert.Equal(t, deadLetterMessageID(DeliveryID(m.ID, "workers")), d.ID)
	case <-time.After(10 * time.Second):
		t.Fatal("no dead-letter copy")
	}
	waitStats(t, q, topic, "workers", Stats{Dead: 1})
	mu.Lock()
	assert.Equal(t, []int{1, 2, 3}, attempts)
	mu.Unlock()
}

func engineContractPermanentSkipsRetries(t *testing.T, backend engineBackend) {
	q := engineQueues(t, backend(t, 1))[0]
	topic := uniqueTopic(t)
	var calls atomic.Int32
	subscribe(t, q, topic, "w", func(ctx context.Context, d *Delivery) error {
		calls.Add(1)
		return Permanent(errors.New("malformed"))
	}, WithMaxAttempts(5))
	publish(t, q, &Message{Topic: topic})
	waitStats(t, q, topic, "w", Stats{Dead: 1})
	assert.Equal(t, int32(1), calls.Load())
}

func engineContractRecoversPanics(t *testing.T, backend engineBackend) {
	q := engineQueues(t, backend(t, 1))[0]
	topic := uniqueTopic(t)
	var calls atomic.Int32
	done := make(chan int, 1)
	subscribe(t, q, topic, "w", func(ctx context.Context, d *Delivery) error {
		if calls.Add(1) == 1 {
			panic("kaboom")
		}
		done <- d.Attempt
		return nil
	})
	publish(t, q, &Message{Topic: topic})
	select {
	case attempt := <-done:
		assert.Equal(t, 2, attempt, "a panic counts as a failed attempt")
	case <-time.After(10 * time.Second):
		t.Fatal("handler never succeeded after panic")
	}
	waitStats(t, q, topic, "w", Stats{})
}

// engineContractHeartbeatKeepsLongHandlersExclusive runs a handler for
// several lease durations while a second instance competes for the same
// group: the heartbeat must keep the lease, so the delivery runs exactly once.
func engineContractHeartbeatKeepsLongHandlersExclusive(t *testing.T, backend engineBackend) {
	pods := engineQueues(t, backend(t, 2))
	topic := uniqueTopic(t)
	var calls atomic.Int32
	h := func(ctx context.Context, d *Delivery) error {
		calls.Add(1)
		select {
		case <-time.After(1500 * time.Millisecond): // ~4 leases
			return nil
		case <-ctx.Done():
			return context.Cause(ctx)
		}
	}
	for _, p := range pods {
		subscribe(t, p, topic, "g", h)
	}
	publish(t, pods[0], &Message{Topic: topic})
	waitStats(t, pods[0], topic, "g", Stats{})
	assert.Equal(t, int32(1), calls.Load())
}

// slowExtendStore stalls every heartbeat past the lease, like a database
// that stops answering, so the real backend reports the lease as no longer
// held. It ignores the heartbeat's own deadline on purpose: honouring it
// would let renewals through in time.
type slowExtendStore struct {
	Store
	delay time.Duration
}

func (s slowExtendStore) Extend(ctx context.Context, cs []*Claimed, lease time.Duration) ([]string, error) {
	time.Sleep(s.delay)
	return s.Store.Extend(context.WithoutCancel(ctx), cs, lease)
}

func engineContractLeaseLossCancelsHandler(t *testing.T, backend engineBackend) {
	const lease = 400 * time.Millisecond
	q := engineQueues(t, []Store{slowExtendStore{Store: backend(t, 1)[0], delay: 2 * lease}})[0]
	topic := uniqueTopic(t)
	started := make(chan struct{})
	cause := make(chan error, 1)
	subscribe(t, q, topic, "w", func(ctx context.Context, d *Delivery) error {
		if d.Attempt > 1 {
			return nil
		}
		close(started)
		<-ctx.Done()
		cause <- context.Cause(ctx)
		return ctx.Err()
	}, WithLeaseDuration(lease))
	publish(t, q, &Message{Topic: topic})
	<-started
	select {
	case err := <-cause:
		assert.ErrorIs(t, err, errLeaseLost)
	case <-time.After(10 * time.Second):
		t.Fatal("handler was not cancelled after losing its lease")
	}
	// The redelivery completes, and nothing is left behind.
	waitStats(t, q, topic, "w", Stats{})
}

func engineContractAttemptsSpentByLostLeases(t *testing.T, backend engineBackend) {
	stores := backend(t, 1)
	q := engineQueues(t, stores)[0]
	ctx := context.Background()
	topic := uniqueTopic(t)
	require.NoError(t, stores[0].EnsureGroup(ctx, topic, "w", StartFromLatest))
	publish(t, q, &Message{Topic: topic})
	// Two holders "crash": each lease expires unfinished.
	for range 2 {
		require.Eventually(t, func() bool {
			cs, err := stores[0].Claim(ctx, topic, "w", "crashed", 1, time.Millisecond)
			require.NoError(t, err)
			return len(cs) == 1
		}, 10*time.Second, 20*time.Millisecond)
	}
	time.Sleep(50 * time.Millisecond)
	var calls atomic.Int32
	subscribe(t, q, topic, "w", func(ctx context.Context, d *Delivery) error {
		calls.Add(1)
		return nil
	}, WithMaxAttempts(2))
	waitStats(t, q, topic, "w", Stats{Dead: 1})
	assert.Zero(t, calls.Load(), "a poison delivery past MaxAttempts is not run again")
}

func engineContractCloseDrainsInFlight(t *testing.T, backend engineBackend) {
	q := engineQueues(t, backend(t, 1))[0]
	topic := uniqueTopic(t)
	started := make(chan struct{})
	release := make(chan struct{})
	sub, err := q.Subscribe(context.Background(), topic, "w", func(ctx context.Context, d *Delivery) error {
		close(started)
		<-release
		return nil
	}, fastOpts()...)
	require.NoError(t, err)
	publish(t, q, &Message{Topic: topic})
	<-started

	closed := make(chan error, 1)
	go func() { closed <- sub.Close(context.Background()) }()
	select {
	case <-closed:
		t.Fatal("Close returned while a handler was running")
	case <-time.After(100 * time.Millisecond):
	}
	close(release)
	require.NoError(t, <-closed)
	waitStats(t, q, topic, "w", Stats{})
}

func engineContractCloseDeadlineKeepsRunningLease(t *testing.T, backend engineBackend) {
	q := engineQueues(t, backend(t, 1))[0]
	topic := uniqueTopic(t)
	started := make(chan struct{})
	cause := make(chan error, 1)
	stuck := make(chan struct{})
	t.Cleanup(func() { close(stuck) })
	sub, err := q.Subscribe(context.Background(), topic, "w", func(ctx context.Context, d *Delivery) error {
		close(started)
		<-ctx.Done()
		cause <- context.Cause(ctx)
		<-stuck // ignores cancellation and keeps running
		return nil
	}, fastOpts(WithLeaseDuration(time.Minute))...)
	require.NoError(t, err)
	publish(t, q, &Message{Topic: topic})
	<-started

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	require.NoError(t, sub.Close(ctx))
	assert.ErrorIs(t, <-cause, errSubscriptionClosed)
	// The handler outlived Close, so its lease is not handed back: nobody else
	// may start the delivery while it might still be running.
	st, err := q.Stats(context.Background(), topic, "w")
	require.NoError(t, err)
	assert.Equal(t, Stats{Leased: 1}, st)
}

func engineContractCloseReleasesUnstarted(t *testing.T, backend engineBackend) {
	stores := backend(t, 1)
	q := engineQueues(t, stores)[0]
	store := stores[0]
	ctx := context.Background()
	topic := uniqueTopic(t)
	s := newSubscription(q, topic, "w", func(context.Context, *Delivery) error { return nil }, SubscribeOptions{}.WithDefaults(), ctx)
	require.NoError(t, store.EnsureGroup(ctx, topic, "w", StartFromLatest))
	publish(t, q, &Message{Topic: topic})
	cs, err := store.Claim(ctx, topic, "w", q.runnerID, 1, time.Minute)
	require.NoError(t, err)
	require.Len(t, cs, 1)
	// Tracked but never handed to a worker, as when Close races a claim.
	require.True(t, s.track(cs[0]))
	close(s.claimerDone)
	close(s.work)
	go s.heartbeatLoop()

	require.NoError(t, s.Close(ctx))
	again, err := store.Claim(ctx, topic, "w", "other", 1, time.Minute)
	require.NoError(t, err)
	require.Len(t, again, 1, "released immediately")
	assert.Equal(t, 1, again[0].Attempt)
}

func engineContractWakeDeliversBeforePoll(t *testing.T, backend engineBackend) {
	q := engineQueues(t, backend(t, 1))[0]
	topic := uniqueTopic(t)
	got := make(chan time.Time, 1)
	subscribe(t, q, topic, "w", func(ctx context.Context, d *Delivery) error {
		got <- time.Now()
		return nil
	}, WithPollInterval(time.Minute))
	time.Sleep(200 * time.Millisecond) // let the first, empty poll pass
	sent := time.Now()
	publish(t, q, &Message{Topic: topic})
	select {
	case at := <-got:
		assert.Less(t, at.Sub(sent), 2*time.Second)
	case <-time.After(10 * time.Second):
		t.Fatal("publish did not wake the subscription")
	}
}

func TestEngineValidation(t *testing.T) {
	q, _ := newTestQueue(t)
	ctx := context.Background()
	assert.ErrorIs(t, q.Publish(ctx, &Message{Topic: "bad topic"}), ErrInvalidName)
	assert.ErrorIs(t, q.Publish(ctx, &Message{Topic: "t", Payload: make([]byte, DefaultMaxPayloadBytes+1)}), ErrPayloadTooLarge)
	assert.NoError(t, q.Publish(ctx))

	noop := func(context.Context, *Delivery) error { return nil }
	_, err := q.Subscribe(ctx, "t", "bad group", noop)
	assert.ErrorIs(t, err, ErrInvalidName)
	_, err = q.Subscribe(ctx, "t", "g", nil)
	assert.Error(t, err)
	_, err = q.Subscribe(ctx, "t", "g", noop, WithLeaseDuration(time.Millisecond))
	assert.Error(t, err)
}

func TestEngineClosedQueue(t *testing.T) {
	store := newMemoryStore()
	q, err := NewStoreQueue(store, EngineConfig{}, nil)
	require.NoError(t, err)
	subscribe(t, q, "t", "g", func(context.Context, *Delivery) error { return nil })
	require.NoError(t, q.Close(context.Background()))
	require.NoError(t, q.Close(context.Background()), "Close is idempotent")

	ctx := context.Background()
	assert.ErrorIs(t, q.Publish(ctx, &Message{Topic: "t"}), ErrClosed)
	_, err = q.Subscribe(ctx, "t", "g", func(context.Context, *Delivery) error { return nil })
	assert.ErrorIs(t, err, ErrClosed)
	assert.ErrorIs(t, q.Ping(ctx), ErrClosed)
	assert.ErrorIs(t, store.Ping(ctx), ErrClosed, "the queue owns and closes its store")
}

func TestEngineJanitorPurges(t *testing.T) {
	store := newMemoryStore()
	q, err := NewStoreQueue(store, EngineConfig{JanitorInterval: "20ms", RetentionHours: 1}, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = q.Close(context.Background()) })
	publish(t, q, &Message{Topic: "nobody-listens"})
	// Age everything past retention.
	store.mu.Lock()
	realNow := store.now
	store.now = func() time.Time { return realNow().Add(2 * time.Hour) }
	store.mu.Unlock()
	require.Eventually(t, func() bool {
		store.mu.Lock()
		defer store.mu.Unlock()
		return len(store.messages) == 0
	}, 5*time.Second, 10*time.Millisecond)
}

func TestMemoryQueueRegistered(t *testing.T) {
	q, err := NewQueue(context.Background(), &Config{Enabled: true, Type: QueueTypeMemory}, Dependencies{}, nil)
	require.NoError(t, err)
	require.NotNil(t, q)
	require.NoError(t, q.Ping(context.Background()))
	require.NoError(t, q.Close(context.Background()))

	_, err = NewQueue(context.Background(), &Config{Enabled: true, Type: QueueTypeMemory, Config: &LogStoreQueueConfig{}}, Dependencies{}, nil)
	assert.Error(t, err, "wrong config type is rejected")
}

func TestMemoryMultiInstance(t *testing.T) {
	store := newMemoryStore()
	runMultiInstanceSimulation(t, func(t *testing.T, logger schemas.Logger) Queue {
		q, err := NewStoreQueue(sharedStore{store}, EngineConfig{}, logger)
		require.NoError(t, err)
		return q
	}, 3, 300)
}

// leaseLossLogger records the deliveries the engine reports as having lost
// their lease, which are the only ones allowed to run more than once.
type leaseLossLogger struct {
	schemas.Logger
	mu   sync.Mutex
	lost map[string]bool
}

func newLeaseLossLogger() *leaseLossLogger {
	return &leaseLossLogger{Logger: bifrost.NewNoOpLogger(), lost: map[string]bool{}}
}

func (l *leaseLossLogger) record(msg string, args []any, idArg int) {
	if !strings.HasPrefix(msg, "queue: lease lost") || len(args) <= idArg {
		return
	}
	if id, ok := args[idArg].(string); ok {
		l.mu.Lock()
		l.lost[id] = true
		l.mu.Unlock()
	}
}

// Warn sees "lease lost before <op> of delivery <id>"; Debug sees the
// heartbeat's "lease lost for delivery <id>".
func (l *leaseLossLogger) Warn(msg string, args ...any)  { l.record(msg, args, 1) }
func (l *leaseLossLogger) Debug(msg string, args ...any) { l.record(msg, args, 0) }

func (l *leaseLossLogger) wasLost(deliveryID string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.lost[deliveryID]
}

// sharedStore lets several engine instances ("pods") share one store without
// the first Close closing it for the others.
type sharedStore struct{ Store }

func (sharedStore) Close(context.Context) error { return nil }

// runMultiInstanceSimulation starts pods engine instances over one shared
// backend and drives perGroup keyed and unkeyed messages through three
// consumer groups, failing a fifth of first attempts. It asserts that every
// (message, group) pair completes, that per-key order holds in every group
// (including one registered while publishing is under way), and that a
// delivery runs twice, or on two instances at once, only when the engine
// reported its lease lost: at-least-once, never silently more.
func runMultiInstanceSimulation(t *testing.T, newPod func(t *testing.T, logger schemas.Logger) Queue, pods, perGroup int) {
	t.Helper()
	ctx := context.Background()
	topic := "sim" + uniqueTopic(t)
	groups := []string{"g1", "g2", "late"}
	const keys = 7

	type keyState struct {
		mu   sync.Mutex
		last map[string]int // group/key -> last sequence handled
	}
	order := keyState{last: map[string]int{}}
	var (
		mu       sync.Mutex
		handled  = map[string]int{}  // group/message -> times completed
		running  = map[string]bool{} // delivery id -> running now
		overlaps = map[string]bool{} // delivery ids that ran concurrently
	)
	losses := newLeaseLossLogger()
	handler := func(group string) Handler {
		return func(ctx context.Context, d *Delivery) error {
			mu.Lock()
			if running[d.DeliveryID] {
				overlaps[d.DeliveryID] = true
			}
			running[d.DeliveryID] = true
			mu.Unlock()
			defer func() {
				mu.Lock()
				delete(running, d.DeliveryID)
				mu.Unlock()
			}()

			if d.Key != "" {
				var seq int
				_, _ = fmt.Sscan(d.Headers["seq"], &seq)
				order.mu.Lock()
				k := group + "/" + d.Key
				if prev, ok := order.last[k]; ok && seq <= prev && d.Attempt == 1 {
					order.mu.Unlock()
					return Permanent(fmt.Errorf("key %s out of order: %d after %d", k, seq, prev))
				}
				order.last[k] = seq
				order.mu.Unlock()
			}
			// Fail a slice of first attempts to exercise retries under contention.
			if n, _ := strconv.Atoi(string(d.Payload)); d.Attempt == 1 && n%5 == 0 {
				return errors.New("transient")
			}
			mu.Lock()
			handled[group+"/"+d.ID]++
			mu.Unlock()
			return nil
		}
	}

	queues := make([]Queue, pods)
	for i := range queues {
		queues[i] = newPod(t, losses)
		t.Cleanup(func() { _ = queues[i].Close(context.Background()) })
		for _, g := range groups[:2] {
			_, err := queues[i].Subscribe(ctx, topic, g, handler(g), fastOpts(WithConcurrency(4), WithBatchSize(8), WithMaxAttempts(10))...)
			require.NoError(t, err)
		}
	}

	ids := make([]string, 0, perGroup)
	var lateOnce sync.Once
	for i := range perGroup {
		m := &Message{Topic: topic, Payload: []byte(fmt.Sprint(i)), Headers: map[string]string{"seq": fmt.Sprint(i)}}
		if i%2 == 0 {
			m.Key = fmt.Sprintf("k%d", i%keys)
		}
		require.NoError(t, queues[i%pods].Publish(ctx, m))
		ids = append(ids, m.ID)
		if i == perGroup/3 {
			// Register the late group mid-stream from another instance. It starts
			// from the earliest retained message, so it must still see everything.
			lateOnce.Do(func() {
				for _, q := range queues {
					_, err := q.Subscribe(ctx, topic, "late", handler("late"), fastOpts(WithConcurrency(4), WithBatchSize(8), WithMaxAttempts(10), WithStartFrom(StartFromEarliest))...)
					require.NoError(t, err)
				}
			})
		}
	}

	require.EventuallyWithT(t, func(c *assert.CollectT) {
		mu.Lock()
		defer mu.Unlock()
		for _, g := range groups {
			for _, id := range ids {
				assert.GreaterOrEqual(c, handled[g+"/"+id], 1, "%s/%s", g, id)
			}
		}
	}, 60*time.Second, 50*time.Millisecond)

	for _, g := range groups {
		waitStats(t, queues[0], topic, g, Stats{})
	}
	mu.Lock()
	defer mu.Unlock()
	for id := range overlaps {
		assert.True(t, losses.wasLost(id), "delivery %s ran on two instances at once without losing its lease", id)
	}
	for _, g := range groups {
		for _, id := range ids {
			if handled[g+"/"+id] > 1 {
				assert.True(t, losses.wasLost(DeliveryID(id, g)), "%s/%s completed %d times without losing its lease", g, id, handled[g+"/"+id])
			}
		}
	}
}
