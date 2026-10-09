package queue

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// contractLease is short so lease-expiry cases stay fast; contractWait
// comfortably outlasts it, including on a database clock.
const (
	contractLease = 400 * time.Millisecond
	contractWait  = 700 * time.Millisecond
	longLease     = time.Minute
)

// storeFactory returns a fresh, empty Store. It registers its own cleanup.
type storeFactory func(t *testing.T) Store

// runStoreContract checks the semantics every Store backend must share. Each
// backend's test calls it with its own factory.
func runStoreContract(t *testing.T, newStore storeFactory) {
	cases := []struct {
		name string
		fn   func(t *testing.T, s Store)
	}{
		{"FanOutToEveryGroup", contractFanOut},
		{"LatestSkipsEarlierMessages", contractStartFrom},
		{"AppendIsIdempotent", contractAppendIdempotent},
		{"ConcurrentClaimersAreExclusive", contractExclusiveClaims},
		{"ExpiredLeaseIsReclaimedAndFenced", contractLeaseExpiry},
		{"RetryHonoursBackoff", contractRetry},
		{"KillDeadLetters", contractKill},
		{"ReleaseKeepsAttempts", contractRelease},
		{"PerKeyFIFO", contractPerKeyFIFO},
		{"ExtendReportsHeldLeases", contractExtend},
		{"Purge", contractPurge},
		{"Stats", contractStats},
		{"PayloadsAndHeadersRoundTrip", contractPayloadsRoundTrip},
		{"EarliestBackfillKeepsPublishOrder", contractBackfillOrder},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tc.fn(t, newStore(t))
		})
	}
}

func TestMemoryStoreContract(t *testing.T) {
	runStoreContract(t, func(t *testing.T) Store {
		s := newMemoryStore()
		t.Cleanup(func() { _ = s.Close(context.Background()) })
		return s
	})
}

func newMsg(topic, key, payload string) *Message {
	return &Message{
		ID:          uuid.NewString(),
		Topic:       topic,
		Key:         key,
		Payload:     []byte(payload),
		Headers:     map[string]string{"p": payload},
		PublishedAt: time.Now().UTC(),
	}
}

func uniqueTopic(t *testing.T) string {
	return "t" + uuid.NewString()[:8]
}

func payloads(cs []*Claimed) []string {
	out := make([]string, len(cs))
	for i, c := range cs {
		out[i] = string(c.Message.Payload)
	}
	return out
}

func mustClaim(t *testing.T, s Store, topic, group string, max int, lease time.Duration) []*Claimed {
	t.Helper()
	cs, err := s.Claim(context.Background(), topic, group, "runner-"+group, max, lease)
	require.NoError(t, err)
	return cs
}

func mustAppend(t *testing.T, s Store, msgs ...*Message) {
	t.Helper()
	require.NoError(t, s.Append(context.Background(), msgs))
}

func mustGroup(t *testing.T, s Store, topic, group string, from StartFrom) {
	t.Helper()
	require.NoError(t, s.EnsureGroup(context.Background(), topic, group, from))
}

func contractFanOut(t *testing.T, s Store) {
	topic := uniqueTopic(t)
	mustGroup(t, s, topic, "g1", StartFromLatest)
	mustGroup(t, s, topic, "g2", StartFromLatest)
	m1, m2 := newMsg(topic, "", "a"), newMsg(topic, "k", "b")
	mustAppend(t, s, m1, m2)
	mustAppend(t, s, newMsg("other"+topic, "", "elsewhere"))

	for _, g := range []string{"g1", "g2"} {
		cs := mustClaim(t, s, topic, g, 10, longLease)
		require.Len(t, cs, 2, g)
		assert.Equal(t, []string{"a", "b"}, payloads(cs), g)
		for i, want := range []*Message{m1, m2} {
			c := cs[i]
			assert.Equal(t, DeliveryID(want.ID, g), c.DeliveryID)
			assert.Equal(t, g, c.Group)
			assert.Equal(t, 1, c.Attempt)
			assert.NotEmpty(t, c.ClaimToken)
			assert.Equal(t, want.ID, c.Message.ID)
			assert.Equal(t, want.Topic, c.Message.Topic)
			assert.Equal(t, want.Key, c.Message.Key)
			assert.Equal(t, want.Headers, c.Message.Headers)
			assert.WithinDuration(t, want.PublishedAt, c.Message.PublishedAt, time.Millisecond)
		}
		assert.Empty(t, mustClaim(t, s, topic, g, 10, longLease), "nothing left for %s", g)
	}
}

func contractStartFrom(t *testing.T, s Store) {
	topic := uniqueTopic(t)
	early := newMsg(topic, "", "early")
	mustAppend(t, s, early)

	mustGroup(t, s, topic, "latest", StartFromLatest)
	mustGroup(t, s, topic, "earliest", StartFromEarliest)
	assert.Empty(t, mustClaim(t, s, topic, "latest", 10, longLease))
	assert.Equal(t, []string{"early"}, payloads(mustClaim(t, s, topic, "earliest", 10, longLease)))

	// Re-registering an existing group never backfills again.
	mustGroup(t, s, topic, "latest", StartFromEarliest)
	assert.Empty(t, mustClaim(t, s, topic, "latest", 10, longLease))

	late := newMsg(topic, "", "late")
	mustAppend(t, s, late)
	assert.Equal(t, []string{"late"}, payloads(mustClaim(t, s, topic, "latest", 10, longLease)))
	assert.Equal(t, []string{"late"}, payloads(mustClaim(t, s, topic, "earliest", 10, longLease)))
}

func contractAppendIdempotent(t *testing.T, s Store) {
	ctx := context.Background()
	topic := uniqueTopic(t)
	mustGroup(t, s, topic, "g", StartFromLatest)
	m := newMsg(topic, "", "once")
	// A duplicate inside one batch is stored once.
	mustAppend(t, s, m, m)

	cs := mustClaim(t, s, topic, "g", 10, longLease)
	require.Len(t, cs, 1)
	held, err := s.Ack(ctx, cs[0])
	require.NoError(t, err)
	require.True(t, held)

	// A producer retry after the delivery completed must not reopen it.
	mustAppend(t, s, m)
	assert.Empty(t, mustClaim(t, s, topic, "g", 10, longLease))

	// A retry while the delivery is leased must not reset the lease.
	m2 := newMsg(topic, "", "leased")
	mustAppend(t, s, m2)
	cs = mustClaim(t, s, topic, "g", 10, longLease)
	require.Len(t, cs, 1)
	mustAppend(t, s, m2)
	assert.Empty(t, mustClaim(t, s, topic, "g", 10, longLease))
	held, err = s.Ack(ctx, cs[0])
	require.NoError(t, err)
	assert.True(t, held)
}

func contractExclusiveClaims(t *testing.T, s Store) {
	topic := uniqueTopic(t)
	mustGroup(t, s, topic, "g", StartFromLatest)
	const total = 120
	msgs := make([]*Message, total)
	for i := range msgs {
		msgs[i] = newMsg(topic, "", fmt.Sprint(i))
	}
	mustAppend(t, s, msgs...)

	var mu sync.Mutex
	seen := map[string]int{}
	var wg sync.WaitGroup
	for w := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				cs, err := s.Claim(context.Background(), topic, "g", fmt.Sprintf("w%d", w), 7, longLease)
				if !assert.NoError(t, err) || len(cs) == 0 {
					return
				}
				mu.Lock()
				for _, c := range cs {
					seen[c.DeliveryID]++
				}
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	assert.Len(t, seen, total)
	for id, n := range seen {
		assert.Equal(t, 1, n, "delivery %s claimed %d times", id, n)
	}
}

func contractLeaseExpiry(t *testing.T, s Store) {
	ctx := context.Background()
	topic := uniqueTopic(t)
	mustGroup(t, s, topic, "g", StartFromLatest)
	mustAppend(t, s, newMsg(topic, "", "x"))

	first := mustClaim(t, s, topic, "g", 1, contractLease)
	require.Len(t, first, 1)
	assert.Empty(t, mustClaim(t, s, topic, "g", 1, contractLease), "a live lease is not claimable")

	time.Sleep(contractWait)
	second := mustClaim(t, s, topic, "g", 1, longLease)
	require.Len(t, second, 1)
	assert.Equal(t, first[0].DeliveryID, second[0].DeliveryID)
	assert.NotEqual(t, first[0].ClaimToken, second[0].ClaimToken)
	assert.Equal(t, 2, second[0].Attempt, "an expired lease counts as a spent attempt")

	// Every write under the stale token is a no-op.
	held, err := s.Ack(ctx, first[0])
	require.NoError(t, err)
	assert.False(t, held)
	held, err = s.Retry(ctx, first[0], 0, "stale")
	require.NoError(t, err)
	assert.False(t, held)
	held, err = s.Kill(ctx, first[0], "stale")
	require.NoError(t, err)
	assert.False(t, held)
	ids, err := s.Extend(ctx, first, longLease)
	require.NoError(t, err)
	assert.Empty(t, ids)
	require.NoError(t, s.Release(ctx, first))
	assert.Empty(t, mustClaim(t, s, topic, "g", 1, longLease), "stale release must not free the new lease")

	held, err = s.Ack(ctx, second[0])
	require.NoError(t, err)
	assert.True(t, held)
}

func contractRetry(t *testing.T, s Store) {
	ctx := context.Background()
	topic := uniqueTopic(t)
	mustGroup(t, s, topic, "g", StartFromLatest)
	mustAppend(t, s, newMsg(topic, "", "r"))

	cs := mustClaim(t, s, topic, "g", 1, longLease)
	require.Len(t, cs, 1)
	held, err := s.Retry(ctx, cs[0], contractLease, "boom")
	require.NoError(t, err)
	require.True(t, held)

	assert.Empty(t, mustClaim(t, s, topic, "g", 1, longLease), "not due during backoff")
	time.Sleep(contractWait)
	again := mustClaim(t, s, topic, "g", 1, longLease)
	require.Len(t, again, 1)
	assert.Equal(t, 2, again[0].Attempt)
}

func contractKill(t *testing.T, s Store) {
	ctx := context.Background()
	topic := uniqueTopic(t)
	mustGroup(t, s, topic, "g", StartFromLatest)
	mustAppend(t, s, newMsg(topic, "", "dead"))

	cs := mustClaim(t, s, topic, "g", 1, longLease)
	require.Len(t, cs, 1)
	forged := *cs[0]
	forged.ClaimToken = uuid.NewString()
	held, err := s.Kill(ctx, &forged, "forged")
	require.NoError(t, err)
	assert.False(t, held, "a wrong token cannot kill")

	held, err = s.Kill(ctx, cs[0], "boom")
	require.NoError(t, err)
	require.True(t, held)
	assert.Empty(t, mustClaim(t, s, topic, "g", 1, longLease))

	st, err := s.Stats(ctx, topic, "g")
	require.NoError(t, err)
	assert.Equal(t, Stats{Dead: 1}, st)
}

func contractRelease(t *testing.T, s Store) {
	ctx := context.Background()
	topic := uniqueTopic(t)
	mustGroup(t, s, topic, "g", StartFromLatest)
	mustAppend(t, s, newMsg(topic, "", "rel"))

	cs := mustClaim(t, s, topic, "g", 1, longLease)
	require.Len(t, cs, 1)
	require.NoError(t, s.Release(ctx, cs))
	again := mustClaim(t, s, topic, "g", 1, longLease)
	require.Len(t, again, 1, "released deliveries are claimable at once")
	assert.Equal(t, 1, again[0].Attempt, "release does not spend an attempt")
}

func contractPerKeyFIFO(t *testing.T, s Store) {
	ctx := context.Background()
	topic := uniqueTopic(t)
	mustGroup(t, s, topic, "g", StartFromLatest)
	for _, m := range []*Message{
		newMsg(topic, "k1", "a"), newMsg(topic, "k1", "b"), newMsg(topic, "k2", "c"),
		newMsg(topic, "", "d"), newMsg(topic, "", "e"), newMsg(topic, "k3", "x"), newMsg(topic, "k3", "y"),
	} {
		// Separate appends give each message its own, strictly later position.
		mustAppend(t, s, m)
	}

	first := mustClaim(t, s, topic, "g", 10, longLease)
	assert.Equal(t, []string{"a", "c", "d", "e", "x"}, payloads(first), "only key heads and unkeyed messages")
	byPayload := map[string]*Claimed{}
	for _, c := range first {
		byPayload[string(c.Message.Payload)] = c
	}

	// A head in retry backoff still blocks its key.
	held, err := s.Retry(ctx, byPayload["a"], contractLease, "boom")
	require.NoError(t, err)
	require.True(t, held)
	assert.Empty(t, mustClaim(t, s, topic, "g", 10, longLease), "b waits behind a's backoff")

	// A dead head unblocks its key.
	held, err = s.Kill(ctx, byPayload["x"], "boom")
	require.NoError(t, err)
	require.True(t, held)
	assert.Equal(t, []string{"y"}, payloads(mustClaim(t, s, topic, "g", 10, longLease)))

	time.Sleep(contractWait)
	retried := mustClaim(t, s, topic, "g", 10, longLease)
	require.Equal(t, []string{"a"}, payloads(retried), "a comes back before b")
	held, err = s.Ack(ctx, retried[0])
	require.NoError(t, err)
	require.True(t, held)
	assert.Equal(t, []string{"b"}, payloads(mustClaim(t, s, topic, "g", 10, longLease)))
}

func contractExtend(t *testing.T, s Store) {
	ctx := context.Background()
	topic := uniqueTopic(t)
	mustGroup(t, s, topic, "g", StartFromLatest)
	mustAppend(t, s, newMsg(topic, "", "1"), newMsg(topic, "", "2"))

	cs := mustClaim(t, s, topic, "g", 2, contractLease)
	require.Len(t, cs, 2)
	held, err := s.Extend(ctx, cs, longLease)
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{cs[0].DeliveryID, cs[1].DeliveryID}, held)

	// Extended past the original lease: still not claimable.
	time.Sleep(contractWait)
	assert.Empty(t, mustClaim(t, s, topic, "g", 2, longLease))

	ok, err := s.Ack(ctx, cs[0])
	require.NoError(t, err)
	require.True(t, ok)
	held, err = s.Extend(ctx, cs, longLease)
	require.NoError(t, err)
	assert.Equal(t, []string{cs[1].DeliveryID}, held)

	held, err = s.Extend(ctx, nil, longLease)
	require.NoError(t, err)
	assert.Empty(t, held)
}

func contractPurge(t *testing.T, s Store) {
	ctx := context.Background()
	topic := uniqueTopic(t)
	mustGroup(t, s, topic, "g", StartFromLatest)
	dead, pending := newMsg(topic, "", "dead"), newMsg(topic, "", "pending")
	mustAppend(t, s, dead)
	cs := mustClaim(t, s, topic, "g", 1, longLease)
	require.Len(t, cs, 1)
	held, err := s.Kill(ctx, cs[0], "boom")
	require.NoError(t, err)
	require.True(t, held)
	mustAppend(t, s, pending)
	mustAppend(t, s, newMsg("nogroups"+topic, "", "orphan"))

	// Nothing is old enough yet.
	n, err := s.Purge(ctx, time.Hour, 100)
	require.NoError(t, err)
	assert.Zero(t, n)

	time.Sleep(50 * time.Millisecond)
	_, err = s.Purge(ctx, time.Millisecond, 100)
	require.NoError(t, err)
	// Backends may keep young dead rows longer than asked (ClickHouse floors
	// the age); what every backend guarantees is that a purge never revives a
	// dead delivery and never drops a live one.
	st, err := s.Stats(ctx, topic, "g")
	require.NoError(t, err)
	assert.Equal(t, int64(1), st.Pending, "pending kept")
	assert.LessOrEqual(t, st.Dead, int64(1))
	cs = mustClaim(t, s, topic, "g", 10, longLease)
	assert.Equal(t, []string{"pending"}, payloads(cs), "a message with a live delivery survives purge; a dead one is not revived")
	assert.Empty(t, mustClaim(t, s, topic, "g", 10, longLease))
}

func contractStats(t *testing.T, s Store) {
	ctx := context.Background()
	topic := uniqueTopic(t)
	mustGroup(t, s, topic, "g", StartFromLatest)
	mustGroup(t, s, topic, "other", StartFromLatest)
	mustAppend(t, s, newMsg(topic, "", "1"), newMsg(topic, "", "2"), newMsg(topic, "", "3"))

	cs := mustClaim(t, s, topic, "g", 2, longLease)
	require.Len(t, cs, 2)
	held, err := s.Kill(ctx, cs[0], "boom")
	require.NoError(t, err)
	require.True(t, held)

	st, err := s.Stats(ctx, topic, "g")
	require.NoError(t, err)
	assert.Equal(t, Stats{Pending: 1, Leased: 1, Dead: 1}, st)
	st, err = s.Stats(ctx, topic, "other")
	require.NoError(t, err)
	assert.Equal(t, Stats{Pending: 3}, st)
	st, err = s.Stats(ctx, topic, "missing")
	require.NoError(t, err)
	assert.Equal(t, Stats{}, st)
}

// TestMemoryStoreClockSkew pins that leases follow the store's clock: an
// instance whose clock runs ahead cannot take over a lease that is still live
// on the clock that granted it. Shared backends use the database clock, so
// only the in-process store has a clock to skew.
func TestMemoryStoreClockSkew(t *testing.T) {
	s := newMemoryStore()
	topic := "skew"
	mustGroup(t, s, topic, "g", StartFromLatest)
	mustAppend(t, s, newMsg(topic, "", "x"))
	require.Len(t, mustClaim(t, s, topic, "g", 1, time.Minute), 1)

	// A Store whose callers pass only durations has no way to be handed a
	// skewed "now"; the store's own clock is the only one that matters.
	s.mu.Lock()
	realNow := s.now
	s.now = func() time.Time { return realNow().Add(30 * time.Second) }
	s.mu.Unlock()
	assert.Empty(t, mustClaim(t, s, topic, "g", 1, time.Minute))

	s.mu.Lock()
	s.now = func() time.Time { return realNow().Add(2 * time.Minute) }
	s.mu.Unlock()
	assert.Len(t, mustClaim(t, s, topic, "g", 1, time.Minute), 1)
}

func TestMemoryStoreClosed(t *testing.T) {
	s := newMemoryStore()
	require.NoError(t, s.Close(context.Background()))
	ctx := context.Background()
	assert.ErrorIs(t, s.Append(ctx, []*Message{newMsg("t", "", "x")}), ErrClosed)
	assert.ErrorIs(t, s.EnsureGroup(ctx, "t", "g", StartFromLatest), ErrClosed)
	_, err := s.Claim(ctx, "t", "g", "r", 1, time.Second)
	assert.ErrorIs(t, err, ErrClosed)
	assert.ErrorIs(t, s.Ping(ctx), ErrClosed)
}

// contractPayloadsRoundTrip stores payloads and headers that trip naive
// encodings: every byte value, an empty payload, quotes, backslashes,
// newlines and non-ASCII text.
func contractPayloadsRoundTrip(t *testing.T, s Store) {
	topic := uniqueTopic(t)
	mustGroup(t, s, topic, "g", StartFromLatest)
	allBytes := make([]byte, 256)
	for i := range allBytes {
		allBytes[i] = byte(i)
	}
	msgs := []*Message{
		newMsg(topic, "", ""),
		newMsg(topic, "", ""),
		newMsg(topic, "ключ-🔑", ""),
	}
	msgs[0].Payload = allBytes
	msgs[0].Headers = map[string]string{"quote": `it's "quoted" \ back`, "newline": "a\nb\r\n", "unicode": "héllo ✓"}
	msgs[1].Payload = nil
	msgs[1].Headers = nil
	msgs[2].Payload = []byte(`{"json":true,"nested":{"a":[1,2]}}`)
	for _, m := range msgs {
		mustAppend(t, s, m)
	}

	cs := mustClaim(t, s, topic, "g", 10, longLease)
	require.Len(t, cs, len(msgs))
	for i, want := range msgs {
		got := cs[i].Message
		assert.Equal(t, string(want.Payload), string(got.Payload), "payload %d", i)
		if len(want.Headers) == 0 {
			assert.Empty(t, got.Headers, "headers %d", i)
		} else {
			assert.Equal(t, want.Headers, got.Headers, "headers %d", i)
		}
		assert.Equal(t, want.Key, got.Key, "key %d", i)
	}
}

// contractBackfillOrder registers an Earliest group after a single batch and
// a run of separate publishes for one key: the backfill must replay them in
// publish order, one at a time.
func contractBackfillOrder(t *testing.T, s Store) {
	ctx := context.Background()
	topic := uniqueTopic(t)
	var batch []*Message
	for i := range 10 {
		batch = append(batch, newMsg(topic, "k", fmt.Sprint(i)))
	}
	mustAppend(t, s, batch...)
	for i := 10; i < 15; i++ {
		mustAppend(t, s, newMsg(topic, "k", fmt.Sprint(i)))
	}
	mustGroup(t, s, topic, "late", StartFromEarliest)

	var got []string
	for range 15 {
		cs := mustClaim(t, s, topic, "late", 10, longLease)
		require.Len(t, cs, 1, "one delivery per key at a time")
		got = append(got, string(cs[0].Message.Payload))
		held, err := s.Ack(ctx, cs[0])
		require.NoError(t, err)
		require.True(t, held)
	}
	want := make([]string, 15)
	for i := range want {
		want[i] = fmt.Sprint(i)
	}
	assert.Equal(t, want, got)
}
