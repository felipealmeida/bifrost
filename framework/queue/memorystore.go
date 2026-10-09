package queue

import (
	"context"
	"maps"
	"sync"
	"time"

	"github.com/google/uuid"
)

// Delivery states shared by every backend. Acknowledged deliveries are
// deleted, except on ClickHouse, which writes an acked tombstone.
const (
	statusPending = "pending"
	statusLeased  = "leased"
	statusDead    = "dead"
)

// memoryStore is an in-process Store. It has the same semantics as the
// database stores and backs the engine tests and single-instance queues.
type memoryStore struct {
	mu       sync.Mutex
	now      func() time.Time
	closed   bool
	seq      int64
	groups   map[string]map[string]bool       // topic -> group -> registered
	messages map[string]*memoryMessage        // message id -> message
	byTopic  map[string][]*memoryMessage      // topic -> messages in publish order
	queues   map[memoryGroupKey][]*memoryItem // (topic, group) -> deliveries in seq order
	items    map[string]*memoryItem           // delivery id -> delivery
}

type memoryGroupKey struct{ topic, group string }

type memoryMessage struct {
	msg      Message
	storedAt time.Time
}

type memoryItem struct {
	id            string
	seq           int64
	group         string
	message       *memoryMessage
	status        string
	attempts      int
	nextAttemptAt time.Time
	claimedBy     string
	claimToken    string
	claimedUntil  time.Time
	lastError     string
	createdAt     time.Time
	removed       bool // acked or purged; dropped from its queue on the next scan
}

func newMemoryStore() *memoryStore {
	return &memoryStore{
		now:      time.Now,
		groups:   map[string]map[string]bool{},
		messages: map[string]*memoryMessage{},
		byTopic:  map[string][]*memoryMessage{},
		queues:   map[memoryGroupKey][]*memoryItem{},
		items:    map[string]*memoryItem{},
	}
}

func (s *memoryStore) EnsureGroup(ctx context.Context, topic, group string, startFrom StartFrom) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosed
	}
	if s.groups[topic] == nil {
		s.groups[topic] = map[string]bool{}
	}
	if s.groups[topic][group] {
		return nil
	}
	s.groups[topic][group] = true
	if startFrom == StartFromEarliest {
		for _, m := range s.byTopic[topic] {
			s.addItemLocked(m, group)
		}
	}
	return nil
}

func (s *memoryStore) Append(ctx context.Context, msgs []*Message) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosed
	}
	now := s.now()
	for _, in := range msgs {
		if _, exists := s.messages[in.ID]; exists {
			continue
		}
		m := &memoryMessage{msg: *in, storedAt: now}
		m.msg.Payload = append([]byte(nil), in.Payload...)
		m.msg.Headers = maps.Clone(in.Headers)
		s.messages[in.ID] = m
		s.byTopic[in.Topic] = append(s.byTopic[in.Topic], m)
		for group := range s.groups[in.Topic] {
			s.addItemLocked(m, group)
		}
	}
	return nil
}

// addItemLocked creates m's delivery to group unless it already exists.
func (s *memoryStore) addItemLocked(m *memoryMessage, group string) {
	id := DeliveryID(m.msg.ID, group)
	if _, exists := s.items[id]; exists {
		return
	}
	s.seq++
	now := s.now()
	it := &memoryItem{
		id:            id,
		seq:           s.seq,
		group:         group,
		message:       m,
		status:        statusPending,
		nextAttemptAt: now,
		createdAt:     now,
	}
	s.items[id] = it
	k := memoryGroupKey{m.msg.Topic, group}
	s.queues[k] = append(s.queues[k], it)
}

func (s *memoryStore) Claim(ctx context.Context, topic, group, runnerID string, max int, lease time.Duration) ([]*Claimed, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, ErrClosed
	}
	if max <= 0 {
		return nil, nil
	}
	now := s.now()
	k := memoryGroupKey{topic, group}
	queue := s.queues[k]
	live := queue[:0]
	blockedKeys := map[string]bool{}
	var out []*Claimed
	for _, it := range queue {
		if it.removed {
			continue
		}
		live = append(live, it)
		if it.status == statusDead {
			continue
		}
		// Only the oldest pending-or-leased delivery of a key is eligible.
		if key := it.message.msg.Key; key != "" {
			if blockedKeys[key] {
				continue
			}
			blockedKeys[key] = true
		}
		if len(out) >= max {
			continue
		}
		switch {
		case it.status == statusPending && !it.nextAttemptAt.After(now):
		case it.status == statusLeased && it.claimedUntil.Before(now):
			// The previous holder died without finishing: that attempt is spent.
			it.attempts++
		default:
			continue
		}
		it.status = statusLeased
		it.claimedBy = runnerID
		it.claimToken = uuid.NewString()
		it.claimedUntil = now.Add(lease)
		out = append(out, s.claimedLocked(it))
	}
	clear(queue[len(live):])
	s.queues[k] = live
	return out, nil
}

func (s *memoryStore) claimedLocked(it *memoryItem) *Claimed {
	msg := it.message.msg
	msg.Payload = append([]byte(nil), msg.Payload...)
	msg.Headers = maps.Clone(msg.Headers)
	return &Claimed{
		DeliveryID: it.id,
		ClaimToken: it.claimToken,
		Group:      it.group,
		Attempt:    it.attempts + 1,
		Message:    &msg,
	}
}

// heldLocked returns c's delivery when c still holds its lease.
func (s *memoryStore) heldLocked(c *Claimed) *memoryItem {
	it := s.items[c.DeliveryID]
	if it == nil || it.removed || it.status != statusLeased || it.claimToken != c.ClaimToken {
		return nil
	}
	return it
}

func (s *memoryStore) Ack(ctx context.Context, c *Claimed) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return false, ErrClosed
	}
	it := s.heldLocked(c)
	if it == nil {
		return false, nil
	}
	it.removed = true
	delete(s.items, it.id)
	return true, nil
}

func (s *memoryStore) Retry(ctx context.Context, c *Claimed, backoff time.Duration, lastErr string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return false, ErrClosed
	}
	it := s.heldLocked(c)
	if it == nil {
		return false, nil
	}
	it.attempts++
	it.status = statusPending
	it.nextAttemptAt = s.now().Add(backoff)
	it.lastError = lastErr
	it.unlease()
	return true, nil
}

func (s *memoryStore) Kill(ctx context.Context, c *Claimed, lastErr string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return false, ErrClosed
	}
	it := s.heldLocked(c)
	if it == nil {
		return false, nil
	}
	it.attempts++
	it.status = statusDead
	it.lastError = lastErr
	it.unlease()
	return true, nil
}

func (s *memoryStore) Extend(ctx context.Context, cs []*Claimed, lease time.Duration) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, ErrClosed
	}
	now := s.now()
	held := make([]string, 0, len(cs))
	for _, c := range cs {
		// An expired lease is not renewed: another consumer may already be
		// entitled to the delivery.
		if it := s.heldLocked(c); it != nil && !it.claimedUntil.Before(now) {
			it.claimedUntil = now.Add(lease)
			held = append(held, c.DeliveryID)
		}
	}
	return held, nil
}

func (s *memoryStore) Release(ctx context.Context, cs []*Claimed) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosed
	}
	now := s.now()
	for _, c := range cs {
		if it := s.heldLocked(c); it != nil {
			it.status = statusPending
			it.nextAttemptAt = now
			it.unlease()
		}
	}
	return nil
}

func (it *memoryItem) unlease() {
	it.claimedBy = ""
	it.claimToken = ""
	it.claimedUntil = time.Time{}
}

func (s *memoryStore) Purge(ctx context.Context, retention time.Duration, batch int) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return 0, ErrClosed
	}
	cutoff := s.now().Add(-retention)
	var removed int64
	for id, it := range s.items {
		if removed >= int64(batch) {
			return removed, nil
		}
		if it.status == statusDead && it.createdAt.Before(cutoff) {
			it.removed = true
			delete(s.items, id)
			removed++
		}
	}
	// A message is orphaned once no delivery still needs it.
	live := map[string]bool{}
	for _, it := range s.items {
		if it.status != statusDead {
			live[it.message.msg.ID] = true
		}
	}
	for topic, msgs := range s.byTopic {
		kept := msgs[:0]
		for _, m := range msgs {
			if removed < int64(batch) && m.storedAt.Before(cutoff) && !live[m.msg.ID] {
				delete(s.messages, m.msg.ID)
				removed++
				continue
			}
			kept = append(kept, m)
		}
		clear(msgs[len(kept):])
		s.byTopic[topic] = kept
	}
	return removed, nil
}

func (s *memoryStore) Stats(ctx context.Context, topic, group string) (Stats, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return Stats{}, ErrClosed
	}
	var st Stats
	for _, it := range s.queues[memoryGroupKey{topic, group}] {
		if it.removed {
			continue
		}
		switch it.status {
		case statusPending:
			st.Pending++
		case statusLeased:
			st.Leased++
		case statusDead:
			st.Dead++
		}
	}
	return st, nil
}

func (s *memoryStore) Ping(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosed
	}
	return nil
}

func (s *memoryStore) Close(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	return nil
}
