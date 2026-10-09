package queue

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash/fnv"
	"slices"
	"sync"
	"time"

	"github.com/bytedance/sonic"
	"github.com/google/uuid"
	"github.com/maximhq/bifrost/core/schemas"
	"github.com/maximhq/bifrost/framework/logstore"
	"gorm.io/gorm"
)

const (
	// chStatusAcked marks an acknowledged delivery. ClickHouse cannot cheaply
	// delete one row, so Ack writes this tombstone and the janitor deletes it.
	chStatusAcked = "acked"
	// chAckedTombstoneAge is how long acked tombstones stay before the janitor
	// deletes them. It must outlast chRegistrationGrace, whose backfill relies
	// on the tombstone to know a delivery already happened.
	chAckedTombstoneAge = 10 * time.Minute
	// chRegistrationGrace is how long after a group registers that its claims
	// re-run the backfill, catching messages whose publisher read the group
	// list just before the group appeared.
	chRegistrationGrace = 30 * time.Second
	// chClaimBudget bounds waiting for the group lock plus the claim itself;
	// a claim that cannot get the lock in time returns nothing and retries
	// on the next poll.
	chClaimBudget = 10 * time.Second
	// chInitialVer is the version of a delivery's first row. Every state change
	// is written with now64(9), so a duplicate first row (a publisher retry, an
	// overlapping backfill) never outranks a claim, ack or retry.
	chInitialVer = "fromUnixTimestamp64Nano(1)"
	chNowMicros  = "toUnixTimestamp64Micro(now64(6))"
	// chRowLockStripes shards the in-process row locks.
	chRowLockStripes = 64
)

// chSchemaStore is the logstore hook that creates extension tables with the
// logstore's own cluster and replication settings.
type chSchemaStore interface {
	EnsureClickHouseTable(ctx context.Context, model any, table, partitionBy, orderBy, ttl string, skipIndexes []string) error
}

// clickhouseStore is the Store for the ClickHouse logstore. ClickHouse has no
// row locks or conditional updates, so every state change re-inserts the row
// into a ReplacingMergeTree with a newer `ver`, reads use FINAL, and claims
// for a (topic, group) are serialised with a distributed lock. Writes after a
// claim do not take the lock: they re-read the row and only the holder of
// its current claim token passes that check.
type clickhouseStore struct {
	db        *gorm.DB
	locker    logstore.DistributedLocker
	retention time.Duration
	logger    schemas.Logger
	now       func() time.Time // app clock; only orders messages within a publish
	graceTime time.Duration    // registration backfill window, chRegistrationGrace
	tombstone time.Duration    // minimum age before purge, chAckedTombstoneAge; must exceed graceTime

	mu     sync.Mutex
	groups map[string]chQueueGroup // topic/group -> registration, cached once seen

	rowLocks [chRowLockStripes]sync.Mutex // serialise this instance's read-then-insert on a delivery
}

// lockRows takes the row-lock stripes of ids in ascending order and returns
// their release. Without it, a heartbeat that read a delivery just before
// the handler acknowledged it would write the older, leased version back
// with a newer ver, and the delivery would run again.
func (s *clickhouseStore) lockRows(ids []string) func() {
	var stripes []int
	for _, id := range ids {
		h := fnv.New32a()
		_, _ = h.Write([]byte(id))
		stripe := int(h.Sum32() % chRowLockStripes)
		if !slices.Contains(stripes, stripe) {
			stripes = append(stripes, stripe)
		}
	}
	slices.Sort(stripes)
	for _, i := range stripes {
		s.rowLocks[i].Lock()
	}
	return func() {
		for _, i := range slices.Backward(stripes) {
			s.rowLocks[i].Unlock()
		}
	}
}

func claimIDs(cs []*Claimed) []string {
	ids := make([]string, len(cs))
	for i, c := range cs {
		ids[i] = c.DeliveryID
	}
	return ids
}

// newClickHouseStore creates the queue tables through the logstore and
// returns a store over its database. locker may be nil, which allows
// publishing but not consuming.
func newClickHouseStore(ctx context.Context, db *gorm.DB, schema chSchemaStore, locker logstore.DistributedLocker, retention time.Duration, logger schemas.Logger) (*clickhouseStore, error) {
	hours := max(int64(retention/time.Hour), 1)
	ttl := fmt.Sprintf("toDateTime(created_at) + INTERVAL %d HOUR", hours)
	tables := []struct {
		model   any
		name    string
		orderBy string
		ttl     string
	}{
		{&chQueueGroup{}, "queue_groups", "(topic, group_name)", ""},
		{&chQueueMessage{}, "queue_messages", "(topic, id)", ttl},
		// The sorting key is the identity ReplacingMergeTree deduplicates on, so
		// it must be exactly (topic, group, delivery id), never include state.
		{&chQueueDelivery{}, "queue_deliveries", "(topic, group_name, id)", ttl},
	}
	create := func() error {
		for _, t := range tables {
			if err := schema.EnsureClickHouseTable(ctx, t.model, t.name, "", t.orderBy, t.ttl, nil); err != nil {
				return err
			}
		}
		return nil
	}
	// The DDL is idempotent; the lock only keeps instances from racing ALTERs.
	if locker != nil {
		held, release, err := locker.Acquire(ctx, "queue:migrate")
		if err != nil {
			return nil, fmt.Errorf("queue: clickhouse migration lock: %w", err)
		}
		defer release()
		ctx = held
	}
	if err := create(); err != nil {
		return nil, fmt.Errorf("queue: create clickhouse tables: %w", err)
	}
	return &clickhouseStore{
		db:        db,
		locker:    locker,
		retention: retention,
		logger:    logger,
		now:       time.Now,
		graceTime: chRegistrationGrace,
		tombstone: chAckedTombstoneAge,
		groups:    map[string]chQueueGroup{},
	}, nil
}

// CanConsume reports whether Subscribe may run on this store.
func (s *clickhouseStore) CanConsume() error {
	if s.locker == nil {
		return fmt.Errorf("%w: consuming from the clickhouse logstore needs a shared distributed locker", ErrNoLocker)
	}
	return nil
}

// groupLockKey bounds the lock key whatever the name lengths.
func groupLockKey(topic, group string) string {
	sum := sha256.Sum256([]byte(topic + "\x00" + group))
	return "queue:" + hex.EncodeToString(sum[:16])
}

// liveWhere drops rows past retention that TTL has not physically removed
// yet, so a surviving older version can never be read back.
func (s *clickhouseStore) liveWhere() (string, int64) {
	return "created_at > now64(3) - toIntervalMicrosecond(?)", s.retention.Microseconds()
}

func (s *clickhouseStore) EnsureGroup(ctx context.Context, topic, group string, startFrom StartFrom) error {
	if err := s.CanConsume(); err != nil {
		return err
	}
	held, release, err := s.locker.Acquire(ctx, groupLockKey(topic, group))
	if err != nil {
		return fmt.Errorf("queue: lock group %s on %s: %w", group, topic, err)
	}
	defer release()
	if _, ok, err := s.loadGroup(held, topic, group); err != nil || ok {
		return err
	}
	if err := s.db.WithContext(held).Exec("INSERT INTO queue_groups (topic, group_name, start_from, created_at, ver) VALUES (?, ?, ?, now64(3), now64(9))",
		topic, group, string(startFrom)).Error; err != nil {
		return fmt.Errorf("register group: %w", err)
	}
	g, ok, err := s.loadGroup(held, topic, group)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("queue: group %s on %s missing right after insert", group, topic)
	}
	return s.backfill(held, g, false)
}

// loadGroup reads a group's registration, caching it once found.
func (s *clickhouseStore) loadGroup(ctx context.Context, topic, group string) (chQueueGroup, bool, error) {
	key := topic + "/" + group
	s.mu.Lock()
	g, ok := s.groups[key]
	s.mu.Unlock()
	if ok {
		return g, true, nil
	}
	var rows []chQueueGroup
	if err := s.db.WithContext(ctx).Raw("SELECT topic, group_name, start_from, created_at FROM queue_groups FINAL WHERE topic = ? AND group_name = ?",
		topic, group).Scan(&rows).Error; err != nil {
		return chQueueGroup{}, false, fmt.Errorf("load group: %w", err)
	}
	if len(rows) == 0 {
		return chQueueGroup{}, false, nil
	}
	s.mu.Lock()
	s.groups[key] = rows[0]
	s.mu.Unlock()
	return rows[0], true, nil
}

// backfill creates g's missing deliveries. At registration (grace false) an
// Earliest group takes every retained message. During the grace window
// (grace true) it re-checks the messages stored since just before the group
// existed, which a concurrent publisher may have fanned out without it. The
// caller holds the group lock.
func (s *clickhouseStore) backfill(ctx context.Context, g chQueueGroup, grace bool) error {
	live, liveArg := s.liveWhere()
	q := "SELECT id, topic, msg_key, seq FROM queue_messages FINAL WHERE topic = ? AND " + live
	args := []any{g.Topic, liveArg}
	switch {
	case grace && StartFrom(g.StartFrom) == StartFromEarliest:
		q += " AND created_at >= fromUnixTimestamp64Milli(?)"
		args = append(args, g.CreatedAt.Add(-s.graceTime).UnixMilli())
	case grace:
		// Latest groups only take messages stored after they registered.
		q += " AND created_at > fromUnixTimestamp64Milli(?)"
		args = append(args, g.CreatedAt.UnixMilli())
	case StartFrom(g.StartFrom) != StartFromEarliest:
		return nil
	}
	var msgs []chQueueMessage
	if err := s.db.WithContext(ctx).Raw(q+" ORDER BY seq, id", args...).Scan(&msgs).Error; err != nil {
		return fmt.Errorf("backfill scan: %w", err)
	}
	if len(msgs) == 0 {
		return nil
	}
	ids := make([]string, len(msgs))
	for i, m := range msgs {
		ids[i] = DeliveryID(m.ID, g.GroupName)
	}
	existing, err := s.existingIDs(ctx, "queue_deliveries", ids)
	if err != nil {
		return err
	}
	var dels []chQueueDelivery
	for _, m := range msgs {
		if id := DeliveryID(m.ID, g.GroupName); !existing[id] {
			dels = append(dels, newCHDelivery(m, g.GroupName))
		}
	}
	return s.insertDeliveries(ctx, dels, chInitialVer, true)
}

func newCHDelivery(m chQueueMessage, group string) chQueueDelivery {
	return chQueueDelivery{
		ID:        DeliveryID(m.ID, group),
		MessageID: m.ID,
		Topic:     m.Topic,
		GroupName: group,
		MsgKey:    m.MsgKey,
		Seq:       m.Seq,
		Status:    statusPending,
	}
}

// existingIDs returns which of ids are present in table.
func (s *clickhouseStore) existingIDs(ctx context.Context, table string, ids []string) (map[string]bool, error) {
	found := make(map[string]bool, len(ids))
	for chunk := range slices.Chunk(ids, sqlChunk) {
		var got []string
		if err := s.db.WithContext(ctx).Raw("SELECT id FROM "+table+" FINAL WHERE id IN ?", chunk).Scan(&got).Error; err != nil {
			return nil, fmt.Errorf("existence check on %s: %w", table, err)
		}
		for _, id := range got {
			found[id] = true
		}
	}
	return found, nil
}

// insertDeliveries writes one version of each row. ver is a SQL expression;
// fresh rows take created_at from the database clock, re-inserted versions
// carry their original created_at so every version expires together.
func (s *clickhouseStore) insertDeliveries(ctx context.Context, rows []chQueueDelivery, ver string, fresh bool) error {
	for chunk := range slices.Chunk(rows, sqlChunk) {
		var b sqlBuilder
		b.add("INSERT INTO queue_deliveries (id, message_id, topic, group_name, msg_key, seq, status, attempts, " +
			"next_attempt_at, claimed_by, claim_token, claimed_until, last_error, created_at, ver) VALUES ")
		for i, r := range chunk {
			if i > 0 {
				b.add(", ")
			}
			b.add("(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ", r.ID, r.MessageID, r.Topic, r.GroupName, r.MsgKey, r.Seq,
				r.Status, r.Attempts, r.NextAttemptAt, r.ClaimedBy, r.ClaimToken, r.ClaimedUntil, r.LastError)
			// Times are bound as epoch milliseconds: the driver formats a
			// time.Time parameter to whole seconds.
			if fresh {
				b.add("now64(3)")
			} else {
				b.add("fromUnixTimestamp64Milli(?)", r.CreatedAt.UnixMilli())
			}
			b.add(", " + ver + ")")
		}
		if err := s.db.WithContext(ctx).Exec(b.String(), b.args...).Error; err != nil {
			return fmt.Errorf("insert deliveries: %w", err)
		}
	}
	return nil
}

func (s *clickhouseStore) Append(ctx context.Context, msgs []*Message) error {
	if len(msgs) == 0 {
		return nil
	}
	seen := make(map[string]bool, len(msgs))
	var unique []*Message
	var ids, topics []string
	for _, m := range msgs {
		if seen[m.ID] {
			continue
		}
		seen[m.ID] = true
		unique = append(unique, m)
		ids = append(ids, m.ID)
		if !slices.Contains(topics, m.Topic) {
			topics = append(topics, m.Topic)
		}
	}
	// Republishing a known message must not fan it out again: its deliveries
	// may already be acknowledged.
	existing, err := s.existingIDs(ctx, "queue_messages", ids)
	if err != nil {
		return err
	}
	var groups []chQueueGroup
	if err := s.db.WithContext(ctx).Raw("SELECT topic, group_name FROM queue_groups FINAL WHERE topic IN ?", topics).Scan(&groups).Error; err != nil {
		return fmt.Errorf("load groups: %w", err)
	}

	base := s.now().UnixMicro() * 1000
	var rows []chQueueMessage
	for i, m := range unique {
		if existing[m.ID] {
			continue
		}
		headers := "{}"
		if len(m.Headers) > 0 {
			if headers, err = sonic.MarshalString(m.Headers); err != nil {
				return fmt.Errorf("encode headers of message %s: %w", m.ID, err)
			}
		}
		rows = append(rows, chQueueMessage{
			ID: m.ID, Topic: m.Topic, MsgKey: m.Key, Seq: base + int64(i%1000),
			Payload: string(m.Payload), Headers: headers, PublishedAt: m.PublishedAt,
		})
	}
	if len(rows) == 0 {
		return nil
	}
	// Messages before deliveries: a delivery never points at a missing message.
	for chunk := range slices.Chunk(rows, sqlChunk) {
		var b sqlBuilder
		b.add("INSERT INTO queue_messages (id, topic, msg_key, seq, payload, headers, published_at, created_at, ver) VALUES ")
		for i, r := range chunk {
			if i > 0 {
				b.add(", ")
			}
			// Payloads are hex-encoded on the wire so arbitrary bytes survive
			// the driver's client-side literal binding.
			b.add("(?, ?, ?, ?, unhex(?), ?, fromUnixTimestamp64Milli(?), now64(3), now64(9))", r.ID, r.Topic, r.MsgKey, r.Seq,
				hex.EncodeToString([]byte(r.Payload)), r.Headers, r.PublishedAt.UnixMilli())
		}
		if err := s.db.WithContext(ctx).Exec(b.String(), b.args...).Error; err != nil {
			return fmt.Errorf("insert messages: %w", err)
		}
	}
	var dels []chQueueDelivery
	for _, r := range rows {
		for _, g := range groups {
			if g.Topic == r.Topic {
				dels = append(dels, newCHDelivery(r, g.GroupName))
			}
		}
	}
	return s.insertDeliveries(ctx, dels, chInitialVer, true)
}

const chDeliveryColumns = "id, message_id, topic, group_name, msg_key, seq, status, attempts, next_attempt_at, " +
	"claimed_by, claim_token, claimed_until, last_error, created_at"

// dbNow reads the database clock in unix microseconds. Lease and retry
// deadlines are computed from it, never from this instance's clock.
func (s *clickhouseStore) dbNow(ctx context.Context) (int64, error) {
	var now int64
	if err := s.db.WithContext(ctx).Raw("SELECT " + chNowMicros).Scan(&now).Error; err != nil {
		return 0, fmt.Errorf("read clock: %w", err)
	}
	return now, nil
}

func (s *clickhouseStore) Claim(ctx context.Context, topic, group, runnerID string, max int, lease time.Duration) ([]*Claimed, error) {
	if err := s.CanConsume(); err != nil {
		return nil, err
	}
	if max <= 0 {
		return nil, nil
	}
	g, ok, err := s.loadGroup(ctx, topic, group)
	if err != nil || !ok {
		return nil, err
	}
	dbNow, err := s.dbNow(ctx)
	if err != nil {
		return nil, err
	}
	inGrace := dbNow-g.CreatedAt.UnixMicro() < s.graceTime.Microseconds()
	if !inGrace {
		// Skip the lock round-trip when nothing is due.
		due, err := s.countDue(ctx, topic, group)
		if err != nil || due == 0 {
			return nil, err
		}
	}

	budget, cancel := context.WithTimeout(ctx, chClaimBudget)
	defer cancel()
	held, release, err := s.locker.Acquire(budget, groupLockKey(topic, group))
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) && ctx.Err() == nil {
			return nil, nil // another instance is claiming; try next poll
		}
		return nil, fmt.Errorf("lock group %s on %s: %w", group, topic, err)
	}
	defer release()

	if inGrace {
		if err := s.backfill(held, g, true); err != nil {
			return nil, err
		}
	}

	// Re-read the clock under the lock: waiting for it may have taken a while.
	if dbNow, err = s.dbNow(held); err != nil {
		return nil, err
	}
	live, liveArg := s.liveWhere()
	var rows []chQueueDelivery
	if err := s.db.WithContext(held).Raw("SELECT "+chDeliveryColumns+" FROM queue_deliveries FINAL "+
		"WHERE topic = ? AND group_name = ? AND status IN ('pending', 'leased') AND "+live+" ORDER BY seq, id",
		topic, group, liveArg).Scan(&rows).Error; err != nil {
		return nil, fmt.Errorf("claim scan: %w", err)
	}

	// Same eligibility as the SQL stores: due, and the oldest live delivery
	// of its key.
	blocked := map[string]bool{}
	var picked []chQueueDelivery
	for _, r := range rows {
		if r.MsgKey != "" {
			if blocked[r.MsgKey] {
				continue
			}
			blocked[r.MsgKey] = true
		}
		if len(picked) >= max {
			continue
		}
		due := (r.Status == statusPending && r.NextAttemptAt <= dbNow) || (r.Status == statusLeased && r.ClaimedUntil < dbNow)
		if due {
			picked = append(picked, r)
		}
	}
	if len(picked) == 0 {
		return nil, nil
	}
	unlock := s.lockPicked(picked)
	defer unlock()

	msgIDs := make([]string, len(picked))
	for i, r := range picked {
		msgIDs[i] = r.MessageID
	}
	msgs, err := s.loadMessages(held, msgIDs)
	if err != nil {
		return nil, err
	}
	var leased []chQueueDelivery
	var out []*Claimed
	for _, r := range picked {
		m, ok := msgs[r.MessageID]
		if !ok {
			continue // its message has not landed yet; claim it on a later poll
		}
		d := r
		if d.Status == statusLeased {
			d.Attempts++ // the previous holder died mid-attempt
		}
		d.Status = statusLeased
		d.ClaimedBy = runnerID
		d.ClaimToken = uuid.NewString()
		d.ClaimedUntil = dbNow + lease.Microseconds()
		leased = append(leased, d)
		out = append(out, &Claimed{DeliveryID: d.ID, ClaimToken: d.ClaimToken, Group: group, Attempt: int(d.Attempts) + 1, Message: m})
	}
	if err := s.insertDeliveries(held, leased, "now64(9)", false); err != nil {
		return nil, err
	}
	return out, nil
}

// lockPicked serialises the claim's write with this instance's own
// heartbeats and acknowledgements of the same deliveries.
func (s *clickhouseStore) lockPicked(rows []chQueueDelivery) func() {
	ids := make([]string, len(rows))
	for i, r := range rows {
		ids[i] = r.ID
	}
	return s.lockRows(ids)
}

func (s *clickhouseStore) countDue(ctx context.Context, topic, group string) (int64, error) {
	live, liveArg := s.liveWhere()
	var n int64
	err := s.db.WithContext(ctx).Raw("SELECT count() FROM queue_deliveries FINAL WHERE topic = ? AND group_name = ? AND "+live+
		" AND ((status = 'pending' AND next_attempt_at <= "+chNowMicros+") OR (status = 'leased' AND claimed_until < "+chNowMicros+"))",
		topic, group, liveArg).Scan(&n).Error
	if err != nil {
		return 0, fmt.Errorf("count due: %w", err)
	}
	return n, nil
}

func (s *clickhouseStore) loadMessages(ctx context.Context, ids []string) (map[string]*Message, error) {
	out := make(map[string]*Message, len(ids))
	for chunk := range slices.Chunk(ids, sqlChunk) {
		var rows []chQueueMessage
		if err := s.db.WithContext(ctx).Raw("SELECT id, topic, msg_key, payload, headers, published_at FROM queue_messages FINAL WHERE id IN ?",
			chunk).Scan(&rows).Error; err != nil {
			return nil, fmt.Errorf("load messages: %w", err)
		}
		for _, r := range rows {
			m := &Message{ID: r.ID, Topic: r.Topic, Key: r.MsgKey, Payload: []byte(r.Payload), PublishedAt: r.PublishedAt.UTC()}
			if r.Headers != "" && r.Headers != "{}" {
				if err := sonic.UnmarshalString(r.Headers, &m.Headers); err != nil {
					return nil, fmt.Errorf("decode headers of message %s: %w", r.ID, err)
				}
			}
			out[r.ID] = m
		}
	}
	return out, nil
}

// heldRows returns the current versions of the deliveries whose claim tokens
// are still the ones given, keyed by claim token, and the database clock.
func (s *clickhouseStore) heldRows(ctx context.Context, cs []*Claimed) (map[string]chQueueDelivery, int64, error) {
	out := make(map[string]chQueueDelivery, len(cs))
	now, err := s.dbNow(ctx)
	if err != nil {
		return nil, 0, err
	}
	for chunk := range slices.Chunk(cs, sqlChunk) {
		ids := make([]string, len(chunk))
		for i, c := range chunk {
			ids[i] = c.DeliveryID
		}
		var rows []chQueueDelivery
		if err := s.db.WithContext(ctx).Raw("SELECT "+chDeliveryColumns+" FROM queue_deliveries FINAL WHERE id IN ?", ids).Scan(&rows).Error; err != nil {
			return nil, 0, fmt.Errorf("read held deliveries: %w", err)
		}
		for _, r := range rows {
			if r.Status == statusLeased && r.ClaimToken != "" {
				out[r.ClaimToken] = r
			}
		}
	}
	return out, now, nil
}

// transition re-reads c's delivery and, if c still holds it, writes the
// version change returns.
func (s *clickhouseStore) transition(ctx context.Context, c *Claimed, change func(d chQueueDelivery, now int64) chQueueDelivery) (bool, error) {
	defer s.lockRows([]string{c.DeliveryID})()
	held, now, err := s.heldRows(ctx, []*Claimed{c})
	if err != nil {
		return false, err
	}
	r, ok := held[c.ClaimToken]
	if !ok || r.ID != c.DeliveryID {
		return false, nil
	}
	if err := s.insertDeliveries(ctx, []chQueueDelivery{change(r, now)}, "now64(9)", false); err != nil {
		return false, err
	}
	return true, nil
}

func unlease(d *chQueueDelivery) {
	d.ClaimedBy = ""
	d.ClaimToken = ""
	d.ClaimedUntil = 0
}

func (s *clickhouseStore) Ack(ctx context.Context, c *Claimed) (bool, error) {
	return s.transition(ctx, c, func(d chQueueDelivery, _ int64) chQueueDelivery {
		d.Status = chStatusAcked
		unlease(&d)
		return d
	})
}

func (s *clickhouseStore) Retry(ctx context.Context, c *Claimed, backoff time.Duration, lastErr string) (bool, error) {
	return s.transition(ctx, c, func(d chQueueDelivery, now int64) chQueueDelivery {
		d.Status = statusPending
		d.Attempts++
		d.NextAttemptAt = now + backoff.Microseconds()
		d.LastError = lastErr
		unlease(&d)
		return d
	})
}

func (s *clickhouseStore) Kill(ctx context.Context, c *Claimed, lastErr string) (bool, error) {
	return s.transition(ctx, c, func(d chQueueDelivery, _ int64) chQueueDelivery {
		d.Status = statusDead
		d.Attempts++
		d.LastError = lastErr
		unlease(&d)
		return d
	})
}

func (s *clickhouseStore) Extend(ctx context.Context, cs []*Claimed, lease time.Duration) ([]string, error) {
	if len(cs) == 0 {
		return nil, nil
	}
	defer s.lockRows(claimIDs(cs))()
	held, now, err := s.heldRows(ctx, cs)
	if err != nil {
		return nil, err
	}
	var renewed []chQueueDelivery
	var ids []string
	for _, c := range cs {
		d, ok := held[c.ClaimToken]
		// An expired lease is not renewed: someone else may be entitled to it.
		if !ok || d.ClaimedUntil < now {
			continue
		}
		d.ClaimedUntil = now + lease.Microseconds()
		renewed = append(renewed, d)
		ids = append(ids, d.ID)
	}
	if err := s.insertDeliveries(ctx, renewed, "now64(9)", false); err != nil {
		return nil, err
	}
	return ids, nil
}

func (s *clickhouseStore) Release(ctx context.Context, cs []*Claimed) error {
	if len(cs) == 0 {
		return nil
	}
	defer s.lockRows(claimIDs(cs))()
	held, now, err := s.heldRows(ctx, cs)
	if err != nil {
		return err
	}
	var released []chQueueDelivery
	for _, c := range cs {
		if d, ok := held[c.ClaimToken]; ok {
			d.Status = statusPending
			d.NextAttemptAt = now
			unlease(&d)
			released = append(released, d)
		}
	}
	return s.insertDeliveries(ctx, released, "now64(9)", false)
}

// Purge deletes acked tombstones once they are no longer needed, and dead
// deliveries past retention. Lightweight deletes match on id, so every
// version of a row goes at once and no older version can resurface. Messages
// and undelivered backlog expire through the table TTL.
//
// Neither kind is deleted before the tombstone age: the registration
// backfill recreates any delivery it cannot find, so a row must outlive every
// backfill window that could still include its message.
func (s *clickhouseStore) Purge(ctx context.Context, retention time.Duration, batch int) (int64, error) {
	retention = max(retention, s.tombstone)
	var ids []string
	if err := s.db.WithContext(ctx).Raw("SELECT id FROM queue_deliveries FINAL WHERE "+
		"(status = ? AND ver < now64(9) - toIntervalMicrosecond(?)) OR (status = ? AND created_at < now64(3) - toIntervalMicrosecond(?)) LIMIT ?",
		chStatusAcked, s.tombstone.Microseconds(), statusDead, retention.Microseconds(), batch).Scan(&ids).Error; err != nil {
		return 0, fmt.Errorf("purge scan: %w", err)
	}
	if len(ids) == 0 {
		return 0, nil
	}
	if err := s.db.WithContext(ctx).Exec("DELETE FROM queue_deliveries WHERE id IN ? SETTINGS lightweight_deletes_sync = 1", ids).Error; err != nil {
		return 0, fmt.Errorf("purge delete: %w", err)
	}
	return int64(len(ids)), nil
}

func (s *clickhouseStore) Stats(ctx context.Context, topic, group string) (Stats, error) {
	live, liveArg := s.liveWhere()
	var rows []struct {
		Status string
		N      int64
	}
	if err := s.db.WithContext(ctx).Raw("SELECT status, count() AS n FROM queue_deliveries FINAL WHERE topic = ? AND group_name = ? AND "+live+" GROUP BY status",
		topic, group, liveArg).Scan(&rows).Error; err != nil {
		return Stats{}, fmt.Errorf("stats: %w", err)
	}
	var st Stats
	for _, r := range rows {
		switch r.Status {
		case statusPending:
			st.Pending = r.N
		case statusLeased:
			st.Leased = r.N
		case statusDead:
			st.Dead = r.N
		}
	}
	return st, nil
}

func (s *clickhouseStore) Ping(ctx context.Context) error {
	return s.db.WithContext(ctx).Exec("SELECT 1").Error
}

// Close does nothing: the database handle belongs to the logstore.
func (s *clickhouseStore) Close(context.Context) error { return nil }

var (
	_ Store          = (*clickhouseStore)(nil)
	_ ConsumeChecker = (*clickhouseStore)(nil)
)
