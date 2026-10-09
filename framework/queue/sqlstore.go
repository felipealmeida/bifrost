package queue

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/bytedance/sonic"
	"github.com/google/uuid"
	"github.com/maximhq/bifrost/core/schemas"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// sqlChunk bounds rows per multi-row statement, well under SQLite's and
// Postgres's bind-parameter limits.
const sqlChunk = 500

// sqlDialect isolates what differs between the SQL backends: where "now"
// comes from, and the row and advisory locks a shared database needs.
type sqlDialect interface {
	// nowMicros writes the current time, in unix microseconds, into b.
	nowMicros(b *sqlBuilder)
	// claimLock is appended to the claim's candidate subquery.
	claimLock() string
	// lockTopics locks topic rows in tx: shared for publishing, exclusive for
	// registering a group.
	lockTopics(tx *gorm.DB, topics []string, exclusive bool) error
	// migrate runs fn under whatever cross-instance lock migrations need.
	migrate(ctx context.Context, db *gorm.DB, fn func() error) error
	// purgeLock takes the janitor lock for the rest of tx and reports whether
	// this instance got it.
	purgeLock(tx *gorm.DB) (bool, error)
}

// sqliteDialect: a single process owns the database file, so the app clock is
// the database clock and SQLite's single writer replaces row locks.
type sqliteDialect struct{ now func() time.Time }

func (d sqliteDialect) nowMicros(b *sqlBuilder) { b.add("?", d.now().UnixMicro()) }
func (sqliteDialect) claimLock() string         { return "" }
func (sqliteDialect) lockTopics(*gorm.DB, []string, bool) error {
	return nil
}
func (sqliteDialect) migrate(_ context.Context, _ *gorm.DB, fn func() error) error { return fn() }
func (sqliteDialect) purgeLock(*gorm.DB) (bool, error)                             { return true, nil }

// sqlBuilder accumulates SQL text and its positional arguments together, so a
// fragment like "now" can carry its own bind values.
type sqlBuilder struct {
	sb   strings.Builder
	args []any
}

func (b *sqlBuilder) add(sql string, args ...any) *sqlBuilder {
	b.sb.WriteString(sql)
	b.args = append(b.args, args...)
	return b
}

func (b *sqlBuilder) String() string { return b.sb.String() }

// sqlStore is the Store for SQLite and Postgres. It borrows its *gorm.DB and
// never closes it.
type sqlStore struct {
	db      *gorm.DB
	dialect sqlDialect
	now     func() time.Time // stamps created_at; leases use the dialect clock
	logger  schemas.Logger
}

// newSQLStore migrates the queue tables into db and returns a store over it.
func newSQLStore(ctx context.Context, db *gorm.DB, logger schemas.Logger) (*sqlStore, error) {
	var d sqlDialect
	switch name := db.Dialector.Name(); name {
	case "sqlite":
		d = sqliteDialect{now: time.Now}
	default:
		return nil, fmt.Errorf("%w: sql dialect %q", ErrUnsupported, name)
	}
	s := &sqlStore{db: db, dialect: d, now: time.Now, logger: logger}
	if err := d.migrate(ctx, db, func() error { return runSQLMigrations(ctx, db, logger) }); err != nil {
		return nil, err
	}
	return s, nil
}

// stamp is a created_at value: UTC at microsecond precision, so SQLite's
// string comparisons and Postgres's timestamp precision agree.
func (s *sqlStore) stamp() time.Time {
	return s.now().UTC().Truncate(time.Microsecond)
}

func (s *sqlStore) EnsureGroup(ctx context.Context, topic, group string, startFrom StartFrom) error {
	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := s.ensureTopics(tx, []string{topic}); err != nil {
			return err
		}
		if err := s.dialect.lockTopics(tx, []string{topic}, true); err != nil {
			return err
		}
		res := tx.Clauses(clause.OnConflict{DoNothing: true}).Create(&tableQueueGroup{Topic: topic, GroupName: group, CreatedAt: s.stamp()})
		if res.Error != nil {
			return res.Error
		}
		if res.RowsAffected == 0 || startFrom != StartFromEarliest {
			return nil // existing group, or nothing to backfill
		}
		return s.backfill(tx, topic, group)
	})
}

// backfill creates group's deliveries for every message retained on topic,
// in publish order. The caller holds the topic lock, so no publish can slip
// between the backfill and the group becoming visible.
func (s *sqlStore) backfill(tx *gorm.DB, topic, group string) error {
	var after int64
	for {
		var msgs []tableQueueMessage
		if err := tx.Select("seq", "id", "topic", "msg_key").
			Where("topic = ? AND seq > ?", topic, after).
			Order("seq").Limit(sqlChunk).Find(&msgs).Error; err != nil {
			return fmt.Errorf("backfill scan: %w", err)
		}
		if len(msgs) == 0 {
			return nil
		}
		dels := make([]tableQueueDelivery, 0, len(msgs))
		for _, m := range msgs {
			dels = append(dels, s.newDelivery(m.ID, topic, group, m.MsgKey))
		}
		if err := tx.Clauses(clause.OnConflict{DoNothing: true}).Create(&dels).Error; err != nil {
			return fmt.Errorf("backfill insert: %w", err)
		}
		after = msgs[len(msgs)-1].Seq
	}
}

func (s *sqlStore) newDelivery(messageID, topic, group string, key *string) tableQueueDelivery {
	return tableQueueDelivery{
		ID:        DeliveryID(messageID, group),
		MessageID: messageID,
		Topic:     topic,
		GroupName: group,
		MsgKey:    key,
		Status:    statusPending,
		CreatedAt: s.stamp(),
		// NextAttemptAt 0 is always due.
	}
}

// ensureTopics inserts any missing topic rows.
func (s *sqlStore) ensureTopics(tx *gorm.DB, topics []string) error {
	rows := make([]tableQueueTopic, len(topics))
	now := s.stamp()
	for i, t := range topics {
		rows[i] = tableQueueTopic{Topic: t, CreatedAt: now}
	}
	return tx.Clauses(clause.OnConflict{DoNothing: true}).Create(&rows).Error
}

func (s *sqlStore) Append(ctx context.Context, msgs []*Message) error {
	if len(msgs) == 0 {
		return nil
	}
	// First occurrence wins, like ON CONFLICT DO NOTHING.
	seen := make(map[string]bool, len(msgs))
	unique := make([]*Message, 0, len(msgs))
	var topics []string
	for _, m := range msgs {
		if seen[m.ID] {
			continue
		}
		seen[m.ID] = true
		unique = append(unique, m)
		if !slices.Contains(topics, m.Topic) {
			topics = append(topics, m.Topic)
		}
	}
	slices.Sort(topics) // consistent lock order across publishers

	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := s.ensureTopics(tx, topics); err != nil {
			return err
		}
		// Groups are read only after the topic lock is held, so a group
		// registered concurrently either sees this publish in its backfill or
		// is seen here.
		if err := s.dialect.lockTopics(tx, topics, false); err != nil {
			return err
		}
		var groups []tableQueueGroup
		if err := tx.Where("topic IN ?", topics).Order("group_name").Find(&groups).Error; err != nil {
			return fmt.Errorf("load groups: %w", err)
		}
		inserted, err := s.insertMessages(tx, unique)
		if err != nil {
			return err
		}
		var dels []tableQueueDelivery
		for _, m := range unique {
			if !inserted[m.ID] {
				continue // already published; its deliveries keep their state
			}
			for _, g := range groups {
				if g.Topic == m.Topic {
					dels = append(dels, s.newDelivery(m.ID, m.Topic, g.GroupName, optionalString(m.Key)))
				}
			}
		}
		if len(dels) == 0 {
			return nil
		}
		return tx.Clauses(clause.OnConflict{DoNothing: true}).CreateInBatches(&dels, sqlChunk).Error
	})
}

// insertMessages inserts msgs and returns the IDs that were new. Only those
// get deliveries: re-publishing a message whose delivery was already
// acknowledged (and deleted) must not deliver it again.
func (s *sqlStore) insertMessages(tx *gorm.DB, msgs []*Message) (map[string]bool, error) {
	inserted := make(map[string]bool, len(msgs))
	now := s.stamp()
	for chunk := range slices.Chunk(msgs, sqlChunk) {
		var b sqlBuilder
		b.add("INSERT INTO queue_messages (id, topic, msg_key, payload, headers, published_at, created_at) VALUES ")
		for i, m := range chunk {
			headers := "{}"
			if len(m.Headers) > 0 {
				raw, err := sonic.MarshalString(m.Headers)
				if err != nil {
					return nil, fmt.Errorf("encode headers of message %s: %w", m.ID, err)
				}
				headers = raw
			}
			if i > 0 {
				b.add(", ")
			}
			b.add("(?, ?, ?, ?, ?, ?, ?)", m.ID, m.Topic, optionalString(m.Key), m.Payload, headers,
				m.PublishedAt.UTC().Truncate(time.Microsecond), now)
		}
		b.add(" ON CONFLICT (id) DO NOTHING RETURNING id")
		var ids []string
		if err := tx.Raw(b.String(), b.args...).Scan(&ids).Error; err != nil {
			return nil, fmt.Errorf("insert messages: %w", err)
		}
		for _, id := range ids {
			inserted[id] = true
		}
	}
	return inserted, nil
}

func optionalString(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

// claimedRow is what the claim's RETURNING clause yields.
type claimedRow struct {
	Seq        int64
	ID         string
	MessageID  string
	Attempts   int
	ClaimToken string
}

func (s *sqlStore) Claim(ctx context.Context, topic, group, runnerID string, max int, lease time.Duration) ([]*Claimed, error) {
	if max <= 0 {
		return nil, nil
	}
	// Each delivery gets its own token: the batch prefix plus the delivery ID.
	batch := uuid.NewString() + ":"
	var b sqlBuilder
	b.add("UPDATE queue_deliveries SET status = 'leased', claimed_by = ?, claim_token = ? || id, claimed_until = ", runnerID, batch)
	s.dialect.nowMicros(&b)
	b.add(" + ?, ", lease.Microseconds())
	// A lease that expired unfinished means its holder died mid-attempt.
	b.add("attempts = attempts + CASE WHEN status = 'leased' THEN 1 ELSE 0 END ")
	b.add("WHERE seq IN (SELECT d.seq FROM queue_deliveries d WHERE d.topic = ? AND d.group_name = ? ", topic, group)
	b.add("AND ((d.status = 'pending' AND d.next_attempt_at <= ")
	s.dialect.nowMicros(&b)
	b.add(") OR (d.status = 'leased' AND d.claimed_until < ")
	s.dialect.nowMicros(&b)
	b.add(")) ")
	// Only the oldest undead delivery of a key is eligible.
	b.add("AND (d.msg_key IS NULL OR NOT EXISTS (SELECT 1 FROM queue_deliveries d2 " +
		"WHERE d2.topic = d.topic AND d2.group_name = d.group_name AND d2.msg_key = d.msg_key " +
		"AND d2.status <> 'dead' AND d2.seq < d.seq)) ")
	b.add("ORDER BY d.seq LIMIT ?", max)
	if lock := s.dialect.claimLock(); lock != "" {
		b.add(" " + lock)
	}
	b.add(") RETURNING seq, id, message_id, attempts, claim_token")

	var rows []claimedRow
	if err := s.db.WithContext(ctx).Raw(b.String(), b.args...).Scan(&rows).Error; err != nil {
		return nil, fmt.Errorf("claim: %w", err)
	}
	if len(rows) == 0 {
		return nil, nil
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].Seq < rows[j].Seq })

	ids := make([]string, len(rows))
	for i, r := range rows {
		ids[i] = r.MessageID
	}
	msgs, err := s.loadMessages(ctx, ids)
	if err != nil {
		// The leases expire on their own; nothing is lost.
		return nil, err
	}
	out := make([]*Claimed, 0, len(rows))
	var orphaned []*Claimed
	for _, r := range rows {
		c := &Claimed{DeliveryID: r.ID, ClaimToken: r.ClaimToken, Group: group, Attempt: r.Attempts + 1}
		if m, ok := msgs[r.MessageID]; ok {
			c.Message = m
			out = append(out, c)
		} else {
			orphaned = append(orphaned, c)
		}
	}
	for _, c := range orphaned {
		// Unreachable unless a message row was removed by hand.
		if _, err := s.Kill(ctx, c, "queue: message row missing"); err != nil && s.logger != nil {
			s.logger.Warn("queue: failed to dead-letter orphaned delivery %s: %v", c.DeliveryID, err)
		}
	}
	return out, nil
}

// messageRow is a queue_messages row as read back for a claim.
type messageRow struct {
	ID          string
	Topic       string
	MsgKey      *string
	Payload     []byte
	Headers     string
	PublishedAt time.Time
}

func (s *sqlStore) loadMessages(ctx context.Context, ids []string) (map[string]*Message, error) {
	out := make(map[string]*Message, len(ids))
	for chunk := range slices.Chunk(ids, sqlChunk) {
		var rows []messageRow
		if err := s.db.WithContext(ctx).Table("queue_messages").
			Select("id", "topic", "msg_key", "payload", "headers", "published_at").
			Where("id IN ?", chunk).Scan(&rows).Error; err != nil {
			return nil, fmt.Errorf("load messages: %w", err)
		}
		for _, r := range rows {
			m := &Message{ID: r.ID, Topic: r.Topic, Payload: r.Payload, PublishedAt: r.PublishedAt.UTC()}
			if r.MsgKey != nil {
				m.Key = *r.MsgKey
			}
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

// heldWhere is the fence every post-claim write applies.
const heldWhere = "id = ? AND claim_token = ? AND status = 'leased'"

func (s *sqlStore) Ack(ctx context.Context, c *Claimed) (bool, error) {
	res := s.db.WithContext(ctx).Exec("DELETE FROM queue_deliveries WHERE "+heldWhere, c.DeliveryID, c.ClaimToken)
	return res.RowsAffected == 1, res.Error
}

func (s *sqlStore) Retry(ctx context.Context, c *Claimed, backoff time.Duration, lastErr string) (bool, error) {
	var b sqlBuilder
	b.add("UPDATE queue_deliveries SET status = 'pending', attempts = attempts + 1, next_attempt_at = ")
	s.dialect.nowMicros(&b)
	b.add(" + ?, last_error = ?, claimed_by = NULL, claim_token = NULL, claimed_until = NULL WHERE "+heldWhere,
		backoff.Microseconds(), lastErr, c.DeliveryID, c.ClaimToken)
	res := s.db.WithContext(ctx).Exec(b.String(), b.args...)
	return res.RowsAffected == 1, res.Error
}

func (s *sqlStore) Kill(ctx context.Context, c *Claimed, lastErr string) (bool, error) {
	res := s.db.WithContext(ctx).Exec("UPDATE queue_deliveries SET status = 'dead', attempts = attempts + 1, last_error = ?, "+
		"claimed_by = NULL, claim_token = NULL, claimed_until = NULL WHERE "+heldWhere, lastErr, c.DeliveryID, c.ClaimToken)
	return res.RowsAffected == 1, res.Error
}

func (s *sqlStore) Extend(ctx context.Context, cs []*Claimed, lease time.Duration) ([]string, error) {
	held := make([]string, 0, len(cs))
	for chunk := range slices.Chunk(cs, sqlChunk) {
		tokens := make([]string, len(chunk))
		for i, c := range chunk {
			tokens[i] = c.ClaimToken
		}
		var b sqlBuilder
		b.add("UPDATE queue_deliveries SET claimed_until = ")
		s.dialect.nowMicros(&b)
		// An expired lease is not renewed: someone else may be entitled to it.
		b.add(" + ? WHERE claim_token IN ? AND status = 'leased' AND claimed_until >= ", lease.Microseconds(), tokens)
		s.dialect.nowMicros(&b)
		b.add(" RETURNING id")
		var ids []string
		if err := s.db.WithContext(ctx).Raw(b.String(), b.args...).Scan(&ids).Error; err != nil {
			return nil, fmt.Errorf("extend: %w", err)
		}
		held = append(held, ids...)
	}
	return held, nil
}

func (s *sqlStore) Release(ctx context.Context, cs []*Claimed) error {
	for chunk := range slices.Chunk(cs, sqlChunk) {
		tokens := make([]string, len(chunk))
		for i, c := range chunk {
			tokens[i] = c.ClaimToken
		}
		var b sqlBuilder
		b.add("UPDATE queue_deliveries SET status = 'pending', next_attempt_at = ")
		s.dialect.nowMicros(&b)
		b.add(", claimed_by = NULL, claim_token = NULL, claimed_until = NULL WHERE claim_token IN ? AND status = 'leased'", tokens)
		if err := s.db.WithContext(ctx).Exec(b.String(), b.args...).Error; err != nil {
			return fmt.Errorf("release: %w", err)
		}
	}
	return nil
}

func (s *sqlStore) Purge(ctx context.Context, retention time.Duration, batch int) (int64, error) {
	cutoff := s.stamp().Add(-retention)
	var removed int64
	err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		ok, err := s.dialect.purgeLock(tx)
		if err != nil || !ok {
			return err // another instance is purging
		}
		res := tx.Exec("DELETE FROM queue_deliveries WHERE seq IN (SELECT seq FROM queue_deliveries "+
			"WHERE status = 'dead' AND created_at < ? ORDER BY seq LIMIT ?)", cutoff, batch)
		if res.Error != nil {
			return fmt.Errorf("purge dead deliveries: %w", res.Error)
		}
		removed += res.RowsAffected
		// A message is orphaned once no delivery still needs it.
		res = tx.Exec("DELETE FROM queue_messages WHERE seq IN (SELECT m.seq FROM queue_messages m "+
			"WHERE m.created_at < ? AND NOT EXISTS (SELECT 1 FROM queue_deliveries d "+
			"WHERE d.message_id = m.id AND d.status <> 'dead') ORDER BY m.seq LIMIT ?)", cutoff, batch)
		if res.Error != nil {
			return fmt.Errorf("purge messages: %w", res.Error)
		}
		removed += res.RowsAffected
		return nil
	})
	return removed, err
}

func (s *sqlStore) Stats(ctx context.Context, topic, group string) (Stats, error) {
	var rows []struct {
		Status string
		N      int64
	}
	if err := s.db.WithContext(ctx).Table("queue_deliveries").Select("status, COUNT(*) AS n").
		Where("topic = ? AND group_name = ?", topic, group).Group("status").Scan(&rows).Error; err != nil {
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

func (s *sqlStore) Ping(ctx context.Context) error {
	sqlDB, err := s.db.DB()
	if err != nil {
		return err
	}
	return sqlDB.PingContext(ctx)
}

// Close does nothing: the database handle belongs to the logstore.
func (s *sqlStore) Close(context.Context) error { return nil }

var _ Store = (*sqlStore)(nil)
