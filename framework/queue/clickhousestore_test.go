package queue

import (
	"context"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	bifrost "github.com/maximhq/bifrost/core"
	"github.com/maximhq/bifrost/core/schemas"
	"github.com/maximhq/bifrost/framework/configstore"
	"github.com/maximhq/bifrost/framework/logstore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

// clickhouseTestConfig targets the tests/docker-compose.yml ClickHouse, with
// the same overrides the logstore suite honours.
func clickhouseTestConfig(t *testing.T) *logstore.ClickHouseConfig {
	t.Helper()
	env := func(key, def string) string {
		if v := os.Getenv(key); v != "" {
			return v
		}
		return def
	}
	cfg := &logstore.ClickHouseConfig{
		Host:     schemas.NewSecretVar(env("BIFROST_TEST_CLICKHOUSE_HOST", "localhost")),
		Port:     schemas.NewSecretVar(env("BIFROST_TEST_CLICKHOUSE_PORT", "9001")),
		Database: schemas.NewSecretVar(env("BIFROST_TEST_CLICKHOUSE_DB", "bifrost")),
		Username: schemas.NewSecretVar(env("BIFROST_TEST_CLICKHOUSE_USER", "bifrost")),
		Password: schemas.NewSecretVar(env("BIFROST_TEST_CLICKHOUSE_PASSWORD", "bifrost_password")),
	}
	if os.Getenv("BIFROST_TEST_CLICKHOUSE_DB") != "" && !strings.Contains(strings.ToLower(cfg.Database.GetValue()), "test") {
		t.Fatalf("refusing to drop queue tables in ClickHouse database %q: overrides must name a database containing \"test\"", cfg.Database.GetValue())
	}
	return cfg
}

type clickhouseTestEnv struct {
	logStore logstore.LogStore
	db       *gorm.DB
	schema   chSchemaStore
}

// setupClickHouse opens the logstore and drops the queue tables so each test
// creates them from scratch. It skips when ClickHouse is not running locally
// and fails in CI, where the compose service is expected to be up.
func setupClickHouse(t *testing.T) clickhouseTestEnv {
	t.Helper()
	ls, err := logstore.NewLogStore(context.Background(), &logstore.Config{
		Enabled: true,
		Type:    logstore.LogStoreTypeClickHouse,
		Config:  clickhouseTestConfig(t),
	}, bifrost.NewNoOpLogger())
	if err != nil {
		if os.Getenv("CI") != "" {
			t.Fatalf("ClickHouse not available in CI: %v", err)
		}
		t.Skipf("ClickHouse not available: %v", err)
	}
	t.Cleanup(func() { _ = ls.Close(context.Background()) })
	db := ls.(scopedDBStore).ScopedDB(context.Background())
	for _, table := range []string{"queue_deliveries", "queue_messages", "queue_groups"} {
		require.NoError(t, db.Exec("DROP TABLE IF EXISTS "+table+" SYNC").Error)
	}
	return clickhouseTestEnv{logStore: ls, db: db, schema: ls.(chSchemaStore)}
}

func (e clickhouseTestEnv) store(t *testing.T, locker logstore.DistributedLocker) *clickhouseStore {
	t.Helper()
	s, err := newClickHouseStore(context.Background(), e.db, e.schema, locker, DefaultRetentionHours*time.Hour, nil)
	require.NoError(t, err)
	return s
}

func TestClickHouseStoreContract(t *testing.T) {
	env := setupClickHouse(t)
	locker := NewProcessLocker()
	runStoreContract(t, func(t *testing.T) Store { return env.store(t, locker) })
}

// TestClickHouseDuplicateFirstRowNeverOutranksAClaim replays a delivery's
// first row after it was claimed and acknowledged, as a retried publish or
// an overlapping backfill would. ReplacingMergeTree keeps the highest ver,
// so the replay must carry a lower one than any state change.
func TestClickHouseDuplicateFirstRowNeverOutranksAClaim(t *testing.T) {
	env := setupClickHouse(t)
	s := env.store(t, NewProcessLocker())
	ctx := context.Background()
	mustGroup(t, s, "replay", "g", StartFromLatest)
	m := newMsg("replay", "", "x")
	mustAppend(t, s, m)
	cs := mustClaim(t, s, "replay", "g", 1, longLease)
	require.Len(t, cs, 1)
	held, err := s.Ack(ctx, cs[0])
	require.NoError(t, err)
	require.True(t, held)

	first := newCHDelivery(chQueueMessage{ID: m.ID, Topic: m.Topic}, "g")
	require.NoError(t, s.insertDeliveries(ctx, []chQueueDelivery{first}, chInitialVer, true))

	st, err := s.Stats(ctx, "replay", "g")
	require.NoError(t, err)
	assert.Equal(t, Stats{}, st)
	assert.Empty(t, mustClaim(t, s, "replay", "g", 1, longLease))
}

func TestClickHousePublishWithoutLockerConsumeRefused(t *testing.T) {
	env := setupClickHouse(t)
	q, err := NewQueue(context.Background(), &Config{Enabled: true, Type: QueueTypeLogStore}, Dependencies{LogStore: env.logStore}, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = q.Close(context.Background()) })

	require.NoError(t, q.Publish(context.Background(), &Message{Topic: "nolock", Payload: []byte("x")}))
	_, err = q.Subscribe(context.Background(), "nolock", "g", func(context.Context, *Delivery) error { return nil })
	assert.ErrorIs(t, err, ErrNoLocker)
}

func TestClickHouseLogStoreQueueEndToEnd(t *testing.T) {
	env := setupClickHouse(t)
	q, err := NewQueue(context.Background(), &Config{Enabled: true, Type: QueueTypeLogStore},
		Dependencies{LogStore: env.logStore, Locker: NewProcessLocker()}, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = q.Close(context.Background()) })

	got := make(chan *Delivery, 1)
	subscribe(t, q, "events", "consumer", func(ctx context.Context, d *Delivery) error {
		got <- d
		return nil
	})
	publish(t, q, &Message{Topic: "events", Key: "k", Payload: []byte("hello")})
	select {
	case d := <-got:
		assert.Equal(t, []byte("hello"), d.Payload)
		assert.Equal(t, "k", d.Key)
	case <-time.After(10 * time.Second):
		t.Fatal("message not delivered")
	}
	waitStats(t, q, "events", "consumer", Stats{})
}

// TestClickHouseMultiInstance runs the simulation with a real
// configstore.DistributedLockManager per instance, all backed by one shared
// lock table, which is how separate pods coordinate in production.
func TestClickHouseMultiInstance(t *testing.T) {
	env := setupClickHouse(t)
	cs, err := configstore.NewConfigStore(context.Background(), &configstore.Config{
		Enabled: true,
		Type:    configstore.ConfigStoreTypeSQLite,
		Config:  &configstore.SQLiteConfig{Path: filepath.Join(t.TempDir(), "config.db")},
	}, bifrost.NewNoOpLogger())
	require.NoError(t, err)
	t.Cleanup(func() { _ = cs.Close(context.Background()) })

	runMultiInstanceSimulation(t, func(t *testing.T, logger schemas.Logger) Queue {
		locker := configstore.NewDistributedLockManager(cs, bifrost.NewNoOpLogger(), configstore.WithDefaultTTL(30*time.Second),
			configstore.WithRetryInterval(20*time.Millisecond))
		q, err := NewStoreQueue(env.store(t, locker), EngineConfig{}, logger)
		require.NoError(t, err)
		return q
	}, 3, 150)
}

// TestClickHousePurgeNeverRevivesWithinGrace kills a delivery and purges
// straight away, inside the group's registration backfill window. Deleting
// the dead row there would let the backfill recreate it as pending.
func TestClickHousePurgeNeverRevivesWithinGrace(t *testing.T) {
	env := setupClickHouse(t)
	s := env.store(t, NewProcessLocker())
	ctx := context.Background()
	mustGroup(t, s, "revive", "g", StartFromEarliest)
	mustAppend(t, s, newMsg("revive", "", "x"))
	cs := mustClaim(t, s, "revive", "g", 1, longLease)
	require.Len(t, cs, 1)
	held, err := s.Kill(ctx, cs[0], "boom")
	require.NoError(t, err)
	require.True(t, held)

	n, err := s.Purge(ctx, time.Millisecond, 100)
	require.NoError(t, err)
	assert.Zero(t, n, "young dead rows are kept past the backfill window")
	assert.Empty(t, mustClaim(t, s, "revive", "g", 1, longLease))
}

func TestClickHouseEngineContract(t *testing.T) {
	env := setupClickHouse(t)
	locker := NewProcessLocker()
	runEngineContract(t, func(t *testing.T, n int) []Store {
		out := make([]Store, n)
		for i := range out {
			// Through the adapter, as the gateway builds it.
			s, err := newLogStoreBackend(context.Background(), Dependencies{LogStore: env.logStore, Locker: locker}, EngineConfig{}.WithDefaults(), nil)
			require.NoError(t, err)
			out[i] = s
		}
		return out
	})
}

// TestClickHouseLeasesUseDatabaseClock gives one instance a clock an hour
// fast. Leases come from now64(), so it cannot take over a live lease.
func TestClickHouseLeasesUseDatabaseClock(t *testing.T) {
	env := setupClickHouse(t)
	locker := NewProcessLocker()
	a, b := env.store(t, locker), env.store(t, locker)
	b.now = func() time.Time { return time.Now().Add(time.Hour) }

	mustGroup(t, a, "clock", "g", StartFromLatest)
	mustAppend(t, b, newMsg("clock", "", "x")) // published by the skewed instance
	require.Len(t, mustClaim(t, a, "clock", "g", 1, time.Minute), 1)
	assert.Empty(t, mustClaim(t, b, "clock", "g", 1, time.Minute))
}

// TestClickHousePurgeDeletesOldTombstonesAndDead shortens the ages so the
// janitor's deletions can be observed: acked tombstones and dead rows are
// removed once old enough, every version at once, and nothing comes back.
func TestClickHousePurgeDeletesOldTombstonesAndDead(t *testing.T) {
	env := setupClickHouse(t)
	s := env.store(t, NewProcessLocker())
	s.graceTime = 100 * time.Millisecond
	s.tombstone = 400 * time.Millisecond
	ctx := context.Background()
	mustGroup(t, s, "purge", "g", StartFromLatest)
	mustAppend(t, s, newMsg("purge", "", "acked"), newMsg("purge", "", "dead"))
	cs := mustClaim(t, s, "purge", "g", 2, longLease)
	require.Len(t, cs, 2)
	held, err := s.Ack(ctx, cs[0])
	require.NoError(t, err)
	require.True(t, held)
	held, err = s.Kill(ctx, cs[1], "boom")
	require.NoError(t, err)
	require.True(t, held)

	n, err := s.Purge(ctx, time.Millisecond, 100)
	require.NoError(t, err)
	assert.Zero(t, n, "younger than the tombstone age")

	time.Sleep(s.tombstone + 200*time.Millisecond)
	n, err = s.Purge(ctx, time.Millisecond, 100)
	require.NoError(t, err)
	assert.Equal(t, int64(2), n)

	var rows int64
	require.NoError(t, env.db.Raw("SELECT count() FROM queue_deliveries WHERE id IN ?",
		[]string{cs[0].DeliveryID, cs[1].DeliveryID}).Scan(&rows).Error)
	assert.Zero(t, rows, "every version of both rows is gone")
	st, err := s.Stats(ctx, "purge", "g")
	require.NoError(t, err)
	assert.Equal(t, Stats{}, st)
	assert.Empty(t, mustClaim(t, s, "purge", "g", 10, longLease), "nothing is revived")
}

// insertRacedMessage stores a message without any deliveries, as a publisher
// that read the group list just before a group registered would leave it.
func insertRacedMessage(t *testing.T, s *clickhouseStore, topic, payload string) string {
	t.Helper()
	id := uuid.NewString()
	require.NoError(t, s.db.Exec("INSERT INTO queue_messages (id, topic, msg_key, seq, payload, headers, published_at, created_at, ver) "+
		"VALUES (?, ?, '', ?, unhex(?), '{}', now64(3), now64(3), now64(9))",
		id, topic, time.Now().UnixMicro()*1000, hex.EncodeToString([]byte(payload))).Error)
	return id
}

// TestClickHouseRegistrationGraceCatchesRacedPublish pins the window during
// which a new group's claims re-run the backfill: a message whose publisher
// missed the group is delivered inside the window, for Latest and Earliest
// groups, and is no longer looked for once the window has closed.
func TestClickHouseRegistrationGraceCatchesRacedPublish(t *testing.T) {
	env := setupClickHouse(t)
	locker := NewProcessLocker()

	for _, from := range []StartFrom{StartFromLatest, StartFromEarliest} {
		s := env.store(t, locker)
		topic := "grace-" + string(from)
		mustGroup(t, s, topic, "g", from)
		insertRacedMessage(t, s, topic, "raced")
		assert.Equal(t, []string{"raced"}, payloads(mustClaim(t, s, topic, "g", 10, longLease)), string(from))
	}

	s := env.store(t, locker)
	s.graceTime = 200 * time.Millisecond
	mustGroup(t, s, "grace-closed", "g", StartFromLatest)
	time.Sleep(s.graceTime + 200*time.Millisecond)
	insertRacedMessage(t, s, "grace-closed", "too-late")
	assert.Empty(t, mustClaim(t, s, "grace-closed", "g", 10, longLease), "after the window only the publisher's own fan-out delivers")
}

// TestClickHouseConcurrentStartup starts several instances against missing
// tables at once, with and without a shared locker: the DDL must be
// idempotent and every instance must come up.
func TestClickHouseConcurrentStartup(t *testing.T) {
	for _, withLocker := range []bool{false, true} {
		env := setupClickHouse(t)
		var locker logstore.DistributedLocker
		if withLocker {
			locker = NewProcessLocker()
		}
		const instances = 5
		stores := make([]*clickhouseStore, instances)
		errs := make([]error, instances)
		var wg sync.WaitGroup
		for i := range instances {
			wg.Add(1)
			go func() {
				defer wg.Done()
				stores[i], errs[i] = newClickHouseStore(context.Background(), env.db, env.schema, locker, DefaultRetentionHours*time.Hour, nil)
			}()
		}
		wg.Wait()
		for i, err := range errs {
			require.NoError(t, err, "instance %d (locker %v)", i, withLocker)
		}
		if withLocker {
			s := stores[0]
			mustGroup(t, s, "startup", "g", StartFromLatest)
			mustAppend(t, stores[1], newMsg("startup", "", "x"))
			assert.Len(t, mustClaim(t, stores[2], "startup", "g", 1, longLease), 1)
		}
	}
}
