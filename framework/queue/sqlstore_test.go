package queue

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/maximhq/bifrost/core/schemas"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/postgres"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	gormlogger "gorm.io/gorm/logger"
)

// openTestSQLite opens a file database with the logstore's SQLite settings.
func openTestSQLite(t *testing.T) *gorm.DB {
	t.Helper()
	path := filepath.Join(t.TempDir(), "queue.db")
	dsn := fmt.Sprintf("%s?_journal_mode=WAL&_synchronous=NORMAL&_busy_timeout=60000&_foreign_keys=1", path)
	db, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{Logger: gormlogger.Default.LogMode(gormlogger.Silent)})
	require.NoError(t, err)
	t.Cleanup(func() {
		if sqlDB, err := db.DB(); err == nil {
			_ = sqlDB.Close()
		}
	})
	return db
}

func newTestSQLiteStore(t *testing.T) *sqlStore {
	t.Helper()
	s, err := newSQLStore(context.Background(), openTestSQLite(t), nil)
	require.NoError(t, err)
	return s
}

func TestSQLiteStoreContract(t *testing.T) {
	runStoreContract(t, func(t *testing.T) Store { return newTestSQLiteStore(t) })
}

func TestSQLiteMigrationsAreIdempotent(t *testing.T) {
	db := openTestSQLite(t)
	ctx := context.Background()
	_, err := newSQLStore(ctx, db, nil)
	require.NoError(t, err)
	_, err = newSQLStore(ctx, db, nil)
	require.NoError(t, err)

	for _, m := range sqlMigrations {
		assert.True(t, db.Migrator().HasTable(m.model), m.id)
		for _, idx := range m.indexes {
			assert.True(t, db.Migrator().HasIndex(m.model, idx), "%s %s", m.id, idx)
		}
	}
	var ids []string
	require.NoError(t, db.Table("migrations").Where("id LIKE ?", "queue_%").Order("id").Pluck("id", &ids).Error)
	assert.Equal(t, []string{"queue_deliveries_init", "queue_groups_init", "queue_messages_init", "queue_topics_init"}, ids)
}

// TestSQLiteRepublishAfterAckDoesNotRedeliver pins the reason Append only
// fans out messages it newly inserted: acknowledged deliveries are deleted,
// so re-inserting deliveries for a known message would resurrect them.
func TestSQLiteRepublishAfterAckDoesNotRedeliver(t *testing.T) {
	s := newTestSQLiteStore(t)
	ctx := context.Background()
	mustGroup(t, s, "t", "g", StartFromLatest)
	m := newMsg("t", "", "x")
	mustAppend(t, s, m)
	cs := mustClaim(t, s, "t", "g", 1, longLease)
	require.Len(t, cs, 1)
	held, err := s.Ack(ctx, cs[0])
	require.NoError(t, err)
	require.True(t, held)

	var n int64
	require.NoError(t, s.db.Table("queue_deliveries").Count(&n).Error)
	require.Zero(t, n, "acknowledged deliveries are deleted")

	mustAppend(t, s, m)
	require.NoError(t, s.db.Table("queue_deliveries").Count(&n).Error)
	assert.Zero(t, n)
}

func TestSQLiteMultiInstance(t *testing.T) {
	db := openTestSQLite(t)
	runMultiInstanceSimulation(t, func(t *testing.T, logger schemas.Logger) Queue {
		s, err := newSQLStore(context.Background(), db, nil)
		require.NoError(t, err)
		q, err := NewStoreQueue(s, EngineConfig{}, logger)
		require.NoError(t, err)
		return q
	}, 3, 200)
}

// sqlPurgeRemovesDeadAndOrphans pins what the SQL janitor deletes: dead
// deliveries and messages no live delivery needs, past retention.
func sqlPurgeRemovesDeadAndOrphans(t *testing.T, s *sqlStore) {
	ctx := context.Background()
	topic := uniqueTopic(t)
	mustGroup(t, s, topic, "g", StartFromLatest)
	mustAppend(t, s, newMsg(topic, "", "dead"))
	cs := mustClaim(t, s, topic, "g", 1, longLease)
	require.Len(t, cs, 1)
	held, err := s.Kill(ctx, cs[0], "boom")
	require.NoError(t, err)
	require.True(t, held)
	orphan := newMsg("nogroups"+topic, "", "orphan")
	mustAppend(t, s, orphan)

	time.Sleep(20 * time.Millisecond)
	n, err := s.Purge(ctx, time.Millisecond, 100)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, n, int64(3), "dead delivery, its message, and the orphan")
	st, err := s.Stats(ctx, topic, "g")
	require.NoError(t, err)
	assert.Equal(t, Stats{}, st)
	var left int64
	require.NoError(t, s.db.Table("queue_messages").Where("id = ?", orphan.ID).Count(&left).Error)
	assert.Zero(t, left)
}

func TestSQLitePurgeRemovesDeadAndOrphans(t *testing.T) {
	sqlPurgeRemovesDeadAndOrphans(t, newTestSQLiteStore(t))
}

// pgTestSchema keeps this package's tables apart from other test packages
// sharing the tests/docker-compose.yml Postgres database.
const pgTestSchema = "queue_test"

const pgTestDSN = "host=localhost user=bifrost password=bifrost_password dbname=bifrost port=5432 sslmode=disable search_path=" + pgTestSchema

// openTestPostgres opens a new connection pool to the test schema, or skips
// the test when Postgres is not running. Each call is a separate pool, which
// is how separate Bifrost instances look to the database.
func openTestPostgres(t *testing.T) *gorm.DB {
	t.Helper()
	db, err := gorm.Open(postgres.Open(pgTestDSN), &gorm.Config{Logger: gormlogger.Default.LogMode(gormlogger.Silent)})
	if err != nil {
		t.Skipf("postgres unavailable: %v", err)
	}
	sqlDB, err := db.DB()
	require.NoError(t, err)
	t.Cleanup(func() { _ = sqlDB.Close() })
	if err := sqlDB.Ping(); err != nil {
		t.Skipf("postgres unavailable: %v", err)
	}
	require.NoError(t, db.Exec("CREATE SCHEMA IF NOT EXISTS "+pgTestSchema).Error)
	return db
}

// resetTestPostgres drops the queue tables and their ledger rows so the next
// store runs its migrations from scratch.
func resetTestPostgres(t *testing.T) *gorm.DB {
	t.Helper()
	db := openTestPostgres(t)
	for _, table := range []string{"queue_deliveries", "queue_messages", "queue_groups", "queue_topics"} {
		require.NoError(t, db.Exec("DROP TABLE IF EXISTS "+table).Error)
	}
	if db.Migrator().HasTable("migrations") {
		require.NoError(t, db.Exec("DELETE FROM migrations WHERE id LIKE 'queue\\_%'").Error)
	}
	return db
}

func TestPostgresStoreContract(t *testing.T) {
	db := resetTestPostgres(t)
	runStoreContract(t, func(t *testing.T) Store {
		s, err := newSQLStore(context.Background(), db, nil)
		require.NoError(t, err)
		return s
	})
}

// TestPostgresConcurrentStartup starts several instances against an empty
// schema at once: the migration advisory lock must let exactly one create the
// tables while the rest wait and then find them.
func TestPostgresConcurrentStartup(t *testing.T) {
	resetTestPostgres(t)
	const instances = 5
	dbs := make([]*gorm.DB, instances)
	for i := range dbs {
		dbs[i] = openTestPostgres(t)
	}
	var wg sync.WaitGroup
	errs := make([]error, instances)
	for i := range dbs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, errs[i] = newSQLStore(context.Background(), dbs[i], nil)
		}()
	}
	wg.Wait()
	for i, err := range errs {
		assert.NoError(t, err, "instance %d", i)
	}
	var n int64
	require.NoError(t, dbs[0].Table("migrations").Where("id LIKE ?", "queue_%").Count(&n).Error)
	assert.Equal(t, int64(len(sqlMigrations)), n)
}

// TestPostgresLeasesUseDatabaseClock gives one instance a clock an hour
// fast. If leases were computed from the app clock, it would see the other
// instance's live lease as expired and take the delivery.
func TestPostgresLeasesUseDatabaseClock(t *testing.T) {
	resetTestPostgres(t)
	ctx := context.Background()
	a, err := newSQLStore(ctx, openTestPostgres(t), nil)
	require.NoError(t, err)
	b, err := newSQLStore(ctx, openTestPostgres(t), nil)
	require.NoError(t, err)
	b.now = func() time.Time { return time.Now().Add(time.Hour) }

	mustGroup(t, a, "clock", "g", StartFromLatest)
	mustAppend(t, a, newMsg("clock", "", "x"))
	require.Len(t, mustClaim(t, a, "clock", "g", 1, time.Minute), 1)
	assert.Empty(t, mustClaim(t, b, "clock", "g", 1, time.Minute))
}

// TestPostgresJanitorRunsOnOneInstance holds the janitor lock from another
// session: a concurrent Purge must skip rather than wait or double-delete.
func TestPostgresJanitorRunsOnOneInstance(t *testing.T) {
	resetTestPostgres(t)
	ctx := context.Background()
	s, err := newSQLStore(ctx, openTestPostgres(t), nil)
	require.NoError(t, err)
	mustAppend(t, s, newMsg("nogroups", "", "orphan"))

	other := openTestPostgres(t)
	tx := other.Begin()
	require.NoError(t, tx.Error)
	var ok bool
	require.NoError(t, tx.Raw("SELECT pg_try_advisory_xact_lock(?)", pgJanitorLockKey).Scan(&ok).Error)
	require.True(t, ok)

	n, err := s.Purge(ctx, 0, 100)
	require.NoError(t, err)
	assert.Zero(t, n, "skipped while another instance holds the janitor lock")

	require.NoError(t, tx.Rollback().Error)
	n, err = s.Purge(ctx, 0, 100)
	require.NoError(t, err)
	assert.Equal(t, int64(1), n)
}

func TestPostgresMultiInstance(t *testing.T) {
	resetTestPostgres(t)
	runMultiInstanceSimulation(t, func(t *testing.T, logger schemas.Logger) Queue {
		// One pool per instance, as separate pods would have.
		s, err := newSQLStore(context.Background(), openTestPostgres(t), nil)
		require.NoError(t, err)
		q, err := NewStoreQueue(s, EngineConfig{}, logger)
		require.NoError(t, err)
		return q
	}, 4, 400)
}

// TestPostgresGroupRegistrationRacesPublish registers an Earliest group on
// one instance while others publish as fast as they can. The topic row lock
// must order the two, so every message is delivered to the new group exactly
// once: either through the backfill or through its publish's own fan-out.
func TestPostgresGroupRegistrationRacesPublish(t *testing.T) {
	resetTestPostgres(t)
	ctx := context.Background()
	const publishers, perPublisher = 6, 60
	stores := make([]*sqlStore, publishers+1)
	for i := range stores {
		s, err := newSQLStore(ctx, openTestPostgres(t), nil)
		require.NoError(t, err)
		stores[i] = s
	}
	topic := "race" + uniqueTopic(t)
	mustGroup(t, stores[0], topic, "early", StartFromLatest)

	var wg sync.WaitGroup
	start := make(chan struct{})
	for p := range publishers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for i := range perPublisher {
				assert.NoError(t, stores[p].Append(ctx, []*Message{newMsg(topic, "", fmt.Sprintf("%d-%d", p, i))}))
			}
		}()
	}
	close(start)
	time.Sleep(20 * time.Millisecond)
	require.NoError(t, stores[publishers].EnsureGroup(ctx, topic, "late", StartFromEarliest))
	wg.Wait()

	db := stores[0].db
	var msgs, late int64
	require.NoError(t, db.Table("queue_messages").Where("topic = ?", topic).Count(&msgs).Error)
	require.NoError(t, db.Table("queue_deliveries").Where("topic = ? AND group_name = ?", topic, "late").Count(&late).Error)
	assert.Equal(t, int64(publishers*perPublisher), msgs)
	assert.Equal(t, msgs, late, "a message published during registration missed the new group")
}

func TestPostgresPurgeRemovesDeadAndOrphans(t *testing.T) {
	resetTestPostgres(t)
	s, err := newSQLStore(context.Background(), openTestPostgres(t), nil)
	require.NoError(t, err)
	sqlPurgeRemovesDeadAndOrphans(t, s)
}
