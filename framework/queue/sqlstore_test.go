package queue

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/maximhq/bifrost/core/schemas"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
