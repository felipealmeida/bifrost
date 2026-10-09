package queue

import (
	"context"
	"fmt"
	"time"

	"gorm.io/gorm"
)

// Postgres advisory lock keys owned by the queue. The logstore uses 1000011,
// 1000012 and 1000015; configstore uses 1000001 and 1000002.
const (
	pgMigrationLockKey int64 = 1000020
	pgJanitorLockKey   int64 = 1000021
)

const (
	pgLockTimeout       = 2 * time.Minute
	pgLockRetryInterval = 250 * time.Millisecond
)

// withPGSessionLock runs fn while holding a session advisory lock on a
// dedicated connection, so the lock and its unlock always run on the same
// session; a pooled connection could otherwise keep the lock forever.
// Acquisition polls pg_try_advisory_lock rather than blocking, so a lock
// held by a crashed instance's lingering session fails startup with a clear
// error instead of hanging it.
func withPGSessionLock(ctx context.Context, db *gorm.DB, key int64, fn func() error) error {
	sqlDB, err := db.DB()
	if err != nil {
		return fmt.Errorf("queue: get sql.DB: %w", err)
	}
	conn, err := sqlDB.Conn(ctx)
	if err != nil {
		return fmt.Errorf("queue: dedicated connection for advisory lock %d: %w", key, err)
	}
	defer conn.Close()

	deadline := time.Now().Add(pgLockTimeout)
	for {
		var acquired bool
		if err := conn.QueryRowContext(ctx, "SELECT pg_try_advisory_lock($1)", key).Scan(&acquired); err != nil {
			return fmt.Errorf("queue: try advisory lock %d: %w", key, err)
		}
		if acquired {
			break
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("queue: advisory lock %d still held by another session after %s; "+
				"find it with: SELECT pid FROM pg_locks WHERE locktype = 'advisory' AND objid = %d AND granted", key, pgLockTimeout, key)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(pgLockRetryInterval):
		}
	}
	defer func() {
		// Unlock even if ctx was cancelled; closing the connection would also
		// release it, but only once the pool really closes the session.
		_, _ = conn.ExecContext(context.WithoutCancel(ctx), "SELECT pg_advisory_unlock($1)", key)
	}()
	return fn()
}
