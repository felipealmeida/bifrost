package queue

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/stdlib"
	"gorm.io/gorm"
)

// pgNotifyChannel carries the topic of every committed publish, so instances
// can wake their consumers instead of waiting for the next poll.
const pgNotifyChannel = "bifrost_queue"

const (
	pgListenRetryMin = time.Second
	pgListenRetryMax = 30 * time.Second
)

// WakeSource is implemented by stores that can report publishes made by
// other instances. The engine runs Listen for the queue's lifetime and wakes
// local subscriptions of each reported topic. It is a latency optimisation
// only: polling still finds every message if notifications are lost.
type WakeSource interface {
	// Listen calls wake with a topic each time a message is published to it,
	// until ctx is done. It returns ErrUnsupported when the store has no way
	// to listen.
	Listen(ctx context.Context, wake func(topic string)) error
}

// notifyTopics queues one notification per topic. Postgres delivers them only
// when tx commits, so listeners never wake for a rolled-back publish.
func notifyTopics(tx *gorm.DB, topics []string) error {
	for _, t := range topics {
		if err := tx.Exec("SELECT pg_notify(?, ?)", pgNotifyChannel, t).Error; err != nil {
			return fmt.Errorf("notify %s: %w", t, err)
		}
	}
	return nil
}

// listenPostgres holds one dedicated connection in LISTEN, reconnecting with
// backoff when it drops, until ctx is done.
func listenPostgres(ctx context.Context, db *gorm.DB, wake func(topic string), warn func(string, ...any)) error {
	sqlDB, err := db.DB()
	if err != nil {
		return err
	}
	retry := pgListenRetryMin
	for {
		err := listenOnce(ctx, sqlDB, wake)
		if ctx.Err() != nil {
			return nil
		}
		if errors.Is(err, ErrUnsupported) {
			return err
		}
		warn("queue: postgres LISTEN dropped, retrying in %s: %v", retry, err)
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(retry):
		}
		retry = min(retry*2, pgListenRetryMax)
	}
}

// listenOnce runs one LISTEN session until it fails or ctx is done.
func listenOnce(ctx context.Context, sqlDB *sql.DB, wake func(topic string)) error {
	conn, err := sqlDB.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	return conn.Raw(func(driverConn any) error {
		sc, ok := driverConn.(*stdlib.Conn)
		if !ok {
			return fmt.Errorf("%w: postgres driver connection %T", ErrUnsupported, driverConn)
		}
		pc := sc.Conn()
		if _, err := pc.Exec(ctx, "LISTEN "+pgNotifyChannel); err != nil {
			return err
		}
		// The connection goes back to the pool afterwards, so it must not stay
		// subscribed.
		defer func() {
			_, _ = pc.Exec(context.WithoutCancel(ctx), "UNLISTEN "+pgNotifyChannel)
		}()
		for {
			n, err := pc.WaitForNotification(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				return err
			}
			wake(n.Payload)
		}
	})
}
