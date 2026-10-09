package queue

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newPostgresTestQueue is one instance with its own connection pool.
func newPostgresTestQueue(t *testing.T) Queue {
	t.Helper()
	s, err := newSQLStore(context.Background(), openTestPostgres(t), nil)
	require.NoError(t, err)
	q, err := NewStoreQueue(s, EngineConfig{}, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = q.Close(context.Background()) })
	return q
}

// awaitDelivery subscribes on consumer with a poll interval far longer than
// the test, publishes on producer, and returns how long delivery took.
func awaitDelivery(t *testing.T, consumer, producer Queue, topic string, publishAfter func()) time.Duration {
	t.Helper()
	got := make(chan time.Time, 1)
	_, err := consumer.Subscribe(context.Background(), topic, "g", func(ctx context.Context, d *Delivery) error {
		got <- time.Now()
		return nil
	}, WithPollInterval(time.Minute))
	require.NoError(t, err)
	time.Sleep(300 * time.Millisecond) // the first, empty poll and LISTEN are in place
	if publishAfter != nil {
		publishAfter()
	}
	sent := time.Now()
	require.NoError(t, producer.Publish(context.Background(), &Message{Topic: topic, Payload: []byte("x")}))
	select {
	case at := <-got:
		return at.Sub(sent)
	case <-time.After(10 * time.Second):
		t.Fatal("delivery waited for the poll interval: no cross-instance wakeup")
		return 0
	}
}

func TestPostgresNotifyWakesOtherInstances(t *testing.T) {
	resetTestPostgres(t)
	consumer, producer := newPostgresTestQueue(t), newPostgresTestQueue(t)
	assert.Less(t, awaitDelivery(t, consumer, producer, "wake", nil), 2*time.Second)
}

// TestPostgresListenReconnects kills the listening session, as a database
// failover or idle-connection reaper would, and expects LISTEN to come back.
func TestPostgresListenReconnects(t *testing.T) {
	db := resetTestPostgres(t)
	consumer, producer := newPostgresTestQueue(t), newPostgresTestQueue(t)
	elapsed := awaitDelivery(t, consumer, producer, "reconnect", func() {
		var killed []bool
		require.NoError(t, db.Raw("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE query = ? AND pid <> pg_backend_pid()",
			"LISTEN "+pgNotifyChannel).Scan(&killed).Error)
		require.NotEmpty(t, killed, "no listening session found")
		time.Sleep(pgListenRetryMin + 500*time.Millisecond)
	})
	assert.Less(t, elapsed, 2*time.Second)
}

func TestSQLiteHasNoWakeSource(t *testing.T) {
	err := newTestSQLiteStore(t).Listen(context.Background(), func(string) {})
	assert.ErrorIs(t, err, ErrUnsupported)
}
