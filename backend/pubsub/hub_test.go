package pubsub_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/tkdn/gqlgen-subscription/backend/jobstore"
	"github.com/tkdn/gqlgen-subscription/backend/pubsub"
)

// testDB は本番用(DB0)や他パッケージのテスト(jobstoreはDB15)と衝突しないよう、
// このパッケージ専用のRedis DB番号を使う。go test ./... はパッケージ単位で
// 並列実行されるため、DB番号を共有するとFlushDBが競合する。
const testDB = 14

func newTestClient(t *testing.T) *redis.Client {
	t.Helper()

	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		addr = "localhost:6379"
	}

	rdb := redis.NewClient(&redis.Options{Addr: addr, DB: testDB})

	ctx := t.Context()
	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Skipf("redis not available at %s: %v", addr, err)
	}
	if err := rdb.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("FlushDB() error = %v", err)
	}

	t.Cleanup(func() {
		if err := rdb.FlushDB(context.Background()).Err(); err != nil {
			t.Errorf("FlushDB() cleanup error = %v", err)
		}
		if err := rdb.Close(); err != nil {
			t.Errorf("Close() cleanup error = %v", err)
		}
	})

	return rdb
}

func waitForNotification(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(time.Second):
		t.Fatal("expected a notification, got none within timeout")
	}
}

func expectNoNotification(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
		t.Fatal("expected no notification, but got one")
	case <-time.After(100 * time.Millisecond):
	}
}

func TestHubDeliversNotificationOnPublish(t *testing.T) {
	rdb := newTestClient(t)
	hub := pubsub.New(rdb)
	store := jobstore.New(rdb)

	ch, unsubscribe, err := hub.Subscribe("pubsub-test-user-a")
	if err != nil {
		t.Fatalf("Subscribe() error = %v", err)
	}
	defer unsubscribe()

	if _, err := store.Create(t.Context(), "pubsub-test-user-a", "job-1"); err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	waitForNotification(t, ch)
}

func TestHubFansOutToMultipleSubscribers(t *testing.T) {
	rdb := newTestClient(t)
	hub := pubsub.New(rdb)
	store := jobstore.New(rdb)

	ch1, unsub1, err := hub.Subscribe("pubsub-test-user-a")
	if err != nil {
		t.Fatalf("Subscribe() error = %v", err)
	}
	defer unsub1()
	ch2, unsub2, err := hub.Subscribe("pubsub-test-user-a")
	if err != nil {
		t.Fatalf("Subscribe() error = %v", err)
	}
	defer unsub2()

	if _, err := store.Create(t.Context(), "pubsub-test-user-a", "job-1"); err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	waitForNotification(t, ch1)
	waitForNotification(t, ch2)
}

func TestHubIsolatesNotificationsByUser(t *testing.T) {
	rdb := newTestClient(t)
	hub := pubsub.New(rdb)
	store := jobstore.New(rdb)

	chA, unsubA, err := hub.Subscribe("pubsub-test-user-a")
	if err != nil {
		t.Fatalf("Subscribe() error = %v", err)
	}
	defer unsubA()
	chB, unsubB, err := hub.Subscribe("pubsub-test-user-b")
	if err != nil {
		t.Fatalf("Subscribe() error = %v", err)
	}
	defer unsubB()

	if _, err := store.Create(t.Context(), "pubsub-test-user-a", "job-1"); err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	waitForNotification(t, chA)
	expectNoNotification(t, chB)
}

func TestHubStopsDeliveringAfterUnsubscribe(t *testing.T) {
	rdb := newTestClient(t)
	hub := pubsub.New(rdb)
	store := jobstore.New(rdb)

	ch, unsubscribe, err := hub.Subscribe("pubsub-test-user-a")
	if err != nil {
		t.Fatalf("Subscribe() error = %v", err)
	}
	unsubscribe()

	if _, err := store.Create(t.Context(), "pubsub-test-user-a", "job-1"); err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	expectNoNotification(t, ch)
}
