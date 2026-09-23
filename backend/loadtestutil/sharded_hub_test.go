package loadtestutil_test

import (
	"context"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/tkdn/gqlgen-subscription/backend/graph/model"
	"github.com/tkdn/gqlgen-subscription/backend/loadtestutil"
)

func setShardedHubTestEnvDefaults(t *testing.T) {
	t.Helper()
	defaults := map[string]string{
		"PGHOST":     "localhost",
		"PGUSER":     "app",
		"PGPASSWORD": "app",
		"PGDATABASE": "app",
		"PGSSLMODE":  "disable",
	}
	for k, v := range defaults {
		if os.Getenv(k) == "" {
			t.Setenv(k, v)
		}
	}
}

func TestShardedHub_OnlyDeliversToSubscribedUser(t *testing.T) {
	setShardedHubTestEnvDefaults(t)

	var listCallsForA, listCallsForB atomic.Int64
	listFunc := func(ctx context.Context, userID string) ([]*model.Job, error) {
		if userID == "user-a" {
			listCallsForA.Add(1)
		}
		if userID == "user-b" {
			listCallsForB.Add(1)
		}
		return []*model.Job{{ID: "job-1", Name: userID, Status: model.JobStatePending}}, nil
	}

	connect := func(ctx context.Context) (*pgx.Conn, error) {
		return pgx.Connect(ctx, "")
	}

	hub := loadtestutil.NewShardedHub(t.Context(), connect, "sharded_test_channel", listFunc)
	t.Cleanup(hub.Close)

	chA, unsubA, err := hub.Subscribe("user-a")
	if err != nil {
		t.Fatalf("Subscribe(user-a) error = %v", err)
	}
	t.Cleanup(unsubA)

	// user-bはSubscribeしない。user-bへのNOTIFYがuser-aに届かないことを確認する。

	// LISTEN確立を待つ簡易な猶予(本番品質の同期はしない、最小実装のため)。
	time.Sleep(300 * time.Millisecond)

	notifyConn, err := pgx.Connect(t.Context(), "")
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer notifyConn.Close(t.Context())

	if _, err := notifyConn.Exec(t.Context(), "SELECT pg_notify('sharded_test_channel_user-b', 'user-b')"); err != nil {
		t.Fatalf("notify user-b: %v", err)
	}
	if _, err := notifyConn.Exec(t.Context(), "SELECT pg_notify('sharded_test_channel_user-a', 'user-a')"); err != nil {
		t.Fatalf("notify user-a: %v", err)
	}

	select {
	case jobs := <-chA:
		if len(jobs) != 1 || jobs[0].Name != "user-a" {
			t.Errorf("chA received %+v, want job for user-a", jobs)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("chA: timed out waiting for notification")
	}

	if got := listCallsForB.Load(); got != 0 {
		t.Errorf("listCallsForB = %d, want 0 (user-b's notification must not reach user-a's process/subscription)", got)
	}
}
