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

func TestShardedHub_DispatchCallsListOnceForMultipleSubscribersOfSameUser(t *testing.T) {
	setShardedHubTestEnvDefaults(t)

	var listCalls atomic.Int64
	listFunc := func(ctx context.Context, userID string) ([]*model.Job, error) {
		listCalls.Add(1)
		return []*model.Job{{ID: "job-1", Name: userID, Status: model.JobStatePending}}, nil
	}

	connect := func(ctx context.Context) (*pgx.Conn, error) {
		return pgx.Connect(ctx, "")
	}

	hub := loadtestutil.NewShardedHub(t.Context(), connect, "sharded_dispatch_test_channel", listFunc)
	t.Cleanup(hub.Close)

	ch1, unsub1, err := hub.Subscribe("dispatch-test-user")
	if err != nil {
		t.Fatalf("Subscribe() error = %v", err)
	}
	t.Cleanup(unsub1)

	ch2, unsub2, err := hub.Subscribe("dispatch-test-user")
	if err != nil {
		t.Fatalf("Subscribe() error = %v", err)
	}
	t.Cleanup(unsub2)

	notifyConn, err := pgx.Connect(t.Context(), "")
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer notifyConn.Close(t.Context())

	if _, err := notifyConn.Exec(t.Context(), "SELECT pg_notify('sharded_dispatch_test_channel_dispatch-test-user', 'dispatch-test-user')"); err != nil {
		t.Fatalf("notify: %v", err)
	}

	select {
	case jobs := <-ch1:
		if len(jobs) != 1 || jobs[0].Name != "dispatch-test-user" {
			t.Errorf("ch1 received %+v, want single job for dispatch-test-user", jobs)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("ch1: timed out waiting for broadcast")
	}

	select {
	case jobs := <-ch2:
		if len(jobs) != 1 || jobs[0].Name != "dispatch-test-user" {
			t.Errorf("ch2 received %+v, want single job for dispatch-test-user", jobs)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("ch2: timed out waiting for broadcast")
	}

	if got := listCalls.Load(); got != 1 {
		t.Errorf("listCalls = %d, want 1 (dispatch should call List once regardless of subscriber count within the same process)", got)
	}
}

func TestShardedHub_NotificationsReceivedCountsNotifyEvenAfterSubscriberLeaves(t *testing.T) {
	setShardedHubTestEnvDefaults(t)

	listFunc := func(ctx context.Context, userID string) ([]*model.Job, error) {
		return []*model.Job{{ID: "job-1", Name: userID, Status: model.JobStatePending}}, nil
	}

	connect := func(ctx context.Context) (*pgx.Conn, error) {
		return pgx.Connect(ctx, "")
	}

	hub := loadtestutil.NewShardedHub(t.Context(), connect, "sharded_notif_count_test_channel", listFunc)
	t.Cleanup(hub.Close)

	ch, unsub, err := hub.Subscribe("notif-count-user")
	if err != nil {
		t.Fatalf("Subscribe() error = %v", err)
	}

	notifyConn, err := pgx.Connect(t.Context(), "")
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer notifyConn.Close(t.Context())

	if _, err := notifyConn.Exec(t.Context(), "SELECT pg_notify('sharded_notif_count_test_channel_notif-count-user', 'notif-count-user')"); err != nil {
		t.Fatalf("notify: %v", err)
	}

	select {
	case <-ch:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for first notification")
	}

	if got := hub.NotificationsReceived(); got != 1 {
		t.Fatalf("NotificationsReceived() = %d, want 1", got)
	}

	// 唯一の購読者が抜けても（LISTEN接続はクローズされる）、その前に受信した
	// カウントは減らない。累積カウンタであることを確認する。
	unsub()

	if got := hub.NotificationsReceived(); got != 1 {
		t.Errorf("NotificationsReceived() after unsubscribe = %d, want 1 (cumulative, must not reset)", got)
	}
}
