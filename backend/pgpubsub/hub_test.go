package pgpubsub_test

import (
	"context"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/tkdn/gqlgen-subscription/backend/pgpubsub"
)

// testChannel は他パッケージのテストと衝突しないよう、このパッケージ専用の
// NOTIFYチャンネル名を使う（チャンネルはDBグローバルのため）。テーブルには
// 触れないので、スキーマ分離は不要。
const testChannel = "job_updates_pgpubsub_test"

// setTestEnvDefaults はlibpq互換環境変数が未設定の場合に、docker-compose.yml
// のpostgresサービスに合わせたデフォルトを設定する。設定済みの環境変数は
// そのまま優先される。
func setTestEnvDefaults(t *testing.T) {
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

// stubList はテスト用の固定ListFunc。
// 呼び出し回数を数えつつ、userIDを含む1件の文字列を返す（呼び出し元がuserIDごとに結果を区別できるようにする）。
// pgpubsubパッケージが特定のドメイン型を要求しないことを示すため、Tにstringを使う。
func stubList(callCount *atomic.Int64) pgpubsub.ListFunc[string] {
	return func(ctx context.Context, userID string) ([]string, error) {
		callCount.Add(1)
		return []string{userID}, nil
	}
}

// newTestHub はHubと、pg_notify発行用の接続を返す。PostgreSQLが起動して
// いなければスキップする。listにはstubList等、テストごとに用意したものを渡す。
func newTestHub(t *testing.T, list pgpubsub.ListFunc[string]) (*pgpubsub.Hub[string], *pgx.Conn) {
	t.Helper()
	setTestEnvDefaults(t)
	ctx := t.Context()

	pub, err := pgx.Connect(ctx, "")
	if err != nil {
		t.Skipf("postgres not available: %v", err)
	}
	t.Cleanup(func() { _ = pub.Close(context.Background()) })

	hub, err := pgpubsub.New(ctx, func(ctx context.Context) (*pgx.Conn, error) {
		return pgx.Connect(ctx, "")
	}, testChannel, list)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	t.Cleanup(hub.Close)

	return hub, pub
}

func publish(t *testing.T, pub *pgx.Conn, userID string) {
	t.Helper()
	if _, err := pub.Exec(t.Context(), "SELECT pg_notify($1, $2)", testChannel, userID); err != nil {
		t.Fatalf("pg_notify() error = %v", err)
	}
}

func waitForNotification(t *testing.T, ch <-chan []string) []string {
	t.Helper()
	select {
	case values := <-ch:
		return values
	case <-time.After(time.Second):
		t.Fatal("expected a notification, got none within timeout")
		return nil
	}
}

func expectNoNotification(t *testing.T, ch <-chan []string) {
	t.Helper()
	select {
	case <-ch:
		t.Fatal("expected no notification, but got one")
	case <-time.After(100 * time.Millisecond):
	}
}

func TestHubDeliversNotificationOnPublish(t *testing.T) {
	var callCount atomic.Int64
	hub, pub := newTestHub(t, stubList(&callCount))

	ch, unsubscribe, err := hub.Subscribe("pgpubsub-test-user-a")
	if err != nil {
		t.Fatalf("Subscribe() error = %v", err)
	}
	defer unsubscribe()

	publish(t, pub, "pgpubsub-test-user-a")

	waitForNotification(t, ch)
}

func TestHubFansOutToMultipleSubscribers(t *testing.T) {
	var callCount atomic.Int64
	hub, pub := newTestHub(t, stubList(&callCount))

	ch1, unsub1, err := hub.Subscribe("pgpubsub-test-user-a")
	if err != nil {
		t.Fatalf("Subscribe() error = %v", err)
	}
	defer unsub1()
	ch2, unsub2, err := hub.Subscribe("pgpubsub-test-user-a")
	if err != nil {
		t.Fatalf("Subscribe() error = %v", err)
	}
	defer unsub2()

	publish(t, pub, "pgpubsub-test-user-a")

	waitForNotification(t, ch1)
	waitForNotification(t, ch2)
}

func TestHubIsolatesNotificationsByUser(t *testing.T) {
	var callCount atomic.Int64
	hub, pub := newTestHub(t, stubList(&callCount))

	chA, unsubA, err := hub.Subscribe("pgpubsub-test-user-a")
	if err != nil {
		t.Fatalf("Subscribe() error = %v", err)
	}
	defer unsubA()
	chB, unsubB, err := hub.Subscribe("pgpubsub-test-user-b")
	if err != nil {
		t.Fatalf("Subscribe() error = %v", err)
	}
	defer unsubB()

	publish(t, pub, "pgpubsub-test-user-a")

	waitForNotification(t, chA)
	expectNoNotification(t, chB)
}

func TestHubStopsDeliveringAfterUnsubscribe(t *testing.T) {
	var callCount atomic.Int64
	hub, pub := newTestHub(t, stubList(&callCount))

	ch, unsubscribe, err := hub.Subscribe("pgpubsub-test-user-a")
	if err != nil {
		t.Fatalf("Subscribe() error = %v", err)
	}
	unsubscribe()

	publish(t, pub, "pgpubsub-test-user-a")

	expectNoNotification(t, ch)
}

func TestHubDispatchCallsListOnceAndBroadcastsToAllSubscribers(t *testing.T) {
	var callCount atomic.Int64
	hub, pub := newTestHub(t, stubList(&callCount))

	ch1, unsub1, err := hub.Subscribe("pgpubsub-test-user-c")
	if err != nil {
		t.Fatalf("Subscribe() error = %v", err)
	}
	defer unsub1()
	ch2, unsub2, err := hub.Subscribe("pgpubsub-test-user-c")
	if err != nil {
		t.Fatalf("Subscribe() error = %v", err)
	}
	defer unsub2()

	publish(t, pub, "pgpubsub-test-user-c")

	values1 := waitForNotification(t, ch1)
	if len(values1) != 1 || values1[0] != "pgpubsub-test-user-c" {
		t.Errorf("ch1 received %+v, want single value for pgpubsub-test-user-c", values1)
	}
	values2 := waitForNotification(t, ch2)
	if len(values2) != 1 || values2[0] != "pgpubsub-test-user-c" {
		t.Errorf("ch2 received %+v, want single value for pgpubsub-test-user-c", values2)
	}

	if got := callCount.Load(); got != 1 {
		t.Errorf("list call count = %d, want 1 (dispatch should call List once regardless of subscriber count)", got)
	}
}

func TestHubNotificationsReceivedCountsEveryNotifyRegardlessOfSubscribers(t *testing.T) {
	var callCount atomic.Int64
	hub, pub := newTestHub(t, stubList(&callCount))

	// 購読者なしでNOTIFYを発行する。dispatchはlistを呼ばないが、
	// NotificationsReceivedはWaitForNotification成功のたびに加算されるべき。
	publish(t, pub, "pgpubsub-test-user-no-subscriber")

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if hub.NotificationsReceived() >= 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if got := hub.NotificationsReceived(); got < 1 {
		t.Fatalf("NotificationsReceived() = %d, want >= 1 even with no subscribers", got)
	}

	before := hub.NotificationsReceived()

	ch, unsubscribe, err := hub.Subscribe("pgpubsub-test-user-d")
	if err != nil {
		t.Fatalf("Subscribe() error = %v", err)
	}
	defer unsubscribe()

	publish(t, pub, "pgpubsub-test-user-d")
	waitForNotification(t, ch)

	if got := hub.NotificationsReceived(); got != before+1 {
		t.Errorf("NotificationsReceived() = %d, want %d", got, before+1)
	}
}
