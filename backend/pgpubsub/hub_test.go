package pgpubsub_test

import (
	"context"
	"os"
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

// newTestHub はHubと、pg_notify発行用の接続を返す。PostgreSQLが起動して
// いなければスキップする。
func newTestHub(t *testing.T) (*pgpubsub.Hub, *pgx.Conn) {
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
	}, testChannel)
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
	hub, pub := newTestHub(t)

	ch, unsubscribe, err := hub.Subscribe("pgpubsub-test-user-a")
	if err != nil {
		t.Fatalf("Subscribe() error = %v", err)
	}
	defer unsubscribe()

	publish(t, pub, "pgpubsub-test-user-a")

	waitForNotification(t, ch)
}

func TestHubFansOutToMultipleSubscribers(t *testing.T) {
	hub, pub := newTestHub(t)

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
	hub, pub := newTestHub(t)

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
	hub, pub := newTestHub(t)

	ch, unsubscribe, err := hub.Subscribe("pgpubsub-test-user-a")
	if err != nil {
		t.Fatalf("Subscribe() error = %v", err)
	}
	unsubscribe()

	publish(t, pub, "pgpubsub-test-user-a")

	expectNoNotification(t, ch)
}
