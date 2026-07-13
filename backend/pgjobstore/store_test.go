package pgjobstore_test

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/cmackenzie1/go-uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/tkdn/gqlgen-subscription/backend/graph/model"
	"github.com/tkdn/gqlgen-subscription/backend/pgclient"
	"github.com/tkdn/gqlgen-subscription/backend/pgjobstore"
)

// testSchema は他パッケージのテストと衝突しないよう、このパッケージ専用の
// スキーマを使う（Redis版テストのDB番号分離に相当）。go test ./... は
// パッケージ単位で並列実行されるため、テーブルを共有するとTRUNCATEが競合する。
const testSchema = "pgjobstore_test"

// testChannel はNOTIFYチャンネル名。チャンネルはスキーマスコープではなく
// DBグローバルのため、スキーマ分離とは別にパッケージ専用の名前で分離する。
const testChannel = "job_updates_pgjobstore_test"

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

// newTestPool は起動しているPostgreSQLのテスト専用スキーマに接続し、
// テスト開始時にjobsテーブルをTRUNCATEして独立性を保証する。
func newTestPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	setTestEnvDefaults(t)

	cfg, err := pgxpool.ParseConfig("")
	if err != nil {
		t.Fatalf("ParseConfig() error = %v", err)
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = testSchema

	ctx := t.Context()
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatalf("NewWithConfig() error = %v", err)
	}
	t.Cleanup(pool.Close)

	if err := pool.Ping(ctx); err != nil {
		t.Skipf("postgres not available: %v", err)
	}
	if _, err := pool.Exec(ctx, "CREATE SCHEMA IF NOT EXISTS "+testSchema); err != nil {
		t.Fatalf("CREATE SCHEMA error = %v", err)
	}
	if err := pgclient.EnsureSchema(ctx, pool); err != nil {
		t.Fatalf("EnsureSchema() error = %v", err)
	}
	if _, err := pool.Exec(ctx, "TRUNCATE jobs"); err != nil {
		t.Fatalf("TRUNCATE error = %v", err)
	}
	return pool
}

func newTestStore(t *testing.T) *pgjobstore.Store {
	t.Helper()
	return pgjobstore.New(newTestPool(t), testChannel)
}

// newListenConn はtestChannelをLISTENする専用接続を張る。
func newListenConn(t *testing.T) *pgx.Conn {
	t.Helper()
	ctx := t.Context()
	conn, err := pgx.Connect(ctx, "")
	if err != nil {
		t.Fatalf("Connect() error = %v", err)
	}
	t.Cleanup(func() { _ = conn.Close(context.Background()) })
	if _, err := conn.Exec(ctx, "LISTEN "+pgx.Identifier{testChannel}.Sanitize()); err != nil {
		t.Fatalf("LISTEN error = %v", err)
	}
	return conn
}

func waitForNotification(t *testing.T, conn *pgx.Conn) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	n, err := conn.WaitForNotification(ctx)
	if err != nil {
		t.Fatalf("expected a notification, got none within timeout: %v", err)
	}
	return n.Payload
}

func expectNoNotification(t *testing.T, conn *pgx.Conn) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
	defer cancel()
	n, err := conn.WaitForNotification(ctx)
	if err == nil {
		t.Fatalf("expected no notification, but got one with payload %q", n.Payload)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("WaitForNotification() error = %v, want deadline exceeded", err)
	}
}

func TestCreateAndList(t *testing.T) {
	store := newTestStore(t)
	ctx := t.Context()
	const userID = "pgjobstore-test-user-a"

	created, err := store.Create(ctx, userID, "job-1")
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	if created.Status != model.JobStatePending {
		t.Fatalf("Create() status = %v, want PENDING", created.Status)
	}

	jobs, err := store.List(ctx, userID)
	if err != nil {
		t.Fatalf("List() error = %v", err)
	}
	if len(jobs) != 1 || jobs[0].Name != "job-1" || jobs[0].Status != model.JobStatePending {
		t.Fatalf("List() = %+v, want single PENDING job-1", jobs)
	}
}

func TestCreateAssignsID(t *testing.T) {
	store := newTestStore(t)
	ctx := t.Context()
	const userID = "pgjobstore-test-user-a"

	created, err := store.Create(ctx, userID, "job-1")
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	if created.ID == "" {
		t.Fatal("Create() ID is empty, want a non-empty UUID")
	}

	jobs, err := store.List(ctx, userID)
	if err != nil {
		t.Fatalf("List() error = %v", err)
	}
	if len(jobs) != 1 || jobs[0].ID != created.ID {
		t.Fatalf("List() = %+v, want ID %q to be echoed back", jobs, created.ID)
	}
}

func TestCreateSameNameTwiceProducesTwoJobs(t *testing.T) {
	store := newTestStore(t)
	ctx := t.Context()
	const userID = "pgjobstore-test-user-a"

	first, err := store.Create(ctx, userID, "job-1")
	if err != nil {
		t.Fatalf("Create() #1 error = %v", err)
	}
	second, err := store.Create(ctx, userID, "job-1")
	if err != nil {
		t.Fatalf("Create() #2 error = %v", err)
	}
	if first.ID == second.ID {
		t.Fatalf("Create() called twice with same name produced identical IDs %q", first.ID)
	}

	jobs, err := store.List(ctx, userID)
	if err != nil {
		t.Fatalf("List() error = %v", err)
	}
	if len(jobs) != 2 {
		t.Fatalf("List() = %+v, want 2 distinct jobs named job-1", jobs)
	}
}

func TestListOrderedByCreation(t *testing.T) {
	store := newTestStore(t)
	ctx := t.Context()
	const userID = "pgjobstore-test-user-a"

	for _, name := range []string{"job-1", "job-2", "job-3"} {
		if _, err := store.Create(ctx, userID, name); err != nil {
			t.Fatalf("Create(%q) error = %v", name, err)
		}
	}

	jobs, err := store.List(ctx, userID)
	if err != nil {
		t.Fatalf("List() error = %v", err)
	}
	if len(jobs) != 3 {
		t.Fatalf("List() returned %d jobs, want 3", len(jobs))
	}
	for i, want := range []string{"job-1", "job-2", "job-3"} {
		if jobs[i].Name != want {
			t.Fatalf("List()[%d].Name = %q, want %q (creation order)", i, jobs[i].Name, want)
		}
	}
}

// TestUpdateStatus はjobIDでジョブを特定してステータスを更新できることを確認する。
func TestUpdateStatus(t *testing.T) {
	store := newTestStore(t)
	ctx := t.Context()
	const userID = "pgjobstore-test-user-a"

	created, err := store.Create(ctx, userID, "job-1")
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	updated, err := store.UpdateStatus(ctx, userID, created.ID, model.JobStateAnalyzing)
	if err != nil {
		t.Fatalf("UpdateStatus() error = %v", err)
	}
	if updated.Status != model.JobStateAnalyzing {
		t.Fatalf("UpdateStatus() status = %v, want ANALYZING", updated.Status)
	}
	if updated.Name != "job-1" {
		t.Fatalf("UpdateStatus() name = %q, want job-1", updated.Name)
	}

	jobs, err := store.List(ctx, userID)
	if err != nil {
		t.Fatalf("List() error = %v", err)
	}
	if len(jobs) != 1 || jobs[0].Status != model.JobStateAnalyzing {
		t.Fatalf("List() = %+v, want single ANALYZING job-1", jobs)
	}
}

func TestUpdateStatusNotFound(t *testing.T) {
	store := newTestStore(t)
	ctx := t.Context()

	id, err := uuid.NewV7()
	if err != nil {
		t.Fatalf("NewV7() error = %v", err)
	}
	if _, err := store.UpdateStatus(ctx, "pgjobstore-test-user-a", id.String(), model.JobStateCompleted); err == nil {
		t.Fatal("UpdateStatus() for missing job = nil error, want not-found error")
	}
}

// TestUpdateStatusTerminalStateIsNoOp は終端状態（COMPLETED/FAILED）に達した
// ジョブへの更新が黙って無視されることを確認する。SQSのat-least-once配信で
// 完了メッセージが重複しても2回目以降が何も起こさないための冪等性の検証。
func TestUpdateStatusTerminalStateIsNoOp(t *testing.T) {
	store := newTestStore(t)
	ctx := t.Context()
	const userID = "pgjobstore-test-user-a"

	created, err := store.Create(ctx, userID, "job-1")
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	if _, err := store.UpdateStatus(ctx, userID, created.ID, model.JobStateCompleted); err != nil {
		t.Fatalf("UpdateStatus(COMPLETED) error = %v", err)
	}

	got, err := store.UpdateStatus(ctx, userID, created.ID, model.JobStateAnalyzing)
	if err != nil {
		t.Fatalf("UpdateStatus() after terminal state error = %v, want no-op success", err)
	}
	if got.Status != model.JobStateCompleted {
		t.Fatalf("UpdateStatus() after terminal state status = %v, want COMPLETED (unchanged)", got.Status)
	}

	jobs, err := store.List(ctx, userID)
	if err != nil {
		t.Fatalf("List() error = %v", err)
	}
	if len(jobs) != 1 || jobs[0].Status != model.JobStateCompleted {
		t.Fatalf("List() = %+v, want single COMPLETED job-1", jobs)
	}
}

func TestListIsolatedByUser(t *testing.T) {
	store := newTestStore(t)
	ctx := t.Context()

	if _, err := store.Create(ctx, "pgjobstore-test-user-a", "job-a"); err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	if _, err := store.Create(ctx, "pgjobstore-test-user-b", "job-b"); err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	jobsA, err := store.List(ctx, "pgjobstore-test-user-a")
	if err != nil {
		t.Fatalf("List(user-a) error = %v", err)
	}
	if len(jobsA) != 1 || jobsA[0].Name != "job-a" {
		t.Fatalf("List(user-a) = %+v, want only job-a", jobsA)
	}

	jobsB, err := store.List(ctx, "pgjobstore-test-user-b")
	if err != nil {
		t.Fatalf("List(user-b) error = %v", err)
	}
	if len(jobsB) != 1 || jobsB[0].Name != "job-b" {
		t.Fatalf("List(user-b) = %+v, want only job-b", jobsB)
	}
}

// TestCreateNotifiesUpdate はCreateがコミット時にuserIDをペイロードとする
// 通知を発行することを確認する（Redis版TestSavePublishesUpdateの対）。
func TestCreateNotifiesUpdate(t *testing.T) {
	store := newTestStore(t)
	const userID = "pgjobstore-test-user-a"

	conn := newListenConn(t)

	if _, err := store.Create(t.Context(), userID, "job-1"); err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	if payload := waitForNotification(t, conn); payload != userID {
		t.Fatalf("notification payload = %q, want %q", payload, userID)
	}
}

// TestUpdateStatusTerminalStateDoesNotNotify は終端状態へのno-op更新が
// 通知を発行しないことを確認する。
func TestUpdateStatusTerminalStateDoesNotNotify(t *testing.T) {
	store := newTestStore(t)
	ctx := t.Context()
	const userID = "pgjobstore-test-user-a"

	created, err := store.Create(ctx, userID, "job-1")
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	if _, err := store.UpdateStatus(ctx, userID, created.ID, model.JobStateFailed); err != nil {
		t.Fatalf("UpdateStatus(FAILED) error = %v", err)
	}

	// 終端状態に達した後にリスナーを張り、no-op更新が通知を発行しないことを見る。
	conn := newListenConn(t)

	if _, err := store.UpdateStatus(ctx, userID, created.ID, model.JobStateCompleted); err != nil {
		t.Fatalf("UpdateStatus() after terminal state error = %v", err)
	}
	expectNoNotification(t, conn)

	// ガード: リスナー自体が生きていることを、通常のCreateの通知で確認する。
	if _, err := store.Create(ctx, userID, "job-2"); err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	waitForNotification(t, conn)
}
