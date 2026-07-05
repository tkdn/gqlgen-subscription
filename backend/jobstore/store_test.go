package jobstore_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/tkdn/gqlgen-subscription/backend/graph/model"
	"github.com/tkdn/gqlgen-subscription/backend/jobstore"
)

// testDB は本番用(DB0)や他パッケージのテスト(pubsubはDB14)と衝突しないよう、
// このパッケージ専用のRedis DB番号を使う。go test ./... はパッケージ単位で
// 並列実行されるため、DB番号を共有するとFlushDBが競合する。
const testDB = 15

// newTestClient は起動しているRedisのテスト専用DBに接続し、
// テスト前後でそのDBをFlushして独立性を保証する。
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

func newTestStore(t *testing.T) (*jobstore.Store, *redis.Client) {
	t.Helper()
	rdb := newTestClient(t)
	return jobstore.New(rdb), rdb
}

func TestCreateAndList(t *testing.T) {
	store, _ := newTestStore(t)
	ctx := t.Context()
	const userID = "jobstore-test-user-a"

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

func TestUpdateStatus(t *testing.T) {
	store, _ := newTestStore(t)
	ctx := t.Context()
	const userID = "jobstore-test-user-a"

	if _, err := store.Create(ctx, userID, "job-1"); err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	updated, err := store.UpdateStatus(ctx, userID, "job-1", model.JobStateAnalyzing)
	if err != nil {
		t.Fatalf("UpdateStatus() error = %v", err)
	}
	if updated.Status != model.JobStateAnalyzing {
		t.Fatalf("UpdateStatus() status = %v, want ANALYZING", updated.Status)
	}

	jobs, err := store.List(ctx, userID)
	if err != nil {
		t.Fatalf("List() error = %v", err)
	}
	if len(jobs) != 1 || jobs[0].Status != model.JobStateAnalyzing {
		t.Fatalf("List() = %+v, want single ANALYZING job-1", jobs)
	}
}

func TestListIsolatedByUser(t *testing.T) {
	store, _ := newTestStore(t)
	ctx := t.Context()

	if _, err := store.Create(ctx, "jobstore-test-user-a", "job-a"); err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	if _, err := store.Create(ctx, "jobstore-test-user-b", "job-b"); err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	jobsA, err := store.List(ctx, "jobstore-test-user-a")
	if err != nil {
		t.Fatalf("List(user-a) error = %v", err)
	}
	if len(jobsA) != 1 || jobsA[0].Name != "job-a" {
		t.Fatalf("List(user-a) = %+v, want only job-a", jobsA)
	}

	jobsB, err := store.List(ctx, "jobstore-test-user-b")
	if err != nil {
		t.Fatalf("List(user-b) error = %v", err)
	}
	if len(jobsB) != 1 || jobsB[0].Name != "job-b" {
		t.Fatalf("List(user-b) = %+v, want only job-b", jobsB)
	}
}

func TestListGarbageCollectsExpiredIndex(t *testing.T) {
	store, rdb := newTestStore(t)
	ctx := t.Context()
	const userID = "jobstore-test-user-a"

	if _, err := store.Create(ctx, userID, "job-1"); err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	// TTL失効を模擬するため、ジョブ実体（Hash）だけを直接削除する。
	if err := rdb.Del(ctx, "job:jobstore-test-user-a:job-1").Err(); err != nil {
		t.Fatalf("Del() error = %v", err)
	}

	jobs, err := store.List(ctx, userID)
	if err != nil {
		t.Fatalf("List() error = %v", err)
	}
	if len(jobs) != 0 {
		t.Fatalf("List() = %+v, want empty after expiry", jobs)
	}

	members, err := rdb.SMembers(ctx, "user:jobstore-test-user-a:jobs").Result()
	if err != nil {
		t.Fatalf("SMembers() error = %v", err)
	}
	if len(members) != 0 {
		t.Fatalf("index set still has members after GC: %v", members)
	}
}

func TestSaveSetsTTL(t *testing.T) {
	store, rdb := newTestStore(t)
	ctx := t.Context()
	const userID = "jobstore-test-user-a"

	if _, err := store.Create(ctx, userID, "job-1"); err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	ttl, err := rdb.TTL(ctx, "job:jobstore-test-user-a:job-1").Result()
	if err != nil {
		t.Fatalf("TTL() error = %v", err)
	}
	if ttl <= 0 || ttl > 5*time.Minute {
		t.Fatalf("TTL = %v, want (0, 5m]", ttl)
	}
}

func TestSavePublishesUpdate(t *testing.T) {
	store, rdb := newTestStore(t)
	ctx := t.Context()
	const userID = "jobstore-test-user-a"

	// (1) このテスト専用のSubscriberを、store.Createが使うのと同じRedis接続先・
	//     同じチャンネル名（job:updates:<userID>）に対して張る。
	sub := rdb.Subscribe(ctx, jobstore.UpdatesChannel(userID))
	defer sub.Close()

	// (2) SUBSCRIBEコマンドがRedisサーバーに実際に受理されたことを確認する。
	//     これを待たずにCreateを呼ぶと、Publishがsubscribe完了前に発行され
	//     メッセージを取りこぼす（Redis Pub/Subは購読前のメッセージを保持しない）。
	if _, err := sub.Receive(ctx); err != nil {
		t.Fatalf("Receive() (subscribe confirmation) error = %v", err)
	}

	// (3) Store.Createの内部（save）が、ジョブ保存に続けて同じチャンネルへ
	//     Publishすることを期待する被験対象の呼び出し。
	if _, err := store.Create(ctx, userID, "job-1"); err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	// (4) Publishされたメッセージを受信できるか検証する。ここでのみタイムアウトを
	//     設けているのは、万一Publishが行われなかった場合にテストが無限に
	//     ブロックするのを防ぐため。
	recvCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()

	msg, err := sub.ReceiveMessage(recvCtx)
	if err != nil {
		t.Fatalf("expected a publish on the user's updates channel: %v", err)
	}
	if msg.Channel != jobstore.UpdatesChannel(userID) {
		t.Fatalf("msg.Channel = %q, want %q", msg.Channel, jobstore.UpdatesChannel(userID))
	}
}
