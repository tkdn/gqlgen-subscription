package consumer_test

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/redis/go-redis/v9"

	"github.com/tkdn/gqlgen-subscription/backend/awsconfig"
	"github.com/tkdn/gqlgen-subscription/backend/consumer"
	"github.com/tkdn/gqlgen-subscription/backend/graph/model"
	"github.com/tkdn/gqlgen-subscription/backend/jobstore"
)

// testDB はconsumerパッケージ専用のRedis DB番号。他パッケージの既存の
// 割り当て（jobstore=15, pubsub=14, e2e=13）と衝突しないよう12を使う。
const testDB = 12

// newTestStore は起動しているRedisのテスト専用DBに接続し、テスト前後で
// そのDBをFlushして独立性を保証する。
func newTestStore(t *testing.T) (*jobstore.Store, *redis.Client) {
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

	return jobstore.New(rdb), rdb
}

// newTestQueue はテスト専用の一意な名前のSQSキューを作成し、そのURLとクライアント
// を返す。到達不能ならスキップする。
func newTestQueue(t *testing.T) (client *sqs.Client, queueURL string) {
	t.Helper()

	ctx := t.Context()
	cfg, err := awsconfig.New(ctx)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	client = awsconfig.SQSClient(cfg)

	pingCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	if _, err := client.ListQueues(pingCtx, &sqs.ListQueuesInput{}); err != nil {
		t.Skipf("sqs endpoint not available: %v", err)
	}

	name := fmt.Sprintf("consumer-test-%s-%d", t.Name(), rand.Int64())
	queueURL, err = awsconfig.EnsureQueue(ctx, client, name)
	if err != nil {
		t.Fatalf("EnsureQueue() error = %v", err)
	}
	return client, queueURL
}

// runInBackground はconsumer.Runをgoroutineで起動し、テスト終了時にctxを
// キャンセルして終了を待つヘルパー。
func runInBackground(t *testing.T, client *sqs.Client, store *jobstore.Store, queueURL string) {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := consumer.Run(ctx, client, store, queueURL); err != nil {
			t.Errorf("Run() error = %v", err)
		}
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})
}

func sendCompletion(t *testing.T, client *sqs.Client, queueURL string, msg consumer.CompletionMessage) {
	t.Helper()

	body, err := json.Marshal(msg)
	if err != nil {
		t.Fatalf("marshal completion message: %v", err)
	}
	if _, err := client.SendMessage(t.Context(), &sqs.SendMessageInput{
		QueueUrl:    aws.String(queueURL),
		MessageBody: aws.String(string(body)),
	}); err != nil {
		t.Fatalf("SendMessage() error = %v", err)
	}
}

func TestRunUpdatesJobStatusFromCompletionMessage(t *testing.T) {
	store, _ := newTestStore(t)
	client, queueURL := newTestQueue(t)

	const userID = "consumer-test-user-a"
	created, err := store.Create(t.Context(), userID, "job-1")
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	runInBackground(t, client, store, queueURL)

	sendCompletion(t, client, queueURL, consumer.CompletionMessage{
		UserID: userID, JobID: created.ID, Status: string(model.JobStateCompleted),
	})

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		jobs, err := store.List(t.Context(), userID)
		if err != nil {
			t.Fatalf("List() error = %v", err)
		}
		if len(jobs) == 1 && jobs[0].Status == model.JobStateCompleted {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatal("timed out waiting for job status to become COMPLETED")
}

func TestRunDeletesMessageWithInvalidStatus(t *testing.T) {
	store, _ := newTestStore(t)
	client, queueURL := newTestQueue(t)

	const userID = "consumer-test-user-b"
	created, err := store.Create(t.Context(), userID, "job-1")
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	runInBackground(t, client, store, queueURL)

	sendCompletion(t, client, queueURL, consumer.CompletionMessage{
		UserID: userID, JobID: created.ID, Status: "NOT_A_REAL_STATUS",
	})

	// consumerが不正メッセージを処理（削除）する時間を与える。ジョブの状態は
	// 変わらないままであるはずなので、これで間接的に処理完了を確認できる。
	time.Sleep(500 * time.Millisecond)

	jobs, err := store.List(t.Context(), userID)
	if err != nil {
		t.Fatalf("List() error = %v", err)
	}
	if len(jobs) != 1 || jobs[0].Status != model.JobStatePending {
		t.Fatalf("List() = %+v, want status unchanged (PENDING)", jobs)
	}

	// 不正なstatusのメッセージが削除され、可視性タイムアウト後も再配信され
	// ない（再配信ループに入らない）ことを確認する回帰テスト。SQS Standard
	// キューの可視性タイムアウトのデフォルトは30秒だが、削除済みメッセージは
	// タイムアウトを待たずキューから消えているため、ここで即座に受信できない
	// ことを確認すれば十分である。
	out, err := client.ReceiveMessage(t.Context(), &sqs.ReceiveMessageInput{
		QueueUrl:            aws.String(queueURL),
		MaxNumberOfMessages: 1,
		WaitTimeSeconds:     1,
	})
	if err != nil {
		t.Fatalf("ReceiveMessage() error = %v", err)
	}
	if len(out.Messages) > 0 {
		t.Fatalf("message with invalid status was not deleted: %+v", out.Messages)
	}
}
