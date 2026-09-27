package e2e_test

import (
	"context"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/tkdn/gqlgen-subscription/backend/awsconfig"
	"github.com/tkdn/gqlgen-subscription/backend/consumer"
	"github.com/tkdn/gqlgen-subscription/backend/graph"
	"github.com/tkdn/gqlgen-subscription/backend/pgjobstore"
	"github.com/tkdn/gqlgen-subscription/backend/sqsdispatch"
	"github.com/tkdn/gqlgen-subscription/backend/workersim"
)

// testWorkersimDelay はe2eテストでworkersimに与える待機時間。本番のデフォルト
// (10秒)ではテストが遅くなりすぎるため、短い値を直接パラメータとして渡す。
const testWorkersimDelay = 300 * time.Millisecond

// newSQSTestServer はnewTestServerと同様にPostgreSQL(スキーマe2e_test)を使う
// テストサーバーを構築するが、Dispatcherにnoopではなく実際のsqsdispatch.Dispatcher
// を使い、in-processのworkersim.Run・consumer.Runもgoroutineとして起動する。
// SQSエンドポイントに到達できなければスキップする。
func newSQSTestServer(t *testing.T) *httptest.Server {
	t.Helper()

	pool := newTestPool(t)

	ctx := t.Context()
	awsCfg, err := awsconfig.New(ctx)
	if err != nil {
		t.Fatalf("awsconfig.New() error = %v", err)
	}
	sqsClient := awsconfig.SQSClient(awsCfg)

	pingCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	if _, err := sqsClient.ListQueues(pingCtx, nil); err != nil {
		t.Skipf("sqs endpoint not available: %v", err)
	}

	requestsURL, err := awsconfig.EnsureQueue(ctx, sqsClient, "job-requests-e2e-test")
	if err != nil {
		t.Fatalf("EnsureQueue(requests) error = %v", err)
	}
	completionsURL, err := awsconfig.EnsureQueue(ctx, sqsClient, "job-completions-e2e-test")
	if err != nil {
		t.Fatalf("EnsureQueue(completions) error = %v", err)
	}

	jobStore := pgjobstore.New(pool, testChannel)

	resolver := &graph.Resolver{
		JobStore:   jobStore,
		Hub:        newTestHub(t),
		Dispatcher: sqsdispatch.New(sqsClient, requestsURL),
	}

	runCtx, runCancel := context.WithCancel(context.Background())
	workersimDone := make(chan struct{})
	go func() {
		defer close(workersimDone)
		if err := workersim.Run(runCtx, sqsClient, requestsURL, completionsURL, testWorkersimDelay); err != nil {
			t.Errorf("workersim.Run() error = %v", err)
		}
	}()
	consumerDone := make(chan struct{})
	go func() {
		defer close(consumerDone)
		if err := consumer.Run(runCtx, sqsClient, jobStore, completionsURL); err != nil {
			t.Errorf("consumer.Run() error = %v", err)
		}
	}()
	t.Cleanup(func() {
		runCancel()
		<-workersimDone
		<-consumerDone
	})

	server := httptest.NewServer(graph.NewHandler(resolver))
	t.Cleanup(server.Close)

	return server
}

// waitForStatus はjobs queryで一覧を取り直し、唯一のジョブのstatusがwantに
// なるまで、invalidationが届くたびに取り直しを繰り返す。取り直しを先に
// 行うので、待ち始める前に遷移が済んでいても拾える。
func waitForStatus(t *testing.T, stream *invalidationStream, serverURL, want string, timeout time.Duration) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for {
		if jobs := queryJobs(t, serverURL).Data.Jobs; len(jobs) == 1 && jobs[0].Status == want {
			return
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			t.Fatalf("timed out waiting for status %q", want)
		}
		if _, ok := stream.next(t, remaining); !ok {
			t.Fatalf("timed out or stream closed waiting for status %q", want)
		}
	}
}

// 作成したジョブがワーカーで完了し、invalidationのたびの取り直しでPENDINGからCOMPLETEDまでを追えること。
func TestSQSCompletionFlow_CreateJobDeliversCompletedStatus(t *testing.T) {
	server := newSQSTestServer(t)
	url := server.URL + "/query"

	stream := subscribeJobsInvalidated(t, url)
	expectInvalidation(t, stream, 3*time.Second)

	graphqlRequest(t, url, `mutation { createJob(name: "job-sqs-e2e-1") { id name status } }`)

	waitForStatus(t, stream, url, "PENDING", 3*time.Second)
	waitForStatus(t, stream, url, "COMPLETED", 3*time.Second)
}

// 名前がfail-で始まるジョブは失敗し、invalidationのたびの取り直しでPENDINGからFAILEDまでを追えること。
func TestSQSCompletionFlow_FailNamePrefixDeliversFailedStatus(t *testing.T) {
	server := newSQSTestServer(t)
	url := server.URL + "/query"

	stream := subscribeJobsInvalidated(t, url)
	expectInvalidation(t, stream, 3*time.Second)

	graphqlRequest(t, url, `mutation { createJob(name: "fail-sqs-e2e-1") { id name status } }`)

	waitForStatus(t, stream, url, "PENDING", 3*time.Second)
	waitForStatus(t, stream, url, "FAILED", 3*time.Second)
}
