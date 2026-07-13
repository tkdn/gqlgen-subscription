package e2e_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
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

// jobStatusesStream はjobStatuses subscriptionのSSEイベントを、単一の
// 背景goroutineで読み進めてチャネルに流し続ける。sseReaderを直接複数の
// goroutineから読むと競合するため、購読開始時に一度だけ読み取りgoroutineを
// 起動し、以後の待ち受けはすべてこのチャネル越しに行う。
type jobStatusesStream struct {
	events chan sseEvent
	closed chan struct{}
}

func (s *jobStatusesStream) next(t *testing.T, timeout time.Duration) (sseEvent, bool) {
	t.Helper()
	select {
	case ev := <-s.events:
		return ev, true
	case <-s.closed:
		return sseEvent{}, false
	case <-time.After(timeout):
		return sseEvent{}, false
	}
}

// waitForStatus はstreamから、statusがwantと一致するスナップショットが
// 届くまでイベントを読み進める。他のstatusのイベント（例: 初期snapshotや
// PENDING）は読み捨てて次を待つ。timeout以内に届かなければテストを失敗させる。
func waitForStatus(t *testing.T, stream *jobStatusesStream, want string, timeout time.Duration) jobStatusesPayload {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			t.Fatalf("timed out waiting for status %q", want)
		}
		ev, ok := stream.next(t, remaining)
		if !ok {
			t.Fatalf("timed out or stream closed waiting for status %q", want)
		}
		var payload jobStatusesPayload
		if err := json.Unmarshal([]byte(ev.Data), &payload); err != nil {
			t.Fatalf("unmarshal SSE event: %v", err)
		}
		if len(payload.Data.JobStatuses) == 1 && payload.Data.JobStatuses[0].Status == want {
			return payload
		}
	}
}

// subscribeJobStatuses はjobStatuses subscriptionへ接続し、以後のイベントを
// 単一の背景goroutineでチャネルへ流し続けるjobStatusesStreamを返す。
func subscribeJobStatuses(t *testing.T, serverURL string) *jobStatusesStream {
	t.Helper()

	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, serverURL,
		strings.NewReader(`{"query": "subscription { jobStatuses { id name status } }"}`))
	if err != nil {
		t.Fatalf("NewRequest() error = %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "text/event-stream")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("subscription request error = %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("subscription status = %d, want 200", resp.StatusCode)
	}
	t.Cleanup(func() { resp.Body.Close() })

	reader := newSSEReader(resp)
	stream := &jobStatusesStream{
		events: make(chan sseEvent),
		closed: make(chan struct{}),
	}
	go func() {
		defer close(stream.closed)
		for {
			ev, ok := reader.next()
			if !ok {
				return
			}
			stream.events <- ev
		}
	}()

	return stream
}

func TestSQSCompletionFlow_CreateJobDeliversCompletedStatus(t *testing.T) {
	server := newSQSTestServer(t)

	stream := subscribeJobStatuses(t, server.URL+"/query")

	// (1) 接続直後の初期スナップショット（ジョブがまだ無いので空配列）。
	ev, ok := stream.next(t, 3*time.Second)
	if !ok {
		t.Fatal("expected initial snapshot event, got none")
	}
	if ev.Event != "next" {
		t.Fatalf("initial event.Event = %q, want %q", ev.Event, "next")
	}

	graphqlRequest(t, server.URL+"/query", `mutation { createJob(name: "job-sqs-e2e-1") { id name status } }`)

	waitForStatus(t, stream, "PENDING", 3*time.Second)
	waitForStatus(t, stream, "COMPLETED", 3*time.Second)
}

func TestSQSCompletionFlow_FailNamePrefixDeliversFailedStatus(t *testing.T) {
	server := newSQSTestServer(t)

	stream := subscribeJobStatuses(t, server.URL+"/query")

	ev, ok := stream.next(t, 3*time.Second)
	if !ok {
		t.Fatal("expected initial snapshot event, got none")
	}
	if ev.Event != "next" {
		t.Fatalf("initial event.Event = %q, want %q", ev.Event, "next")
	}

	graphqlRequest(t, server.URL+"/query", `mutation { createJob(name: "fail-sqs-e2e-1") { id name status } }`)

	waitForStatus(t, stream, "PENDING", 3*time.Second)
	waitForStatus(t, stream, "FAILED", 3*time.Second)
}
