package e2e_test

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/tkdn/gqlgen-subscription/backend/graph"
	"github.com/tkdn/gqlgen-subscription/backend/graph/model"
	"github.com/tkdn/gqlgen-subscription/backend/jobstore"
	"github.com/tkdn/gqlgen-subscription/backend/pubsub"
)

// noopDispatcher はgraph.JobDispatcherの何もしない実装。SQS投入自体を検証
// しないテスト（SSE配信の疎通確認等）で、Resolverの必須フィールドを埋める
// ために使う。
type noopDispatcher struct{}

func (noopDispatcher) Dispatch(ctx context.Context, userID string, job *model.Job) error {
	return nil
}

// testDB は本番用(DB0)や他パッケージのテストと衝突しないよう、
// このパッケージ専用のRedis DB番号を使う。
const testDB = 13

func newTestServer(t *testing.T) *httptest.Server {
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

	resolver := &graph.Resolver{
		JobStore:   jobstore.New(rdb),
		Hub:        pubsub.New(rdb),
		Dispatcher: noopDispatcher{},
	}

	server := httptest.NewServer(graph.NewHandler(resolver))
	t.Cleanup(server.Close)

	return server
}

// graphqlRequest は`serverURL`の/queryにGraphQLクエリをPOSTし、レスポンスボディを返す。
func graphqlRequest(t *testing.T, serverURL, query string) []byte {
	t.Helper()

	body := fmt.Sprintf(`{"query": %q}`, query)
	resp, err := http.Post(serverURL, "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("POST %s error = %v", serverURL, err)
	}
	defer resp.Body.Close()

	buf := make([]byte, 0, 4096)
	tmp := make([]byte, 4096)
	for {
		n, err := resp.Body.Read(tmp)
		buf = append(buf, tmp[:n]...)
		if err != nil {
			break
		}
	}
	return buf
}

// sseEvent はgqlgenのSSEトランスポートが送る1件分のイベントを表す。
type sseEvent struct {
	Event string
	Data  string
}

// sseReader はSSEレスポンスボディから "event: xxx\ndata: yyy\n\n" 形式の
// イベントを1件ずつ読み出す。
type sseReader struct {
	scanner *bufio.Scanner
}

func newSSEReader(resp *http.Response) *sseReader {
	return &sseReader{scanner: bufio.NewScanner(resp.Body)}
}

func (r *sseReader) next() (sseEvent, bool) {
	var ev sseEvent
	for r.scanner.Scan() {
		line := r.scanner.Text()
		switch {
		case line == "":
			if ev.Event != "" || ev.Data != "" {
				return ev, true
			}
		case strings.HasPrefix(line, "event: "):
			ev.Event = strings.TrimPrefix(line, "event: ")
		case strings.HasPrefix(line, "data: "):
			ev.Data = strings.TrimPrefix(line, "data: ")
		}
	}
	return sseEvent{}, false
}

// jobStatusesPayload はjobStatuses subscriptionのdataペイロードの形。
type jobStatusesPayload struct {
	Data struct {
		JobStatuses []struct {
			Name   string `json:"name"`
			Status string `json:"status"`
		} `json:"jobStatuses"`
	} `json:"data"`
}

// createJobPayload はcreateJob mutationのdataペイロードの形。
type createJobPayload struct {
	Data struct {
		CreateJob struct {
			ID string `json:"id"`
		} `json:"createJob"`
	} `json:"data"`
}

func TestSSESubscription_DeliversInitialSnapshotAndUpdates(t *testing.T) {
	server := newTestServer(t)

	createResp := graphqlRequest(t, server.URL+"/query", `mutation { createJob(name: "job-1") { id name status } }`)
	var created createJobPayload
	if err := json.Unmarshal(createResp, &created); err != nil {
		t.Fatalf("unmarshal createJob response: %v", err)
	}
	jobID := created.Data.CreateJob.ID
	if jobID == "" {
		t.Fatalf("createJob response has empty id: %s", createResp)
	}

	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, server.URL+"/query",
		strings.NewReader(`{"query": "subscription { jobStatuses { name status } }"}`))
	if err != nil {
		t.Fatalf("NewRequest() error = %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "text/event-stream")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("subscription request error = %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("subscription status = %d, want 200", resp.StatusCode)
	}

	reader := newSSEReader(resp)

	// (1) 接続直後の初期スナップショット。
	ev, ok := reader.next()
	if !ok {
		t.Fatal("expected initial snapshot event, got none")
	}
	if ev.Event != "next" {
		t.Fatalf("initial event.Event = %q, want %q", ev.Event, "next")
	}
	var initial jobStatusesPayload
	if err := json.Unmarshal([]byte(ev.Data), &initial); err != nil {
		t.Fatalf("unmarshal initial event: %v", err)
	}
	if len(initial.Data.JobStatuses) != 1 || initial.Data.JobStatuses[0].Status != "PENDING" {
		t.Fatalf("initial snapshot = %+v, want single PENDING job-1", initial.Data.JobStatuses)
	}

	// (2) updateJobStatusをトリガーに、更新後のスナップショットが流れてくる。
	updateDone := make(chan struct{})
	go func() {
		defer close(updateDone)
		graphqlRequest(t, server.URL+"/query",
			fmt.Sprintf(`mutation { updateJobStatus(id: %q, status: ANALYZING) { name status } }`, jobID))
	}()
	<-updateDone

	type result struct {
		ev sseEvent
		ok bool
	}
	resultCh := make(chan result, 1)
	go func() {
		ev, ok := reader.next()
		resultCh <- result{ev, ok}
	}()

	select {
	case res := <-resultCh:
		if !res.ok {
			t.Fatal("expected update event, got none")
		}
		var updated jobStatusesPayload
		if err := json.Unmarshal([]byte(res.ev.Data), &updated); err != nil {
			t.Fatalf("unmarshal update event: %v", err)
		}
		if len(updated.Data.JobStatuses) != 1 || updated.Data.JobStatuses[0].Status != "ANALYZING" {
			t.Fatalf("updated snapshot = %+v, want single ANALYZING job-1", updated.Data.JobStatuses)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for update event")
	}
}
