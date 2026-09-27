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

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/tkdn/gqlgen-subscription/backend/graph"
	"github.com/tkdn/gqlgen-subscription/backend/graph/model"
	"github.com/tkdn/gqlgen-subscription/backend/pgclient"
	"github.com/tkdn/gqlgen-subscription/backend/pgjobstore"
	"github.com/tkdn/gqlgen-subscription/backend/pgpubsub"
)

// noopDispatcher はgraph.JobDispatcherの何もしない実装。SQS投入自体を検証
// しないテスト（SSE配信の疎通確認等）で、Resolverの必須フィールドを埋める
// ために使う。
type noopDispatcher struct{}

func (noopDispatcher) Dispatch(ctx context.Context, userID string, job *model.Job) error {
	return nil
}

// testSchema は他パッケージのテストと衝突しないよう、このパッケージ専用の
// PostgreSQLスキーマを使う（Redis版テストのDB番号分離に相当）。
const testSchema = "e2e_test"

// testChannel はNOTIFYチャンネル名。チャンネルはスキーマスコープではなく
// DBグローバルのため、スキーマ分離とは別にパッケージ専用の名前で分離する。
const testChannel = "job_updates_e2e_test"

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
// PostgreSQLが起動していなければスキップする。
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

// newTestHub はtestChannelを購読するpgpubsub.Hubを生成する。
func newTestHub(t *testing.T) *pgpubsub.Hub {
	t.Helper()
	hub, err := pgpubsub.New(t.Context(), func(ctx context.Context) (*pgx.Conn, error) {
		return pgx.Connect(ctx, "")
	}, testChannel)
	if err != nil {
		t.Fatalf("pgpubsub.New() error = %v", err)
	}
	t.Cleanup(hub.Close)
	return hub
}

func newTestServer(t *testing.T) *httptest.Server {
	t.Helper()

	pool := newTestPool(t)

	resolver := &graph.Resolver{
		JobStore:   pgjobstore.New(pool, testChannel),
		Hub:        newTestHub(t),
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

// invalidationStream はjobsInvalidated subscriptionのSSEイベントをチャネルで
// 受け取れるようにする。sseReaderを複数のgoroutineから読むと競合するので、
// 購読の開始時にsseReaderを読み続けるgoroutineを1つだけ起動し、読んだ
// イベントをeventsへ送る。テストはeventsとclosedだけを見て待つ。
type invalidationStream struct {
	header http.Header
	events chan sseEvent
	closed chan struct{}
}

func (s *invalidationStream) next(t *testing.T, timeout time.Duration) (sseEvent, bool) {
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

// subscribeJobsInvalidated はjobsInvalidated subscriptionへ接続し、以後の
// イベントを流し続けるinvalidationStreamを返す。
func subscribeJobsInvalidated(t *testing.T, serverURL string) *invalidationStream {
	t.Helper()

	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, serverURL,
		strings.NewReader(`{"query": "subscription { jobsInvalidated }"}`))
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
	t.Cleanup(func() { _ = resp.Body.Close() })

	reader := newSSEReader(resp)
	stream := &invalidationStream{
		header: resp.Header,
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

// jobsInvalidatedPayload はjobsInvalidated subscriptionのdataペイロードの形。
type jobsInvalidatedPayload struct {
	Data struct {
		JobsInvalidated bool `json:"jobsInvalidated"`
	} `json:"data"`
}

// expectInvalidation はstreamからinvalidationが1件届くことを確かめる。
func expectInvalidation(t *testing.T, stream *invalidationStream, timeout time.Duration) {
	t.Helper()

	ev, ok := stream.next(t, timeout)
	if !ok {
		t.Fatal("timed out or stream closed waiting for an invalidation")
	}
	if ev.Event != "next" {
		t.Fatalf("event.Event = %q, want %q", ev.Event, "next")
	}
	var payload jobsInvalidatedPayload
	if err := json.Unmarshal([]byte(ev.Data), &payload); err != nil {
		t.Fatalf("unmarshal invalidation event: %v", err)
	}
	if !payload.Data.JobsInvalidated {
		t.Fatalf("jobsInvalidated = false in %s, want true", ev.Data)
	}
}

// jobsPayload はjobs queryのdataペイロードの形。
type jobsPayload struct {
	Data struct {
		Jobs []struct {
			ID     string `json:"id"`
			Name   string `json:"name"`
			Status string `json:"status"`
		} `json:"jobs"`
	} `json:"data"`
}

// queryJobs はjobs queryで現在のジョブ一覧を取得する。
func queryJobs(t *testing.T, serverURL string) jobsPayload {
	t.Helper()

	resp := graphqlRequest(t, serverURL, `query { jobs { id name status } }`)
	var payload jobsPayload
	if err := json.Unmarshal(resp, &payload); err != nil {
		t.Fatalf("unmarshal jobs response: %v (%s)", err, resp)
	}
	return payload
}

// createJobPayload はcreateJob mutationのdataペイロードの形。
type createJobPayload struct {
	Data struct {
		CreateJob struct {
			ID string `json:"id"`
		} `json:"createJob"`
	} `json:"data"`
}

// SSEで接続直後と更新時にinvalidationが届き、そのたびにjobs queryで最新の一覧が取れること。
func TestSSESubscription_DeliversInvalidationOnConnectAndOnUpdate(t *testing.T) {
	server := newTestServer(t)
	url := server.URL + "/query"

	createResp := graphqlRequest(t, url, `mutation { createJob(name: "job-1") { id name status } }`)
	var created createJobPayload
	if err := json.Unmarshal(createResp, &created); err != nil {
		t.Fatalf("unmarshal createJob response: %v", err)
	}
	jobID := created.Data.CreateJob.ID
	if jobID == "" {
		t.Fatalf("createJob response has empty id: %s", createResp)
	}

	stream := subscribeJobsInvalidated(t, url)

	// (1) 接続直後のinvalidation。これを受けて取り直した一覧に、接続前に作ったジョブが含まれる。
	expectInvalidation(t, stream, 3*time.Second)
	if jobs := queryJobs(t, url).Data.Jobs; len(jobs) != 1 || jobs[0].Status != "PENDING" {
		t.Fatalf("jobs after connect = %+v, want single PENDING job-1", jobs)
	}

	// (2) 更新をきっかけにinvalidationが届き、取り直した一覧に更新が反映されている。
	graphqlRequest(t, url, fmt.Sprintf(`mutation { updateJobStatus(id: %q, status: ANALYZING) { id } }`, jobID))
	expectInvalidation(t, stream, 3*time.Second)
	if jobs := queryJobs(t, url).Data.Jobs; len(jobs) != 1 || jobs[0].Status != "ANALYZING" {
		t.Fatalf("jobs after update = %+v, want single ANALYZING job-1", jobs)
	}
}
