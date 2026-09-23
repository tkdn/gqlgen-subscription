// Command loadtest はSSE fan-out負荷検証専用のクライアント。指定した
// プロセス群に対し、同一ユーザーで複数のSSE接続（タブ役）を張り、
// 一定間隔でupdateJobStatusミューテーション（更新役）を発行し続け、
// 各タブの受信回数と各プロセスのList呼び出し回数を出力する。
//
// 本番コードではなく検証専用。
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type createJobResponse struct {
	Data struct {
		CreateJob struct {
			ID string `json:"id"`
		} `json:"createJob"`
	} `json:"data"`
}

type statsResponse struct {
	ListCallCountsByUser map[string]int64 `json:"list_call_counts_by_user"`
}

func main() {
	tabs := flag.Int("tabs", 3, "同一ユーザーが張るSSE接続（タブ）の本数")
	processesFlag := flag.String("processes", "http://localhost:8080", "カンマ区切りのプロセスURLリスト。タブはこれらに均等に振り分けて接続する")
	updates := flag.Int("updates", 3, "updateJobStatusを発行する回数")
	interval := flag.Duration("interval", 2*time.Second, "updateJobStatus発行の間隔")
	userIDFlag := flag.String("user", "", "X-User-Idヘッダーに使うユーザーID。未指定時は実行のたびに一意な値を自動生成する")
	flag.Parse()

	userID := *userIDFlag
	if userID == "" {
		userID = fmt.Sprintf("loadtest-user-%d", time.Now().UnixNano())
	}

	processes := strings.Split(*processesFlag, ",")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// updateJobStatus対象のJobを1件作る。
	jobID := createJob(processes[0], userID)
	log.Printf("created job id=%s on %s", jobID, processes[0])

	var receivedCounts sync.Map // tabIndex(int) -> *atomic.Int64

	for i := 0; i < *tabs; i++ {
		proc := processes[i%len(processes)]
		counter := new(atomic.Int64)
		receivedCounts.Store(i, counter)
		go subscribeTab(ctx, proc, userID, i, counter)
	}

	// タブが接続し終わるのを待つ簡易な猶予。
	time.Sleep(500 * time.Millisecond)

	states := []string{"ANALYZING", "GENERATING", "COMPLETED"}
	for i := 0; i < *updates; i++ {
		state := states[i%len(states)]
		fireTime := time.Now()
		updateJobStatus(processes[0], userID, jobID, state)
		log.Printf("[%s] fired updateJobStatus id=%s status=%s", fireTime.Format(time.RFC3339Nano), jobID, state)
		time.Sleep(*interval)
	}

	// 最後の通知が全タブに届くのを待つ猶予。
	time.Sleep(*interval)
	cancel()
	time.Sleep(200 * time.Millisecond)

	fmt.Println("\n=== 結果 ===")
	fmt.Printf("タブ数: %d, 更新回数: %d, ユーザー: %s\n", *tabs, *updates, userID)
	fmt.Println("\n--- 各タブの受信回数 ---")
	for i := 0; i < *tabs; i++ {
		v, _ := receivedCounts.Load(i)
		counter := v.(*atomic.Int64)
		fmt.Printf("タブ%d (接続先 %s): %d回受信\n", i, processes[i%len(processes)], counter.Load())
	}

	fmt.Println("\n--- 各プロセスのList呼び出し回数（累積） ---")
	for _, proc := range uniqueStrings(processes) {
		stats, err := fetchStats(proc)
		if err != nil {
			log.Printf("fetch stats from %s: %v", proc, err)
			continue
		}
		fmt.Printf("%s: %v\n", proc, stats.ListCallCountsByUser)
	}
}

func uniqueStrings(in []string) []string {
	seen := make(map[string]struct{})
	var out []string
	for _, s := range in {
		if _, ok := seen[s]; !ok {
			seen[s] = struct{}{}
			out = append(out, s)
		}
	}
	return out
}

func createJob(baseURL, userID string) string {
	body := `{"query": "mutation { createJob(name: \"loadtest-job\") { id } }"}`
	req, err := http.NewRequest(http.MethodPost, baseURL+"/query", strings.NewReader(body))
	if err != nil {
		log.Fatalf("createJob: new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-User-Id", userID)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Fatalf("createJob: request: %v", err)
	}
	defer resp.Body.Close()

	var parsed createJobResponse
	if err := json.NewDecoder(resp.Body).Decode(&parsed); err != nil {
		log.Fatalf("createJob: decode: %v", err)
	}
	if parsed.Data.CreateJob.ID == "" {
		log.Fatalf("createJob: empty id in response")
	}
	return parsed.Data.CreateJob.ID
}

func updateJobStatus(baseURL, userID, jobID, status string) {
	query := fmt.Sprintf(`{"query": "mutation { updateJobStatus(id: \"%s\", status: %s) { id status } }"}`, jobID, status)
	req, err := http.NewRequest(http.MethodPost, baseURL+"/query", strings.NewReader(query))
	if err != nil {
		log.Printf("updateJobStatus: new request: %v", err)
		return
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-User-Id", userID)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Printf("updateJobStatus: request: %v", err)
		return
	}
	defer resp.Body.Close()
}

// subscribeTab は1本のSSE接続（1タブ相当）を張り、受信したイベントの回数を
// counterに積算する。ctxがキャンセルされたら接続を終了する。
func subscribeTab(ctx context.Context, baseURL, userID string, tabIndex int, counter *atomic.Int64) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, baseURL+"/query",
		strings.NewReader(`{"query": "subscription { jobStatuses { id name status } }"}`))
	if err != nil {
		log.Printf("tab %d: new request: %v", tabIndex, err)
		return
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "text/event-stream")
	req.Header.Set("X-User-Id", userID)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Printf("tab %d: request: %v", tabIndex, err)
		return
	}
	defer resp.Body.Close()

	scanner := bufio.NewScanner(resp.Body)
	var eventLine string
	for scanner.Scan() {
		line := scanner.Text()
		switch {
		case strings.HasPrefix(line, "event: "):
			eventLine = strings.TrimPrefix(line, "event: ")
		case strings.HasPrefix(line, "data: "):
			if eventLine == "next" {
				counter.Add(1)
				log.Printf("[%s] タブ%d 受信", time.Now().Format(time.RFC3339Nano), tabIndex)
			}
			eventLine = ""
		}
	}
	if err := scanner.Err(); err != nil {
		log.Printf("tab %d: scan error: %v", tabIndex, err)
	}
}

func fetchStats(baseURL string) (*statsResponse, error) {
	resp, err := http.Get(baseURL + "/debug/loadtest-stats")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var stats statsResponse
	if err := json.NewDecoder(resp.Body).Decode(&stats); err != nil {
		return nil, err
	}
	return &stats, nil
}
