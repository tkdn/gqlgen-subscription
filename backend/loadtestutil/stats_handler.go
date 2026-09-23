package loadtestutil

import (
	"encoding/json"
	"net/http"
)

// statsResponse は/debug/loadtest-statsが返すJSONの形。
type statsResponse struct {
	ListCallCountsByUser map[string]int64 `json:"list_call_counts_by_user"`
}

// NewStatsHandler はCountingJobStoreの現在のカウントをJSONで返すHTTPハンドラを生成する。
// 検証クライアント（backend/cmd/loadtest）がプロセスごとの内訳を取得するために叩く。
func NewStatsHandler(store *CountingJobStore) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		resp := statsResponse{ListCallCountsByUser: store.ListCallCountsByUser()}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(resp); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	})
}
