package loadtestutil

import (
	"encoding/json"
	"net/http"
)

// NotificationCounter はLISTEN接続がNOTIFYを受信した累積回数を返す。
// pgpubsub.Hub[T]とShardedHub[T]の両方がこの形のNotificationsReceivedメソッドを持つ。
type NotificationCounter interface {
	NotificationsReceived() int64
}

// statsResponse は/debug/loadtest-statsが返すJSONの形。
type statsResponse struct {
	ListCallCountsByUser  map[string]int64 `json:"list_call_counts_by_user"`
	NotificationsReceived int64            `json:"notifications_received"`
}

// NewStatsHandler はCountingJobStoreの現在のカウントと、hubのNOTIFY受信件数を
// JSONで返すHTTPハンドラを生成する。hubがnilの場合はnotifications_receivedを0のまま返す
// （テスト等、Hubなしでハンドラだけ検証したいケースのため）。
// 検証クライアント（backend/cmd/loadtest）がプロセスごとの内訳を取得するために叩く。
func NewStatsHandler(store *CountingJobStore, hub NotificationCounter) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		resp := statsResponse{ListCallCountsByUser: store.ListCallCountsByUser()}
		if hub != nil {
			resp.NotificationsReceived = hub.NotificationsReceived()
		}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(resp); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	})
}
