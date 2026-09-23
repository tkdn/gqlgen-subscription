package loadtestutil_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/tkdn/gqlgen-subscription/backend/loadtestutil"
)

func TestStatsHandler_ReturnsListCallCountsAsJSON(t *testing.T) {
	store := loadtestutil.NewCountingJobStore(stubJobStore{})
	ctx := t.Context()
	if _, err := store.List(ctx, "user-a"); err != nil {
		t.Fatalf("List() error = %v", err)
	}
	if _, err := store.List(ctx, "user-a"); err != nil {
		t.Fatalf("List() error = %v", err)
	}

	handler := loadtestutil.NewStatsHandler(store, nil)

	req := httptest.NewRequest(http.MethodGet, "/debug/loadtest-stats", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}

	var body struct {
		ListCallCountsByUser  map[string]int64 `json:"list_call_counts_by_user"`
		NotificationsReceived int64            `json:"notifications_received"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if body.ListCallCountsByUser["user-a"] != 2 {
		t.Errorf("list_call_counts_by_user[user-a] = %d, want 2", body.ListCallCountsByUser["user-a"])
	}
	if body.NotificationsReceived != 0 {
		t.Errorf("notifications_received = %d, want 0 (nil hub passed)", body.NotificationsReceived)
	}
}

type stubNotificationCounter struct {
	count int64
}

func (s stubNotificationCounter) NotificationsReceived() int64 { return s.count }

func TestStatsHandler_ReturnsNotificationsReceivedFromHub(t *testing.T) {
	store := loadtestutil.NewCountingJobStore(stubJobStore{})
	handler := loadtestutil.NewStatsHandler(store, stubNotificationCounter{count: 5})

	req := httptest.NewRequest(http.MethodGet, "/debug/loadtest-stats", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	var body struct {
		NotificationsReceived int64 `json:"notifications_received"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if body.NotificationsReceived != 5 {
		t.Errorf("notifications_received = %d, want 5", body.NotificationsReceived)
	}
}
