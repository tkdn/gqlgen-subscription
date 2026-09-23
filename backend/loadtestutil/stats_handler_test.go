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

	handler := loadtestutil.NewStatsHandler(store)

	req := httptest.NewRequest(http.MethodGet, "/debug/loadtest-stats", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}

	var body struct {
		ListCallCountsByUser map[string]int64 `json:"list_call_counts_by_user"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if body.ListCallCountsByUser["user-a"] != 2 {
		t.Errorf("list_call_counts_by_user[user-a] = %d, want 2", body.ListCallCountsByUser["user-a"])
	}
}
