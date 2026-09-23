package userctx_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/tkdn/gqlgen-subscription/backend/userctx"
)

func TestMiddleware_UsesXUserIdHeaderWhenPresent(t *testing.T) {
	var gotUserID string
	handler := userctx.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotUserID = userctx.UserID(r.Context())
	}))

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("X-User-Id", "user-42")
	handler.ServeHTTP(httptest.NewRecorder(), req)

	if gotUserID != "user-42" {
		t.Fatalf("UserID() = %q, want %q", gotUserID, "user-42")
	}
}

func TestMiddleware_FallsBackToFixedUserIDWhenHeaderAbsent(t *testing.T) {
	var gotUserID string
	handler := userctx.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotUserID = userctx.UserID(r.Context())
	}))

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	handler.ServeHTTP(httptest.NewRecorder(), req)

	if gotUserID != "user-1" {
		t.Fatalf("UserID() = %q, want %q", gotUserID, "user-1")
	}
}
