package graph_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/tkdn/gqlgen-subscription/backend/graph"
	"github.com/tkdn/gqlgen-subscription/backend/graph/model"
)

// postGraphQL はurlへGraphQLリクエストをPOSTする。acceptが空でなければAcceptヘッダーに設定する。
func postGraphQL(t *testing.T, ctx context.Context, url, body, accept string) *http.Response {
	t.Helper()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, strings.NewReader(body))
	if err != nil {
		t.Fatalf("NewRequest() error = %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if accept != "" {
		req.Header.Set("Accept", accept)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST %s error = %v", url, err)
	}
	t.Cleanup(func() { _ = resp.Body.Close() })
	return resp
}

// SSEのレスポンスにX-Accel-Buffering: noが付くこと。
func TestNewHandler_SSEResponseDisablesProxyBuffering(t *testing.T) {
	hub := &mockHub{
		subscribeFn: func(userID string) (<-chan struct{}, func(), error) {
			return make(chan struct{}), func() {}, nil
		},
	}
	server := httptest.NewServer(graph.NewHandler(&graph.Resolver{Hub: hub}))
	t.Cleanup(server.Close)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	resp := postGraphQL(t, ctx, server.URL, `{"query": "subscription { jobsInvalidated }"}`, "text/event-stream")

	if got := resp.Header.Get("Content-Type"); got != "text/event-stream" {
		t.Fatalf("Content-Type = %q, want %q", got, "text/event-stream")
	}
	if got := resp.Header.Get("X-Accel-Buffering"); got != "no" {
		t.Errorf("X-Accel-Buffering = %q, want %q", got, "no")
	}
}

// SSE以外のレスポンスにはX-Accel-Bufferingが付かないこと。
func TestNewHandler_NonSSEResponseLeavesProxyBufferingAlone(t *testing.T) {
	store := &mockJobStore{
		listFn: func(ctx context.Context, userID string) ([]*model.Job, error) {
			return nil, nil
		},
	}
	server := httptest.NewServer(graph.NewHandler(&graph.Resolver{JobStore: store}))
	t.Cleanup(server.Close)

	resp := postGraphQL(t, t.Context(), server.URL, `{"query": "{ jobs { id } }"}`, "")

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if got := resp.Header.Get("X-Accel-Buffering"); got != "" {
		t.Errorf("X-Accel-Buffering = %q, want unset", got)
	}
}
