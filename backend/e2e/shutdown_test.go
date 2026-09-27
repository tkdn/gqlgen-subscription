package e2e_test

import (
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/tkdn/gqlgen-subscription/backend/graph"
	"github.com/tkdn/gqlgen-subscription/backend/httpserver"
	"github.com/tkdn/gqlgen-subscription/backend/pgjobstore"
)

// シャットダウンでSSEの接続が切れるとき、event: completeが送られないこと。
func TestShutdown_ClosesSSEWithoutSendingComplete(t *testing.T) {
	pool := newTestPool(t)
	resolver := &graph.Resolver{
		JobStore:   pgjobstore.New(pool, testChannel),
		Hub:        newTestHub(t),
		Dispatcher: noopDispatcher{},
	}

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen() error = %v", err)
	}
	srv := &http.Server{Handler: graph.NewHandler(resolver)}
	go func() { _ = srv.Serve(ln) }()

	stream := subscribeJobsInvalidated(t, "http://"+ln.Addr().String()+"/query")
	expectInvalidation(t, stream, 3*time.Second)

	if err := httpserver.Shutdown(srv, 100*time.Millisecond); err != nil {
		t.Fatalf("Shutdown() error = %v", err)
	}

	deadline := time.After(3 * time.Second)
	for {
		select {
		case ev := <-stream.events:
			if ev.Event == "complete" {
				t.Fatal("received event: complete, want the connection to drop without it")
			}
		case <-stream.closed:
			return
		case <-deadline:
			t.Fatal("stream was not closed after Shutdown")
		}
	}
}
