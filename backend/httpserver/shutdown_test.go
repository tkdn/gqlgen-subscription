package httpserver_test

import (
	"fmt"
	"io"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/tkdn/gqlgen-subscription/backend/httpserver"
)

// startServer はhandlerを配信するhttp.Serverをループバックの空きポートで起動し、
// サーバーとベースURLを返す。
func startServer(t *testing.T, handler http.Handler) (*http.Server, string) {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen() error = %v", err)
	}
	srv := &http.Server{Handler: handler}
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() { _ = srv.Close() })

	return srv, "http://" + ln.Addr().String()
}

// grace内に終わる処理中のリクエストは、最後まで応答できること。
func TestShutdown_LetsInFlightRequestFinishWithinGrace(t *testing.T) {
	started := make(chan struct{})
	srv, url := startServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(started)
		time.Sleep(100 * time.Millisecond)
		w.WriteHeader(http.StatusNoContent)
	}))

	result := make(chan error, 1)
	go func() {
		resp, err := http.Get(url)
		if err != nil {
			result <- err
			return
		}
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusNoContent {
			result <- fmt.Errorf("status = %d, want %d", resp.StatusCode, http.StatusNoContent)
			return
		}
		result <- nil
	}()
	<-started

	if err := httpserver.Shutdown(srv, time.Second); err != nil {
		t.Fatalf("Shutdown() error = %v", err)
	}
	if err := <-result; err != nil {
		t.Fatalf("in-flight request error = %v, want it to finish", err)
	}
}

// grace後も続いている接続は、強制的に切られること。
func TestShutdown_ClosesConnectionsStillActiveAfterGrace(t *testing.T) {
	srv, url := startServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	}))

	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("GET error = %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	start := time.Now()
	if err := httpserver.Shutdown(srv, 50*time.Millisecond); err != nil {
		t.Fatalf("Shutdown() error = %v", err)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("Shutdown() took %v, want it to give up after the grace period", elapsed)
	}
	if _, err := io.ReadAll(resp.Body); err == nil {
		t.Fatal("reading the body succeeded, want the connection to be cut")
	}
}
