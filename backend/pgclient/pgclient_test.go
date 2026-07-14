package pgclient_test

import (
	"testing"

	"github.com/tkdn/gqlgen-subscription/backend/pgclient"
)

// pgxpool.NewWithConfigは接続を遅延して張るため、以下のテストは実際の
// PostgreSQLがなくても動く（プール構築と設定値の検証のみ）。

func TestNewOverridesMaxConnsFromEnv(t *testing.T) {
	t.Setenv("PGPOOL_MAX_CONNS", "2")

	pool, err := pgclient.New(t.Context())
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer pool.Close()

	if got := pool.Config().MaxConns; got != 2 {
		t.Errorf("MaxConns = %d, want 2", got)
	}
}

func TestNewRejectsInvalidMaxConns(t *testing.T) {
	t.Setenv("PGPOOL_MAX_CONNS", "not-a-number")

	if _, err := pgclient.New(t.Context()); err == nil {
		t.Fatal("New() error = nil, want parse error")
	}
}
