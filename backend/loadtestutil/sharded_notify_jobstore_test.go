package loadtestutil_test

import (
	"os"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/tkdn/gqlgen-subscription/backend/loadtestutil"
)

func TestShardedNotifyJobStore_DelegatesToInner(t *testing.T) {
	inner := stubJobStore{}
	store := loadtestutil.NewShardedNotifyJobStore(inner, newTestPoolForShardedNotify(t), "sharded_notify_test_channel")

	ctx := t.Context()
	job, err := store.Create(ctx, "user-a", "job-name")
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	if job.Name != "job-name" {
		t.Errorf("Create() Name = %q, want %q", job.Name, "job-name")
	}
}

func newTestPoolForShardedNotify(t *testing.T) *pgxpool.Pool {
	t.Helper()
	defaults := map[string]string{
		"PGHOST":     "localhost",
		"PGUSER":     "app",
		"PGPASSWORD": "app",
		"PGDATABASE": "app",
		"PGSSLMODE":  "disable",
	}
	for k, v := range defaults {
		if os.Getenv(k) == "" {
			t.Setenv(k, v)
		}
	}
	pool, err := pgxpool.New(t.Context(), "")
	if err != nil {
		t.Fatalf("pgxpool.New() error = %v", err)
	}
	t.Cleanup(pool.Close)
	if err := pool.Ping(t.Context()); err != nil {
		t.Skipf("postgres not available: %v", err)
	}
	return pool
}
