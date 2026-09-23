package loadtestutil_test

import (
	"context"
	"testing"

	"github.com/tkdn/gqlgen-subscription/backend/graph/model"
	"github.com/tkdn/gqlgen-subscription/backend/loadtestutil"
)

type stubJobStore struct{}

func (stubJobStore) Create(ctx context.Context, userID, name string) (*model.Job, error) {
	return &model.Job{ID: "id-1", Name: name, Status: model.JobStatePending}, nil
}

func (stubJobStore) UpdateStatus(ctx context.Context, userID, jobID string, status model.JobState) (*model.Job, error) {
	return &model.Job{ID: jobID, Status: status}, nil
}

func (stubJobStore) List(ctx context.Context, userID string) ([]*model.Job, error) {
	return []*model.Job{}, nil
}

func TestCountingJobStore_CountsListCallsPerUser(t *testing.T) {
	store := loadtestutil.NewCountingJobStore(stubJobStore{})
	ctx := t.Context()

	if _, err := store.List(ctx, "user-a"); err != nil {
		t.Fatalf("List() error = %v", err)
	}
	if _, err := store.List(ctx, "user-a"); err != nil {
		t.Fatalf("List() error = %v", err)
	}
	if _, err := store.List(ctx, "user-b"); err != nil {
		t.Fatalf("List() error = %v", err)
	}

	counts := store.ListCallCountsByUser()
	if counts["user-a"] != 2 {
		t.Errorf("counts[user-a] = %d, want 2", counts["user-a"])
	}
	if counts["user-b"] != 1 {
		t.Errorf("counts[user-b] = %d, want 1", counts["user-b"])
	}
}

func TestCountingJobStore_DelegatesCreateAndUpdateStatus(t *testing.T) {
	store := loadtestutil.NewCountingJobStore(stubJobStore{})
	ctx := t.Context()

	job, err := store.Create(ctx, "user-a", "job-name")
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	if job.Name != "job-name" {
		t.Errorf("Create() Name = %q, want %q", job.Name, "job-name")
	}

	updated, err := store.UpdateStatus(ctx, "user-a", "id-1", model.JobStateAnalyzing)
	if err != nil {
		t.Fatalf("UpdateStatus() error = %v", err)
	}
	if updated.Status != model.JobStateAnalyzing {
		t.Errorf("UpdateStatus() Status = %q, want %q", updated.Status, model.JobStateAnalyzing)
	}
}
