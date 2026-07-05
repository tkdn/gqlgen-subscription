package graph_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/tkdn/gqlgen-subscription/backend/graph"
	"github.com/tkdn/gqlgen-subscription/backend/graph/model"
	"github.com/tkdn/gqlgen-subscription/backend/userctx"
)

// testContext はuserctx.Middlewareを実際に通し、resolverが使うctxを
// テストに持ち出すためのヘルパー。固定ユーザーIDが注入されたctxを返す。
func testContext(t *testing.T) (ctx context.Context) {
	t.Helper()

	next := http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) { ctx = r.Context() })
	userctx.Middleware(next).ServeHTTP(nil, httptest.NewRequest(http.MethodGet, "/", nil))
	return ctx
}

// mockJobStore はgraph.JobStoreのテスト用実装。呼び出された引数を記録し、
// 用意した戻り値をそのまま返す。
type mockJobStore struct {
	createFn func(ctx context.Context, userID, name string) (*model.Job, error)
	updateFn func(ctx context.Context, userID, name string, status model.JobState) (*model.Job, error)
	listFn   func(ctx context.Context, userID string) ([]*model.Job, error)
}

func (m *mockJobStore) Create(ctx context.Context, userID, name string) (*model.Job, error) {
	return m.createFn(ctx, userID, name)
}

func (m *mockJobStore) UpdateStatus(ctx context.Context, userID, name string, status model.JobState) (*model.Job, error) {
	return m.updateFn(ctx, userID, name, status)
}

func (m *mockJobStore) List(ctx context.Context, userID string) ([]*model.Job, error) {
	return m.listFn(ctx, userID)
}

// mockHub はgraph.Hubのテスト用実装。
type mockHub struct {
	subscribeFn func(userID string) (<-chan struct{}, func(), error)
}

func (m *mockHub) Subscribe(userID string) (<-chan struct{}, func(), error) {
	return m.subscribeFn(userID)
}

func TestMutationResolver_CreateJob(t *testing.T) {
	ctx := testContext(t)
	wantUserID := userctx.UserID(ctx)

	var gotUserID, gotName string
	store := &mockJobStore{
		createFn: func(ctx context.Context, userID, name string) (*model.Job, error) {
			gotUserID, gotName = userID, name
			return &model.Job{Name: name, Status: model.JobStatePending}, nil
		},
	}

	r := (&graph.Resolver{JobStore: store}).Mutation()

	job, err := r.CreateJob(ctx, "job-1")
	if err != nil {
		t.Fatalf("CreateJob() error = %v", err)
	}
	if gotUserID != wantUserID {
		t.Errorf("CreateJob() called with userID = %q, want %q", gotUserID, wantUserID)
	}
	if gotName != "job-1" {
		t.Errorf("CreateJob() called with name = %q, want %q", gotName, "job-1")
	}
	if job.Status != model.JobStatePending {
		t.Errorf("CreateJob() job.Status = %v, want PENDING", job.Status)
	}
}

func TestMutationResolver_UpdateJobStatus(t *testing.T) {
	ctx := testContext(t)
	wantUserID := userctx.UserID(ctx)

	var gotUserID, gotName string
	var gotStatus model.JobState
	store := &mockJobStore{
		updateFn: func(ctx context.Context, userID, name string, status model.JobState) (*model.Job, error) {
			gotUserID, gotName, gotStatus = userID, name, status
			return &model.Job{Name: name, Status: status}, nil
		},
	}

	r := (&graph.Resolver{JobStore: store}).Mutation()

	job, err := r.UpdateJobStatus(ctx, "job-1", model.JobStateAnalyzing)
	if err != nil {
		t.Fatalf("UpdateJobStatus() error = %v", err)
	}
	if gotUserID != wantUserID || gotName != "job-1" || gotStatus != model.JobStateAnalyzing {
		t.Errorf("UpdateJobStatus() called with (%q, %q, %v), want (%q, %q, %v)",
			gotUserID, gotName, gotStatus, wantUserID, "job-1", model.JobStateAnalyzing)
	}
	if job.Status != model.JobStateAnalyzing {
		t.Errorf("UpdateJobStatus() job.Status = %v, want ANALYZING", job.Status)
	}
}

func TestMutationResolver_CreateJob_PropagatesError(t *testing.T) {
	ctx := testContext(t)
	wantErr := errors.New("boom")

	store := &mockJobStore{
		createFn: func(ctx context.Context, userID, name string) (*model.Job, error) {
			return nil, wantErr
		},
	}

	r := (&graph.Resolver{JobStore: store}).Mutation()

	if _, err := r.CreateJob(ctx, "job-1"); !errors.Is(err, wantErr) {
		t.Fatalf("CreateJob() error = %v, want %v", err, wantErr)
	}
}

func TestQueryResolver_Jobs(t *testing.T) {
	ctx := testContext(t)
	wantUserID := userctx.UserID(ctx)
	want := []*model.Job{{Name: "job-1", Status: model.JobStatePending}}

	var gotUserID string
	store := &mockJobStore{
		listFn: func(ctx context.Context, userID string) ([]*model.Job, error) {
			gotUserID = userID
			return want, nil
		},
	}

	r := (&graph.Resolver{JobStore: store}).Query()

	jobs, err := r.Jobs(ctx)
	if err != nil {
		t.Fatalf("Jobs() error = %v", err)
	}
	if gotUserID != wantUserID {
		t.Errorf("Jobs() called with userID = %q, want %q", gotUserID, wantUserID)
	}
	if len(jobs) != 1 || jobs[0] != want[0] {
		t.Errorf("Jobs() = %+v, want %+v", jobs, want)
	}
}

func TestSubscriptionResolver_JobStatuses_DeliversInitialSnapshotAndUpdates(t *testing.T) {
	ctx, cancel := context.WithCancel(testContext(t))
	defer cancel()

	notify := make(chan struct{}, 1)
	unsubscribeCalled := make(chan struct{}, 1)

	store := &mockJobStore{
		listFn: func(ctx context.Context, userID string) ([]*model.Job, error) {
			return []*model.Job{{Name: "job-1", Status: model.JobStatePending}}, nil
		},
	}
	hub := &mockHub{
		subscribeFn: func(userID string) (<-chan struct{}, func(), error) {
			return notify, func() { unsubscribeCalled <- struct{}{} }, nil
		},
	}

	r := (&graph.Resolver{JobStore: store, Hub: hub}).Subscription()

	ch, err := r.JobStatuses(ctx)
	if err != nil {
		t.Fatalf("JobStatuses() error = %v", err)
	}

	initial := <-ch
	if len(initial) != 1 || initial[0].Name != "job-1" {
		t.Fatalf("initial snapshot = %+v, want single job-1", initial)
	}

	notify <- struct{}{}
	updated := <-ch
	if len(updated) != 1 || updated[0].Name != "job-1" {
		t.Fatalf("updated snapshot = %+v, want single job-1", updated)
	}

	cancel()

	select {
	case <-unsubscribeCalled:
	case <-context.Background().Done():
	}

	if _, ok := <-ch; ok {
		t.Fatal("expected channel to be closed after ctx cancellation")
	}
}

func TestSubscriptionResolver_JobStatuses_PropagatesSubscribeError(t *testing.T) {
	ctx := testContext(t)
	wantErr := errors.New("subscribe failed")

	hub := &mockHub{
		subscribeFn: func(userID string) (<-chan struct{}, func(), error) {
			return nil, nil, wantErr
		},
	}

	r := (&graph.Resolver{Hub: hub}).Subscription()

	if _, err := r.JobStatuses(ctx); !errors.Is(err, wantErr) {
		t.Fatalf("JobStatuses() error = %v, want %v", err, wantErr)
	}
}
