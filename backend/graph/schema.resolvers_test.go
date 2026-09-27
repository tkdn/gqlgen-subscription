package graph_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"testing/synctest"

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
	updateFn func(ctx context.Context, userID, jobID string, status model.JobState) (*model.Job, error)
	listFn   func(ctx context.Context, userID string) ([]*model.Job, error)
}

func (m *mockJobStore) Create(ctx context.Context, userID, name string) (*model.Job, error) {
	return m.createFn(ctx, userID, name)
}

func (m *mockJobStore) UpdateStatus(ctx context.Context, userID, jobID string, status model.JobState) (*model.Job, error) {
	return m.updateFn(ctx, userID, jobID, status)
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

// mockJobDispatcher はgraph.JobDispatcherのテスト用実装。
type mockJobDispatcher struct {
	dispatchFn func(ctx context.Context, userID string, job *model.Job) error
}

func (m *mockJobDispatcher) Dispatch(ctx context.Context, userID string, job *model.Job) error {
	return m.dispatchFn(ctx, userID, job)
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

	var gotDispatchUserID string
	var gotDispatchJob *model.Job
	dispatcher := &mockJobDispatcher{
		dispatchFn: func(ctx context.Context, userID string, job *model.Job) error {
			gotDispatchUserID, gotDispatchJob = userID, job
			return nil
		},
	}

	r := (&graph.Resolver{JobStore: store, Dispatcher: dispatcher}).Mutation()

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
	if gotDispatchUserID != wantUserID {
		t.Errorf("CreateJob() dispatched with userID = %q, want %q", gotDispatchUserID, wantUserID)
	}
	if gotDispatchJob != job {
		t.Errorf("CreateJob() dispatched job = %+v, want the same job returned by JobStore.Create", gotDispatchJob)
	}
}

func TestMutationResolver_CreateJob_PropagatesDispatchError(t *testing.T) {
	ctx := testContext(t)
	wantErr := errors.New("dispatch boom")

	store := &mockJobStore{
		createFn: func(ctx context.Context, userID, name string) (*model.Job, error) {
			return &model.Job{Name: name, Status: model.JobStatePending}, nil
		},
	}
	dispatcher := &mockJobDispatcher{
		dispatchFn: func(ctx context.Context, userID string, job *model.Job) error {
			return wantErr
		},
	}

	r := (&graph.Resolver{JobStore: store, Dispatcher: dispatcher}).Mutation()

	if _, err := r.CreateJob(ctx, "job-1"); !errors.Is(err, wantErr) {
		t.Fatalf("CreateJob() error = %v, want wrapping %v", err, wantErr)
	}
}

func TestMutationResolver_UpdateJobStatus(t *testing.T) {
	ctx := testContext(t)
	wantUserID := userctx.UserID(ctx)

	var gotUserID, gotJobID string
	var gotStatus model.JobState
	store := &mockJobStore{
		updateFn: func(ctx context.Context, userID, jobID string, status model.JobState) (*model.Job, error) {
			gotUserID, gotJobID, gotStatus = userID, jobID, status
			return &model.Job{ID: jobID, Status: status}, nil
		},
	}

	r := (&graph.Resolver{JobStore: store}).Mutation()

	job, err := r.UpdateJobStatus(ctx, "job-1", model.JobStateAnalyzing)
	if err != nil {
		t.Fatalf("UpdateJobStatus() error = %v", err)
	}
	if gotUserID != wantUserID || gotJobID != "job-1" || gotStatus != model.JobStateAnalyzing {
		t.Errorf("UpdateJobStatus() called with (%q, %q, %v), want (%q, %q, %v)",
			gotUserID, gotJobID, gotStatus, wantUserID, "job-1", model.JobStateAnalyzing)
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

// takeInvalidation はchに届いているinvalidationを1件受け取る。synctest.Waitの後に呼ぶ。
func takeInvalidation(t *testing.T, ch <-chan bool) {
	t.Helper()
	select {
	case v, ok := <-ch:
		if !ok {
			t.Fatal("channel closed, want an invalidation")
		}
		if !v {
			t.Fatal("invalidation = false, want true")
		}
	default:
		t.Fatal("no invalidation pending, want one")
	}
}

// expectNoInvalidation はchに受け取れる値が届いていないことを確かめる。synctest.Waitの後に呼ぶ。
func expectNoInvalidation(t *testing.T, ch <-chan bool) {
	t.Helper()
	select {
	case v, ok := <-ch:
		t.Fatalf("received (%v, %v), want nothing pending", v, ok)
	default:
	}
}

// 購読の開始直後と、Hubから通知が来るたびに、invalidationが1件ずつ届くこと。
func TestSubscriptionResolver_JobsInvalidated_DeliversOnSubscribeAndOnEachNotification(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(testContext(t))
		defer cancel()
		wantUserID := userctx.UserID(ctx)

		notify := make(chan struct{})
		var gotUserID string
		hub := &mockHub{
			subscribeFn: func(userID string) (<-chan struct{}, func(), error) {
				gotUserID = userID
				return notify, func() {}, nil
			},
		}

		ch, err := (&graph.Resolver{Hub: hub}).Subscription().JobsInvalidated(ctx)
		if err != nil {
			t.Fatalf("JobsInvalidated() error = %v", err)
		}
		if gotUserID != wantUserID {
			t.Errorf("Subscribe() called with userID = %q, want %q", gotUserID, wantUserID)
		}

		synctest.Wait()
		takeInvalidation(t, ch)
		for range 2 {
			notify <- struct{}{}
			synctest.Wait()
			takeInvalidation(t, ch)
		}
		expectNoInvalidation(t, ch)
	})
}

// 未読のinvalidationが残っている間に通知が続いても1件にまとまり、Hubからの送信が詰まらないこと。
func TestSubscriptionResolver_JobsInvalidated_CoalescesNotificationsWhileOnePending(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(testContext(t))
		defer cancel()

		notify := make(chan struct{})
		hub := &mockHub{
			subscribeFn: func(userID string) (<-chan struct{}, func(), error) {
				return notify, func() {}, nil
			},
		}

		ch, err := (&graph.Resolver{Hub: hub}).Subscription().JobsInvalidated(ctx)
		if err != nil {
			t.Fatalf("JobsInvalidated() error = %v", err)
		}

		// クライアントが最初の1件を読まないうちに通知が続いても、Hubからの送信は詰まらない。
		for range 3 {
			notify <- struct{}{}
		}
		synctest.Wait()

		takeInvalidation(t, ch)
		expectNoInvalidation(t, ch)
	})
}

// 購読のcontextが終わると、Hubの購読を解除してからチャネルを閉じること。
func TestSubscriptionResolver_JobsInvalidated_UnsubscribesAndClosesOnCancel(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(testContext(t))
		defer cancel()

		unsubscribed := make(chan struct{})
		hub := &mockHub{
			subscribeFn: func(userID string) (<-chan struct{}, func(), error) {
				return make(chan struct{}), func() { close(unsubscribed) }, nil
			},
		}

		ch, err := (&graph.Resolver{Hub: hub}).Subscription().JobsInvalidated(ctx)
		if err != nil {
			t.Fatalf("JobsInvalidated() error = %v", err)
		}
		synctest.Wait()
		takeInvalidation(t, ch)

		cancel()
		synctest.Wait()

		select {
		case <-unsubscribed:
		default:
			t.Fatal("unsubscribe was not called after ctx cancellation")
		}
		select {
		case _, ok := <-ch:
			if ok {
				t.Fatal("received an invalidation after cancellation, want the channel closed")
			}
		default:
			t.Fatal("channel was not closed after ctx cancellation")
		}
	})
}

// Hubへの購読が失敗したら、そのエラーを返すこと。
func TestSubscriptionResolver_JobsInvalidated_PropagatesSubscribeError(t *testing.T) {
	ctx := testContext(t)
	wantErr := errors.New("subscribe failed")

	hub := &mockHub{
		subscribeFn: func(userID string) (<-chan struct{}, func(), error) {
			return nil, nil, wantErr
		},
	}

	if _, err := (&graph.Resolver{Hub: hub}).Subscription().JobsInvalidated(ctx); !errors.Is(err, wantErr) {
		t.Fatalf("JobsInvalidated() error = %v, want %v", err, wantErr)
	}
}
