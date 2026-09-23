// Package loadtestutil はSSE fan-out負荷検証専用のユーティリティを提供する。
// 本番配線には含めない検証専用コード。
package loadtestutil

import (
	"context"
	"sync"

	"github.com/tkdn/gqlgen-subscription/backend/graph/model"
)

// JobStore はgraph.JobStoreと同一の形のインターフェース。loadtestutilパッケージが
// graph パッケージに依存すると循環importになるため、ここで同じ形を再宣言する。
type JobStore interface {
	Create(ctx context.Context, userID, name string) (*model.Job, error)
	UpdateStatus(ctx context.Context, userID, jobID string, status model.JobState) (*model.Job, error)
	List(ctx context.Context, userID string) ([]*model.Job, error)
}

// CountingJobStore はJobStoreをラップし、Listの呼び出し回数をuserIDごとに
// 数える。SSE fan-out負荷検証で「タブ数に比例してListが何回呼ばれるか」を
// 定量的に確認するために使う。
type CountingJobStore struct {
	inner JobStore

	mu     sync.Mutex
	counts map[string]int64
}

var _ JobStore = (*CountingJobStore)(nil)

// NewCountingJobStore はinnerをラップするCountingJobStoreを生成する。
func NewCountingJobStore(inner JobStore) *CountingJobStore {
	return &CountingJobStore{inner: inner, counts: make(map[string]int64)}
}

func (c *CountingJobStore) Create(ctx context.Context, userID, name string) (*model.Job, error) {
	return c.inner.Create(ctx, userID, name)
}

func (c *CountingJobStore) UpdateStatus(ctx context.Context, userID, jobID string, status model.JobState) (*model.Job, error) {
	return c.inner.UpdateStatus(ctx, userID, jobID, status)
}

func (c *CountingJobStore) List(ctx context.Context, userID string) ([]*model.Job, error) {
	c.mu.Lock()
	c.counts[userID]++
	c.mu.Unlock()
	return c.inner.List(ctx, userID)
}

// ListCallCountsByUser は現時点までのList呼び出し回数をuserIDごとに返す。
func (c *CountingJobStore) ListCallCountsByUser() map[string]int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	result := make(map[string]int64, len(c.counts))
	for k, v := range c.counts {
		result[k] = v
	}
	return result
}
