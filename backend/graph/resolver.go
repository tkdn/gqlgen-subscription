package graph

import (
	"context"

	"github.com/tkdn/gqlgen-subscription/backend/graph/model"
)

// This file will not be regenerated automatically.
//
// It serves as dependency injection for your app, add any dependencies you require
// here.

// JobStore はresolverが必要とするジョブ永続化層のインターフェース。
// 実装は jobstore.Store が満たす。単体テストではモックに差し替える。
type JobStore interface {
	Create(ctx context.Context, userID, name string) (*model.Job, error)
	UpdateStatus(ctx context.Context, userID, jobID string, status model.JobState) (*model.Job, error)
	List(ctx context.Context, userID string) ([]*model.Job, error)
}

// Hub はresolverが必要とするジョブ更新通知のfan-out層のインターフェース。
// 実装は pubsub.Hub が満たす。単体テストではモックに差し替える。
type Hub interface {
	Subscribe(userID string) (ch <-chan struct{}, unsubscribe func(), err error)
}

type Resolver struct {
	JobStore JobStore
	Hub      Hub
}
