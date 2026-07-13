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
// 実装は pgjobstore.Store（本番配線）と jobstore.Store（Redisの参照実装）が
// 満たす。単体テストではモックに差し替える。
//
// UpdateStatusの冪等保証（終端状態COMPLETED/FAILEDからの遷移拒否）は
// 実装依存: pgjobstore.Store は保証し、jobstore.Store は保証しない
// （upsert・遷移制約なしのまま凍結された参照実装）。
type JobStore interface {
	Create(ctx context.Context, userID, name string) (*model.Job, error)
	UpdateStatus(ctx context.Context, userID, jobID string, status model.JobState) (*model.Job, error)
	List(ctx context.Context, userID string) ([]*model.Job, error)
}

// Hub はresolverが必要とするジョブ更新通知のfan-out層のインターフェース。
// 実装は pgpubsub.Hub（本番配線）と pubsub.Hub（Redisの参照実装）が満たす。
// 単体テストではモックに差し替える。
type Hub interface {
	Subscribe(userID string) (ch <-chan struct{}, unsubscribe func(), err error)
}

// JobDispatcher はresolverが必要とする非同期ワーカーへのジョブ投入層の
// インターフェース。実装は sqsdispatch.Dispatcher が満たす。単体テストでは
// モックに差し替える。
type JobDispatcher interface {
	Dispatch(ctx context.Context, userID string, job *model.Job) error
}

type Resolver struct {
	JobStore   JobStore
	Hub        Hub
	Dispatcher JobDispatcher
}
