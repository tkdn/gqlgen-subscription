package loadtestutil

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/tkdn/gqlgen-subscription/backend/graph/model"
)

// ShardedNotifyJobStore はJobStoreをラップし、Create/UpdateStatusの後にuserID専用チャンネル（channelPrefix_userID）へ追加でNOTIFYを送る。
// pgjobstore.Store自体は単一チャンネルへの通知しか行わないため、ShardedHub（userID単位のLISTEN）と組み合わせて使うための検証専用の橋渡し。
//
// 本来のNOTIFY（pgjobstore.Store.notifyが送る単一チャンネル宛のもの）はinnerの呼び出しの中でそのまま発行され続ける。
// ShardedHubを使う検証時は、単一チャンネル側（pgpubsub.Hub）を配線しないことで二重配信を避ける。
type ShardedNotifyJobStore struct {
	inner         JobStore
	pool          *pgxpool.Pool
	channelPrefix string
}

var _ JobStore = (*ShardedNotifyJobStore)(nil)

// NewShardedNotifyJobStore はShardedNotifyJobStoreを生成する。
func NewShardedNotifyJobStore(inner JobStore, pool *pgxpool.Pool, channelPrefix string) *ShardedNotifyJobStore {
	return &ShardedNotifyJobStore{inner: inner, pool: pool, channelPrefix: channelPrefix}
}

func (s *ShardedNotifyJobStore) Create(ctx context.Context, userID, name string) (*model.Job, error) {
	job, err := s.inner.Create(ctx, userID, name)
	if err != nil {
		return nil, err
	}
	if err := s.notify(ctx, userID); err != nil {
		return nil, err
	}
	return job, nil
}

func (s *ShardedNotifyJobStore) UpdateStatus(ctx context.Context, userID, jobID string, status model.JobState) (*model.Job, error) {
	job, err := s.inner.UpdateStatus(ctx, userID, jobID, status)
	if err != nil {
		return nil, err
	}
	if err := s.notify(ctx, userID); err != nil {
		return nil, err
	}
	return job, nil
}

func (s *ShardedNotifyJobStore) List(ctx context.Context, userID string) ([]*model.Job, error) {
	return s.inner.List(ctx, userID)
}

func (s *ShardedNotifyJobStore) notify(ctx context.Context, userID string) error {
	channel := s.channelPrefix + "_" + userID
	if _, err := s.pool.Exec(ctx, "SELECT pg_notify($1, $2)", channel, userID); err != nil {
		return fmt.Errorf("loadtestutil: sharded notify: %w", err)
	}
	return nil
}
