package jobstore

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/tkdn/gqlgen-subscription/backend/graph/model"
)

// jobTTL はジョブ実体（Hash）のTTL。検証用途のため揮発してよい。
const jobTTL = 5 * time.Minute

// Store はユーザーごとのジョブをRedisで管理する。
type Store struct {
	rdb *redis.Client
}

// New はStoreを生成する。
func New(rdb *redis.Client) *Store {
	return &Store{rdb: rdb}
}

func indexKey(userID string) string {
	return fmt.Sprintf("user:%s:jobs", userID)
}

func jobKey(userID, name string) string {
	return fmt.Sprintf("job:%s:%s", userID, name)
}

func updatesChannel(userID string) string {
	return fmt.Sprintf("job:updates:%s", userID)
}

// Create は新しいジョブをPENDING状態で作成する。userIDの解決は呼び出し側（resolverなど）の責務とする。
func (s *Store) Create(ctx context.Context, userID, name string) (*model.Job, error) {
	job := &model.Job{Name: name, Status: model.JobStatePending}

	if err := s.save(ctx, userID, job); err != nil {
		return nil, err
	}
	return job, nil
}

// UpdateStatus は既存ジョブのステータスを任意の値に変更する（検証目的で遷移制約なし）。
func (s *Store) UpdateStatus(ctx context.Context, userID, name string, status model.JobState) (*model.Job, error) {
	job := &model.Job{Name: name, Status: status}

	if err := s.save(ctx, userID, job); err != nil {
		return nil, err
	}
	return job, nil
}

// save はジョブ実体をHashとして書き込み、索引Setへの登録・TTL再設定・Publishを行う。
func (s *Store) save(ctx context.Context, userID string, job *model.Job) error {
	key := jobKey(userID, job.Name)

	if err := s.rdb.HSet(ctx, key, "status", string(job.Status)).Err(); err != nil {
		return fmt.Errorf("jobstore: save job: %w", err)
	}
	if err := s.rdb.Expire(ctx, key, jobTTL).Err(); err != nil {
		return fmt.Errorf("jobstore: set ttl: %w", err)
	}
	if err := s.rdb.SAdd(ctx, indexKey(userID), job.Name).Err(); err != nil {
		return fmt.Errorf("jobstore: index job: %w", err)
	}
	if err := s.rdb.Publish(ctx, updatesChannel(userID), "").Err(); err != nil {
		return fmt.Errorf("jobstore: publish update: %w", err)
	}
	return nil
}

// List はユーザーの全ジョブを返す。TTLで失効したジョブは索引Setから読み取り時に除去する。
func (s *Store) List(ctx context.Context, userID string) ([]*model.Job, error) {
	idxKey := indexKey(userID)

	names, err := s.rdb.SMembers(ctx, idxKey).Result()
	if err != nil {
		return nil, fmt.Errorf("jobstore: list job names: %w", err)
	}

	jobs := make([]*model.Job, 0, len(names))
	for _, name := range names {
		fields, err := s.rdb.HGetAll(ctx, jobKey(userID, name)).Result()
		if err != nil {
			return nil, fmt.Errorf("jobstore: get job %q: %w", name, err)
		}

		status, ok := fields["status"]
		if !ok {
			// TTL失効によりHash実体が消えている。索引から取り除く。
			if err := s.rdb.SRem(ctx, idxKey, name).Err(); err != nil {
				return nil, fmt.Errorf("jobstore: gc stale index %q: %w", name, err)
			}
			continue
		}

		jobs = append(jobs, &model.Job{Name: name, Status: model.JobState(status)})
	}

	return jobs, nil
}

// UpdatesChannel はユーザーのジョブ更新通知チャンネル名を返す。
func UpdatesChannel(userID string) string {
	return updatesChannel(userID)
}
