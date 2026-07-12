package jobstore

import (
	"context"
	"fmt"
	"time"

	"github.com/cmackenzie1/go-uuid"
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

// jobKey はジョブ実体のキーを組み立てる。jobIDが実キーであり、nameは表示用の
// 属性に過ぎない（同じuserIDで同名のジョブが複数存在してよい）。
func jobKey(userID, jobID string) string {
	return fmt.Sprintf("job:%s:%s", userID, jobID)
}

func updatesChannel(userID string) string {
	return fmt.Sprintf("job:updates:%s", userID)
}

// Create は新しいジョブをPENDING状態で作成する。userIDの解決は呼び出し側（resolverなど）の責務とする。
// ここで採番するIDは、SQS等の非同期メッセージがどのジョブに対応するかを示す相関IDであり、
// Redis上の実キーでもある（同一userIDで同名のジョブを複数回作成しても別ジョブとして扱われる）。
// UUIDv7（時刻順ソート可能）を使い、将来Redis以外のストアに移行してもキーの局所性を保てるようにする。
func (s *Store) Create(ctx context.Context, userID, name string) (*model.Job, error) {
	id, err := uuid.NewV7()
	if err != nil {
		return nil, fmt.Errorf("jobstore: generate job id: %w", err)
	}

	job := &model.Job{ID: id.String(), Name: name, Status: model.JobStatePending}

	if err := s.save(ctx, userID, job); err != nil {
		return nil, err
	}
	return job, nil
}

// UpdateStatus は既存ジョブのステータスを任意の値に変更する（検証目的で遷移制約なし）。
// jobIDでジョブを直接特定するため、nameを経由しない。戻り値のNameは空のままになる
// （呼び出し側がnameを必要とする場合はListで取得する）。
func (s *Store) UpdateStatus(ctx context.Context, userID, jobID string, status model.JobState) (*model.Job, error) {
	job := &model.Job{ID: jobID, Status: status}

	if err := s.save(ctx, userID, job); err != nil {
		return nil, err
	}
	return job, nil
}

// save はジョブ実体をHashとして書き込み、索引Setへの登録・TTL再設定・Publishを行う。
// job.Nameが空文字列の場合（UpdateStatus経由の呼び出し）はnameフィールドを書き換えない。
func (s *Store) save(ctx context.Context, userID string, job *model.Job) error {
	key := jobKey(userID, job.ID)

	fields := map[string]any{"id": job.ID, "status": string(job.Status)}
	if job.Name != "" {
		fields["name"] = job.Name
	}
	if err := s.rdb.HSet(ctx, key, fields).Err(); err != nil {
		return fmt.Errorf("jobstore: save job: %w", err)
	}
	if err := s.rdb.Expire(ctx, key, jobTTL).Err(); err != nil {
		return fmt.Errorf("jobstore: set ttl: %w", err)
	}
	if err := s.rdb.SAdd(ctx, indexKey(userID), job.ID).Err(); err != nil {
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

	ids, err := s.rdb.SMembers(ctx, idxKey).Result()
	if err != nil {
		return nil, fmt.Errorf("jobstore: list job ids: %w", err)
	}

	jobs := make([]*model.Job, 0, len(ids))
	for _, id := range ids {
		fields, err := s.rdb.HGetAll(ctx, jobKey(userID, id)).Result()
		if err != nil {
			return nil, fmt.Errorf("jobstore: get job %q: %w", id, err)
		}

		status, ok := fields["status"]
		if !ok {
			// TTL失効によりHash実体が消えている。索引から取り除く。
			if err := s.rdb.SRem(ctx, idxKey, id).Err(); err != nil {
				return nil, fmt.Errorf("jobstore: gc stale index %q: %w", id, err)
			}
			continue
		}

		jobs = append(jobs, &model.Job{ID: id, Name: fields["name"], Status: model.JobState(status)})
	}

	return jobs, nil
}

// UpdatesChannel はユーザーのジョブ更新通知チャンネル名を返す。
func UpdatesChannel(userID string) string {
	return updatesChannel(userID)
}
