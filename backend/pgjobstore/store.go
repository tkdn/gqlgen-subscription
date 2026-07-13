// Package pgjobstore はユーザーごとのジョブをPostgreSQLで管理する。
// 書き込みと更新通知(pg_notify)を同一トランザクションで行うため、
// 「DBは更新されたのに通知だけが失われる」取りこぼしが起きない。
// 通知はコミット成功時のみ配送される。
package pgjobstore

import (
	"context"
	"errors"
	"fmt"

	"github.com/cmackenzie1/go-uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/tkdn/gqlgen-subscription/backend/graph/model"
)

// UpdatesChannel はジョブ更新通知のNOTIFYチャンネル名のデフォルト。
// ユーザーごとにチャンネルを分けず、単一チャンネルのペイロードにuserIDを
// 載せて運ぶ（受信側のHubがuserIDで購読者へ振り分ける）。
const UpdatesChannel = "job_updates"

// Store はユーザーごとのジョブをPostgreSQLで管理する。
type Store struct {
	pool    *pgxpool.Pool
	channel string
}

// New はStoreを生成する。channelは更新通知のNOTIFYチャンネル名で、
// 通常はUpdatesChannelを渡す。購読側(pgpubsub.Hub)と同じ値で揃える必要が
// あるため、配線側(main.go等)で明示的に渡す。
func New(pool *pgxpool.Pool, channel string) *Store {
	return &Store{pool: pool, channel: channel}
}

// Create は新しいジョブをPENDING状態で作成する。userIDの解決は呼び出し側（resolverなど）の責務とする。
// ここで採番するIDは、SQS等の非同期メッセージがどのジョブに対応するかを示す相関IDであり、
// jobsテーブルの主キーでもある。UUIDv7（時刻順ソート可能）を使い、主キーの局所性を保つ。
func (s *Store) Create(ctx context.Context, userID, name string) (*model.Job, error) {
	id, err := uuid.NewV7()
	if err != nil {
		return nil, fmt.Errorf("pgjobstore: generate job id: %w", err)
	}

	job := &model.Job{ID: id.String(), Name: name, Status: model.JobStatePending}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("pgjobstore: begin: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // コミット後のRollbackは常にエラーを返すが無害

	if _, err := tx.Exec(ctx,
		`INSERT INTO jobs (id, user_id, name, status) VALUES ($1, $2, $3, $4::job_state)`,
		job.ID, userID, job.Name, string(job.Status),
	); err != nil {
		return nil, fmt.Errorf("pgjobstore: insert job: %w", err)
	}
	if err := s.notify(ctx, tx, userID); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("pgjobstore: commit: %w", err)
	}
	return job, nil
}

// UpdateStatus は既存ジョブのステータスを変更する。終端状態
// （COMPLETED/FAILED）に達したジョブへの更新は黙って無視し、現在の行を
// そのまま返す（通知も発行しない）。SQSのat-least-once配信で完了メッセージが
// 重複しても、2回目以降が何も起こさないことを保証するための冪等化。
// ジョブが存在しない場合はエラーを返す。
func (s *Store) UpdateStatus(ctx context.Context, userID, jobID string, status model.JobState) (*model.Job, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("pgjobstore: begin: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // コミット後のRollbackは常にエラーを返すが無害

	var name string
	err = tx.QueryRow(ctx,
		`UPDATE jobs SET status = $3::job_state, updated_at = now()
		 WHERE user_id = $1 AND id = $2 AND status NOT IN ('COMPLETED', 'FAILED')
		 RETURNING name`,
		userID, jobID, string(status),
	).Scan(&name)

	if errors.Is(err, pgx.ErrNoRows) {
		// 更新0行は「行が存在しない」か「終端状態」のどちらか。区別して返す。
		var currentName, currentStatus string
		err := tx.QueryRow(ctx,
			`SELECT name, status FROM jobs WHERE user_id = $1 AND id = $2`,
			userID, jobID,
		).Scan(&currentName, &currentStatus)
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, fmt.Errorf("pgjobstore: job %q not found", jobID)
		}
		if err != nil {
			return nil, fmt.Errorf("pgjobstore: get current job: %w", err)
		}
		return &model.Job{ID: jobID, Name: currentName, Status: model.JobState(currentStatus)}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("pgjobstore: update job: %w", err)
	}

	if err := s.notify(ctx, tx, userID); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("pgjobstore: commit: %w", err)
	}
	return &model.Job{ID: jobID, Name: name, Status: status}, nil
}

// List はユーザーの全ジョブを作成順（UUIDv7の時刻順）で返す。
func (s *Store) List(ctx context.Context, userID string) ([]*model.Job, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT id, name, status FROM jobs WHERE user_id = $1 ORDER BY id`, userID)
	if err != nil {
		return nil, fmt.Errorf("pgjobstore: list jobs: %w", err)
	}
	defer rows.Close()

	jobs := make([]*model.Job, 0)
	for rows.Next() {
		var id, name, status string
		if err := rows.Scan(&id, &name, &status); err != nil {
			return nil, fmt.Errorf("pgjobstore: scan job: %w", err)
		}
		jobs = append(jobs, &model.Job{ID: id, Name: name, Status: model.JobState(status)})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("pgjobstore: iterate jobs: %w", err)
	}
	return jobs, nil
}

// notify はトランザクション内で更新通知を発行する。ペイロードは購読者振り分け
// 用のuserIDのみで、ジョブ内容は運ばない（受信側はListでスナップショットを
// 取り直す）。
func (s *Store) notify(ctx context.Context, tx pgx.Tx, userID string) error {
	if _, err := tx.Exec(ctx, `SELECT pg_notify($1, $2)`, s.channel, userID); err != nil {
		return fmt.Errorf("pgjobstore: notify update: %w", err)
	}
	return nil
}
