// Package pgclient はPostgreSQLへの接続とスキーマの冪等な確保を提供する。
// 接続情報はコード側で決め打ちせず、libpq互換の標準解決チェーン
// （PGHOST/PGPORT/PGUSER/PGPASSWORD/PGDATABASE/PGSSLMODE等の環境変数）に
// 完全に委ねる。環境が変わっても環境変数の差し替えだけで動くようにするため。
package pgclient

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// schemaDDL はジョブ管理に必要なスキーマ一式。起動のたびに冪等に実行される
// 前提で書く（検証目的のリポジトリであり、マイグレーションツールによる変更
// 履歴管理より起動の単純さを優先する）。
//
// CREATE TYPEにはIF NOT EXISTS構文がないため、duplicate_objectを握りつぶす
// DOブロックで冪等化する。ENUMの値はgraph/modelのJobStateと同一の5値を
// ミラーしており、値を追加する場合はALTER TYPE job_state ADD VALUEが必要
// （値の削除はできない）。
//
// テスト用にスキーマ(search_path)を分離できるよう、DDLはスキーマ修飾なしで
// 書く（ENUM型もスキーマスコープなので、テスト用スキーマごとに独立して作られる）。
const schemaDDL = `
DO $$ BEGIN
    CREATE TYPE job_state AS ENUM ('PENDING', 'ANALYZING', 'GENERATING', 'COMPLETED', 'FAILED');
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

CREATE TABLE IF NOT EXISTS jobs (
    id         uuid PRIMARY KEY,
    user_id    text NOT NULL,
    name       text NOT NULL,
    status     job_state NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS jobs_user_id_idx ON jobs (user_id);
`

// New は標準解決チェーンの設定で接続プールを生成する。
func New(ctx context.Context) (*pgxpool.Pool, error) {
	pool, err := pgxpool.New(ctx, "")
	if err != nil {
		return nil, fmt.Errorf("pgclient: new pool: %w", err)
	}
	return pool, nil
}

// Connect は標準解決チェーンの設定で単一接続を生成する。LISTENのような
// セッションレベルの機能はプールの接続では状態が保てないため、専用接続を
// 必要とする呼び出し側（pgpubsub.Hub）が使う。
func Connect(ctx context.Context) (*pgx.Conn, error) {
	conn, err := pgx.Connect(ctx, "")
	if err != nil {
		return nil, fmt.Errorf("pgclient: connect: %w", err)
	}
	return conn, nil
}

// EnsureSchema はスキーマ一式を冪等に確保する。起動時に呼ぶ。
func EnsureSchema(ctx context.Context, pool *pgxpool.Pool) error {
	if _, err := pool.Exec(ctx, schemaDDL); err != nil {
		return fmt.Errorf("pgclient: ensure schema: %w", err)
	}
	return nil
}
