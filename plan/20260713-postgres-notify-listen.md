# (3) Pub/Sub・ジョブストアの PostgreSQL NOTIFY/LISTEN 移行

## Context

[docs/20260712-brushup-feasibility.md](../docs/20260712-brushup-feasibility.md) の見立てに基づく (3)→(2) 逐次実行の第1弾。現行の Redis 実装には、`HSET` と `PUBLISH` が別コマンドであるため、2コマンドの間でプロセスが落ちると「DB は更新されたのに通知だけが失われる」取りこぼしが起こりうる、という弱点があり、Postgres では `UPDATE` と `NOTIFY` を同一トランザクションに入れることでこれを解消できる。今後 DB として PostgreSQL を使う予定があることも採用根拠。

確認済みの設計判断:

1. **インターフェースは共通化し、main.go は Postgres 固定**。Redis 実装（`jobstore`/`pubsub`/`redisclient`）はテストごと参照実装として残す。docker-compose の redis サービスも残留（削除すると Redis 実装テストが全 skip されデッドコード化するため）
2. **TTL は廃止・永続化**（Redis 版の5分揮発は引き継がない）
3. **スキーマは起動時に `CREATE TABLE IF NOT EXISTS`**（マイグレーションツールなし。`awsconfig.EnsureQueue` と同じ冪等確保の思想。本リポジトリが検証目的であり、スキーマ変更履歴の管理より起動の単純さを優先するため）
4. **冪等化（終端状態からの遷移拒否）を本スコープに含める**。Redis 実装との意味論の乖離は許容し、インターフェースコメントで「冪等保証は実装依存」と文書化する

## 全体像

```
現行: jobstore(Redis Hash+PUBLISH) / pubsub(Redis SUBSCRIBE, ユーザーごと接続)
移行: pgjobstore(INSERT/UPDATE + pg_notify 同一tx) / pgpubsub(単一LISTEN接続 + userIDでデマルチプレクス)
```

- NOTIFY チャンネルは単一（`job_updates`）、ペイロードに userID を載せる。Redis 版の「ユーザーごとチャンネル・ユーザーごと接続」から「単一接続・Hub 内デマルチプレクス」に変わる（Postgres の `max_connections` 圧迫回避、識別子63バイト制限回避）
- 通知はトリガーのみ。ペイロードの userID は「単一チャンネルに全ユーザーの通知が混ざるため、どの購読者へ配るか」を Hub が判別するルーティング情報であり、それ以外の意味を持たない。受信側はこれまでどおり `List` でスナップショットを取り直す（スナップショット取得を省略できるわけではない）。ジョブ内容を運ばないのは、DB を正本とする現行方針を維持し、NOTIFY ペイロード上限（8000バイト）や通知の順序・欠落の問題を配信経路に持ち込まないため
- 依存追加: `github.com/jackc/pgx/v5`（最新 v5.10.0）。LISTEN は `pgx.Conn.WaitForNotification`、Store は `pgxpool`
- 接続設定は `pgxpool.ParseConfig("")` により libpq 互換環境変数（`PGHOST`/`PGPORT`/`PGUSER`/`PGPASSWORD`/`PGDATABASE`/`PGSSLMODE`）の標準解決チェーンに完全に委ねる（awsconfig と同じ「決め打ちせず標準解決チェーンに委ねる」方針）

## スキーマ

```sql
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
```

- `status` は ENUM 型 `job_state` とし、値は GraphQL スキーマの `JobState`（`graph/model`）と同一の5値をミラーする。不正値は DB 層でも拒否される
- `CREATE TYPE` には `IF NOT EXISTS` 構文がないため、`duplicate_object` を握りつぶす `DO` ブロックで冪等化する
- 将来 `JobState` に値を追加する場合は `ALTER TYPE job_state ADD VALUE` が必要（値の削除はできない）。GraphQL enum と DDL の二重管理になる点は ENUM 採用の代償として許容する
- `id` は既存の UUIDv7 採番（`jobstore.Store.Create` と同じ `cmackenzie1/go-uuid`）をそのまま使う。テスト用にスキーマ（`search_path`）を分離できるよう、DDL はスキーマ修飾なしで書く（ENUM 型もスキーマスコープなので、テスト用スキーマごとに独立して作られる）

## 新規パッケージ

### `backend/pgclient/`（`redisclient` の対）

- `New(ctx) (*pgxpool.Pool, error)`: `pgxpool.ParseConfig("")` → `pgxpool.NewWithConfig`。環境変数未設定時は libpq デフォルト（localhost:5432）に落ちる
- `EnsureSchema(ctx, pool) error`: 上記 DDL を冪等実行（`awsconfig.EnsureQueue` と同じ位置づけ）

### `backend/pgjobstore/`（`jobstore` の対）

`graph.JobStore` インターフェースを満たす `Store`。

- `Create`: `INSERT` + `SELECT pg_notify('job_updates', user_id)` を同一 tx で実行
- `UpdateStatus`: 条件付き UPDATE で冪等化。

  ```sql
  UPDATE jobs SET status = $3::job_state, updated_at = now()
  WHERE user_id = $1 AND id = $2 AND status NOT IN ('COMPLETED', 'FAILED')
  RETURNING id, name, status
  ```

  （`status` は ENUM のため、テキストで渡すパラメータは `::job_state` で明示キャストする）

  - 1行更新: 同一 tx 内で `pg_notify` して返す
  - 0行: 行の存在を確認し、(a) 終端状態で存在 → **no-op 成功**（NOTIFY なし。SQS 再配信で consumer が二重処理しても安全）、(b) 行が存在しない → エラー（メッセージは削除されず可視性タイムアウト後に再配信、現行 consumer のエラーパスと同じ）
  - Redis 版の「upsert・遷移制約なし」との意味論乖離は `graph.JobStore` のコメントに明記
- `List`: `SELECT ... WHERE user_id = $1 ORDER BY id`（UUIDv7 なので作成順。Redis 版の SMembers 無順序より決定的になる）
- チャンネル名定数 `UpdatesChannel = "job_updates"` をここで export（Redis 版の `jobstore.UpdatesChannel(userID)` の対。ただし引数なしの単一チャンネル）

### `backend/pgpubsub/`（`pubsub` の対）

`graph.Hub` インターフェースを満たす `Hub`。

- `New(ctx, connConfig, channel)` で専用の `*pgx.Conn` を1本張り `LISTEN` を確立、背景 goroutine で `WaitForNotification` ループを回す。channel を引数にするのはテスト分離のため（NOTIFY チャンネルは DB グローバルでスキーマ分離が効かない。並列実行される他パッケージのテストとのクロストーク防止）
- `Subscribe(userID)` は購読者マップへの登録のみ。LISTEN は起動時に確立済みのため、Redis 版にあった「SUBSCRIBE ack を待ってから返す」race 対策は構造的に不要になる
- 通知受信時、ペイロード（userID）に一致する購読者へ非ブロッキング送信（現行 `pubsub.Hub.relay` と同じ `select`+`default` パターン）
- 接続エラー時は log を出して再接続・再 LISTEN（小さな backoff 付きループ）。再接続中の通知欠落は許容（スナップショット方式のため次の通知で回復。コメントで明記）
- `Close()` で接続と goroutine を停止（テストの Cleanup 用）

## 既存コードの変更

### `backend/consumer/consumer.go`

`Run` が具象型 `*jobstore.Store` を受けている箇所をインターフェースに変更（(2) Lambda 化の土台にもなる継ぎ目）:

```go
type JobStatusUpdater interface {
    UpdateStatus(ctx context.Context, userID, jobID string, status model.JobState) (*model.Job, error)
}
```

`jobstore.Store`・`pgjobstore.Store` の両方が満たす。consumer_test.go は既存のまま Redis 実装で通る（シグネチャ互換のため変更最小）。

### `backend/graph/resolver.go`

`JobStore` インターフェースのコメントに「冪等保証（終端状態からの遷移拒否）は実装依存。pgjobstore は保証し、jobstore(Redis) は保証しない参照実装」と追記。シグネチャ変更なし。

### `backend/cmd/main.go`

`redisclient.New()` → `pgclient.New(ctx)` + `pgclient.EnsureSchema`。`jobstore.New(rdb)` → `pgjobstore.New(pool)`、`pubsub.New(rdb)` → `pgpubsub.New(...)`。Redis への参照を除去。

### `docker-compose.yml`

`postgres` サービス追加（`postgres:18-alpine`、`POSTGRES_USER/PASSWORD/DB=app`、`pg_isready` healthcheck、5432公開）。redis は残留。

### `backend/e2e/`（sse_test.go / sqs_completion_test.go）

本番配線（main.go）が Postgres 固定になるため、e2e も Postgres 接続に切り替える。`newTestServer`/`newSQSTestServer` の Redis 接続 + FlushDB を、Postgres 接続 + `TRUNCATE jobs` + テスト専用チャンネル名に置き換え。Postgres 未起動時は Redis 版同様 `t.Skipf`。

### `README.md`

起動手順に postgres を追加（`docker compose up -d postgres redis kumo`）、`PGHOST` 等の環境変数設定例を追記。TTL 廃止（ジョブは永続化、掃除は `TRUNCATE jobs`）を明記。

## テスト計画（regression-first の順で実施）

1. **`pgjobstore/store_test.go`**: 既存 `jobstore/store_test.go` の Create/UpdateStatus/List 系テストを移植 + 冪等化の新規テスト（COMPLETED 後の UpdateStatus が no-op になり NOTIFY も飛ばないこと）。テスト分離は専用スキーマ（`CREATE SCHEMA` + `search_path` を接続設定に指定）+ TRUNCATE。Postgres 未起動時 skip
2. **`pgpubsub/hub_test.go`**: 既存 `pubsub/hub_test.go` の fan-out・unsubscribe 系テストを移植。テスト専用チャンネル名で分離
3. **e2e**: 既存2ファイルの移行。SQS 完了フロー（PENDING→COMPLETED / fail- プレフィックス→FAILED）が Postgres 経由で通ることを確認
4. 既存の Redis 実装テスト（jobstore/pubsub/consumer）が無変更で通り続けることを確認

## 検証手順

1. `docker compose up -d postgres redis kumo`
2. `go test ./...`（backend。Redis 系・Postgres 系の両方が実行されることを確認）
3. `golangci-lint run`
4. 手動 e2e: README の手順どおり main.go + workersim + frontend を起動し、`createJob` → SSE で COMPLETED 配信、`fail-` → FAILED 配信、終端後の `updateJobStatus` が no-op になることを確認

## スコープ外

- Redis 実装への冪等化バックポート（乖離は文書化で対応）
- pgbouncer 等プーリング層との LISTEN 両立（採用時の制約として feasibility メモに記載済み）
- (2) Lambda 化（consumer のインターフェース切り出しまでが本スコープの準備）
- architecture.md の §7 更新は実装完了後に別途行う
