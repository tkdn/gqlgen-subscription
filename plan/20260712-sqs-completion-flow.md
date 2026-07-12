# ローカルSQS完了通知フローの実装計画（kumoエミュレーション）

## Context

[`docs/architecture.md`](../docs/architecture.md)（ChatGPTとの壁打ちメモ）で検討した「サービスBの完了通知をイベントとして受け取る」という考え方を、実際に検証する段階に進む。現状はGraphQL mutation（`updateJobStatus`）で状態変化を直接シミュレートしているだけで、非同期ワーカーは存在しない。

本計画では、ローカル環境限定（実AWSへのデプロイ・IAM・コストは対象外）で、[kumo](https://github.com/sivchari/kumo) を使い、SQS Standardキューを介した実際の非同期パイプラインを構築する。

- **サービスB（`workersim`）**: 形式的なワーカー。SQSの依頼キューをlong pollingし、受信後10秒待ってから完了メッセージを送る最小実装。特定のjob名パターンで意図的に失敗（`FAILED`送信）させられる
- **Consumer**: 完了メッセージを受け取り、既存の`jobstore.Store.UpdateStatus`を呼ぶ。既存のPublish→Hub→SSE経路は一切変更しない

[`docs/architecture.md`](../docs/architecture.md)で示された「最初は同一プロセスにConsumerを同居させる」という考え方を踏襲し、**Consumerは別バイナリに分離せず、`cmd/main.go`内でgoroutineとして起動する**（当初案では別バイナリ`backend/cmd/consumer`を検討していたが、複雑化を避けるため撤回）。GraphQL APIサーバーと同じプロセス内でSQS `job-completions`をlong pollingし、既存の`signal.NotifyContext`によるグレースフルシャットダウンにそのまま乗せる。

新たに導入する唯一のドメイン概念は、`job_id`（UUID）という相関ID。`jobstore.Store.Create`が採番し、SQSメッセージ（依頼・完了の両方）に載せて運ぶ。

## 確定した設計判断

- **相関IDの運び方**: 完了メッセージは `(userID, job_id, status)` を運ぶ。`name`は運ばない。理由: Redisの実体キー自体を`name`ベースから`job_id`ベースに変更する（後述）ため、`job_id`だけでジョブを一意に特定でき、`name`→`id`や`id`→`name`の逆引きが不要になる。
- **SQSキュー種別**: Standard（FIFOは不採用）。workersimは1ジョブにつき完了メッセージを1通しか送らないため、順序入れ替わりは本質的に発生しない。重複自体は`HSet`が冪等操作のため実害なし。
- **冪等性**: 今回は未実装。既知の課題として記録するのみ。
- **Dispatch失敗時の挙動**: `createJob`内でRedis作成は成功したがSQS投入が失敗した場合、mutation自体をエラーにする（best-effortで握り潰さない）。ローカル検証では「createJobが成功した=workersimが必ず起動される」という単純な保証の方が扱いやすい。
- **キュー配置**: `job-requests`（A→B）と`job-completions`（B→Consumer）の2つ。プロビジョニングは各Goバイナリ起動時に`CreateQueue`を冪等呼び出し（初期化コンテナは使わない）。
- **workersimのトリガー方式**: workersim自身が`job-requests`をlong pollingする。「10秒固定で無条件に1回だけ実行するデモ」ではなく、`createJob`から実際に非連結の非同期パイプラインとして駆動される構成にする。

## 1. スキーマ・モデル変更

`backend/graph/schema.graphqls`に`Job.id: ID!`を追加する。加えて、`updateJobStatus`の第一引数を`name: String!`から`id: ID!`に変更する（後述のRedisキー設計変更に合わせ、クライアントもidでジョブを指定する形に揃える）。`createJob`/`jobs`のシグネチャは変更しない。`go tool gqlgen generate`で再生成する。

```graphql
type Job {
  id: ID!
  name: String!
  status: JobState!
}

type Mutation {
  createJob(name: String!): Job!
  updateJobStatus(id: ID!, status: JobState!): Job!
}
```

`schema.resolvers.go`の`UpdateJobStatus`も`id`を受け取って`JobStore.UpdateStatus`に渡す形に変わる。

## 2. `jobstore.Store` 変更（Redisキー設計の見直しを含む）

critレビューで「job_id導入を機にRedisのデータシェイプ自体を見直してよい」との指摘を受け、実体キーを`name`ベースから`job_id`ベースに変更する。理由: `id`を単にHashの1フィールドとして付け足すだけだと、`name`が依然として唯一の実キーであり続け、(1) 同一ユーザーが同名で`createJob`すると上書きされてUUIDが失われる、(2) `UpdateStatus`が`job_id`を知らないまま`name`で書き込むため、SQS完了メッセージから直接`UpdateStatus`を呼ぶ経路が不自然になる、という歪みが生じるため。

### 新しいキー設計

```
user:<userID>:jobs         Set<job_id>              # 索引: job_idのSet（TTLなし）
job:<userID>:<job_id>      Hash{id, name, status}    # 実体（TTL 5分）
job:updates:<userID>       Pub/Sub channel           # 変更なし
```

`id`（UUIDv7、`github.com/cmackenzie1/go-uuid`の`uuid.NewV7()`で採番）が実体キーの一部になり、名実ともに一意な主キーになる。UUIDv7を選ぶ理由は時刻順ソート可能な性質を持ち、将来Redis以外のストアに移行した場合もキーの局所性を保てるため（本プロジェクトのGoバージョンはUUID標準ライブラリ化前のGo 1.26.2であり、`google/uuid`ではなくこのライブラリを明示的に採用する）。

```go
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

// UpdateStatus は job_id でジョブを直接特定する（name は経由しない）。
func (s *Store) UpdateStatus(ctx context.Context, userID, jobID string, status model.JobState) (*model.Job, error) {
    job := &model.Job{ID: jobID, Status: status}
    if err := s.save(ctx, userID, job); err != nil {
        return nil, err
    }
    return job, nil // name はsave()内でHGetAllせず、呼び出し側は必要ならListで取得する
}

func jobKey(userID, jobID string) string {
    return fmt.Sprintf("job:%s:%s", userID, jobID)
}

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
```

`UpdateStatus`は`job.Name`を持たないため、`save()`は`name`フィールドを空文字列で上書きしないようガードする（`Create`時に一度書いた`name`は`UpdateStatus`では保持される）。

`List()`は索引Setのメンバーが`job_id`になった点以外は構造が同じ（各`job_id`について`HGetAll`し、`fields["name"]`・`fields["status"]`・`fields["id"]`（`= job_id`と同じ値）から`model.Job`を組み立てる）。

新規依存: `github.com/cmackenzie1/go-uuid`（`go get`で追加、バージョンは実装時に解決）。

テスト追加・変更（`store_test.go`、DB15）:
- `Create`が空でない`ID`を返すこと、`List`が同じ`ID`を返すこと
- `UpdateStatus`は`jobID`引数で特定すること（`name`ではなく作成時に得た`ID`を渡す形にテストを書き換える）
- 同一ユーザーが同じ`name`で`Create`を2回呼んでも、両方が別ジョブとして`List`に残ること（旧設計での上書き問題が解消したことの回帰テスト）
- 既存の`TestListGarbageCollectsExpiredIndex`等、Redisキー文字列を直接組み立てているテストは`job:<userID>:<id>`形式に書き換える

## 3. 共有AWS/SQSクライアントパッケージ: `backend/awsconfig`

`redisclient`と同様のフラットパッケージ。

```go
package awsconfig

func New(ctx context.Context) (aws.Config, error)          // kumoエンドポイント向けaws.Config構築
func SQSClient(cfg aws.Config) *sqs.Client                  // SQSクライアント構築
func EnsureQueue(ctx context.Context, client *sqs.Client, name string) (url string, err error) // CreateQueueの冪等呼び出し
```

- エンドポイント: `AWS_ENDPOINT_URL`環境変数（デフォルト`http://localhost:4566`）。SDK v2のconfigローダーが標準で認識する環境変数名を優先し、実装時に実際のSDKバージョンの挙動を確認する。
- 認証情報: `credentials.NewStaticCredentialsProvider("test", "test", "")`（ダミー、kumoは実認証しない）。
- リージョン: デフォルト`us-east-1`、`AWS_REGION`で上書き可能。
- SQSクライアントのエンドポイント上書きは、非推奨の`EndpointResolverWithOptions`ではなく`sqs.NewFromConfig(cfg, func(o *sqs.Options) { o.BaseEndpoint = aws.String(endpoint) })`を使う（実装時に実際のSDKバージョンのAPIを確認）。

新規依存（正確なバージョンは実装時に`go get`で解決）:
- `github.com/aws/aws-sdk-go-v2`
- `github.com/aws/aws-sdk-go-v2/config`
- `github.com/aws/aws-sdk-go-v2/credentials`
- `github.com/aws/aws-sdk-go-v2/service/sqs`

## 4. `docker-compose.yml` 変更

`kumo`サービスを追加。実際のDockerイメージ名・タグ・ヘルスチェック方式（HTTPエンドポイントの有無等）は実装時に[kumoのリポジトリ](https://github.com/sivchari/kumo)を確認して決定する（本計画では仮置き）。

```yaml
services:
  redis:
    # ...既存のまま...

  kumo:
    image: <実装時に確認>
    ports:
      - "4566:4566"
    healthcheck:
      # 実装時にkumoの実際のヘルスチェック方式を確認して設定
      interval: 5s
      timeout: 3s
      retries: 5
```

## 5. `graph`パッケージ: 新しいDispatchインターフェース

既存の「インターフェースは利用側で定義する」慣習に従う。

```go
// backend/graph/resolver.go
type JobDispatcher interface {
    Dispatch(ctx context.Context, userID string, job *model.Job) error
}

type Resolver struct {
    JobStore   JobStore
    Hub        Hub
    Dispatcher JobDispatcher
}
```

`schema.resolvers.go`の`createJob`:

```go
func (r *mutationResolver) CreateJob(ctx context.Context, name string) (*model.Job, error) {
    userID := userctx.UserID(ctx)
    job, err := r.JobStore.Create(ctx, userID, name)
    if err != nil {
        return nil, err
    }
    if err := r.Dispatcher.Dispatch(ctx, userID, job); err != nil {
        return nil, fmt.Errorf("dispatch job: %w", err)
    }
    return job, nil
}
```

Dispatch失敗時はmutation全体をエラーにする（確定判断）。

新規パッケージ `backend/sqsdispatch`（フラット、`graph`に依存しない。`graph/model`にのみ依存）:

```go
package sqsdispatch

type Dispatcher struct {
    client   *sqs.Client
    queueURL string
}

func New(client *sqs.Client, queueURL string) *Dispatcher

type requestMessage struct {
    UserID string `json:"user_id"`
    Name   string `json:"name"`
    JobID  string `json:"job_id"`
}

func (d *Dispatcher) Dispatch(ctx context.Context, userID string, job *model.Job) error {
    // json.Marshal → sqs.SendMessage
}
```

既存の`schema.resolvers_test.go`等で`graph.Resolver{...}`を組み立てているテストは、`Dispatcher`フィールド用のテストダブル（記録用フェイク等）を追加する必要がある。

## 6. `backend/cmd/workersim`（サービスBの形式的実装）

`job-requests`をlong polling。メッセージ受信ごとに、待機（デフォルト10秒、`WORKERSIM_DELAY`環境変数でDuration文字列として上書き可能）→完了メッセージ送信→元の依頼メッセージを削除。

**意図的な失敗の注入**: job名が`fail-`で始まる場合、`Status: "COMPLETED"`ではなく`Status: "FAILED"`を送信する（例: `fail-timeout`, `fail-anything`）。これにより検証時に任意のタイミングで失敗パスを再現できる。ハードコードされた`const failNamePrefix = "fail-"`で判定し、確率的な失敗注入は導入しない（テストの再現性を優先する）。判定には依頼メッセージに含まれる`Name`を使うが、完了メッセージ自体には`Name`を含めない（`job_id`のみでConsumerが処理できるため）。

```go
type completionMessage struct {
    UserID string `json:"user_id"`
    JobID  string `json:"job_id"`
    Status string `json:"status"`
}
```

ループ本体は`Run`関数として切り出す。Goの`package main`は外部からimportできないため、`backend/cmd/workersim`ではなく`backend/workersim`（フラット、`consumer`パッケージと同じ配置方針）に`Run`関数を置き、`cmd/workersim/main.go`はこれを呼ぶ薄い起動コードのみにする。e2eテストは`workersim.Run`を直接importして呼べる。

```go
// backend/workersim/workersim.go
package workersim

func Run(ctx context.Context, client *sqs.Client, requestsURL, completionsURL string, delay time.Duration) error
```

グレースフルシャットダウン: `signal.NotifyContext`パターン。10秒待機中にシャットダウンシグナルを受けた場合は完了メッセージを送らずに終了する（`ctx.Done()`のselect分岐）。

## 7. Consumer（`cmd/main.go`内のgoroutine）

`job-completions`をlong polling。メッセージごとに`completionMessage{UserID, JobID, Status}`をUnmarshalし（`name`は運ばない。`jobstore.Store`の実体キーが`job_id`ベースになったため不要）、`model.JobState(status).IsValid()`を確認した上で`jobstore.Store.UpdateStatus(ctx, userID, jobID, status)`を呼ぶ。成功したらメッセージ削除、失敗時は削除せず再配信に任せる（冪等性は今回未実装のため許容）。

別バイナリではなく、`backend/consumer`パッケージ（フラット、`cmd/`配下ではない）に`Run`関数を切り出し、`cmd/main.go`の`main()`からgoroutineとして起動する。e2eテストからも同じ関数を直接呼べる。

```go
// backend/consumer/consumer.go
package consumer

func Run(ctx context.Context, client *sqs.Client, store *jobstore.Store, completionsURL string) error
```

`cmd/main.go`の変更点: 既存の`signal.NotifyContext`で作った`ctx`を使い、`go consumer.Run(ctx, sqsClient, jobStore, completionsURL)`をサーバー起動前後どちらかで開始する。`ctx.Done()`でconsumerのポーリングループも自然に終了するため、追加のシャットダウン処理は不要（`http.Server.Shutdown`とは独立した並行処理として扱う。goroutineの終了を待ち合わせる必要がある場合は`sync.WaitGroup`を検討するが、今回はログ目的の`log.Println("consumer: shutting down")`のみで足りると判断し、待ち合わせは行わない）。

Hubは不要（consumerはSSEを配信しない。Redis Publishは`jobstore.Store.UpdateStatus`が内部で行い、同じプロセス内のHubがそれを拾ってSSEに流す、という既存の経路をそのまま利用する）。

## 8. テスト方針

- `backend/jobstore/store_test.go`（DB15）に`job_id`関連のアサーションを追加（既存）。
- `backend/awsconfig/awsconfig_test.go`: kumoに対する実結合テスト。到達不能なら`t.Skipf`（Redis不在時と同じパターン）。`EnsureQueue`を2回呼んで同じURLが返ることを確認。
- `backend/sqsdispatch/dispatch_test.go`: kumoに対する実結合テスト。テストごとに一意なキュー名を使い、テスト間のメッセージ混入を避ける。
- `backend/consumer/consumer_test.go`: `Run`が受信したメッセージから`UpdateStatus`を正しく呼ぶことを検証する結合テスト（実Redis DB15 + kumo）。不正な`status`値のメッセージが渡された場合に削除されること（再配信ループに入らないこと）も確認する。
- `backend/workersim/workersim_test.go`: `fail-`プレフィックスでの失敗注入ロジック（`Status: "COMPLETED"`か`"FAILED"`かの判定）を単体テストで確認する。長時間の待機を伴う結合テスト（kumo相手にキュー経由で一連の流れを検証するもの）は、e2eテストでカバーし、ここでは作らない。
- **e2eテスト**（`backend/e2e/sqs_completion_test.go`、既存の`sse_test.go`と同じDB13を再利用）: `workersim.Run`をin-processのgoroutineとして起動し、`consumer.Run`も同様にin-processで起動する（`cmd/main.go`同様、テストサーバー構築時に一緒に起動する）。`delay`は数百ミリ秒程度の短い値を直接パラメータとして渡す（`WORKERSIM_DELAY`環境変数には依存しない）。2ケース検証する: (1) 通常のjob名で`createJob`→SSE購読で`COMPLETED`状態のスナップショットが届くこと、(2) `fail-`プレフィックスのjob名で`createJob`→SSE購読で`FAILED`状態が届くこと。kumoが起動していなければ`t.Skipf`。

## 9. 実装ステップ（1ステップ=1コミット+1 critレビュー）

1. スキーマ+モデル: `Job.id`追加、`updateJobStatus`引数を`id: ID!`に変更、`gqlgen generate`
2. `jobstore.Store`: 実体キーを`name`ベースから`job_id`ベースに変更（UUIDv7採番・保存・読み出し）+ `schema.resolvers.go`のUpdateJobStatus対応 + テスト
3. `backend/awsconfig`パッケージ（kumo向けAWS設定・SQSクライアント・キュー冪等作成）+ テスト
4. `docker-compose.yml`にkumoサービス追加（実際のイメージ・ヘルスチェックを確認して設定）
5. `backend/sqsdispatch`パッケージ + `graph.JobDispatcher`インターフェース + `createJob`リゾルバ変更 + 既存テストのDispatcherダブル追加
6. `backend/workersim`パッケージ（`Run`関数 + `fail-`プレフィックス失敗注入） + 薄い`cmd/workersim/main.go`
7. `backend/consumer`パッケージ（`Run`関数） + `cmd/main.go`へのgoroutine統合 + テスト
8. e2eテスト（`sqs_completion_test.go`、通常系・失敗系の両方）
9. ドキュメント更新: `docs/architecture.md`に実装結果の追記セクション、`README.md`に起動手順・動作確認手順を追加

各ステップは単独でコンパイル・テストが通る状態を維持する。ステップ5が既存のresolver挙動を変更する唯一のステップのため、critレビューで特に注意する。

## 10. 動作確認（ローカル・手動）

```bash
# 1. インフラ起動
docker compose up -d redis kumo

# 2. 別ターミナルでそれぞれ起動（backend/から）
go run cmd/main.go        # GraphQL API + Consumer(goroutine), :8080
go run cmd/workersim

# 3. ジョブ作成（通常系）
curl -s http://localhost:8080/query -H 'content-type: application/json' \
  --data '{"query":"mutation { createJob(name: \"job-1\") { id name status } }"}'

# 3b. 失敗系を再現したい場合は fail- プレフィックスを使う
curl -s http://localhost:8080/query -H 'content-type: application/json' \
  --data '{"query":"mutation { createJob(name: \"fail-job-1\") { id name status } }"}'

# 4. SSE購読（別ターミナル、任意）
curl -N -s http://localhost:8080/query \
  -H 'content-type: application/json' -H 'accept: text/event-stream' \
  --data '{"query":"subscription { jobStatuses { id name status } }"}'

# 5. 約10秒後、workersim/main.goのログとSSEストリーム（またはjobsクエリ再実行）で
#    job-1のstatusがCOMPLETED（fail-job-1はFAILED）になっていることを確認する
```

`go test ./...`は、`docker compose up -d redis kumo`が起動していれば全パッケージがフルに実行され、起動していなければ実インフラ依存テストが`t.Skipf`で優雅にスキップされる。

## 今回のスコープ外として残す既知の課題

- 冪等性なし（重複配信は`HSet`の上書きで無害だが、将来複数状態遷移メッセージを送る設計になった場合は再検討が必要）
- FIFO/順序保証なし（workersimが1ジョブ1完了メッセージのみ送るため今回は問題にならない）
- デッドレターキューなし（`UpdateStatus`が恒久的に失敗し続けるケースは可視性タイムアウトによる再配信ループになる）
- kumoの実SQSとの挙動差異は未検証（今回はユーザーの明示的な選定によりkumoを採用し、細部の忠実性は検証範囲外とする）
- Consumerのgoroutineは`cmd/main.go`のシャットダウン時に明示的な待ち合わせ（`sync.WaitGroup`等）をしない。プロセス終了時にポーリング中のリクエストが中断される可能性があるが、ローカル検証の範囲では許容する

### 実装時に確認が必要な点

1. kumoの実際のDockerイメージ名・タグ・ヘルスチェック方式
2. AWS SDK v2の実際に解決されるバージョンでのエンドポイント上書きAPI（`BaseEndpoint`等）
