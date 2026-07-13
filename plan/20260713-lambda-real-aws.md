# (2) SQS完了メッセージのLambda化 + 実AWSデプロイ（ECS Fargate / RDS / SQS / Lambda）

## Context

[docs/20260713-real-aws-migration-policy.md](../docs/20260713-real-aws-migration-policy.md) で確定した方針の実装計画。kumo（ローカルAWSエミュレーター）はSQS→Lambda event source mappingのコントロールプレーンAPI（`CreateEventSourceMapping`等のCRUD）のみ実装しており、データプレーン（実際にLambdaを起動する処理）が存在しないことが調査で判明した（kumo PR #202、`internal/service/sqs/`にEventSourceMapping参照が皆無）。この制約下ではLambdaハンドラを書いても「SQSトリガーとして実際に動く」ことをローカルで検証できないため、(2)の検証を実AWS環境（リージョン: **ap-northeast-1**）で行う。

既存実装（`backend/consumer/consumer.go`の`JobStatusUpdater`インターフェース、`backend/pgjobstore/store.go`の冪等な`UpdateStatus`、`backend/awsconfig`・`backend/pgclient`の標準解決チェーン委譲）はLambda化の継ぎ目として既に用意されている（[plan/20260713-postgres-notify-listen.md](./20260713-postgres-notify-listen.md)参照）。

## 確定した設計判断

以降の実装で迷わないよう、この会話で確定した判断を先に列挙する。

1. **実AWS化の対象はSQS・Lambda・PostgreSQL(RDS)のみ**。Redisは対象外（方針ドキュメント既定）
2. **RDSはプライベートサブネットに配置**。app（ECS）・workersim（ECS）・Lambdaはいずれも同一VPCの同一プライベートサブネットに配置
3. **Lambdaは VPC アタッチするが SQS 到達性は不要**。SQSのポーリング自体はLambdaサービス側のマネージドインフラで行われ、Lambda関数自体はイベントを受け取るだけでSQS APIを能動的に呼ばないため
4. **appタスク（ECS）はSQS到達性が必要**（`EnsureQueue`と`sqsdispatch.Dispatch`のSendMessageのため）。**Interface VPC Endpoint**（`com.amazonaws.ap-northeast-1.sqs`）経由で到達させる。NAT Gatewayは使わない（低トラフィックのためVPCエンドポイントの方が安価）
5. **実AWS検証はECS Exec（`aws ecs execute-command`）でappタスク内に入りcurlで確認する**。ローカルのREADME手動確認手順と同じ操作感をECS上で再現する
6. **ECS Exec（SSM Session Manager経由）のため、追加でVPCエンドポイントが3つ必要**: `com.amazonaws.ap-northeast-1.ssmmessages`, `ssm`, `ec2messages`
7. **appタスクのイメージにはcurlが必要**（ECS Exec検証のため）
8. **DockerfileではなくkoでビルドしECRへプッシュ**する。app・workersimの両方をkoでビルドする。ko使用時はデフォルトがdistroless系（curl・shellなし）になるため、**`.ko.yaml`でcurl入りのカスタムbase imageを明示指定する**（7の要件を満たすため）
9. **workersimも同一ECSクラスター内の別サービスとしてタスク化する**。RDS接続は不要なため、SQS到達性（VPCエンドポイント経由）のみで完結する。データフロー: `app(ECS Exec)→createJob→SQS依頼キュー→workersim(ECS)→SQS完了キュー→Lambda→RDS更新→app の pgpubsub.Hub(LISTEN)→SSE`。workersimはappへ直接応答を返さない
10. **`SKIP_COMPLETION_CONSUMER`環境変数を`cmd/main.go`に追加**し、ECSタスク定義でのみ`true`に設定する（ローカルは未設定のまま、デフォルト値は「スキップしない」＝現状維持）。これによりECS上ではconsumer.RunがSQS完了キューをlong-pollingしなくなり、完了メッセージの処理はLambdaだけが行うようになる。Lambda発火の検証信号を明確にするための変更
11. **`completionMessage`を`consumer.CompletionMessage`としてexport**し、Lambdaハンドラから再利用する（3つ目の重複を避ける）
12. **Lambdaは`ReportBatchItemFailures`による部分バッチ失敗レポートを使う**。`batch_size=1`（既存consumerの1メッセージ単位処理と挙動を揃え、複雑さを抑える）
13. **RDSのパスワードはSecrets Manager経由**。ECSタスク定義は`secrets`ブロックでネイティブに解決。Lambdaは環境変数（KMS暗号化）に静的注入（LambdaのenvironmentブロックはECSのsecretsブロックのような実行時解決に対応していないため）
14. **Lambdaのコードデプロイ形式はコンテナイメージ（ko + ECR）**。app・workersimと同じ`ko`でビルドしECRへプッシュする（`aws-lambda-go/lambda`の`lambda.Start()`呼び出しはzip方式でもコンテナ方式でもコード変更不要のため統一できる。参考: [ko公式Lambdaガイド](https://ko.build/advanced/lambda/)）。3成果物すべてのビルド手段が`ko`に一本化される。**lambroll（Lambda版のecspresso）・koのTerraform Providerは導入しない** — 方針ドキュメントで「ECSのデプロイ自動化（ecspresso）は今回のスコープ外、別途検討」と決めているため、対称性を保ちLambda側も同じ扱いとする。Terraformが管理するのはLambda関数本体（初回作成、`package_type=Image`+`image_uri`参照）・IAMロール・event source mappingで、以降のコード更新の自動化は両方とも別タスク。curl入りカスタムbase imageがLambda Runtime API実装イメージと両立できるかは実装時に検証する（Lambda向けbase imageには通常特有の制約があるため）
15. **ECS Execによる検証手順の最終的な記録形式（README追記かscripts/化か）は決定を先送りする**。まず動くことを確認してから決める
16. **ALB・公開HTTPエンドポイントは作らない**。検証は全てECS Exec経由のcurlで完結させる。プライベートサブネットのみでSSE/GraphQL APIの外部公開は本計画のスコープ外

## 全体構成

```
[実AWS: ap-northeast-1, 単一VPC・プライベートサブネットのみ]

ECS Cluster (gqlgen-subscription)
 ├─ Service: app (Fargate, desiredCount=1, ECS Exec有効)
 │    - GraphQL API (Query/Mutation/Subscription/SSE)
 │    - SKIP_COMPLETION_CONSUMER=true（consumer.Runを起動しない）
 │    - RDSへ接続、SQS依頼キューへSendMessage、job-completionsキューへEnsureQueueのみ
 │
 └─ Service: workersim (Fargate, desiredCount=1)
      - SQS依頼キューをlong-polling、待機後、完了キューへSendMessage
      - RDS接続なし

RDS for PostgreSQL (db.t4g.micro, Single-AZ, プライベートサブネット)

SQS: job-requests, job-completions（現行と同じ2キュー構成）

Lambda: completion-handler
 - job-completionsキューのevent source mappingで起動
 - VPCアタッチ（RDSアクセスのため）、SQS到達性は不要
 - RDSをUPDATE + pg_notify（同一tx、pgjobstore.Storeをそのまま再利用）

VPC Interface Endpoints: sqs, ssmmessages, ssm, ec2messages（4つ、Interface型のみ、NAT Gatewayなし）
```

## Go コード変更

### `backend/consumer/consumer.go`

- `completionMessage` → `CompletionMessage`にexport（フィールドは既にexported）。ロジック変更なし
- `consumer.Run`・`JobStatusUpdater`はそのまま維持（ローカルのkumo経由の動作を一切変えないため。ECS上ではSKIP_COMPLETION_CONSUMERで無効化されるだけで、コード自体は削除しない）

### `backend/cmd/main.go`

`consumer.Run`のgoroutine起動を環境変数でスキップ可能にする:

```go
skipConsumer := os.Getenv("SKIP_COMPLETION_CONSUMER") == "true"
if !skipConsumer {
    go func() {
        if err := consumer.Run(ctx, sqsClient, jobStore, completionsURL); err != nil {
            log.Printf("consumer: %v", err)
        }
    }()
}
```

未設定時（ローカル）は現状と完全に同一の挙動。

### 新規パッケージ `backend/lambdahandler/handler.go`

`consumer`・`workersim`と同じ「flat package + 薄いcmd/エントリポイント」方針（`package main`はテストからimportできないため）。

```go
type Handler struct {
    Store consumer.JobStatusUpdater
}

func New(store consumer.JobStatusUpdater) *Handler

func (h *Handler) HandleRequest(ctx context.Context, sqsEvent events.SQSEvent) (events.SQSEventResponse, error)
```

判断基準はconsumer.Runと同一にする:

| ケース | 挙動 |
|---|---|
| 不正JSON | ack（BatchItemFailuresに含めない）、ログのみ |
| 不正status | ack（BatchItemFailuresに含めない）、ログのみ |
| UpdateStatus成功（冪等no-op含む） | ack |
| UpdateStatusのエラー | BatchItemFailuresにMessageIdを追加（再配信） |

### 新規 `backend/cmd/lambda/main.go`

```go
func main() {
    ctx := context.Background()
    pool, err := pgclient.New(ctx) // PGPOOL_MAX_CONNSで小さくプールを制限
    // EnsureSchema、pgjobstore.New、lambdahandler.New、lambda.Start(h.HandleRequest)
}
```

- コールドスタート時に一度だけpoolを構築（deferでのCloseはしない。ウォームコンテナ間で使い回す）
- `pgclient.New`に`PGPOOL_MAX_CONNS`環境変数のサポートを追加（`pgxpool.ParseConfig("")`→`MaxConns`上書き→`NewWithConfig`。未設定時は現状と完全に同じ挙動）。Lambda側は`PGPOOL_MAX_CONNS=2`、`reserved_concurrent_executions=5`でRDSの接続数上限を抑える

### `go.mod`

`github.com/aws/aws-lambda-go`を追加（`go get github.com/aws/aws-lambda-go@latest` → `go mod tidy`）。

### テスト

- `backend/lambdahandler/handler_test.go`: フェイクの`JobStatusUpdater`（インメモリ）+ `events.SQSEvent`を直接構築。実AWS不要、**ビルドタグなしで`go test ./...`に含める**（consumer_test.goと違い、Lambda側は受け取ったイベントを処理するだけでSQS API自体を呼ばないため、実SQSが不要）
- `backend/consumer/consumer_test.go`: `completionMessage`→`consumer.CompletionMessage`利用に変更するのみ、他は無変更
- **実AWS検証（ECS Exec経由）はGoテストにしない**。対応するテストコードが存在しないため、ビルドタグでの分離対象自体がない。手順は決定14で先送り

## Terraform（`/home/tkdn/ghq/github.com/tkdn/gqlgen-subscription/terraform/`）

単一ルートモジュール、リージョン`ap-northeast-1`固定。

```
terraform/
├── versions.tf / providers.tf
├── variables.tf / outputs.tf
├── network.tf       # VPC・プライベートサブネット×2AZ・SG・4つのInterface VPCエンドポイント
├── rds.tf            # RDSインスタンス・サブネットグループ
├── sqs.tf              # job-requests / job-completions
├── ecr.tf                # app・workersim用ECRリポジトリ（ko push先）
├── iam.tf                 # ECS実行/タスクロール×2種、Lambda実行ロール
├── ecs.tf                   # クラスター、app/workersimのタスク定義・サービス
├── lambda.tf                 # Lambda関数（コンテナイメージ、package_type=Image）、event source mapping
├── secrets.tf                  # RDSパスワード（random_password + Secrets Manager）
└── terraform.tfvars.example
```

### ネットワーク詳細

- VPC 1つ、プライベートサブネット2AZ（RDSサブネットグループの最小要件）。**パブリックサブネット・IGW（Internet Gateway）・NAT Gatewayを一切作らない**
- SG: `vpc_endpoints`（app/workersim/lambdaから443受信）、`ecs_app`（SQS/SSMエンドポイントへ443、RDSへ5432）、`ecs_workersim`（SQSエンドポイントへ443のみ、RDSアクセスなし）、`lambda`（RDSへ5432のみ、SQS/SSMは不要）、`rds`（app/workersim/lambdaから5432受信）
- Interface VPCエンドポイント4つ: `sqs`（app・workersim用）、`ssmmessages`・`ssm`・`ec2messages`（app用、ECS Exec用）

**internet-facingにならないことの根拠（攻撃ベクター増加の懸念への回答）**: IGWを作らない設計のため、VPC内から外部への経路（NAT Gateway等）も、外部からVPC内への経路（ALB・パブリックIP等）も構成上存在しない。ECS（app/workersim）は`assign_public_ip=false`相当（プライベートサブネットのみのためパブリックIP自体を持てない）、ALBも作らない（決定16）ためインバウンド到達経路がない。RDS・LambdaもプライベートサブネットのみでpubliclyAccessibleではない。Interface VPCエンドポイントはPrivateLink経由でインターネットを経由しない片方向の経路であり、エンドポイント自体もインバウンド公開されない。ECS Exec（curl検証）はSSMのデータチャネル（AWS管理のトンネル）を使うため、待受ポートを外部に開ける操作ではない。

**実装時に必ず確認すべき点として明記**: 上記はTerraformコードが設計どおりに書かれていることが前提。VPCエンドポイントのセキュリティグループで意図せず広い許可（例: `0.0.0.0/0`からの443受信）を入れてしまうと、SG設定ミス単体でinternet-facingになるわけではないが（IGWがない限りインターネットからの到達自体が起きない）、将来IGWを追加する変更が入った場合に危険な設定が温存されるリスクがある。実装時のコードレビューでSGのCIDR/Source指定が各SG間の参照（`security_group_id`指定）になっており`0.0.0.0/0`を含まないことを確認する。

### RDS

- `db.t4g.micro`、Single-AZ、`allocated_storage=20`、`skip_final_snapshot=true`、`deletion_protection=false`（destroy可能性を優先）
- エンジンバージョン・ストレージタイプ（gp2/gp3）・無料枠の正確な適用条件は実装時にAWSコンソール/CLIで確認する（方針ドキュメントの既存の留保を踏襲）

### SQS

- `job-requests`、`job-completions`（現行と同名）
- `job-completions`の`visibility_timeout_seconds`はLambdaタイムアウトの6倍を目安に設定（Lambdaタイムアウト30秒なら180秒）
- Terraform作成後もapp起動時の`awsconfig.EnsureQueue`（属性なしCreateQueue）が同名キューに対して呼ばれるが、属性を指定しないため冪等性は保たれる（衝突しない）

### ECS

- クラスター1つ、Fargate、`cpu=256`/`memory=512`（app・workersim共通の小規模設定）
- appサービス: `enable_execute_command=true`、`desiredCount=1`、ALBなし。環境変数に`SKIP_COMPLETION_CONSUMER=true`、`PGHOST`等（RDSエンドポイント）、`secrets`ブロックで`PGPASSWORD`
- workersimサービス: `desiredCount=1`、ALBなし、RDS関連環境変数は不要。`WORKERSIM_DELAY`は明示指定（検証を早めるため短縮を検討、例: `10s`のまま or `3s`に短縮は実装時に判断）
- イメージはkoでECRにプッシュしたものをタスク定義で参照

### Lambda

- `package_type="Image"`、`image_uri`はkoでECRへプッシュしたイメージURI、`architectures=["arm64"]`、`timeout=30`、`memory_size=256`
- `vpc_config`でプライベートサブネット+`lambda` SGにアタッチ
- `environment`: `PGHOST`/`PGPORT`/`PGUSER`/`PGDATABASE`/`PGSSLMODE`/`PGPASSWORD`（KMS暗号化）/`PGPOOL_MAX_CONNS=2`
- `reserved_concurrent_executions=5`
- コードは`ko build ./backend/cmd/lambda`でビルド・ECRへプッシュ（Lambda用ECRリポジトリを追加）。Goコード側は`aws-lambda-go/lambda`の`lambda.Start()`のままで変更不要

### IAM

- ECS実行ロール: `AmazonECSTaskExecutionRolePolicy` + Secrets Manager `GetSecretValue`（RDSシークレットARNへスコープ）
- ECSタスクロール（app用）: SQS（`CreateQueue`/`SendMessage`/`GetQueueUrl`/`GetQueueAttributes`、両キューARNへスコープ）+ ECS Exec用SSM権限（`ssmmessages:CreateControlChannel`/`CreateDataChannel`/`OpenControlChannel`/`OpenDataChannel`、Resource `*`）
- ECSタスクロール（workersim用）: SQS（`CreateQueue`/`ReceiveMessage`/`DeleteMessage`/`SendMessage`/`GetQueueAttributes`、両キューARNへスコープ）
- Lambda実行ロール: `AWSLambdaVPCAccessExecutionRole`（ENI管理）+ SQS（`ReceiveMessage`/`DeleteMessage`/`GetQueueAttributes`/`ChangeMessageVisibility`、job-completions ARNへスコープ。event source mappingのポーリング自体がLambda実行ロールを使うため必要）+ KMS `Decrypt`（環境変数暗号化キー）

### ECR

- app用・workersim用・Lambda用の3リポジトリ、`force_delete=true`（destroy可能性のため）

## ビルド・デプロイ手順（初回）

1. `terraform apply -target=aws_ecr_repository.app -target=aws_ecr_repository.workersim -target=aws_ecr_repository.lambda` — ECRリポジトリのみ先行作成
2. `ko build ./backend/cmd/main --platform=linux/arm64` / `ko build ./backend/cmd/workersim --platform=linux/arm64`（`.ko.yaml`でcurl入りbase image・ECRリポジトリを指定、`KO_DOCKER_REPO`環境変数でプッシュ先を指定）
3. `ko build ./backend/cmd/lambda --platform=linux/arm64`（Lambda用ECRリポジトリへプッシュ。base imageがLambda Runtime API実装イメージである必要があるため、curl入り指定と両立できるかは実装時に検証する）
4. `terraform apply`（フル）— VPC/RDS/SQS/IAM/ECS/Lambda一式を作成。ECSタスク定義・Lambda関数はいずれも2・3でプッシュ済みのイメージURIを参照
5. RDS利用可能・ECSサービスがRUNNINGになるまで待機（ヘルスチェック・ALBなしのため、`aws ecs describe-services`で`runningCount`を確認）

## 検証手順

```bash
aws ecs list-tasks --cluster gqlgen-subscription --service-name app --region ap-northeast-1
aws ecs execute-command --cluster gqlgen-subscription --task <task-arn> \
  --container app --interactive --command "/bin/sh" --region ap-northeast-1
```

タスク内で:

```bash
curl -s http://localhost:8080/query -H 'content-type: application/json' \
  --data '{"query":"mutation { createJob(name: \"job-1\") { id name status } }"}'
```

workersimはECS上で自動的にjob-requestsキューを処理し完了メッセージを送るため、追加操作は不要（ローカルのworkersim起動コマンドに相当する操作はECS上では常時稼働のサービスとして代替される）。数秒〜`WORKERSIM_DELAY`後、再度タスク内でcurlし`jobs`クエリを叩いてCOMPLETED/FAILEDへの遷移を確認する。

```bash
curl -s http://localhost:8080/query -H 'content-type: application/json' \
  --data '{"query":"query { jobs { id name status } }"}'
```

Lambdaが実際に発火したことは、CloudWatch Logsで確認する（ECS側のconsumerはSKIP_COMPLETION_CONSUMER=trueで無効化されているため、ステータス変化はLambda以外に原因がありえない）:

```bash
aws logs tail /aws/lambda/gqlgen-subscription-completion-handler --follow --region ap-northeast-1
```

## テストの実行範囲

- `go test ./...`: 変更なし（kumo経由の既存テスト・e2e）+ `backend/lambdahandler`の新規ユニットテストが追加（実AWS不要、ビルドタグなし）
- 実AWS検証: 上記の手動ランブックのみ。Goテストとしては存在しない

## 検証後のクリーンアップ

```bash
terraform destroy
```

- `aws_ecr_repository`の`force_delete=true`、RDSの`skip_final_snapshot=true`・`deletion_protection=false`により、destroyが失敗しないようにしている
- RDSは停止しても7日で自動再起動される仕様（[AWS公式ドキュメント](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/USER_StopInstance.html)）があるため、「止めて再開」ではなく検証後は都度`terraform destroy`する運用とする

## スコープ外（今回含めないこと、方針ドキュメントの既定に加えて）

- ECS・Lambdaのデプロイ自動化ツール（ecspresso・lambroll）導入。両方とも別タスクとして先送り
- ALB・公開HTTPエンドポイント
- Multi-AZ・リードレプリカ等のRDS高可用性構成
- ECS水平スケール時の実負荷検証
- ECS Exec検証手順の最終的なドキュメント形式（README追記かscripts化か）の決定
