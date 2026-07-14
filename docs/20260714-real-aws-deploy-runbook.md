# 実AWSデプロイ・検証ランブック（ap-northeast-1）

[plan/20260713-lambda-real-aws.md](../plan/20260713-lambda-real-aws.md) の(2)を
実AWSで検証する際の実施手順（2026-07-14作成）。設計の根拠と「Terraformで
解決できない依存関係」の詳細は [terraform/README.md](../terraform/README.md) を参照。

手順はfish想定で書いている。bashで実行する場合は
`set -x VAR value`→`export VAR=value`、`set VAR (cmd)`→`VAR=$(cmd)`、
`env VAR=value command`→`VAR=value command`に読み替える。

## 前提

- ツール: `terraform` / `ko` / `aws` CLI / `docker`（ECRログインの資格情報保存に使う）
- ブランチ: `plan-to-aws`（Goコード・`backend/.ko.yaml`・`terraform/`一式がコミット済み）
- 課金: RDS `db.t4g.micro`・Interface VPCエンドポイント8つ（合計約$0.11/時）等が
  作成中ずっと発生する。**検証が終わったら都度destroyする**（手順8）

## 0. 認証（SSO）

```fish
aws sso login --profile tkdn.developer
set -x AWS_PROFILE tkdn.developer
aws sts get-caller-identity   # アカウントIDとロールが表示されればOK
```

以降のterraform / aws / koコマンドはすべてこのシェル（`AWS_PROFILE`設定済み）で
実行する。リージョンはTerraform側で`ap-northeast-1`に固定してあるが、
awsコマンドには明示的に`--region`を付けている。

## 1. ECRリポジトリのみ先行作成

Lambda関数（`package_type=Image`）は作成時点でイメージが実在しないと失敗する
ため、まずECRだけを`-target`で作る:

```fish
cd terraform
terraform init

terraform plan \
  -target=aws_ecr_repository.app \
  -target=aws_ecr_repository.workersim \
  -target=aws_ecr_repository.lambda

terraform apply \
  -target=aws_ecr_repository.app \
  -target=aws_ecr_repository.workersim \
  -target=aws_ecr_repository.lambda
```

`-target`使用時にTerraformが「targeted applyは例外的な操作」という警告を出すが、
この段階では意図どおりなので無視してよい。作成されるのはECRリポジトリ3つのみ。

## 2. koで3イメージをビルドしECRへプッシュ

```fish
# ECRへのdockerログイン（koはこの資格情報を使ってpushする）
aws ecr get-login-password --region ap-northeast-1 |
  docker login --username AWS --password-stdin \
  (terraform output -raw ecr_app_repository_url | cut -d/ -f1)

set APP_REPO (terraform output -raw ecr_app_repository_url)
set WORKERSIM_REPO (terraform output -raw ecr_workersim_repository_url)
set LAMBDA_REPO (terraform output -raw ecr_lambda_repository_url)

cd ../backend
env KO_DOCKER_REPO=$APP_REPO ko build --bare ./cmd
env KO_DOCKER_REPO=$WORKERSIM_REPO ko build --bare ./cmd/workersim
env KO_DOCKER_REPO=$LAMBDA_REPO ko build --bare ./cmd/lambda
cd ../terraform
```

- **`backend/`から実行する**（goモジュールルートであり、`.ko.yaml`もそこにある）
- **`--bare`必須**。ECRリポジトリ名とイメージ名を1対1で対応させるため
  （なしだと`<repo>/<basename>-<md5>`という別名にpushされ、タスク定義・
  Lambdaの参照先と一致しなくなる）
- プラットフォームは`.ko.yaml`の`defaultPlatforms`（`linux/arm64`）が効くため
  フラグ指定は不要。タグはkoのデフォルトで`latest`

## 3. フルapply

```fish
terraform plan    # 差分を確認（VPC/RDS/SQS/IAM/ECS/Lambda一式）
terraform apply   # RDS作成に10分程度かかる
```

手順2を飛ばしてフルapplyすると、Lambda作成が「image not found」で失敗する。
その場合は手順2を実施してから再度`terraform apply`すればよい。

## 4. ECSサービスの起動確認

ヘルスチェック・ALBがないため、`runningCount`で確認する:

```fish
aws ecs describe-services --cluster gqlgen-subscription --services app workersim \
  --region ap-northeast-1 \
  --query 'services[].{name:serviceName,running:runningCount,desired:desiredCount}'
```

両サービスとも`running`が`desired`（=1）に一致するまで待つ。

## 5. ECS Execでアプリを検証

appタスクに入る:

```fish
set TASK_ARN (aws ecs list-tasks --cluster gqlgen-subscription --service-name app \
  --region ap-northeast-1 --query 'taskArns[0]' --output text)

aws ecs execute-command --cluster gqlgen-subscription --task "$TASK_ARN" \
  --container app --interactive --command "/bin/sh" --region ap-northeast-1
```

タスク内で（シェルはコンテナ側の`/bin/sh`なのでfish記法ではない。
ローカルREADMEの手動確認と同じ操作感）:

```sh
# ジョブ作成。レスポンスはstatus=PENDING
curl -s http://localhost:8080/query -H 'content-type: application/json' \
  --data '{"query":"mutation { createJob(name: \"job-1\") { id name status } }"}'

# workersim(3s待機)→SQS完了キュー→Lambda→RDS更新の後、COMPLETEDに遷移している
# （数秒待ってから実行）
curl -s http://localhost:8080/query -H 'content-type: application/json' \
  --data '{"query":"query { jobs { id name status } }"}'

# fail-プレフィックスのジョブはworkersimが意図的にFAILEDを返す（失敗パスの確認）
curl -s http://localhost:8080/query -H 'content-type: application/json' \
  --data '{"query":"mutation { createJob(name: \"fail-job-1\") { id name status } }"}'
```

SSE配信（PostgreSQL LISTEN→Hub→SSE）まで通しで見る場合は、タスク内で
購読を開きっぱなしにし、別のECS Execセッションから`createJob`を打つ:

```sh
curl -N -s http://localhost:8080/query \
  -H 'accept: text/event-stream' -H 'content-type: application/json' \
  --data '{"query":"subscription { jobStatuses { id name status } }"}'
```

## 6. Lambda発火の確認

appのconsumerは`SKIP_COMPLETION_CONSUMER=true`で無効化されているため、
手順5のステータス遷移はLambda以外に原因がありえない。ログでも直接確認する:

```fish
aws logs tail /aws/lambda/gqlgen-subscription-completion-handler --follow \
  --region ap-northeast-1
```

`lambdahandler: updated job_id=... status=COMPLETED`が出ていればよい。

## 7. コード更新の反映（再検証時のみ）

`latest`タグの再pushは`image_uri`文字列を変えないため**Terraformは変更を
検知しない**。手順2で再pushした後、次で反映する:

```fish
# Lambda
aws lambda update-function-code \
  --function-name gqlgen-subscription-completion-handler \
  --image-uri "$LAMBDA_REPO:latest" --region ap-northeast-1

# ECS（app / workersim それぞれ）
aws ecs update-service --cluster gqlgen-subscription --service app \
  --force-new-deployment --region ap-northeast-1
aws ecs update-service --cluster gqlgen-subscription --service workersim \
  --force-new-deployment --region ap-northeast-1
```

## 8. クリーンアップ

RDSは停止しても7日で自動再起動されるため、「止めて放置」はせず都度destroyする:

```fish
terraform destroy
```

ECR（`force_delete`）・RDS（`skip_final_snapshot`・`deletion_protection=false`）・
Secrets Manager（`recovery_window_in_days=0`）はdestroyが失敗しないよう設定済み。
LambdaをVPCアタッチしているため、ENIの解放待ちでサブネット・セキュリティ
グループのdestroyに時間がかかることがある（Terraformは解放を待つので放置でよい）。

## トラブルシューティング

- `execute-command`が`TargetNotConnectedException`で失敗する:
  タスク起動直後でSSMエージェントがまだ接続していないことが多い。1〜2分待って
  再実行する。次でエージェントの状態を確認できる（`lastStatus: RUNNING`が期待値）:

  ```fish
  aws ecs describe-tasks --cluster gqlgen-subscription --tasks "$TASK_ARN" \
    --region ap-northeast-1 --query 'tasks[].containers[].managedAgents'
  ```

- タスクが起動せず`stoppedReason`が`CannotPullContainerError`:
  手順2のpush漏れ、またはECR/S3系VPCエンドポイントの疎通不良。
  `aws ecs describe-tasks`の`stoppedReason`で詳細を確認する

- フルapplyでLambda作成が`InvalidParameterValueException`（image not found）:
  手順2の前にフルapplyしている。手順2実施後に`terraform apply`を再実行する
