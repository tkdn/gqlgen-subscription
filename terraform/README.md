# 実AWS環境（ap-northeast-1）

[plan/20260713-lambda-real-aws.md](../plan/20260713-lambda-real-aws.md) の実装。
単一VPC・プライベートサブネットのみで、IGW・NAT Gateway・ALBを作らない
（外部公開なし、検証はECS Exec経由）。

## Terraformで解決できない依存関係

コンテナイメージの実在はTerraformのリソースグラフで表現できないため、
applyを2段階に分ける必要がある:

1. `aws_lambda_function`（package_type=Image）は**作成時点でimage_uriの
   イメージが実在しないと失敗する**。ECSサービスもイメージがなければ
   タスクが起動できずrunningCountが0のままになる
2. イメージをプッシュするには先にECRリポジトリが必要

したがって順序は「ECRのみ先行apply → ko push → フルapply」となる。

また、`image_tag`（デフォルト`latest`）の再プッシュはimage_uri文字列を
変えないため**Terraformは変更を検知しない**。コード更新の反映は別途:

- Lambda: `aws lambda update-function-code --function-name <name> --image-uri <uri>`
- ECS: `aws ecs update-service --cluster <cluster> --service <app|workersim> --force-new-deployment`

（デプロイ自動化ツールの導入はスコープ外・別タスク）

## 初回デプロイ手順

```bash
cd terraform
terraform init

# 1. ECRリポジトリのみ先行作成
terraform apply \
  -target=aws_ecr_repository.app \
  -target=aws_ecr_repository.workersim \
  -target=aws_ecr_repository.lambda

# 2. ECRへログインし、koで3イメージをビルド・プッシュ
#    （.ko.yamlがbackend/にあるためbackend/から実行する。
#      イメージ名を1対1で対応させるため--bare必須）
aws ecr get-login-password --region ap-northeast-1 |
  docker login --username AWS --password-stdin \
  "$(terraform output -raw ecr_app_repository_url | cut -d/ -f1)"

APP_REPO=$(terraform output -raw ecr_app_repository_url)
WORKERSIM_REPO=$(terraform output -raw ecr_workersim_repository_url)
LAMBDA_REPO=$(terraform output -raw ecr_lambda_repository_url)

cd ../backend
KO_DOCKER_REPO=$APP_REPO       ko build --bare ./cmd
KO_DOCKER_REPO=$WORKERSIM_REPO ko build --bare ./cmd/workersim
KO_DOCKER_REPO=$LAMBDA_REPO    ko build --bare ./cmd/lambda
cd ../terraform

# 3. フルapply（VPC/RDS/SQS/IAM/ECS/Lambda一式。RDS作成に10分程度かかる）
terraform apply

# 4. ECSサービスの起動を待つ（ヘルスチェック・ALBがないためrunningCountで確認）
aws ecs describe-services --cluster gqlgen-subscription --services app workersim \
  --region ap-northeast-1 \
  --query 'services[].{name:serviceName,running:runningCount,desired:desiredCount}'
```

## 検証手順（ECS Exec）

```bash
TASK_ARN=$(aws ecs list-tasks --cluster gqlgen-subscription --service-name app \
  --region ap-northeast-1 --query 'taskArns[0]' --output text)

aws ecs execute-command --cluster gqlgen-subscription --task "$TASK_ARN" \
  --container app --interactive --command "/bin/sh" --region ap-northeast-1
```

タスク内で:

```bash
# ジョブ作成（依頼キューへ送信される）
curl -s http://localhost:8080/query -H 'content-type: application/json' \
  --data '{"query":"mutation { createJob(name: \"job-1\") { id name status } }"}'

# workersim(3s)→完了キュー→Lambda→RDS更新後、COMPLETEDに遷移していることを確認
curl -s http://localhost:8080/query -H 'content-type: application/json' \
  --data '{"query":"query { jobs { id name status } }"}'
```

Lambdaが発火したこと（＝ステータス変化の唯一の経路であること）はCloudWatch
Logsで確認する。appのconsumerはSKIP_COMPLETION_CONSUMER=trueで無効化されて
いるため、遷移はLambda以外に原因がありえない:

```bash
aws logs tail /aws/lambda/gqlgen-subscription-completion-handler --follow \
  --region ap-northeast-1
```

## クリーンアップ

検証が終わったら都度destroyする（RDSは停止しても7日で自動再起動されるため、
止めて放置する運用はしない）:

```bash
terraform destroy
```

ECR（force_delete）・RDS（skip_final_snapshot、deletion_protection=false）・
Secrets Manager（recovery_window_in_days=0）はdestroyが失敗しないよう設定済み。
