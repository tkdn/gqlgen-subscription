# 実AWSデプロイ・検証ランブック（ap-northeast-1）

[plan/20260713-lambda-real-aws.md](../plan/20260713-lambda-real-aws.md) の(2)を
実AWSで検証する際の実施手順（2026-07-14作成）。設計の根拠と「Terraformで
解決できない依存関係」の詳細は [terraform/README.md](../terraform/README.md) を参照。

手順はfish想定で書いている。bashで実行する場合は
`set -x VAR value`→`export VAR=value`、`set VAR (cmd)`→`VAR=$(cmd)`、
`env VAR=value command`→`VAR=value command`に読み替える。

## 前提

- ツール: `terraform` / `ko` / `aws` CLI / `docker`（ECRログインの資格情報保存に使う）/
  `session-manager-plugin`（手順5のECS Execに必要。brew/aquaにLinux版が
  ないため、公式debを`dpkg-deb -x`で展開しバイナリをPATH上に置く）
- 権限: SSOの権限セットが`PowerUserAccess`ベースの場合、そのままでは
  IAMロールが作成できずフルapplyが失敗する。権限セットへのインライン
  ポリシー追加が事前に必要（トラブルシューティングの`iam:CreateRole`の
  項を参照）
- ブランチ: `plan-to-aws`（Goコード・`backend/.ko.yaml`・`terraform/`一式がコミット済み）
- 課金: RDS `db.t4g.micro`・Interface VPCエンドポイント8つ（合計約$0.11/時）等が
  作成中ずっと発生する。**検証が終わったら都度destroyする**（手順8）

## 0. 認証（SSO）

ローカル検証（kumo）用の環境変数が残っていると実AWSに届かないため、
先に外す。`AWS_ENDPOINT_URL`はawsコマンド全体を`localhost:4566`へ向け、
静的な資格情報の環境変数は`AWS_PROFILE`より**優先される**:

```fish
set -e AWS_ENDPOINT_URL AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY

aws sso login --profile tkdn.developer
set -x AWS_PROFILE tkdn.developer
# ArnがAWSReservedSSO_...のassumed-roleで、アカウントIDが想定どおりならOK
aws sts get-caller-identity
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

続けてコンテナログで起動状態を確認する。特にappの
`completion consumer disabled by SKIP_COMPLETION_CONSUMER`は、
手順6の「ステータス遷移の原因はLambda以外にない」という判断の
根拠なので必ず見る:

```fish
aws logs tail /ecs/gqlgen-subscription/app --region ap-northeast-1 --since 10m
# → completion consumer disabled by SKIP_COMPLETION_CONSUMER
# → connect to http://localhost:8080/ for GraphQL playground

aws logs tail /ecs/gqlgen-subscription/workersim --region ap-northeast-1 --since 10m
# → workersim: polling job-requests, delay=3s
```

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

- フルapplyでIAMロール作成が`AccessDenied`（`iam:CreateRole`）:
  権限セット`tkdn.PowerUserAccess`のベースであるAWS管理ポリシー
  `PowerUserAccess`はIAMの書き込みを許可しない。IAM Identity Center
  （管理アカウント）で権限セットにインラインポリシーを追加する:
  権限セット`tkdn.PowerUserAccess`→「インラインポリシー」に以下を貼り、
  保存（アカウントへの再プロビジョニングは自動で走る。既存のSTS
  クレデンシャルはロールにポリシーが付くだけなので再ログイン不要）。
  `iam:PassRole`はロールを事前作成しても別途必要になる（ECSタスク定義・
  Lambda作成時に呼び出し元へ要求される）ため、ここで一緒に付ける。
  ResourceのアカウントID部分は`*`にしてある（IAMアクションは同一
  アカウント内のロールにしか作用しないため意味は変わらない。自アカウント
  IDに置き換えてもよい）:

  ```json
  {
    "Version": "2012-10-17",
    "Statement": [
      {
        "Sid": "ManageProjectRoles",
        "Effect": "Allow",
        "Action": [
          "iam:CreateRole",
          "iam:DeleteRole",
          "iam:GetRole",
          "iam:TagRole",
          "iam:UntagRole",
          "iam:UpdateAssumeRolePolicy",
          "iam:ListRolePolicies",
          "iam:ListAttachedRolePolicies",
          "iam:ListInstanceProfilesForRole",
          "iam:PutRolePolicy",
          "iam:GetRolePolicy",
          "iam:DeleteRolePolicy",
          "iam:AttachRolePolicy",
          "iam:DetachRolePolicy"
        ],
        "Resource": "arn:aws:iam::*:role/gqlgen-subscription-*"
      },
      {
        "Sid": "PassProjectRoles",
        "Effect": "Allow",
        "Action": "iam:PassRole",
        "Resource": "arn:aws:iam::*:role/gqlgen-subscription-*",
        "Condition": {
          "StringEquals": {
            "iam:PassedToService": [
              "ecs-tasks.amazonaws.com",
              "lambda.amazonaws.com"
            ]
          }
        }
      }
    ]
  }
  ```

  追加後に`terraform apply`を再実行すれば、失敗したIAMロール以降の
  リソースが続きから作成される（applyは冪等）。

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

- applyが途中で失敗した後の再planで`is tainted, so must be replaced`と出る:
  作成の途中（リソース本体は作成済みで属性設定に失敗など）で止まった
  リソースを、Terraformが作りかけ扱いにしている。そのままapplyすれば
  作り直されて収束する（それで問題ない）。作成済みリソースを活かして
  差分更新にしたい場合は`terraform untaint <リソースアドレス>`してから
  applyする
