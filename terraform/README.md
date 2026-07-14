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

## デプロイ・検証・クリーンアップの手順

実施手順（SSOログイン→ECR先行apply→ko push→フルapply→ECS Exec検証→destroy）
は [docs/20260714-real-aws-deploy-runbook.md](../docs/20260714-real-aws-deploy-runbook.md)
にまとめてある。手順の記載はランブック側に一本化し、ここでは設計の根拠のみを
残す（同じコマンド列を二重管理してドリフトさせないため）。
