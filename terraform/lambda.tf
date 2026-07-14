# completion-handler。job-completionsキューのevent source mappingで起動し、
# RDSのUPDATEとpg_notifyを同一トランザクションで行う（コードは
# backend/cmd/lambda）。イメージはkoでECRへプッシュ済みであることが作成の
# 前提（README.mdの段階的applyを参照）。

resource "aws_cloudwatch_log_group" "lambda" {
  name              = "/aws/lambda/${var.project}-completion-handler"
  retention_in_days = 7
}

resource "aws_lambda_function" "completion_handler" {
  function_name = "${var.project}-completion-handler"
  package_type  = "Image"
  image_uri     = "${aws_ecr_repository.lambda.repository_url}:${var.image_tag}"
  role          = aws_iam_role.lambda.arn
  architectures = ["arm64"]
  timeout       = 30
  memory_size   = 256

  # 同時実行の制限はevent source mapping側のmaximum_concurrencyで行う。
  # reserved_concurrent_executionsはアカウントの同時実行クォータ（この
  # アカウントは10）から「非予約分を最低10残す」制約により1すら予約
  # できないため使えない。

  # RDSアクセスのためVPCへアタッチする。SQS到達性は不要（ポーリングは
  # LambdaサービスのマネージドインフラがVPC外で行う）。
  vpc_config {
    subnet_ids         = aws_subnet.private[*].id
    security_group_ids = [aws_security_group.lambda.id]
  }

  environment {
    variables = {
      PGHOST           = aws_db_instance.main.address
      PGPORT           = "5432"
      PGUSER           = var.db_username
      PGDATABASE       = var.db_name
      PGSSLMODE        = "require"
      PGPASSWORD       = random_password.rds.result
      PGPOOL_MAX_CONNS = "2"
    }
  }

  # 関数が先に作られると自動生成のロググループ（retention無期限）が
  # できてしまうため、明示的に作ったロググループを先行させる
  depends_on = [
    aws_cloudwatch_log_group.lambda,
    aws_iam_role_policy_attachment.lambda_vpc,
  ]
}

resource "aws_lambda_event_source_mapping" "completions" {
  event_source_arn = aws_sqs_queue.job_completions.arn
  function_name    = aws_lambda_function.completion_handler.arn
  # 既存consumerの1メッセージ単位処理と挙動を揃える
  batch_size = 1
  # 部分バッチ失敗レポート（lambdahandlerがBatchItemFailuresを返す前提）
  function_response_types = ["ReportBatchItemFailures"]

  # 同時実行数×PGPOOL_MAX_CONNS(2)がRDS(db.t4g.micro)の接続数上限を
  # 圧迫しないよう制限する（2はmaximum_concurrencyの最小値）
  scaling_config {
    maximum_concurrency = 2
  }
}
