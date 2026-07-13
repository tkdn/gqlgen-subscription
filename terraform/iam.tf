data "aws_iam_policy_document" "ecs_tasks_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["ecs-tasks.amazonaws.com"]
    }
  }
}

data "aws_iam_policy_document" "lambda_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

# --- ECS実行ロール（app/workersim共用） ---
# イメージpull・ログ送信に加え、タスク定義のsecretsブロック（PGPASSWORD）の
# 解決に使う。

resource "aws_iam_role" "ecs_execution" {
  name               = "${var.project}-ecs-execution"
  assume_role_policy = data.aws_iam_policy_document.ecs_tasks_assume.json
}

resource "aws_iam_role_policy_attachment" "ecs_execution_managed" {
  role       = aws_iam_role.ecs_execution.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}

resource "aws_iam_role_policy" "ecs_execution_secrets" {
  name = "read-rds-password"
  role = aws_iam_role.ecs_execution.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = "secretsmanager:GetSecretValue"
      Resource = aws_secretsmanager_secret.rds_password.arn
    }]
  })
}

# --- appタスクロール ---
# EnsureQueue(CreateQueue)とsqsdispatchのSendMessage、ECS Exec用のSSM
# データチャネル。ssmmessagesはリソースレベルの制限に対応していないため
# Resource "*"。

resource "aws_iam_role" "app_task" {
  name               = "${var.project}-app-task"
  assume_role_policy = data.aws_iam_policy_document.ecs_tasks_assume.json
}

resource "aws_iam_role_policy" "app_task_sqs" {
  name = "sqs"
  role = aws_iam_role.app_task.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = [
        "sqs:CreateQueue",
        "sqs:SendMessage",
        "sqs:GetQueueUrl",
        "sqs:GetQueueAttributes",
      ]
      Resource = [
        aws_sqs_queue.job_requests.arn,
        aws_sqs_queue.job_completions.arn,
      ]
    }]
  })
}

resource "aws_iam_role_policy" "app_task_exec" {
  name = "ecs-exec"
  role = aws_iam_role.app_task.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = [
        "ssmmessages:CreateControlChannel",
        "ssmmessages:CreateDataChannel",
        "ssmmessages:OpenControlChannel",
        "ssmmessages:OpenDataChannel",
      ]
      Resource = "*"
    }]
  })
}

# --- workersimタスクロール ---

resource "aws_iam_role" "workersim_task" {
  name               = "${var.project}-workersim-task"
  assume_role_policy = data.aws_iam_policy_document.ecs_tasks_assume.json
}

resource "aws_iam_role_policy" "workersim_task_sqs" {
  name = "sqs"
  role = aws_iam_role.workersim_task.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = [
        "sqs:CreateQueue",
        "sqs:ReceiveMessage",
        "sqs:DeleteMessage",
        "sqs:SendMessage",
        "sqs:GetQueueAttributes",
      ]
      Resource = [
        aws_sqs_queue.job_requests.arn,
        aws_sqs_queue.job_completions.arn,
      ]
    }]
  })
}

# --- Lambda実行ロール ---
# event source mappingのSQSポーリング自体がこのロールで行われるため、
# 関数コードがSQSを呼ばなくてもSQS権限が必要。環境変数の暗号化はAWS管理
# キー（aws/lambda）を使うため、明示的なkms:Decrypt権限は不要。

resource "aws_iam_role" "lambda" {
  name               = "${var.project}-lambda"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume.json
}

resource "aws_iam_role_policy_attachment" "lambda_vpc" {
  role       = aws_iam_role.lambda.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaVPCAccessExecutionRole"
}

resource "aws_iam_role_policy" "lambda_sqs" {
  name = "sqs"
  role = aws_iam_role.lambda.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = [
        "sqs:ReceiveMessage",
        "sqs:DeleteMessage",
        "sqs:GetQueueAttributes",
        "sqs:ChangeMessageVisibility",
      ]
      Resource = aws_sqs_queue.job_completions.arn
    }]
  })
}
