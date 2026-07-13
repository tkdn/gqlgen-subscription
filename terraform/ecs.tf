resource "aws_ecs_cluster" "main" {
  name = var.project
}

resource "aws_cloudwatch_log_group" "app" {
  name              = "/ecs/${var.project}/app"
  retention_in_days = 7
}

resource "aws_cloudwatch_log_group" "workersim" {
  name              = "/ecs/${var.project}/workersim"
  retention_in_days = 7
}

# --- app ---
# GraphQL API本体。完了メッセージの処理はLambdaへ一本化するため
# SKIP_COMPLETION_CONSUMER=trueでconsumerを無効化する。検証はECS Exec
# （タスク内curl）で行うためALB・公開エンドポイントは作らない。

resource "aws_ecs_task_definition" "app" {
  family                   = "${var.project}-app"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = "256"
  memory                   = "512"
  execution_role_arn       = aws_iam_role.ecs_execution.arn
  task_role_arn            = aws_iam_role.app_task.arn

  # koで--platform=linux/arm64ビルドしたイメージに合わせる
  runtime_platform {
    operating_system_family = "LINUX"
    cpu_architecture        = "ARM64"
  }

  container_definitions = jsonencode([{
    name      = "app"
    image     = "${aws_ecr_repository.app.repository_url}:${var.image_tag}"
    essential = true
    portMappings = [{
      containerPort = 8080
      protocol      = "tcp"
    }]
    environment = [
      { name = "SKIP_COMPLETION_CONSUMER", value = "true" },
      { name = "PGHOST", value = aws_db_instance.main.address },
      { name = "PGPORT", value = "5432" },
      { name = "PGUSER", value = var.db_username },
      { name = "PGDATABASE", value = var.db_name },
      { name = "PGSSLMODE", value = "require" },
    ]
    secrets = [{
      name      = "PGPASSWORD"
      valueFrom = aws_secretsmanager_secret.rds_password.arn
    }]
    logConfiguration = {
      logDriver = "awslogs"
      options = {
        "awslogs-group"         = aws_cloudwatch_log_group.app.name
        "awslogs-region"        = local.region
        "awslogs-stream-prefix" = "app"
      }
    }
    # ECS ExecのSSMエージェントプロセスの回収のためinitプロセスを有効化する
    # （AWSドキュメントの推奨設定）
    linuxParameters = {
      initProcessEnabled = true
    }
  }])
}

resource "aws_ecs_service" "app" {
  name                   = "app"
  cluster                = aws_ecs_cluster.main.id
  task_definition        = aws_ecs_task_definition.app.arn
  desired_count          = 1
  launch_type            = "FARGATE"
  enable_execute_command = true

  network_configuration {
    subnets          = aws_subnet.private[*].id
    security_groups  = [aws_security_group.ecs_app.id]
    assign_public_ip = false
  }
}

# --- workersim ---
# 依頼キューをポーリングし完了キューへ送るだけの常時稼働サービス。
# RDS接続なし・ECS Execなし。

resource "aws_ecs_task_definition" "workersim" {
  family                   = "${var.project}-workersim"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = "256"
  memory                   = "512"
  execution_role_arn       = aws_iam_role.ecs_execution.arn
  task_role_arn            = aws_iam_role.workersim_task.arn

  runtime_platform {
    operating_system_family = "LINUX"
    cpu_architecture        = "ARM64"
  }

  container_definitions = jsonencode([{
    name      = "workersim"
    image     = "${aws_ecr_repository.workersim.repository_url}:${var.image_tag}"
    essential = true
    environment = [
      { name = "WORKERSIM_DELAY", value = var.workersim_delay },
    ]
    logConfiguration = {
      logDriver = "awslogs"
      options = {
        "awslogs-group"         = aws_cloudwatch_log_group.workersim.name
        "awslogs-region"        = local.region
        "awslogs-stream-prefix" = "workersim"
      }
    }
  }])
}

resource "aws_ecs_service" "workersim" {
  name            = "workersim"
  cluster         = aws_ecs_cluster.main.id
  task_definition = aws_ecs_task_definition.workersim.arn
  desired_count   = 1
  launch_type     = "FARGATE"

  network_configuration {
    subnets          = aws_subnet.private[*].id
    security_groups  = [aws_security_group.ecs_workersim.id]
    assign_public_ip = false
  }
}
