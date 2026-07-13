output "ecr_app_repository_url" {
  description = "appイメージのko pushプッシュ先"
  value       = aws_ecr_repository.app.repository_url
}

output "ecr_workersim_repository_url" {
  description = "workersimイメージのko pushプッシュ先"
  value       = aws_ecr_repository.workersim.repository_url
}

output "ecr_lambda_repository_url" {
  description = "Lambdaイメージのko pushプッシュ先"
  value       = aws_ecr_repository.lambda.repository_url
}

output "ecs_cluster_name" {
  description = "ECS Exec検証で使うクラスター名"
  value       = aws_ecs_cluster.main.name
}

output "rds_endpoint" {
  description = "RDSのエンドポイント（ホスト名）"
  value       = aws_db_instance.main.address
}

output "lambda_function_name" {
  description = "completion-handlerの関数名（CloudWatch Logsの確認に使う）"
  value       = aws_lambda_function.completion_handler.function_name
}
