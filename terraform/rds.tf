resource "aws_db_subnet_group" "main" {
  name       = var.project
  subnet_ids = aws_subnet.private[*].id

  tags = { Name = var.project }
}

# 検証用の最小構成。destroy可能性を最優先し、スナップショット・バックアップ・
# 削除保護をすべて無効にする（plan/20260713-lambda-real-aws.md）。
resource "aws_db_instance" "main" {
  identifier     = var.project
  engine         = "postgres"
  engine_version = var.rds_engine_version
  instance_class = "db.t4g.micro"

  allocated_storage = 20
  storage_type      = "gp3"

  db_name  = var.db_name
  username = var.db_username
  password = random_password.rds.result

  db_subnet_group_name   = aws_db_subnet_group.main.name
  vpc_security_group_ids = [aws_security_group.rds.id]
  publicly_accessible    = false
  multi_az               = false

  backup_retention_period = 0
  skip_final_snapshot     = true
  deletion_protection     = false
  apply_immediately       = true

  tags = { Name = var.project }
}
