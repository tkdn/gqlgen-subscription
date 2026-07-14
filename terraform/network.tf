# 単一VPC・プライベートサブネットのみの構成。IGW・NAT Gateway・パブリック
# サブネットを一切作らないため、インターネットとの間に経路が構成上存在しない
# （internet-facingにならない根拠。plan/20260713-lambda-real-aws.md参照）。
# AWSサービスへの到達はすべてVPCエンドポイント経由。

data "aws_availability_zones" "available" {
  state = "available"
}

resource "aws_vpc" "main" {
  cidr_block = var.vpc_cidr
  # Interface VPCエンドポイントのprivate DNS（本来のサービスDNS名が
  # エンドポイントENIへ解決される）に両方とも必須。
  enable_dns_support   = true
  enable_dns_hostnames = true

  tags = { Name = var.project }
}

# RDSサブネットグループの最小要件（2AZ）を満たすため2つ作る。
resource "aws_subnet" "private" {
  count = 2

  vpc_id            = aws_vpc.main.id
  cidr_block        = cidrsubnet(var.vpc_cidr, 8, count.index)
  availability_zone = data.aws_availability_zones.available.names[count.index]

  tags = { Name = "${var.project}-private-${count.index}" }
}

# S3 Gateway型エンドポイントの関連付け先としてルートテーブルを明示的に作る
# （ローカルルートのみ。インターネットへのルートは追加しない）。
resource "aws_route_table" "private" {
  vpc_id = aws_vpc.main.id

  tags = { Name = "${var.project}-private" }
}

resource "aws_route_table_association" "private" {
  count = 2

  subnet_id      = aws_subnet.private[count.index].id
  route_table_id = aws_route_table.private.id
}

# --- セキュリティグループ ---
# ルールはすべてSG間参照（referenced_security_group_id）またはS3プレフィックス
# リストで書き、0.0.0.0/0を一切使わない（将来IGWが追加されても危険な設定が
# 温存されないようにするため。計画の実装時確認事項）。
# SG間の相互参照で循環になるため、ルールはインラインではなく個別リソースで定義する。

resource "aws_security_group" "vpc_endpoints" {
  name        = "${var.project}-vpc-endpoints"
  description = "Interface VPC endpoints: HTTPS from app/workersim tasks"
  vpc_id      = aws_vpc.main.id

  tags = { Name = "${var.project}-vpc-endpoints" }
}

resource "aws_security_group" "ecs_app" {
  name        = "${var.project}-ecs-app"
  description = "app task: egress to VPC endpoints, S3 and RDS only"
  vpc_id      = aws_vpc.main.id

  tags = { Name = "${var.project}-ecs-app" }
}

resource "aws_security_group" "ecs_workersim" {
  name        = "${var.project}-ecs-workersim"
  description = "workersim task: egress to VPC endpoints and S3 only (no RDS)"
  vpc_id      = aws_vpc.main.id

  tags = { Name = "${var.project}-ecs-workersim" }
}

resource "aws_security_group" "lambda" {
  name        = "${var.project}-lambda"
  description = "completion-handler: egress to RDS only (no SQS/SSM reachability needed)"
  vpc_id      = aws_vpc.main.id

  tags = { Name = "${var.project}-lambda" }
}

resource "aws_security_group" "rds" {
  name        = "${var.project}-rds"
  description = "RDS: PostgreSQL from app task and Lambda"
  vpc_id      = aws_vpc.main.id

  tags = { Name = "${var.project}-rds" }
}

# vpc_endpoints: app/workersimからの443受信
resource "aws_vpc_security_group_ingress_rule" "endpoints_from_app" {
  security_group_id            = aws_security_group.vpc_endpoints.id
  referenced_security_group_id = aws_security_group.ecs_app.id
  from_port                    = 443
  to_port                      = 443
  ip_protocol                  = "tcp"
}

resource "aws_vpc_security_group_ingress_rule" "endpoints_from_workersim" {
  security_group_id            = aws_security_group.vpc_endpoints.id
  referenced_security_group_id = aws_security_group.ecs_workersim.id
  from_port                    = 443
  to_port                      = 443
  ip_protocol                  = "tcp"
}

# ecs_app: エンドポイントへの443（SQS/SSM/ECR/Secrets/Logs）、S3への443
# （ECRイメージレイヤー取得）、RDSへの5432
resource "aws_vpc_security_group_egress_rule" "app_to_endpoints" {
  security_group_id            = aws_security_group.ecs_app.id
  referenced_security_group_id = aws_security_group.vpc_endpoints.id
  from_port                    = 443
  to_port                      = 443
  ip_protocol                  = "tcp"
}

resource "aws_vpc_security_group_egress_rule" "app_to_s3" {
  security_group_id = aws_security_group.ecs_app.id
  prefix_list_id    = aws_vpc_endpoint.s3.prefix_list_id
  from_port         = 443
  to_port           = 443
  ip_protocol       = "tcp"
}

resource "aws_vpc_security_group_egress_rule" "app_to_rds" {
  security_group_id            = aws_security_group.ecs_app.id
  referenced_security_group_id = aws_security_group.rds.id
  from_port                    = 5432
  to_port                      = 5432
  ip_protocol                  = "tcp"
}

# ecs_workersim: エンドポイントへの443とS3への443のみ（RDSアクセスなし）
resource "aws_vpc_security_group_egress_rule" "workersim_to_endpoints" {
  security_group_id            = aws_security_group.ecs_workersim.id
  referenced_security_group_id = aws_security_group.vpc_endpoints.id
  from_port                    = 443
  to_port                      = 443
  ip_protocol                  = "tcp"
}

resource "aws_vpc_security_group_egress_rule" "workersim_to_s3" {
  security_group_id = aws_security_group.ecs_workersim.id
  prefix_list_id    = aws_vpc_endpoint.s3.prefix_list_id
  from_port         = 443
  to_port           = 443
  ip_protocol       = "tcp"
}

# lambda: RDSへの5432のみ。SQSポーリングはLambdaサービス側のマネージド
# インフラが行い、関数自身はSQS APIを呼ばないためSQS到達性は不要。
# CloudWatch LogsへのログもLambdaサービス経由でVPC経路を通らない。
resource "aws_vpc_security_group_egress_rule" "lambda_to_rds" {
  security_group_id            = aws_security_group.lambda.id
  referenced_security_group_id = aws_security_group.rds.id
  from_port                    = 5432
  to_port                      = 5432
  ip_protocol                  = "tcp"
}

# rds: app/Lambdaからの5432受信
resource "aws_vpc_security_group_ingress_rule" "rds_from_app" {
  security_group_id            = aws_security_group.rds.id
  referenced_security_group_id = aws_security_group.ecs_app.id
  from_port                    = 5432
  to_port                      = 5432
  ip_protocol                  = "tcp"
}

resource "aws_vpc_security_group_ingress_rule" "rds_from_lambda" {
  security_group_id            = aws_security_group.rds.id
  referenced_security_group_id = aws_security_group.lambda.id
  from_port                    = 5432
  to_port                      = 5432
  ip_protocol                  = "tcp"
}

# --- VPCエンドポイント ---
# 計画に列挙された4つ（sqs, ssmmessages, ssm, ec2messages）に加え、NAT
# Gatewayなしのプライベートサブネットに置いたFargateタスクの起動要件として
# 以下を追加している（Fargateはタスク起動のたびにタスクENI経由で外部へ出る
# ため。Lambdaのイメージは関数作成時にLambdaサービス側が取得・キャッシュ
# するためVPC経路を使わない）:
#   - ecr.api / ecr.dkr / s3(Gateway): ECRからのイメージpull
#   - secretsmanager: タスク定義のsecretsブロック（PGPASSWORD）の解決
#   - logs: awslogsログドライバの送信先
locals {
  interface_endpoint_services = [
    "sqs",
    "ssmmessages",
    "ssm",
    "ec2messages",
    "ecr.api",
    "ecr.dkr",
    "secretsmanager",
    "logs",
  ]
}

resource "aws_vpc_endpoint" "interface" {
  for_each = toset(local.interface_endpoint_services)

  vpc_id              = aws_vpc.main.id
  service_name        = "com.amazonaws.${local.region}.${each.value}"
  vpc_endpoint_type   = "Interface"
  private_dns_enabled = true
  # 検証用途でAZ冗長は不要のため、時間課金を抑える目的で単一サブネットに
  # 置く（private DNSはVPC全体で解決されるため他AZのタスクからも使える）。
  subnet_ids         = [aws_subnet.private[0].id]
  security_group_ids = [aws_security_group.vpc_endpoints.id]

  tags = { Name = "${var.project}-${each.value}" }
}

resource "aws_vpc_endpoint" "s3" {
  vpc_id            = aws_vpc.main.id
  service_name      = "com.amazonaws.${local.region}.s3"
  vpc_endpoint_type = "Gateway"
  route_table_ids   = [aws_route_table.private.id]

  tags = { Name = "${var.project}-s3" }
}
