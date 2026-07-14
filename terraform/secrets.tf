# RDSのマスターパスワード。ECSタスク定義はsecretsブロックで実行時に解決し、
# Lambdaは環境変数へ静的注入する（Lambdaのenvironmentブロックには実行時
# 解決の仕組みがないため。plan決定13）。

resource "random_password" "rds" {
  length = 32
  # RDSのマスターパスワードは '/', '@', '"', 半角スペースを許容しないため
  # 記号自体を使わない（32文字の英数字で強度は十分）。
  special = false
}

resource "aws_secretsmanager_secret" "rds_password" {
  name = "${var.project}/rds-password"
  # terraform destroy後に同名で再作成できるよう、削除の猶予期間を置かない。
  recovery_window_in_days = 0
}

resource "aws_secretsmanager_secret_version" "rds_password" {
  secret_id     = aws_secretsmanager_secret.rds_password.id
  secret_string = random_password.rds.result
}
