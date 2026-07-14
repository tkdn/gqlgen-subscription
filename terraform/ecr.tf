# koのプッシュ先。イメージが残っていてもdestroyできるようforce_deleteを
# 有効にする。ECRリポジトリはECS/Lambdaより先に存在し、その間にko pushが
# 必要（Terraformのグラフでは表現できない依存。README.md参照）。

resource "aws_ecr_repository" "app" {
  name         = "${var.project}/app"
  force_delete = true
}

resource "aws_ecr_repository" "workersim" {
  name         = "${var.project}/workersim"
  force_delete = true
}

resource "aws_ecr_repository" "lambda" {
  name         = "${var.project}/lambda"
  force_delete = true
}
