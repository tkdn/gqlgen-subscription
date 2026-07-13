# リージョンはap-northeast-1固定（plan/20260713-lambda-real-aws.md）。
# VPCエンドポイントのservice_nameやawslogs-regionでも参照するためlocalsに置く。
locals {
  region = "ap-northeast-1"
}

provider "aws" {
  region = local.region
}
