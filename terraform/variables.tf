variable "project" {
  description = "リソース名のプレフィックス兼ECSクラスター名"
  type        = string
  default     = "gqlgen-subscription"
}

variable "vpc_cidr" {
  description = "VPCのCIDRブロック"
  type        = string
  default     = "10.0.0.0/16"
}

variable "db_username" {
  description = "RDSのマスターユーザー名（ローカルのdocker-composeと同じ値）"
  type        = string
  default     = "app"
}

variable "db_name" {
  description = "RDSのデータベース名（ローカルのdocker-composeと同じ値）"
  type        = string
  default     = "app"
}

variable "rds_engine_version" {
  description = "RDS PostgreSQLのエンジンバージョン。メジャーのみ指定するとマイナーはRDS側で自動選択される"
  type        = string
  default     = "17"
}

variable "workersim_delay" {
  description = "workersimが依頼受信から完了送信までに待機する時間（ECS Exec検証を早めるためローカルのデフォルト10sより短縮）"
  type        = string
  default     = "3s"
}

variable "image_tag" {
  description = "app/workersim/Lambdaのイメージタグ。ko build --bareのデフォルトはlatest"
  type        = string
  default     = "latest"
}
