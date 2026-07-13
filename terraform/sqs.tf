# キュー名はローカル（kumo）と同一。app起動時のawsconfig.EnsureQueueは
# 属性なしのCreateQueueを同名キューへ発行するが、リクエストに属性を含まない
# 限り既存キューの属性と衝突せずURLが返るだけなので、ここで属性を設定しても
# 冪等性は保たれる。

resource "aws_sqs_queue" "job_requests" {
  name = "job-requests"
}

resource "aws_sqs_queue" "job_completions" {
  name = "job-completions"
  # Lambdaタイムアウト（30秒）の6倍。AWSの推奨目安に合わせる。
  visibility_timeout_seconds = 180
}
