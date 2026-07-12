# gqlgen-subscription

GraphQL subscriptionによるリアルタイム通知の疎通検証プロジェクト。バックエンド(Go/gqlgen)がジョブの状態変化をRedis Pub/Sub経由で検知し、SSE(Server-Sent Events)でフロントエンド(Angular)へ配信する。

サービスBの完了通知をSQS経由で受け取る非同期パイプラインもローカル検証している（[`docs/architecture.md`](docs/architecture.md)参照）。[kumo](https://github.com/sivchari/kumo)（ローカルAWSエミュレーター）を使い、`createJob` → SQS依頼キュー → workersim（形式的なワーカー）→ SQS完了キュー → Consumer → Redis更新 → SSE配信、という流れを実際に動かせる。

## 起動手順

RedisとkumoをDockerで起動する。

```bash
docker compose up -d redis kumo
```

kumo（SQSエミュレーター）向けの環境変数を設定する。認証情報はダミー値でよい。

```bash
export AWS_ENDPOINT_URL=http://localhost:4566
export AWS_REGION=us-east-1
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test
```

バックエンドを起動する(`http://localhost:8080`)。GraphQL APIとSQS完了通知のConsumerが同一プロセスで動く。

```bash
cd backend
go run ./cmd/main.go
```

別ターミナルでサービスBの形式的なワーカー(workersim)を起動する。`WORKERSIM_DELAY`で待機時間を上書きできる（未指定時は10秒）。

```bash
cd backend
go run ./cmd/workersim
```

さらに別ターミナルでフロントエンドを起動する(`http://localhost:4200`)。

```bash
cd frontend
pnpm install
pnpm start
```

ブラウザで `http://localhost:4200` を開く。

## 動作確認

ブラウザを開いた状態で、別ターミナルからcurlでmutationを実行すると、画面に反映される。

```bash
# ジョブを作成する（数秒後、workersim経由でCOMPLETEDになる）
curl -s http://localhost:8080/query -H 'content-type: application/json' \
  --data '{"query":"mutation { createJob(name: \"job-1\") { id name status } }"}'

# job名がfail-プレフィックスの場合、workersimが意図的にFAILEDを返す
curl -s http://localhost:8080/query -H 'content-type: application/json' \
  --data '{"query":"mutation { createJob(name: \"fail-job-1\") { id name status } }"}'

# ジョブの現在の状態を確認する
curl -s http://localhost:8080/query -H 'content-type: application/json' \
  --data '{"query":"query { jobs { id name status } }"}'

# ジョブのステータスを手動で変更する（idはcreateJobのレスポンスから取得したもの）
curl -s http://localhost:8080/query -H 'content-type: application/json' \
  --data '{"query":"mutation { updateJobStatus(id: \"<job id>\", status: ANALYZING) { id name status } }"}'
```
