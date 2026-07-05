# gqlgen-subscription

GraphQL subscriptionによるリアルタイム通知の疎通検証プロジェクト。バックエンド(Go/gqlgen)がジョブの状態変化をRedis Pub/Sub経由で検知し、SSE(Server-Sent Events)でフロントエンド(Angular)へ配信する。

## 起動手順

Redisを起動する。

```bash
docker compose up -d redis
```

バックエンドを起動する(`http://localhost:8080`)。

```bash
cd backend
go run cmd/main.go
```

別ターミナルでフロントエンドを起動する(`http://localhost:4200`)。

```bash
cd frontend
pnpm install
pnpm start
```

ブラウザで `http://localhost:4200` を開く。

## 動作確認

ブラウザを開いた状態で、別ターミナルからcurlでmutationを実行すると、画面に即座に反映される。

```bash
# ジョブを作成する
curl -s http://localhost:8080/query -H 'content-type: application/json' \
  --data '{"query":"mutation { createJob(name: \"job-1\") { name status } }"}'

# ジョブのステータスを変更する
curl -s http://localhost:8080/query -H 'content-type: application/json' \
  --data '{"query":"mutation { updateJobStatus(name: \"job-1\", status: ANALYZING) { name status } }"}'
```
