# SSE Fan-out 負荷検証の実測結果

[docs/20260919-sse-fanout-load-analysis.md](./20260919-sse-fanout-load-analysis.md)で検討した2案の効果を、実際に検証環境を構築して定量的に確認した記録。検証用のコードは`experiment/sse-fanout-loadtest`ブランチにあり、mainにはマージしない使い捨てである。

## 検証環境

- 複数ユーザー: `backend/userctx/userctx.go`を`X-User-Id`ヘッダー対応に拡張（本番の認証機構ではない）
- 複数タブ: `backend/cmd/loadtest`が複数のSSE接続を並列に張る
- 複数プロセス: 同一マシンで`PORT`を変えて`go run ./cmd`を複数回起動
- 測定: `pg_stat_statements`（一次証拠）と`CountingJobStore`経由の`/debug/loadtest-stats`（プロセスごとの内訳）を併用

## 1. ベースライン計測（改善前）

条件: タブ5枚、単一プロセス、updateJobStatus 3回、間隔2秒。

コマンド:

```
cd backend
go run ./cmd/loadtest -tabs=5 -processes=http://localhost:8080 -updates=3 -interval=2s
```

標準出力（該当部分）:

```
=== 結果 ===
タブ数: 5, 更新回数: 3, ユーザー: loadtest-user-1790156077645583207

--- 各タブの受信回数 ---
タブ0 (接続先 http://localhost:8080): 4回受信
タブ1 (接続先 http://localhost:8080): 4回受信
タブ2 (接続先 http://localhost:8080): 4回受信
タブ3 (接続先 http://localhost:8080): 4回受信
タブ4 (接続先 http://localhost:8080): 4回受信

--- 各プロセスのList呼び出し回数（累積） ---
http://localhost:8080: map[loadtest-user-1790156077645583207:20]
```

`pg_stat_statements`（`pg_stat_statements_reset()`実行直後にloadtestを実行）:

```
                              query                               | calls
------------------------------------------------------------------+-------
 SELECT id, name, status FROM jobs WHERE user_id = $1 ORDER BY id |    20
(1 row)
```

- 各タブの受信回数: タブ0〜4がそれぞれ4回受信（初回スナップショット1回 + updateJobStatus 3回分）
- `/debug/loadtest-stats`のList呼び出し回数: `loadtest-user-1790156077645583207`: 20回
- `pg_stat_statements`の`SELECT ... FROM jobs WHERE user_id`のcalls: 20回

**観察:** アプリ側カウンタ（20回）とDB側の`calls`（20回）が完全に一致した。内訳はタブ数5 × (初期スナップショット1回 + updateJobStatus 3回) = 5 × 4 = 20回であり、仮説どおり「1回の`updateJobStatus`ごとにタブの本数分だけ`JobStore.List`が呼ばれる」構造になっている。初期接続時のスナップショット取得も同じ`List`を経由するため、更新3回だけでなく接続時点でも1タブにつき1回加算される点に注意。タブ数を増やすほど、更新1回あたりのDBアクセス数がタブ数に比例して増える（N倍化）ことが実測で確認できた。
