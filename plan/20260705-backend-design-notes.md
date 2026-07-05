# バックエンド実装で得た設計判断・教訓

`20260705-1st-plan.md`の初手計画を実装する過程で、計画時点では想定していなかった設計判断や、実装中に発覚した問題とその対処をまとめる。フロントエンド実装や今後の拡張時に参照する。

## 1. Go 1.24+ の `tool` ディレクティブで gqlgen を管理する

`go get github.com/99designs/gqlgen@v0.17.93` ではなく `go get -tool github.com/99designs/gqlgen@v0.17.93` を使い、`go.mod` に `tool` ディレクティブとして登録した。以後 `gqlgen init`/`generate` は `go tool gqlgen ...` で実行する。

- `go.mod` にツールの実行バージョンが固定され、`go run` 経由の暗黙的なバージョン解決に依存しなくなる。
- `go mod tidy` 後も `tool` として参照されるモジュールは `require` ブロックで `// indirect` のままになる（コード内で直接importされていない限り）。これは仕様であり不具合ではない。

## 2. パッケージ構成はフラットにする（`internal/` を使わない）

当初のプランは `internal/userctx`, `internal/jobstore` のように `internal/` 配下にまとめる構成だったが、`userctx/`, `jobstore/`, `pubsub/`, `redisclient/` をbackend直下にフラット配置する構成に変更した。

## 3. jobstore/pubsub は `userctx` に依存しない設計にする

最初の実装では `jobstore.Store.Create(ctx, name)` のように `ctx` から直接 `userctx.UserID(ctx)` を取り出していたが、レビューで「jobstoreがuserctxを知りすぎている」と指摘され、`Create(ctx, userID, name)` のように **userIDを引数で受け取る形** に変更した。

- UserIDの解決（`userctx.UserID(ctx)`）はresolver層の責務とし、jobstore/pubsubは認証の仕組みを一切知らない。
- 副作用として、テストで`userctx.WithUserID`のようなヘルパーが不要になった（jobstore/pubsubのテストは素の文字列userIDを直接渡すだけで済む）。

## 4. resolverはインターフェース経由でjobstore/Hubに依存する

`graph.Resolver` は `*jobstore.Store` / `*pubsub.Hub` という具体型ではなく、`graph`パッケージ内で定義した `JobStore` / `Hub` インターフェースに依存する（Goの「インターフェースは利用側で定義する」慣習）。これにより `schema.resolvers_test.go` でモックに差し替えて resolver 単体をテストできる。

フィールド名は `Resolver.Store` ではなく `Resolver.JobStore`（インターフェース名とフィールド名を一致させる）。

## 5. pubsub.Hub はペイロードを使わず「トリガー」としてのみ通知する

Redis Publishのメッセージ本文（Payload）は空文字列のままとし、Hubの通知チャネルも`chan struct{}`（データを運ばない）にした。理由:

- `Subscription.jobStatuses` は「全件スナップショット方式」で確定しているため、Payloadに1件分の更新データを乗せても、購読側はどのみち`jobstore.List`相当の全件取得が必要になる。
- jobstoreとpubsub間でペイロードのJSON形式（フィールド名・バージョニング）を共有する必要がなくなり、結合度が下がる。
- ジョブ数が多い・更新頻度が高い本番運用や、差分配信への仕様変更を行う場合は、この判断を再検討する必要がある（Payloadに変更内容を乗せてRedis往復を減らす設計に切り替える）。

## 6. Hub.Subscribeは戻り値にerrorを持たせる

`pubsub.Hub.Subscribe(userID)` は当初 `(ch, unsubscribe)` の2値を返していたが、内部で `ps.Receive(ctx)` によるSUBSCRIBE受理確認を追加した際、この確認が失敗しうるため `(ch, unsubscribe, error)` の3値に変更した。

`Subscribe`内で `ps.Receive()` を待ってから返すことで、「Subscribe直後にPublishすると購読前のため通知を取りこぼす」というレースを構造的に防いでいる（jobstoreの `TestSavePublishesUpdate` で最初に発見した問題と同種）。

## 7. Redis Pub/SubはSELECTしたDB番号をまたいでグローバル

**最も重要な教訓。** Redisの `PUBLISH`/`SUBSCRIBE` は、`SELECT`したDB番号に関係なくRedisサーバー全体でグローバルなチャンネル名前空間を共有する。

これに気づかず、`jobstore`と`pubsub`のテストコードで同じユーザーID文字列（`"user-a"`, `"user-b"`）を使っていたため、`go test ./... -race -count=N` のようにパッケージが並列実行されると、片方のパッケージの`Create`（Publish）をもう片方のテストが誤って受信し、`TestHubIsolatesNotificationsByUser`が数回に1回フレーキーに失敗する現象が発生した。

対処: テストのuserIDにパッケージ固有のプレフィックス（`jobstore-test-user-a`, `pubsub-test-user-a`など）を付け、DB番号（jobstore=15, pubsub=14, e2e=13）も分離した。`-count=10 -race`で安定を確認済み。

**今後の教訓**: 複数パッケージで実Redisを使う統合テストを書く際は、(1) DB番号を分離する、(2) それだけでは不十分でPub/Subのチャンネル名（＝ユーザーID等のキー）自体も一意にする、の両方が必要。

## 8. テストは実Redis（DB番号分離）を使う。miniredisは不採用

当初計画では `miniredis` を使う想定だったが、`jobstore`のPub/Subテスト（`TestSavePublishesUpdate`）で `i/o timeout` が発生し、原因調査よりも実Redis（docker-composeで起動、テスト専用DB番号を用いてFlushDBで独立性を保証）に切り替える方が確実と判断した。

- テストヘルパーは `REDIS_ADDR` 環境変数（未設定時 `localhost:6379`）に接続し、`t.Cleanup`でFlushDB。
- `t.Context()`（Go 1.24+）を使い、テスト終了時に自動キャンセルされるコンテキストを使う（`t.Cleanup`内だけは`t.Context()`が既にキャンセル済みのため`context.Background()`を使う必要がある）。

## 9. handler構築ロジックは `graph.NewHandler` として共有する

`cmd/main.go`（`package main`）はe2eテストから直接importできないため、gqlgenのhandler構築・transport登録（`transport.SSE`を先頭に追加）・`userctx.Middleware`でのラップを `graph.NewHandler(resolver) http.Handler` として`graph`パッケージに切り出した。`cmd/main.go`とe2eテストの両方がこれを呼ぶことで、実装の食い違いを防いでいる。

## 10. graceful shutdown

`signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)` + `http.Server.Shutdown(ctx)` の組み合わせ。`http.ListenAndServe`を直接呼ぶのではなく、`http.Server`インスタンスを明示的に保持する必要がある（雛形の`http.ListenAndServe(":"+port, nil)`はこれができないため書き換えが必要だった）。

## リポジトリ構成の変遷（参考）

- `server.go`（ルート直下）→ `backend/cmd/server.go` → `backend/cmd/main.go`（最終形）。理由は特になく、ユーザーの好みによる配置変更。
