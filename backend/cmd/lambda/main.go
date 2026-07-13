// completion-handler Lambdaのエントリポイント。SQSの完了キューを
// event source mappingで受け取り、pgjobstore.Store経由でRDSを更新する
// （UPDATEとpg_notifyは同一トランザクション）。
package main

import (
	"context"
	"log"

	"github.com/aws/aws-lambda-go/lambda"

	"github.com/tkdn/gqlgen-subscription/backend/lambdahandler"
	"github.com/tkdn/gqlgen-subscription/backend/pgclient"
	"github.com/tkdn/gqlgen-subscription/backend/pgjobstore"
)

func main() {
	// コールドスタート時に一度だけプールを構築し、ウォームコンテナ間で
	// 使い回す（Closeしない）。プールサイズはPGPOOL_MAX_CONNSで制限する
	// 前提（Lambdaの同時実行数×プールサイズがRDSの接続数上限を圧迫する
	// ため）。
	ctx := context.Background()
	pool, err := pgclient.New(ctx)
	if err != nil {
		log.Fatalf("pg pool: %v", err)
	}
	if err := pgclient.EnsureSchema(ctx, pool); err != nil {
		log.Fatalf("ensure schema: %v", err)
	}

	h := lambdahandler.New(pgjobstore.New(pool, pgjobstore.UpdatesChannel))
	lambda.Start(h.HandleRequest)
}
