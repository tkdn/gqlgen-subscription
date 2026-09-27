// Package httpserver はHTTPサーバーの停止手順をまとめる。
package httpserver

import (
	"context"
	"errors"
	"net/http"
	"time"
)

// Shutdown は新しい接続の受け付けをやめ、処理中のリクエストが終わるのを
// graceだけ待ってから、残った接続を強制的に閉じる。SSEの接続は必ず強制的に
// 閉じる側に回るので、gqlgenが購読の終了時に送る`event: complete`は
// クライアントに届かず、クライアントはこれを切断として扱って再接続する。
func Shutdown(srv *http.Server, grace time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), grace)
	defer cancel()

	if err := srv.Shutdown(ctx); !errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	return srv.Close()
}
