package userctx

import (
	"context"
	"net/http"
)

type contextKey struct{}

var userIDKey contextKey

// fixedUserID は実際のユーザー識別の代わりとなる固定値。本来はここで
// リクエスト（例えばAuthorizationヘッダー）からトークンを検証し、
// そこからユーザーIDを導出すべき。
const fixedUserID = "user-1"

// Middleware は固定のユーザーIDをリクエストコンテキストに注入する。
// 実際の認証の代わりとなるものであり、本来はここでトークンを検証し、
// 検証に失敗したリクエストを拒否すべき。
func Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := context.WithValue(r.Context(), userIDKey, fixedUserID)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// UserID はMiddlewareによってctxに格納されたユーザーIDを取り出す。
func UserID(ctx context.Context) string {
	id, _ := ctx.Value(userIDKey).(string)
	return id
}
