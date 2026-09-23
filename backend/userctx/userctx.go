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

// loadTestUserIDHeader は負荷検証で複数ユーザーを模すためのヘッダーで、
// 本番の認証機構ではない。
const loadTestUserIDHeader = "X-User-Id"

// Middleware はユーザーIDをリクエストコンテキストに注入する。
// X-User-Idヘッダーがあればそれを使い、なければfixedUserIDにフォールバックする。
// 実際の認証の代わりとなるものであり、本来はここでトークンを検証し、
// 検証に失敗したリクエストを拒否すべき。
func Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		userID := r.Header.Get(loadTestUserIDHeader)
		if userID == "" {
			userID = fixedUserID
		}
		ctx := context.WithValue(r.Context(), userIDKey, userID)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// UserID はMiddlewareによってctxに格納されたユーザーIDを取り出す。
func UserID(ctx context.Context) string {
	id, _ := ctx.Value(userIDKey).(string)
	return id
}
