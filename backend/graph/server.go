package graph

import (
	"net/http"
	"time"

	"github.com/99designs/gqlgen/graphql/handler"
	"github.com/99designs/gqlgen/graphql/handler/extension"
	"github.com/99designs/gqlgen/graphql/handler/lru"
	"github.com/99designs/gqlgen/graphql/handler/transport"
	"github.com/vektah/gqlparser/v2/ast"

	"github.com/tkdn/gqlgen-subscription/backend/userctx"
)

// NewHandler はresolverを組み込んだGraphQL用HTTPハンドラを構築する。
// cmd/main.goと e2e テストの両方から共有して使う。
func NewHandler(resolver *Resolver) http.Handler {
	srv := handler.New(NewExecutableSchema(Config{Resolvers: resolver}))

	// SSEはWebSocketなしでsubscriptionを配信するため、他のtransportより先に追加する。
	srv.AddTransport(transport.SSE{KeepAlivePingInterval: 10 * time.Second})
	srv.AddTransport(transport.Options{})
	srv.AddTransport(transport.GET{})
	srv.AddTransport(transport.POST{})

	srv.SetQueryCache(lru.New[*ast.QueryDocument](1000))

	srv.Use(extension.Introspection{})
	srv.Use(extension.AutomaticPersistedQuery{
		Cache: lru.New[string](100),
	})

	return userctx.Middleware(srv)
}
