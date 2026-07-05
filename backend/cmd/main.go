package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/99designs/gqlgen/graphql/playground"

	"github.com/tkdn/gqlgen-subscription/backend/graph"
	"github.com/tkdn/gqlgen-subscription/backend/jobstore"
	"github.com/tkdn/gqlgen-subscription/backend/pubsub"
	"github.com/tkdn/gqlgen-subscription/backend/redisclient"
)

const defaultPort = "8080"

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = defaultPort
	}

	rdb := redisclient.New()
	defer rdb.Close()

	resolver := &graph.Resolver{
		JobStore: jobstore.New(rdb),
		Hub:      pubsub.New(rdb),
	}

	mux := http.NewServeMux()
	mux.Handle("/", playground.Handler("GraphQL playground", "/query"))
	mux.Handle("/query", graph.NewHandler(resolver))

	httpServer := &http.Server{
		Addr:    ":" + port,
		Handler: mux,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go func() {
		log.Printf("connect to http://localhost:%s/ for GraphQL playground", port)
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("listen: %v", err)
		}
	}()

	<-ctx.Done()
	stop()
	log.Println("shutting down...")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		log.Printf("server shutdown: %v", err)
	}
}
