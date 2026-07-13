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

	"github.com/tkdn/gqlgen-subscription/backend/awsconfig"
	"github.com/tkdn/gqlgen-subscription/backend/consumer"
	"github.com/tkdn/gqlgen-subscription/backend/graph"
	"github.com/tkdn/gqlgen-subscription/backend/pgclient"
	"github.com/tkdn/gqlgen-subscription/backend/pgjobstore"
	"github.com/tkdn/gqlgen-subscription/backend/pgpubsub"
	"github.com/tkdn/gqlgen-subscription/backend/sqsdispatch"
)

const (
	defaultPort          = "8080"
	requestsQueueName    = "job-requests"
	completionsQueueName = "job-completions"
)

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = defaultPort
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := pgclient.New(ctx)
	if err != nil {
		log.Fatalf("pg pool: %v", err)
	}
	defer pool.Close()
	if err := pgclient.EnsureSchema(ctx, pool); err != nil {
		log.Fatalf("ensure schema: %v", err)
	}

	awsCfg, err := awsconfig.New(ctx)
	if err != nil {
		log.Fatalf("aws config: %v", err)
	}
	sqsClient := awsconfig.SQSClient(awsCfg)

	requestsURL, err := awsconfig.EnsureQueue(ctx, sqsClient, requestsQueueName)
	if err != nil {
		log.Fatalf("ensure queue %q: %v", requestsQueueName, err)
	}
	completionsURL, err := awsconfig.EnsureQueue(ctx, sqsClient, completionsQueueName)
	if err != nil {
		log.Fatalf("ensure queue %q: %v", completionsQueueName, err)
	}

	jobStore := pgjobstore.New(pool, pgjobstore.UpdatesChannel)

	hub, err := pgpubsub.New(ctx, pgclient.Connect, pgjobstore.UpdatesChannel)
	if err != nil {
		log.Fatalf("pg pubsub: %v", err)
	}
	defer hub.Close()

	resolver := &graph.Resolver{
		JobStore:   jobStore,
		Hub:        hub,
		Dispatcher: sqsdispatch.New(sqsClient, requestsURL),
	}

	mux := http.NewServeMux()
	mux.Handle("/", playground.Handler("GraphQL playground", "/query"))
	mux.Handle("/query", graph.NewHandler(resolver))

	httpServer := &http.Server{
		Addr:    ":" + port,
		Handler: mux,
	}

	// ECS上では完了メッセージの処理をLambdaに一本化するため、consumerを
	// 無効化する（Lambda発火の検証信号を明確にする）。ローカルでは未設定の
	// ままconsumerが動く。
	if os.Getenv("SKIP_COMPLETION_CONSUMER") == "true" {
		log.Println("completion consumer disabled by SKIP_COMPLETION_CONSUMER")
	} else {
		go func() {
			if err := consumer.Run(ctx, sqsClient, jobStore, completionsURL); err != nil {
				log.Printf("consumer: %v", err)
			}
		}()
	}

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
