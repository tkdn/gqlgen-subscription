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
	"github.com/tkdn/gqlgen-subscription/backend/jobstore"
	"github.com/tkdn/gqlgen-subscription/backend/pubsub"
	"github.com/tkdn/gqlgen-subscription/backend/redisclient"
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

	rdb := redisclient.New()
	defer rdb.Close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

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

	jobStore := jobstore.New(rdb)

	resolver := &graph.Resolver{
		JobStore:   jobStore,
		Hub:        pubsub.New(rdb),
		Dispatcher: sqsdispatch.New(sqsClient, requestsURL),
	}

	mux := http.NewServeMux()
	mux.Handle("/", playground.Handler("GraphQL playground", "/query"))
	mux.Handle("/query", graph.NewHandler(resolver))

	httpServer := &http.Server{
		Addr:    ":" + port,
		Handler: mux,
	}

	go func() {
		if err := consumer.Run(ctx, sqsClient, jobStore, completionsURL); err != nil {
			log.Printf("consumer: %v", err)
		}
	}()

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
