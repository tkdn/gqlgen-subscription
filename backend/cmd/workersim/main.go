package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/tkdn/gqlgen-subscription/backend/awsconfig"
	"github.com/tkdn/gqlgen-subscription/backend/workersim"
)

const (
	requestsQueueName    = "job-requests"
	completionsQueueName = "job-completions"
	defaultDelay         = 10 * time.Second
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cfg, err := awsconfig.New(ctx)
	if err != nil {
		log.Fatalf("aws config: %v", err)
	}
	client := awsconfig.SQSClient(cfg)

	requestsURL, err := awsconfig.EnsureQueue(ctx, client, requestsQueueName)
	if err != nil {
		log.Fatalf("ensure queue %q: %v", requestsQueueName, err)
	}
	completionsURL, err := awsconfig.EnsureQueue(ctx, client, completionsQueueName)
	if err != nil {
		log.Fatalf("ensure queue %q: %v", completionsQueueName, err)
	}

	delay := defaultDelay
	if v := os.Getenv("WORKERSIM_DELAY"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			log.Fatalf("parse WORKERSIM_DELAY %q: %v", v, err)
		}
		delay = d
	}

	log.Printf("workersim: polling %s, delay=%s", requestsQueueName, delay)
	if err := workersim.Run(ctx, client, requestsURL, completionsURL, delay); err != nil {
		log.Fatalf("workersim: %v", err)
	}
	log.Println("workersim: shutting down")
}
