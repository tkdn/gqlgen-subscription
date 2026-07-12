package awsconfig_test

import (
	"context"
	"fmt"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/sqs"

	"github.com/tkdn/gqlgen-subscription/backend/awsconfig"
)

// newTestClient はSQSクライアントを返す。到達不能ならスキップする。
func newTestClient(t *testing.T) (*sqs.Client, context.Context) {
	t.Helper()

	ctx := t.Context()
	cfg, err := awsconfig.New(ctx)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	client := awsconfig.SQSClient(cfg)

	pingCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	if _, err := client.ListQueues(pingCtx, &sqs.ListQueuesInput{}); err != nil {
		t.Skipf("sqs endpoint not available: %v", err)
	}

	return client, ctx
}

func testQueueName(t *testing.T) string {
	t.Helper()
	return fmt.Sprintf("awsconfig-test-%s-%d", t.Name(), rand.Int64())
}

func TestEnsureQueueIsIdempotent(t *testing.T) {
	client, ctx := newTestClient(t)
	name := testQueueName(t)

	url1, err := awsconfig.EnsureQueue(ctx, client, name)
	if err != nil {
		t.Fatalf("EnsureQueue() #1 error = %v", err)
	}
	if url1 == "" {
		t.Fatal("EnsureQueue() #1 returned empty URL")
	}

	url2, err := awsconfig.EnsureQueue(ctx, client, name)
	if err != nil {
		t.Fatalf("EnsureQueue() #2 error = %v", err)
	}
	if url1 != url2 {
		t.Fatalf("EnsureQueue() called twice returned different URLs: %q != %q", url1, url2)
	}
}
