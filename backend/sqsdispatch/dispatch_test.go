package sqsdispatch_test

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"

	"github.com/tkdn/gqlgen-subscription/backend/awsconfig"
	"github.com/tkdn/gqlgen-subscription/backend/graph/model"
	"github.com/tkdn/gqlgen-subscription/backend/sqsdispatch"
)

// newTestQueue はテスト専用の一意な名前のSQSキューを作成し、そのURLを返す。
// 到達不能ならスキップする。テストごとに一意な名前を使うことで、他のテスト
// 実行が残したメッセージの混入を避ける。
func newTestQueue(t *testing.T) (client *sqs.Client, queueURL string, ctx context.Context) {
	t.Helper()

	ctx = t.Context()
	cfg, err := awsconfig.New(ctx)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	client = awsconfig.SQSClient(cfg)

	pingCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	if _, err := client.ListQueues(pingCtx, &sqs.ListQueuesInput{}); err != nil {
		t.Skipf("sqs endpoint not available: %v", err)
	}

	name := fmt.Sprintf("sqsdispatch-test-%s-%d", t.Name(), rand.Int64())
	queueURL, err = awsconfig.EnsureQueue(ctx, client, name)
	if err != nil {
		t.Fatalf("EnsureQueue() error = %v", err)
	}

	return client, queueURL, ctx
}

func TestDispatchSendsRequestMessage(t *testing.T) {
	client, queueURL, ctx := newTestQueue(t)

	d := sqsdispatch.New(client, queueURL)
	job := &model.Job{ID: "job-id-1", Name: "job-1", Status: model.JobStatePending}

	if err := d.Dispatch(ctx, "user-1", job); err != nil {
		t.Fatalf("Dispatch() error = %v", err)
	}

	out, err := client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:            aws.String(queueURL),
		MaxNumberOfMessages: 1,
		WaitTimeSeconds:     5,
	})
	if err != nil {
		t.Fatalf("ReceiveMessage() error = %v", err)
	}
	if len(out.Messages) != 1 {
		t.Fatalf("ReceiveMessage() got %d messages, want 1", len(out.Messages))
	}

	var body struct {
		UserID string `json:"user_id"`
		Name   string `json:"name"`
		JobID  string `json:"job_id"`
	}
	if err := json.Unmarshal([]byte(aws.ToString(out.Messages[0].Body)), &body); err != nil {
		t.Fatalf("unmarshal message body: %v", err)
	}
	if body.UserID != "user-1" || body.Name != "job-1" || body.JobID != "job-id-1" {
		t.Fatalf("message body = %+v, want {user-1, job-1, job-id-1}", body)
	}
}
