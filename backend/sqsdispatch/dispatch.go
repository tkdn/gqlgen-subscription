// Package sqsdispatch はジョブ作成をSQSの依頼キューへ投入する。
package sqsdispatch

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"

	"github.com/tkdn/gqlgen-subscription/backend/graph/model"
)

// Dispatcher はジョブ作成をSQSへ通知する。graph.JobDispatcherインターフェース
// （graph側で定義）を満たす。
type Dispatcher struct {
	client   *sqs.Client
	queueURL string
}

// New はDispatcherを生成する。
func New(client *sqs.Client, queueURL string) *Dispatcher {
	return &Dispatcher{client: client, queueURL: queueURL}
}

// requestMessage はサービスBへの依頼メッセージのペイロード。
// nameはworkersimの失敗注入判定（fail-プレフィックス）に使われる。
type requestMessage struct {
	UserID string `json:"user_id"`
	Name   string `json:"name"`
	JobID  string `json:"job_id"`
}

// Dispatch はjobの作成をSQS依頼キューへ通知する。
func (d *Dispatcher) Dispatch(ctx context.Context, userID string, job *model.Job) error {
	body, err := json.Marshal(requestMessage{UserID: userID, Name: job.Name, JobID: job.ID})
	if err != nil {
		return fmt.Errorf("sqsdispatch: marshal request: %w", err)
	}

	_, err = d.client.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl:    aws.String(d.queueURL),
		MessageBody: aws.String(string(body)),
	})
	if err != nil {
		return fmt.Errorf("sqsdispatch: send message: %w", err)
	}
	return nil
}
