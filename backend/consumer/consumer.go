// Package consumer はSQSの完了キューをlong pollingし、受け取った完了通知を
// jobstore.Storeへ反映する。
package consumer

import (
	"context"
	"encoding/json"
	"log"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"

	"github.com/tkdn/gqlgen-subscription/backend/graph/model"
	"github.com/tkdn/gqlgen-subscription/backend/jobstore"
)

// completionMessage はworkersimが送信する完了メッセージのペイロード。
// nameは運ばれない（jobstore.Storeの実体キーがjob_idベースのため不要）。
type completionMessage struct {
	UserID string `json:"user_id"`
	JobID  string `json:"job_id"`
	Status string `json:"status"`
}

// Run はcompletionsURLをlong pollingし、メッセージごとにjobstore.Store.UpdateStatus
// を呼んでジョブの状態を更新する。ctxがキャンセルされるとポーリングループを終了する。
func Run(ctx context.Context, client *sqs.Client, store *jobstore.Store, completionsURL string) error {
	for {
		select {
		case <-ctx.Done():
			log.Println("consumer: shutting down")
			return nil
		default:
		}

		out, err := client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
			QueueUrl:            aws.String(completionsURL),
			MaxNumberOfMessages: 1,
			WaitTimeSeconds:     20,
		})
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			log.Printf("consumer: receive error: %v", err)
			continue
		}

		for _, msg := range out.Messages {
			var comp completionMessage
			if err := json.Unmarshal([]byte(aws.ToString(msg.Body)), &comp); err != nil {
				log.Printf("consumer: bad message, deleting: %v", err)
				deleteMessage(ctx, client, completionsURL, msg.ReceiptHandle)
				continue
			}

			status := model.JobState(comp.Status)
			if !status.IsValid() {
				log.Printf("consumer: invalid status %q for job_id=%s, deleting", comp.Status, comp.JobID)
				deleteMessage(ctx, client, completionsURL, msg.ReceiptHandle)
				continue
			}

			if _, err := store.UpdateStatus(ctx, comp.UserID, comp.JobID, status); err != nil {
				// 冪等性は今回未実装のため、削除せず可視性タイムアウト後の
				// 再配信に任せる。
				log.Printf("consumer: update status failed for job_id=%s, will redeliver: %v", comp.JobID, err)
				continue
			}
			log.Printf("consumer: updated job_id=%s status=%s", comp.JobID, status)

			deleteMessage(ctx, client, completionsURL, msg.ReceiptHandle)
		}
	}
}

func deleteMessage(ctx context.Context, client *sqs.Client, queueURL string, receiptHandle *string) {
	if _, err := client.DeleteMessage(ctx, &sqs.DeleteMessageInput{
		QueueUrl:      aws.String(queueURL),
		ReceiptHandle: receiptHandle,
	}); err != nil {
		log.Printf("consumer: delete message error: %v", err)
	}
}
