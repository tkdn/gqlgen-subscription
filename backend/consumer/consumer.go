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
)

// JobStatusUpdater はConsumerが完了通知の反映に必要とするストア操作の
// インターフェース。jobstore.Store（Redis）とpgjobstore.Store（PostgreSQL）
// の両方が満たす。
type JobStatusUpdater interface {
	UpdateStatus(ctx context.Context, userID, jobID string, status model.JobState) (*model.Job, error)
}

// CompletionMessage はworkersimが送信する完了メッセージのペイロード。
// nameは運ばれない（jobstore.Storeの実体キーがjob_idベースのため不要）。
// consumer.Runに加えてLambdaハンドラ(lambdahandler)も同じペイロードを
// 解釈するためexportしている。
type CompletionMessage struct {
	UserID string `json:"user_id"`
	JobID  string `json:"job_id"`
	Status string `json:"status"`
}

// Run はcompletionsURLをlong pollingし、メッセージごとにjobstore.Store.UpdateStatus
// を呼んでジョブの状態を更新する。ctxがキャンセルされるとポーリングループを終了する。
func Run(ctx context.Context, client *sqs.Client, store JobStatusUpdater, completionsURL string) error {
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
			var comp CompletionMessage
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
				// エラー時はメッセージを削除せず、可視性タイムアウト後の再配信に
				// 任せる。正常系での重複メッセージ耐性（終端状態からの遷移拒否）は
				// JobStatusUpdater実装側の責務（pgjobstore.Storeは保証、
				// jobstore.Storeは保証しない。graph.JobStoreのコメント参照）。
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
