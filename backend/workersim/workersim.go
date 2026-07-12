// Package workersim はサービスBの形式的な実装。SQSの依頼キューを
// long pollingし、一定時間待ってから完了キューへ結果を送る。
package workersim

import (
	"context"
	"encoding/json"
	"log"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

// failNamePrefix で始まるジョブ名は、検証時に意図的に失敗パスを再現する
// ためのマーカーとして扱う。
const failNamePrefix = "fail-"

// requestMessage はsqsdispatchが送信する依頼メッセージのペイロード。
type requestMessage struct {
	UserID string `json:"user_id"`
	Name   string `json:"name"`
	JobID  string `json:"job_id"`
}

// completionMessage はConsumerへ送る完了メッセージのペイロード。
// nameは運ばない（jobstore.Storeの実体キーがjob_idベースのため不要）。
type completionMessage struct {
	UserID string `json:"user_id"`
	JobID  string `json:"job_id"`
	Status string `json:"status"`
}

// resultStatus はjob名から完了ステータスを決定する。failNamePrefixで
// 始まる名前は検証用に意図的にFAILEDを返す。
func resultStatus(name string) string {
	if strings.HasPrefix(name, failNamePrefix) {
		return "FAILED"
	}
	return "COMPLETED"
}

// Run はrequestsURLをlong pollingし、メッセージ受信ごとにdelay待機した後、
// completionsURLへ完了メッセージを送信する。ctxがキャンセルされるとポーリング
// ループを終了する。
func Run(ctx context.Context, client *sqs.Client, requestsURL, completionsURL string, delay time.Duration) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		out, err := client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
			QueueUrl:            aws.String(requestsURL),
			MaxNumberOfMessages: 1,
			WaitTimeSeconds:     20,
		})
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			log.Printf("workersim: receive error: %v", err)
			continue
		}

		for _, msg := range out.Messages {
			var req requestMessage
			if err := json.Unmarshal([]byte(aws.ToString(msg.Body)), &req); err != nil {
				log.Printf("workersim: bad message, skipping: %v", err)
				continue
			}
			log.Printf("workersim: received job_id=%s name=%q user_id=%s, waiting %s", req.JobID, req.Name, req.UserID, delay)

			select {
			case <-time.After(delay):
			case <-ctx.Done():
				// シャットダウン中は完了メッセージを送らず、依頼メッセージも
				// 削除しない（可視性タイムアウト後に再配信される）。
				return nil
			}

			status := resultStatus(req.Name)
			body, err := json.Marshal(completionMessage{UserID: req.UserID, JobID: req.JobID, Status: status})
			if err != nil {
				log.Printf("workersim: marshal completion: %v", err)
				continue
			}
			if _, err := client.SendMessage(ctx, &sqs.SendMessageInput{
				QueueUrl:    aws.String(completionsURL),
				MessageBody: aws.String(string(body)),
			}); err != nil {
				log.Printf("workersim: send completion error: %v", err)
				continue
			}
			log.Printf("workersim: sent completion job_id=%s status=%s", req.JobID, status)

			if _, err := client.DeleteMessage(ctx, &sqs.DeleteMessageInput{
				QueueUrl:      aws.String(requestsURL),
				ReceiptHandle: msg.ReceiptHandle,
			}); err != nil {
				log.Printf("workersim: delete request message error: %v", err)
			}
		}
	}
}
