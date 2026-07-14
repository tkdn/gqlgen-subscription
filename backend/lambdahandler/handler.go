// Package lambdahandler はSQSの完了キューをevent source mappingで受け取る
// Lambdaハンドラ。consumer.Runと同じ判断基準で完了通知をストアへ反映する:
// 不正なメッセージ（JSON不正・不正status）はackして捨て（ログのみ）、
// UpdateStatusのエラーのみBatchItemFailuresで再配信を要求する。
// 冪等性（終端状態からの遷移拒否）はJobStatusUpdater実装側
// （pgjobstore.Store）の責務。
package lambdahandler

import (
	"context"
	"encoding/json"
	"log"

	"github.com/aws/aws-lambda-go/events"

	"github.com/tkdn/gqlgen-subscription/backend/consumer"
	"github.com/tkdn/gqlgen-subscription/backend/graph/model"
)

// Handler はSQSイベントを処理し、完了通知をStoreへ反映する。
type Handler struct {
	Store consumer.JobStatusUpdater
}

// New はHandlerを生成する。
func New(store consumer.JobStatusUpdater) *Handler {
	return &Handler{Store: store}
}

// HandleRequest はSQSイベント内の各メッセージを処理する。処理に失敗した
// メッセージのみBatchItemFailuresに積んで返し、SQS側の部分バッチ失敗
// レポート（ReportBatchItemFailures）で再配信させる。エラーを返すと
// バッチ全体が再配信されてしまうため、常にnilエラーで返す。
func (h *Handler) HandleRequest(ctx context.Context, sqsEvent events.SQSEvent) (events.SQSEventResponse, error) {
	var resp events.SQSEventResponse
	for _, record := range sqsEvent.Records {
		var comp consumer.CompletionMessage
		if err := json.Unmarshal([]byte(record.Body), &comp); err != nil {
			log.Printf("lambdahandler: bad message, acking: %v", err)
			continue
		}

		status := model.JobState(comp.Status)
		if !status.IsValid() {
			log.Printf("lambdahandler: invalid status %q for job_id=%s, acking", comp.Status, comp.JobID)
			continue
		}

		if _, err := h.Store.UpdateStatus(ctx, comp.UserID, comp.JobID, status); err != nil {
			log.Printf("lambdahandler: update status failed for job_id=%s, will redeliver: %v", comp.JobID, err)
			resp.BatchItemFailures = append(resp.BatchItemFailures, events.SQSBatchItemFailure{
				ItemIdentifier: record.MessageId,
			})
			continue
		}
		log.Printf("lambdahandler: updated job_id=%s status=%s", comp.JobID, status)
	}
	return resp, nil
}
