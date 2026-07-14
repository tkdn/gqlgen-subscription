package lambdahandler_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/aws/aws-lambda-go/events"

	"github.com/tkdn/gqlgen-subscription/backend/consumer"
	"github.com/tkdn/gqlgen-subscription/backend/graph/model"
	"github.com/tkdn/gqlgen-subscription/backend/lambdahandler"
)

// updateCall はfakeUpdaterが記録するUpdateStatusの呼び出し内容。
type updateCall struct {
	UserID string
	JobID  string
	Status model.JobState
}

// fakeUpdater はconsumer.JobStatusUpdaterのインメモリ実装。failJobIDに
// 一致するジョブへの更新は決定的に失敗させる。
type fakeUpdater struct {
	calls     []updateCall
	failJobID string
}

func (f *fakeUpdater) UpdateStatus(_ context.Context, userID, jobID string, status model.JobState) (*model.Job, error) {
	f.calls = append(f.calls, updateCall{UserID: userID, JobID: jobID, Status: status})
	if jobID == f.failJobID {
		return nil, errors.New("injected failure")
	}
	return &model.Job{ID: jobID, Status: status}, nil
}

// sqsRecord は完了メッセージをSQSEventのレコードとして構築するヘルパー。
func sqsRecord(t *testing.T, messageID string, msg consumer.CompletionMessage) events.SQSMessage {
	t.Helper()

	body, err := json.Marshal(msg)
	if err != nil {
		t.Fatalf("marshal completion message: %v", err)
	}
	return events.SQSMessage{MessageId: messageID, Body: string(body)}
}

func TestHandleRequestUpdatesJobStatus(t *testing.T) {
	store := &fakeUpdater{}
	h := lambdahandler.New(store)

	resp, err := h.HandleRequest(t.Context(), events.SQSEvent{Records: []events.SQSMessage{
		sqsRecord(t, "msg-1", consumer.CompletionMessage{
			UserID: "user-a", JobID: "job-1", Status: string(model.JobStateCompleted),
		}),
	}})
	if err != nil {
		t.Fatalf("HandleRequest() error = %v", err)
	}
	if len(resp.BatchItemFailures) != 0 {
		t.Errorf("BatchItemFailures = %+v, want empty", resp.BatchItemFailures)
	}
	want := []updateCall{{UserID: "user-a", JobID: "job-1", Status: model.JobStateCompleted}}
	if len(store.calls) != 1 || store.calls[0] != want[0] {
		t.Errorf("UpdateStatus calls = %+v, want %+v", store.calls, want)
	}
}

func TestHandleRequestAcksBadJSON(t *testing.T) {
	store := &fakeUpdater{}
	h := lambdahandler.New(store)

	resp, err := h.HandleRequest(t.Context(), events.SQSEvent{Records: []events.SQSMessage{
		{MessageId: "msg-1", Body: "{not json"},
	}})
	if err != nil {
		t.Fatalf("HandleRequest() error = %v", err)
	}
	if len(resp.BatchItemFailures) != 0 {
		t.Errorf("BatchItemFailures = %+v, want empty (bad JSON must be acked)", resp.BatchItemFailures)
	}
	if len(store.calls) != 0 {
		t.Errorf("UpdateStatus calls = %+v, want none", store.calls)
	}
}

func TestHandleRequestAcksInvalidStatus(t *testing.T) {
	store := &fakeUpdater{}
	h := lambdahandler.New(store)

	resp, err := h.HandleRequest(t.Context(), events.SQSEvent{Records: []events.SQSMessage{
		sqsRecord(t, "msg-1", consumer.CompletionMessage{
			UserID: "user-a", JobID: "job-1", Status: "NOT_A_REAL_STATUS",
		}),
	}})
	if err != nil {
		t.Fatalf("HandleRequest() error = %v", err)
	}
	if len(resp.BatchItemFailures) != 0 {
		t.Errorf("BatchItemFailures = %+v, want empty (invalid status must be acked)", resp.BatchItemFailures)
	}
	if len(store.calls) != 0 {
		t.Errorf("UpdateStatus calls = %+v, want none", store.calls)
	}
}

func TestHandleRequestReportsFailedUpdateOnly(t *testing.T) {
	store := &fakeUpdater{failJobID: "job-fail"}
	h := lambdahandler.New(store)

	resp, err := h.HandleRequest(t.Context(), events.SQSEvent{Records: []events.SQSMessage{
		sqsRecord(t, "msg-ok", consumer.CompletionMessage{
			UserID: "user-a", JobID: "job-ok", Status: string(model.JobStateCompleted),
		}),
		sqsRecord(t, "msg-fail", consumer.CompletionMessage{
			UserID: "user-a", JobID: "job-fail", Status: string(model.JobStateFailed),
		}),
	}})
	if err != nil {
		t.Fatalf("HandleRequest() error = %v", err)
	}
	if len(resp.BatchItemFailures) != 1 || resp.BatchItemFailures[0].ItemIdentifier != "msg-fail" {
		t.Errorf("BatchItemFailures = %+v, want only msg-fail", resp.BatchItemFailures)
	}
	if len(store.calls) != 2 {
		t.Errorf("UpdateStatus calls = %+v, want 2 calls", store.calls)
	}
}
