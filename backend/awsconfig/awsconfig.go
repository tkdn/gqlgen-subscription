// Package awsconfig はAWS SDK v2の設定とSQSクライアントを構築する。
package awsconfig

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

// New はaws.Configを構築する。リージョン・認証情報・エンドポイントは
// SDKの標準チェーン（環境変数・共有設定ファイル・ECSタスクロール等）に
// すべて委ねる。ローカル検証時はAWS_ENDPOINT_URL・AWS_REGION・
// AWS_ACCESS_KEY_ID等の環境変数で明示的に上書きする。
func New(ctx context.Context) (aws.Config, error) {
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return aws.Config{}, fmt.Errorf("awsconfig: load config: %w", err)
	}
	return cfg, nil
}

// SQSClient はcfgからSQSクライアントを構築する。
func SQSClient(cfg aws.Config) *sqs.Client {
	return sqs.NewFromConfig(cfg)
}

// EnsureQueue はnameという名前のSQSキューが存在することを保証し、そのURLを返す。
// SQSのCreateQueueは同名・同属性のキューに対して冪等なため、既存キューがあれば
// そのURLがそのまま返る。事前に別途キューの存在確認を行う必要はない。
func EnsureQueue(ctx context.Context, client *sqs.Client, name string) (string, error) {
	out, err := client.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName: aws.String(name),
	})
	if err != nil {
		return "", fmt.Errorf("awsconfig: ensure queue %q: %w", name, err)
	}
	return aws.ToString(out.QueueUrl), nil
}
