package loadtestutil

import (
	"context"
	"fmt"
	"log"
	"sync"

	"github.com/jackc/pgx/v5"

	"github.com/tkdn/gqlgen-subscription/backend/graph/model"
)

// ShardedConnectFunc はSubscribe用のLISTEN専用接続を生成する。
type ShardedConnectFunc func(ctx context.Context) (*pgx.Conn, error)

// ListFunc はuserIDの最新ジョブ一覧を取得する関数。
type ShardedListFunc func(ctx context.Context, userID string) ([]*model.Job, error)

// ShardedHub はuserIDごとに専用のNOTIFYチャンネル(channelPrefix_userID)を
// LISTENする検証専用のfan-out層。参照カウントによるUNLISTEN共有などの
// 本番品質の最適化は行わない最小実装。
type ShardedHub struct {
	connect       ShardedConnectFunc
	channelPrefix string
	list          ShardedListFunc

	mu      sync.Mutex
	closers []func()
}

// NewShardedHub はShardedHubを生成する。実際のLISTENはSubscribe呼び出しの
// たびに、そのuserID専用の接続で行われる(Hub生成時点では何もLISTENしない)。
func NewShardedHub(ctx context.Context, connect ShardedConnectFunc, channelPrefix string, list ShardedListFunc) *ShardedHub {
	return &ShardedHub{connect: connect, channelPrefix: channelPrefix, list: list}
}

// Subscribe はuserID専用のチャンネル(channelPrefix_userID)に対する新しいLISTEN接続を張る。
// unsubscribeはこの接続をクローズする。
// 最小実装のため、同一userIDへの複数回のSubscribeはそれぞれ独立した接続を張る(本番品質の接続共有・参照カウントは行わない)。
func (h *ShardedHub) Subscribe(userID string) (ch <-chan []*model.Job, unsubscribe func(), err error) {
	ctx, cancel := context.WithCancel(context.Background())

	conn, err := h.connect(ctx)
	if err != nil {
		cancel()
		return nil, nil, fmt.Errorf("loadtestutil: sharded hub connect: %w", err)
	}

	channel := h.channelPrefix + "_" + userID
	if _, err := conn.Exec(ctx, "LISTEN "+pgx.Identifier{channel}.Sanitize()); err != nil {
		_ = conn.Close(ctx)
		cancel()
		return nil, nil, fmt.Errorf("loadtestutil: sharded hub listen %q: %w", channel, err)
	}

	dataCh := make(chan []*model.Job, 1)

	go func() {
		defer close(dataCh)
		for {
			notification, err := conn.WaitForNotification(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				log.Printf("loadtestutil: sharded hub wait for notification (%s): %v", channel, err)
				return
			}
			jobs, err := h.list(ctx, notification.Payload)
			if err != nil {
				log.Printf("loadtestutil: sharded hub list jobs for %q: %v", notification.Payload, err)
				continue
			}
			select {
			case dataCh <- jobs:
			default:
			}
		}
	}()

	closeFunc := func() {
		cancel()
		_ = conn.Close(context.Background())
	}

	h.mu.Lock()
	h.closers = append(h.closers, closeFunc)
	h.mu.Unlock()

	return dataCh, closeFunc, nil
}

// Close は生成された全てのLISTEN接続をクローズする。
func (h *ShardedHub) Close() {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, closeFunc := range h.closers {
		closeFunc()
	}
}
