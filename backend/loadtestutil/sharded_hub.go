package loadtestutil

import (
	"context"
	"fmt"
	"log"
	"sync"

	"github.com/jackc/pgx/v5"
)

// ShardedConnectFunc はSubscribe用のLISTEN専用接続を生成する。
type ShardedConnectFunc func(ctx context.Context) (*pgx.Conn, error)

// ShardedListFunc はuserIDの最新一覧を取得する関数。
type ShardedListFunc[T any] func(ctx context.Context, userID string) ([]T, error)

// shardedChannel はuserID1件分のLISTEN接続と購読者集合を保持する。
// 同一userIDへの複数回のSubscribeはこのインスタンスを共有し、dispatch時に
// listを1回だけ呼んで全購読者へ同じ結果を配る（pgpubsub.Hub[T]と同じ集約設計）。
type shardedChannel[T any] struct {
	mu   sync.Mutex
	subs map[chan []T]struct{}

	cancel context.CancelFunc
	done   chan struct{}
}

// ShardedHub はuserIDごとに専用のNOTIFYチャンネル（channelPrefix_userID）を
// LISTENする検証専用のfan-out層。同一userIDの複数購読者は1本のLISTEN接続を
// 共有し、dispatch時にlistを1回だけ呼んで全購読者へ配る。配るデータの型は
// ジェネリクスで、このパッケージは呼び出し側のドメイン型を一切知らない。
type ShardedHub[T any] struct {
	connect       ShardedConnectFunc
	channelPrefix string
	list          ShardedListFunc[T]

	mu       sync.Mutex
	channels map[string]*shardedChannel[T]
}

// NewShardedHub はShardedHubを生成する。実際のLISTENはそのuserIDへの最初の
// Subscribe呼び出し時に行われる（Hub生成時点では何もLISTENしない）。
func NewShardedHub[T any](ctx context.Context, connect ShardedConnectFunc, channelPrefix string, list ShardedListFunc[T]) *ShardedHub[T] {
	return &ShardedHub[T]{
		connect:       connect,
		channelPrefix: channelPrefix,
		list:          list,
		channels:      make(map[string]*shardedChannel[T]),
	}
}

// Subscribe はuserID専用のチャンネル（channelPrefix_userID）を購読する。
// 同一userIDへの2回目以降のSubscribeは、既存のLISTEN接続を共有し、新しい
// 接続は張らない。unsubscribeは購読を解除し、そのuserIDの最後の購読者が
// 抜けたタイミングで接続をクローズする。
func (h *ShardedHub[T]) Subscribe(userID string) (ch <-chan []T, unsubscribe func(), err error) {
	h.mu.Lock()
	sc, exists := h.channels[userID]
	if !exists {
		sc = &shardedChannel[T]{subs: make(map[chan []T]struct{})}
		h.channels[userID] = sc
	}
	h.mu.Unlock()

	if !exists {
		if err := h.startListening(userID, sc); err != nil {
			h.mu.Lock()
			delete(h.channels, userID)
			h.mu.Unlock()
			return nil, nil, err
		}
	}

	dataCh := make(chan []T, 1)
	sc.mu.Lock()
	sc.subs[dataCh] = struct{}{}
	sc.mu.Unlock()

	unsubscribe = func() {
		sc.mu.Lock()
		delete(sc.subs, dataCh)
		empty := len(sc.subs) == 0
		sc.mu.Unlock()

		if empty {
			h.mu.Lock()
			if h.channels[userID] == sc {
				delete(h.channels, userID)
			}
			h.mu.Unlock()
			sc.cancel()
			<-sc.done
		}
	}

	return dataCh, unsubscribe, nil
}

// startListening はuserID専用のLISTEN接続を張り、受信ループを開始する。
//
// ctxをcontext.Background()から独立させて作る（Subscribe呼び出し元のctxを
// 継承しない）のは、pgpubsub.Hub[T]のNewと同じ設計判断による。この接続の
// 寿命はSubscribe呼び出し元のリクエストスコープではなく、unsubscribe/Close
// が呼ばれるまでの間、複数の購読者に横断して共有され続ける必要があるため、
// 個々のSubscribe呼び出しのctxに縛られてはならない。
func (h *ShardedHub[T]) startListening(userID string, sc *shardedChannel[T]) error {
	ctx, cancel := context.WithCancel(context.Background())
	sc.cancel = cancel
	sc.done = make(chan struct{})

	conn, err := h.connect(ctx)
	if err != nil {
		cancel()
		return fmt.Errorf("loadtestutil: sharded hub connect: %w", err)
	}

	channel := h.channelPrefix + "_" + userID
	if _, err := conn.Exec(ctx, "LISTEN "+pgx.Identifier{channel}.Sanitize()); err != nil {
		_ = conn.Close(ctx)
		cancel()
		return fmt.Errorf("loadtestutil: sharded hub listen %q: %w", channel, err)
	}

	go func() {
		defer close(sc.done)
		defer conn.Close(context.Background())
		for {
			_, err := conn.WaitForNotification(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				log.Printf("loadtestutil: sharded hub wait for notification (%s): %v", channel, err)
				return
			}
			h.dispatch(ctx, userID, sc)
		}
	}()

	return nil
}

// dispatch はuserIDの購読者すべてへ、最新の一覧を配る。
// 購読者数に関わらずlistは1回だけ呼ぶ。
func (h *ShardedHub[T]) dispatch(ctx context.Context, userID string, sc *shardedChannel[T]) {
	sc.mu.Lock()
	targets := make([]chan []T, 0, len(sc.subs))
	for ch := range sc.subs {
		targets = append(targets, ch)
	}
	sc.mu.Unlock()

	if len(targets) == 0 {
		return
	}

	values, err := h.list(ctx, userID)
	if err != nil {
		log.Printf("loadtestutil: sharded hub list for %q: %v", userID, err)
		return
	}

	for _, ch := range targets {
		select {
		case ch <- values:
		default:
		}
	}
}

// Close は全userIDのLISTEN接続をクローズする。
func (h *ShardedHub[T]) Close() {
	h.mu.Lock()
	channels := make([]*shardedChannel[T], 0, len(h.channels))
	for _, sc := range h.channels {
		channels = append(channels, sc)
	}
	h.channels = make(map[string]*shardedChannel[T])
	h.mu.Unlock()

	for _, sc := range channels {
		sc.cancel()
		<-sc.done
	}
}
