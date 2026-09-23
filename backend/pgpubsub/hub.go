// Package pgpubsub はPostgreSQLのLISTEN/NOTIFYによるジョブ更新通知の
// fan-out層。専用のLISTEN接続を1本だけ張り、単一チャンネルに流れてくる
// 通知をペイロード（userID）で購読者へ振り分ける。ユーザー数が増えても
// DB接続は増えない。プロセス内に1つ生成して使う。
//
// dispatch時にuserIDあたり1回だけListFuncを呼び、結果を購読者全員へ配る。
// これにより、同一ユーザーの購読者数が増えても、1回の通知あたりのList呼び出しは1回のままになる。
// 配るデータの型はジェネリクスで、このパッケージは呼び出し側のドメイン型を一切知らない。
package pgpubsub

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
)

// reconnectInterval は接続断からの再接続を試みる間隔。
const reconnectInterval = time.Second

// ConnectFunc はLISTEN専用接続を生成する。再接続時にも呼ばれる。
type ConnectFunc func(ctx context.Context) (*pgx.Conn, error)

// ListFunc はuserIDの最新一覧を取得する関数。通常はpgjobstore.Store.List。
type ListFunc[T any] func(ctx context.Context, userID string) ([]T, error)

// Hub はLISTEN接続を1本保持し、複数の購読者へfan-outする。
type Hub[T any] struct {
	connect ConnectFunc
	channel string
	list    ListFunc[T]

	mu   sync.Mutex
	subs map[string]map[chan []T]struct{}

	cancel context.CancelFunc
	done   chan struct{}
}

// New はLISTENを確立したHubを生成し、受信ループを開始する。
// channelは発行側（pgjobstore.Store）と同じ値で揃える必要があるため、配線側で明示的に渡す。
// listは通知受信時に最新一覧を取得する関数で、通常はpgjobstore.Store.Listを渡す。
// ctxは接続確立にのみ使い、受信ループの寿命はCloseで管理する。
func New[T any](ctx context.Context, connect ConnectFunc, channel string, list ListFunc[T]) (*Hub[T], error) {
	conn, err := connect(ctx)
	if err != nil {
		return nil, fmt.Errorf("pgpubsub: connect: %w", err)
	}
	if err := listen(ctx, conn, channel); err != nil {
		_ = conn.Close(ctx)
		return nil, err
	}

	runCtx, cancel := context.WithCancel(context.Background())
	h := &Hub[T]{
		connect: connect,
		channel: channel,
		list:    list,
		subs:    make(map[string]map[chan []T]struct{}),
		cancel:  cancel,
		done:    make(chan struct{}),
	}
	go h.run(runCtx, conn)
	return h, nil
}

func listen(ctx context.Context, conn *pgx.Conn, channel string) error {
	if _, err := conn.Exec(ctx, "LISTEN "+pgx.Identifier{channel}.Sanitize()); err != nil {
		return fmt.Errorf("pgpubsub: listen %q: %w", channel, err)
	}
	return nil
}

// Subscribe はuserIDの更新通知を購読する。
// 戻り値のチャネルには、該当ユーザーの更新のたびに最新の一覧そのものが届く。
// dispatch側がuserIDあたり1回だけlistを呼び、同一ユーザーの全購読者へ同じ結果を配るため、呼び出し側で改めてlistを呼び直す必要はない。
//
// LISTENはHub生成時に確立済みのため、Redis版（pubsub.Hub）にあった
// 「SUBSCRIBE受理を待ってから返す」race対策は構造的に不要。登録のみで返る。
func (h *Hub[T]) Subscribe(userID string) (ch <-chan []T, unsubscribe func(), err error) {
	dataCh := make(chan []T, 1)

	h.mu.Lock()
	if h.subs[userID] == nil {
		h.subs[userID] = make(map[chan []T]struct{})
	}
	h.subs[userID][dataCh] = struct{}{}
	h.mu.Unlock()

	return dataCh, func() {
		h.mu.Lock()
		defer h.mu.Unlock()
		delete(h.subs[userID], dataCh)
		if len(h.subs[userID]) == 0 {
			delete(h.subs, userID)
		}
	}, nil
}

// Close は受信ループを停止しLISTEN接続を閉じる。
func (h *Hub[T]) Close() {
	h.cancel()
	<-h.done
}

// run は通知を受信するたびにペイロード（userID）に一致する購読者へ
// 非ブロッキングで配送する。接続が切れた場合は再接続・再LISTENを試みる。
// 再接続中に発行された通知は失われるが、受信側は通知のたびに最新
// スナップショットを取り直すため、次の通知で回復する。
func (h *Hub[T]) run(ctx context.Context, conn *pgx.Conn) {
	defer close(h.done)
	defer func() {
		if conn != nil {
			_ = conn.Close(context.Background())
		}
	}()

	for {
		notification, err := conn.WaitForNotification(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("pgpubsub: wait for notification: %v", err)
			_ = conn.Close(context.Background())
			conn = h.reconnect(ctx)
			if conn == nil {
				return
			}
			continue
		}
		h.dispatch(ctx, notification.Payload)
	}
}

// reconnect は接続と再LISTENに成功するまでreconnectIntervalごとに試み続ける。
// ctxがキャンセルされたらnilを返す。
func (h *Hub[T]) reconnect(ctx context.Context) *pgx.Conn {
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(reconnectInterval):
		}

		conn, err := h.connect(ctx)
		if err != nil {
			log.Printf("pgpubsub: reconnect: %v", err)
			continue
		}
		if err := listen(ctx, conn, h.channel); err != nil {
			log.Printf("pgpubsub: %v", err)
			_ = conn.Close(context.Background())
			continue
		}
		log.Printf("pgpubsub: reconnected, listening on %q", h.channel)
		return conn
	}
}

// dispatch はuserIDに登録されている購読者すべてへ、最新の一覧を配る。
// 購読者数に関わらずlistは1回だけ呼ぶ。
// 購読者が1人もいなければlist自体を呼ばない。
func (h *Hub[T]) dispatch(ctx context.Context, userID string) {
	h.mu.Lock()
	subscribers := h.subs[userID]
	if len(subscribers) == 0 {
		h.mu.Unlock()
		return
	}
	// リストのコピーを取ってからロックを解放する。list呼び出し（DBアクセス）を
	// ロック保持中に行うと、Subscribe/unsubscribeがその間ブロックされるため。
	targets := make([]chan []T, 0, len(subscribers))
	for ch := range subscribers {
		targets = append(targets, ch)
	}
	h.mu.Unlock()

	values, err := h.list(ctx, userID)
	if err != nil {
		log.Printf("pgpubsub: list for %q: %v", userID, err)
		return
	}

	for _, ch := range targets {
		select {
		case ch <- values:
		default:
			// バッファ済みの通知が残っている場合は取りこぼしても問題ない
			// （次の通知で最新状態に復元されるため）。
		}
	}
}
