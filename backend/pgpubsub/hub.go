// Package pgpubsub はPostgreSQLのLISTEN/NOTIFYによるジョブ更新通知の
// fan-out層。専用のLISTEN接続を1本だけ張り、単一チャンネルに流れてくる
// 通知をペイロード（userID）で購読者へ振り分ける。ユーザー数が増えても
// DB接続は増えない。プロセス内に1つ生成して使う。
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

// Hub はLISTEN接続を1本保持し、複数の購読者へfan-outする。
type Hub struct {
	connect ConnectFunc
	channel string

	mu   sync.Mutex
	subs map[string]map[chan struct{}]struct{}

	cancel context.CancelFunc
	done   chan struct{}
}

// New はLISTENを確立したHubを生成し、受信ループを開始する。channelは
// 発行側（pgjobstore.Store）と同じ値で揃える必要があるため、配線側で
// 明示的に渡す。ctxは接続確立にのみ使い、受信ループの寿命はCloseで管理する。
func New(ctx context.Context, connect ConnectFunc, channel string) (*Hub, error) {
	conn, err := connect(ctx)
	if err != nil {
		return nil, fmt.Errorf("pgpubsub: connect: %w", err)
	}
	if err := listen(ctx, conn, channel); err != nil {
		_ = conn.Close(ctx)
		return nil, err
	}

	runCtx, cancel := context.WithCancel(context.Background())
	h := &Hub{
		connect: connect,
		channel: channel,
		subs:    make(map[string]map[chan struct{}]struct{}),
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

// Subscribe はuserIDのジョブ更新通知を購読する。戻り値のチャネルは、
// 該当ユーザーのジョブが更新されるたびに（ペイロード内容を問わず）
// トリガーとして通知を受け取る。呼び出し側はこの通知をきっかけに
// jobstore.List等でスナップショットを取り直す想定。
//
// LISTENはHub生成時に確立済みのため、Redis版（pubsub.Hub）にあった
// 「SUBSCRIBE受理を待ってから返す」race対策は構造的に不要。登録のみで返る。
func (h *Hub) Subscribe(userID string) (ch <-chan struct{}, unsubscribe func(), err error) {
	triggerCh := make(chan struct{}, 1)

	h.mu.Lock()
	if h.subs[userID] == nil {
		h.subs[userID] = make(map[chan struct{}]struct{})
	}
	h.subs[userID][triggerCh] = struct{}{}
	h.mu.Unlock()

	return triggerCh, func() {
		h.mu.Lock()
		defer h.mu.Unlock()
		delete(h.subs[userID], triggerCh)
		if len(h.subs[userID]) == 0 {
			delete(h.subs, userID)
		}
	}, nil
}

// Close は受信ループを停止しLISTEN接続を閉じる。
func (h *Hub) Close() {
	h.cancel()
	<-h.done
}

// run は通知を受信するたびにペイロード（userID）に一致する購読者へ
// 非ブロッキングで配送する。接続が切れた場合は再接続・再LISTENを試みる。
// 再接続中に発行された通知は失われるが、受信側は通知のたびに最新
// スナップショットを取り直すため、次の通知で回復する。
func (h *Hub) run(ctx context.Context, conn *pgx.Conn) {
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
		h.dispatch(notification.Payload)
	}
}

// reconnect は接続と再LISTENに成功するまでreconnectIntervalごとに試み続ける。
// ctxがキャンセルされたらnilを返す。
func (h *Hub) reconnect(ctx context.Context) *pgx.Conn {
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

// dispatch はuserIDに登録されている購読者すべてへ非ブロッキングで通知する。
func (h *Hub) dispatch(userID string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for triggerCh := range h.subs[userID] {
		select {
		case triggerCh <- struct{}{}:
		default:
			// バッファ済みの通知が残っている場合は取りこぼしても問題ない
			// （受信側は通知のたびに最新スナップショットを取り直すため）。
		}
	}
}
