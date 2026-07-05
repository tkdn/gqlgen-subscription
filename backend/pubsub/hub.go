package pubsub

import (
	"context"
	"fmt"
	"sync"

	"github.com/redis/go-redis/v9"

	"github.com/tkdn/gqlgen-subscription/backend/jobstore"
)

// Hub はユーザーごとのRedis Subscribe接続を一元管理し、複数の購読者へ
// fan-outする。プロセス内に1つ生成して使う。
type Hub struct {
	rdb *redis.Client

	mu   sync.Mutex
	subs map[string]*userSub
}

// userSub は1ユーザー分のRedis Subscribe接続と、そこにぶら下がる購読者を保持する。
type userSub struct {
	ps          *redis.PubSub
	subscribers map[chan struct{}]struct{}
}

// New はHubを生成する。
func New(rdb *redis.Client) *Hub {
	return &Hub{
		rdb:  rdb,
		subs: make(map[string]*userSub),
	}
}

// Subscribe はuserIDのジョブ更新通知を購読する。戻り値のチャネルは、
// 該当ユーザーのジョブが更新されるたびに（ペイロード内容を問わず）
// トリガーとして通知を受け取る。呼び出し側はこの通知をきっかけに
// jobstore.List等でスナップショットを取り直す想定。
//
// unsubscribe を呼ぶまでに複数回Subscribeが行われても、同一userIDに
// 対するRedis Subscribe接続は1つだけ生成され、以後の呼び出しは
// 参照カウントを増やすだけでその接続にfan-outされる。誰も購読しなく
// なった時点で接続を破棄する。
func (h *Hub) Subscribe(userID string) (ch <-chan struct{}, unsubscribe func(), err error) {
	h.mu.Lock()
	sub, ok := h.subs[userID]
	if !ok {
		ps := h.rdb.Subscribe(context.Background(), jobstore.UpdatesChannel(userID))
		// SUBSCRIBEがRedisサーバーに実際に受理されたことを確認してから返す。
		// これを待たずに返すと、呼び出し直後のPublishが購読前に発行され
		// 通知を取りこぼす可能性がある。
		if _, err := ps.Receive(context.Background()); err != nil {
			_ = ps.Close()
			h.mu.Unlock()
			return nil, nil, fmt.Errorf("pubsub: subscribe %q: %w", userID, err)
		}

		sub = &userSub{
			ps:          ps,
			subscribers: make(map[chan struct{}]struct{}),
		}
		h.subs[userID] = sub
		go h.relay(userID, sub)
	}

	triggerCh := make(chan struct{}, 1)
	sub.subscribers[triggerCh] = struct{}{}
	h.mu.Unlock()

	return triggerCh, func() {
		h.unsubscribe(userID, triggerCh)
	}, nil
}

// relay はRedisからのメッセージを受信するたびに、登録されている
// 購読者すべてへ非ブロッキングで通知する。ps.Close()されるとメッセージ
// チャネルが閉じ、このgoroutineは終了する。
func (h *Hub) relay(userID string, sub *userSub) {
	for range sub.ps.Channel() {
		h.mu.Lock()
		for triggerCh := range sub.subscribers {
			select {
			case triggerCh <- struct{}{}:
			default:
				// バッファ済みの通知が残っている場合は取りこぼしても問題ない
				// （受信側は通知のたびに最新スナップショットを取り直すため）。
			}
		}
		h.mu.Unlock()
	}
}

// unsubscribe は購読者をuserSubから取り除き、誰もいなくなったら
// Redis Subscribe接続を破棄する。
func (h *Hub) unsubscribe(userID string, triggerCh chan struct{}) {
	h.mu.Lock()
	defer h.mu.Unlock()

	sub, ok := h.subs[userID]
	if !ok {
		return
	}

	delete(sub.subscribers, triggerCh)
	if len(sub.subscribers) == 0 {
		_ = sub.ps.Close()
		delete(h.subs, userID)
	}
}

