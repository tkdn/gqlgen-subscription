package redisclient

import (
	"os"

	"github.com/redis/go-redis/v9"
)

// defaultAddr は環境変数 REDIS_ADDR が未設定の場合に使うアドレス。
const defaultAddr = "localhost:6379"

// New は環境変数 REDIS_ADDR（未設定なら defaultAddr）を使ってRedisクライアントを生成する。
func New() *redis.Client {
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		addr = defaultAddr
	}

	return redis.NewClient(&redis.Options{
		Addr: addr,
	})
}
