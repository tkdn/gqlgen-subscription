export const RETRY_MAX_BACKOFF_MS = 30_000;

const JITTER_MIN_MS = 300;
const JITTER_MAX_MS = 3_000;

/**
 * retries回目（0始まり）の再試行までの待ち時間。1秒から倍々で伸ばしてRETRY_MAX_BACKOFF_MSで
 * 頭打ちにし、その上にジッターを足す。頭打ちのあとも全クライアントの再試行時刻が揃わない。
 */
export function retryDelayMs(retries: number, random: () => number = Math.random): number {
  const backoff = Math.min(1_000 * 2 ** retries, RETRY_MAX_BACKOFF_MS);
  return backoff + Math.floor(random() * (JITTER_MAX_MS - JITTER_MIN_MS) + JITTER_MIN_MS);
}

export function waitForRetry(retries: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, retryDelayMs(retries)));
}
