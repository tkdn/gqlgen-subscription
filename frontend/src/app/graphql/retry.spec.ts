import { afterEach, describe, expect, it, vi } from 'vitest';

import { RETRY_MAX_BACKOFF_MS, retryDelayMs, waitForRetry } from './retry';

describe('retryDelayMs', () => {
  // 待ち時間は1秒から倍々で伸びる。
  it('doubles the backoff starting from one second', () => {
    expect(retryDelayMs(0, () => 0)).toBe(1_300);
    expect(retryDelayMs(1, () => 0)).toBe(2_300);
    expect(retryDelayMs(3, () => 0)).toBe(8_300);
  });

  // 何回目の再試行でも、倍々の部分は上限で頭打ちになる。
  it('caps the backoff at RETRY_MAX_BACKOFF_MS no matter how many retries', () => {
    expect(retryDelayMs(5, () => 0)).toBe(RETRY_MAX_BACKOFF_MS + 300);
    expect(retryDelayMs(1_000, () => 0)).toBe(RETRY_MAX_BACKOFF_MS + 300);
  });

  // ジッター（300ms〜3s）は頭打ちの上に足される。
  it('adds a jitter between 300ms and 3s on top of the cap', () => {
    expect(retryDelayMs(1_000, () => 0.999_999)).toBe(RETRY_MAX_BACKOFF_MS + 2_999);
  });
});

describe('waitForRetry', () => {
  afterEach(() => {
    vi.useRealTimers();
    vi.restoreAllMocks();
  });

  // retryDelayMsの時間だけ待ってから解決する。
  it('resolves after retryDelayMs', async () => {
    vi.useFakeTimers();
    vi.spyOn(Math, 'random').mockReturnValue(0);

    let resolved = false;
    void waitForRetry(0).then(() => {
      resolved = true;
    });

    await vi.advanceTimersByTimeAsync(1_299);
    expect(resolved).toBe(false);
    await vi.advanceTimersByTimeAsync(1);
    expect(resolved).toBe(true);
  });
});
