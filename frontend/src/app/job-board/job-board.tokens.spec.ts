import { TestBed } from '@angular/core/testing';
import { afterEach, beforeEach, describe, expect, it } from 'vitest';

import { PAGE_VISIBLE, RESUBSCRIBE_DELAY_MS } from './job-board.tokens';
import { retryDelayMs } from '../graphql/retry';

describe('PAGE_VISIBLE', () => {
  let state: DocumentVisibilityState;

  beforeEach(() => {
    state = 'visible';
    Object.defineProperty(document, 'visibilityState', { configurable: true, get: () => state });
  });

  afterEach(() => {
    Reflect.deleteProperty(document, 'visibilityState');
  });

  function setVisibility(next: DocumentVisibilityState) {
    state = next;
    document.dispatchEvent(new Event('visibilitychange'));
  }

  // 購読時の表示状態を流し、以後は変化したときだけ流す。
  it('emits the current visibility and then each change', () => {
    const values: boolean[] = [];
    const subscription = TestBed.inject(PAGE_VISIBLE).subscribe((visible) => values.push(visible));

    setVisibility('hidden');
    setVisibility('hidden');
    setVisibility('visible');
    subscription.unsubscribe();

    expect(values).toEqual([true, false, true]);
  });

  // 表示状態はトークンの生成時ではなく購読時に読む。
  it('reads the visibility at subscription time', () => {
    const visible$ = TestBed.inject(PAGE_VISIBLE);
    state = 'hidden';

    const values: boolean[] = [];
    visible$.subscribe((visible) => values.push(visible)).unsubscribe();

    expect(values).toEqual([false]);
  });
});

describe('RESUBSCRIBE_DELAY_MS', () => {
  // 張り直し間隔の既定はretryDelayMs。
  it('defaults to retryDelayMs', () => {
    expect(TestBed.inject(RESUBSCRIBE_DELAY_MS)).toBe(retryDelayMs);
  });
});
