import { DOCUMENT, inject, InjectionToken } from '@angular/core';
import { defer, distinctUntilChanged, fromEvent, map, Observable, startWith } from 'rxjs';

import { retryDelayMs } from '../graphql/retry';

/** タブが表示されているか。購読した時点の値をすぐに流し、以後は変化のたびに流す。 */
export const PAGE_VISIBLE = new InjectionToken<Observable<boolean>>('PAGE_VISIBLE', {
  providedIn: 'root',
  factory: () => {
    const document = inject(DOCUMENT);
    const isVisible = () => document.visibilityState === 'visible';
    return defer(() =>
      fromEvent(document, 'visibilitychange').pipe(map(isVisible), startWith(isVisible())),
    ).pipe(distinctUntilChanged());
  },
});

/** 購読が終わってから張り直すまでの待ち時間。retriesは0始まり。 */
export const RESUBSCRIBE_DELAY_MS = new InjectionToken<(retries: number) => number>(
  'RESUBSCRIBE_DELAY_MS',
  { providedIn: 'root', factory: () => retryDelayMs },
);
