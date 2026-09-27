import { ApolloTestingController, ApolloTestingModule, TestOperation } from 'apollo-angular/testing';
import { render, screen, waitFor } from '@testing-library/angular';
import userEvent from '@testing-library/user-event';
import { BehaviorSubject } from 'rxjs';
import { describe, expect, it } from 'vitest';

import { Job } from '../models/job.model';
import { JobBoard } from './job-board';
import { PAGE_VISIBLE, RETRY_DELAY_MS } from './job-board.tokens';

const JOB_1: Job = { id: 'job-id-1', name: 'job-1', status: 'PENDING' };
const JOB_2: Job = { id: 'job-id-2', name: 'job-2', status: 'ANALYZING' };

async function renderJobBoard() {
  const visible = new BehaviorSubject(true);
  const result = await render(JobBoard, {
    imports: [ApolloTestingModule],
    providers: [
      { provide: PAGE_VISIBLE, useValue: visible.asObservable() },
      { provide: RETRY_DELAY_MS, useValue: () => 0 },
    ],
  });
  return {
    ...result,
    visible,
    controller: result.fixture.debugElement.injector.get(ApolloTestingController),
  };
}

function invalidate(subscription: TestOperation) {
  subscription.flush({ data: { jobsInvalidated: true } });
}

/** invalidationを1件流し、それを受けて発行されたJobs queryにjobsで応答する。 */
async function invalidateAndRespond(
  controller: ApolloTestingController,
  subscription: TestOperation,
  jobs: Job[],
) {
  invalidate(subscription);
  const query = await waitFor(() => controller.expectOne('Jobs'));
  query.flush({ data: { jobs } });
}

/** 保留中のPromiseとタイマーを1巡させる。 */
function settle() {
  return new Promise((resolve) => setTimeout(resolve, 0));
}

describe('JobBoard', () => {
  // invalidationを受けたらjobs queryを発行し、結果を表示する。
  it('fetches jobs when the subscription signals an invalidation', async () => {
    const { controller } = await renderJobBoard();

    const subscription = controller.expectOne('JobsInvalidated');
    controller.expectNone('Jobs');
    await invalidateAndRespond(controller, subscription, [JOB_1]);

    expect(await screen.findByText('job-1')).toBeTruthy();
    expect((screen.getByRole('button', { name: 'PENDING' }) as HTMLButtonElement).disabled).toBe(
      true,
    );
    controller.verify();
  });

  // invalidationのたびに一覧を丸ごと置き換える。
  it('replaces the whole list on every invalidation', async () => {
    const { controller } = await renderJobBoard();
    const subscription = controller.expectOne('JobsInvalidated');

    await invalidateAndRespond(controller, subscription, [JOB_1]);
    await screen.findByText('job-1');
    await invalidateAndRespond(controller, subscription, [JOB_2]);

    await screen.findByText('job-2');
    await waitFor(() => expect(screen.queryByText('job-1')).toBeNull());
    controller.verify();
  });

  // 最後のinvalidationのあとに始まった取得の結果が表示に残り、古い取得の結果は捨てられる。
  it('shows the result of the query started after the latest invalidation', async () => {
    const { controller, detectChanges } = await renderJobBoard();
    const subscription = controller.expectOne('JobsInvalidated');

    invalidate(subscription);
    const stale = await waitFor(() => controller.expectOne('Jobs'));
    invalidate(subscription);
    const latest = await waitFor(() => controller.expectOne('Jobs'));

    latest.flush({ data: { jobs: [JOB_2] } });
    await screen.findByText('job-2');
    stale.flush({ data: { jobs: [JOB_1] } });
    await settle();
    detectChanges();

    expect(screen.queryByText('job-1')).toBeNull();
    expect(screen.getByText('job-2')).toBeTruthy();
    controller.verify();
  });

  // 非表示の間は購読を閉じてinvalidationを無視し、表示に戻るとすぐに購読し直す。
  it('closes the subscription while the tab is hidden and resubscribes as soon as it is visible', async () => {
    const { controller, visible } = await renderJobBoard();
    const hiddenSubscription = controller.expectOne('JobsInvalidated');

    visible.next(false);
    invalidate(hiddenSubscription);
    await settle();
    controller.expectNone('Jobs');

    visible.next(true);
    const subscription = controller.expectOne('JobsInvalidated');
    await invalidateAndRespond(controller, subscription, [JOB_1]);

    expect(await screen.findByText('job-1')).toBeTruthy();
    controller.verify();
  });

  // サーバーが購読をcompleteで終えたら、購読し直す。
  it('resubscribes when the server completes the subscription', async () => {
    const { controller } = await renderJobBoard();

    controller.expectOne('JobsInvalidated').complete();
    const subscription = await waitFor(() => controller.expectOne('JobsInvalidated'));
    await invalidateAndRespond(controller, subscription, [JOB_1]);

    expect(await screen.findByText('job-1')).toBeTruthy();
    controller.verify();
  });

  // 購読がエラーで終わったら、購読し直す。
  it('resubscribes when the subscription ends with an error', async () => {
    const { controller } = await renderJobBoard();

    controller.expectOne('JobsInvalidated').networkError(new Error('connection lost'));
    const subscription = await waitFor(() => controller.expectOne('JobsInvalidated'));
    controller.expectNone('Jobs');
    await invalidateAndRespond(controller, subscription, [JOB_1]);

    expect(await screen.findByText('job-1')).toBeTruthy();
    controller.verify();
  });

  // jobs queryが再試行しても失敗し続けたら、表示中の一覧を保ち、次のinvalidationで取り直す。
  it('keeps showing the list and keeps listening after a jobs query fails', async () => {
    const { controller } = await renderJobBoard();
    const subscription = controller.expectOne('JobsInvalidated');
    await invalidateAndRespond(controller, subscription, [JOB_1]);
    await screen.findByText('job-1');

    invalidate(subscription);
    for (let attempt = 0; attempt < 4; attempt++) {
      (await waitFor(() => controller.expectOne('Jobs'))).networkError(new Error('query failed'));
    }
    await settle();
    expect(screen.getByText('job-1')).toBeTruthy();
    controller.expectNone('Jobs');

    await invalidateAndRespond(controller, subscription, [JOB_2]);
    expect(await screen.findByText('job-2')).toBeTruthy();
    controller.verify();
  });

  // jobs queryが失敗しても、待ち時間を挟んで取り直し、次のinvalidationを待たずに表示する。
  it('retries a failed jobs query without waiting for the next invalidation', async () => {
    const { controller } = await renderJobBoard();
    const subscription = controller.expectOne('JobsInvalidated');

    invalidate(subscription);
    (await waitFor(() => controller.expectOne('Jobs'))).networkError(new Error('query failed'));
    (await waitFor(() => controller.expectOne('Jobs'))).flush({ data: { jobs: [JOB_1] } });

    expect(await screen.findByText('job-1')).toBeTruthy();
    controller.verify();
  });

  // Create JobボタンでcreateJob mutationを発行する。
  it('creates a job via the createJob mutation', async () => {
    const { controller } = await renderJobBoard();
    await invalidateAndRespond(controller, controller.expectOne('JobsInvalidated'), []);

    const user = userEvent.setup();
    await user.type(screen.getByPlaceholderText('job name'), 'job-2');
    await user.click(screen.getByRole('button', { name: 'Create Job' }));

    const op = controller.expectOne('CreateJob');
    expect(op.operation.variables['name']).toBe('job-2');
    op.flush({ data: { createJob: { id: 'job-id-2', name: 'job-2', status: 'PENDING' } } });

    controller.verify();
  });

  // ステータスのボタンでupdateJobStatus mutationを発行する。
  it('updates job status via the updateJobStatus mutation', async () => {
    const { controller } = await renderJobBoard();
    await invalidateAndRespond(controller, controller.expectOne('JobsInvalidated'), [JOB_1]);
    await screen.findByText('job-1');

    const user = userEvent.setup();
    await user.click(screen.getByRole('button', { name: 'COMPLETED' }));

    const op = controller.expectOne('UpdateJobStatus');
    expect(op.operation.variables['id']).toBe('job-id-1');
    expect(op.operation.variables['status']).toBe('COMPLETED');
    op.flush({ data: { updateJobStatus: { id: 'job-id-1', name: 'job-1', status: 'COMPLETED' } } });

    controller.verify();
  });
});
