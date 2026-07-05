import { provideHttpClient, withFetch } from '@angular/common/http';
import { HttpTestingController, provideHttpClientTesting } from '@angular/common/http/testing';
import { render, screen } from '@testing-library/angular';
import userEvent from '@testing-library/user-event';
import { Subject } from 'rxjs';
import { describe, expect, it } from 'vitest';

import { GraphqlSubscriptionService } from '../graphql/graphql-subscription.service';
import { Job } from '../models/job.model';
import { JobBoard } from './job-board';

async function renderJobBoard(jobs$: Subject<Job[]>) {
  const result = await render(JobBoard, {
    providers: [
      provideHttpClient(withFetch()),
      provideHttpClientTesting(),
      {
        provide: GraphqlSubscriptionService,
        useValue: { jobStatuses: () => jobs$.asObservable() },
      },
    ],
  });
  return {
    ...result,
    httpMock: result.fixture.debugElement.injector.get(HttpTestingController),
  };
}

describe('JobBoard', () => {
  it('renders jobs pushed through the subscription', async () => {
    const jobs$ = new Subject<Job[]>();
    await renderJobBoard(jobs$);

    jobs$.next([{ name: 'job-1', status: 'PENDING' }]);

    expect(await screen.findByText('job-1')).toBeTruthy();
    expect((screen.getByRole('button', { name: 'PENDING' }) as HTMLButtonElement).disabled).toBe(
      true
    );
  });

  it('creates a job via the createJob mutation', async () => {
    const jobs$ = new Subject<Job[]>();
    const { httpMock } = await renderJobBoard(jobs$);
    const user = userEvent.setup();

    await user.type(screen.getByPlaceholderText('job name'), 'job-2');
    await user.click(screen.getByRole('button', { name: 'Create Job' }));

    const req = httpMock.expectOne('/query');
    const body = req.request.body as { query: string; variables?: Record<string, unknown> };
    expect(body.query).toContain('createJob');
    expect(body.variables?.['name']).toBe('job-2');

    req.flush({ data: { createJob: { name: 'job-2', status: 'PENDING' } } });
    httpMock.verify();
  });

  it('updates job status via the updateJobStatus mutation', async () => {
    const jobs$ = new Subject<Job[]>();
    const { httpMock } = await renderJobBoard(jobs$);
    jobs$.next([{ name: 'job-1', status: 'PENDING' }]);
    await screen.findByText('job-1');

    const user = userEvent.setup();
    await user.click(screen.getByRole('button', { name: 'COMPLETED' }));

    const req = httpMock.expectOne('/query');
    const body = req.request.body as { query: string; variables?: Record<string, unknown> };
    expect(body.query).toContain('updateJobStatus');
    expect(body.variables?.['name']).toBe('job-1');
    expect(body.variables?.['status']).toBe('COMPLETED');

    req.flush({ data: { updateJobStatus: { name: 'job-1', status: 'COMPLETED' } } });
    httpMock.verify();
  });
});
