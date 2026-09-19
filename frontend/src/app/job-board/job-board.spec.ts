import { ApolloTestingController, ApolloTestingModule } from 'apollo-angular/testing';
import { render, screen } from '@testing-library/angular';
import userEvent from '@testing-library/user-event';
import { describe, expect, it } from 'vitest';

import { JobBoard } from './job-board';

async function renderJobBoard() {
  const result = await render(JobBoard, {
    imports: [ApolloTestingModule],
  });
  return {
    ...result,
    controller: result.fixture.debugElement.injector.get(ApolloTestingController),
  };
}

describe('JobBoard', () => {
  it('renders jobs pushed through the subscription', async () => {
    const { controller } = await renderJobBoard();

    const op = controller.expectOne('JobStatuses');
    op.flush({
      data: { jobStatuses: [{ id: 'job-id-1', name: 'job-1', status: 'PENDING' }] },
    });

    expect(await screen.findByText('job-1')).toBeTruthy();
    expect((screen.getByRole('button', { name: 'PENDING' }) as HTMLButtonElement).disabled).toBe(
      true,
    );

    controller.verify();
  });

  it('creates a job via the createJob mutation', async () => {
    const { controller } = await renderJobBoard();
    controller.expectOne('JobStatuses').flush({ data: { jobStatuses: [] } });

    const user = userEvent.setup();
    await user.type(screen.getByPlaceholderText('job name'), 'job-2');
    await user.click(screen.getByRole('button', { name: 'Create Job' }));

    const op = controller.expectOne('CreateJob');
    expect(op.operation.variables['name']).toBe('job-2');
    op.flush({ data: { createJob: { id: 'job-id-2', name: 'job-2', status: 'PENDING' } } });

    controller.verify();
  });

  it('updates job status via the updateJobStatus mutation', async () => {
    const { controller } = await renderJobBoard();
    controller
      .expectOne('JobStatuses')
      .flush({ data: { jobStatuses: [{ id: 'job-id-1', name: 'job-1', status: 'PENDING' }] } });
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
