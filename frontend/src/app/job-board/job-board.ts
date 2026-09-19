import { AsyncPipe } from '@angular/common';
import { Component, inject, signal } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { Apollo, gql } from 'apollo-angular';
import { firstValueFrom, map, Observable } from 'rxjs';

import { Job, JOB_STATES, JobState } from '../models/job.model';

const CREATE_JOB_MUTATION = gql`
  mutation CreateJob($name: String!) {
    createJob(name: $name) {
      id
      name
      status
    }
  }
`;

const UPDATE_JOB_STATUS_MUTATION = gql`
  mutation UpdateJobStatus($id: ID!, $status: JobState!) {
    updateJobStatus(id: $id, status: $status) {
      id
      name
      status
    }
  }
`;

const JOB_STATUSES_SUBSCRIPTION = gql`
  subscription JobStatuses {
    jobStatuses {
      id
      name
      status
    }
  }
`;

@Component({
  selector: 'app-job-board',
  imports: [AsyncPipe, FormsModule],
  templateUrl: './job-board.html',
  styleUrl: './job-board.css',
})
export class JobBoard {
  private readonly apollo = inject(Apollo);

  protected readonly jobStates = JOB_STATES;
  protected readonly newJobName = signal('');
  protected readonly jobs$: Observable<Job[]> = this.apollo
    .subscribe<{ jobStatuses: Job[] }>({ query: JOB_STATUSES_SUBSCRIPTION })
    .pipe(map((result) => result.data?.jobStatuses ?? []));

  async createJob(): Promise<void> {
    const name = this.newJobName().trim();
    if (!name) {
      return;
    }
    await firstValueFrom(
      this.apollo.mutate<{ createJob: Job }>({
        mutation: CREATE_JOB_MUTATION,
        variables: { name },
      })
    );
    this.newJobName.set('');
  }

  async updateJobStatus(id: string, status: JobState): Promise<void> {
    await firstValueFrom(
      this.apollo.mutate<{ updateJobStatus: Job }>({
        mutation: UPDATE_JOB_STATUS_MUTATION,
        variables: { id, status },
      })
    );
  }
}
