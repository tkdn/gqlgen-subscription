import { AsyncPipe } from '@angular/common';
import { Component, inject, signal } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { Apollo, gql } from 'apollo-angular';
import {
  catchError,
  EMPTY,
  filter,
  firstValueFrom,
  map,
  Observable,
  repeat,
  switchMap,
  timer,
} from 'rxjs';

import { Job, JOB_STATES, JobState } from '../models/job.model';
import { PAGE_VISIBLE, RESUBSCRIBE_DELAY_MS } from './job-board.tokens';

const JOBS_QUERY = gql`
  query Jobs {
    jobs {
      id
      name
      status
    }
  }
`;

const JOBS_INVALIDATED_SUBSCRIPTION = gql`
  subscription JobsInvalidated {
    jobsInvalidated
  }
`;

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

@Component({
  selector: 'app-job-board',
  imports: [AsyncPipe, FormsModule],
  templateUrl: './job-board.html',
  styleUrl: './job-board.css',
})
export class JobBoard {
  private readonly apollo = inject(Apollo);
  private readonly resubscribeDelayMs = inject(RESUBSCRIBE_DELAY_MS);

  protected readonly jobStates = JOB_STATES;
  protected readonly newJobName = signal('');

  // Apolloはsubscriptionのエラーを結果に畳み込んでからcompleteするので、
  // 終わり方を問わずrepeatで張り直せば、表示中は常に購読している状態を保てる。
  private readonly invalidations$ = this.apollo
    .subscribe<{ jobsInvalidated: boolean }>({ query: JOBS_INVALIDATED_SUBSCRIPTION })
    .pipe(
      filter((result) => result.data?.jobsInvalidated === true),
      repeat({ delay: (count) => timer(this.resubscribeDelayMs(count - 1)) }),
    );

  protected readonly jobs$: Observable<Job[]> = inject(PAGE_VISIBLE).pipe(
    switchMap((visible) => (visible ? this.invalidations$ : EMPTY)),
    switchMap(() => this.fetchJobs()),
  );

  async createJob(): Promise<void> {
    const name = this.newJobName().trim();
    if (!name) {
      return;
    }
    await firstValueFrom(
      this.apollo.mutate<{ createJob: Job }>({
        mutation: CREATE_JOB_MUTATION,
        variables: { name },
      }),
    );
    this.newJobName.set('');
  }

  async updateJobStatus(id: string, status: JobState): Promise<void> {
    await firstValueFrom(
      this.apollo.mutate<{ updateJobStatus: Job }>({
        mutation: UPDATE_JOB_STATUS_MUTATION,
        variables: { id, status },
      }),
    );
  }

  private fetchJobs(): Observable<Job[]> {
    return this.apollo
      .query<{ jobs: Job[] }>({
        query: JOBS_QUERY,
        fetchPolicy: 'no-cache',
        // 実行中の同じqueryに相乗りすると、直前のinvalidationより前に始まった取得の結果を受け取ってしまう。
        context: { queryDeduplication: false },
      })
      .pipe(
        map((result) => result.data?.jobs ?? []),
        // 取得に失敗しても表示中の一覧を保ち、次のinvalidationで取り直す。
        catchError(() => EMPTY),
      );
  }
}
