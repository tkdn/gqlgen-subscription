import { AsyncPipe } from '@angular/common';
import { Component, inject, signal } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { Observable } from 'rxjs';

import { GraphqlClientService } from '../graphql/graphql-client.service';
import { GraphqlSubscriptionService } from '../graphql/graphql-subscription.service';
import { Job, JOB_STATES, JobState } from '../models/job.model';

@Component({
  selector: 'app-job-board',
  imports: [AsyncPipe, FormsModule],
  templateUrl: './job-board.html',
  styleUrl: './job-board.css',
})
export class JobBoard {
  private readonly graphqlClient = inject(GraphqlClientService);
  private readonly graphqlSubscription = inject(GraphqlSubscriptionService);

  protected readonly jobStates = JOB_STATES;
  protected readonly newJobName = signal('');
  protected readonly jobs$: Observable<Job[]> = this.graphqlSubscription.jobStatuses();

  async createJob(): Promise<void> {
    const name = this.newJobName().trim();
    if (!name) {
      return;
    }
    await this.graphqlClient.createJob(name);
    this.newJobName.set('');
  }

  async updateJobStatus(name: string, status: JobState): Promise<void> {
    await this.graphqlClient.updateJobStatus(name, status);
  }
}
