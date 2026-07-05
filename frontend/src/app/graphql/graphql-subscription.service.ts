import { Service } from '@angular/core';
import { Client, createClient } from 'graphql-sse';
import { Observable } from 'rxjs';

import { Job } from '../models/job.model';

const JOB_STATUSES_SUBSCRIPTION = `
  subscription JobStatuses {
    jobStatuses {
      name
      status
    }
  }
`;

@Service()
export class GraphqlSubscriptionService {
  private readonly client: Client = createClient({ url: '/query' });

  jobStatuses(): Observable<Job[]> {
    return new Observable<Job[]>((subscriber) => {
      return this.client.subscribe<{ jobStatuses: Job[] }>(
        { query: JOB_STATUSES_SUBSCRIPTION },
        {
          next: (result) => {
            if (result.errors?.length) {
              subscriber.error(new Error(result.errors.map((error) => error.message).join(', ')));
              return;
            }
            if (result.data) {
              subscriber.next(result.data.jobStatuses);
            }
          },
          error: (error) => subscriber.error(error),
          complete: () => subscriber.complete(),
        }
      );
    });
  }
}
