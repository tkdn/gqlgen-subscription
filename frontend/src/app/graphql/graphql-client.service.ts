import { HttpClient } from '@angular/common/http';
import { Service, inject } from '@angular/core';
import { firstValueFrom } from 'rxjs';

import { Job, JobState } from '../models/job.model';

interface GraphQLResponse<T> {
  data?: T;
  errors?: { message: string }[];
}

const JOBS_QUERY = `
  query Jobs {
    jobs {
      name
      status
    }
  }
`;

const CREATE_JOB_MUTATION = `
  mutation CreateJob($name: String!) {
    createJob(name: $name) {
      name
      status
    }
  }
`;

const UPDATE_JOB_STATUS_MUTATION = `
  mutation UpdateJobStatus($name: String!, $status: JobState!) {
    updateJobStatus(name: $name, status: $status) {
      name
      status
    }
  }
`;

@Service()
export class GraphqlClientService {
  private readonly http = inject(HttpClient);

  async jobs(): Promise<Job[]> {
    const result = await this.execute<{ jobs: Job[] }>(JOBS_QUERY);
    return result.jobs;
  }

  async createJob(name: string): Promise<Job> {
    const result = await this.execute<{ createJob: Job }>(CREATE_JOB_MUTATION, { name });
    return result.createJob;
  }

  async updateJobStatus(name: string, status: JobState): Promise<Job> {
    const result = await this.execute<{ updateJobStatus: Job }>(UPDATE_JOB_STATUS_MUTATION, {
      name,
      status,
    });
    return result.updateJobStatus;
  }

  private async execute<T>(query: string, variables?: Record<string, unknown>): Promise<T> {
    const response = await firstValueFrom(
      this.http.post<GraphQLResponse<T>>('/query', { query, variables })
    );

    if (response.errors?.length) {
      throw new Error(response.errors.map((error) => error.message).join(', '));
    }
    if (!response.data) {
      throw new Error('GraphQL response contained no data');
    }
    return response.data;
  }
}
