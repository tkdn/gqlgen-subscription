export type JobState = 'PENDING' | 'ANALYZING' | 'GENERATING' | 'COMPLETED' | 'FAILED';

export const JOB_STATES: readonly JobState[] = [
  'PENDING',
  'ANALYZING',
  'GENERATING',
  'COMPLETED',
  'FAILED',
];

export interface Job {
  name: string;
  status: JobState;
}
