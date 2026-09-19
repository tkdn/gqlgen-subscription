import { ApolloLink, execute } from '@apollo/client';
import gql from 'graphql-tag';
import { Client } from 'graphql-sse';
import { describe, expect, it, vi } from 'vitest';

import { SSELink } from './sse-link';

const TEST_QUERY = gql`
  subscription TestSubscription {
    jobStatuses {
      id
      name
      status
    }
  }
`;

// @apollo/client v4's `execute` requires a third `ApolloLink.ExecuteContext`
// argument (a breaking change from v3). SSELink.request() never reads
// `operation.client`, so an empty context is safe here.
const EXECUTE_CONTEXT = {} as ApolloLink.ExecuteContext;

function createMockClient(subscribeImpl: Client['subscribe']): Client {
  return {
    subscribe: subscribeImpl,
    iterate: vi.fn() as unknown as Client['iterate'],
    dispose: vi.fn(),
  };
}

describe('SSELink', () => {
  it('calls client.subscribe with the printed query and forwards next/complete to the sink', () => {
    const unsubscribe = vi.fn();
    const subscribeMock = vi.fn().mockReturnValue(unsubscribe);
    const client = createMockClient(subscribeMock);
    const link = new SSELink(client);

    const results: unknown[] = [];
    let completed = false;

    const observable = execute(link, { query: TEST_QUERY, variables: {} }, EXECUTE_CONTEXT);
    observable.subscribe({
      next: (value) => results.push(value),
      error: () => {},
      complete: () => {
        completed = true;
      },
    });

    expect(subscribeMock).toHaveBeenCalledTimes(1);
    const [request, sink] = subscribeMock.mock.calls[0];
    expect(request.query).toContain('TestSubscription');
    expect(request.operationName).toBe('TestSubscription');

    sink.next({ data: { jobStatuses: [{ id: '1', name: 'job-1', status: 'PENDING' }] } });
    expect(results).toEqual([
      { data: { jobStatuses: [{ id: '1', name: 'job-1', status: 'PENDING' }] } },
    ]);

    sink.complete();
    expect(completed).toBe(true);
  });

  it('propagates sink errors to the observable error handler', () => {
    const subscribeMock = vi.fn().mockReturnValue(vi.fn());
    const client = createMockClient(subscribeMock);
    const link = new SSELink(client);

    let receivedError: unknown;
    const observable = execute(link, { query: TEST_QUERY, variables: {} }, EXECUTE_CONTEXT);
    observable.subscribe({
      next: () => {},
      error: (err) => {
        receivedError = err;
      },
      complete: () => {},
    });

    const [, sink] = subscribeMock.mock.calls[0];
    const testError = new Error('boom');
    sink.error(testError);

    expect(receivedError).toBe(testError);
  });

  it('unsubscribing the observable calls the dispose function returned by client.subscribe', () => {
    const unsubscribe = vi.fn();
    const subscribeMock = vi.fn().mockReturnValue(unsubscribe);
    const client = createMockClient(subscribeMock);
    const link = new SSELink(client);

    const observable = execute(link, { query: TEST_QUERY, variables: {} }, EXECUTE_CONTEXT);
    const subscription = observable.subscribe({
      next: () => {},
      error: () => {},
      complete: () => {},
    });

    subscription.unsubscribe();

    expect(unsubscribe).toHaveBeenCalledTimes(1);
  });
});
