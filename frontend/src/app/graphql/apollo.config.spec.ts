import { ApolloLink, execute, Observable } from '@apollo/client';
import gql from 'graphql-tag';
import { HttpLink } from 'apollo-angular/http';
import { describe, expect, it, vi } from 'vitest';

import { apolloOptionsFactory } from './apollo.config';
import { SSELink } from './sse-link';

// @apollo/client v4's `execute` requires a third `ApolloLink.ExecuteContext`
// argument (a breaking change from v3). Neither link under test reads
// `operation.client`, so an empty context is safe here.
const EXECUTE_CONTEXT = {} as ApolloLink.ExecuteContext;

const TEST_QUERY = gql`
  query TestQuery {
    jobs {
      id
      name
      status
    }
  }
`;

const TEST_MUTATION = gql`
  mutation TestMutation($name: String!) {
    createJob(name: $name) {
      id
      name
      status
    }
  }
`;

const TEST_SUBSCRIPTION = gql`
  subscription TestSubscription {
    jobStatuses {
      id
      name
      status
    }
  }
`;

/** A marker error used to prove which link's `request()` handled an operation. */
class RouteMarker extends Error {
  constructor(readonly route: 'http' | 'sse') {
    super(`routed:${route}`);
  }
}

function createHttpLinkSpy() {
  const requestSpy = vi.fn();
  const httpLink = new ApolloLink(() => {
    return new Observable((sink) => {
      requestSpy();
      sink.error(new RouteMarker('http'));
    });
  });
  return { httpLink, requestSpy };
}

function runOperation(link: ApolloLink, query: ApolloLink.Operation['query']) {
  return new Promise<unknown>((resolve, reject) => {
    execute(link, { query, variables: {} }, EXECUTE_CONTEXT).subscribe({
      next: (value) => resolve(value),
      error: (err) => reject(err),
      complete: () => resolve(undefined),
    });
  });
}

describe('apolloOptionsFactory', () => {
  it('creates the HttpLink with the /query endpoint', () => {
    const { httpLink } = createHttpLinkSpy();
    const createSpy = vi.fn().mockReturnValue(httpLink);
    const httpLinkService = { create: createSpy } as unknown as HttpLink;

    apolloOptionsFactory(httpLinkService);

    expect(createSpy).toHaveBeenCalledWith({ uri: '/query' });
  });

  it('routes subscription operations to the SSE link, not the HTTP link', async () => {
    const { httpLink, requestSpy: httpRequestSpy } = createHttpLinkSpy();
    const httpLinkService = { create: vi.fn().mockReturnValue(httpLink) } as unknown as HttpLink;

    const subscribeSpy = vi.fn().mockReturnValue(vi.fn());
    vi.spyOn(SSELink.prototype, 'request').mockImplementation(() => {
      return new Observable((sink) => {
        subscribeSpy();
        sink.error(new RouteMarker('sse'));
      });
    });

    try {
      const { link } = apolloOptionsFactory(httpLinkService);

      await expect(runOperation(link, TEST_SUBSCRIPTION)).rejects.toMatchObject({
        route: 'sse',
      });
      expect(subscribeSpy).toHaveBeenCalledTimes(1);
      expect(httpRequestSpy).not.toHaveBeenCalled();
    } finally {
      vi.restoreAllMocks();
    }
  });

  it('routes query operations to the HTTP link, not the SSE link', async () => {
    const { httpLink, requestSpy: httpRequestSpy } = createHttpLinkSpy();
    const httpLinkService = { create: vi.fn().mockReturnValue(httpLink) } as unknown as HttpLink;

    const subscribeSpy = vi.fn().mockReturnValue(vi.fn());
    vi.spyOn(SSELink.prototype, 'request').mockImplementation(() => {
      return new Observable((sink) => {
        subscribeSpy();
        sink.error(new RouteMarker('sse'));
      });
    });

    try {
      const { link } = apolloOptionsFactory(httpLinkService);

      await expect(runOperation(link, TEST_QUERY)).rejects.toMatchObject({ route: 'http' });
      expect(httpRequestSpy).toHaveBeenCalledTimes(1);
      expect(subscribeSpy).not.toHaveBeenCalled();
    } finally {
      vi.restoreAllMocks();
    }
  });

  it('routes mutation operations to the HTTP link, not the SSE link', async () => {
    const { httpLink, requestSpy: httpRequestSpy } = createHttpLinkSpy();
    const httpLinkService = { create: vi.fn().mockReturnValue(httpLink) } as unknown as HttpLink;

    const subscribeSpy = vi.fn().mockReturnValue(vi.fn());
    vi.spyOn(SSELink.prototype, 'request').mockImplementation(() => {
      return new Observable((sink) => {
        subscribeSpy();
        sink.error(new RouteMarker('sse'));
      });
    });

    try {
      const { link } = apolloOptionsFactory(httpLinkService);

      await expect(runOperation(link, TEST_MUTATION)).rejects.toMatchObject({ route: 'http' });
      expect(httpRequestSpy).toHaveBeenCalledTimes(1);
      expect(subscribeSpy).not.toHaveBeenCalled();
    } finally {
      vi.restoreAllMocks();
    }
  });
});
