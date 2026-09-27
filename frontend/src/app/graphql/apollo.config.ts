import { InMemoryCache, split } from '@apollo/client';
import { getMainDefinition } from '@apollo/client/utilities';
import { HttpLink } from 'apollo-angular/http';
import { ClientOptions } from 'graphql-sse';

import { waitForRetry } from './retry';
import { SSELink } from './sse-link';

export const SSE_CLIENT_OPTIONS: ClientOptions = {
  url: '/query',
  // 切断と再接続は正常系なので回数では諦めない。待ち時間はwaitForRetryが上限で抑える。
  retryAttempts: Infinity,
  retry: waitForRetry,
};

export function apolloOptionsFactory(httpLink: HttpLink) {
  const http = httpLink.create({ uri: '/query' });
  const sse = new SSELink(SSE_CLIENT_OPTIONS);

  const link = split(
    ({ query }) => {
      const definition = getMainDefinition(query);
      return definition.kind === 'OperationDefinition' && definition.operation === 'subscription';
    },
    sse,
    http
  );

  return {
    link,
    cache: new InMemoryCache(),
  };
}
