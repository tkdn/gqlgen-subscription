import { InMemoryCache, split } from '@apollo/client';
import { getMainDefinition } from '@apollo/client/utilities';
import { HttpLink } from 'apollo-angular/http';

import { SSELink } from './sse-link';

export function apolloOptionsFactory(httpLink: HttpLink) {
  const http = httpLink.create({ uri: '/query' });
  const sse = new SSELink({ url: '/query' });

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
