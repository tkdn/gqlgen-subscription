import { ApolloLink, Observable } from '@apollo/client';
import { print } from 'graphql';
import { Client, ClientOptions, createClient } from 'graphql-sse';

function isClient(target: ClientOptions | Client): target is Client {
  return typeof (target as Client).subscribe === 'function';
}

export class SSELink extends ApolloLink {
  private readonly client: Client;

  constructor(optionsOrClient: ClientOptions | Client) {
    super();
    this.client = isClient(optionsOrClient) ? optionsOrClient : createClient(optionsOrClient);
  }

  override request(operation: ApolloLink.Operation): Observable<ApolloLink.Result> {
    return new Observable((sink) => {
      return this.client.subscribe(
        {
          operationName: operation.operationName,
          query: print(operation.query),
          variables: operation.variables,
        },
        {
          next: (result) => sink.next(result as ApolloLink.Result),
          error: (error) => sink.error(error),
          complete: () => sink.complete(),
        },
      );
    });
  }
}
