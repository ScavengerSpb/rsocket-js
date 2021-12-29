import {
  RSocketConnector,
  RSocketServer
} from "@rsocket/core";
import {
  Codec,
  RSocketRequester,
  RSocketResponder,
  DefaultRespondersFactory
} from "@rsocket/messaging";
import { RxRespondersFactory } from "@rsocket/rxjs";
import { AsyncRequestersFactory } from "@rsocket/async";
import { TcpClientTransport } from "@rsocket/transport-tcp-client";
import { TcpServerTransport } from "@rsocket/transport-tcp-server";
import { exit } from "process";
import { map, Observable, tap, timer, interval, take } from "rxjs";
import Logger from "../shared/logger";

class StringCodec implements Codec<string> {
  readonly mimeType: string = "text/plain";

  decode(buffer: Buffer): string {
    return buffer.toString();
  }

  encode(entity: string): Buffer {
    return Buffer.from(entity);
  }
}

const stringCodec = new StringCodec();
const codecs = { inputCodec: stringCodec, outputCodec: stringCodec };

class RawEchoService {
  handleEchoRequestStream(data: string, initialRequestN, subscriber) {
    let requested = initialRequestN;
    let sent = 0;
    let isDone = false;

    const interval = setInterval(() => {
      sent++;
      isDone = sent >= requested;
      const response = `Echo: ${data}`;
      Logger.info("[server] sending", response);
      subscriber.onNext({
        data: codecs.inputCodec.encode(response)
      }, isDone);
      if (isDone) {
        clearInterval(interval);
      }
    }, 1000);

    return {
      request(n) {
        requested += n;
      },
      cancel() {
        clearInterval(interval);
      },
      onExtension() {
      }
    };
  }
}

class RxEchoService {
  handleEchoRequestResponse(data: string): Observable<string> {
    return timer(1000).pipe(map(() => `Echo: ${data}`));
  }

  // TODO: look into why only first value is ever emitted.
  //  suspect issue with `drain()` in rx adapter
  handleEchoRequestStream(data: string): Observable<string> {
    return interval(1000)
      .pipe(
        map(() => `Echo: ${data}`),
        take(5),
        tap(v => console.log(`[server] sending: ${v}`))
      );
  }
}

let serverCloseable;

function makeServer() {
  return new RSocketServer({
    transport: new TcpServerTransport({
      listenOptions: {
        port: 9090,
        host: "127.0.0.1"
      }
    }),
    acceptor: {
      accept: async () => {

        const rawEchoService = new RawEchoService();
        const rxEchoService = new RxEchoService();

        const builder = RSocketResponder.builder();

        builder.route(
          "EchoService.echo",
          RxRespondersFactory.requestResponse(
            rxEchoService.handleEchoRequestResponse,
            codecs
          ));

        builder.route(
          "EchoService.echo",
          DefaultRespondersFactory.requestStreamHandler(
            rawEchoService.handleEchoRequestStream,
            codecs
          ));

        return builder.build();
      }
    }
  });
}

function makeConnector() {
  return new RSocketConnector({
    transport: new TcpClientTransport({
      connectionOptions: {
        host: "127.0.0.1",
        port: 9090
      }
    })
  });
}

async function main() {
  const server = makeServer();
  const connector = makeConnector();

  serverCloseable = await server.bind();
  const rsocket = await connector.connect();
  const requester = RSocketRequester.wrap(rsocket);

  const knownRoute = "EchoService.echo";
  const unknownRoute = "UnknownService.unknown";

  // this request will fail on the server but the client
  // will NOT be notified as per fireAndForget spec
  await requester
    .route(knownRoute)
    .request(
      AsyncRequestersFactory.fireAndForget(
        "Hello World",
        stringCodec
      )
    );

  Logger.info("fireAndForget done");

  // this request will succeed
  let data = await requester
    .route(knownRoute)
    .request(
      AsyncRequestersFactory.requestResponse(
        "Hello World",
        stringCodec,
        stringCodec
      )
    );

  Logger.info("requestResponse done", data);

  // this request will reject (unknown route)
  try {
    await requester
      // TODO: server responds with TypeError when passing `undefined` here.
      //  server should likely mask input errors
      .route(unknownRoute)
      .request(
        AsyncRequestersFactory.requestResponse(
          "Hello World",
          stringCodec,
          stringCodec
        )
      );
  } catch (e) {
    Logger.error("requestResponse error", e);
  }

  const iterable = requester
    .route(knownRoute)
    .request(AsyncRequestersFactory.requestStream(
      "Hello World",
      stringCodec,
      stringCodec,
      3
    ));

  Logger.info("requestStream to async iterable");

  for await (const value of iterable) {
    Logger.info(`[client] received`, value);
  }

  Logger.info("requestStream to async iterable done");
}

main()
  .then(() => exit())
  .catch((error: Error) => {
    console.error(error);
    exit(1);
  });
