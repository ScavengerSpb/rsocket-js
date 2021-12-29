import { encodeCompositeMetadata, WellKnownMimeType } from "@rsocket/composite-metadata";
import { Payload, RSocket } from "@rsocket/core";
import { Codec } from "@rsocket/messaging";
import RSocketPublisherToPrefetchingAsyncIterable from "./lib/RSocketPublisherToPrefetchingAsyncIterable";

type VoidResolvingPromiseExecutor = (
  rsocket: RSocket,
  metadata: Map<string | number | WellKnownMimeType, Buffer>
) => Promise<void>;

type PayloadResolvingPromiseExecutor<RData> = (
  rsocket: RSocket,
  metadata: Map<string | number | WellKnownMimeType, Buffer>
) => Promise<RData>;

export function fireAndForget<TData>(
  data: TData,
  inputCodec: Codec<TData>
): VoidResolvingPromiseExecutor {
  return (
    rsocket: RSocket,
    metadata: Map<string | number | WellKnownMimeType, Buffer>
  ) => {
    const payload = {
      // TODO: should inputCodec be responsible for handling empty data?
      // TODO: Buffer needs to be injectable to support browsers
      data: data ? inputCodec.encode(data) : Buffer.allocUnsafe(0),
      metadata: encodeCompositeMetadata(metadata)
    };
    return new Promise((resolve, reject) => {
      rsocket.fireAndForget(payload, {
        onComplete(): void {
          resolve();
        },
        onError(error: Error): void {
          reject(error);
        }
      });
    });
  };
}

export function requestResponse<TData, RData>(
  data: TData,
  inputCodec: Codec<TData>,
  outputCodec: Codec<RData>
): PayloadResolvingPromiseExecutor<RData> {
  return (
    rsocket: RSocket,
    metadata: Map<string | number | WellKnownMimeType, Buffer>
  ) => {
    const payload = {
      // TODO: should inputCodec be responsible for handling empty data?
      // TODO: Buffer may need to be injectable to support browsers
      data: data ? inputCodec.encode(data) : Buffer.allocUnsafe(0),
      metadata: encodeCompositeMetadata(metadata)
    };
    return new Promise((resolve, reject) => {
      rsocket.requestResponse(payload, {
        onExtension(extendedType: number,
                    content: Buffer | null | undefined, canBeIgnored: boolean): void {
        },
        onNext(payload: Payload, isComplete: boolean): void {
          // TODO: data loss by only resolving with `payload.data`?
          //  Should we preserve metadata in the resolved value?
          resolve(outputCodec.decode(payload.data));
        },
        onComplete(): void {
          resolve(null);
        },
        onError(error: Error): void {
          reject(error);
        }
      });
    });
  };
}

export function requestStream<TData, RData>(
  data: TData,
  inputCodec: Codec<TData>,
  outputCode: Codec<RData>,
  prefetch: number = 256
): (
  rsocket: RSocket,
  metadata: Map<string | number | WellKnownMimeType, Buffer>
) => AsyncIterable<any> {

  return function (
    rsocket: RSocket,
    metadata: Map<string | number | WellKnownMimeType, Buffer>
  ) {

    const exchangeFunction = (subscriber, initialRequestN) => {
      const payload = {
        // TODO: should inputCodec be responsible for handling empty data?
        // TODO: Buffer needs to be injectable to support browsers
        data: data ? inputCodec.encode(data) : Buffer.allocUnsafe(0),
        metadata: encodeCompositeMetadata(metadata)
      };
      return rsocket.requestStream(payload, initialRequestN, subscriber);
    };

    return new RSocketPublisherToPrefetchingAsyncIterable(
      exchangeFunction,
      prefetch,
      outputCode);
  };
}
