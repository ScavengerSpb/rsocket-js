import { Codec } from "@rsocket/messaging";
import { RSocket } from "@rsocket/core";
import { WellKnownMimeType } from "@rsocket/composite-metadata";
import { asapScheduler, from } from "rxjs";
import { RxRequestersFactory } from "@rsocket/rxjs";
import { eachValueFrom } from "rxjs-for-await";

export default function requestChannel<TData, RData>(
  datas: AsyncIterable<TData>,
  inputCodec: Codec<TData>,
  outputCodec: Codec<RData>,
  prefetch: number = 256
): (
  rsocket: RSocket,
  metadata: Map<string | number | WellKnownMimeType, Buffer>
) => AsyncIterable<any> {
  return (
    rsocket: RSocket,
    metadata: Map<string | number | WellKnownMimeType, Buffer>
  ) => {
    const $requesterObs = from(datas);

    // TODO: is using `@rsocket/rxjs` as intermediary adapter a bad idea?
    //  - what is bundle size consequences of relying on `@rsocket/rxjs`?
    //  - what is bundle size consequences of relying on `rxjs` and `rxjs-for-await`
    const $responderObs = RxRequestersFactory.requestChannel(
      $requesterObs,
      inputCodec,
      outputCodec,
      prefetch,
      asapScheduler
    )(rsocket, metadata);

    return eachValueFrom($responderObs);
  };
}
