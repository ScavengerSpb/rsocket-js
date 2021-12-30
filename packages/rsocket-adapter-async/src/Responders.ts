import { Codec } from "@rsocket/messaging";
import {
  Cancellable,
  FrameTypes,
  OnExtensionSubscriber,
  OnNextSubscriber,
  OnTerminalSubscriber,
  Payload, Requestable
} from "@rsocket/core";

export function fireAndForget<IN>(
  handler: (data: IN) => Promise<void>,
  codec: Codec<IN>
): ((
  p: Payload,
  s: OnTerminalSubscriber
) => Cancellable) & { requestType: FrameTypes.REQUEST_FNF; } {
  return Object.assign<((
    p: Payload,
    s: OnTerminalSubscriber
  ) => Cancellable), { requestType: FrameTypes.REQUEST_FNF; }>(
    (p: Payload, s: OnTerminalSubscriber) => {
      handler(codec.decode(p.data));
      return {
        cancel() {
        }
      };
    },
    { requestType: FrameTypes.REQUEST_FNF }
  );
}

export function requestResponse<IN, OUT>(
  handler: (data: IN) => Promise<OUT>,
  codecs: {
    inputCodec: IN extends void | null | undefined ? undefined : Codec<IN>;
    outputCodec: OUT extends void | null | undefined ? undefined : Codec<OUT>;
  }
): ((
  p: Payload,
  s: OnTerminalSubscriber & OnNextSubscriber & OnExtensionSubscriber
) => Cancellable & OnExtensionSubscriber) & {
  requestType: FrameTypes.REQUEST_RESPONSE;
} {

  const handle = (p: Payload, s: OnTerminalSubscriber & OnNextSubscriber & OnExtensionSubscriber) => {
    let cancelled = false;
    handler(codecs.inputCodec.decode(p.data))
      .then((value) => {
        if (cancelled) return;
        s.onNext({ data: codecs.outputCodec.encode(value) }, true);
      })
      .catch((e) => {
        if (cancelled) return;
        s.onError(e);
      });
    return {
      cancel() {
        cancelled = true;
      },
      onExtension() {
      }
    };
  };

  return Object.assign<(
    p: Payload,
    s: OnTerminalSubscriber & OnNextSubscriber & OnExtensionSubscriber
  ) => Cancellable & OnExtensionSubscriber, { requestType: FrameTypes.REQUEST_RESPONSE; }>(
    handle,
    {
      requestType: FrameTypes.REQUEST_RESPONSE
    }
  );
}

export function requestStream<IN, OUT>(
  handler: (data: IN) => AsyncIterable<OUT>,
  codecs: {
    inputCodec: IN extends void | null | undefined ? undefined : Codec<IN>;
    outputCodec: OUT extends void | null | undefined ? undefined : Codec<OUT>;
  }
): ((
  p: Payload,
  r: number,
  s: OnTerminalSubscriber & OnNextSubscriber & OnExtensionSubscriber
) => Cancellable & OnExtensionSubscriber & Requestable) & {
  requestType: FrameTypes.REQUEST_STREAM;
} {

  const handle = (p: Payload, r: number, s: OnTerminalSubscriber & OnNextSubscriber & OnExtensionSubscriber) => {
    let cancelled = false;
    let produced = 0;
    let requested = r;

    (async () => {
      try {
        const iterable = handler(codecs.inputCodec.decode(p.data));
        for await (const value of iterable) {
          if (cancelled) {
            break;
          }
          produced++;
          const isComplete = produced === requested;
          s.onNext({ data: codecs.outputCodec.encode(value) }, isComplete);
          if (produced === requested) {
            break;
          }
        }
      } catch (e) {
        if (cancelled) return;
        s.onError(e);
      }
    })();

    return {
      cancel() {
        cancelled = true;
      },
      request(n) {
        requested += n;
      },
      onExtension() {
      }
    };
  };

  return Object.assign<(
    p: Payload,
    r: number,
    s: OnTerminalSubscriber & OnNextSubscriber & OnExtensionSubscriber
  ) => Cancellable & OnExtensionSubscriber & Requestable, { requestType: FrameTypes.REQUEST_STREAM; }>(
    handle,
    {
      requestType: FrameTypes.REQUEST_STREAM
    }
  );
}
