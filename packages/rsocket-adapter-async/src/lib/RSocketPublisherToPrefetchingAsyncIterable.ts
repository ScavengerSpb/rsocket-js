import { Cancellable, OnExtensionSubscriber, OnNextSubscriber, OnTerminalSubscriber, Requestable } from "@rsocket/core";
import { Codec } from "@rsocket/messaging";
import IterableSubscriber from "./IterableSubscriber";
import BufferingForwardingSubscriber from "./BufferingForwardingSubscriber";

export default class RSocketPublisherToPrefetchingAsyncIterable<
  T,
  TSignalSender extends Requestable & Cancellable & OnExtensionSubscriber
  > implements AsyncIterable<T> {

  private readonly limit: number;
  protected subscriber: TSignalSender;

  constructor(
    private readonly exchangeFunction: (
      subscriber: OnNextSubscriber & OnTerminalSubscriber & OnExtensionSubscriber,
      n: number
    ) => TSignalSender,
    protected readonly prefetch: number,
    private readonly responseCodec?: Codec<T>
  ) {
    this.limit = prefetch - (prefetch >> 2);
  }

  private asyncIterator(): AsyncIterator<T> {
    const forwardingSubscriber = new BufferingForwardingSubscriber();
    const subscription = this.exchangeFunction(forwardingSubscriber, this.limit);
    const subscribingAsyncIterable = new IterableSubscriber(subscription, this.limit, this.responseCodec);
    forwardingSubscriber.subscribe(subscribingAsyncIterable);
    return subscribingAsyncIterable;
  }

  [Symbol.asyncIterator](): AsyncIterator<T> {
    return this.asyncIterator();
  }

  return(): Promise<IteratorResult<T, void>> {
    this.subscriber.cancel();
    return Promise.resolve(null);
  }
}
