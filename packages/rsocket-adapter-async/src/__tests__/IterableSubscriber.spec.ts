import IterableSubscriber from "../lib/IterableSubscriber";
import { mock } from "jest-mock-extended";
import { Requestable } from "@rsocket/core";
import { Codec } from "@rsocket/messaging";

jest.useFakeTimers();

class StringCodec implements Codec<string> {
  readonly mimeType: string = "text/plain";

  decode(buffer: Buffer): string {
    return buffer.toString();
  }

  encode(entity: string): Buffer {
    return Buffer.from(entity);
  }
}

describe("IterableSubscriber", function () {
  it("iterates over emitted values", async function () {
    let subscriber;
    const subscription = mock<Requestable>({
      request(requestN: number) {
        for (let i = 0; i < requestN; i++) {
          setTimeout(() => {
            subscriber.onNext({
              data: Buffer.from(`${i}`),
              metadata: undefined
            }, i === requestN - 1);
          });
        }
      }
    });
    const requestSpy = jest.spyOn(subscription, "request");

    const initialRequestN = 3;
    subscriber = new IterableSubscriber(subscription, initialRequestN * 2, new StringCodec());
    subscription.request(initialRequestN);

    jest.runAllTimers();

    const values = [];
    for await (const value of subscriber) {
      jest.runAllTimers();
      values.push(value);
    }

    expect(values).toStrictEqual(["0", "1", "2"]);
    expect(requestSpy).toBeCalledTimes(1);
  });

  it("iterates over emitted values until onComplete", async function () {
    let subscriber;
    const subscription = mock<Requestable>({
      request(requestN: number) {
        for (let i = 0; i < requestN; i++) {
          setTimeout(() => {
            if (i === requestN - 1) {
              subscriber.onComplete();
            } else {
              subscriber.onNext({
                data: Buffer.from(`${i}`),
                metadata: undefined
              }, false);
            }
          });
        }
      }
    });
    const requestSpy = jest.spyOn(subscription, "request");

    const initialRequestN = 3;
    subscriber = new IterableSubscriber(subscription, initialRequestN * 2, new StringCodec());
    subscription.request(initialRequestN);

    jest.runAllTimers();

    const values = [];
    for await (const value of subscriber) {
      jest.runAllTimers();
      values.push(value);
    }

    expect(values).toStrictEqual(["0", "1"]);
    expect(requestSpy).toBeCalledTimes(1);
  });
});
