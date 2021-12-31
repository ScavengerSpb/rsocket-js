// jest.useFakeTimers();

// import{ AsyncRespondersFactory } from "../../index";
import { Codec } from "@rsocket/messaging";
import { mock } from "jest-mock-extended";
import { OnExtensionSubscriber, OnNextSubscriber, OnTerminalSubscriber, RSocket } from "@rsocket/core";
import { requestResponse } from "../../lib/requesters";

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

describe("AsyncRespondersFactory", function() {
  describe("requestResponse", function() {
    it("Emits value resolved from handlers resolved promise", function() {
      const mockRSocket = mock<RSocket>();
      const mockSubscriber = mock<OnNextSubscriber & OnTerminalSubscriber & OnExtensionSubscriber>();

      const handler = requestResponse(
        "hello world",
        codecs.inputCodec,
        codecs.outputCodec);

      handler(mockRSocket, new Map());

      expect(mockRSocket.requestResponse).toBeCalledWith("howdy ho neighbor")
    });
  });
});
