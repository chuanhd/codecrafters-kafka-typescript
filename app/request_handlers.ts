import { KafkaRequestHeader } from "./dtos/requests/kafka_request_header";
import type IRequestHandler from "./models/request_handlers/interface_request_handler";

export class KafkaRequestHandlerCenter {
  private handler: IRequestHandler[];

  constructor() {
    this.handler = [];
  }

  public registerHandler(handler: IRequestHandler): KafkaRequestHandlerCenter {
    this.handler.push(handler);

    return this;
  }

  public handleRequest(request: Buffer): Buffer {
    const header = KafkaRequestHeader.fromBuffer(request.subarray(4));
    const apiKey = header.apiKey;

    const handler = this.handler.find((h) => h.apiKey === apiKey);
    if (!handler) {
      throw new Error(`No handler found for API key: ${apiKey}`);
    }

    return handler.handleRequest(header, request).toBuffer();
  }
}