import { KafkaApiVersionsResponseBody } from "./kafka_api_version_resp.ts";
import { type IResponseBufferSerializable } from "./interface_buffer_serializable.ts";

class KafkaResponseHeader implements IResponseBufferSerializable {
  correlationId: number;
  tagBuffer: number;

  constructor(correlationId: number) {
    this.correlationId = correlationId;
    this.tagBuffer = 0;
  }

  public getBufferSize() {
    return 4 + 1;
  }

  public toBuffer() {
    const buffer = Buffer.alloc(4);
    buffer.writeUInt32BE(this.correlationId);

    const tagBuffer = Buffer.alloc(1);
    tagBuffer.writeUInt8(this.tagBuffer);

    return Buffer.concat([buffer, tagBuffer]);
  }
}

export class KafkaResponse {
  header: KafkaResponseHeader;
  body: IResponseBufferSerializable;

  constructor(correlationId: number, body: IResponseBufferSerializable) {
    this.header = new KafkaResponseHeader(correlationId);
    this.body = body;
  }

  public toBuffer() {
    // Buffer size of `messageSize` value
    const messageSizeBufferSize = 4; // 32 bits = 4 bytes
    const messageSizeBuffer = Buffer.alloc(messageSizeBufferSize);
    const messageSize = this.header.getBufferSize() + this.body.getBufferSize();
    console.log("[Response] mesageSize: ", messageSize);
    messageSizeBuffer.writeUInt32BE(messageSize);

    return Buffer.concat([
      messageSizeBuffer,
      this.header.toBuffer(),
      this.body.toBuffer(),
    ]);
  }
}
