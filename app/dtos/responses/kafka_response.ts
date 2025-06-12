import type { IResponseBufferSerializable } from "../../models/common/interface_buffer_serializable";

class KafkaResponseHeader implements IResponseBufferSerializable {
  constructor(
    public correlationId: number,
    public tagBuffer: number | undefined = undefined
  ) {}

  public getBufferSize() {
    const tagBufferSize = this.tagBuffer !== undefined ? 1 : 0;
    return 4 + tagBufferSize; // 4 bytes for correlationId + 1 byte for tagBuffer if exists
  }

  public toBuffer() {
    const buffer = Buffer.alloc(4);
    buffer.writeUInt32BE(this.correlationId);

    if (this.tagBuffer === undefined) {
      return buffer;
    }
    const tagBuffer = Buffer.alloc(1);
    tagBuffer.writeUInt8(this.tagBuffer);

    return Buffer.concat([buffer, tagBuffer]);
  }
}

export class KafkaResponse {
  header: KafkaResponseHeader;
  body: IResponseBufferSerializable;

  constructor(
    correlationId: number,
    tagBuffer: number | undefined,
    body: IResponseBufferSerializable
  ) {
    this.header = new KafkaResponseHeader(correlationId, tagBuffer);
    this.body = body;
  }

  public toBuffer() {
    const headerBuffer = this.header.toBuffer();
    const bodyBuffer = this.body.toBuffer();

    // Buffer size of `messageSize` value
    const messageSizeBufferSize = 4; // 32 bits = 4 bytes
    const messageSizeBuffer = Buffer.alloc(messageSizeBufferSize);
    const messageSize = bodyBuffer.length + headerBuffer.length;
    console.log(
      `[Response]  header size: ${headerBuffer.length} - body size: ${bodyBuffer.length} - total size: ${messageSize}`
    );
    messageSizeBuffer.writeUInt32BE(messageSize);

    return Buffer.concat([
      messageSizeBuffer,
      this.header.toBuffer(),
      this.body.toBuffer(),
    ]);
  }
}
