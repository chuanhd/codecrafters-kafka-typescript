import type { IResponseBufferSerializable } from "../interface_buffer_serializable";
import { writeVarInt } from "../utils/utils";

export class KafkaFetchResponseBody implements IResponseBufferSerializable {
  constructor(
    public throttleTime: number,
    public errorCode: number,
    public sessionId: number
  ) {}

  toBuffer(): Buffer {
    const throttleTimeBuffer = Buffer.alloc(4);
    throttleTimeBuffer.writeUInt32BE(this.throttleTime);
    const errorCodeBuffer = Buffer.alloc(2);
    errorCodeBuffer.writeUInt16BE(this.errorCode);
    const sessionIdBuffer = Buffer.alloc(4);
    sessionIdBuffer.writeUInt32BE(this.sessionId);

    const numResponsesBuffer = writeVarInt(0);
    const tagBufferBuffer = Buffer.alloc(1);
    tagBufferBuffer.writeInt8(0);

    return Buffer.concat([
      throttleTimeBuffer,
      errorCodeBuffer,
      sessionIdBuffer,
      numResponsesBuffer,
      tagBufferBuffer,
    ]);
  }
}
