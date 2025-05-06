export class KafkaResponse {
  correlationId: number; // 32 bits unsigned integer
  errorCode: number; // 16 bits unsigned integer

  constructor(correlationId: number, errorCode: number) {
    this.correlationId = correlationId;
    this.errorCode = errorCode;
  }

  public toBuffer() {
    const correlationIdSize = 4; // 32 bits = 4 bytes
    const correlationIdBuffer = Buffer.alloc(correlationIdSize);
    correlationIdBuffer.writeUInt32BE(this.correlationId);

    const errorCodeSize = 2; // 16 bits = 2 bytes
    const errorCodeBuffer = Buffer.alloc(errorCodeSize);
    errorCodeBuffer.writeUInt16BE(this.errorCode);

    // Buffer size of `messageSize` value
    const messageSizeBufferSize = 4; // 32 bits = 4 bytes
    const messageSizeBuffer = Buffer.alloc(messageSizeBufferSize);
    messageSizeBuffer.writeUInt32BE(correlationIdSize + errorCodeSize);

    return Buffer.concat([
      messageSizeBuffer,
      correlationIdBuffer,
      errorCodeBuffer,
    ]);
  }
}
