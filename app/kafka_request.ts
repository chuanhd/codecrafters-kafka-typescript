export class KafkaRequest {
  messageSize: number;
  requestApiKey: number;
  requestApiVersion: number;
  correllationId: number;

  constructor(
    _messageSize: number,
    _requestApiKey: number,
    _requestApiVersion: number,
    _correllationId: number,
  ) {
    this.messageSize = _messageSize;
    this.requestApiKey = _requestApiKey;
    this.requestApiVersion = _requestApiVersion;
    this.correllationId = _correllationId;
  }

  public static fromBuffer(data: Buffer) {
    // TOOD: should create a wrapper for Buffer to advance offset automatically
    let currentOffset = 0;
    // Read first 4 bytes to know message size
    const messageSize = data.readUIntBE(currentOffset, 4);
    currentOffset += 4;
    // Next 2 bytes is request_api_key
    const requestApiKey = data.readUint16BE(currentOffset);
    currentOffset += 2;
    // Next 2 bypes is request_api_version
    const requestApiVersion = data.readUInt16BE(currentOffset);
    currentOffset += 2;
    // Next 4 bypes is correllationId
    const correlationId = data.readUInt32BE(currentOffset);
    currentOffset += 4;

    const request = new KafkaRequest(
      messageSize,
      requestApiKey,
      requestApiVersion,
      correlationId,
    );

    return request;
  }

  public debug() {
    console.log(
      `messageSize: ${this.messageSize} - requestApiKey: ${this.requestApiKey} - requestApiVersion: ${this.requestApiVersion} - correlationId: ${this.correllationId}`,
    );
  }
}
