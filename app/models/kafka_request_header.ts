class KafkaRequestHeaderClientID {
  length: number;
  content: string;

  constructor(length: number, content: string) {
    this.length = length;
    this.content = content;
  }

  public debug() {
    return `[RequestHeaderClientID] length: ${this.length} - content: ${this.content}`;
  }
}

export class KafkaRequestHeader {
  apiKey: number;
  apiVersion: number;
  correlationId: number;
  clientId: KafkaRequestHeaderClientID;
  tagBuffer: number;

  constructor(
    apiKey: number,
    apiVersion: number,
    correlationId: number,
    clientIdLength: number,
    clientIdContent: string,
    tagBuffer: number
  ) {
    this.apiKey = apiKey;
    this.apiVersion = apiVersion;
    this.correlationId = correlationId;
    this.clientId = new KafkaRequestHeaderClientID(
      clientIdLength,
      clientIdContent
    );
    this.tagBuffer = tagBuffer;
  }

  public static fromBuffer(buffer: Buffer): KafkaRequestHeader {
    let currentOffset = 0;
    // Next 2 bytes is request_api_key
    const requestApiKey = buffer.readUint16BE(currentOffset);
    currentOffset += 2;
    console.log("requestApiKey: ", requestApiKey);
    // Next 2 bytes is request_api_version
    const requestApiVersion = buffer.readUInt16BE(currentOffset);
    currentOffset += 2;
    console.log("requestApiVersion: ", requestApiVersion);
    // Next 4 bytes is correllationId
    const correlationId = buffer.readUInt32BE(currentOffset);
    currentOffset += 4;
    console.log("correlationId: ", correlationId);

    const clientIdLength = buffer.readUInt16BE(currentOffset);
    currentOffset += 2;
    console.log("clientIdLength: ", clientIdLength);

    const clientIdContent = buffer.toString(
      "utf-8",
      currentOffset,
      currentOffset + clientIdLength
    );
    currentOffset += clientIdLength;
    console.log("clientIdContent: ", clientIdContent);

    const headerTagBuffer = buffer.readUInt8(currentOffset);
    currentOffset += 1;
    console.log("headerTagBuffer: ", headerTagBuffer);

    const header = new KafkaRequestHeader(
      requestApiKey,
      requestApiVersion,
      correlationId,
      clientIdLength,
      clientIdContent,
      headerTagBuffer
    );

    return header;
  }

  public debugString() {
    return `[RequestHeader] apiKey: ${this.apiKey} apiVersion: ${
      this.apiVersion
    } correlationId: ${
      this.correlationId
    } clientId: ${this.clientId.debug()} tagBuffer: ${this.tagBuffer}`;
  }
}
