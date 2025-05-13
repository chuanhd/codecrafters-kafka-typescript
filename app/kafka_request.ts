class KafkaRequestHeaderClientID {
  length: number;
  content: string;

  constructor(length: number, content: string) {
    this.length = length;
    this.content = content;
  }

  public debug() {
    console.log(
      `[RequestHeaderClientID] length: ${this.length} - content: ${this.content}`,
    );
  }
}

class KafkafRequestHeader {
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
    tagBuffer: number,
  ) {
    this.apiKey = apiKey;
    this.apiVersion = apiVersion;
    this.correlationId = correlationId;
    this.clientId = new KafkaRequestHeaderClientID(
      clientIdLength,
      clientIdContent,
    );
    this.tagBuffer = tagBuffer;
  }

  public debug() {
    console.log(
      `[RequestHeader] apiKey: ${this.apiKey} apiVersion: ${this.apiVersion} correlationId: ${this.correlationId} clientId: ${this.clientId.debug()} tagBuffer: ${this.tagBuffer}`,
    );
  }
}

class KafkaRequestTopic {
  topicNameLength: number;
  topicName: string;
  tagBuffer: number;

  constructor(topicNameLength: number, topicName: string, tagBuffer: number) {
    this.topicNameLength = topicNameLength;
    this.topicName = topicName;
    this.tagBuffer = tagBuffer;
  }

  public debug() {
    console.log(
      `[RequestTopic] topicNameLength: ${this.topicNameLength} topicName: ${this.topicName} tagBuffer: ${this.tagBuffer}`,
    );
  }
}

export class KafkaRequest {
  messageSize: number;
  header: KafkafRequestHeader;
  topics: KafkaRequestTopic[];
  responsePartitionLimit: number;
  cursor: number;
  tagBuffer: number;

  constructor(
    _messageSize: number,
    _header: KafkafRequestHeader,
    _topics: KafkaRequestTopic[],
    _responsePartitionLimit: number,
    _cursor: number,
    _tagBuffer: number,
  ) {
    this.messageSize = _messageSize;
    this.header = _header;
    this.topics = _topics;
    this.responsePartitionLimit = _responsePartitionLimit;
    this.cursor = _cursor;
    this.tagBuffer = _tagBuffer;
  }

  public static fromBuffer(data: Buffer) {
    console.log(`data's length: ${data.length}`);
    // TOOD: should create a wrapper for Buffer to advance offset automatically
    let currentOffset = 0;
    // Read first 4 bytes to know message size
    const messageSize = data.readUIntBE(currentOffset, 4);
    currentOffset += 4;
    console.log("messageSize: ", messageSize);
    // Next 2 bytes is request_api_key
    const requestApiKey = data.readUint16BE(currentOffset);
    currentOffset += 2;
    console.log("requestApiKey: ", requestApiKey);
    // Next 2 bypes is request_api_version
    const requestApiVersion = data.readUInt16BE(currentOffset);
    currentOffset += 2;
    console.log("requestApiVersion: ", requestApiVersion);
    // Next 4 bypes is correllationId
    const correlationId = data.readUInt32BE(currentOffset);
    currentOffset += 4;
    console.log("correlationId: ", correlationId);

    const clientIdLength = data.readUInt16BE(currentOffset);
    currentOffset += 2;
    console.log("clientIdLength: ", clientIdLength);

    const clientIdContent = data.toString(
      "utf-8",
      currentOffset,
      currentOffset + clientIdLength,
    );
    currentOffset += clientIdLength;
    console.log("clientIdContent: ", clientIdContent);

    const headerTagBuffer = data.readUInt8(currentOffset);
    currentOffset += 1;
    console.log("headerTagBuffer: ", headerTagBuffer);

    const header = new KafkafRequestHeader(
      requestApiKey,
      requestApiVersion,
      correlationId,
      clientIdLength,
      clientIdContent,
      headerTagBuffer,
    );

    // Read content of DescribeTopicPartitionsRequest body
    // Read topic array section
    // Next 1 byte is topic array length
    const topicArrayLength = data.readUInt8(currentOffset);
    currentOffset += 1;
    console.log("topicArrayLength: ", topicArrayLength);

    let readTopicArray: Array<KafkaRequestTopic> = [];
    let remainingTopicToRead = topicArrayLength - 1;
    while (remainingTopicToRead > 0) {
      const topicNameLength = data.readUInt8(currentOffset) - 1;
      currentOffset += 1;
      console.log("topicNameLength: ", topicNameLength);
      const topicName = data.toString(
        "utf-8",
        currentOffset,
        currentOffset + topicNameLength,
      );
      currentOffset += topicNameLength;
      console.log("topicName: ", topicName);
      const topicTagBuffer = data.readUInt8(currentOffset);
      currentOffset += 1;
      console.log("topicTagBuffer: ", topicTagBuffer);

      const topic = new KafkaRequestTopic(
        topicNameLength,
        topicName,
        topicTagBuffer,
      );
      readTopicArray.push(topic);

      // Reduce remainingTopicToRead by 1
      remainingTopicToRead -= 1;
    }

    const responsePartitionLimit = data.readUInt32BE(currentOffset);
    currentOffset += 4;

    const cursor = data.readUInt8(currentOffset);
    currentOffset += 1;

    const tagBuffer = data.readUInt8(currentOffset);
    currentOffset += 1;

    const request = new KafkaRequest(
      messageSize,
      header,
      readTopicArray,
      responsePartitionLimit,
      cursor,
      tagBuffer,
    );

    return request;
  }

  public debug() {
    console.log(`***DEBUG REQUEST***`);
    console.log(`[Request] messageSize: ${this.messageSize}`);
    console.log(`[Request] header: ${this.header.debug()}`);
    for (const topic of this.topics) {
      topic.debug();
    }
    console.log(
      `[Request] responsePartitionLimit: ${this.responsePartitionLimit}`,
    );
    console.log(`[Request] cursor: ${this.cursor}`);
    console.log(`[Request] tagBuffer: ${this.tagBuffer}`);
  }
}
