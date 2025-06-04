import { KafkaRequestHeader } from "./kafka_request_header";

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
      `[RequestTopic] topicNameLength: ${this.topicNameLength} topicName: ${this.topicName} tagBuffer: ${this.tagBuffer}`
    );
  }
}

export class KafkaDescribePartitionRequest {
  messageSize: number;
  header: KafkaRequestHeader;
  topics: KafkaRequestTopic[];
  responsePartitionLimit: number;
  cursor: number;
  tagBuffer: number;

  constructor(
    _messageSize: number,
    _header: KafkaRequestHeader,
    _topics: KafkaRequestTopic[],
    _responsePartitionLimit: number,
    _cursor: number,
    _tagBuffer: number
  ) {
    this.messageSize = _messageSize;
    this.header = _header;
    this.topics = _topics;
    this.responsePartitionLimit = _responsePartitionLimit;
    this.cursor = _cursor;
    this.tagBuffer = _tagBuffer;
  }

  public static fromBuffer(data: Buffer, header: KafkaRequestHeader): KafkaDescribePartitionRequest {
    let currentOffset = 0;

    const messageSize = data.readUInt32BE(currentOffset);
    console.log(`messageSize: ${messageSize} at offset ${currentOffset}`);
    currentOffset += 4;

    // Advance currentOffset by header size
    currentOffset += header.getBufferSize();

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
        currentOffset + topicNameLength
      );
      currentOffset += topicNameLength;
      console.log("topicName: ", topicName);
      const topicTagBuffer = data.readUInt8(currentOffset);
      currentOffset += 1;
      console.log("topicTagBuffer: ", topicTagBuffer);

      const topic = new KafkaRequestTopic(
        topicNameLength,
        topicName,
        topicTagBuffer
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

    const request = new KafkaDescribePartitionRequest(
      messageSize,
      header,
      readTopicArray,
      responsePartitionLimit,
      cursor,
      tagBuffer
    );

    return request;
  }

  public debug() {
    console.log(`***DEBUG REQUEST***`);
    console.log(`[Request] messageSize: ${this.messageSize}`);
    console.log(`[Request] header: ${this.header.debugString()}`);
    for (const topic of this.topics) {
      topic.debug();
    }
    console.log(
      `[Request] responsePartitionLimit: ${this.responsePartitionLimit}`
    );
    console.log(`[Request] cursor: ${this.cursor}`);
    console.log(`[Request] tagBuffer: ${this.tagBuffer}`);
    console.log(`***End of DEBUG REQUEST***`);
  }
}
