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

export class KafkaDescribeTopicPartitionsRequest {
  topics: KafkaRequestTopic[];
  responsePartitionLimit: number;
  cursor: number;
  tagBuffer: number;

  constructor(
    topics: KafkaRequestTopic[],
    responsePartitionLimit: number,
    cursor: number,
    tagBuffer: number,
  ) {
    this.topics = topics;
    this.responsePartitionLimit = responsePartitionLimit;
    this.cursor = cursor;
    this.tagBuffer = tagBuffer;
  }

  public static fromBuffer(data: Buffer, currentOffset: number) {
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

    const body = new KafkaDescribeTopicPartitionsRequest(
      readTopicArray,
      responsePartitionLimit,
      cursor,
      tagBuffer,
    );

    return body;
  }
}
