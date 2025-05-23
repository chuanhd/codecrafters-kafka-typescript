import { KafkaTopicPartitionItemReq } from "./kafka_topic_partition_item_req";

export class KafkaRequestTopicPartition {
  topics: KafkaTopicPartitionItemReq[];
  responsePartitionLimit: number;
  cursor: number;
  tagBuffer: number;

  constructor(
    topics: KafkaTopicPartitionItemReq[],
    responsePartitionLimit: number,
    cursor: number,
    tagBuffer: number
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

    let readTopicArray: Array<KafkaTopicPartitionItemReq> = [];
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

      const topic = new KafkaTopicPartitionItemReq(
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

    const body = new KafkaRequestTopicPartition(
      readTopicArray,
      responsePartitionLimit,
      cursor,
      tagBuffer
    );

    return body;
  }
}
