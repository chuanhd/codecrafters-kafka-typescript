import { readVarInt } from "../../utils/utils";
import { KafkaRequestHeader } from "./kafka_request_header";

export class KafkaFetchRequestTopicItem {
  private bufferSize: number = 0;
  public getBufferSize(): number {
    return this.bufferSize;
  }

  constructor(public topicId: Buffer, public partitions: Buffer[]) {}

  static fromBuffer(buffer: Buffer): KafkaFetchRequestTopicItem {
    let currentOffset = 0;
    const topicId = buffer.subarray(currentOffset, currentOffset + 16);
    currentOffset += 16;
    console.log(
      `topicId: ${topicId.toString("hex")} at offset ${currentOffset}`
    );

    const { value: numOfPartitionsPlusOne, length: numOfPartitionsBufferLength } =
      readVarInt(buffer.subarray(currentOffset));
    currentOffset += numOfPartitionsBufferLength;
    const numOfPartitions = numOfPartitionsPlusOne - 1;
    console.log(
      `numOfPartitions: ${numOfPartitions} - numOfPartitionsBufferLength: ${numOfPartitionsBufferLength}`
    );

    let partitions = [];
    for (let i = 0; i < numOfPartitions; i++) {
      const partitionId = buffer.subarray(currentOffset, currentOffset + 4);
      console.log(
        `partitionId: ${partitionId.toString("hex")} at offset ${currentOffset}`
      );
      currentOffset += 4;
      partitions.push(partitionId);
    }

    const topicItem = new KafkaFetchRequestTopicItem(topicId, partitions);
    topicItem.bufferSize =
      16 + numOfPartitionsBufferLength + numOfPartitions * 4;

    return topicItem;
  }
}

export class KafkaFetchRequest {
  constructor(
    public messageSize: number,
    public header: KafkaRequestHeader,
    public topics: KafkaFetchRequestTopicItem[]
  ) {}

  static fromBuffer(buffer: Buffer, header: KafkaRequestHeader): KafkaFetchRequest {
    let currentOffset = 0;
    console.log("Reading KafkaFetchRequest from buffer:", buffer.length);

    const messageSize = buffer.readUInt32BE(currentOffset);
    console.log(`messageSize: ${messageSize} at offset ${currentOffset}`);
    currentOffset += 4;

    // Advance currentOffset by header size
    currentOffset += header.getBufferSize();

    const maxWaitTime = buffer.readUInt32BE(currentOffset);
    currentOffset += 4;
    console.log("maxWaitTime: ", maxWaitTime);
    const minBytes = buffer.readUInt32BE(currentOffset);
    currentOffset += 4;
    console.log("minBytes: ", minBytes);
    const maxBytes = buffer.readUInt32BE(currentOffset);
    currentOffset += 4;
    console.log("maxBytes: ", maxBytes);
    const isolationLevel = buffer.readUInt8(currentOffset);
    currentOffset += 1;
    console.log("isolationLevel: ", isolationLevel);
    const sessionId = buffer.readUInt32BE(currentOffset);
    currentOffset += 4;
    console.log("sessionId: ", sessionId);
    const sessionEpoch = buffer.readUInt32BE(currentOffset);
    currentOffset += 4;
    console.log("sessionEpoch: ", sessionEpoch);
    console.log(
      `currentOffset after reading til sessionEpoch: ${currentOffset} - data length: ${buffer.length}`
    );

    const { value: numOfTopicsPlusOne, length: numOfTopicsBufferLength } = readVarInt(
      buffer.subarray(currentOffset)
    );
    currentOffset += numOfTopicsBufferLength;
    const numOfTopics = numOfTopicsPlusOne - 1;
    console.log(
      `numOfTopics: ${numOfTopics} - numOfTopicsBufferLength: ${numOfTopicsBufferLength}`
    );

    let topics = [];
    for (let i = 0; i < numOfTopics; i++) {
      const topicItem = KafkaFetchRequestTopicItem.fromBuffer(
        buffer.subarray(currentOffset)
      );
      currentOffset += topicItem.getBufferSize();
      topics.push(topicItem);
    }

    console.log(
      `currentOffset after reading request: ${currentOffset} - data length: ${buffer.length}`
    );

    return new KafkaFetchRequest(messageSize, header, topics);
  }
}
