import type { BufferEncode } from "../../models/common/interface_encode.ts";
import type { KafkaTopicPartitionItemResp } from "./kafka_topic_partition_item_resp.ts";

const TagBufferBufferSize = 1; // 1 byte
const CorrelationIdBufferSize = 4; // 4 bytes
const CursorBufferSize = 1; // 1 byte
const ThrottleTimeBufferSize = 4; //  4 bytes
const TopicArrayLengthBufferSize = 1; // 1 byte
const NextCursorBufferSize = 1;
const TopicNameLengthBufferSize = 1;
const ErrorCodeBufferSize = 2; // 2 bytes
const TopicIdBufferSize = 16; // 16 bytes
const IsInternalBufferSize = 1; // 1 byte
const PartitionsArrayLengthBufferSize = 1; // 1 byte
const TopicAuthorizedOpBufferSize = 4; // 4 byte

export class KafkaDescribeTopicPartitionsHeader
  implements BufferEncode
{
  correlationId: number;
  tagBuffer: number;

  constructor(correlationId: number, tagBuffer: number) {
    this.correlationId = correlationId;
    this.tagBuffer = tagBuffer;
  }
  getBufferSize(): number {
    return TagBufferBufferSize + CorrelationIdBufferSize;
  }

  encodeTo(): Buffer {
    const correlationIdBuffer = Buffer.alloc(CorrelationIdBufferSize);
    correlationIdBuffer.writeUInt32BE(this.correlationId);

    const tagBufferBuffer = Buffer.alloc(TagBufferBufferSize);
    tagBufferBuffer.writeUInt8(this.tagBuffer);

    return Buffer.concat([correlationIdBuffer, tagBufferBuffer]);
  }
}

export class KafkaDescribeTopicPartitionsTopicItem
  implements BufferEncode
{
  errorCode: number;
  topicNameLength: number;
  topicName: string;
  topicId: Buffer;
  isInternal: boolean;
  partitionsArrayLength: number;
  partitions: KafkaTopicPartitionItemResp[];
  topicAuthorizedOp: number;
  tagBuffer: number;

  constructor(
    errorCode: number,
    topicName: string,
    topicId: Buffer,
    isInternal: boolean,
    partitions: KafkaTopicPartitionItemResp[],
    topicAuthorizedOp: number,
    tagBuffer: number
  ) {
    this.errorCode = errorCode;
    this.topicNameLength = topicName.length;
    this.topicName = topicName;
    this.topicId = topicId;
    this.isInternal = isInternal;
    this.partitions = partitions;
    this.partitionsArrayLength = partitions.length + 1; // +1 for the length byte
    this.topicAuthorizedOp = topicAuthorizedOp;
    this.tagBuffer = tagBuffer;
  }

  // getBufferSize(): number {
  //   const topicNameBufferSize = this.topicNameLength;
  //   return (
  //     ErrorCodeBufferSize +
  //     TopicNameLengthBufferSize +
  //     topicNameBufferSize +
  //     TopicIdBufferSize +
  //     IsInternalBufferSize +
  //     PartitionsArrayLengthBufferSize +
  //     TopicAuthorizedOpBufferSize +
  //     TagBufferBufferSize
  //   );
  // }

  encodeTo(): Buffer {
    const errorCodeBuffer = Buffer.alloc(ErrorCodeBufferSize);
    errorCodeBuffer.writeUInt16BE(this.errorCode);

    const topicNameLengthBuffer = Buffer.alloc(TopicNameLengthBufferSize);
    topicNameLengthBuffer.writeUInt8(this.topicNameLength + 1);

    const topicNameBuffer = Buffer.alloc(this.topicNameLength);
    topicNameBuffer.write(this.topicName, "utf-8");

    const isInternalBuffer = Buffer.alloc(IsInternalBufferSize);
    isInternalBuffer.writeUInt8(this.isInternal ? 1 : 0);

    const partitionsArrayLengthBuffer = Buffer.alloc(
      PartitionsArrayLengthBufferSize
    );
    partitionsArrayLengthBuffer.writeUInt8(this.partitionsArrayLength);

    // Serialize partitions
    const partitionsBuffers = this.partitions.map((partition) =>
      partition.encodeTo()
    );
    const partitionsBuffer = Buffer.concat(partitionsBuffers);
    console.log(`partitionsBuffer size: ${partitionsBuffer.length}`);

    const topicAuthorizedOpBuffer = Buffer.alloc(TopicAuthorizedOpBufferSize);
    topicAuthorizedOpBuffer.writeUInt32BE(this.topicAuthorizedOp);

    const tagBufferBuffer = Buffer.alloc(TagBufferBufferSize);
    tagBufferBuffer.writeUInt8(this.tagBuffer);

    return Buffer.concat([
      errorCodeBuffer,
      topicNameLengthBuffer,
      topicNameBuffer,
      this.topicId,
      isInternalBuffer,
      partitionsArrayLengthBuffer,
      partitionsBuffer,
      topicAuthorizedOpBuffer,
      tagBufferBuffer,
    ]);
  }
}

export class KafkaDescribeTopicPartitionsRespBody
  implements BufferEncode
{
  throttleTime: number;
  topicsLength: number;
  topics: Array<KafkaDescribeTopicPartitionsTopicItem>;
  nextCursor: number;
  tagBuffer: number;

  constructor(
    throttleTime: number,
    nextCursor: number,
    tagBuffer: number,
    topics: Array<KafkaDescribeTopicPartitionsTopicItem>
  ) {
    this.topics = topics;
    this.topicsLength = this.topics.length + 1;
    this.throttleTime = throttleTime;
    this.nextCursor = nextCursor;
    this.tagBuffer = tagBuffer;
  }

  // getBufferSize(): number {
  //   const topicsBufferSize = this.topics.reduce((prev, curVal) => {
  //     return prev + curVal.getBufferSize();
  //   }, 0);

  //   return (
  //     ThrottleTimeBufferSize +
  //     TopicArrayLengthBufferSize +
  //     topicsBufferSize +
  //     NextCursorBufferSize +
  //     TagBufferBufferSize
  //   );
  // }

  encodeTo(): Buffer {
    const throttleTimeBuffer = Buffer.alloc(ThrottleTimeBufferSize);
    throttleTimeBuffer.writeUInt8(this.throttleTime);

    const topicArrayLengthBuffer = Buffer.alloc(TopicArrayLengthBufferSize);
    topicArrayLengthBuffer.writeUInt8(this.topicsLength);

    const topicBuffers = this.topics.map((e) => e.encodeTo());

    const cursorBuffer = Buffer.alloc(CursorBufferSize);
    cursorBuffer.writeUInt8(255);

    const tagBufferBuffer = Buffer.alloc(TagBufferBufferSize);
    tagBufferBuffer.writeUInt8(this.tagBuffer);

    return Buffer.concat([
      throttleTimeBuffer,
      topicArrayLengthBuffer,
      ...topicBuffers,
      cursorBuffer,
      tagBufferBuffer,
    ]);
  }
}
