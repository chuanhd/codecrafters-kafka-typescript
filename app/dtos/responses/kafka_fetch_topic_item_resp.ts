import type { IResponseBufferSerializable } from "../../interface_buffer_serializable";
import { writeVarInt } from "../../utils/utils";
import type { KafkaFetchTopicPartitionItemResp } from "./kafka_fetch_topic_partition_item_resp";

export class KafkaFetchTopicItemResp implements IResponseBufferSerializable {
  constructor(
    public topicId: Buffer,
    public partitions: KafkaFetchTopicPartitionItemResp[]
  ) {}

  toBuffer(): Buffer {
    const numPartitionsBuffer = writeVarInt(this.partitions.length + 1);
    const partitionsBuffer = Buffer.concat(
      this.partitions.map((partition) => partition.toBuffer())
    );

    const tagBufferBuffer = writeVarInt(0); // Placeholder for tag buffer

    console.log(
      `[KafkaFetchTopicItemResp] topicId size: ${this.topicId.length}, numPartitionsBuffer size: ${numPartitionsBuffer.length}, partitionsBuffer size: ${partitionsBuffer.length}, tagBufferBuffer size: ${tagBufferBuffer.length}`
    );
    
    return Buffer.concat([this.topicId, numPartitionsBuffer, partitionsBuffer, tagBufferBuffer]);
  }
}
