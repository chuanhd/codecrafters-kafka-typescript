import type { BufferEncode } from "../../models/common/interface_encode";
import { UVarIntField } from "../../models/fields/atom_field";
import type { KafkaFetchTopicPartitionItemResp } from "./kafka_fetch_topic_partition_item_resp";

export class KafkaFetchTopicItemResp implements BufferEncode {
  public numPartitions: UVarIntField;
  public tagBuffer: UVarIntField;

  constructor(
    public topicId: Buffer,
    public partitions: KafkaFetchTopicPartitionItemResp[]
  ) {
    this.numPartitions = new UVarIntField(this.partitions.length + 1);
    this.tagBuffer = new UVarIntField(0); // Placeholder for tag buffer
  }

  encodeTo(): Buffer {
    const numPartitionsBuffer = this.numPartitions.encode();
    const partitionsBuffer = Buffer.concat(
      this.partitions.map((partition) => partition.encodeTo())
    );

    const tagBufferBuffer = this.tagBuffer.encode(); // Placeholder for tag buffer
    
    return Buffer.concat([this.topicId, numPartitionsBuffer, partitionsBuffer, tagBufferBuffer]);
  }
}
