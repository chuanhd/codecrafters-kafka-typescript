import { KafkaClusterMetadataTopicRecord } from './../../models/metadata_log_file/kafka_cluster_metadata_topic_record';
import type { IResponseBufferSerializable } from "../../interface_buffer_serializable";
import { writeVarInt } from "../../utils/utils";
import { ErrorCode } from '../../consts';
import { KafkaPartitionLogFile } from '../../models/metadata_partition_log_file/kafka_partition_log_file';
import { KafkaPartitionRecordBatch } from '../../models/metadata_partition_log_file/kafka_partition_record_batch';

export class KafkaFetchTopicPartitionItemResp
  implements IResponseBufferSerializable
{
  constructor(public partitionIndex: number, public topic?: KafkaClusterMetadataTopicRecord) {}

  toBuffer(): Buffer {
    const partitionIndexBuffer = Buffer.alloc(4);
    partitionIndexBuffer.writeUInt32BE(this.partitionIndex);

    const errorCode = this.topic ? ErrorCode.NO_ERROR : ErrorCode.UNKNOWN_TOPIC;
    const errorCodeBuffer = Buffer.alloc(2);
    errorCodeBuffer.writeUInt16BE(errorCode);

    const highWaterMarkBuffer = Buffer.alloc(8);
    highWaterMarkBuffer.writeBigInt64BE(BigInt(0)); // Placeholder for high watermark

    const lastStableOffsetBuffer = Buffer.alloc(8);
    lastStableOffsetBuffer.writeBigInt64BE(BigInt(0)); // Placeholder for last stable offset

    const logStartOffsetBuffer = Buffer.alloc(8);
    logStartOffsetBuffer.writeBigInt64BE(BigInt(0)); // Placeholder for log start offset

    const abortedTransactionsBuffer = writeVarInt(1);

    const preferredReadReplicaBuffer = Buffer.alloc(4);
    preferredReadReplicaBuffer.writeUInt32BE(0); // Placeholder for preferred read replica

    let records: KafkaPartitionRecordBatch[] = [];

    if (errorCode === ErrorCode.NO_ERROR) {
      const recordLogFile = KafkaPartitionLogFile.fromFile(
                `/tmp/kraft-combined-logs/${this.topic!.name}-${this.partitionIndex}/00000000000000000000.log`
              );
      records = recordLogFile.getRecords();
    }
    
    const compactRecordsLengthBuffer = writeVarInt(records.length + 1); // Placeholder for compact records length
    const recordBuffers = records.map(record => record.encodeTo());

    const tagFieldsArrayLength = 0; // Placeholder for tag fields array length
    const tagBufferBuffer = writeVarInt(tagFieldsArrayLength);

    // Log all buffer sizes for debugging
    // console.log(
    //   `[KafkaFetchTopicPartitionItemResp] partitionIndexBuffer size: ${partitionIndexBuffer.length}, errorCodeBuffer size: ${errorCodeBuffer.length}, highWaterMarkBuffer size: ${highWaterMarkBuffer.length}, lastStableOffsetBuffer size: ${lastStableOffsetBuffer.length}, logStartOffsetBuffer size: ${logStartOffsetBuffer.length}, abortedTransactionsBuffer size: ${abortedTransactionsBuffer.length}, preferredReadReplicaBuffer size: ${preferredReadReplicaBuffer.length}, compactRecordsLengthBuffer size: ${compactRecordsLengthBuffer.length}, tagBufferBuffer size: ${tagBufferBuffer.length}`
    // );

    return Buffer.concat([
      partitionIndexBuffer,
      errorCodeBuffer,
      highWaterMarkBuffer,
      lastStableOffsetBuffer,
      logStartOffsetBuffer,
      abortedTransactionsBuffer,
      preferredReadReplicaBuffer,
      compactRecordsLengthBuffer,
      ...recordBuffers,
      tagBufferBuffer,
    ]);
  }
}
