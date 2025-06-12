import { KafkaClusterMetadataTopicRecord } from './../../models/metadata_log_file/kafka_cluster_metadata_topic_record';
import { ErrorCode } from '../../utils/consts';
import { KafkaPartitionLogFile } from '../../models/metadata_partition_log_file/kafka_partition_log_file';
import { KafkaPartitionRecordBatch } from '../../models/metadata_partition_log_file/kafka_partition_record_batch';
import { UInt16Field, UInt32Field, UInt64Field, UVarIntField, VarIntField } from '../../models/fields/atom_field';
import type { BufferEncode } from '../../models/common/interface_encode';

export class KafkaFetchTopicPartitionItemResp
  implements BufferEncode
{
  public partitionIndex: UInt32Field;
  public errorCode: UInt16Field;
  public highWaterMark: UInt64Field;
  public lastStableOffset: UInt64Field;
  public logStartOffset: UInt64Field;
  public abortedTransactions: VarIntField;
  public preferredReadReplica: UInt32Field;
  public compactRecordsLength: UVarIntField;
  public records: KafkaPartitionRecordBatch[];
  public tagFieldsArrayLength: VarIntField;

  constructor(partitionIndex: number, public topic?: KafkaClusterMetadataTopicRecord) {
    this.partitionIndex = new UInt32Field(partitionIndex);
    const errorCode = this.topic ? ErrorCode.NO_ERROR : ErrorCode.UNKNOWN_TOPIC;
    this.errorCode = new UInt16Field(errorCode);
    this.highWaterMark = new UInt64Field(0n);
    this.lastStableOffset = new UInt64Field(0n); 
    this.logStartOffset = new UInt64Field(0n);
    this.abortedTransactions = new VarIntField(0); 
    this.preferredReadReplica = new UInt32Field(0); 

    let records: KafkaPartitionRecordBatch[] = [];

    if (errorCode === ErrorCode.NO_ERROR) {
      const recordLogFile = KafkaPartitionLogFile.fromFile(
                `/tmp/kraft-combined-logs/${this.topic!.name}-${this.partitionIndex.value}/00000000000000000000.log`
              );
      records = recordLogFile.getRecords();
    }

    const totalRecordsSize = records.reduce((total, record) => total + record.bufferSize(), 0);
    this.compactRecordsLength = new UVarIntField(totalRecordsSize); 
    this.records = records;
    this.tagFieldsArrayLength = new VarIntField(0);
    
  }

  encodeTo(): Buffer {
    const buffers = [];
    buffers.push(this.partitionIndex.encode());
    buffers.push(this.errorCode.encode());
    buffers.push(this.highWaterMark.encode());
    buffers.push(this.lastStableOffset.encode());
    buffers.push(this.logStartOffset.encode());
    buffers.push(this.abortedTransactions.encode());
    buffers.push(this.preferredReadReplica.encode());
    buffers.push(this.compactRecordsLength.encode());
    this.records.forEach((record) => {
      buffers.push(record.encodeTo());
    });
    buffers.push(this.tagFieldsArrayLength.encode());

    return Buffer.concat(buffers);
  }
}
