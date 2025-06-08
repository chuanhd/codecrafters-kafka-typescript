import { UInt16Field, UInt32Field, UInt64Field, UInt8Field } from './../fields/atom_field';
import { crc32c } from "../../utils/utils";
import { Wrapper, type Offset } from "../wrapper";
import { KafkaPartitionRecordItem } from "./kafka_partition_record_item";
import type { BufferDecode, BufferEncode } from '../common/interface_encode';

export class KafkaPartitionRecordBatch implements BufferEncode, BufferDecode {
  public baseOffset: UInt64Field;
  public batchLength: UInt32Field;
  public partitionLeaderEpoch: UInt32Field;
  public magicByte: UInt8Field;
  public crc: UInt32Field;
  public attributes: UInt16Field;
  public lastOffsetDelta: UInt32Field;
  public baseTimestamp: UInt64Field;
  public maxTimestamp: UInt64Field;
  public producerId: UInt64Field;
  public producerEpoch: UInt16Field;
  public baseSequence: UInt32Field;
  public recordCount: UInt32Field;
  public recordBatchItems: KafkaPartitionRecordItem[];

  constructor() {
    this.baseOffset = new UInt64Field(BigInt(0));
    this.batchLength = new UInt32Field(0);
    this.partitionLeaderEpoch = new UInt32Field(0);
    this.magicByte = new UInt8Field(0);
    this.crc = new UInt32Field(0);
    this.attributes = new UInt16Field(0);
    this.lastOffsetDelta = new UInt32Field(0);
    this.baseTimestamp = new UInt64Field(BigInt(0));
    this.maxTimestamp = new UInt64Field(BigInt(0));
    this.producerId = new UInt64Field(BigInt(0));
    this.producerEpoch = new UInt16Field(0);
    this.baseSequence = new UInt32Field(0);
    this.recordCount = new UInt32Field(0);
    this.recordBatchItems = [];
  }

  encodeTo(): Buffer {
    const buffers = []
    buffers.push(this.baseOffset.encode());
    buffers.push(this.batchLength.encode());
    buffers.push(this.partitionLeaderEpoch.encode());
    buffers.push(this.magicByte.encode());
    buffers.push(this.crc.encode());
    buffers.push(this.attributes.encode());
    buffers.push(this.lastOffsetDelta.encode());
    buffers.push(this.baseTimestamp.encode());
    buffers.push(this.maxTimestamp.encode());
    buffers.push(this.producerId.encode());
    buffers.push(this.producerEpoch.encode());
    buffers.push(this.baseSequence.encode());
    buffers.push(this.recordCount.encode());

    const recordBatchItemsBuffers = this.recordBatchItems.map((item) =>
      item.encode()
    );

    buffers.push(...recordBatchItemsBuffers);

    const recordBatchBuffer = Buffer.concat(buffers);

    const batchLength = recordBatchBuffer.length - 12; // 8 bytes for baseOffset and 4 bytes for batchLength
    recordBatchBuffer.writeUInt32BE(batchLength, 8); // Update the batch length in the buffer

    // Recalculate CRC
    const crcStartOffset = 17; // CRC starts after the first 17 bytes\
    const crcEndOffset = crcStartOffset + this.crc.size;
    const crc = crc32c(recordBatchBuffer.subarray(crcEndOffset));
    recordBatchBuffer.writeUInt32BE(crc, crcStartOffset); // Update the CRC in the buffer

    return recordBatchBuffer;
  }

  decodeFrom(buffer: Buffer): number {
    let currentOffset = 0;

    this.baseOffset.decode(buffer.subarray(currentOffset));
    currentOffset += this.baseOffset.size;

    this.batchLength.decode(buffer.subarray(currentOffset));
    currentOffset += this.batchLength.size;

    this.partitionLeaderEpoch.decode(buffer.subarray(currentOffset));
    currentOffset += this.partitionLeaderEpoch.size;

    this.magicByte.decode(buffer.subarray(currentOffset));
    currentOffset += this.magicByte.size;

    this.crc.decode(buffer.subarray(currentOffset));
    currentOffset += this.crc.size;

    this.attributes.decode(buffer.subarray(currentOffset));
    currentOffset += this.attributes.size;

    this.lastOffsetDelta.decode(buffer.subarray(currentOffset));
    currentOffset += this.lastOffsetDelta.size;

    this.baseTimestamp.decode(buffer.subarray(currentOffset));
    currentOffset += this.baseTimestamp.size;

    this.maxTimestamp.decode(buffer.subarray(currentOffset));
    currentOffset += this.maxTimestamp.size;

    this.producerId.decode(buffer.subarray(currentOffset));
    currentOffset += this.producerId.size;

    this.producerEpoch.decode(buffer.subarray(currentOffset));
    currentOffset += this.producerEpoch.size;

    this.baseSequence.decode(buffer.subarray(currentOffset));
    currentOffset += this.baseSequence.size;

    this.recordCount.decode(buffer.subarray(currentOffset));
    currentOffset += this.recordCount.size;

    // Read the record batch items
    this.recordBatchItems = [];
    for (let i = 0; i < this.recordCount.value; i++) {
      const reportBatchItem = new KafkaPartitionRecordItem();
      const offsetWrapper = new Wrapper<number>(0);
      reportBatchItem.decode(
        buffer.subarray(currentOffset),
        offsetWrapper
      );
      currentOffset += offsetWrapper.value; 
      this.recordBatchItems.push(reportBatchItem);
    }

    return currentOffset; // Return number of bytes read
  }

  bufferSize(): number {
    return (
      8 + // baseOffset size itself
      4 + // batchLength size itself
      this.batchLength.value
    );
  }

  debugString(): string {
    return `KafkaClusterMetadataRecordBatch {
      baseOffset: ${this.baseOffset.value},
      batchLength: ${this.batchLength.value},
      partitionLeaderEpoch: ${this.partitionLeaderEpoch.value},
      magicByte: ${this.magicByte.value},
      crc: ${this.crc.value},
      attributes: ${this.attributes.value},
      lastOffsetDelta: ${this.lastOffsetDelta.value},
      baseTimestamp: ${this.baseTimestamp.value},
      maxTimestamp: ${this.maxTimestamp.value},
      producerId: ${this.producerId.value},
      producerEpoch: ${this.producerEpoch.value},
      baseSequence: ${this.baseSequence.value},
      recordCount: ${this.recordCount.value}
      recordBatchItems: ${this.recordBatchItems
        .map((item) => item.debugString())
        .join(", ")}
    }`;
  }
}