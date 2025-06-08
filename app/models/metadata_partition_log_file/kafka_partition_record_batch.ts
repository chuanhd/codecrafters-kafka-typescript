import { crc32c, readSignedVarInt, readVarInt } from "../../utils/utils";
import { Wrapper } from "../wrapper";
import { KafkaPartitionRecordItem } from "./kafka_partition_record_item";

export class KafkaPartitionRecordBatch {
  constructor(
    public baseOffset: bigint,
    public batchLength: number,
    public partitionLeaderEpoch: number,
    public magicByte: number,
    public crc: number,
    public attributes: number,
    public lastOffsetDelta: number,
    public baseTimestamp: bigint,
    public maxTimestamp: bigint,
    public producerId: bigint,
    public producerEpoch: number,
    public baseSequence: number,
    public recordCount: number,
    public recordBatchItems: KafkaPartitionRecordItem[]
  ) {}

  bufferSize(): number {
    return (
      8 + // baseOffset size itself
      4 + // batchLength size itself
      this.batchLength
    );
  }

  static fromBuffer(buffer: Buffer): KafkaPartitionRecordBatch {
    let currentOffset = 0;

    const baseOffset = buffer.readBigInt64BE(currentOffset);
    // console.log("baseOffset:", baseOffset);
    currentOffset += 8;

    const batchLength = buffer.readInt32BE(currentOffset);
    currentOffset += 4;

    const partitionLeaderEpoch = buffer.readInt32BE(currentOffset);
    currentOffset += 4;

    const magicByte = buffer.readInt8(currentOffset);
    currentOffset += 1;

    const crc = buffer.readUInt32BE(currentOffset);
    currentOffset += 4;

    const attributes = buffer.readInt16BE(currentOffset);
    currentOffset += 2;

    const lastOffsetDelta = buffer.readInt32BE(currentOffset);
    currentOffset += 4;

    const baseTimestamp = buffer.readBigInt64BE(currentOffset);
    currentOffset += 8;

    const maxTimestamp = buffer.readBigInt64BE(currentOffset);
    // console.log("maxTimestamp:", maxTimestamp);
    currentOffset += 8;

    const producerId = buffer.readBigInt64BE(currentOffset);
    currentOffset += 8;

    const producerEpoch = buffer.readInt16BE(currentOffset);
    // console.log("producerEpoch:", producerEpoch);
    currentOffset += 2;

    const baseSequence = buffer.readInt32BE(currentOffset);
    // console.log("baseSequence:", baseSequence);
    currentOffset += 4;

    const recordCount = buffer.readUInt32BE(currentOffset);
    currentOffset += 4;

    // Read the record batch items
    const recordBatchItems: KafkaPartitionRecordItem[] = [];

    // for (let i = 0; i < recordCount; i++) {
    //   console.log(`Reading record batch item ${i} at offset ${currentOffset}`);
      // const reportBatchItem = new KafkaPartitionRecordItem();
      // const offsetWrapper = new Wrapper<number>(0);

      // reportBatchItem.decode(
      //   buffer.subarray(currentOffset),
      //   offsetWrapper
      // );

    //   currentOffset += offsetWrapper.value;

    //   recordBatchItems.push(reportBatchItem);

    //   console.log(`Record ${i} ${reportBatchItem.debugString()}`);
    //   console.log(`End of record ${i} at offset ${currentOffset} - total buffer length: ${buffer.length}`);
    // }

    for (let i = 0; i < recordCount; i++) {
      const debugOffset = currentOffset;
      console.log(`Reading record batch item ${i} at offset ${currentOffset}`);
      const { value: recordLength, length: recordLengthSize } = readSignedVarInt(
        buffer.subarray(currentOffset, currentOffset + 4)
      );
      console.log(
        `Record ${i}: length: ${recordLength} - recordLengthSize: ${recordLengthSize}`
      );
      currentOffset += recordLengthSize;

      const attributes = buffer.readUInt8(currentOffset);
      console.log(`Record ${i}: attributes: ${attributes}`);
      currentOffset += 1;

      const timestampDelta = buffer.readInt8(currentOffset);
      console.log(`Record ${i}: timestampDelta: ${timestampDelta}`);
      currentOffset += 1;

      const offsetDelta = buffer.readInt8(currentOffset);
      console.log(`Record ${i}: offsetDelta: ${offsetDelta}`);
      currentOffset += 1;

      const { value: keyLength, length: keyLengthSize } = readVarInt(
        buffer.subarray(currentOffset, currentOffset + 4)
      );
      currentOffset += keyLengthSize;

      console.log(`Record ${i}: keyLength: ${keyLength}`);
      let key = undefined;
      if (keyLength > 0) {
        console.log(
        `Record ${i}: keyLength: ${keyLength} - keyLengthSize: ${keyLengthSize}`
        );
        key = buffer.subarray(
          currentOffset,
          currentOffset + keyLength
        );
        console.log(`Record ${i}: key: ${key.toHex()}`);
      }
      
      console.log('valueLength buffer: ', buffer.subarray(currentOffset, currentOffset + 4).toHex())
      const { value: valueLength, length: valueLengthSize } = readSignedVarInt(
        buffer.subarray(currentOffset, currentOffset + 4)
      );
      currentOffset += valueLengthSize;

      const value = buffer.subarray(currentOffset, currentOffset + valueLength);
      console.log(
        `Record ${i}: valueLength: ${valueLength} - valueLengthSize: ${valueLengthSize} - value: ${value.toString()}`
      );

      currentOffset += valueLength;

      const { value: headersLength, length: headersLengthSize } = readVarInt(buffer.subarray(currentOffset));
      currentOffset += headersLengthSize;
      console.log(`Record ${i}: headersLength: ${headersLength} - headersLengthSize: ${headersLengthSize}`);

      const header = headersLength > 0 ? buffer.subarray(
        currentOffset,
        currentOffset + headersLength
      ) : Buffer.alloc(0);
      currentOffset += headersLength;

      // console.log(`Record ${i}: headersLength: ${headersLength}`);
      console.log(`End of record ${i} at offset ${currentOffset} - total buffer length: ${buffer.length}`);

      const reportBatchItem = new KafkaPartitionRecordItem();
      const offsetWrapper = new Wrapper<number>(0);

      reportBatchItem.decode(
        buffer.subarray(debugOffset),
        offsetWrapper
      );

      recordBatchItems.push(reportBatchItem);

      // recordBatchItems.push(
      //   new KafkaPartitionRecordItem(
      //     recordLength,
      //     attributes,
      //     timestampDelta,
      //     offsetDelta,
      //     keyLength,
      //     key,
      //     valueLength,
      //     value,
      //     headersLength,
      //     header
      //   )
      // );
    }

    // console.log(`recordBatchItems: ${recordBatchItems.length}`);

    console.log(`Finished reading KafkaPartitionRecordBatch from buffer at offset ${currentOffset}`);

    const recordBatch = new KafkaPartitionRecordBatch(
      baseOffset,
      batchLength,
      partitionLeaderEpoch,
      magicByte,
      crc,
      attributes,
      lastOffsetDelta,
      baseTimestamp,
      maxTimestamp,
      producerId,
      producerEpoch,
      baseSequence,
      recordCount,
      recordBatchItems
    );

    return recordBatch;
  }

  toBuffer(): Buffer {
    const baseOffsetBuffer = Buffer.alloc(8);
    baseOffsetBuffer.writeBigInt64BE(this.baseOffset);
  
    const batchLengthBuffer = Buffer.alloc(4);
    batchLengthBuffer.writeUInt32BE(this.batchLength);
    

    const partitionLeaderEpochBuffer = Buffer.alloc(4);
    partitionLeaderEpochBuffer.writeInt32BE(this.partitionLeaderEpoch);

    const magicByteBuffer = Buffer.alloc(1);
    magicByteBuffer.writeInt8(this.magicByte);

    const crcBuffer = Buffer.alloc(4);
    crcBuffer.writeUInt32BE(this.crc);

    const attributesBuffer = Buffer.alloc(2);
    attributesBuffer.writeInt16BE(this.attributes);

    const lastOffsetDeltaBuffer = Buffer.alloc(4);
    lastOffsetDeltaBuffer.writeInt32BE(this.lastOffsetDelta);

    const baseTimestampBuffer = Buffer.alloc(8);
    baseTimestampBuffer.writeBigInt64BE(this.baseTimestamp);

    const maxTimestampBuffer = Buffer.alloc(8);
    maxTimestampBuffer.writeBigInt64BE(this.maxTimestamp);

    const producerIdBuffer = Buffer.alloc(8);
    producerIdBuffer.writeBigInt64BE(this.producerId);

    const producerEpochBuffer = Buffer.alloc(2);
    producerEpochBuffer.writeInt16BE(this.producerEpoch);

    const baseSequenceBuffer = Buffer.alloc(4);
    baseSequenceBuffer.writeInt32BE(this.baseSequence);

    const recordCountBuffer = Buffer.alloc(4);
    recordCountBuffer.writeUInt32BE(this.recordCount);

    const recordBatchItemsBuffers = this.recordBatchItems.map((item) =>
      item.encode()
    );

    const recordBatchBuffer = Buffer.concat([
      baseOffsetBuffer, // 8
      batchLengthBuffer, // 4
      partitionLeaderEpochBuffer, // 4
      magicByteBuffer, // 1
      crcBuffer, // 4
      attributesBuffer,
      lastOffsetDeltaBuffer,
      baseTimestampBuffer,
      maxTimestampBuffer,
      producerIdBuffer,
      producerEpochBuffer,
      baseSequenceBuffer,
      recordCountBuffer,
      ...recordBatchItemsBuffers,
    ]);

    const batchLength = recordBatchBuffer.length - 12; // 8 bytes for baseOffset and 4 bytes for batchLength
    recordBatchBuffer.writeUInt32BE(batchLength, 8); // Update the batch length in the buffer

    // Recalculate CRC
    const crcStartOffset = 17; // CRC starts after the first 17 bytes\
    const crcEndOffset = crcStartOffset + crcBuffer.length;
    const crc = crc32c(recordBatchBuffer.subarray(crcEndOffset));
    recordBatchBuffer.writeUInt32BE(crc, crcStartOffset); // Update the CRC in the buffer

    // Log all buffers for debugging
    // console.log(`KafkaPartitionRecordBatch toBuffer lengths:
    //   baseOffset: ${baseOffsetBuffer.toHex()},
    //   batchLength: ${batchLengthBuffer.toHex()},
    //   partitionLeaderEpoch: ${partitionLeaderEpochBuffer.toHex()},
    //   magicByte: ${magicByteBuffer.toHex()},
    //   crc: ${crcBuffer.toHex()},
    //   attributes: ${attributesBuffer.toHex()},
    //   lastOffsetDelta: ${lastOffsetDeltaBuffer.toHex()},
    //   baseTimestamp: ${baseTimestampBuffer.toHex()},
    //   maxTimestamp: ${maxTimestampBuffer.toHex()},
    //   producerId: ${producerIdBuffer.toHex()},
    //   producerEpoch: ${producerEpochBuffer.toHex()},
    //   baseSequence: ${baseSequenceBuffer.toHex()},
    //   recordCount: ${recordCountBuffer.toHex()},
    //   recordBatchItems: ${recordBatchItemsBuffers.map((buf) => buf.toHex()).join(", ")}
    // `);

    return recordBatchBuffer;
  }

  debugString(): string {
    return `KafkaClusterMetadataRecordBatch {
      baseOffset: ${this.baseOffset},
      batchLength: ${this.batchLength},
      partitionLeaderEpoch: ${this.partitionLeaderEpoch},
      magicByte: ${this.magicByte},
      crc: ${this.crc},
      attributes: ${this.attributes},
      lastOffsetDelta: ${this.lastOffsetDelta},
      baseTimestamp: ${this.baseTimestamp},
      maxTimestamp: ${this.maxTimestamp},
      producerId: ${this.producerId},
      producerEpoch: ${this.producerEpoch},
      baseSequence: ${this.baseSequence},
      recordCount: ${this.recordCount}
      recordBatchItems: ${this.recordBatchItems
        .map((item) => item.debugString())
        .join(", ")}
    }`;
  }
}