import fs from "node:fs";
import { KafkaClusterMetadataTopicRecord } from "./kafka_cluster_metadata_topic_record";
import { KafkaClusterMetadataPartitionRecord } from "./kafka_cluster_metadata_partition_record";
import { readSignedVarInt } from "../utils/utils";
import { KafkaClusterMetadataFeatureLevelRecord } from "./kafka_cluster_metadata_feature_level_record";

export class KafkaClusterMetadataRecordBatchItem {
  constructor(
    public length: number,
    public attributes: number,
    public timestampDelta: number,
    public offsetDelta: number,
    public keyLength: number,
    public key: Buffer | null,
    public valueLength: number,
    public value:
      | KafkaClusterMetadataTopicRecord
      | KafkaClusterMetadataPartitionRecord
      | KafkaClusterMetadataFeatureLevelRecord
      | null,
    public headersLength: number
  ) {}

  debugString(): string {
    return `KafkaClusterMetadataRecordBatchItem {
      length: ${this.length},
      attributes: ${this.attributes},
      timestampDelta: ${this.timestampDelta},
      offsetDelta: ${this.offsetDelta},
      keyLength: ${this.keyLength},
      key: ${this.key ? this.key.toString("hex") : null},
      valueLength: ${this.valueLength},
      value: ${this.value !== null ? this.value.debugString() : null},
      headersLength: ${this.headersLength}
    }`;
  }
}

export class KafkaClusterMetadataRecordBatch {
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
    public recordBatchItems: KafkaClusterMetadataRecordBatchItem[]
  ) {}

  bufferSize(): number {
    return (
      8 + // baseOffset size itself
      4 + // batchLength size itself
      this.batchLength
    );
  }

  static fromBuffer(buffer: Buffer): KafkaClusterMetadataRecordBatch {
    // dump the hex dump of the buffer
    // console.log("Buffer:", buffer.toString("hex"));

    let currentOffset = 0;

    const baseOffset = buffer.readBigInt64BE(currentOffset);
    // console.log("baseOffset:", baseOffset);
    currentOffset += 8;

    const batchLength = buffer.readInt32BE(currentOffset);
    // console.log("batchLength:", batchLength);
    currentOffset += 4;

    const partitionLeaderEpoch = buffer.readInt32BE(currentOffset);
    // console.log("partitionLeaderEpoch:", partitionLeaderEpoch);
    currentOffset += 4;

    const magicByte = buffer.readInt8(currentOffset);
    // console.log("magicByte:", magicByte);
    currentOffset += 1;

    const crc = buffer.readUInt32BE(currentOffset);
    // console.log("crc:", crc);
    currentOffset += 4;

    const attributes = buffer.readInt16BE(currentOffset);
    // console.log("attributes:", attributes);
    currentOffset += 2;

    const lastOffsetDelta = buffer.readInt32BE(currentOffset);
    // console.log("lastOffsetDelta:", lastOffsetDelta);
    currentOffset += 4;

    const baseTimestamp = buffer.readBigInt64BE(currentOffset);
    // console.log("baseTimestamp:", baseTimestamp);
    currentOffset += 8;

    const maxTimestamp = buffer.readBigInt64BE(currentOffset);
    // console.log("maxTimestamp:", maxTimestamp);
    currentOffset += 8;

    const producerId = buffer.readBigInt64BE(currentOffset);
    // console.log("producerId:", producerId);
    // const debugBuffer = buffer.subarray(currentOffset, currentOffset + 8);
    // console.log("producerId debugBuffer:", debugBuffer.toString("hex"));
    currentOffset += 8;

    const producerEpoch = buffer.readInt16BE(currentOffset);
    // console.log("producerEpoch:", producerEpoch);
    currentOffset += 2;

    const baseSequence = buffer.readInt32BE(currentOffset);
    // console.log("baseSequence:", baseSequence);
    currentOffset += 4;

    const recordCount = buffer.readUInt32BE(currentOffset);
    // console.log("recordCount:", recordCount);
    currentOffset += 4;

    // Read the record batch items
    const recordBatchItems: KafkaClusterMetadataRecordBatchItem[] = [];

    for (let i = 0; i < recordCount; i++) {
      // console.log(`Reading record batch item ${i} at offset ${currentOffset}`);
      const { value: recordLength, length: recordLengthSize } = readSignedVarInt(
        buffer.subarray(currentOffset, currentOffset + 4)
      );
      console.log(
        `Record ${i}: length: ${recordLength} - recordLengthSize: ${recordLengthSize}`
      );
      currentOffset += recordLengthSize;

      const attributes = buffer.readUInt8(currentOffset);
      // console.log(`Record ${i}: attributes: ${attributes}`);
      currentOffset += 1;

      const timestampDelta = buffer.readInt8(currentOffset);
      // console.log(`Record ${i}: timestampDelta: ${timestampDelta}`);
      currentOffset += 1;

      const offsetDelta = buffer.readInt8(currentOffset);
      // console.log(`Record ${i}: offsetDelta: ${offsetDelta}`);
      currentOffset += 1;

      const { value: keyLength, length: keyLengthSize } = readSignedVarInt(
        buffer.subarray(currentOffset, currentOffset + 4)
      );
      console.log(
        `Record ${i}: keyLength: ${keyLength} - keyLengthSize: ${keyLengthSize}`
      );
      currentOffset += keyLengthSize;

      const key = null;

      const { value: valueLength, length: valueLengthSize } = readSignedVarInt(
        buffer.subarray(currentOffset, currentOffset + 4)
      );
      console.log(
        `Record ${i}: valueLength: ${valueLength} - valueLengthSize: ${valueLengthSize}`
      );
      currentOffset += valueLengthSize;

      const value = buffer.subarray(currentOffset, currentOffset + valueLength);
      // console.log(`Record ${i}: value buffer length: ${value.length}`);
      const recordType = value.readInt8(1);
      // console.log(`Record ${i}: recordType: ${recordType}`);

      let valueRecord:
        | KafkaClusterMetadataTopicRecord
        | KafkaClusterMetadataPartitionRecord
        | KafkaClusterMetadataFeatureLevelRecord
        | null = null;
      switch (recordType) {
        case 12:
          valueRecord =
            KafkaClusterMetadataFeatureLevelRecord.fromBuffer(value);
          break;
        case 2:
          valueRecord = KafkaClusterMetadataTopicRecord.fromBuffer(value);
          break;
        case 3:
          valueRecord = KafkaClusterMetadataPartitionRecord.fromBuffer(value);
          break;
        default:
          console.log(
            `Record ${i}: Unknown record type: ${recordType}, skipping`
          );
          break;
      }

      currentOffset += valueLength;

      const headersLength = buffer.readUInt8(currentOffset);
      currentOffset += 1; // Skip headers

      // console.log(`Record ${i}: headersLength: ${headersLength}`);

      recordBatchItems.push(
        new KafkaClusterMetadataRecordBatchItem(
          recordLength,
          attributes,
          timestampDelta,
          offsetDelta,
          keyLength,
          key,
          valueLength,
          valueRecord,
          headersLength
        )
      );
    }

    // console.log(`recordBatchItems: ${recordBatchItems.length}`);

    const recordBatch = new KafkaClusterMetadataRecordBatch(
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

  getTopicRecord(): KafkaClusterMetadataTopicRecord {
    const topicRecord = this.recordBatchItems.find(
      (item) => item.value instanceof KafkaClusterMetadataTopicRecord
    )?.value as KafkaClusterMetadataTopicRecord;

    return topicRecord;
  }

  getPartitionRecords(): KafkaClusterMetadataPartitionRecord[] {
    const partitionRecord = this.recordBatchItems
      .filter(
        (item) => item.value instanceof KafkaClusterMetadataPartitionRecord
      )
      .map((item) => item.value as KafkaClusterMetadataPartitionRecord);

    return partitionRecord;
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

export class KafkaClusterMetadataLogFile {
  constructor(public batches: KafkaClusterMetadataRecordBatch[]) {}

  public static fromFile(filePath: string): KafkaClusterMetadataLogFile {
    // Handle file not found error
    if (!fs.existsSync(filePath)) {
      throw new Error(`File not found: ${filePath}`);
    }
    const data = fs.readFileSync(filePath);
    console.log(`Reading file: ${filePath} with size: ${data.length}`);
    return KafkaClusterMetadataLogFile.fromBuffer(data);
  }

  public static fromBuffer(buffer: Buffer): KafkaClusterMetadataLogFile {
    let currentOffset = 0;
    const batches: KafkaClusterMetadataRecordBatch[] = [];

    while (currentOffset < buffer.length) {
      // Start reading first record batch
      const batch = KafkaClusterMetadataRecordBatch.fromBuffer(
        buffer.subarray(currentOffset)
      );

      currentOffset += batch.bufferSize();
      batches.push(batch);
    }

    const logFile = new KafkaClusterMetadataLogFile(batches);

    return logFile;
  }

  public debugString(): string {
    return `KafkaClusterMetadataLogFile {
      batches: ${this.batches.map((batch) => batch.debugString()).join(", ")}
    }`;
  }

  getTopicRecords(): KafkaClusterMetadataTopicRecord[] {
    const topicRecords = this.batches
      .map((batch) => batch.getTopicRecord())
      .filter(
        (record) => record !== null && record !== undefined
      ) as KafkaClusterMetadataTopicRecord[];

    return topicRecords;
  }

  getPartitionRecordsMatchTopicUuid(
    topicUuid: Buffer
  ): KafkaClusterMetadataPartitionRecord[] {
    const partitionRecords = this.batches
      .map((batch) => batch.getPartitionRecords())
      .flat()
      .filter((record) =>
        record.topicUuid.equals(topicUuid)
      ) as KafkaClusterMetadataPartitionRecord[];

    return partitionRecords;
  }
}
