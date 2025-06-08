import fs from "node:fs";

import { KafkaPartitionRecordBatch } from "./kafka_partition_record_batch";

export class KafkaPartitionLogFile {
  constructor(public batches: KafkaPartitionRecordBatch[]) {}
  
  public static fromFile(filePath: string): KafkaPartitionLogFile {
    // Handle file not found error
    if (!fs.existsSync(filePath)) {
      throw new Error(`File not found: ${filePath}`);
    }
    const data = fs.readFileSync(filePath);
    console.log(`Reading file: ${filePath} with size: ${data.length}`);
    return KafkaPartitionLogFile.fromBuffer(data);
  }

  public static fromBuffer(buffer: Buffer): KafkaPartitionLogFile {
    let currentOffset = 0;
    const batches: KafkaPartitionRecordBatch[] = [];

    while (currentOffset < buffer.length) {
      // Start reading first record batch
      const batch = KafkaPartitionRecordBatch.fromBuffer(
        buffer.subarray(currentOffset)
      );
      console.log(`[KafkaPartitionRecordBatch] debug: ${batch.debugString()}`);
      currentOffset += batch.bufferSize();
      batches.push(batch);
    }

    const logFile = new KafkaPartitionLogFile(batches);

    return logFile;
  }

  getRecords(): KafkaPartitionRecordBatch[] {
    return this.batches;
  }
}
