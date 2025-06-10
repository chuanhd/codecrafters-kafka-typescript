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
      const batch = new KafkaPartitionRecordBatch();
      const numberOfBytesRead = batch.decodeFrom(buffer.subarray(currentOffset));
      console.log(`[KafkaPartitionRecordBatch] debug: ${batch.debugString()}`);
      currentOffset += numberOfBytesRead;
      batches.push(batch);
    }

    console.log(`batches size: ${batches.length}`);

    const logFile = new KafkaPartitionLogFile(batches);

    return logFile;
  }

  getRecords(): KafkaPartitionRecordBatch[] {
    return this.batches;
  }
}
