export class KafkaClusterMetadataTopicRecord {
  constructor(
    public frameVersion: number,
    public type: number,
    public version: number,
    public nameLength: number,
    public name: string,
    public uuid: Buffer,
    public tagFieldsCount: number
  ) {}

  debugString(): string {
    return `KafkaClusterMetadataTopicRecord {
      frameVersion: ${this.frameVersion},
      type: ${this.type},
      version: ${this.version},
      nameLength: ${this.nameLength},
      name: ${this.name},
      uuid: ${this.uuid.toString("hex")},
      tagFieldsCount: ${this.tagFieldsCount}
    }`;
  }

  static fromBuffer(buffer: Buffer): KafkaClusterMetadataTopicRecord {
    let currentOffset = 0;
    console.log(
      "Reading KafkaClusterMetadataTopicRecord from buffer:",
      buffer.length
    );

    const frameVersion = buffer.readUInt8(currentOffset);
    console.log(`frameVersion: ${frameVersion} at offset ${currentOffset}`);
    currentOffset += 1;

    const type = buffer.readUInt8(currentOffset);
    console.log(`type: ${type} at offset ${currentOffset}`);
    currentOffset += 1;

    const version = buffer.readUInt8(currentOffset);
    console.log(`version: ${version} at offset ${currentOffset}`);
    currentOffset += 1;

    const nameLength = buffer.readUInt8(currentOffset) - 1;
    console.log(
      `nameLength: ${nameLength} (original: ${buffer.readUInt8(
        currentOffset
      )}) at offset ${currentOffset}`
    );
    currentOffset += 1;

    const name = buffer
      .subarray(currentOffset, currentOffset + nameLength)
      .toString("utf-8");
    console.log(`name: "${name}" at offset ${currentOffset}`);
    currentOffset += nameLength;

    const uuid = buffer.subarray(currentOffset, currentOffset + 16);
    console.log(`uuid: ${uuid.toString("hex")} at offset ${currentOffset}`);
    currentOffset += 16;

    const tagFieldsCount = buffer.readUInt8(currentOffset);
    console.log(`tagFieldsCount: ${tagFieldsCount} at offset ${currentOffset}`);
    currentOffset += 1;

    return new KafkaClusterMetadataTopicRecord(
      frameVersion,
      type,
      version,
      nameLength,
      name,
      uuid,
      tagFieldsCount
    );
  }
}
