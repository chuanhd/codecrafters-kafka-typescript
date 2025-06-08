export class KafkaClusterMetadataFeatureLevelRecord {
  constructor(
    public frameVersion: number,
    public type: number,
    public version: number,
    public nameLength: number,
    public name: string,
    public featureLevel: number,
    public tagFieldsCount: number
  ) {}

  debugString(): string {
    return `KafkaClusterMetadataFeatureLevelRecord {
      frameVersion: ${this.frameVersion},
      type: ${this.type},
      version: ${this.version},
      nameLength: ${this.nameLength},
      name: ${this.name},
      uuid: ${this.featureLevel},
      tagFieldsCount: ${this.tagFieldsCount}
    }`;
  }

  static fromBuffer(buffer: Buffer): KafkaClusterMetadataFeatureLevelRecord {
    let currentOffset = 0;
    console.log(
      "Reading KafkaClusterMetadataFeatureLevelRecord from buffer:",
      buffer.length
    );

    const frameVersion = buffer.readUInt8(currentOffset);
    // console.log(`frameVersion: ${frameVersion} at offset ${currentOffset}`);
    currentOffset += 1;

    const type = buffer.readUInt8(currentOffset);
    // console.log(`type: ${type} at offset ${currentOffset}`);
    currentOffset += 1;

    const version = buffer.readUInt8(currentOffset);
    // console.log(`version: ${version} at offset ${currentOffset}`);
    currentOffset += 1;

    const nameLength = buffer.readUInt8(currentOffset) - 1;
    // console.log(
    //   `nameLength: ${nameLength} (original: ${buffer.readUInt8(
    //     currentOffset
    //   )}) at offset ${currentOffset}`
    // );
    currentOffset += 1;

    const name = buffer
      .subarray(currentOffset, currentOffset + nameLength)
      .toString("utf-8");
    // console.log(`name: "${name}" at offset ${currentOffset}`);
    currentOffset += nameLength;

    const featureLevel = buffer.readUInt16BE(currentOffset);
    // console.log(`featureLevel: ${featureLevel} at offset ${currentOffset}`);
    currentOffset += 2;

    const tagFieldsCount = buffer.readUInt8(currentOffset);
    // console.log(`tagFieldsCount: ${tagFieldsCount} at offset ${currentOffset}`);
    currentOffset += 1;

    return new KafkaClusterMetadataFeatureLevelRecord(
      frameVersion,
      type,
      version,
      nameLength,
      name,
      featureLevel,
      tagFieldsCount
    );
  }
}
