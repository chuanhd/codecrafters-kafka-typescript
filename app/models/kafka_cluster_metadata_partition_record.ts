export class KafkaClusterMetadataPartitionRecord {
  constructor(
    public frameVersion: number,
    public type: number,
    public version: number,
    public partitionId: number,
    public topicUuid: Buffer,
    public lengthOfReplicas: number,
    public replicas: number[],
    public lengthOfIsr: number,
    public isr: number[],
    public lengthOfRemovingReplicas: number,
    public removingReplicas: number[],
    public lengthOfAddingReplicas: number,
    public addingReplicas: number[],
    public leader: number,
    public leaderEpoch: number,
    public partitionEpoch: number,
    public lengthOfDirectory: number,
    public directory: Buffer,
    public tagFieldsCount: number
  ) {}

  debugString(): string {
    return `KafkaClusterMetadataPartitionRecord {
      frameVersion: ${this.frameVersion},
      type: ${this.type},
      version: ${this.version},
      partitionId: ${this.partitionId},
      topicUuid: ${this.topicUuid.toString("hex")},
      lengthOfReplicas: ${this.lengthOfReplicas},
      replicas: ${this.replicas.join(", ")},
      lengthOfIsr: ${this.lengthOfIsr},
      isr: ${this.isr.join(", ")},
      lengthOfRemovingReplicas: ${this.lengthOfRemovingReplicas},
      removingReplicas: ${this.removingReplicas.join(", ")},
      lengthOfAddingReplicas: ${this.lengthOfAddingReplicas},
      addingReplicas: ${this.addingReplicas.join(", ")},
      leader: ${this.leader},
      leaderEpoch: ${this.leaderEpoch},
      partitionEpoch: ${this.partitionEpoch},
      lengthOfDirectory: ${this.lengthOfDirectory},
      directory: ${this.directory.toString("hex")},
      tagFieldsCount: ${this.tagFieldsCount}
    }`;
  }

  static fromBuffer(buffer: Buffer): KafkaClusterMetadataPartitionRecord {
    let currentOffset = 0;

    const frameVersion = buffer.readUInt8(currentOffset);
    currentOffset += 1;

    const type = buffer.readUInt8(currentOffset);
    currentOffset += 1;

    const version = buffer.readUInt8(currentOffset);
    currentOffset += 1;

    const partitionId = buffer.readUInt32BE(currentOffset);
    currentOffset += 4;

    const topicUuid = buffer.subarray(currentOffset, currentOffset + 16);
    currentOffset += 16;

    const lengthOfReplicas = buffer.readUInt8(currentOffset) - 1;
    currentOffset += 1;
    console.log(
      `lengthOfReplicas: ${lengthOfReplicas} - currentOffset: ${currentOffset}`
    );
    const replicas = [];
    for (let i = 0; i < lengthOfReplicas; i++) {
      const replicaId = buffer.readUInt32BE(currentOffset);
      replicas.push(replicaId);
      currentOffset += 4;
    }
    console.log(`replicas: ${replicas.join(", ")}`);

    currentOffset += lengthOfReplicas * 4;

    const lengthOfIsr = buffer.readUInt8(currentOffset) - 1;
    currentOffset += 1;

    const isr = [];
    for (let i = 0; i < lengthOfIsr; i++) {
      const isrId = buffer.readUInt32BE(currentOffset);
      isr.push(isrId);
      currentOffset += 4;
    }
    console.log(`isr: ${isr.join(", ")}`);
    console.log(
      `lengthOfIsr: ${lengthOfIsr} - currentOffset: ${currentOffset}`
    );

    const lengthOfRemovingReplicas = buffer.readUInt8(currentOffset) - 1;
    currentOffset += 1;
    console.log(
      `lengthOfRemovingReplicas: ${lengthOfRemovingReplicas} - currentOffset: ${currentOffset}`
    );

    const removingReplicas = [];
    for (let i = 0; i < lengthOfRemovingReplicas; i++) {
      const removingReplicaId = buffer.readUInt32BE(currentOffset);
      removingReplicas.push(removingReplicaId);
      currentOffset += 4;
    }
    console.log(`removingReplicas: ${removingReplicas.join(", ")}`);

    const lengthOfAddingReplicas = buffer.readUInt8(currentOffset) - 1;
    currentOffset += 1;
    console.log(
      `lengthOfAddingReplicas: ${lengthOfAddingReplicas} - currentOffset: ${currentOffset}`
    );

    const addingReplicas = [];
    for (let i = 0; i < lengthOfAddingReplicas; i++) {
      const addingReplicaId = buffer.readUInt32BE(currentOffset);
      addingReplicas.push(addingReplicaId);
      currentOffset += 4;
    }
    console.log(`addingReplicas: ${addingReplicas.join(", ")}`);

    const leader = buffer.readUInt32BE(currentOffset);
    currentOffset += 4;
    const leaderEpoch = buffer.readUInt32BE(currentOffset);
    currentOffset += 4;
    const partitionEpoch = buffer.readUInt32BE(currentOffset);
    currentOffset += 4;

    const lengthOfDirectory = buffer.readUInt8(currentOffset) - 1;
    currentOffset += 1;
    const directory = buffer.subarray(
      currentOffset,
      currentOffset + lengthOfDirectory * 16
    );
    currentOffset += lengthOfDirectory * 16;

    const tagFieldsCount = buffer.readUInt8(currentOffset);
    currentOffset += 1;

    return new KafkaClusterMetadataPartitionRecord(
      frameVersion,
      type,
      version,
      partitionId,
      topicUuid,
      lengthOfReplicas,
      replicas,
      lengthOfIsr,
      isr,
      lengthOfRemovingReplicas,
      removingReplicas,
      lengthOfAddingReplicas,
      addingReplicas,
      leader,
      leaderEpoch,
      partitionEpoch,
      lengthOfDirectory,
      directory,
      tagFieldsCount
    );
  }
}
