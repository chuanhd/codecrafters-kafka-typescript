import { KafkaClusterMetadataPartitionRecord } from "../../models/metadata_log_file/kafka_cluster_metadata_partition_record";
import type { IResponseBufferSerializable } from "../../interface_buffer_serializable";
import { writeVarInt } from "../../utils/utils";
import { ErrorCode } from "../../consts";

const ErrorCodeBufferSize = 2; // 2 bytes
const PartitionIndexBufferSize = 4; // 4 bytes
const LeaderIdBufferSize = 4; // 4 bytes
const LeaderEpochBufferSize = 4; // 4 bytes
const ReplicasArrayItemBufferSize = 4; // 4 bytes
const IsrArrayItemBufferSize = 4; // 4 bytes
const EligibleReplicasArrayItemBufferSize = 4; // 4 bytes
const LastKnownELRArrayItemBufferSize = 4; // 4 bytes
const OfflineReplicasArrayItemBufferSize = 4; // 4 bytes
const TagBufferBufferSize = 1; // 1 byte

export class KafkaTopicPartitionItemResp
  implements IResponseBufferSerializable
{
  constructor(
    public errorCode: number,
    public partitionIndex: number,
    public leaderId: number,
    public leaderEpoch: number,
    public replicas: number[],
    public isr: number[],
    public eligibleReplicas: number[],
    public lastKnownELR: number[],
    public offlineReplicas: number[],
    public tagBuffer: number
  ) {}

  static fromLogRecord(
    record: KafkaClusterMetadataPartitionRecord,
    index: number
  ): KafkaTopicPartitionItemResp {
    return new KafkaTopicPartitionItemResp(
      ErrorCode.NO_ERROR,
      index,
      record.leader,
      record.leaderEpoch,
      record.replicas,
      [1],
      [],
      [],
      [],
      0 // Assuming tagBuffer is not used in this context
    );
  }

  toBuffer(): Buffer {
    // Create buffers for each field and write the values
    const errorCodeBuffer = Buffer.alloc(ErrorCodeBufferSize);
    errorCodeBuffer.writeUInt16BE(this.errorCode);

    // Create buffers for other fields
    const partitionIndexBuffer = Buffer.alloc(PartitionIndexBufferSize);
    partitionIndexBuffer.writeUInt32BE(this.partitionIndex);
    const leaderIdBuffer = Buffer.alloc(LeaderIdBufferSize);
    leaderIdBuffer.writeUInt32BE(this.leaderId);
    const leaderEpochBuffer = Buffer.alloc(LeaderEpochBufferSize);
    leaderEpochBuffer.writeUInt32BE(this.leaderEpoch);

    const replicaLength = this.replicas.length;
    const replicaLengthBuffer = writeVarInt(replicaLength + 1); // +1 for the length byte
    console.log(
      `replicaLengthBuffer: ${replicaLengthBuffer.toHex()} replaceLengthBuffer'size: ${
        replicaLengthBuffer.length
      }`
    );
    const replicasBuffer = Buffer.alloc(
      replicaLength * ReplicasArrayItemBufferSize
    );
    console.log(
      `replicasBuffer size: ${replicasBuffer.length} replicasLength: ${replicaLength} replicaBuffer: ${replicasBuffer}`
    );
    this.replicas.forEach((replica, index) => {
      replicasBuffer.writeUInt32BE(
        replica,
        index * ReplicasArrayItemBufferSize
      );
    });
    console.log(
      `replicasBuffer: ${replicasBuffer.toHex()} replicasBuffer'size: ${
        replicasBuffer.length
      }`
    );

    const isrLength = this.isr.length;
    const isrLengthBuffer = writeVarInt(isrLength + 1); // +1 for the length byte
    const isrBuffer = Buffer.alloc(isrLength * IsrArrayItemBufferSize);
    this.isr.forEach((isr, index) => {
      isrBuffer.writeUInt32BE(isr, index * IsrArrayItemBufferSize);
    });

    const eligibleReplicasLength = this.eligibleReplicas.length;
    const eligibleReplicasLengthBuffer = writeVarInt(
      eligibleReplicasLength + 1 // +1 for the length byte
    );
    const eligibleReplicasBuffer = Buffer.alloc(
      eligibleReplicasLength * EligibleReplicasArrayItemBufferSize
    );
    this.eligibleReplicas.forEach((replica, index) => {
      eligibleReplicasBuffer.writeUInt32BE(
        replica,
        index * EligibleReplicasArrayItemBufferSize
      );
    });

    const lastKnownELRLength = this.lastKnownELR.length;
    const lastKnownELRLengthBuffer = writeVarInt(
      lastKnownELRLength + 1 // +1 for the length byte
    );
    const lastKnownELRBuffer = Buffer.alloc(
      lastKnownELRLength * LastKnownELRArrayItemBufferSize
    );
    this.lastKnownELR.forEach((elr, index) => {
      lastKnownELRBuffer.writeUInt32BE(
        elr,
        index * LastKnownELRArrayItemBufferSize
      );
    });
    const offlineReplicasLength = this.offlineReplicas.length;
    const offlineReplicasLengthBuffer = writeVarInt(
      offlineReplicasLength + 1 // +1 for the length byte
    );
    const offlineReplicasBuffer = Buffer.alloc(
      offlineReplicasLength * OfflineReplicasArrayItemBufferSize
    );
    this.offlineReplicas.forEach((replica, index) => {
      offlineReplicasBuffer.writeUInt32BE(
        replica,
        index * OfflineReplicasArrayItemBufferSize
      );
    });
    const tagBuffer = Buffer.alloc(TagBufferBufferSize);
    tagBuffer.writeUInt8(this.tagBuffer);

    return Buffer.concat([
      errorCodeBuffer,
      partitionIndexBuffer,
      leaderIdBuffer,
      leaderEpochBuffer,
      replicaLengthBuffer,
      replicasBuffer,
      isrLengthBuffer,
      isrBuffer,
      eligibleReplicasLengthBuffer,
      eligibleReplicasBuffer,
      lastKnownELRLengthBuffer,
      lastKnownELRBuffer,
      offlineReplicasLengthBuffer,
      offlineReplicasBuffer,
      tagBuffer,
    ]);
  }
}
