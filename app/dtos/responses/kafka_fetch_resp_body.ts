import type { IResponseBufferSerializable } from "../../models/common/interface_buffer_serializable";
import { writeVarInt } from "../../utils/utils";
import type { KafkaFetchTopicItemResp } from "./kafka_fetch_topic_item_resp";

export class KafkaFetchResponseBody implements IResponseBufferSerializable {
  constructor(
    public throttleTime: number,
    public errorCode: number,
    public sessionId: number,
    public topics: KafkaFetchTopicItemResp[]
  ) {}

  toBuffer(): Buffer {
    const throttleTimeBuffer = Buffer.alloc(4);
    throttleTimeBuffer.writeUInt32BE(this.throttleTime);
    const errorCodeBuffer = Buffer.alloc(2);
    errorCodeBuffer.writeUInt16BE(this.errorCode);
    const sessionIdBuffer = Buffer.alloc(4);
    sessionIdBuffer.writeUInt32BE(this.sessionId);

    const numResponsesBuffer = writeVarInt(this.topics.length + 1);
    const topicsBuffer = Buffer.concat(
      this.topics.map((topic) => topic.toBuffer())
    );
    const tagBufferBuffer = writeVarInt(0);

    // console.log(
    //   `[KafkaFetchResponseBody] throttleTimeBuffer size: ${throttleTimeBuffer.length}, errorCodeBuffer size: ${errorCodeBuffer.length}, sessionIdBuffer size: ${sessionIdBuffer.length}, numResponsesBuffer size: ${numResponsesBuffer.length}, topicsBuffer size: ${topicsBuffer.length}, tagBufferBuffer size: ${tagBufferBuffer.length}`
    // );

    return Buffer.concat([
      throttleTimeBuffer,
      errorCodeBuffer,
      sessionIdBuffer,
      numResponsesBuffer,
      topicsBuffer,
      tagBufferBuffer,
    ]);
  }
}
