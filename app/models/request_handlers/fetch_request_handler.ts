import { ErrorCode, ResponseType } from "../../consts";
import { KafkaFetchRequest } from "../../dtos/requests/kafka_fetch_request";
import { KafkaRequestHeader } from "../../dtos/requests/kafka_request_header";
import { KafkaFetchResponseBody } from "../../dtos/responses/kafka_fetch_resp_body";
import { KafkaFetchTopicItemResp } from "../../dtos/responses/kafka_fetch_topic_item_resp";
import { KafkaFetchTopicPartitionItemResp } from "../../dtos/responses/kafka_fetch_topic_partition_item_resp";
import { KafkaResponse } from "../../dtos/responses/kafka_response";
import { KafkaClusterMetadataLogFile } from "../metadata_log_file/kafka_cluster_metadata_log_file";
import IRequestHandler from "./interface_request_handler";

export class FetchRequestHandler extends IRequestHandler {
  constructor() {
    super(ResponseType.FETCH); // API key for FetchRequest
  }

  handleRequest(header: KafkaRequestHeader, reqData: Buffer): KafkaResponse {
    // Handle fetch request
    const request = KafkaFetchRequest.fromBuffer(reqData, header);

    const metadataLogFile = KafkaClusterMetadataLogFile.fromFile(
      "/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log"
    );

    const topicsInResponse = request.topics.map((topicReq) => {
      const partitionRecordsResponse = topicReq.partitions.map(
        (partitionRecord) =>
        {
          const matchedTopicRecord = metadataLogFile.getMatchTopicRecord(
            topicReq.topicId
          );
          console.log(`matchedTopicRecord: ${matchedTopicRecord?.name} - partitionIndex: ${partitionRecord.partitionId}`);
          return new KafkaFetchTopicPartitionItemResp(partitionRecord.partitionId, matchedTopicRecord)
        }
      );

      const topic = new KafkaFetchTopicItemResp(
        topicReq.topicId, // topicId
        partitionRecordsResponse
      );

      return topic;
    });

    const errorCode = ErrorCode.NO_ERROR;
    
    const body = new KafkaFetchResponseBody(
      0, // throttleTime
      errorCode, // errorCode
      0, // sessionId
      topicsInResponse
    );
    const response = new KafkaResponse(
      request.header.correlationId,
      0,
      body
    );
    
    return response;
  }
}