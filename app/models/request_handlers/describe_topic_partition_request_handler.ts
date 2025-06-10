import { ErrorCode, ResponseType } from "../../consts";
import { KafkaDescribePartitionRequest } from "../../dtos/requests/kafka_describe_partition_req";
import { KafkaRequestHeader } from "../../dtos/requests/kafka_request_header";
import { KafkaDescribeTopicPartitionsRespBody, KafkaDescribeTopicPartitionsTopicItem } from "../../dtos/responses/kafka_describe_topic_partition_resp";
import { KafkaResponse } from "../../dtos/responses/kafka_response";
import { KafkaTopicPartitionItemResp } from "../../dtos/responses/kafka_topic_partition_item_resp";
import { KafkaClusterMetadataLogFile } from "../metadata_log_file/kafka_cluster_metadata_log_file";
import IRequestHandler from "./interface_request_handler";

export class DescribeTopicPartitionRequestHandler extends IRequestHandler {
  constructor() {
    super(ResponseType.DESCRIBE_TOPIC_PARTITIONS); // API key for DescribeTopicPartitionsRequest
  }

  handleRequest(header: KafkaRequestHeader, reqData: Buffer): KafkaResponse {
    const request = KafkaDescribePartitionRequest.fromBuffer(reqData, header);
    // Read content of metadata log file to KafkaClusterMetadataLogFile
    const metadataLogFile = KafkaClusterMetadataLogFile.fromFile(
      "/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log"
    );
    const topicRecords = metadataLogFile.getTopicRecords();
    console.log(`topicRecords: ${topicRecords.length}`);

    const topics = request.topics.map((topicReq) => {
      const matchingTopicRecord = topicRecords.find(
        (topicRecord) => topicRecord.name === topicReq.topicName
      );

      const errorCode =
        matchingTopicRecord !== undefined
          ? ErrorCode.NO_ERROR
          : ErrorCode.UNKNOWN_TOPIC_OR_PARTITION;

      const topicId = matchingTopicRecord?.uuid ?? Buffer.alloc(16);

      const partitionRecords =
        metadataLogFile.getPartitionRecordsMatchTopicUuid(topicId);
      const partitionRecordsResponse = partitionRecords.map(
        (partitionRecord, index) =>
          KafkaTopicPartitionItemResp.fromLogRecord(
            partitionRecord,
            index
          )
      );

      const topic = new KafkaDescribeTopicPartitionsTopicItem(
        errorCode,
        topicReq.topicName,
        topicId,
        false,
        partitionRecordsResponse,
        0,
        0
      );

      return topic;
    });

    const body = new KafkaDescribeTopicPartitionsRespBody(
      0,
      0,
      0,
      topics
    );
    const response = new KafkaResponse(
      request.header.correlationId,
      0,
      body
    );

    return response;
  }
}