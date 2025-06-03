import { KafkaDescribePartitionRequest } from "./models/kafka_describe_partition_req";
import net from "net";
import { KafkaResponse } from "./kafka_response";
import { ErrorCode, ResponseType } from "./consts";
import { KafkaApiVersionsResponseBody } from "./kafka_api_version_resp";
import {
  KafkaDescribeTopicPartitionsRespBody,
  KafkaDescribeTopicPartitionsTopicItem,
} from "./kafka_describe_topic_partition_resp";
import { KafkaClusterMetadataLogFile } from "./models/kafka_cluster_metadata_log_file";
import { KafkaTopicPartitionItemResp } from "./models/kafka_topic_partition_item_resp";
import { KafkaFetchResponseBody } from "./models/kafka_fetch_resp_body";
import { KafkaFetchTopicPartitionItemResp } from "./models/kafka_fetch_topic_partition_item_resp";
import { KafkaRequestHeader } from "./models/kafka_request_header";
import { KafkaFetchRequest } from "./models/kafka_fetch_request";
import { KafkaFetchTopicItemResp } from "./models/kafka_fetch_topic_item_resp";

const server: net.Server = net.createServer((connection: net.Socket) => {
  // Handle connection
  connection.on("data", (data: Buffer) => {
    const header = KafkaRequestHeader.fromBuffer(data.subarray(4));

    const apiVersion = header.apiVersion;
    switch (header.apiKey) {
      case ResponseType.API_VERSIONS:
        {
          const errorCode =
            apiVersion < 0 || apiVersion > 4
              ? ErrorCode.UNSUPPORTED_VERSION
              : ErrorCode.NO_ERROR;
          const body = new KafkaApiVersionsResponseBody(
            errorCode,
            header.apiKey,
            header.apiVersion
          );
          const response = new KafkaResponse(
            header.correlationId,
            undefined,
            body
          );

          connection.write(response.toBuffer());
        }
        break;
      case ResponseType.DESCRIBE_TOPIC_PARTITIONS:
        {
          const request = KafkaDescribePartitionRequest.fromBuffer(data);
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

          connection.write(response.toBuffer());
        }
        break;
      case ResponseType.FETCH:
        {
          // Handle fetch request
          const request = KafkaFetchRequest.fromBuffer(data);
          const errorCode = ErrorCode.NO_ERROR;
          const topicsInResponse = request.topics.map((topic) => {
            const partitions = topic.partitions.map(
              (_partition, index) =>
                new KafkaFetchTopicPartitionItemResp(index, 100) // Assuming 100 as the error code
            );
            return new KafkaFetchTopicItemResp(
              topic.topicId, // topicId
              partitions
            );
          });
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

          connection.write(response.toBuffer());
        }
        break;
      default:
        console.error("Unsupported API key");
        break;
    }
  });
});

server.listen(9092, "127.0.0.1");
