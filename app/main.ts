import net from "net";
import { KafkaRequest } from "./kafka_request";
import { KafkaResponse } from "./kafka_response";
import { ErrorCode, ResponseType } from "./consts";
import { KafkaApiVersionsResponseBody } from "./kafka_api_version_resp";
import {
  KafkaDescribeTopicPartitionsRespBody,
  KafkaDescribeTopicPartitionsTopicItem,
} from "./kafka_describe_topic_partition_resp";
import { KafkaClusterMetadataLogFile } from "./models/kafka_cluster_metadata_log_file";
import { KafkaTopicPartitionItemResp } from "./models/kafka_topic_partition_item_resp";

const server: net.Server = net.createServer((connection: net.Socket) => {
  // Handle connection
  connection.on("data", (data: Buffer) => {
    // const args = process.argv.slice(2);
    // console.log("args: ", args);
    // const [metadataLogFilePath] = args;
    // console.log("metadataLogFilePath: ", metadataLogFilePath);

    const request = KafkaRequest.fromBuffer(data);
    request.debug();

    const apiVersion = request.header.apiVersion;
    switch (request.header.apiKey) {
      case ResponseType.API_VERSIONS:
        {
          const errorCode =
            apiVersion < 0 || apiVersion > 4
              ? ErrorCode.UNSUPPORTED_VERSION
              : ErrorCode.NO_ERROR;
          const body = new KafkaApiVersionsResponseBody(
            errorCode,
            request.header.apiKey,
            request.header.apiVersion
          );
          const response = new KafkaResponse(
            request.header.correlationId,
            undefined,
            body
          );

          connection.write(response.toBuffer());
        }
        break;
      case ResponseType.DESCRIBE_TOPIC_PARTITIONS:
        {
          // Read content of metadata log file to KafkaClusterMetadataLogFile
          const metadataLogFile = KafkaClusterMetadataLogFile.fromFile(
            "/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log"
          );
          // console.log("metadataLogFile: ", metadataLogFile.debugString());
          const topicRecords = metadataLogFile.getTopicRecords();
          console.log(`topicRecords: ${topicRecords.length}`);

          const firstRequestTopic = request.topics[0];

          const matchTopicRecord = topicRecords.find((record) => {
            // console.log(`record: ${record.debugString()}`);
            return record.name === firstRequestTopic.topicName;
          });

          const errorCode =
            matchTopicRecord !== undefined
              ? ErrorCode.NO_ERROR
              : ErrorCode.UNKNOWN_TOPIC_OR_PARTITION;

          const topicId = matchTopicRecord?.uuid ?? Buffer.alloc(16);

          const partitionRecords =
            metadataLogFile.getPartitionRecordsMatchTopicUuid(topicId);
          const partitionRecordsResponse = partitionRecords.map(
            (partitionRecord, index) =>
              KafkaTopicPartitionItemResp.fromLogRecord(partitionRecord, index)
          );

          const topic = new KafkaDescribeTopicPartitionsTopicItem(
            errorCode,
            firstRequestTopic.topicName,
            topicId,
            false,
            partitionRecordsResponse,
            0,
            0
          );

          const body = new KafkaDescribeTopicPartitionsRespBody(0, 0, 0, [
            topic,
          ]);
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
