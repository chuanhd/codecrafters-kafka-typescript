import net from "net";
import { KafkaRequest } from "./kafka_request";
import { KafkaResponse } from "./kafka_response";
import { ErrorCode, ResponseType } from "./consts";
import { KafkaApiVersionsResponseBody } from "./kafka_api_version_resp";
import {
  KafkaDescribeTopicPartitionsRespBody,
  KafkaDescribeTopicPartitionsTopicItem,
} from "./kafka_describe_topic_partition_resp";

const server: net.Server = net.createServer((connection: net.Socket) => {
  // Handle connection
  connection.on("data", (data: Buffer) => {
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
            request.header.apiVersion,
          );
          const response = new KafkaResponse(
            request.header.correlationId,
            body,
          );

          connection.write(response.toBuffer());
        }
        break;
      case ResponseType.DESCRIBE_TOPIC_PARTITIONS:
        {
          const errorCode = ErrorCode.UNKNOWN_TOPIC_OR_PARTITION;
          const firstRequestTopic = request.topics[0];
          const topic = new KafkaDescribeTopicPartitionsTopicItem(
            errorCode,
            firstRequestTopic.topicName,
            "0",
            false,
            0,
            0,
            0,
          );
          const body = new KafkaDescribeTopicPartitionsRespBody(0, 0, 0, [
            topic,
          ]);
          const response = new KafkaResponse(
            request.header.correlationId,
            body,
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
