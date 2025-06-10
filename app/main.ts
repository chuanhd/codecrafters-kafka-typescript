import net from "net";
import { KafkaRequestHandlerCenter } from "./request_handlers";
import { APIVersionRequestHandler } from "./models/request_handlers/api_version_request_handler";
import { DescribeTopicPartitionRequestHandler } from "./models/request_handlers/describe_topic_partition_request_handler";
import { FetchRequestHandler } from "./models/request_handlers/fetch_request_handler";

const server: net.Server = net.createServer((connection: net.Socket) => {
  const requestHandlerCenter = new KafkaRequestHandlerCenter();
  requestHandlerCenter
    .registerHandler(new APIVersionRequestHandler())
    .registerHandler(new DescribeTopicPartitionRequestHandler())
    .registerHandler(new FetchRequestHandler());

  // Handle connection
  connection.on("data", (data: Buffer) => {
    const response = requestHandlerCenter.handleRequest(data);
    connection.write(response);
  });
});

server.listen(9092, "127.0.0.1");
