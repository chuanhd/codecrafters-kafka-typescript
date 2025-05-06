import net from "net";
import { KafkaRequest } from "./kafka_request";
import { KafkaResponse } from "./kafka_response";
import { ErrorCode } from "./consts";

// Uncomment this block to pass the first stage
const server: net.Server = net.createServer((connection: net.Socket) => {
  // Handle connection
  connection.on("data", (data: Buffer) => {
    const request = KafkaRequest.fromBuffer(data);
    request.debug();

    const errorCode =
      request.requestApiVersion < 0 || request.requestApiVersion > 4
        ? ErrorCode.UNSUPPORTED_VERSION
        : 0;

    const response = new KafkaResponse(request.correllationId, errorCode);

    connection.write(response.toBuffer());
  });
});

server.listen(9092, "127.0.0.1");
