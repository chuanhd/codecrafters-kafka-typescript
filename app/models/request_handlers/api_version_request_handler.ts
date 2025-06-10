import { ErrorCode, ResponseType } from "../../consts";
import { KafkaRequestHeader } from "../../dtos/requests/kafka_request_header";
import { KafkaApiVersionsResponseBody } from "../../dtos/responses/kafka_api_version_resp";
import { KafkaResponse } from "../../dtos/responses/kafka_response";
import IRequestHandler from "./interface_request_handler";

export class APIVersionRequestHandler extends IRequestHandler {
  constructor() {
    super(ResponseType.API_VERSIONS); // API key for ApiVersionsRequest
  }

  handleRequest(header: KafkaRequestHeader, _request: Buffer): KafkaResponse {
    const apiVersion = header.apiVersion;
    const errorCode =
                apiVersion < 0 || apiVersion > 4
                  ? ErrorCode.UNSUPPORTED_VERSION
                  : ErrorCode.NO_ERROR;
    const responseBody = new KafkaApiVersionsResponseBody(
      errorCode,
      header.apiKey,
      header.apiVersion
    );
    const response = new KafkaResponse(
      header.correlationId,
      undefined,
      responseBody
    );
    return response;
  }
}