import type { KafkaRequestHeader } from "../../dtos/requests/kafka_request_header";
import type { KafkaResponse } from "../../dtos/responses/kafka_response";

abstract class IRequestHandler {
  private _apiKey: number;

  constructor(apiKey: number) {
    this._apiKey = apiKey;
  }

  public get apiKey(): number {
    return this._apiKey;
  }

  /**
   * Handles a request and returns a response.
   * @param header - The Kafka request header containing metadata.
   * @param reqData - The request data as a Buffer.
   * @returns The response data as a Buffer.
   */
  abstract handleRequest(header: KafkaRequestHeader, reqData: Buffer): KafkaResponse;
}

export default IRequestHandler;