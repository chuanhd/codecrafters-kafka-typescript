import { ResponseType } from "./consts.ts";

class KafkaApiVersionItemBody {
  apiKey: number;
  minSupportVersion: number;
  maxSupportVersion: number;
  tagBuffer: number;

  constructor(
    apiKey: number,
    minSupportVersion: number,
    maxSupportVersion: number,
    tagBuffer: number,
  ) {
    this.apiKey = apiKey;
    this.minSupportVersion = minSupportVersion;
    this.maxSupportVersion = maxSupportVersion;
    this.tagBuffer = tagBuffer;
  }

  private static apiKeyBufferSize = 2;
  private static minSupportVersionBufferSize = 2;
  private static maxSupportVersionBufferSize = 2;
  private static tagBufferBufferSize = 1;

  public getBufferSize() {
    return (
      KafkaApiVersionItemBody.apiKeyBufferSize +
      KafkaApiVersionItemBody.minSupportVersionBufferSize +
      KafkaApiVersionItemBody.maxSupportVersionBufferSize +
      KafkaApiVersionItemBody.tagBufferBufferSize
    );
  }

  public toBuffer() {
    const apiKeyBuffer = Buffer.alloc(KafkaApiVersionItemBody.apiKeyBufferSize);
    apiKeyBuffer.writeUInt16BE(this.apiKey);

    const minSupportVersionBuffer = Buffer.alloc(
      KafkaApiVersionItemBody.minSupportVersionBufferSize,
    );
    minSupportVersionBuffer.writeUInt16BE(this.minSupportVersion);

    const maxSupportVersionBuffer = Buffer.alloc(
      KafkaApiVersionItemBody.maxSupportVersionBufferSize,
    );
    maxSupportVersionBuffer.writeUInt16BE(this.maxSupportVersion);

    const tagBufferBuffer = Buffer.alloc(
      KafkaApiVersionItemBody.tagBufferBufferSize,
    );
    tagBufferBuffer.writeUIntBE(
      this.tagBuffer,
      0,
      KafkaApiVersionItemBody.tagBufferBufferSize,
    );

    return Buffer.concat([
      apiKeyBuffer,
      minSupportVersionBuffer,
      maxSupportVersionBuffer,
      tagBufferBuffer,
    ]);
  }
}

class KafkaApiVerionsArrayBody {
  length: number;
  apiVersionItems: Array<KafkaApiVersionItemBody>;

  constructor(apiVerions: Array<KafkaApiVersionItemBody>) {
    this.length = apiVerions.length + 1;
    this.apiVersionItems = apiVerions;
  }

  private static lengthBufferSize = 1;

  public getBufferSize() {
    const apiVersionItemsBufferSize = this.apiVersionItems.reduce(
      (prev, cur) => {
        return prev + cur.getBufferSize();
      },
      0,
    );

    return (
      KafkaApiVerionsArrayBody.lengthBufferSize + apiVersionItemsBufferSize
    );
  }

  public toBuffer() {
    const lengthBuffer = Buffer.alloc(1);
    lengthBuffer.writeUInt8(this.length);

    const itemBuffers = this.apiVersionItems.map((item) => item.toBuffer());

    return Buffer.concat([lengthBuffer, ...itemBuffers]);
  }
}

export class KafkaApiVersionsResponseBody {
  errorCode: number;
  apiKey: number;
  apiVersion: number;
  private apiVersionsArray: KafkaApiVerionsArrayBody;
  throttleTime: number;
  tagBuffer: number;

  constructor(errorCode: number, apiKey: number, apiVersion: number) {
    this.errorCode = errorCode;
    this.apiKey = apiKey;
    this.apiVersion = apiVersion;
    this.throttleTime = 0;
    this.tagBuffer = 0;

    switch (this.apiKey) {
      case ResponseType.NONE:
        this.apiVersionsArray = new KafkaApiVerionsArrayBody([]);
        break;
      case ResponseType.API_VERSIONS:
        {
          const apiVersionItem = new KafkaApiVersionItemBody(18, 0, 4, 0);
          const describeTopicPartitionsItem = new KafkaApiVersionItemBody(
            75,
            0,
            0,
            0,
          );
          this.apiVersionsArray = new KafkaApiVerionsArrayBody([
            apiVersionItem,
            describeTopicPartitionsItem,
          ]);
        }
        break;
      case ResponseType.DESCRIBE_TOPIC_PARTITIONS:
        {
          const describeTopicPartitionsItem = new KafkaApiVersionItemBody(
            75,
            0,
            0,
            0,
          );
          this.apiVersionsArray = new KafkaApiVerionsArrayBody([
            describeTopicPartitionsItem,
          ]);
        }
        break;
      default:
        throw "Unsupported API key";
    }
  }

  private static errorCodeBufferSize = 2;
  private static throttleTimeBufferSize = 4;
  private static tagBufferBufferSize = 1;

  public getBufferSize() {
    return (
      KafkaApiVersionsResponseBody.errorCodeBufferSize +
      KafkaApiVersionsResponseBody.throttleTimeBufferSize +
      KafkaApiVersionsResponseBody.tagBufferBufferSize +
      this.apiVersionsArray.getBufferSize()
    );
  }

  public toBuffer() {
    const errorCodeBuffer = Buffer.alloc(
      KafkaApiVersionsResponseBody.errorCodeBufferSize,
    );
    errorCodeBuffer.writeUInt16BE(this.errorCode);

    const throttleTimeBuffer = Buffer.alloc(
      KafkaApiVersionsResponseBody.throttleTimeBufferSize,
    );
    throttleTimeBuffer.writeUInt32BE(this.throttleTime);

    const tagBufferBuffer = Buffer.alloc(
      KafkaApiVersionsResponseBody.tagBufferBufferSize,
    );
    tagBufferBuffer.writeUInt8(this.tagBuffer);

    return Buffer.concat([
      errorCodeBuffer,
      this.apiVersionsArray.toBuffer(),
      throttleTimeBuffer,
      tagBufferBuffer,
    ]);
  }
}
