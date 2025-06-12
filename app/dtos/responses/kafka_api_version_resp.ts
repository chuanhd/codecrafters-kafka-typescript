import { ResponseType } from "../../utils/consts.ts";

class KafkaApiVersionItemBody {
  apiKey: number;
  minSupportVersion: number;
  maxSupportVersion: number;
  tagBuffer: number;

  constructor(
    apiKey: number,
    minSupportVersion: number,
    maxSupportVersion: number,
    tagBuffer: number
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
      KafkaApiVersionItemBody.minSupportVersionBufferSize
    );
    minSupportVersionBuffer.writeUInt16BE(this.minSupportVersion);

    const maxSupportVersionBuffer = Buffer.alloc(
      KafkaApiVersionItemBody.maxSupportVersionBufferSize
    );
    maxSupportVersionBuffer.writeUInt16BE(this.maxSupportVersion);

    const tagBufferBuffer = Buffer.alloc(
      KafkaApiVersionItemBody.tagBufferBufferSize
    );
    tagBufferBuffer.writeUIntBE(
      this.tagBuffer,
      0,
      KafkaApiVersionItemBody.tagBufferBufferSize
    );

    return Buffer.concat([
      apiKeyBuffer,
      minSupportVersionBuffer,
      maxSupportVersionBuffer,
      tagBufferBuffer,
    ]);
  }
}

class KafkaApiVersionsArrayBody {
  length: number;
  apiVersionItems: Array<KafkaApiVersionItemBody>;

  constructor(apiVersions: Array<KafkaApiVersionItemBody>) {
    this.length = apiVersions.length + 1;
    this.apiVersionItems = apiVersions;
  }

  private static lengthBufferSize = 1;

  public getBufferSize() {
    const apiVersionItemsBufferSize = this.apiVersionItems.reduce(
      (prev, cur) => {
        return prev + cur.getBufferSize();
      },
      0
    );

    return (
      KafkaApiVersionsArrayBody.lengthBufferSize + apiVersionItemsBufferSize
    );
  }

  public toBuffer() {
    const lengthBuffer = Buffer.alloc(1);
    lengthBuffer.writeUInt8(this.length);

    console.log("[KafkaApiVersionsArrayBody] length: ", this.length);

    const itemBuffers = this.apiVersionItems.map((item) => item.toBuffer());

    return Buffer.concat([lengthBuffer, ...itemBuffers]);
  }
}

export class KafkaApiVersionsResponseBody {
  errorCode: number;
  apiKey: number;
  apiVersion: number;
  private apiVersionsArray: KafkaApiVersionsArrayBody;
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
        this.apiVersionsArray = new KafkaApiVersionsArrayBody([]);
        break;
      case ResponseType.API_VERSIONS:
        {
          const apiVersionItem = new KafkaApiVersionItemBody(18, 0, 4, 0);
          const describeTopicPartitionsItem = new KafkaApiVersionItemBody(
            75,
            0,
            0,
            0
          );
          const fetchItem = new KafkaApiVersionItemBody(1, 0, 16, 0);
          this.apiVersionsArray = new KafkaApiVersionsArrayBody([
            apiVersionItem,
            describeTopicPartitionsItem,
            fetchItem,
          ]);
        }
        break;
      case ResponseType.DESCRIBE_TOPIC_PARTITIONS:
        {
          const describeTopicPartitionsItem = new KafkaApiVersionItemBody(
            75,
            0,
            0,
            0
          );
          this.apiVersionsArray = new KafkaApiVersionsArrayBody([
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
      KafkaApiVersionsResponseBody.errorCodeBufferSize + // 2
      this.apiVersionsArray.getBufferSize() + // 14
      KafkaApiVersionsResponseBody.throttleTimeBufferSize + // 4
      KafkaApiVersionsResponseBody.tagBufferBufferSize // 1
    );
  }

  public toBuffer() {
    const errorCodeBuffer = Buffer.alloc(
      KafkaApiVersionsResponseBody.errorCodeBufferSize
    );
    errorCodeBuffer.writeUInt16BE(this.errorCode);

    const throttleTimeBuffer = Buffer.alloc(
      KafkaApiVersionsResponseBody.throttleTimeBufferSize
    );
    throttleTimeBuffer.writeUInt32BE(this.throttleTime);

    const tagBufferBuffer = Buffer.alloc(
      KafkaApiVersionsResponseBody.tagBufferBufferSize
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
