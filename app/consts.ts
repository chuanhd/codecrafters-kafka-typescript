export const enum ErrorCode {
  UNSUPPORTED_VERSION = 35,
  UNKNOWN_TOPIC_OR_PARTITION = 3,
  NO_ERROR = 0,
}

export const enum ResponseType {
  NONE = 0,
  FETCH = 1,
  API_VERSIONS = 18,
  DESCRIBE_TOPIC_PARTITIONS = 75,
}
