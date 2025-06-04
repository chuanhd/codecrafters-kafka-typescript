export const enum ErrorCode {
  UNKNOWN_TOPIC = 100,
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

export const enum MetadataRecordType {
  TOPIC = 2,
  PARTITION = 3,
  FEATURE_LEVEL = 12
}
