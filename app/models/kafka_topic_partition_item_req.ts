export class KafkaTopicPartitionItemReq {
  topicNameLength: number;
  topicName: string;
  tagBuffer: number;

  constructor(topicNameLength: number, topicName: string, tagBuffer: number) {
    this.topicNameLength = topicNameLength;
    this.topicName = topicName;
    this.tagBuffer = tagBuffer;
  }

  public debug() {
    console.log(
      `[RequestTopic] topicNameLength: ${this.topicNameLength} topicName: ${this.topicName} tagBuffer: ${this.tagBuffer}`
    );
  }
}
