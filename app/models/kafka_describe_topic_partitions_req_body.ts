import { type BufferFieldDefinition } from "../types.declaration";

export enum FieldName {
  TopicsArrayLength = "TopicsArrayLength",
  TopicsArray = "TopicsArray",
  TopicNameLength = "TopicNameLength",
  TopicName = "TopicName",
  TopicTagBuffer = "TopicTagBuffer",
  ResponsePartitionLimit = "ResponsePartitionLimit",
  Cursor = "Cursor",
  TagBuffer = "TagBuffer",
}

const kafkaApiVersionsRequestBodyDef: Array<BufferFieldDefinition> = [
  {
    name: FieldName.TopicsArrayLength,
    size: 1,
    type: "number",
  },
  {
    name: FieldName.TopicsArray,
    size: FieldName.TopicsArrayLength,
    type: "array",
    fields: [
      {
        name: FieldName.TopicNameLength,
        size: 1,
        type: "number",
      },
      {
        name: FieldName.TopicName,
        size: FieldName.TopicNameLength,
        type: "string",
      },
      {
        name: FieldName.TopicTagBuffer,
        size: 1,
        type: "number",
      },
    ],
  },
  {
    name: FieldName.ResponsePartitionLimit,
    size: 4,
    type: "number",
  },
  {
    name: FieldName.Cursor,
    size: 1,
    type: "number",
  },
  {
    name: FieldName.TagBuffer,
    size: 1,
    type: "number",
  },
];

class Wrapper<T> {
  constructor(public value: T) {}
}

const readNumberAtomField = (
  field: BufferFieldDefinition,
  data: Buffer,
  currentOffset: Wrapper<number>,
  result: Map<string, string | number>,
) => {
  if (field.type !== "number") {
    throw new Error("Not support other than number type");
  }
  console.log(
    `[readNumberAtomField] field: ${JSON.stringify(field)} - currentOffset: ${currentOffset.value}`,
  );
  if (typeof field.size === "number") {
    const value = data.readUIntBE(currentOffset.value, field.size);
    currentOffset.value += field.size;
    result.set(field.name, value);

    console.log(
      `[readNumberAtonField] after read currentOffset: ${currentOffset.value} - value: ${value}`,
    );
  } else {
    console.log("Size of a number field should be a number");
    console.log(`fields: ${JSON.stringify(field)}`);
  }

  return result;
};

const readStringAtomField = (
  field: BufferFieldDefinition,
  data: Buffer,
  currentOffset: Wrapper<number>,
  result: Map<string, string | number>,
) => {
  let realBufferSize = 0;
  if (typeof field.size === "number") {
    realBufferSize = field.size;
  } else {
    // Read content of referenced field
    const referencedField = field.size as string;
    const referencedFieldValue = result.get(referencedField);
    if (referencedFieldValue == null) {
      throw new Error(`Referenced field ${referencedField} not found`);
    }
    if (typeof referencedFieldValue !== "number") {
      throw new Error("Referenced field is not a number");
    }
    realBufferSize = referencedFieldValue;
  }
  const value = data.toString(
    "utf-8",
    currentOffset.value,
    currentOffset.value + realBufferSize,
  );
  result.set(field.name, value);
  currentOffset.value += realBufferSize;

  return result;
};

const readAtomField = (
  field: BufferFieldDefinition,
  data: Buffer,
  currentOffset: Wrapper<number>,
  result: Map<string, string | number>,
) => {
  switch (field.type) {
    case "number":
      {
        readNumberAtomField(field, data, currentOffset, result);
      }
      break;
    case "string":
      {
        readStringAtomField(field, data, currentOffset, result);
      }
      break;
    default:
      throw new Error(
        `readAtomField only support number or string field type: ${field.type}`,
      );
  }

  return result;
};

type FieldValueType = string | number | Map<string, string | number>;

export class KafkaDescribeTopicPartitionsRequestBody {
  private fields: Map<string, FieldValueType>;
  private static StructureDefinition = kafkaApiVersionsRequestBodyDef;

  constructor() {
    this.fields = new Map<string, string | number>();
  }

  public static fromBuffer(
    data: Buffer,
    startOffset: number,
  ): KafkaDescribeTopicPartitionsRequestBody {
    const body = new KafkaDescribeTopicPartitionsRequestBody();
    let currentOffset = startOffset;

    KafkaDescribeTopicPartitionsRequestBody.StructureDefinition.forEach(
      (field) => {
        switch (field.type) {
          case "number":
            {
              if (typeof field.size === "number") {
                const value = data.readUIntBE(currentOffset, field.size);
                currentOffset += field.size;
                body.fields.set(field.name, value);
              } else {
                console.log("Size of a number field should be a number");
                console.log(`fields: ${JSON.stringify(field)}`);
              }
            }
            break;
          case "string":
            {
              let realBufferSize = 0;
              if (typeof field.size === "number") {
                realBufferSize = field.size;
              } else {
                // Read content of referenced field
                const referencedField = field.size as string;
                const referencedFieldValue = body.fields.get(referencedField);
                if (referencedFieldValue == null) {
                  throw new Error(
                    `Referenced field ${referencedField} not found`,
                  );
                }
                if (typeof referencedFieldValue !== "number") {
                  throw new Error("Referenced field is not a number");
                }
                realBufferSize = referencedFieldValue;
              }
              const value = data.toString(
                "utf-8",
                currentOffset,
                currentOffset + realBufferSize,
              );
              body.fields.set(field.name, value);
              currentOffset += realBufferSize;
            }
            break;
          case "array":
            {
              // Get array size by reading reference field value
              // Read content of referenced field
              const referencedField = field.size as string;
              const referencedFieldValue = body.fields.get(referencedField);
              if (referencedFieldValue == null) {
                throw new Error(
                  `Referenced field ${referencedField} not found`,
                );
              }
              if (typeof referencedFieldValue !== "number") {
                throw new Error("Referenced field is not a number");
              }
              const arraySize = referencedFieldValue;
              let readIndex = 0;
              while (readIndex < arraySize) {
                const subFields = field.fields as Array<BufferFieldDefinition>;
                let subResult = new Map<string, string | number>();
                for (let index = 0; index < subFields.length; index++) {
                  const subField = subFields[index];
                  const wrappedOffset = new Wrapper<number>(currentOffset);
                  readAtomField(subField, data, wrappedOffset, subResult);
                }

                body.fields.set(`${field.name}[${readIndex}]`, subResult);

                readIndex++;
              }
            }
            break;
          default:
            throw new Error(`Unknown field type: ${field.type}`);
        }
      },
    );

    return body;
  }
}
