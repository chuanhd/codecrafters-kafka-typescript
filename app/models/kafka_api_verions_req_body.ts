import { type BufferFieldDefinition } from "../types.declaration";

export enum FieldName {
  ClientIdLength = "clientIdLength",
  ClientId = "clientId",
  ClientSoftwareVersionLength = "clientSoftwareVersionLength",
  ClientSoftware = "clientSoftware",
  TagBuffer = "tagBuffer",
}

const kafkaApiVersionsRequestBodyDef: Array<BufferFieldDefinition> = [
  {
    name: FieldName.ClientIdLength,
    size: 1,
    type: "number",
  },
  {
    name: FieldName.ClientId,
    size: FieldName.ClientIdLength, // depends on "clientIdLength"
    type: "string",
  },
  {
    name: FieldName.ClientSoftwareVersionLength,
    size: 1,
    type: "number",
  },
  {
    name: FieldName.ClientSoftware,
    size: FieldName.ClientSoftwareVersionLength,
    type: "string",
  },
  {
    name: FieldName.TagBuffer,
    size: 1,
    type: "number",
  },
];

export class KafkaApiVersionsRequestBody {
  private fields: Map<string, string | number>;
  private static StructureDefinition: Array<BufferFieldDefinition> =
    kafkaApiVersionsRequestBodyDef;

  constructor() {
    this.fields = new Map<string, string | number>();
  }

  public static fromBuffer(
    data: Buffer,
    startOffset: number,
  ): KafkaApiVersionsRequestBody {
    const body = new KafkaApiVersionsRequestBody();
    let currentOffset = startOffset;

    KafkaApiVersionsRequestBody.StructureDefinition.forEach((field) => {
      if (typeof field.size === "number") {
        const value = data.readUIntBE(currentOffset, field.size);
        currentOffset += field.size;
        body.fields.set(field.name, value);
      } else {
        // Read content of referenced field
        const referencedField = field.size as string;
        const realBufferSize = body.fields.get(referencedField);
        if (realBufferSize == null) {
          throw new Error(`Referenced field ${referencedField} not found`);
        }
        if (typeof realBufferSize !== "number") {
          throw new Error("Referenced field is not a number");
        }
        const buffer = Buffer.alloc(realBufferSize);
        const value = buffer.toString(
          "utf-8",
          currentOffset,
          currentOffset + realBufferSize,
        );
        body.fields.set(field.name, value);
        currentOffset += realBufferSize;
      }
    });

    return body;
  }
}
