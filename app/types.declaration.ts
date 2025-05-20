export type BufferFieldType = "string" | "number" | "array";

export type BufferFieldDefinition = {
  name: string;
  size: number | string;
  type: BufferFieldType;
  fields?: Array<BufferFieldDefinition>;
};

export type ArrayFieldDefinition = {
  name: string;
  size: number;
  fields: Array<BufferFieldDefinition>;
};
