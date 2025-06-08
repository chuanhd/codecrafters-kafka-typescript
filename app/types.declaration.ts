export type BufferFieldType = "string" | "uint8" | "uint16" | "uint32" | "uint64" | "varint" | "uvarint" | "compact_array";

interface BufferField {
  name: string;
  size: number | string;
  type: BufferFieldType;

  encode(): Buffer;
  
  decode(buffer: Buffer): void;
}

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

