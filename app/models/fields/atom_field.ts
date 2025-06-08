import { readVarInt, writeVarInt, readSignedVarInt } from "../../utils/utils";

export type BufferFieldType = "string" | "uint8" | "uint16" | "uint32" | "uint64" | "varint" | "uvarint" | "compact_array" | "buffer";

interface BufferField {
  size: number;
  type: BufferFieldType;

  encode(): Buffer;
  
  decode(buffer: Buffer): void;
}

export class StringField implements BufferField {
  size: number;
  type: BufferFieldType = "string";
  value: string;

  constructor(value: string) {
    this.value = value;
    this.size = Buffer.byteLength(value, 'utf8'); // Size in bytes
  }

  encode(): Buffer {
    return Buffer.from(this.value, 'utf8');
  }

  decode(buffer: Buffer): void {
    this.value = buffer.toString('utf8');
    this.size = buffer.length; // Update size based on the buffer length
  }
}

export class UInt8Field implements BufferField {
  size: number;
  type: BufferFieldType = "uint8";
  value: number;

  constructor(value: number) {
    this.size = 1; // Size of a uint8 is always 1 byte
    this.value = value;
  }

  encode(): Buffer {
    const buffer = Buffer.alloc(this.size);
    buffer.writeUInt8(this.value, 0);
    return buffer;
  }

  decode(buffer: Buffer): void {
    this.value = buffer.readUInt8(0);
  }
}

export class UInt16Field implements BufferField {
  size: number;
  type: BufferFieldType = "uint16";
  value: number;

  constructor(value: number) {
    this.size = 2; // Size of a uint16 is always 2 bytes
    this.value = value;
  }

  encode(): Buffer {
    const buffer = Buffer.alloc(this.size);
    buffer.writeUInt16BE(this.value, 0);
    return buffer;
  }

  decode(buffer: Buffer): void {
    this.value = buffer.readUInt16BE(0);
  }
}

export class UInt32Field implements BufferField {
  size: number;
  type: BufferFieldType = "uint32";
  value: number;

  constructor(value: number) {
    this.size = 4; // Size of a uint32 is always 4 bytes
    this.value = value;
  }

  encode(): Buffer {
    const buffer = Buffer.alloc(this.size);
    buffer.writeUInt32BE(this.value, 0);
    return buffer;
  }

  decode(buffer: Buffer): void {
    this.value = buffer.readUInt32BE(0);
  }
}

export class UInt64Field implements BufferField {
  size: number;
  type: BufferFieldType = "uint64";
  value: bigint;

  constructor(value: bigint) {
    this.size = 8; // Size of a uint64 is always 8 bytes
    this.value = value;
  }

  encode(): Buffer {
    const buffer = Buffer.alloc(this.size);
    buffer.writeBigUInt64BE(this.value, 0);
    return buffer;
  }

  decode(buffer: Buffer): void {
    this.value = buffer.readBigUInt64BE(0);
  }
}

export class VarIntField implements BufferField {
  size: number;
  type: BufferFieldType = "varint";
  value: number;

  constructor(value: number) {
    this.value = value;
    this.size = this.calculateVarIntSize(value);
  }

  calculateVarIntSize(value: number): number {
    if (value < 0) {
      throw new Error("Negative values are not supported for VarIntField");
    }
    let size = 0;
    do {
      size++;
      value >>>= 7; // Shift right by 7 bits
    } while (value > 0);
    return size;
  }

  encode(): Buffer {

    return writeVarInt(this.value);
  }

  decode(buffer: Buffer): void {
    const { value, length } = readSignedVarInt(buffer);

    this.value = value; // Store the decoded value
    this.size = length; // Update size based on how many bytes were read
  }
}

export class UVarIntField implements BufferField {
  size: number;
  type: BufferFieldType = "uvarint";
  value: number;

  constructor(value: number) {
    this.value = value;
    this.size = this.calculateUVarIntSize(value);
  }

  calculateUVarIntSize(value: number): number {
    if (value < 0) {
      throw new Error("Negative values are not supported for UVarIntField");
    }
    let size = 0;
    do {
      size++;
      value >>>= 7; // Shift right by 7 bits
    } while (value > 0);
    return size;
  }

  encode(): Buffer {
    return writeVarInt(this.value);
  }

  decode(buffer: Buffer): void {
    const { value, length } = readVarInt(buffer);

    this.value = value; // Store the decoded value
    this.size = length; // Update size based on how many bytes were read
  }
}

export class CompactArrayField implements BufferField {
  size: number;
  type: BufferFieldType = "compact_array";
  fields: BufferField[];

  constructor(fields: BufferField[]) {
    this.fields = fields;
    this.size = this.calculateSize();
  }

  calculateSize(): number {
    return this.fields.reduce((total, field) => total + field.size, 0);
  }

  encode(): Buffer {
    const buffers = this.fields.map(field => field.encode());
    return Buffer.concat(buffers);
  }

  decode(buffer: Buffer): void {
    let offset = 0;
    for (const field of this.fields) {
      const fieldBuffer = buffer.slice(offset, offset + field.size);
      field.decode(fieldBuffer);
      offset += field.size;
    }
  }
}