import { UInt8Field, VarIntField } from "../fields/atom_field";
import type { Offset } from "../wrapper";
import { decodeSigned } from "../../utils/signed_varint";

export class KafkaPartitionRecordItem {
  public length: VarIntField;
  public attributes: UInt8Field
  public timestampDelta: VarIntField;
  public offsetDelta: VarIntField;
  public keyLength: VarIntField;
  public key: Buffer;
  public valueLength: VarIntField;
  public value: Buffer;
  public headerLength: VarIntField;
  public headers: Buffer;

  constructor() {
    this.length = new VarIntField(0);
    this.attributes = new UInt8Field(0);
    this.timestampDelta = new VarIntField(0);
    this.offsetDelta = new VarIntField(0);
    this.keyLength = new VarIntField(0);
    this.key = Buffer.alloc(0); // Key can be undefined
    this.valueLength = new VarIntField(0);
    this.value = Buffer.alloc(0); // Default to empty buffer
    this.headerLength = new VarIntField(0);
    this.headers = Buffer.alloc(0); // Default to empty buffer 
  }

  decode(buffer: Buffer, offset: Offset): void {
    let currentOffset = offset.value;

    this.length.decode(buffer.subarray(currentOffset));
    console.log(`Decoded length: ${this.length.value} at offset ${currentOffset}`);
    currentOffset += this.length.size;

    this.attributes.decode(buffer.subarray(currentOffset));
    console.log(`Decoded attributes: ${this.attributes.value} at offset ${currentOffset}`);
    currentOffset += this.attributes.size;

    this.timestampDelta.decode(buffer.subarray(currentOffset));
    console.log(`Decoded timestampDelta: ${this.timestampDelta.value} at offset ${currentOffset}`);
    currentOffset += this.timestampDelta.size;

    this.offsetDelta.decode(buffer.subarray(currentOffset));
    console.log(`Decoded offsetDelta: ${this.offsetDelta.value} at offset ${currentOffset}`);
    currentOffset += this.offsetDelta.size;

    this.keyLength.decode(buffer.subarray(currentOffset));
    const keyLength = decodeSigned(buffer.subarray(currentOffset));
    currentOffset += this.keyLength.size;

    if (this.keyLength.value > 0) {
      this.key = buffer.subarray(currentOffset, currentOffset + this.keyLength.value);
      // currentOffset += this.keyLength.value;
    } else {
      this.key = Buffer.alloc(0); // Handle undefined key
    }

    this.valueLength.decode(buffer.subarray(currentOffset));
    currentOffset += this.valueLength.size;

    if (this.valueLength.value > 0) {
      this.value = buffer.subarray(currentOffset, currentOffset + this.valueLength.value);
      currentOffset += this.valueLength.value;
    } else {
      this.value = Buffer.alloc(0); // Default to empty buffer
    }

    this.headerLength.decode(buffer.subarray(currentOffset));
    currentOffset += this.headerLength.size;

    if (this.headerLength.value > 0) {
      this.headers = buffer.subarray(currentOffset, currentOffset + this.headerLength.value);
      currentOffset += this.headerLength.value;
    } else {
      this.headers = Buffer.alloc(0); // Default to empty buffer
    }

    offset.value = currentOffset; // Update the offset
  }

  encode(): Buffer {
    const buffers: Buffer[] = [];
    buffers.push(this.length.encode());
    buffers.push(this.attributes.encode());
    buffers.push(this.timestampDelta.encode());
    buffers.push(this.offsetDelta.encode());
    buffers.push(this.keyLength.encode());
    buffers.push(this.key);
    buffers.push(this.valueLength.encode());
    buffers.push(this.value);
    buffers.push(this.headerLength.encode());
    buffers.push(this.headers);

    const result = Buffer.concat(buffers);

    return result;
  }

  debugString(): string {
    return `KafkaPartitionRecordItem {
      length: ${this.length.value},
      attributes: ${this.attributes.value},
      timestampDelta: ${this.timestampDelta.value},
      offsetDelta: ${this.offsetDelta.value},
      keyLength: ${this.keyLength.value},
      key: ${this.key !== undefined ? this.key.toString("hex") : "undefined"},
      valueLength: ${this.valueLength.value},
      value: ${this.value.toString()},
      headerLength: ${this.headerLength.value},
      headers: ${this.headers.toString("hex")}
    }`;
  }
}