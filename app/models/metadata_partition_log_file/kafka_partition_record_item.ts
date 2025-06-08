import { UInt32Field, UInt64Field, UInt8Field, UVarIntField, VarIntField } from "../fields/atom_field";
import type { Offset } from "../wrapper";

export class KafkaPartitionRecordItem {
  public length: VarIntField;
  public attributes: UInt8Field
  public timestampDelta: VarIntField;
  public offsetDelta: VarIntField;
  public keyLength: UVarIntField;
  public key: Buffer | undefined;
  public valueLength: VarIntField;
  public value: Buffer;
  public headerLength: VarIntField;
  public headers: Buffer;

  constructor() {
    this.length = new VarIntField(0);
    this.attributes = new UInt8Field(0);
    this.timestampDelta = new VarIntField(0);
    this.offsetDelta = new VarIntField(0);
    this.keyLength = new UVarIntField(0);
    this.key = undefined; // Key can be undefined
    this.valueLength = new VarIntField(0);
    this.value = Buffer.alloc(0); // Default to empty buffer
    this.headerLength = new VarIntField(0);
    this.headers = Buffer.alloc(0); // Default to empty buffer 
  }

  decode(buffer: Buffer, offset: Offset): void {
    let currentOffset = offset.value;
    console.log(`Decoding KafkaPartitionRecordItem from buffer: ${buffer.length} from offset: ${currentOffset}`, );

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

    this.keyLength.decode(buffer.subarray(currentOffset, currentOffset + 4));
    console.log(`Decoded keyLength: ${this.keyLength.value} at offset ${currentOffset}`);
    currentOffset += this.keyLength.size;

    if (this.keyLength.value > 0) {
      this.key = buffer.subarray(currentOffset, currentOffset + this.keyLength.value);
      console.log(`Decoded key: ${this.key.toHex()} at offset ${currentOffset}`);
      // currentOffset += this.keyLength.value;
    } else {
      this.key = undefined; // Handle undefined key
    }

    console.log('valueLength buffer: ', buffer.subarray(currentOffset, currentOffset + 4).toHex())
    this.valueLength.decode(buffer.subarray(currentOffset, currentOffset + 4));
    console.log(`Decoded valueLength: ${this.valueLength.value} at offset ${currentOffset}`);
    currentOffset += this.valueLength.size;

    if (this.valueLength.value > 0) {
      this.value = buffer.subarray(currentOffset, currentOffset + this.valueLength.value);
      console.log(`Decoded value: ${this.value.toString()} at offset ${currentOffset}`);
      currentOffset += this.valueLength.value;
    } else {
      this.value = Buffer.alloc(0); // Default to empty buffer
    }

    this.headerLength.decode(buffer.subarray(currentOffset));
    console.log(`Decoded headerLength: ${this.headerLength.value} at offset ${currentOffset}`);
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
    
    if (this.key !== undefined) {
      buffers.push(this.key);
    } else {
      buffers.push(Buffer.alloc(0)); // Handle undefined key
    }

    buffers.push(this.valueLength.encode());
    buffers.push(this.value);
    buffers.push(this.headerLength.encode());
    buffers.push(this.headers);

    return Buffer.concat(buffers);
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

// export class KafkaPartitionRecordItem {
//   constructor(
//     public length: number,
//     public attributes: number,
//     public timestampDelta: number,
//     public offsetDelta: number,
//     public keyLength: number,
//     public key: Buffer | undefined,
//     public valueLength: number,
//     public value: Buffer,
//     public headerLength: number,
//     public headers: Buffer
//   ){}

//   // static fromBuffer(buffer: Buffer): KafkaPartitionRecordItem { 
//   //   let currentOffset = 0;
//   //   console.log("Reading KafkaPartitionRecordItem from buffer:", buffer.length);

//   //   const length = buffer.readUInt32BE(currentOffset);
//   //   console.log(`length: ${length} at offset ${currentOffset}`);
//   //   currentOffset += 4;

//   //   const attributes = buffer.readUInt8(currentOffset);
//   //   console.log(`attributes: ${attributes} at offset ${currentOffset}`);
//   //   currentOffset += 1;

//   //   const timestampDelta = BigInt(buffer.readBigInt64BE(currentOffset));
//   //   console.log(`timestampDelta: ${timestampDelta} at offset ${currentOffset}`);
//   //   currentOffset += 8;

//   //   const offsetDelta = buffer.readInt32BE(currentOffset);
//   //   console.log(`offsetDelta: ${offsetDelta} at offset ${currentOffset}`);
//   //   currentOffset += 4;

//   //   const { value: keyLength, length: keyLengthBufferSize } = readSignedVarInt(buffer.subarray(currentOffset));
//   //   currentOffset += keyLengthBufferSize;
//   //   const key = buffer.subarray(currentOffset, currentOffset + keyLength);
//   //   console.log(`key: ${key.toString("hex")} at offset ${currentOffset}`);
//   //   currentOffset += keyLength;

//   //   const { value: valueLength, length: valueLengthBufferSize } = readSignedVarInt(buffer.subarray(currentOffset));
//   //   currentOffset += valueLengthBufferSize;
//   //   const value = buffer.subarray(currentOffset, currentOffset + valueLength);
//   //   console.log(`value: ${value.toString("hex")} at offset ${currentOffset}`);
//   //   currentOffset += valueLength;

//   //   const headersLength = buffer.readUInt32BE(currentOffset);
//   //   currentOffset += 4;
//   //   const headers = buffer.subarray(currentOffset, currentOffset + headersLength);
//   //   console.log(`headers: ${headers.toString("hex")} at offset ${currentOffset}`);

//   //   return new KafkaPartitionRecordItem(
//   //     length,
//   //     attributes,
//   //     timestampDelta,
//   //     offsetDelta,
//   //     key,
//   //     value,
//   //     headers
//   //   );
//   // }

//   toBuffer(): Buffer {
//     const lengthBuffer = writeVarInt(this.length);
  
//     const attributesBuffer = Buffer.alloc(1);
//     attributesBuffer.writeUInt8(this.attributes);

//     const timestampDeltaBuffer = writeVarInt(this.timestampDelta);

//     const offsetDeltaBuffer = writeVarInt(this.offsetDelta);

//     const keyLengthBuffer = writeVarInt(this.keyLength);

//     const valueLengthBuffer = writeVarInt(this.valueLength);

//     console.log(`valueLengthBuffer: ${valueLengthBuffer.toHex()}`);

//     const headerLengthBuffer = writeVarInt(this.headerLength);

//     console.log(`headerLengthBuffer: ${headerLengthBuffer.toHex()}`);

//     return Buffer.concat([
//       lengthBuffer,
//       attributesBuffer,
//       timestampDeltaBuffer,
//       offsetDeltaBuffer,
//       keyLengthBuffer,
//       this.key !== undefined ? this.key : Buffer.alloc(0), // Handle undefined key
//       valueLengthBuffer,
//       this.value,
//       headerLengthBuffer,
//       this.headers
//     ]);
//   }

//   debugString(): string {
//     return `KafkaPartitionRecordItem {
//       length: ${this.length},
//       attributes: ${this.attributes},
//       timestampDelta: ${this.timestampDelta},
//       offsetDelta: ${this.offsetDelta},
//       key: ${this.key !== undefined ? this.key.toHex() : "undefined"},
//       value: ${this.value.toHex()},
//       headers: ${this.headers.toHex()}
//     }`;
//   }
// }