export class Wrapper<T> {
  constructor(public value: T) {}
}

export type Offset = Wrapper<number>;

// A wrapper for Buffer class, which trackings the offset and size of the buffer
// This is useful for decoding and encoding operations where we need to keep track of the current position in the buffer
export class SmartBuffer {
  private buffer: Buffer;
  private offset: number;

  constructor(size: number) {
    this.buffer = Buffer.alloc(size);
    this.offset = 0;
  }

  readUInt8(): number {
    const value = this.buffer.readUInt8(this.offset);
    this.offset += 1;
    return value;
  }

  getBuffer(): Buffer {
    return this.buffer.subarray(0, this.offset);
  }

  getOffset(): number {
    return this.offset;
  }

  reset(): void {
    this.offset = 0;
  }
}