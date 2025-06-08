export interface BufferEncode {
  encodeTo(): Buffer;
}

export interface BufferDecode {
  /**
   * Decodes the buffer starting from the specified offset.
   * @param buffer The buffer to decode from.
   * @returns The number of bytes read.
   */
  decodeFrom(buffer: Buffer): number;
}

export interface DebugString {
  debugString(): string;
}