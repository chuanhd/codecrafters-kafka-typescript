
export function readVarInt(buffer: Buffer): { value: number; length: number } {
  let value = 0;
  let length = 0; // Length of the varint in bytes
  let currentByte;

  while (true) {
    if (length >= buffer.length) {
      throw new Error('Buffer underflow while reading varint');
    }
    currentByte = buffer[length];
    value |= (currentByte & 0x7f) << (length * 7); // Mask the 7 least significant bits and shift them accordingly
    length += 1; // Increment shift to the next 7 bits
    if ((currentByte & 0x80) == 0) break; // If the MSB is 0, we have reached the last byte
    
    // Check if we're about to exceed the safe integer limit in JavaScript
    if (length >= 9) {
      throw new Error('VarInt is too large');
    }
  }
  
  // Convert to signed integer if the value exceeds half the maximum possible value
  const maxValueForBytes = Math.pow(2, length * 7);
  if (value >= maxValueForBytes / 2) {
    value = value - maxValueForBytes;
  }
  
  return { value, length };
}

export function readSignedVarInt(buffer: Buffer): { value: number; length: number } {
  let value = 0;
  let length = 0; // Length of the varint in bytes
  let currentByte;

  while (true) {
    if (length >= buffer.length) {
      throw new Error('Buffer underflow while reading varint');
    }
    currentByte = buffer[length];
    value |= (currentByte & 0x7f) << (length * 7); // Mask the 7 least significant bits and shift them accordingly
    length += 1; // Increment shift to the next 7 bits
    if ((currentByte & 0x80) == 0) break; // If the MSB is 0, we have reached the last byte
    
    // Check if we're about to exceed the safe integer limit in JavaScript
    if (length >= 9) {
      throw new Error('VarInt is too large');
    }
  }
  
  return { value: value / 2, length };
}

export function writeVarInt(value: number): Buffer {
  let buffer = Buffer.alloc(0);
  let v = value;
  let length = 0;

  do {
    let byte = v & 0x7f; // Get the last 7 bits
    v >>= 7; // Shift right by 7 bits
    if (v !== 0) {
      byte |= 0x80; // Set the MSB if there are more bytes to come
    }
    buffer = Buffer.concat([buffer, Buffer.from([byte])]);
    length += 1; // Increment the length of the buffer
  } while (v !== 0);

  return buffer;
}
