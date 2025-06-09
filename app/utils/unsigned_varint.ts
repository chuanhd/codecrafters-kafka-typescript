const LIMIT = 0x7f

export function encodingLength(value: number): number {
	let i = 0

	for (; value >= 0x80; i++) {
		value >>= 7
	}

	return i + 1
}

export type Encode = {
	(i: number, buffer?: ArrayBuffer, byteOffset?: number): Uint8Array
}

export const encode: Encode = (i: number, buffer?: ArrayBuffer, byteOffset = 0): Uint8Array => {
	if (i < 0n) {
		throw new RangeError("value must be unsigned")
	}

	const byteLength = encodingLength(i)
	buffer = buffer ?? new ArrayBuffer(byteLength)
	if (buffer.byteLength < byteOffset + byteLength) {
		throw new RangeError("the buffer is too small to encode the number at the given offset")
	}

	const array = new Uint8Array(buffer, byteOffset, byteLength)

	let offset = 0
	while (LIMIT < i) {
		array[offset++] = Number(i & LIMIT) | 0x80
		i >>= 7
	}

	array[offset] = Number(i)

	return array
}

export type Decode = {
	bytes?: number
	(data: Uint8Array, offset?: number): number
}

export const decode: Decode = (data: Uint8Array, offset = 0): number => {
	let i = 0
	let n = 0
	let b: number
	do {
		b = data[offset + n]
		if (b === undefined) {
			throw new RangeError("offset out of range")
		}

		i += (b & 0x7f) << (n * 7)
		n++
	} while (0x80 <= b)

	decode.bytes = n
	return i
}