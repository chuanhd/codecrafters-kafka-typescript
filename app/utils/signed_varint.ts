import * as unsigned from "./unsigned_varint.ts"

export function signedEncodingLength(v: number) {
	return unsigned.encodingLength(v >= 0 ? v * 2 : v * -2 - 1)
}

export type Encode = {
	(i: number, buffer?: ArrayBuffer, byteOffset?: number): Uint8Array
}

export const encodeSigned = (v: number, buffer?: ArrayBuffer, byteOffset = 0): Uint8Array => {
	v = v >= 0 ? v * 2 : v * -2 - 1
	return unsigned.encode(v, buffer, byteOffset)
}

export type Decode = {
	bytes?: number
	(data: Uint8Array, offset?: number): number
}

export const decodeSigned: Decode = (data: Uint8Array, offset = 0) => {
	const prevUnsignedBytes = unsigned.decode.bytes
	const v = unsigned.decode(data, offset)
	decodeSigned.bytes = unsigned.decode.bytes
	unsigned.decode.bytes = prevUnsignedBytes
	return v & 1 ? (v + 1) / -2 : v / 2
}