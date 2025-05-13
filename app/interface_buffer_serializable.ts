export interface IResponseBufferSerializable {
  getBufferSize(): number;
  toBuffer(): Buffer;
}
