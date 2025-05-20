import { type BufferFieldDefinition } from "../types.declaration";

export const kafkaApiVersionsRequestBodyDef: Array<BufferFieldDefinition> = [
  {
    name: "clientIdLength",
    size: 1,
    type: "number",
  },
  {
    name: "clientId",
    size: "clientIdLength", // depends on "clientIdLength"
    type: "string",
  },
  {
    name: "clientSoftwareVersionLength",
    size: 1,
    type: "string",
  },
  {
    name: "clientSoftware",
    size: "clientSoftwareVersionLength",
    type: "string",
  },
  {
    name: "tagBuffer",
    size: 0,
    type: "string",
  },
];
