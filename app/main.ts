import net from "net";

// You can use print statements as follows for debugging, they'll be visible when running tests.
// console.log("Logs from your program will appear here!");

const toInt32 = (num: number) => {
  return num | 0;
};

// Uncomment this block to pass the first stage
const server: net.Server = net.createServer((connection: net.Socket) => {
  // Handle connection
  connection.on("data", (data: Buffer) => {
    const messageSize = 0;
    const messageSizeBuffer = Buffer.alloc(4);
    messageSizeBuffer.writeUInt32BE(messageSize, 0);
    console.log("messageSizeBuffer: ", messageSizeBuffer);
    const correlationId = 7;
    const correlationIdBuffer = Buffer.alloc(4);
    messageSizeBuffer.writeUInt32BE(correlationId, 0);
    const response = Buffer.concat([correlationIdBuffer, messageSizeBuffer]);
    console.log(response);
    connection.write(response);
  });
});

server.listen(9092, "127.0.0.1");
