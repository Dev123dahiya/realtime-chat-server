import { createChatServer } from "./server.js";

const port = Number(process.env.PORT ?? 8080);
const server = createChatServer();

server.listen(port).then((actual) => {
  console.log(`chat server listening on ws://localhost:${actual}/?user=<name>  (health: http://localhost:${actual}/health)`);
});

// Graceful shutdown: tell clients we are going away so they can reconnect elsewhere.
for (const signal of ["SIGINT", "SIGTERM"] as const) {
  process.on(signal, () => {
    console.log(`${signal} received, closing`);
    void server.close().then(() => process.exit(0));
  });
}
