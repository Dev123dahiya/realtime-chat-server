import assert from "node:assert/strict";
import { after, before, test } from "node:test";
import WebSocket from "ws";
import type { ServerMessage } from "../src/protocol.js";
import { type ChatServer, createChatServer } from "../src/server.js";

let server: ChatServer;
let port: number;

before(async () => {
  server = createChatServer({ heartbeatMs: 60_000 });
  port = await server.listen(0, "127.0.0.1");
});
after(() => server.close());

/** A real WebSocket client that queues frames so tests can await them in order. */
class Client {
  private queue: ServerMessage[] = [];
  private waiters: ((m: ServerMessage) => void)[] = [];
  private constructor(readonly ws: WebSocket) {
    ws.on("message", (data) => {
      const message = JSON.parse(data.toString()) as ServerMessage;
      const waiter = this.waiters.shift();
      if (waiter) waiter(message);
      else this.queue.push(message);
    });
  }
  static open(user: string): Promise<Client> {
    return new Promise((resolve, reject) => {
      const ws = new WebSocket(`ws://127.0.0.1:${port}/?user=${user}`);
      const client = new Client(ws);
      ws.once("open", () => resolve(client));
      ws.once("error", reject);
    });
  }
  send(frame: object): void {
    this.ws.send(JSON.stringify(frame));
  }
  next(): Promise<ServerMessage> {
    const queued = this.queue.shift();
    if (queued) return Promise.resolve(queued);
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error("timed out waiting for a frame")), 2_000);
      this.waiters.push((m) => {
        clearTimeout(timer);
        resolve(m);
      });
    });
  }
  async until<T extends ServerMessage["type"]>(type: T): Promise<Extract<ServerMessage, { type: T }>> {
    for (;;) {
      const message = await this.next();
      if (message.type === type) return message as Extract<ServerMessage, { type: T }>;
    }
  }
  close(): Promise<void> {
    return new Promise((resolve) => {
      this.ws.once("close", () => resolve());
      this.ws.close();
    });
  }
}

test("two real clients chat over WebSockets", async () => {
  const alice = await Client.open("alice");
  const bob = await Client.open("bob");
  assert.deepEqual(await alice.next(), { type: "welcome", user: "alice" });

  alice.send({ type: "join", room: "lobby" });
  assert.deepEqual((await alice.until("joined")).members, ["alice"]);
  assert.equal((await alice.until("presence")).user, "alice"); // a room hears about every arrival, including your own
  bob.send({ type: "join", room: "lobby" });
  assert.deepEqual((await bob.until("joined")).members, ["alice", "bob"]);
  const online = await alice.until("presence");
  assert.deepEqual([online.user, online.status], ["bob", "online"]);

  bob.send({ type: "message", room: "lobby", text: "hello alice", clientId: "c1" });
  const received = await alice.until("message");
  assert.equal(received.message.text, "hello alice");
  assert.equal(received.message.user, "bob");
  assert.deepEqual(await bob.until("ack"), { type: "ack", clientId: "c1", id: received.message.id });

  await bob.close();
  const offline = await alice.until("presence");
  assert.deepEqual([offline.user, offline.status], ["bob", "offline"]);
  await alice.close();
});

test("connections without a valid user are refused with 401", async () => {
  const status = await new Promise<number>((resolve) => {
    const ws = new WebSocket(`ws://127.0.0.1:${port}/?user=bad%20name`);
    ws.on("unexpected-response", (_request, response) => resolve(response.statusCode ?? 0));
    ws.on("error", () => undefined);
  });
  assert.equal(status, 401);
});

test("binary frames are rejected and health reports live connections", async () => {
  const carol = await Client.open("carol");
  await carol.next();
  // Earlier tests' sockets close asynchronously on the server side; wait for them to drain.
  for (let i = 0; i < 50 && server.hub.connectionCount !== 1; i++) await new Promise((r) => setTimeout(r, 10));
  const health = (await (await fetch(`http://127.0.0.1:${port}/health`)).json()) as { status: string; connections: number };
  assert.equal(health.status, "ok");
  assert.equal(health.connections, 1);

  const code = await new Promise<number>((resolve) => {
    carol.ws.once("close", (c) => resolve(c));
    carol.ws.send(Buffer.from([1, 2, 3]));
  });
  assert.equal(code, 1003);
});
