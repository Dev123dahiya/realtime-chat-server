import assert from "node:assert/strict";
import { test } from "node:test";
import { InMemoryBus, InMemoryRoomStore } from "../src/bus.js";
import { type Connection, Hub } from "../src/hub.js";
import { parseClientMessage, type ServerMessage } from "../src/protocol.js";

class FakeConn implements Connection {
  sent: ServerMessage[] = [];
  closed: { code?: number; reason?: string } | null = null;
  bufferedAmount = 0;
  send(data: string): void {
    this.sent.push(JSON.parse(data) as ServerMessage);
  }
  close(code?: number, reason?: string): void {
    this.closed = { code, reason };
  }
  ofType<T extends ServerMessage["type"]>(type: T): Extract<ServerMessage, { type: T }>[] {
    return this.sent.filter((m): m is Extract<ServerMessage, { type: T }> => m.type === type);
  }
  last(): ServerMessage {
    return this.sent[this.sent.length - 1]!;
  }
}

const say = (hub: Hub, conn: FakeConn, frame: object) => hub.receive(conn, JSON.stringify(frame));

function setup(options: ConstructorParameters<typeof Hub>[0] = {}) {
  const hub = new Hub(options);
  const alice = new FakeConn();
  const bob = new FakeConn();
  hub.connect(alice, "alice");
  hub.connect(bob, "bob");
  return { hub, alice, bob };
}

test("messages reach everyone in the room, with increasing ids", () => {
  const { hub, alice, bob } = setup();
  say(hub, alice, { type: "join", room: "general" });
  say(hub, bob, { type: "join", room: "general" });
  say(hub, alice, { type: "message", room: "general", text: "  hello  " });
  say(hub, bob, { type: "message", room: "general", text: "hi" });

  for (const conn of [alice, bob]) {
    const got = conn.ofType("message").map((m) => [m.message.id, m.message.user, m.message.text]);
    assert.deepEqual(got, [[1, "alice", "hello"], [2, "bob", "hi"]]);
  }
});

test("rooms are isolated", () => {
  const { hub, alice, bob } = setup();
  say(hub, alice, { type: "join", room: "a" });
  say(hub, bob, { type: "join", room: "b" });
  say(hub, alice, { type: "message", room: "a", text: "secret" });
  assert.equal(bob.ofType("message").length, 0);
});

test("cannot post to a room you have not joined", () => {
  const { hub, alice } = setup();
  say(hub, alice, { type: "message", room: "general", text: "hi" });
  assert.deepEqual(alice.last(), { type: "error", code: "not_in_room", detail: "general" });
});

test("join returns members and recent history", () => {
  const { hub, alice, bob } = setup({ historyOnJoin: 2 });
  say(hub, alice, { type: "join", room: "general" });
  for (const text of ["one", "two", "three"]) say(hub, alice, { type: "message", room: "general", text });
  say(hub, bob, { type: "join", room: "general" });
  const joined = bob.ofType("joined")[0]!;
  assert.deepEqual(joined.members, ["alice", "bob"]);
  assert.deepEqual(joined.history.map((m) => m.text), ["two", "three"]);
});

test("a reconnecting client replays exactly what it missed", () => {
  const { hub, alice, bob } = setup();
  say(hub, alice, { type: "join", room: "general" });
  say(hub, bob, { type: "join", room: "general" });
  say(hub, alice, { type: "message", room: "general", text: "seen" });
  hub.disconnect(bob); // bob's network drops after message 1
  say(hub, alice, { type: "message", room: "general", text: "missed 1" });
  say(hub, alice, { type: "message", room: "general", text: "missed 2" });

  const bobAgain = new FakeConn();
  hub.connect(bobAgain, "bob");
  say(hub, bobAgain, { type: "join", room: "general", since: 1 });
  assert.deepEqual(bobAgain.ofType("joined")[0]!.history.map((m) => m.id), [2, 3]);
});

test("ack maps the client's id to the server's id", () => {
  const { hub, alice } = setup();
  say(hub, alice, { type: "join", room: "general" });
  say(hub, alice, { type: "message", room: "general", text: "hi", clientId: "tmp-1" });
  assert.deepEqual(alice.ofType("ack"), [{ type: "ack", clientId: "tmp-1", id: 1 }]);
});

test("presence: online once, offline only when the last tab closes", () => {
  const { hub, alice, bob } = setup();
  say(hub, alice, { type: "join", room: "general" });
  say(hub, bob, { type: "join", room: "general" });
  const bobTab2 = new FakeConn();
  hub.connect(bobTab2, "bob");
  say(hub, bobTab2, { type: "join", room: "general" });

  const presence = () => alice.ofType("presence").filter((p) => p.user === "bob").map((p) => p.status);
  assert.deepEqual(presence(), ["online"]); // second tab did not announce again
  hub.disconnect(bob);
  assert.deepEqual(presence(), ["online"]); // still online through the other tab
  hub.disconnect(bobTab2);
  assert.deepEqual(presence(), ["online", "offline"]);
});

test("typing indicators go to others but not back to the sender", () => {
  const { hub, alice, bob } = setup();
  say(hub, alice, { type: "join", room: "general" });
  say(hub, bob, { type: "join", room: "general" });
  say(hub, alice, { type: "typing", room: "general" });
  assert.equal(alice.ofType("typing").length, 0);
  assert.deepEqual(bob.ofType("typing"), [{ type: "typing", room: "general", user: "alice" }]);
});

test("rate limit rejects a burst, then recovers in the next window", () => {
  let now = 1_000;
  const { hub, alice } = setup({ now: () => now, rateLimit: { messages: 3, windowMs: 1_000 } });
  say(hub, alice, { type: "join", room: "general" });
  for (let i = 0; i < 5; i++) say(hub, alice, { type: "message", room: "general", text: `m${i}` });
  assert.equal(alice.ofType("message").length, 3);
  assert.equal(alice.ofType("error").filter((e) => e.code === "rate_limited").length, 2);
  now += 1_000;
  say(hub, alice, { type: "message", room: "general", text: "later" });
  assert.equal(alice.ofType("message").length, 4);
});

test("slow consumers are disconnected instead of buffering forever", () => {
  const { hub, alice, bob } = setup({ maxBufferedBytes: 1_000 });
  say(hub, alice, { type: "join", room: "general" });
  say(hub, bob, { type: "join", room: "general" });
  bob.bufferedAmount = 5_000; // bob's socket has stopped draining
  say(hub, alice, { type: "message", room: "general", text: "hello" });
  assert.equal(bob.closed?.code, 1013);
  assert.equal(hub.connectionCount, 1);
  assert.equal(hub.stats.droppedSlowConsumers, 1);
  assert.equal(alice.ofType("message").length, 1); // healthy clients are unaffected
});

test("malformed frames get an error and never crash the hub", () => {
  const { hub, alice } = setup();
  const bad = ["not json", "[]", '{"type":"join","room":"Has Spaces"}', '{"type":"message","room":"general","text":""}',
    '{"type":"message","room":"general","text":42}', '{"type":"join","room":"general","since":-1}', '{"type":"nope","room":"general"}',
    JSON.stringify({ type: "message", room: "general", text: "x".repeat(2_001) }), "x".repeat(9_000)];
  for (const frame of bad) hub.receive(alice, frame);
  assert.equal(alice.ofType("error").length, bad.length);
  assert.ok(alice.ofType("error").every((e) => e.code === "bad_request"));
  assert.deepEqual(parseClientMessage('{"type":"ping"}'), { type: "ping" });
});

test("two hubs sharing a bus and store behave like one server", () => {
  const bus = new InMemoryBus();
  const store = new InMemoryRoomStore();
  const hubA = new Hub({ bus, store });
  const hubB = new Hub({ bus, store });
  const alice = new FakeConn();
  const bob = new FakeConn();
  hubA.connect(alice, "alice"); // alice is on server A
  hubB.connect(bob, "bob"); // bob is on server B
  say(hubA, alice, { type: "join", room: "general" });
  say(hubB, bob, { type: "join", room: "general" });

  say(hubA, alice, { type: "message", room: "general", text: "across servers" });
  assert.deepEqual(bob.ofType("message").map((m) => m.message.text), ["across servers"]);
  assert.equal(alice.ofType("message").length, 1); // delivered once, not twice
  assert.deepEqual(bob.ofType("joined")[0]!.members, ["alice", "bob"]);
  assert.deepEqual(alice.ofType("presence").map((p) => [p.user, p.status]), [["alice", "online"], ["bob", "online"]]);

  hubB.close();
  assert.deepEqual(alice.ofType("presence").at(-1), { type: "presence", room: "general", user: "bob", status: "offline" });
  assert.equal(bob.closed?.code, 1001);
});
