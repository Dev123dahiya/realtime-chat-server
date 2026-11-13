import { randomUUID } from "node:crypto";
import { type Bus, type Envelope, InMemoryBus, InMemoryRoomStore, type RoomEvent, type RoomStore } from "./bus.js";
import { type ErrorCode, LIMITS, parseClientMessage, type ServerMessage } from "./protocol.js";

/** What the hub needs from a transport. A WebSocket satisfies it; so does a test double. */
export interface Connection {
  send(data: string): void;
  close(code?: number, reason?: string): void;
  /** Bytes queued but not yet flushed to the network. */
  readonly bufferedAmount: number;
}

export interface HubOptions {
  bus?: Bus;
  store?: RoomStore;
  now?: () => number;
  /** Messages allowed per window, per connection. */
  rateLimit?: { messages: number; windowMs: number };
  historyOnJoin?: number;
  /** Disconnect clients whose outbound queue grows past this many bytes. */
  maxBufferedBytes?: number;
}

interface Session {
  user: string;
  conn: Connection;
  rooms: Set<string>;
  windowStart: number;
  sentInWindow: number;
}

/**
 * Transport-agnostic chat core. All state changes go through here, so the same
 * logic is exercised by unit tests (fake connections) and by the real server.
 */
export class Hub {
  readonly id = randomUUID();
  private readonly bus: Bus;
  private readonly store: RoomStore;
  private readonly now: () => number;
  private readonly rateLimit: { messages: number; windowMs: number };
  private readonly historyOnJoin: number;
  private readonly maxBufferedBytes: number;
  private readonly sessions = new Map<Connection, Session>();
  private readonly roomSessions = new Map<string, Set<Session>>();
  private readonly unsubscribe: () => void;
  readonly stats = { delivered: 0, droppedSlowConsumers: 0, rateLimited: 0 };

  constructor(options: HubOptions = {}) {
    this.bus = options.bus ?? new InMemoryBus();
    this.store = options.store ?? new InMemoryRoomStore();
    this.now = options.now ?? Date.now;
    this.rateLimit = options.rateLimit ?? { messages: 10, windowMs: 5_000 };
    this.historyOnJoin = options.historyOnJoin ?? 50;
    this.maxBufferedBytes = options.maxBufferedBytes ?? 1_000_000;
    this.unsubscribe = this.bus.subscribe((envelope) => this.deliver(envelope));
  }

  get connectionCount(): number {
    return this.sessions.size;
  }

  connect(conn: Connection, user: string): void {
    this.sessions.set(conn, { user, conn, rooms: new Set(), windowStart: this.now(), sentInWindow: 0 });
    this.sendTo(conn, { type: "welcome", user });
  }

  disconnect(conn: Connection): void {
    const session = this.sessions.get(conn);
    if (!session) return;
    for (const room of [...session.rooms]) this.leave(session, room, false);
    this.sessions.delete(conn);
  }

  /** Handle one raw frame from a client. Never throws on bad input. */
  receive(conn: Connection, raw: string): void {
    const session = this.sessions.get(conn);
    if (!session) return;
    const message = parseClientMessage(raw);
    if ("error" in message) return this.error(conn, "bad_request", message.error);

    switch (message.type) {
      case "ping":
        return this.sendTo(conn, { type: "pong" });
      case "join":
        return this.join(session, message.room, message.since);
      case "leave":
        if (!session.rooms.has(message.room)) return this.error(conn, "not_in_room", message.room);
        return this.leave(session, message.room, true);
      case "typing":
        if (!session.rooms.has(message.room)) return this.error(conn, "not_in_room", message.room);
        return this.publish({ kind: "typing", room: message.room, user: session.user });
      case "message": {
        if (!session.rooms.has(message.room)) return this.error(conn, "not_in_room", message.room);
        if (!this.allow(session)) {
          this.stats.rateLimited++;
          return this.error(conn, "rate_limited", `at most ${this.rateLimit.messages} messages per ${this.rateLimit.windowMs} ms`);
        }
        const stored = this.store.append(message.room, session.user, message.text, this.now());
        // Ack first so the sender can match its optimistic copy to the real id.
        if (message.clientId !== undefined) this.sendTo(conn, { type: "ack", clientId: message.clientId, id: stored.id });
        return this.publish({ kind: "message", message: stored });
      }
    }
  }

  close(): void {
    this.unsubscribe();
    for (const conn of [...this.sessions.keys()]) {
      this.disconnect(conn);
      conn.close(1001, "server shutting down");
    }
  }

  private join(session: Session, room: string, since?: number): void {
    if (session.rooms.has(room)) {
      // Re-join is how a reconnecting client asks for what it missed.
      return this.sendJoined(session, room, since);
    }
    if (session.rooms.size >= LIMITS.maxRoomsPerConnection) {
      return this.error(session.conn, "too_many_rooms", `at most ${LIMITS.maxRoomsPerConnection} rooms per connection`);
    }
    session.rooms.add(room);
    const set = this.roomSessions.get(room) ?? new Set<Session>();
    set.add(session);
    this.roomSessions.set(room, set);
    const firstPresence = this.store.addMember(room, session.user);
    this.sendJoined(session, room, since);
    if (firstPresence) this.publish({ kind: "presence", room, user: session.user, status: "online" });
  }

  private sendJoined(session: Session, room: string, since?: number): void {
    const history = this.store.history(room, since ?? 0, since === undefined ? this.historyOnJoin : Number.MAX_SAFE_INTEGER);
    this.sendTo(session.conn, { type: "joined", room, members: this.store.members(room), history });
  }

  private leave(session: Session, room: string, notifySelf: boolean): void {
    session.rooms.delete(room);
    const set = this.roomSessions.get(room);
    set?.delete(session);
    if (set && set.size === 0) this.roomSessions.delete(room);
    if (notifySelf) this.sendTo(session.conn, { type: "left", room });
    if (this.store.removeMember(room, session.user)) {
      this.publish({ kind: "presence", room, user: session.user, status: "offline" });
    }
  }

  /** Fixed-window limiter per connection. */
  private allow(session: Session): boolean {
    const now = this.now();
    if (now - session.windowStart >= this.rateLimit.windowMs) {
      session.windowStart = now;
      session.sentInWindow = 0;
    }
    if (session.sentInWindow >= this.rateLimit.messages) return false;
    session.sentInWindow++;
    return true;
  }

  private publish(event: RoomEvent): void {
    this.bus.publish({ origin: this.id, event });
  }

  /** Fan one room event out to this hub's local sockets. Runs for local and remote events alike. */
  private deliver({ event }: Envelope): void {
    const room = event.kind === "message" ? event.message.room : event.room;
    const targets = this.roomSessions.get(room);
    if (!targets) return;
    const frame: ServerMessage =
      event.kind === "message"
        ? { type: "message", message: event.message }
        : event.kind === "presence"
          ? { type: "presence", room: event.room, user: event.user, status: event.status }
          : { type: "typing", room: event.room, user: event.user };
    const encoded = JSON.stringify(frame); // serialise once, not once per recipient
    for (const session of [...targets]) {
      if (event.kind === "typing" && session.user === event.user) continue;
      this.sendRaw(session.conn, encoded);
    }
  }

  private sendTo(conn: Connection, message: ServerMessage): void {
    this.sendRaw(conn, JSON.stringify(message));
  }

  private sendRaw(conn: Connection, encoded: string): void {
    // Backpressure: a client that cannot keep up is dropped rather than allowed
    // to grow the server's memory without bound.
    if (conn.bufferedAmount > this.maxBufferedBytes) {
      this.stats.droppedSlowConsumers++;
      this.disconnect(conn);
      conn.close(1013, "client too slow");
      return;
    }
    conn.send(encoded);
    this.stats.delivered++;
  }

  private error(conn: Connection, code: ErrorCode, detail: string): void {
    this.sendTo(conn, { type: "error", code, detail });
  }
}
