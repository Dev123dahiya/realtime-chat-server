// The seam for running more than one server process.
//
// A Hub only knows about its own sockets. To reach users connected to other
// processes it publishes every room event to a bus and delivers whatever the
// bus hands back. In production the bus is Redis pub/sub or NATS; the in-memory
// one below lets tests run several hubs in one process.

import type { ChatMessage } from "./protocol.js";

export type RoomEvent =
  | { kind: "message"; message: ChatMessage }
  | { kind: "presence"; room: string; user: string; status: "online" | "offline" }
  | { kind: "typing"; room: string; user: string };

export type Envelope = { origin: string; event: RoomEvent };

export interface Bus {
  publish(envelope: Envelope): void;
  subscribe(handler: (envelope: Envelope) => void): () => void;
}

export class InMemoryBus implements Bus {
  private handlers = new Set<(envelope: Envelope) => void>();

  publish(envelope: Envelope): void {
    for (const handler of [...this.handlers]) handler(envelope);
  }

  subscribe(handler: (envelope: Envelope) => void): () => void {
    this.handlers.add(handler);
    return () => this.handlers.delete(handler);
  }
}

/** Shared room state: message sequence numbers, history and membership. */
export interface RoomStore {
  append(room: string, user: string, text: string, at: number): ChatMessage;
  history(room: string, since: number, limit: number): ChatMessage[];
  addMember(room: string, user: string): boolean; // true if the user was not present before
  removeMember(room: string, user: string): boolean; // true if the user is now fully gone
  members(room: string): string[];
}

/**
 * In-memory store. Membership is reference-counted because one user can be
 * connected from several tabs: they go offline only when the last one leaves.
 */
export class InMemoryRoomStore implements RoomStore {
  private messages = new Map<string, ChatMessage[]>();
  private nextId = new Map<string, number>();
  private membership = new Map<string, Map<string, number>>();

  constructor(private readonly maxHistory = 200) {}

  append(room: string, user: string, text: string, at: number): ChatMessage {
    const id = (this.nextId.get(room) ?? 0) + 1;
    this.nextId.set(room, id);
    const message: ChatMessage = { id, room, user, text, at };
    const list = this.messages.get(room) ?? [];
    list.push(message);
    if (list.length > this.maxHistory) list.splice(0, list.length - this.maxHistory);
    this.messages.set(room, list);
    return message;
  }

  history(room: string, since: number, limit: number): ChatMessage[] {
    const list = (this.messages.get(room) ?? []).filter((m) => m.id > since);
    return list.slice(-limit);
  }

  addMember(room: string, user: string): boolean {
    const counts = this.membership.get(room) ?? new Map<string, number>();
    const before = counts.get(user) ?? 0;
    counts.set(user, before + 1);
    this.membership.set(room, counts);
    return before === 0;
  }

  removeMember(room: string, user: string): boolean {
    const counts = this.membership.get(room);
    const before = counts?.get(user) ?? 0;
    if (!counts || before === 0) return false;
    if (before === 1) {
      counts.delete(user);
      if (counts.size === 0) this.membership.delete(room);
      return true;
    }
    counts.set(user, before - 1);
    return false;
  }

  members(room: string): string[] {
    return [...(this.membership.get(room)?.keys() ?? [])].sort();
  }
}
