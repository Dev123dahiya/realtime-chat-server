// Wire protocol. Every frame is one JSON object with a "type".

export type ClientMessage =
  | { type: "join"; room: string; since?: number }
  | { type: "leave"; room: string }
  | { type: "message"; room: string; text: string; clientId?: string }
  | { type: "typing"; room: string }
  | { type: "ping" };

export interface ChatMessage {
  id: number; // per-room sequence number, starts at 1
  room: string;
  user: string;
  text: string;
  at: number; // epoch milliseconds
}

export type ServerMessage =
  | { type: "welcome"; user: string }
  | { type: "joined"; room: string; members: string[]; history: ChatMessage[] }
  | { type: "left"; room: string }
  | { type: "message"; message: ChatMessage }
  | { type: "ack"; clientId: string; id: number }
  | { type: "presence"; room: string; user: string; status: "online" | "offline" }
  | { type: "typing"; room: string; user: string }
  | { type: "pong" }
  | { type: "error"; code: ErrorCode; detail: string };

export type ErrorCode = "bad_request" | "not_in_room" | "rate_limited" | "too_many_rooms";

export const LIMITS = {
  roomName: /^[a-z0-9][a-z0-9_-]{0,31}$/,
  userName: /^[A-Za-z0-9_-]{1,24}$/,
  maxTextLength: 2_000,
  maxFrameBytes: 8_192,
  maxRoomsPerConnection: 20,
} as const;

/** Parse and validate one raw frame. Returns an error string instead of throwing. */
export function parseClientMessage(raw: string): ClientMessage | { error: string } {
  if (raw.length > LIMITS.maxFrameBytes) return { error: "frame too large" };
  let data: unknown;
  try {
    data = JSON.parse(raw);
  } catch {
    return { error: "frame is not valid JSON" };
  }
  if (typeof data !== "object" || data === null) return { error: "frame must be a JSON object" };
  const m = data as Record<string, unknown>;

  if (m.type === "ping") return { type: "ping" };
  if (typeof m.room !== "string" || !LIMITS.roomName.test(m.room)) return { error: "invalid room name" };

  switch (m.type) {
    case "join":
      if (m.since !== undefined && (!Number.isInteger(m.since) || (m.since as number) < 0)) {
        return { error: "since must be a non-negative integer" };
      }
      return { type: "join", room: m.room, since: m.since as number | undefined };
    case "leave":
      return { type: "leave", room: m.room };
    case "typing":
      return { type: "typing", room: m.room };
    case "message": {
      if (typeof m.text !== "string") return { error: "text must be a string" };
      const text = m.text.trim();
      if (text.length === 0 || text.length > LIMITS.maxTextLength) return { error: "text must be 1-2000 characters" };
      if (m.clientId !== undefined && (typeof m.clientId !== "string" || m.clientId.length > 64)) {
        return { error: "clientId must be a string of at most 64 characters" };
      }
      return { type: "message", room: m.room, text, clientId: m.clientId as string | undefined };
    }
    default:
      return { error: "unknown message type" };
  }
}
