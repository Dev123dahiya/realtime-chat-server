import { createServer, type IncomingMessage, type Server } from "node:http";
import { WebSocketServer, type WebSocket } from "ws";
import { Hub, type HubOptions } from "./hub.js";
import { LIMITS } from "./protocol.js";

export interface ChatServerOptions extends HubOptions {
  /** How often to ping clients; one missed pong closes the socket. */
  heartbeatMs?: number;
  /** Return the user name for a connection, or null to reject it. */
  authenticate?: (request: IncomingMessage) => string | null;
}

/** Default: take ?user=name from the URL. Replace with real token checks in production. */
export function userFromQuery(request: IncomingMessage): string | null {
  const user = new URL(request.url ?? "/", "http://localhost").searchParams.get("user");
  return user && LIMITS.userName.test(user) ? user : null;
}

export interface ChatServer {
  hub: Hub;
  http: Server;
  listen(port: number, host?: string): Promise<number>;
  close(): Promise<void>;
}

export function createChatServer(options: ChatServerOptions = {}): ChatServer {
  const hub = new Hub(options);
  const authenticate = options.authenticate ?? userFromQuery;
  const heartbeatMs = options.heartbeatMs ?? 30_000;

  const http = createServer((request, response) => {
    if (request.url === "/health") {
      response.writeHead(200, { "content-type": "application/json" });
      response.end(JSON.stringify({ status: "ok", connections: hub.connectionCount, ...hub.stats }));
      return;
    }
    response.writeHead(404).end();
  });

  const wss = new WebSocketServer({ noServer: true, maxPayload: LIMITS.maxFrameBytes });
  const alive = new WeakMap<WebSocket, boolean>();

  http.on("upgrade", (request, socket, head) => {
    const user = authenticate(request);
    if (user === null) {
      // Reject before the WebSocket handshake so unauthenticated clients cost almost nothing.
      socket.write("HTTP/1.1 401 Unauthorized\r\nConnection: close\r\n\r\n");
      socket.destroy();
      return;
    }
    wss.handleUpgrade(request, socket, head, (ws) => {
      alive.set(ws, true);
      hub.connect(ws, user);
      ws.on("message", (data, isBinary) => {
        if (isBinary) return ws.close(1003, "text frames only");
        hub.receive(ws, data.toString());
      });
      ws.on("pong", () => alive.set(ws, true));
      ws.on("close", () => hub.disconnect(ws));
      ws.on("error", () => hub.disconnect(ws));
    });
  });

  // Heartbeat: TCP can leave a dead peer looking connected for minutes.
  const heartbeat = setInterval(() => {
    for (const ws of wss.clients) {
      if (alive.get(ws) === false) {
        ws.terminate();
        continue;
      }
      alive.set(ws, false);
      ws.ping();
    }
  }, heartbeatMs);
  heartbeat.unref();

  return {
    hub,
    http,
    listen: (port, host = "0.0.0.0") =>
      new Promise((resolve) => {
        http.listen(port, host, () => {
          const address = http.address();
          resolve(typeof address === "object" && address ? address.port : port);
        });
      }),
    close: () =>
      new Promise((resolve) => {
        clearInterval(heartbeat);
        hub.close();
        wss.close(() => http.close(() => resolve()));
      }),
  };
}
