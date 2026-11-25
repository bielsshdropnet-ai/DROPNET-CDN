import http from "node:http";
import net from "node:net";
import tls from "node:tls";

const VPS_HOST = "gm.dpnet.online";
const MAX_HEADER_BYTES = 32 * 1024;
const CONNECT_TIMEOUT_MS = 15_000;

const server = http.createServer((req, res) => {
  const url = new URL(req.url || "/", "http://localhost");

  if (url.pathname === "/api/proxy" && url.searchParams.get("health") === "1") {
    res.writeHead(200, { "content-type": "application/json", "cache-control": "no-store" });
    res.end(JSON.stringify({ service: "DROPNET CDN", status: "online", upstream: VPS_HOST }));
    return;
  }

  res.writeHead(426, {
    "content-type": "text/plain; charset=utf-8",
    "connection": "close",
    "upgrade": "websocket"
  });
  res.end("DROPNET CDN: use WebSocket em /api/proxy");
});

function reject(socket, status, message) {
  if (socket.destroyed) return;
  socket.end(
    `HTTP/1.1 ${status} ${message}\r\n` +
    "Connection: close\r\n" +
    "Content-Length: 0\r\n\r\n"
  );
}

server.on("upgrade", (req, clientSocket, head) => {
  const incoming = new URL(req.url || "/", "http://localhost");
  if (incoming.pathname !== "/api/proxy") {
    reject(clientSocket, 404, "Not Found");
    return;
  }

  const upgrade = String(req.headers.upgrade || "").toLowerCase();
  const connection = String(req.headers.connection || "").toLowerCase();
  const wsKey = req.headers["sec-websocket-key"];
  const wsVersion = req.headers["sec-websocket-version"];

  if (upgrade !== "websocket" || !connection.split(/,\s*/).includes("upgrade") || !wsKey || wsVersion !== "13") {
    reject(clientSocket, 400, "Bad Request");
    return;
  }

  const portValue = incoming.searchParams.get("port") || "443";
  if (portValue !== "443" && portValue !== "80") {
    reject(clientSocket, 400, "Invalid Port");
    return;
  }

  const upstreamPath = incoming.searchParams.get("path") || "/";
  if (!upstreamPath.startsWith("/") || upstreamPath.startsWith("//") || /[\r\n\0]/.test(upstreamPath)) {
    reject(clientSocket, 400, "Invalid Path");
    return;
  }

  const port = Number(portValue);
  let upstream;
  let settled = false;
  let headerBuffer = Buffer.alloc(0);

  clientSocket.pause();

  const fail = (status = 502, message = "Bad Gateway") => {
    if (settled) return;
    settled = true;
    if (upstream && !upstream.destroyed) upstream.destroy();
    reject(clientSocket, status, message);
  };

  const onClientClose = () => {
    if (upstream && !upstream.destroyed) upstream.destroy();
  };
  clientSocket.on("close", onClientClose);
  clientSocket.on("error", onClientClose);

  const options = { host: VPS_HOST, port };
  upstream = port === 443
    ? tls.connect({ ...options, servername: VPS_HOST, rejectUnauthorized: true })
    : net.connect(options);

  upstream.setTimeout(CONNECT_TIMEOUT_MS);
  upstream.once("timeout", () => fail(504, "Gateway Timeout"));
  upstream.once("error", () => fail(502, "Bad Gateway"));

  const sendHandshake = () => {
    const lines = [`GET ${upstreamPath} HTTP/1.1`, `Host: ${VPS_HOST}`];

    for (const [name, rawValue] of Object.entries(req.headers)) {
      const lower = name.toLowerCase();
      if (lower === "host" || lower === "connection" || lower === "upgrade" || lower === "proxy-connection") continue;
      const values = Array.isArray(rawValue) ? rawValue : [rawValue];
      for (const value of values) {
        if (typeof value === "string" && !/[\r\n]/.test(value)) lines.push(`${name}: ${value}`);
      }
    }

    lines.push("Connection: Upgrade", "Upgrade: websocket", "", "");
    upstream.write(lines.join("\r\n"));
  };

  if (port === 443) upstream.once("secureConnect", sendHandshake);
  else upstream.once("connect", sendHandshake);

  upstream.on("data", (chunk) => {
    if (settled) return;

    headerBuffer = Buffer.concat([headerBuffer, chunk]);
    if (headerBuffer.length > MAX_HEADER_BYTES) {
      fail(502, "Upstream Header Too Large");
      return;
    }

    const boundary = headerBuffer.indexOf("\r\n\r\n");
    if (boundary === -1) return;

    const responseHeader = headerBuffer.subarray(0, boundary + 4);
    const remaining = headerBuffer.subarray(boundary + 4);
    const statusLine = responseHeader.toString("latin1").split("\r\n", 1)[0];
    const match = /^HTTP\/1\.[01] (\d{3})(?:\s|$)/.exec(statusLine);
    if (!match) {
      fail(502, "Invalid Upstream Response");
      return;
    }

    const status = Number(match[1]);
    settled = true;
    upstream.setTimeout(0);

    // Forward the origin's response unchanged. In particular, only the origin's
    // real 101 response completes the WebSocket handshake at the client.
    clientSocket.write(responseHeader);

    if (remaining.length) clientSocket.write(remaining);

    if (status === 101) {
      upstream.pipe(clientSocket);
      clientSocket.pipe(upstream);
      clientSocket.resume();
      upstream.on("error", () => clientSocket.destroy());
      clientSocket.on("error", () => upstream.destroy());
    } else {
      upstream.pipe(clientSocket);
      upstream.on("end", () => clientSocket.end());
      clientSocket.on("close", () => upstream.destroy());
    }
  });

  upstream.once("close", () => {
    if (!settled && !clientSocket.destroyed) fail(502, "Upstream Closed");
  });
});

export default server;
