import http from "node:http";
import net from "node:net";
import tls from "node:tls";
import { WebSocketServer, WebSocket } from "ws";

const VPS_HOST = "cdn.dpnet.online";

const server = http.createServer((req, res) => {
  const url = new URL(req.url || "/", "http://localhost");

  if (url.pathname === "/api/proxy" && url.searchParams.get("health") === "1") {
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify({
      service: "DROPNET CDN",
      status: "online",
      host: VPS_HOST,
      ports: [80, 443],
      mode: "tcp-bridge"
    }));
    return;
  }

  res.writeHead(426, {
    "Content-Type": "text/plain",
    "Upgrade": "websocket"
  });
  res.end("DROPNET CDN - WebSocket necessário.");
});

const wss = new WebSocketServer({
  noServer: true,
  perMessageDeflate: false,
  maxPayload: 64 * 1024 * 1024
});

server.on("upgrade", (req, clientSocket, head) => {
  const incoming = new URL(req.url || "/", "http://localhost");

  if (incoming.pathname !== "/api/proxy") {
    clientSocket.destroy();
    return;
  }

  const port = incoming.searchParams.get("port") || "443";

  if (port !== "80" && port !== "443") {
    clientSocket.destroy();
    return;
  }

  const path = incoming.searchParams.get("path") || "/";

  if (!path.startsWith("/") || path.startsWith("//") || /[\r\n]/.test(path)) {
    clientSocket.destroy();
    return;
  }

  const key = req.headers["sec-websocket-key"];

  if (!key) {
    clientSocket.destroy();
    return;
  }

  const headers = [
    `GET ${path} HTTP/1.1`,
    `Host: ${VPS_HOST}`,
    "Connection: Upgrade",
    "Upgrade: websocket",
    `Sec-WebSocket-Key: ${key}`,
    "Sec-WebSocket-Version: 13"
  ];

  for (const [name, value] of Object.entries(req.headers)) {
    const lower = name.toLowerCase();

    if (
      lower === "host" ||
      lower === "connection" ||
      lower === "upgrade" ||
      lower.startsWith("sec-websocket-") ||
      lower === "content-length"
    ) continue;

    if (typeof value === "string" && !/[\r\n]/.test(value)) {
      headers.push(`${name}: ${value}`);
    }
  }

  const request = headers.join("\r\n") + "\r\n\r\n";

  wss.handleUpgrade(req, clientSocket, head, (client) => {
    let upstream;
    let closed = false;

    const closeBoth = () => {
      if (closed) return;
      closed = true;

      if (upstream && !upstream.destroyed) {
        upstream.destroy();
      }

      if (client.readyState === WebSocket.OPEN) {
        client.close();
      }
    };

    const options = {
      host: VPS_HOST,
      port: Number(port)
    };

    upstream = port === "443"
      ? tls.connect({
          ...options,
          servername: VPS_HOST,
          rejectUnauthorized: true
        })
      : net.connect(options);

    upstream.setTimeout(300000);

    upstream.on("connect", () => {
      upstream.write(request);
    });

    upstream.on("secureConnect", () => {
      upstream.write(request);
    });

    upstream.on("data", (data) => {
      if (client.readyState === WebSocket.OPEN) {
        client.send(data, { binary: true });
      }
    });

    client.on("message", (data) => {
      if (upstream && !upstream.destroyed) {
        upstream.write(data);
      }
    });

    upstream.on("timeout", closeBoth);
    upstream.on("error", closeBoth);
    upstream.on("close", closeBoth);
    client.on("close", closeBoth);
    client.on("error", closeBoth);
  });
});

export default server;
