import http from "node:http";
import net from "node:net";
import tls from "node:tls";
import { WebSocketServer, WebSocket } from "ws";

const VPS_HOST = "cdn.dpnet.online";

// Responde um erro HTTP simples antes do upgrade (em vez de derrubar a conexao calado)
function reject(socket, status, text) {
  if (socket.writable) {
    socket.write(
      `HTTP/1.1 ${status} ${text}\r\nConnection: close\r\nContent-Length: 0\r\n\r\n`
    );
  }
  socket.destroy();
}

const server = http.createServer((req, res) => {
  const url = new URL(req.url || "/", "http://localhost");

  if (url.searchParams.get("health") === "1") {
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(
      JSON.stringify({
        service: "DROPNET CDN",
        status: "online",
        host: VPS_HOST,
        ports: [80, 443],
        mode: "tcp-bridge"
      })
    );
    return;
  }

  res.writeHead(426, {
    "Content-Type": "text/plain; charset=utf-8",
    Upgrade: "websocket"
  });
  res.end("DROPNET CDN - WebSocket necessario.");
});

const wss = new WebSocketServer({
  noServer: true,
  perMessageDeflate: false,
  maxPayload: 64 * 1024 * 1024
});

server.on("upgrade", (req, clientSocket, head) => {
  const incoming = new URL(req.url || "/", "http://localhost");

  const port = incoming.searchParams.get("port") || "443";
  if (port !== "80" && port !== "443") {
    return reject(clientSocket, 400, "Bad Request");
  }

  const path = incoming.searchParams.get("path") || "/";
  if (!path.startsWith("/") || path.startsWith("//") || /[\r\n]/.test(path)) {
    return reject(clientSocket, 400, "Bad Request");
  }

  const key = req.headers["sec-websocket-key"];
  if (!key) {
    return reject(clientSocket, 400, "Bad Request");
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
    ) {
      continue;
    }

    if (typeof value === "string" && !/[\r\n]/.test(value)) {
      headers.push(`${name}: ${value}`);
    }
  }

  const request = headers.join("\r\n") + "\r\n\r\n";

  wss.handleUpgrade(req, clientSocket, head, (client) => {
    let upstream;
    let closed = false;
    let ready = false;
    const queue = [];

    const closeBoth = () => {
      if (closed) return;
      closed = true;

      if (upstream && !upstream.destroyed) {
        upstream.destroy();
      }

      if (
        client.readyState === WebSocket.OPEN ||
        client.readyState === WebSocket.CONNECTING
      ) {
        client.close();
      }
    };

    // Chamado UMA vez quando a conexao com o VPS esta pronta
    // (443: apos o handshake TLS; 80: apos conectar o TCP)
    const onReady = () => {
      ready = true;
      upstream.write(request);
      for (const chunk of queue) upstream.write(chunk);
      queue.length = 0;
    };

    upstream =
      port === "443"
        ? tls.connect(
            { host: VPS_HOST, port: 443, servername: VPS_HOST, rejectUnauthorized: true },
            onReady
          )
        : net.connect({ host: VPS_HOST, port: 80 }, onReady);

    upstream.setTimeout(300000);

    upstream.on("data", (data) => {
      if (client.readyState === WebSocket.OPEN) {
        client.send(data, { binary: true });
      }
    });

    client.on("message", (data) => {
      if (!upstream || upstream.destroyed) return;
      if (ready) upstream.write(data);
      else queue.push(data);
    });

    upstream.on("timeout", closeBoth);
    upstream.on("error", closeBoth);
    upstream.on("close", closeBoth);
    client.on("close", closeBoth);
    client.on("error", closeBoth);
  });
});

export default server;
