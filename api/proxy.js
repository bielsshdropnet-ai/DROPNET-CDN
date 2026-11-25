
import http from "node:http";
import WebSocket, { WebSocketServer } from "ws";

const VPS_HOST = "cdn.dpnet.online";

const TARGETS = {
  "80": `ws://${VPS_HOST}:80`,
  "443": `wss://${VPS_HOST}:443`
};

const server = http.createServer((req, res) => {
  const url = new URL(req.url || "/", "http://localhost");

  if (url.pathname === "/api/proxy/health") {
    res.writeHead(200, {
      "Content-Type": "application/json"
    });
    res.end(JSON.stringify({
      service: "DROPNET CDN",
      status: "online",
      ports: [80, 443]
    }));
    return;
  }

  res.writeHead(426, {
    "Content-Type": "text/plain",
    "Upgrade": "websocket"
  });
  res.end("WebSocket necessário.");
});

const wss = new WebSocketServer({
  noServer: true,
  perMessageDeflate: false,
  maxPayload: 64 * 1024 * 1024
});

function errorResponse(socket, status, message) {
  if (socket.destroyed) return;

  socket.end(
    `HTTP/1.1 ${status}\r\n` +
    "Connection: close\r\n" +
    "Content-Type: text/plain; charset=utf-8\r\n" +
    `Content-Length: ${Buffer.byteLength(message)}\r\n\r\n` +
    message
  );
}

server.on("upgrade", (req, socket, head) => {
  const incoming = new URL(req.url || "/", "http://localhost");

  if (incoming.pathname !== "/api/proxy") {
    errorResponse(socket, "404 Not Found", "Rota inexistente.");
    return;
  }

  const port = incoming.searchParams.get("port") || "443";
  const base = TARGETS[port];

  if (!base) {
    errorResponse(socket, "400 Bad Request", "Porta inválida.");
    return;
  }

  const path = incoming.searchParams.get("path") || "/";

  if (!path.startsWith("/") || path.startsWith("//")) {
    errorResponse(socket, "400 Bad Request", "Caminho inválido.");
    return;
  }

  const target = new URL(path, base);
  const expected = new URL(base);

  if (target.host !== expected.host) {
    errorResponse(socket, "400 Bad Request", "Destino inválido.");
    return;
  }

  for (const [key, value] of incoming.searchParams) {
    if (key !== "port" && key !== "path") {
      target.searchParams.append(key, value);
    }
  }

  const headers = {};

  for (const [name, value] of Object.entries(req.headers)) {
    const key = name.toLowerCase();

    if (
      key === "host" ||
      key === "connection" ||
      key === "upgrade" ||
      key.startsWith("sec-websocket-") ||
      key === "content-length"
    ) continue;

    headers[name] = value;
  }

  const upstream = new WebSocket(target.toString(), {
    headers,
    handshakeTimeout: 15000,
    perMessageDeflate: false,
    rejectUnauthorized: true
  });

  let finished = false;

  function fail(message) {
    if (finished) return;
    finished = true;
    upstream.terminate();
    errorResponse(socket, "502 Bad Gateway", message);
  }

  socket.on("close", () => {
    if (!finished) upstream.terminate();
  });

  socket.on("error", () => upstream.terminate());

  upstream.on("unexpected-response", (request, response) => {
    const status = response.statusCode || "desconhecido";
    response.resume();
    fail(`Handshake da origem rejeitado: HTTP ${status}.`);
  });

  upstream.on("error", error => {
    fail(`Falha na origem: ${error.message}`);
  });

  upstream.on("open", () => {
    if (finished || socket.destroyed) {
      upstream.terminate();
      return;
    }

    wss.handleUpgrade(req, socket, head, client => {
      finished = true;

      client.on("message", (data, binary) => {
        if (upstream.readyState === WebSocket.OPEN) {
          upstream.send(data, { binary });
        }
      });

      upstream.on("message", (data, binary) => {
        if (client.readyState === WebSocket.OPEN) {
          client.send(data, { binary });
        }
      });

      client.on("close", () => upstream.terminate());
      upstream.on("close", () => client.close());
      client.on("error", () => upstream.terminate());
      upstream.on("error", () => client.terminate());
    });
  });
});

export default server;
