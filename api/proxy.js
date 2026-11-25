import http from "node:http";
import net from "node:net";
import tls from "node:tls";
import { WebSocketServer, WebSocket } from "ws";

// Servidor para onde a Vercel conecta (DNS)
const VPS_HOST = "gm.dpnet.online";
// Nome do certificado HTTPS que o VPS apresenta (SNI + validacao)
const TLS_NAME = "cdn.dpnet.online";

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
        tlsName: TLS_NAME,
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
    let bytesFromVps = 0;
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

    const onReady = () => {
      ready = true;
      console.log(`[proxy] conectado ao VPS ${VPS_HOST}:${port}`);
      upstream.write(request);
      for (const chunk of queue) upstream.write(chunk);
      queue.length = 0;
    };

    upstream =
      port === "443"
        ? tls.connect(
            {
              host: VPS_HOST,
              port: 443,
              servername: TLS_NAME,
              rejectUnauthorized: true
            },
            onReady
          )
        : net.connect({ host: VPS_HOST, port: 80 }, onReady);

    upstream.setTimeout(300000);

    let loggedFirst = false;
    upstream.on("data", (data) => {
      bytesFromVps += data.length;
      if (!loggedFirst) {
        loggedFirst = true;
        const first = data
          .subarray(0, 160)
          .toString("latin1")
          .replace(/\r/g, "\\r")
          .replace(/\n/g, "\\n");
        console.log(`[proxy] primeira resposta do VPS: ${first}`);
      }
      if (client.readyState === WebSocket.OPEN) {
        client.send(data, { binary: true });
      }
    });

    client.on("message", (data) => {
      if (!upstream || upstream.destroyed) return;
      if (ready) upstream.write(data);
      else queue.push(data);
    });

    upstream.on("timeout", () => {
      console.error("[proxy] timeout na conexao com o VPS");
      closeBoth();
    });
    upstream.on("error", (err) => {
      console.error(`[proxy] erro upstream: ${err.code || ""} ${err.message}`);
      closeBoth();
    });
    upstream.on("close", () => {
      console.log(`[proxy] upstream fechou (bytes recebidos: ${bytesFromVps})`);
      closeBoth();
    });
    client.on("close", closeBoth);
    client.on("error", closeBoth);
  });
});

export default server;
