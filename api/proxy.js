import http from "node:http";
import net from "node:net";
import tls from "node:tls";
import crypto from "node:crypto";
import { WebSocketServer, WebSocket } from "ws";

// Servidor para onde a Vercel conecta (DNS)
const VPS_HOST = "gm.dpnet.online";
// Nome do certificado HTTPS que o VPS apresenta (usado so na porta 443)
const TLS_NAME = "cdn.dpnet.online";
const WS_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";

function reject(socket, status, text) {
  if (socket.writable) {
    socket.write(
      `HTTP/1.1 ${status} ${text}\r\nConnection: close\r\nContent-Length: 0\r\n\r\n`
    );
  }
  socket.destroy();
}

// STRIPPER-START
// Remove os cabecalhos "HTTP/1.1 101 ..." / "HTTP/1.1 200 ..." que o servico do VPS
// manda no inicio, deixando passar so os dados reais (ex.: banner do SSH).
function createHeadStripper() {
  let pending = Buffer.alloc(0);
  let done = false;
  return (data) => {
    if (done) return data;
    pending = Buffer.concat([pending, data]);
    while (pending.length >= 5 && pending.subarray(0, 5).toString("latin1") === "HTTP/") {
      const end = pending.indexOf("\r\n\r\n");
      if (end === -1) return null; // cabecalho incompleto, espera mais dados
      pending = pending.subarray(end + 4);
    }
    if (pending.length === 0) return null;
    if (pending.length < 5) return null; // pode ser o comeco de outro "HTTP/"
    done = true;
    const out = pending;
    pending = Buffer.alloc(0);
    return out;
  };
}
// STRIPPER-END

function connectUpstream(port, onReady) {
  return port === "443"
    ? tls.connect(
        { host: VPS_HOST, port: 443, servername: TLS_NAME, rejectUnauthorized: true },
        onReady
      )
    : net.connect({ host: VPS_HOST, port: 80 }, onReady);
}

// Modo "raw" (HTTP Injector / payload): responde 101 e depois so repassa bytes crus
function startRawTunnel(client, head, port, request) {
  let upstream;
  let closed = false;
  let ready = false;
  const queue = [];
  const strip = createHeadStripper();

  const closeBoth = () => {
    if (closed) return;
    closed = true;
    client.destroy();
    if (upstream && !upstream.destroyed) upstream.destroy();
  };

  const onReady = () => {
    ready = true;
    console.log(`[raw] conectado ao VPS ${VPS_HOST}:${port}`);
    upstream.write(request);
    if (head && head.length) upstream.write(head);
    for (const chunk of queue) upstream.write(chunk);
    queue.length = 0;
  };

  upstream = connectUpstream(port, onReady);
  upstream.setTimeout(300000);

  upstream.on("data", (data) => {
    const out = strip(data);
    if (out && out.length && client.writable) client.write(out);
  });

  client.on("data", (data) => {
    if (!upstream || upstream.destroyed) return;
    if (ready) upstream.write(data);
    else queue.push(data);
  });

  upstream.on("timeout", () => {
    console.error("[raw] timeout na conexao com o VPS");
    closeBoth();
  });
  upstream.on("error", (err) => {
    console.error(`[raw] erro upstream: ${err.code || ""} ${err.message}`);
    closeBoth();
  });
  upstream.on("close", closeBoth);
  client.on("close", closeBoth);
  client.on("error", closeBoth);
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
        modes: ["ws", "raw"]
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

  const raw = incoming.searchParams.get("raw") === "1";

  let key = req.headers["sec-websocket-key"];
  if (!key) {
    if (raw) key = crypto.randomBytes(16).toString("base64");
    else return reject(clientSocket, 400, "Bad Request");
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

  if (raw) {
    const accept = crypto
      .createHash("sha1")
      .update(key + WS_GUID)
      .digest("base64");
    clientSocket.write(
      "HTTP/1.1 101 Switching Protocols\r\n" +
        "Upgrade: websocket\r\n" +
        "Connection: Upgrade\r\n" +
        `Sec-WebSocket-Accept: ${accept}\r\n\r\n`
    );
    startRawTunnel(clientSocket, head, port, request);
    return;
  }

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

    upstream = connectUpstream(port, onReady);
    upstream.setTimeout(300000);

    upstream.on("data", (data) => {
      bytesFromVps += data.length;
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
