import http from "node:http";
import net from "node:net";
import tls from "node:tls";
import crypto from "node:crypto";
import { WebSocketServer, WebSocket } from "ws";

const VPS_HOST = "gm.dpnet.online";
const TLS_NAME = "cdn.dpnet.online";
const WS_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";

function reject(socket, status, message) {
  if (socket.writable) {
    socket.end(
      `HTTP/1.1 ${status} ${message}\r\n` +
      "Connection: close\r\nContent-Length: 0\r\n\r\n"
    );
  }
}

function validPath(path) {
  return (
    path.startsWith("/") &&
    !path.startsWith("//") &&
    !/[\r\n]/.test(path)
  );
}

function connectUpstream(port) {
  return new Promise((resolve, rejectPromise) => {
    const socket = port === "443"
      ? tls.connect({
          host: VPS_HOST,
          port: 443,
          servername: TLS_NAME,
          rejectUnauthorized: true
        })
      : net.connect({
          host: VPS_HOST,
          port: 80
        });

    const onError = (error) => {
      socket.removeListener("connect", onConnect);
      rejectPromise(error);
    };

    const onConnect = () => {
      socket.removeListener("error", onError);
      resolve(socket);
    };

    socket.once("error", onError);
    socket.once(port === "443" ? "secureConnect" : "connect", onConnect);
  });
}

function createHeadStripper() {
  let pending = Buffer.alloc(0);
  let done = false;

  return (data) => {
    if (done) return data;

    pending = Buffer.concat([pending, data]);

    while (
      pending.length >= 5 &&
      pending.subarray(0, 5).toString("latin1") === "HTTP/"
    ) {
      const end = pending.indexOf("\r\n\r\n");
      if (end === -1) return null;
      pending = pending.subarray(end + 4);
    }

    if (pending.length === 0) return null;
    if (pending.length < 5) return null;

    done = true;
    const result = pending;
    pending = Buffer.alloc(0);
    return result;
  };
}

function makeUpstreamRequest(req, path) {
  const headers = [
    `GET ${path} HTTP/1.1`,
    `Host: ${VPS_HOST}`,
    "Connection: Upgrade",
    "Upgrade: websocket",
    `Sec-WebSocket-Key: ${req.headers["sec-websocket-key"]}`,
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

  return headers.join("\r\n") + "\r\n\r\n";
}

function startRawTunnel(client, head, upstream, request, debug) {
  let closed = false;
  let ready = false;
  const queue = [];
  const strip = createHeadStripper();

  const closeBoth = () => {
    if (closed) return;
    closed = true;
    client.destroy();
    if (!upstream.destroyed) upstream.destroy();
  };

  upstream.write(request);

  if (head?.length) upstream.write(head);

  upstream.on("data", (data) => {
    const output = debug ? data : strip(data);
    if (output?.length && client.writable) {
      client.write(output);
    }
  });

  client.on("data", (data) => {
    if (upstream.destroyed) return;
    if (ready) upstream.write(data);
    else queue.push(data);
  });

  upstream.on("connect", () => {
    ready = true;
    for (const chunk of queue) upstream.write(chunk);
    queue.length = 0;
  });

  upstream.on("secureConnect", () => {
    ready = true;
    for (const chunk of queue) upstream.write(chunk);
    queue.length = 0;
  });

  upstream.on("error", closeBoth);
  upstream.on("close", closeBoth);
  client.on("close", closeBoth);
  client.on("error", closeBoth);
}

const server = http.createServer(async (req, res) => {
  const url = new URL(req.url || "/", "http://localhost");

  if (url.searchParams.get("health") === "1") {
    res.writeHead(200, {
      "Content-Type": "application/json",
      "Cache-Control": "no-store"
    });

    return res.end(JSON.stringify({
      service: "DROPNET CDN",
      status: "online",
      host: VPS_HOST,
      ports: [80, 443],
      modes: ["raw", "ws"]
    }));
  }

  res.writeHead(426, {
    "Content-Type": "text/plain",
    "Cache-Control": "no-store",
    Upgrade: "websocket"
  });

  res.end("DROPNET CDN - WebSocket necessario.");
});

const wss = new WebSocketServer({
  noServer: true,
  perMessageDeflate: false,
  maxPayload: 64 * 1024 * 1024
});

server.on("upgrade", async (req, clientSocket, head) => {
  const incoming = new URL(req.url || "/", "http://localhost");
  const port = incoming.searchParams.get("port") || "443";
  const path = incoming.searchParams.get("path") || "/";
  const raw = incoming.searchParams.get("raw") === "1";
  const debug = incoming.searchParams.get("debug") === "1";

  if (port !== "80" && port !== "443") {
    return reject(clientSocket, 400, "Bad Request");
  }

  if (!validPath(path)) {
    return reject(clientSocket, 400, "Bad Request");
  }

  const key = req.headers["sec-websocket-key"];

  if (
    typeof key !== "string" ||
    !/^[A-Za-z0-9+/]{22}==$/.test(key) ||
    !req.headers.upgrade?.toLowerCase().includes("websocket")
  ) {
    return reject(clientSocket, 400, "Bad WebSocket Request");
  }

  let upstream;

  try {
    upstream = await connectUpstream(port);
  } catch (error) {
    console.error("[DROPNET] upstream:", error.message);
    return reject(clientSocket, 502, "Bad Gateway");
  }

  const request = makeUpstreamRequest(req, path);

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

    startRawTunnel(clientSocket, head, upstream, request, debug);
    return;
  }

  wss.handleUpgrade(req, clientSocket, head, (client) => {
    let closed = false;

    const closeBoth = () => {
      if (closed) return;
      closed = true;
      if (!upstream.destroyed) upstream.destroy();
      if (client.readyState === WebSocket.OPEN) client.close();
    };

    upstream.write(request);

    upstream.on("data", (data) => {
      if (client.readyState === WebSocket.OPEN) {
        client.send(data, { binary: true });
      }
    });

    client.on("message", (data) => {
      if (!upstream.destroyed) upstream.write(data);
    });

    upstream.on("error", closeBoth);
    upstream.on("close", closeBoth);
    client.on("close", closeBoth);
    client.on("error", closeBoth);
  });
});

export default server;
