import http from "node:http";
import net from "node:net";
import tls from "node:tls";
import { WebSocketServer, WebSocket } from "ws";

const VPS_HOST = "gm.dpnet.online";
const TLS_NAME = "cdn.dpnet.online";

function reject(socket, status, message) {
  if (!socket.destroyed && socket.writable) {
    socket.end(
      `HTTP/1.1 ${status} ${message}\r\n` +
      "Connection: close\r\n" +
      "Content-Length: 0\r\n\r\n"
    );
  }
}

function validPath(path) {
  return (
    typeof path === "string" &&
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

    const event = port === "443" ? "secureConnect" : "connect";

    const timer = setTimeout(() => {
      socket.destroy();
      rejectPromise(new Error("Tempo esgotado ao conectar à origem"));
    }, 10000);

    const onError = (error) => {
      clearTimeout(timer);
      rejectPromise(error);
    };

    socket.once("error", onError);

    socket.once(event, () => {
      clearTimeout(timer);
      socket.removeListener("error", onError);
      resolve(socket);
    });
  });
}

// A origem responde com cabeçalhos HTTP próprios.
// Retiramos esses cabeçalhos antes de transportar os dados pelo WebSocket.
function createResponseStripper() {
  let pending = Buffer.alloc(0);
  let finished = false;

  return (chunk) => {
    if (finished) return chunk;

    pending = Buffer.concat([pending, chunk]);

    while (pending.length >= 5) {
      if (pending.subarray(0, 5).toString("latin1") !== "HTTP/") {
        finished = true;
        const output = pending;
        pending = Buffer.alloc(0);
        return output;
      }

      const end = pending.indexOf("\r\n\r\n");

      if (end === -1) {
        if (pending.length > 65536) {
          throw new Error("Cabeçalho da origem excedeu o limite");
        }
        return null;
      }

      pending = pending.subarray(end + 4);
    }

    return null;
  };
}

// Requisição HTTP enviada à origem, não é um segundo handshake WebSocket.
function makeOriginRequest(path) {
  return [
    `GET ${path} HTTP/1.1`,
    `Host: ${VPS_HOST}`,
    "Connection: Upgrade",
    "Upgrade: websocket",
    "User-Agent: DROPNET-CDN/2.1",
    "",
    ""
  ].join("\r\n");
}

const server = http.createServer((req, res) => {
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
      originPort: 80,
      websocket: true,
      mode: "tcp-bridge"
    }));
  }

  res.writeHead(426, {
    "Content-Type": "text/plain",
    "Cache-Control": "no-store",
    Upgrade: "websocket"
  });

  res.end("DROPNET CDN - Conexao WebSocket necessaria.");
});

const wss = new WebSocketServer({
  noServer: true,
  perMessageDeflate: false,
  maxPayload: 16 * 1024 * 1024
});

server.on("upgrade", async (req, socket, head) => {
  const incoming = new URL(req.url || "/", "http://localhost");

  // Porta da ORIGEM. A conexão do cliente com a Vercel é HTTPS/WSS.
  const port = incoming.searchParams.get("port") || "80";
  const path = incoming.searchParams.get("path") || "/";

  if (port !== "80" && port !== "443") {
    return reject(socket, 400, "Invalid Origin Port");
  }

  if (!validPath(path)) {
    return reject(socket, 400, "Invalid Origin Path");
  }

  const key = req.headers["sec-websocket-key"];

  if (
    typeof key !== "string" ||
    !req.headers.upgrade?.toLowerCase().includes("websocket")
  ) {
    return reject(socket, 400, "Invalid WebSocket Request");
  }

  let upstream;

  try {
    upstream = await connectUpstream(port);
  } catch (error) {
    console.error("[DROPNET] Origin connection:", error.message);
    return reject(socket, 502, "Origin Connection Failed");
  }

  wss.handleUpgrade(req, socket, head, (client) => {
    let closed = false;
    const stripHeaders = createResponseStripper();

    const closeBoth = () => {
      if (closed) return;
      closed = true;

      if (!upstream.destroyed) upstream.destroy();

      if (
        client.readyState === WebSocket.OPEN ||
        client.readyState === WebSocket.CONNECTING
      ) {
        client.terminate();
      }
    };

    // Dados TCP da origem -> frames WebSocket para o cliente.
    upstream.on("data", (chunk) => {
      let output;

      try {
        output = stripHeaders(chunk);
      } catch (error) {
        console.error("[DROPNET] Origin response:", error.message);
        closeBoth();
        return;
      }

      if (
        output &&
        output.length > 0 &&
        client.readyState === WebSocket.OPEN
      ) {
        client.send(output, { binary: true }, (error) => {
          if (error) closeBoth();
        });
      }
    });

    // Mensagens WebSocket do cliente -> bytes TCP para a origem.
    client.on("message", (data) => {
      if (upstream.destroyed || !upstream.writable) {
        closeBoth();
        return;
      }

      const payload = Array.isArray(data)
        ? Buffer.concat(data)
        : Buffer.from(data);

      upstream.write(payload);
    });

    upstream.on("error", (error) => {
      console.error("[DROPNET] Origin error:", error.message);
      closeBoth();
    });

    upstream.on("end", () => {
      if (client.readyState === WebSocket.OPEN) {
        client.close(1000, "Origin ended connection");
      }
    });

    upstream.on("close", closeBoth);
    client.on("close", closeBoth);
    client.on("error", (error) => {
      console.error("[DROPNET] WebSocket error:", error.message);
      closeBoth();
    });

    // Inicia a requisição HTTP na origem depois de aceitar o WebSocket.
    upstream.write(makeOriginRequest(path));
  });
});

export default server;
