import http from "node:http";
import net from "node:net";
import { WebSocketServer, WebSocket } from "ws";

const VPS_HOST = "gm.dpnet.online";

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
  return typeof path === "string" &&
    path.startsWith("/") &&
    !path.startsWith("//") &&
    !/[\r\n]/.test(path);
}

function connectUpstream() {
  return new Promise((resolve, rejectPromise) => {
    const socket = net.connect({
      host: VPS_HOST,
      port: 80
    });

    const timer = setTimeout(() => {
      socket.destroy();
      rejectPromise(new Error("Timeout na origem"));
    }, 10000);

    socket.once("error", error => {
      clearTimeout(timer);
      rejectPromise(error);
    });

    socket.once("connect", () => {
      clearTimeout(timer);
      resolve(socket);
    });
  });
}

function createResponseStripper() {
  let pending = Buffer.alloc(0);
  let finished = false;

  return chunk => {
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
          throw new Error("Cabeçalho muito grande");
        }
        return null;
      }

      pending = pending.subarray(end + 4);
    }

    return null;
  };
}

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
      port: 80,
      mode: "websocket-tcp"
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
  maxPayload: 16 * 1024 * 1024
});

server.on("upgrade", async (req, socket, head) => {
  const url = new URL(req.url || "/", "http://localhost");
  const path = url.searchParams.get("path") || "/";

  if (!validPath(path)) {
    return reject(socket, 400, "Invalid Path");
  }

  if (
    !req.headers["sec-websocket-key"] ||
    !req.headers.upgrade?.toLowerCase().includes("websocket")
  ) {
    return reject(socket, 400, "Invalid WebSocket");
  }

  let upstream;

  try {
    upstream = await connectUpstream();
  } catch (error) {
    console.error("[DROPNET] Origin:", error.message);
    return reject(socket, 502, "Origin Connection Failed");
  }

  wss.handleUpgrade(req, socket, head, client => {
    let closed = false;
    const stripHeaders = createResponseStripper();

    function closeBoth() {
      if (closed) return;
      closed = true;

      if (!upstream.destroyed) upstream.destroy();

      if (client.readyState === WebSocket.OPEN) {
        client.terminate();
      }
    }

    upstream.on("data", chunk => {
      let output;

      try {
        output = stripHeaders(chunk);
      } catch (error) {
        console.error("[DROPNET] Response:", error.message);
        closeBoth();
        return;
      }

      if (
        output?.length &&
        client.readyState === WebSocket.OPEN
      ) {
        client.send(output, { binary: true }, error => {
          if (error) closeBoth();
        });
      }
    });

    client.on("message", data => {
      if (upstream.destroyed || !upstream.writable) {
        closeBoth();
        return;
      }

      upstream.write(Buffer.from(data));
    });

    upstream.on("error", error => {
      console.error("[DROPNET] TCP:", error.message);
      closeBoth();
    });

    upstream.on("end", () => {
      if (client.readyState === WebSocket.OPEN) {
        client.close(1000, "Origin ended");
      }
    });

    upstream.on("close", closeBoth);
    client.on("close", closeBoth);
    client.on("error", closeBoth);

    upstream.write(makeOriginRequest(path));
  });
});

export default server;
