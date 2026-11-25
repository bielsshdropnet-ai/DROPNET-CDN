import http from "node:http";
import WebSocket, { WebSocketServer } from "ws";

const VPS_HOST = "cdn.dpnet.online";

const DESTINOS = {
  "80": `ws://${VPS_HOST}:80`,
  "443": `wss://${VPS_HOST}:443`
};

const HANDSHAKE_TIMEOUT = 15000;

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

  res.end("DROPNET CDN - WebSocket necessário.");
});

const wss = new WebSocketServer({
  noServer: true,
  perMessageDeflate: false,
  maxPayload: 64 * 1024 * 1024
});

function responderErro(socket, status, mensagem) {
  if (socket.destroyed) return;

  const body = mensagem;

  socket.end(
    `HTTP/1.1 ${status}\r\n` +
    "Connection: close\r\n" +
    "Content-Type: text/plain; charset=utf-8\r\n" +
    `Content-Length: ${Buffer.byteLength(body)}\r\n\r\n` +
    body
  );
}

function encaminhar(cliente, upstream) {
  cliente.on("message", (data, binary) => {
    if (upstream.readyState === WebSocket.OPEN) {
      upstream.send(data, { binary });
    }
  });

  upstream.on("message", (data, binary) => {
    if (cliente.readyState === WebSocket.OPEN) {
      cliente.send(data, { binary });
    }
  });

  cliente.on("close", () => {
    if (upstream.readyState === WebSocket.OPEN) {
      upstream.close();
    } else {
      upstream.terminate();
    }
  });

  upstream.on("close", () => {
    if (cliente.readyState === WebSocket.OPEN) {
      cliente.close();
    }
  });

  cliente.on("error", () => upstream.terminate());
  upstream.on("error", () => cliente.terminate());
}

server.on("upgrade", (req, socket, head) => {
  const incoming = new URL(
    req.url || "/",
    "http://localhost"
  );

  if (incoming.pathname !== "/api/proxy") {
    responderErro(socket, "404 Not Found", "Rota inexistente.");
    return;
  }

  const port = incoming.searchParams.get("port") || "443";
  const base = DESTINOS[port];

  if (!base) {
    responderErro(socket, "400 Bad Request", "Porta inválida. Use 80 ou 443.");
    return;
  }

  const path = incoming.searchParams.get("path") || "/";

  if (!path.startsWith("/") || path.startsWith("//")) {
    responderErro(socket, "400 Bad Request", "Caminho inválido.");
    return;
  }

  let target;

  try {
    target = new URL(path, base);

    const expected = new URL(base);

    if (target.host !== expected.host) {
      responderErro(socket, "400 Bad Request", "Destino inválido.");
      return;
    }

    for (const [key, value] of incoming.searchParams) {
      if (key !== "port" && key !== "path") {
        target.searchParams.append(key, value);
      }
    }
  } catch {
    responderErro(socket, "400 Bad Request", "URL inválida.");
    return;
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
    handshakeTimeout: HANDSHAKE_TIMEOUT,
    perMessageDeflate: false,
    rejectUnauthorized: true
  });

  let finished = false;

  function falhar(mensagem) {
    if (finished) return;
    finished = true;

    upstream.terminate();

    responderErro(socket, "502 Bad Gateway", mensagem);
  }

  socket.on("close", () => {
    if (!finished) upstream.terminate();
  });

  socket.on("error", () => upstream.terminate());

  upstream.on("unexpected-response", (request, response) => {
    const status = response.statusCode || "desconhecido";

    falhar(
      `A VPS rejeitou o handshake. HTTP ${status}. Verifique o caminho e o serviço WebSocket.`
    );

    response.resume();
  });

  upstream.on("error", (error) => {
    falhar(`Falha na conexão com a VPS: ${error.message}`);
  });

  upstream.on("open", () => {
    if (finished || socket.destroyed) {
      upstream.terminate();
      return;
    }

    try {
      wss.handleUpgrade(req, socket, head, (cliente) => {
        finished = true;
        encaminhar(cliente, upstream);
      });
    } catch (error) {
      falhar("Falha ao aceitar conexão do cliente.");
    }
  });
});

export default server;
