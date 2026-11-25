import http from "node:http";
import WebSocket, { WebSocketServer } from "ws";

// ======================================
// DROPNET CDN - WEBSOCKET REVERSE PROXY
// Vercel Functions + Node.js + ws
// ======================================

// Coloque somente o domínio ou IP da VPS.
// Não coloque http://, https:// ou a porta.
const VPS_HOST = "cdn.dpnet.online";

// Destinos disponíveis na VPS.
const DESTINOS = {
  "80": `ws://${VPS_HOST}:80`,
  "443": `wss://${VPS_HOST}:443`,
};

const HANDSHAKE_TIMEOUT = 12000;

function responderErro(socket, status, mensagem) {
  if (socket.destroyed) return;

  const body = mensagem;
  socket.end(
    `HTTP/1.1 ${status}\r\n` +
    "Connection: close\r\n" +
    "Content-Type: text/plain; charset=utf-8\r\n" +
    `Content-Length: ${Buffer.byteLength(body)}\r\n` +
    "\r\n" +
    body
  );
}

function codigoFechamentoValido(code) {
  return (
    code === 1000 ||
    code === 1001 ||
    code === 1002 ||
    code === 1003 ||
    (code >= 1007 && code <= 1014) ||
    (code >= 3000 && code <= 4999)
  ) ? code : 1000;
}

function encaminhar(client, upstream) {
  client.on("message", (data, isBinary) => {
    if (upstream.readyState === WebSocket.OPEN) {
      upstream.send(data, { binary: isBinary }, (error) => {
        if (error && client.readyState === WebSocket.OPEN) {
          client.close(1011, "Erro ao encaminhar dados");
        }
      });
    }
  });

  upstream.on("message", (data, isBinary) => {
    if (client.readyState === WebSocket.OPEN) {
      client.send(data, { binary: isBinary }, (error) => {
        if (error && upstream.readyState === WebSocket.OPEN) {
          upstream.close(1011, "Erro ao encaminhar dados");
        }
      });
    }
  });

  client.on("close", (code, reason) => {
    if (upstream.readyState === WebSocket.OPEN) {
      upstream.close(codigoFechamentoValido(code), reason.toString());
    } else if (upstream.readyState === WebSocket.CONNECTING) {
      upstream.terminate();
    }
  });

  upstream.on("close", (code, reason) => {
    if (client.readyState === WebSocket.OPEN) {
      client.close(codigoFechamentoValido(code), reason.toString());
    }
  });

  client.on("error", () => upstream.terminate());

  upstream.on("error", () => {
    if (client.readyState === WebSocket.OPEN) {
      client.close(1011, "Erro na VPS");
    }
  });
}

// Servidor HTTP da Function.
const server = http.createServer((req, res) => {
  const url = new URL(req.url || "/", "http://localhost");

  if (url.pathname === "/api/proxy/health") {
    res.writeHead(200, {
      "Content-Type": "application/json"
    });

    res.end(JSON.stringify({
      service: "DROPNET CDN",
      status: "online",
      websocket: true,
      ports: [80, 443]
    }));

    return;
  }

  res.writeHead(426, {
    "Content-Type": "text/plain; charset=utf-8",
    "Upgrade": "websocket"
  });

  res.end("DROPNET CDN: conecte usando WebSocket.");
});

const wss = new WebSocketServer({
  noServer: true,
  perMessageDeflate: false,
  maxPayload: 64 * 1024 * 1024,

  handleProtocols(protocols, request) {
    const selected = request.__dropnetProtocol;

    return selected && protocols.has(selected)
      ? selected
      : false;
  }
});

// Recebe o Upgrade WebSocket.
server.on("upgrade", (req, socket, head) => {
  const incoming = new URL(
    req.url || "/",
    "http://localhost"
  );

  if (incoming.pathname !== "/api/proxy") {
    responderErro(socket, "404 Not Found", "Rota inexistente.");
    return;
  }

  // Seleciona a porta de destino.
  const port = incoming.searchParams.get("port") || "443";
  const base = DESTINOS[port];

  if (!base || VPS_HOST === "SEU-DOMINIO-VPS") {
    responderErro(
      socket,
      "500 Internal Server Error",
      "Configure o domínio da VPS em VPS_HOST."
    );
    return;
  }

  // Caminho que será enviado à VPS.
  const targetPath = incoming.searchParams.get("path") || "/";

  if (
    !targetPath.startsWith("/") ||
    targetPath.startsWith("//")
  ) {
    responderErro(socket, "400 Bad Request", "Caminho inválido.");
    return;
  }

  let target;

  try {
    target = new URL(targetPath, base);

    const expected = new URL(base);

    if (target.host !== expected.host) {
      responderErro(socket, "400 Bad Request", "Destino inválido.");
      return;
    }

    // Encaminha os parâmetros adicionais.
    for (const [key, value] of incoming.searchParams) {
      if (key !== "port" && key !== "path") {
        target.searchParams.append(key, value);
      }
    }
  } catch {
    responderErro(socket, "400 Bad Request", "URL inválida.");
    return;
  }

  // Protocolos WebSocket solicitados pelo cliente.
  const protocols = String(
    req.headers["sec-websocket-protocol"] || ""
  )
    .split(",")
    .map(value => value.trim())
    .filter(Boolean);

  // Preserva cabeçalhos úteis, removendo os do handshake original.
  const headers = {};

  for (const [name, value] of Object.entries(req.headers)) {
    const key = name.toLowerCase();

    if (
      key === "host" ||
      key === "connection" ||
      key === "upgrade" ||
      key.startsWith("sec-websocket-") ||
      key === "content-length"
    ) {
      continue;
    }

    headers[name] = value;
  }

  // Abre primeiro a conexão com a VPS.
  const upstream = new WebSocket(
    target.toString(),
    protocols,
    {
      headers,
      handshakeTimeout: HANDSHAKE_TIMEOUT,
      perMessageDeflate: false,
      rejectUnauthorized: true
    }
  );

  let accepted = false;
  let finished = false;

  function falhar(mensagem) {
    if (finished) return;

    finished = true;
    upstream.terminate();

    responderErro(
      socket,
      "502 Bad Gateway",
      mensagem
    );
  }

  socket.on("error", () => upstream.terminate());

  socket.on("close", () => {
    if (!accepted) upstream.terminate();
  });

  upstream.on("unexpected-response", (request, response) => {
    const status = response.statusCode || "desconhecido";
    response.resume();

    falhar(`A VPS não aceitou o WebSocket. HTTP ${status}.`);
  });

  upstream.on("error", (error) => {
    if (!accepted) {
      falhar(`Falha na conexão com a VPS: ${error.message}`);
    }
  });

  upstream.on("close", () => {
    if (!accepted && !finished) {
      falhar("A VPS fechou a conexão antes do handshake.");
    }
  });

  // Só aceita o cliente depois que a VPS responder ao handshake.
  upstream.once("open", () => {
    if (socket.destroyed || finished) {
      upstream.terminate();
      return;
    }

    req.__dropnetProtocol = upstream.protocol || "";

    try {
      wss.handleUpgrade(req, socket, head, (client) => {
        accepted = true;
        finished = true;

        encaminhar(client, upstream);
      });
    } catch {
      upstream.terminate();
      responderErro(
        socket,
        "502 Bad Gateway",
        "Falha ao concluir o handshake WebSocket."
      );
    }
  });
});

// A Vercel gerencia o servidor; não chame server.listen().
export default server;
