import http from 'node:http';
import net from 'node:net';
import tls from 'node:tls';
import crypto from 'node:crypto';

// Mantém o destino usado pelo projeto DROPNET atual.
const UPSTREAM_HOST = process.env.UPSTREAM_HOST || 'gm.dpnet.online';
const TLS_SERVERNAME = process.env.TLS_SERVERNAME || 'cdn.dpnet.online';
const PORT = Number(process.env.PORT || 3000);
const WS_GUID = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11';

const server = http.createServer((req, res) => {
  const url = new URL(req.url || '/', 'http://localhost');
  if (url.pathname === '/api/proxy' && url.searchParams.get('health') === '1') {
    res.writeHead(200, {
      'content-type': 'application/json; charset=utf-8',
      'cache-control': 'no-store'
    });
    res.end(JSON.stringify({
      service: 'DROPNET CDN',
      status: 'online',
      upstream: UPSTREAM_HOST,
      ports: [80, 443],
      mode: 'websocket-raw-tunnel'
    }));
    return;
  }
  res.writeHead(426, {
    'content-type': 'text/plain; charset=utf-8',
    'cache-control': 'no-store',
    connection: 'close',
    upgrade: 'websocket'
  });
  res.end('DROPNET CDN: use WebSocket em /api/proxy');
});

function reject(socket, status, message) {
  if (!socket.destroyed && socket.writable) {
    socket.end(`HTTP/1.1 ${status} ${message}\r\nConnection: close\r\nContent-Length: 0\r\n\r\n`);
  }
}

function isValidWsRequest(req) {
  const key = req.headers['sec-websocket-key'];
  let decoded;
  try { decoded = Buffer.from(String(key || ''), 'base64'); } catch { return false; }
  return req.method === 'GET' &&
    String(req.headers.upgrade || '').toLowerCase() === 'websocket' &&
    String(req.headers.connection || '').toLowerCase().split(',').map(v => v.trim()).includes('upgrade') &&
    req.headers['sec-websocket-version'] === '13' &&
    typeof key === 'string' && decoded.length === 16;
}

function makeHandshake(path, port, incomingHeaders) {
  const key = crypto.randomBytes(16).toString('base64');
  const hostHeader = UPSTREAM_HOST;
  const lines = [
    `GET ${path} HTTP/1.1`,
    `Host: ${hostHeader}`,
    'Connection: Upgrade',
    'Upgrade: websocket',
    `Sec-WebSocket-Key: ${key}`,
    'Sec-WebSocket-Version: 13'
  ];
  for (const [name, value] of Object.entries(incomingHeaders)) {
    const lower = name.toLowerCase();
    if (['host', 'connection', 'upgrade', 'content-length', 'sec-websocket-key', 'sec-websocket-version', 'sec-websocket-extensions', 'sec-websocket-protocol'].includes(lower)) continue;
    if (typeof value === 'string' && !/[\r\n]/.test(value)) lines.push(`${name}: ${value}`);
  }
  const expectedAccept = crypto.createHash('sha1').update(key + WS_GUID).digest('base64');
  return { request: lines.join('\r\n') + '\r\n\r\n', expectedAccept };
}

server.on('upgrade', (req, client, head) => {
  const url = new URL(req.url || '/', 'http://localhost');
  if (url.pathname !== '/api/proxy') return reject(client, 404, 'Not Found');
  if (!isValidWsRequest(req)) return reject(client, 400, 'Bad WebSocket Request');

  const port = url.searchParams.get('port') || '443';
  const path = url.searchParams.get('path') || '/';
  if (!['80', '443'].includes(port)) return reject(client, 400, 'Invalid Upstream Port');
  if (!path.startsWith('/') || path.startsWith('//') || /[\r\n]/.test(path)) return reject(client, 400, 'Invalid Upstream Path');

  const upstream = port === '443'
    ? tls.connect({ host: UPSTREAM_HOST, port: 443, servername: TLS_SERVERNAME, rejectUnauthorized: true })
    : net.connect({ host: UPSTREAM_HOST, port: 80 });

  let settled = false;
  let responseBuffer = Buffer.alloc(0);
  let upstreamReady = false;
  const { request, expectedAccept } = makeHandshake(path, port, req.headers);

  const closeBoth = () => {
    if (!client.destroyed) client.destroy();
    if (!upstream.destroyed) upstream.destroy();
  };

  const fail = (status, message) => {
    if (settled) return;
    settled = true;
    reject(client, status, message);
    if (!upstream.destroyed) upstream.destroy();
  };

  upstream.setTimeout(30000);
  upstream.on(port === '443' ? 'secureConnect' : 'connect', () => {
    if (!settled) upstream.write(request);
  });

  upstream.on('data', (chunk) => {
    if (settled || upstreamReady) {
      if (upstreamReady && !client.destroyed) client.write(chunk);
      return;
    }
    responseBuffer = Buffer.concat([responseBuffer, chunk]);
    if (responseBuffer.length > 32768) return fail(502, 'Upstream Header Too Large');
    const end = responseBuffer.indexOf('\r\n\r\n');
    if (end === -1) return;

    const headerText = responseBuffer.subarray(0, end).toString('latin1');
    const statusLine = headerText.split('\r\n', 1)[0];
    const acceptLine = headerText.split('\r\n').find(line => /^sec-websocket-accept:/i.test(line));
    const receivedAccept = acceptLine ? acceptLine.split(':').slice(1).join(':').trim() : '';
    if (!/^HTTP\/1\.[01] 101\b/.test(statusLine) || receivedAccept !== expectedAccept) {
      console.error('[DROPNET] upstream recusou o WebSocket:', statusLine);
      return fail(502, 'Upstream WebSocket Rejected');
    }

    settled = true;
    upstreamReady = true;
    const clientKey = req.headers['sec-websocket-key'];
    const clientAccept = crypto.createHash('sha1').update(clientKey + WS_GUID).digest('base64');
    client.write(
      'HTTP/1.1 101 Switching Protocols\r\n' +
      'Upgrade: websocket\r\n' +
      'Connection: Upgrade\r\n' +
      `Sec-WebSocket-Accept: ${clientAccept}\r\n\r\n`
    );

    const remaining = responseBuffer.subarray(end + 4);
    responseBuffer = Buffer.alloc(0);
    if (head?.length) upstream.write(head);
    if (remaining.length) client.write(remaining);
    client.pipe(upstream);
    upstream.pipe(client);
  });

  upstream.on('timeout', () => fail(504, 'Upstream Timeout'));
  upstream.on('error', (err) => {
    console.error('[DROPNET] upstream error:', err.message);
    if (!settled) fail(502, 'Bad Gateway'); else closeBoth();
  });
  upstream.on('close', () => { if (upstreamReady) closeBoth(); else if (!settled) fail(502, 'Upstream Closed'); });
  client.on('error', closeBoth);
  client.on('close', closeBoth);
});

server.listen(PORT, '0.0.0.0', () => {
  console.log(`[DROPNET CDN] online na porta ${PORT}; upstream ${UPSTREAM_HOST}`);
});
