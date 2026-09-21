// VLESS over WebSocket relay for Render (Node.js 20+)
// Listens on the port provided by Render (default 10000), upgrades
// WebSocket connections and forwards VLESS payloads to real TCP targets.

import http from 'node:http';
import net from 'node:net';
import { WebSocketServer } from 'ws';

const PORT = process.env.PORT || 10000;
const UUID = (process.env.UUID || '54fe82c6-a5db-4250-8be8-1bbb2ba9533f').toLowerCase();
const UUID_HEX = UUID.replace(/-/g, '');

function uuidMatches(buf, offset) {
  for (let i = 0; i < 16; i++) {
    if (buf[offset + i] !== parseInt(UUID_HEX.substr(i * 2, 2), 16)) return false;
  }
  return true;
}

const server = http.createServer((req, res) => {
  // Lightweight health endpoint (also used to keep the service awake)
  if (req.url === '/' || req.url === '/health') {
    res.writeHead(200, { 'content-type': 'application/json' });
    res.end(JSON.stringify({ ok: true, service: 'vless-ws', ts: Date.now() }));
    return;
  }
  res.writeHead(404, { 'content-type': 'text/plain' });
  res.end('Not Found');
});

const wss = new WebSocketServer({ server });

wss.on('connection', (ws) => {
  let remote = null;
  let handshakeDone = false;

  const closeAll = () => {
    try { if (remote && !remote.destroyed) remote.destroy(); } catch (_) {}
    try { ws.close(); } catch (_) {}
  };

  ws.on('message', (data) => {
    const buf = Buffer.isBuffer(data) ? data : Buffer.from(data);

    if (!handshakeDone) {
      try {
        if (buf.length < 24) { closeAll(); return; }
        const version = buf[0];

        // --- UUID check (bytes 1..16) ---
        if (!uuidMatches(buf, 1)) { closeAll(); return; }

        // --- parse VLESS request header ---
        const optLen = buf[17];
        let p = 18 + optLen + 1; // skip: addons + command byte
        const port = buf.readUInt16BE(p);
        p += 2;
        const atyp = buf[p];
        p += 1;

        let host = '';
        if (atyp === 1) {
          host = `${buf[p]}.${buf[p + 1]}.${buf[p + 2]}.${buf[p + 3]}`;
          p += 4;
        } else if (atyp === 2) {
          const dlen = buf[p];
          p += 1;
          host = buf.slice(p, p + dlen).toString('utf8');
          p += dlen;
        } else if (atyp === 3) {
          const parts = [];
          for (let j = 0; j < 8; j++) parts.push(buf.readUInt16BE(p + j * 2).toString(16));
          host = parts.join(':');
          p += 16;
        } else {
          closeAll(); return;
        }

        handshakeDone = true;
        const initial = buf.slice(p);

        // --- dial the target ---
        remote = net.connect({ host, port }, () => {
          // VLESS response: version + status 0
          try { ws.send(Buffer.from([version, 0])); } catch (_) {}
          if (initial.length > 0) remote.write(initial);
        });

        remote.on('data', (chunk) => {
          try { ws.send(chunk); } catch (_) { closeAll(); }
        });
        remote.on('error', () => closeAll());
        remote.on('close', () => closeAll());
      } catch (_) {
        closeAll();
      }
      return;
    }

    // --- steady state: forward client -> remote ---
    try {
      if (remote && !remote.destroyed) remote.write(buf);
    } catch (_) { closeAll(); }
  });

  ws.on('close', () => { try { if (remote && !remote.destroyed) remote.destroy(); } catch (_) {} });
  ws.on('error', () => { try { if (remote && !remote.destroyed) remote.destroy(); } catch (_) {} });
});

server.listen(PORT, '0.0.0.0', () => {
  console.log(`vless-ws relay listening on 0.0.0.0:${PORT}`);
});
