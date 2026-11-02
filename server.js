const http = require('http');
const fs = require('fs');
const path = require('path');
const { Server } = require('socket.io');
const { TikTokLiveConnection, WebcastEvent, ControlEvent } = require('tiktok-live-connector');

const PORT = process.env.PORT || 3000;
const DEFAULT_USERNAME = process.env.TIKTOK_USERNAME || 'bodondau';
const RECONNECT_DELAY_MS = 5000;
const MAX_RECONNECTS = 5;

// Serve the test client
const server = http.createServer((req, res) => {
  fs.readFile(path.join(__dirname, 'public', 'index.html'), (err, data) => {
    if (err) {
      res.writeHead(500);
      return res.end('Cannot load client');
    }
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
    res.end(data);
  });
});

const io = new Server(server, { cors: { origin: '*' } });

let connection = null;
let currentUsername = null;
let reconnects = 0;
let reconnectTimer = null;
let streamEnded = false;

function emitStatus(status, extra = {}) {
  io.emit('status', { status, username: currentUsername, ...extra });
  console.log(`[status] ${status}`, extra.message || '');
}

function teardown() {
  clearTimeout(reconnectTimer);
  if (connection) {
    connection.removeAllListeners();
    try { connection.disconnect(); } catch (_) { /* already closed */ }
    connection = null;
  }
}

function scheduleReconnect() {
  if (streamEnded || !currentUsername) return;
  if (reconnects >= MAX_RECONNECTS) {
    emitStatus('failed', { message: `Gave up after ${MAX_RECONNECTS} reconnect attempts` });
    return;
  }
  reconnects++;
  emitStatus('reconnecting', { attempt: reconnects, inMs: RECONNECT_DELAY_MS });
  reconnectTimer = setTimeout(() => connectTo(currentUsername, true), RECONNECT_DELAY_MS);
}

async function connectTo(username, isRetry = false) {
  teardown();
  currentUsername = username;
  streamEnded = false;
  if (!isRetry) reconnects = 0;

  const conn = new TikTokLiveConnection(username, {});
  connection = conn;

  // Forward every event to all socket clients
  const who = (d) => d?.user?.nickname || d?.id || 'unknown';
  conn.on(WebcastEvent.CHAT, (d) => {
    console.log(`[chat] ${who(d)}: ${d?.content}`);
    io.emit('chat', d);
  });
  conn.on(WebcastEvent.GIFT, (d) => {
    console.log(`[gift] ${who(d)} ${d.gift.describe} --> type: ${d?.gift.type} x${d?.repeatCount ?? 1}`);
    io.emit('gift', d);
  });
  conn.on(WebcastEvent.MEMBER, (d) => {
    console.log(`[member] ${who(d)} joined`);
    io.emit('member', d);
  });
  conn.on(WebcastEvent.LIKE, (d) => {
    console.log(`[like] ${who(d)} liked`);
    io.emit('like', d);
  });

  conn.on(WebcastEvent.STREAM_END, () => {
    streamEnded = true;
    emitStatus('stream_ended', { message: 'Livestream has ended' });
    teardown();
  });

  conn.on(ControlEvent.DISCONNECTED, () => {
    if (connection !== conn) return;
    emitStatus('disconnected', { message: 'Connection lost' });
    scheduleReconnect();
  });

  conn.on(ControlEvent.ERROR, (err) => {
    console.error('[tiktok error]', err);
    io.emit('error_event', { message: String(err && err.message ? err.message : err) });
  });

  emitStatus('connecting');
  try {
    const state = await conn.connect();
    reconnects = 0;
    emitStatus('connected', { roomId: state.roomId });
  } catch (err) {
    if (connection !== conn) return;
    // Typically: user not live, user not found, or blocked
    emitStatus('connect_failed', { message: String(err && err.message ? err.message : err) });
    scheduleReconnect();
  }
}

io.on('connection', (socket) => {
  console.log('[client] connected', socket.id);
  socket.emit('status', {
    status: connection ? 'connected' : 'idle',
    username: currentUsername,
  });

  socket.conn.on('upgrade', (transport) => {
    console.log(`[socket] ${socket.id} transport upgraded to ${transport.name}`);
  });

  socket.on('start', (username) => {
    console.log(`[socket] ${socket.id} -> start`, username);
    const name = String(username || '').trim().replace(/^@/, '');
    if (!name) return socket.emit('error_event', { message: 'Username is required' });
    connectTo(name);
  });

  socket.on('stop', () => {
    console.log(`[socket] ${socket.id} -> stop`);
    streamEnded = true; // suppress auto-reconnect
    teardown();
    emitStatus('idle');
  });

  socket.on('error', (err) => {
    console.error(`[socket] ${socket.id} error:`, err && err.message ? err.message : err);
  });

  socket.on('disconnect', (reason) => {
    console.log(`[socket] disconnected id=${socket.id} reason=${reason} total=${io.engine.clientsCount}`);
  });
});

io.engine.on('connection_error', (err) => {
  console.error(`[socket] connection_error code=${err.code} message=${err.message}`);
});

process.on('unhandledRejection', (e) => console.error('[unhandledRejection]', e));
process.on('SIGINT', () => { teardown(); process.exit(0); });

server.listen(PORT, () => {
  console.log(`Server running at http://localhost:${PORT}`);
  if (DEFAULT_USERNAME) connectTo(DEFAULT_USERNAME);
});
