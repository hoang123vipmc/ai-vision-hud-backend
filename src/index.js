'use strict';

require('dotenv').config();

const http = require('http');
const express = require('express');
const cors = require('cors');
const WebSocket = require('ws');
const { handleWebSocketConnection } = require('./routes/scan');

// ─── Config ───────────────────────────────────────────────────────────────────
const PORT = process.env.PORT || 3000;
const app = express();

// ─── Middleware ───────────────────────────────────────────────────────────────
app.use(cors());
app.use(express.json({ limit: '10mb' }));

// ─── REST Endpoints ───────────────────────────────────────────────────────────

/**
 * Health check endpoint — used by Render to verify the service is alive.
 * Also used as a simple ping from the Android client.
 */
app.get('/api/health', (req, res) => {
  res.json({
    status: 'ok',
    model: process.env.GEMINI_MODEL || 'gemini-3.6-flash',
    timestamp: new Date().toISOString(),
  });
});

/**
 * REST fallback for /api/scan (non-streaming).
 * Primarily the app uses WebSocket, but this is kept for debugging via Postman.
 */
app.post('/api/scan', async (req, res) => {
  const { text, prompt } = req.body;
  if (!text && !prompt) {
    return res.status(400).json({ error: 'Missing text or prompt in request body.' });
  }

  try {
    const { analyzeOnce } = require('./gemini');
    const result = await analyzeOnce(text || '', prompt || '');
    res.json({ result });
  } catch (err) {
    console.error('[REST /api/scan] Error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// 404 catch-all
app.use((req, res) => {
  res.status(404).json({ error: 'Not found' });
});

// ─── HTTP + WebSocket Server ──────────────────────────────────────────────────
const server = http.createServer(app);

// Attach WebSocket server to the same HTTP server (no extra port needed)
const wss = new WebSocket.Server({ server, path: '/ws' });

wss.on('connection', (ws, req) => {
  const clientIp = req.socket.remoteAddress;
  console.log(`[WS] Client connected from ${clientIp}. Total clients: ${wss.clients.size}`);
  handleWebSocketConnection(ws);
});

wss.on('error', (err) => {
  console.error('[WS Server] Error:', err.message);
});

server.listen(PORT, () => {
  console.log(`[Server] AI Vision HUD Backend running on port ${PORT}`);
  console.log(`[Server] WebSocket endpoint: ws://localhost:${PORT}/ws`);
  console.log(`[Server] Health check: http://localhost:${PORT}/api/health`);
  console.log(`[Server] Gemini model: ${process.env.GEMINI_MODEL || 'gemini-3.6-flash'}`);
});

// Graceful shutdown
process.on('SIGTERM', () => {
  console.log('[Server] SIGTERM received. Closing...');
  wss.clients.forEach((client) => client.close());
  server.close(() => process.exit(0));
});
