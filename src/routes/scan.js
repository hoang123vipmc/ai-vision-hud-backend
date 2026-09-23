'use strict';

const { streamAnalyze } = require('../gemini');

// ─── WebSocket Message Protocol ───────────────────────────────────────────────
//
// Client → Server (Android sends):
//   { type: "scan", requestId: "uuid", payload: { text: "...", prompt: "..." } }
//   { type: "ping" }
//
// Server → Client (Backend sends):
//   { type: "stream_start",  requestId: "uuid" }
//   { type: "stream_chunk",  requestId: "uuid", chunk: "..." }
//   { type: "stream_end",    requestId: "uuid" }
//   { type: "stream_error",  requestId: "uuid", error: "..." }
//   { type: "pong" }
//   { type: "error",         message: "..." }

/**
 * Handles a single WebSocket client connection lifecycle.
 * Called once per connected client.
 *
 * @param {WebSocket} ws
 */
function handleWebSocketConnection(ws) {
  let isProcessing = false; // Prevent concurrent requests from same client

  ws.on('message', async (rawMessage) => {
    let message;

    // ── Parse JSON ───────────────────────────────────────────────────────────
    try {
      message = JSON.parse(rawMessage.toString());
    } catch (_) {
      console.warn('[WS] Received non-JSON message, ignoring.');
      safeSend(ws, { type: 'error', message: 'Invalid JSON format.' });
      return;
    }

    const { type, requestId, payload } = message;
    console.log(`[WS] Message received: type=${type}, requestId=${requestId}`);

    // ── Ping / Pong (keep-alive) ─────────────────────────────────────────────
    if (type === 'ping') {
      safeSend(ws, { type: 'pong' });
      return;
    }

    // ── Scan Request ─────────────────────────────────────────────────────────
    if (type === 'scan') {
      if (!payload || !payload.text) {
        safeSend(ws, {
          type: 'error',
          requestId,
          message: 'Missing payload.text in scan request.',
        });
        return;
      }

      // Throttle: only one concurrent request per client connection
      if (isProcessing) {
        console.log(`[WS] Client sent new scan while previous is still processing. Skipping.`);
        safeSend(ws, {
          type: 'error',
          requestId,
          message: 'Previous request still processing. Please wait.',
        });
        return;
      }

      const { text, prompt } = payload;

      // Basic validation
      if (text.trim().length === 0) {
        safeSend(ws, {
          type: 'stream_start', requestId,
        });
        safeSend(ws, {
          type: 'stream_chunk', requestId,
          chunk: '(Không phát hiện văn bản trong khung hình)',
        });
        safeSend(ws, { type: 'stream_end', requestId });
        return;
      }

      isProcessing = true;
      try {
        await streamAnalyze(text, prompt || '', ws, requestId || 'unknown');
      } finally {
        isProcessing = false;
      }
      return;
    }

    // ── Unknown message type ─────────────────────────────────────────────────
    console.warn(`[WS] Unknown message type: ${type}`);
    safeSend(ws, { type: 'error', message: `Unknown message type: ${type}` });
  });

  ws.on('close', (code, reason) => {
    console.log(`[WS] Client disconnected. Code: ${code}, Reason: ${reason.toString()}`);
  });

  ws.on('error', (err) => {
    console.error('[WS] Client socket error:', err.message);
  });

  // Send welcome message so client can verify connection
  safeSend(ws, {
    type: 'connected',
    message: 'AI Vision HUD Backend ready.',
    model: process.env.GEMINI_MODEL || 'gemini-3.5-flash-lite',
  });
}

// ─── Helper ───────────────────────────────────────────────────────────────────
function safeSend(ws, payload) {
  if (ws.readyState === ws.OPEN) {
    ws.send(JSON.stringify(payload));
  }
}

module.exports = { handleWebSocketConnection };
