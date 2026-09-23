'use strict';

const { GoogleGenerativeAI } = require('@google/generative-ai');

// ─── Validate API Key ─────────────────────────────────────────────────────────
if (!process.env.GEMINI_API_KEY) {
  console.error('[Gemini] FATAL: GEMINI_API_KEY is not set in environment variables!');
  process.exit(1);
}

const genAI = new GoogleGenerativeAI(process.env.GEMINI_API_KEY);

// ─── System Instruction ───────────────────────────────────────────────────────
// Instructing the model to be concise — critical for the HUD overlay use case
// where long text would clutter the screen.
const SYSTEM_INSTRUCTION = `Bạn là một trợ lý AI phân tích hình ảnh/văn bản siêu nhanh tích hợp trên HUD (Head-Up Display).

QUY TẮC BẮT BUỘC:
1. Trả lời CỰC KỲ ngắn gọn. Tối đa 2-3 câu hoặc 1 đoạn bullet ngắn.
2. KHÔNG giải thích dài dòng, KHÔNG thêm lời chào/kết, KHÔNG lặp lại câu hỏi.
3. Ưu tiên thông tin quan trọng nhất, bỏ qua chi tiết thừa.
4. Nếu là văn bản: tóm tắt ý chính hoặc trả lời câu hỏi trực tiếp.
5. Nếu là mã code: giải thích chức năng trong 1 dòng, chỉ ra lỗi nếu có.
6. Dùng tiếng Việt nếu input là tiếng Việt, dùng tiếng Anh nếu input là tiếng Anh.`;

// ─── Model Factory ────────────────────────────────────────────────────────────
function getModel() {
  const modelName = process.env.GEMINI_MODEL || 'gemini-2.0-flash-exp';
  return genAI.getGenerativeModel({
    model: modelName,
    systemInstruction: SYSTEM_INSTRUCTION,
    generationConfig: {
      maxOutputTokens: 256,   // Hard cap — keep overlay text short
      temperature: 0.4,        // Slightly creative but mostly factual
      topP: 0.8,
    },
  });
}

// ─── Streaming Analysis ───────────────────────────────────────────────────────
/**
 * Streams Gemini response chunks to a WebSocket client in real-time.
 * Each chunk is sent as a JSON message as soon as it arrives.
 *
 * @param {string} inputText  - OCR-extracted text or raw text from the device
 * @param {string} userPrompt - Optional user instruction (e.g. "Dịch sang tiếng Việt")
 * @param {WebSocket} ws      - The WebSocket connection to stream chunks to
 * @param {string} requestId  - Unique ID for this request (for client-side tracking)
 */
async function streamAnalyze(inputText, userPrompt, ws, requestId) {
  const model = getModel();

  // Build the combined prompt
  const combinedPrompt = userPrompt
    ? `${userPrompt}\n\nNội dung:\n${inputText}`
    : `Phân tích nội dung sau:\n${inputText}`;

  console.log(`[Gemini] Starting stream for requestId=${requestId}, inputLength=${inputText.length}`);

  try {
    const result = await model.generateContentStream(combinedPrompt);

    // Send stream start signal
    safeSend(ws, { type: 'stream_start', requestId });

    let totalChunks = 0;
    for await (const chunk of result.stream) {
      const chunkText = chunk.text();
      if (chunkText) {
        totalChunks++;
        safeSend(ws, {
          type: 'stream_chunk',
          requestId,
          chunk: chunkText,
        });
      }
    }

    // Send stream end signal
    safeSend(ws, { type: 'stream_end', requestId });
    console.log(`[Gemini] Stream complete for requestId=${requestId}. Chunks sent: ${totalChunks}`);
  } catch (err) {
    console.error(`[Gemini] Stream error for requestId=${requestId}:`, err.message);
    safeSend(ws, {
      type: 'stream_error',
      requestId,
      error: err.message,
    });
  }
}

// ─── One-shot Analysis (REST fallback) ───────────────────────────────────────
/**
 * Returns a single complete response (no streaming).
 * Used by the REST POST /api/scan fallback.
 */
async function analyzeOnce(inputText, userPrompt) {
  const model = getModel();
  const combinedPrompt = userPrompt
    ? `${userPrompt}\n\nNội dung:\n${inputText}`
    : `Phân tích nội dung sau:\n${inputText}`;

  const result = await model.generateContent(combinedPrompt);
  return result.response.text();
}

// ─── Helper ───────────────────────────────────────────────────────────────────
/**
 * Safely send a JSON message to a WebSocket client.
 * Silently drops message if connection is not OPEN.
 */
function safeSend(ws, payload) {
  if (ws.readyState === ws.OPEN) {
    ws.send(JSON.stringify(payload));
  }
}

module.exports = { streamAnalyze, analyzeOnce };
