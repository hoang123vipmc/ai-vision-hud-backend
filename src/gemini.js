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
const SYSTEM_INSTRUCTION = `Bạn là trợ lý AI siêu nhanh trên HUD (Head-Up Display) điện thoại.
MỤC TIÊU: Phân tích tức thì, đưa ra câu trả lời trực tiếp trong 1-2 giây.

QUY TẮC CỐT LÕI:
1. ĐỐI VỚI CÂU HỎI TRẮC NGHIỆM / BÀI TẬP:
   - In NGAY DÒNG ĐẦU TIÊN: 👉 Đáp án: [A/B/C/D hoặc câu trả lời cụ thể]
   - Thêm 1 câu giải thích ngắn gọn, súc tích bên dưới.
2. ĐỐI VỚI ĐOẠN VĂN BẢN / TIN TỨC:
   - Tóm tắt tối đa 2 gạch đầu dòng ý chính quan trọng nhất.
3. ĐỐI VỚI CODE / BÁO LỖI:
   - Dòng 1: Nguyên nhân lỗi.
   - Dòng 2: Cách sửa nhanh.
4. ĐỐI VỚI DỊCH THUẬT / TỪ VỰNG:
   - Dịch nghĩa trực tiếp + phiên âm nếu có.
5. TUYỆT ĐỐI KHÔNG: Chào hỏi, lặp lại đề bài, viết rườm rà.`;

// ─── Model Factory ────────────────────────────────────────────────────────────
function getModel() {
  const modelName = process.env.GEMINI_MODEL || 'gemini-3.6-flash';
  return genAI.getGenerativeModel({
    model: modelName,
    systemInstruction: SYSTEM_INSTRUCTION,
    generationConfig: {
      maxOutputTokens: 150,    // Punchy short response — lightning fast
      temperature: 0.2,        // Highly focused and deterministic
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
