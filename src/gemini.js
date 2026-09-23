'use strict';

const { GoogleGenerativeAI } = require('@google/generative-ai');

// ─── Validate API Key ─────────────────────────────────────────────────────────
if (!process.env.GEMINI_API_KEY) {
  console.error('[Gemini] FATAL: GEMINI_API_KEY is not set in environment variables!');
  process.exit(1);
}

const genAI = new GoogleGenerativeAI(process.env.GEMINI_API_KEY);

// ─── System Instruction ───────────────────────────────────────────────────────
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

// Candidate models for automatic failover when 429 quota is reached
const CANDIDATE_MODELS = [
  process.env.GEMINI_MODEL || 'gemini-3.5-flash-lite',
  'gemini-3.6-flash',
];

// ─── Model Factory ────────────────────────────────────────────────────────────
function getModel(modelName) {
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

// ─── Streaming Analysis with Multi-Model Quota Failover ────────────────────────
/**
 * Streams Gemini response chunks to a WebSocket client in real-time.
 * Automatically fails over to alternate models if 429 Quota Exceeded occurs.
 */
async function streamAnalyze(inputText, userPrompt, ws, requestId) {
  const combinedPrompt = userPrompt
    ? `${userPrompt}\n\nNội dung:\n${inputText}`
    : `Phân tích nội dung sau:\n${inputText}`;

  console.log(`[Gemini] Starting stream for requestId=${requestId}, inputLength=${inputText.length}`);

  let lastError = null;

  for (const modelName of CANDIDATE_MODELS) {
    try {
      console.log(`[Gemini] Attempting with model: ${modelName} for requestId=${requestId}`);
      const model = getModel(modelName);
      const result = await model.generateContentStream(combinedPrompt);

      // Send stream start signal
      safeSend(ws, { type: 'stream_start', requestId, model: modelName });

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
      console.log(`[Gemini] Stream complete with ${modelName} for requestId=${requestId}. Chunks sent: ${totalChunks}`);
      return; // Successful stream, stop trying other models
    } catch (err) {
      console.error(`[Gemini] Model ${modelName} failed for requestId=${requestId}:`, err.message);
      lastError = err;

      // If quota exceeded (429), failover immediately to next model
      if (err.message && (err.message.includes('429') || err.message.includes('quota') || err.message.includes('Quota'))) {
        console.log(`[Gemini] 429 Quota exceeded on ${modelName}, failing over to alternate model...`);
        continue;
      }
      break;
    }
  }

  // All candidate models failed
  safeSend(ws, {
    type: 'stream_error',
    requestId,
    error: lastError ? lastError.message : 'Tất cả model đều quá tải hạn ngạch.',
  });
}

// ─── One-shot Analysis (REST fallback) ───────────────────────────────────────
async function analyzeOnce(inputText, userPrompt) {
  const combinedPrompt = userPrompt
    ? `${userPrompt}\n\nNội dung:\n${inputText}`
    : `Phân tích nội dung sau:\n${inputText}`;

  for (const modelName of CANDIDATE_MODELS) {
    try {
      const model = getModel(modelName);
      const result = await model.generateContent(combinedPrompt);
      return result.response.text();
    } catch (err) {
      if (err.message && (err.message.includes('429') || err.message.includes('quota'))) {
        continue;
      }
      throw err;
    }
  }
}

// ─── Helper ───────────────────────────────────────────────────────────────────
function safeSend(ws, payload) {
  if (ws.readyState === ws.OPEN) {
    ws.send(JSON.stringify(payload));
  }
}

module.exports = { streamAnalyze, analyzeOnce, CANDIDATE_MODELS };
