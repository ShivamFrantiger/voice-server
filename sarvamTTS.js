// sarvamTTS.js
// Sarvam Streaming TTS WebSocket wrapper
// Endpoint: wss://api.sarvam.ai/text-to-speech/ws
// Sends text chunks, receives base64-encoded PCM audio chunks.

const WebSocket = require("ws");

/**
 * Creates a Sarvam Streaming TTS WebSocket connection.
 *
 * @param {string} apiKey         - Sarvam API key
 * @param {string} speaker        - Voice speaker, e.g. 'priya', 'neha', 'ritu' or 'svc-...'
 * @param {string} languageCode   - BCP-47 language code, e.g. 'hi-IN' or 'en-IN'
 * @param {function} onAudioChunk - Callback: (pcmBuffer: Buffer) => void
 * @returns {WebSocket}           - The connected WebSocket instance (with .synthesize helper)
 */
function createSarvamTTS(apiKey, speaker, languageCode, onAudioChunk) {
  const isClonedVoice = speaker && speaker.startsWith("svc-");

  if (isClonedVoice) {
    console.log(`[TTS] Using REST API fallback for cloned voice: ${speaker}`);

    // Mock the WebSocket interface expected by server.js
    const mockWs = {
      readyState: 1, // WebSocket.OPEN equivalent
      close: () => {},
      synthesize: async (text) => {
        if (!text || !text.trim()) return;
        try {
          const fd = new FormData();
          fd.append("text", text);
          fd.append("language_code", languageCode || "hi-IN");
          fd.append("voice_id", speaker);

          const response = await fetch("https://api.sarvam.ai/voices/clone", {
            method: "POST",
            headers: {
              "api-subscription-key": apiKey,
            },
            body: fd,
          });

          const data = await response.json();
          if (data.error) {
            console.error("[TTS REST Error]", data.error);
            return;
          }

          const base64Audio =
            (data.audios && data.audios[0]) || data.audio || data.audio_b64;
          if (base64Audio) {
            const rawBuffer = Buffer.from(base64Audio, "base64");
            onAudioChunk(rawBuffer);
          } else {
            console.error("[TTS REST Error] No audio found in response:", data);
          }
        } catch (err) {
          console.error("[TTS REST Exception]", err.message);
        }
      },
    };
    return mockWs;
  }

  const ws = new WebSocket("wss://api.sarvam.ai/text-to-speech/ws", {
    headers: {
      "api-subscription-key": apiKey,
    },
  });

  ws.on("open", () => {
    console.log("[TTS] Connected to Sarvam TTS");

    // Send initial config
    ws.send(
      JSON.stringify({
        type: "config",
        data: {
          model: "bulbul:v3",
          language_code: languageCode,
          speaker: speaker,
          pace: 1.0,
          pitch: 0,
          loudness: 1.5,
          output_audio_codec: "linear16",
        },
      }),
    );
  });

  ws.on("message", (raw) => {
    let msg;
    try {
      msg = JSON.parse(raw.toString());
    } catch {
      return;
    }

    // Audio chunks arrive as base64-encoded PCM
    if (msg.type === "audio" && msg.data?.audio) {
      const pcmBuffer = Buffer.from(msg.data.audio, "base64");
      onAudioChunk(pcmBuffer);
    }
  });

  ws.on("error", (err) => {
    console.error("[TTS] WebSocket error:", err.message);
  });

  ws.on("close", (code, reason) => {
    console.log(`[TTS] Closed (code=${code}, reason=${reason})`);
  });

  /**
   * Send text to synthesize.
   * Appends a flush signal to trigger immediate audio generation.
   * @param {string} text
   */
  ws.synthesize = (text) => {
    if (ws.readyState !== WebSocket.OPEN) {
      console.warn("[TTS] Cannot synthesize — WebSocket not open");
      return;
    }
    ws.send(JSON.stringify({ type: "text", data: { text } }));
    ws.send(JSON.stringify({ type: "flush" }));
  };

  return ws;
}

module.exports = { createSarvamTTS };
