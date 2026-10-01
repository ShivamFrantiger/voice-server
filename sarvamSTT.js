// sarvamSTT.js
// Sarvam Realtime Speech-to-Text WebSocket wrapper
// Endpoint: wss://api.sarvam.ai/speech-to-text-realtime/ws
//
// Audio input:  JSON  { event: "audio_input", audio: "<base64 PCM16 16kHz>" }
// Responses:    JSON  { event: "transcript.partial" | "transcript.final", transcript: "..." }
//               OR    { type: "data", data: { transcript: "..." } }  (legacy format — handled too)

const WebSocket = require('ws');

/**
 * Creates a Sarvam Realtime STT WebSocket connection.
 *
 * @param {string} apiKey         - Sarvam API key
 * @param {string} languageCode   - BCP-47 language code, e.g. 'hi-IN' or 'en-IN'
 * @param {function} onTranscript - Callback: (text: string, isFinal: boolean) => void
 * @returns {WebSocket}           - The connected WebSocket instance
 *                                  Has a .sendAudio(pcmBuffer) helper method.
 */
function createSarvamSTT(apiKey, languageCode, onTranscript) {
  // Pass model + language as query params so no config message is needed
  const url = `wss://api.sarvam.ai/speech-to-text-realtime/ws` +
    `?language_code=${encodeURIComponent(languageCode)}` +
    `&model=saaras:v3-realtime`;

  const ws = new WebSocket(url, {
    headers: {
      'api-subscription-key': apiKey,
    },
  });

  ws.on('open', () => {
    console.log('[STT] Connected to Sarvam STT WebSocket');
  });

  ws.on('message', (raw) => {
    // ── Raw dump — log EVERYTHING so we can see the actual server format ──────
    const text = raw.toString();
    console.log('[STT] RAW message from Sarvam:', text.slice(0, 300));

    let msg;
    try {
      msg = JSON.parse(text);
    } catch {
      console.warn('[STT] Non-JSON message received:', text.slice(0, 100));
      return;
    }

    // ── Handle all known Sarvam response shapes ────────────────────────────────
    //
    // Shape A (v3-realtime event style):
    //   { event: "transcript.final",   transcript: "text" }
    //   { event: "transcript.partial", transcript: "text" }
    //
    // Shape B (legacy / saaras:v3 style):
    //   { type: "transcript", data: { transcript: "text", is_final: true } }
    //
    // Shape C (observed in some builds):
    //   { type: "data", data: { transcript: "text" } }

    // Shape A
    if (msg.event && msg.transcript !== undefined) {
      const isFinal = msg.event === 'transcript.final';
      const partial = msg.event === 'transcript.partial';
      if ((isFinal || partial) && msg.transcript.trim().length > 0) {
        console.log(`[STT] ${isFinal ? 'FINAL' : 'partial'} (shape A): "${msg.transcript}"`);
        onTranscript(msg.transcript, isFinal);
      }
      return;
    }

    // Shape B — { type: "transcript", data: { transcript, is_final } }
    if (msg.type === 'transcript' && msg.data?.transcript) {
      const isFinal = msg.data.is_final ?? true;
      console.log(`[STT] ${isFinal ? 'FINAL' : 'partial'} (shape B): "${msg.data.transcript}"`);
      onTranscript(msg.data.transcript, isFinal);
      return;
    }

    // Shape C — { type: "data", data: { transcript } }
    if (msg.type === 'data' && msg.data?.transcript) {
      console.log(`[STT] FINAL (shape C): "${msg.data.transcript}"`);
      onTranscript(msg.data.transcript, true);
      return;
    }

    // Anything else — already dumped above via RAW log
  });

  ws.on('error', (err) => {
    console.error('[STT] WebSocket error:', err.message);
  });

  ws.on('close', (code, reason) => {
    console.log(`[STT] Closed (code=${code}, reason=${reason.toString()})`);
  });

  /**
   * Send a PCM16 @ 16kHz Buffer as a base64-encoded JSON audio_input event.
   * This is what Sarvam STT's saaras:v3-realtime endpoint expects.
   *
   * @param {Buffer} pcmBuffer
   */
  ws.sendAudio = (pcmBuffer) => {
    if (ws.readyState !== WebSocket.OPEN) return;
    ws.send(JSON.stringify({
      event: 'audio_input',
      audio: pcmBuffer.toString('base64'),
    }));
  };

  return ws;
}

module.exports = { createSarvamSTT };
