// sarvamSTT.js
// Sarvam Realtime Speech-to-Text WebSocket wrapper
// Endpoint: wss://api.sarvam.ai/speech-to-text-realtime/ws
// Sends raw PCM16 16kHz audio, receives transcript events.

const WebSocket = require('ws');

/**
 * Creates a Sarvam Realtime STT WebSocket connection.
 *
 * @param {string} apiKey         - Sarvam API key
 * @param {string} languageCode   - BCP-47 language code, e.g. 'hi-IN' or 'en-IN'
 * @param {function} onTranscript - Callback: (text: string, isFinal: boolean) => void
 * @returns {WebSocket}           - The connected WebSocket instance
 */
function createSarvamSTT(apiKey, languageCode, onTranscript) {
  const url = `wss://api.sarvam.ai/speech-to-text-realtime/ws?language_code=${encodeURIComponent(languageCode)}`;
  const ws = new WebSocket(url, {
    headers: {
      'api-subscription-key': apiKey,
    },
  });

  ws.on('open', () => {
    console.log('[STT] Connected to Sarvam STT');
    // Send initial config
    ws.send(JSON.stringify({
      type: 'config',
      data: {
        language_code: languageCode,
        model: 'saaras:v3-realtime',
        enable_partial_transcripts: true,
      },
    }));
  });

  ws.on('message', (raw) => {
    let msg;
    try {
      msg = JSON.parse(raw.toString());
    } catch {
      return;
    }

    if (msg.type === 'transcript' && msg.data?.transcript) {
      onTranscript(msg.data.transcript, msg.data.is_final ?? true);
    }
  });

  ws.on('error', (err) => {
    console.error('[STT] WebSocket error:', err.message);
  });

  ws.on('close', (code, reason) => {
    console.log(`[STT] Closed (code=${code}, reason=${reason})`);
  });

  return ws;
}

module.exports = { createSarvamSTT };
