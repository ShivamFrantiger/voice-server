// sarvamTTS.js
// Sarvam Streaming TTS WebSocket wrapper
// Endpoint: wss://api.sarvam.ai/text-to-speech/ws
// Sends text chunks, receives base64-encoded PCM audio chunks.

const WebSocket = require('ws');

/**
 * Creates a Sarvam Streaming TTS WebSocket connection.
 *
 * @param {string} apiKey         - Sarvam API key
 * @param {string} speaker        - Voice speaker, e.g. 'priya', 'neha', 'ritu'
 * @param {string} languageCode   - BCP-47 language code, e.g. 'hi-IN' or 'en-IN'
 * @param {function} onAudioChunk - Callback: (pcmBuffer: Buffer) => void
 * @returns {WebSocket}           - The connected WebSocket instance (with .synthesize helper)
 */
function createSarvamTTS(apiKey, speaker, languageCode, onAudioChunk) {
  const ws = new WebSocket('wss://api.sarvam.ai/text-to-speech/ws', {
    headers: {
      'api-subscription-key': apiKey,
    },
  });

  ws.on('open', () => {
    console.log('[TTS] Connected to Sarvam TTS');
    // Send initial config
    ws.send(JSON.stringify({
      type: 'config',
      data: {
        model: 'bulbul:v3',
        language_code: languageCode,
        speaker: speaker,
        pace: 1.0,
        pitch: 0,
        loudness: 1.5,
        output_audio_codec: 'mulaw',
        sample_rate: 8000,
        audio_format: 'mulaw'
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

    // Audio chunks arrive as base64-encoded PCM
    if (msg.type === 'audio' && msg.data?.audio) {
      const pcmBuffer = Buffer.from(msg.data.audio, 'base64');
      onAudioChunk(pcmBuffer);
    }
  });

  ws.on('error', (err) => {
    console.error('[TTS] WebSocket error:', err.message);
  });

  ws.on('close', (code, reason) => {
    console.log(`[TTS] Closed (code=${code}, reason=${reason})`);
  });

  /**
   * Send text to synthesize.
   * Appends a flush signal to trigger immediate audio generation.
   * @param {string} text
   */
  ws.synthesize = (text) => {
    if (ws.readyState !== WebSocket.OPEN) {
      console.warn('[TTS] Cannot synthesize — WebSocket not open');
      return;
    }
    ws.send(JSON.stringify({ type: 'text', data: { text } }));
    ws.send(JSON.stringify({ type: 'flush' }));
  };

  return ws;
}

module.exports = { createSarvamTTS };
