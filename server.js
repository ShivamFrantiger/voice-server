// server.js — Voice Modulation Server
// Bridges Plivo WebSocket streaming with Sarvam STT + TTS
// to convert the agent's voice to female before sending to the customer.

require('dotenv').config();

const express    = require('express');
const http       = require('http');
const { WebSocketServer } = require('ws');

const { dialAgent }       = require('./plivoClient');
const { createSarvamSTT } = require('./sarvamSTT');
const { createSarvamTTS } = require('./sarvamTTS');
const { mulawToPcm16k, pcm16kToMulaw } = require('./audioUtils');

// ─── Config ──────────────────────────────────────────────────────────────────

const PORT          = process.env.PORT || 8080;
const SARVAM_KEY    = process.env.SARVAM_API_KEY;
const SPEAKER       = process.env.SARVAM_FEMALE_SPEAKER || 'priya';
const LANG          = process.env.SARVAM_LANGUAGE_CODE  || 'hi-IN';

// ─── App Setup ───────────────────────────────────────────────────────────────

const app    = express();
const server = http.createServer(app);
const wss    = new WebSocketServer({ server, path: '/stream' });

app.use(express.json());
app.use(express.urlencoded({ extended: true }));

// Health check endpoint
app.get('/', (req, res) => {
  res.json({ status: 'ok', message: 'Voice modulation server is running' });
});

// ─── WebSocket Handler ───────────────────────────────────────────────────────

// Track active sessions: callSid → { sttWs, ttsWs }
const sessions = new Map();

wss.on('connection', (plivoWs, req) => {
  console.log('\n[WS] ─── New Plivo connection ─────────────────────');

  let callUUID  = null;
  let sessionId = null;

  // ── Create Sarvam STT ──
  // When a transcript arrives from Sarvam STT, pass it to TTS
  const sttWs = createSarvamSTT(SARVAM_KEY, LANG, (transcript, isFinal) => {
    console.log(`[STT] ${isFinal ? 'FINAL' : 'partial'}: "${transcript}"`);

    if (isFinal && transcript.trim().length > 0) {
      const session = sessions.get(sessionId);
      if (session?.ttsWs) {
        session.ttsWs.synthesize(transcript);
      }
    }
  });

  // ── Create Sarvam TTS ──
  // When female audio arrives from Sarvam TTS, encode to μ-law and send to Plivo
  const ttsWs = createSarvamTTS(SARVAM_KEY, SPEAKER, LANG, (pcmBuffer) => {
    if (plivoWs.readyState !== 1 /* OPEN */) return;

    try {
      const mulawBuf = pcm16kToMulaw(pcmBuffer);
      const playback = JSON.stringify({
        event: 'playAudio',
        media: {
          contentType: 'audio/x-mulaw',
          sampleRate:  8000,
          payload:     mulawBuf.toString('base64'),
        },
      });
      plivoWs.send(playback);
    } catch (err) {
      console.error('[TTS→Plivo] Error encoding/sending audio:', err.message);
    }
  });

  // ── Handle Plivo Events ──
  plivoWs.on('message', async (raw) => {
    let msg;
    try {
      msg = JSON.parse(raw.toString());
    } catch {
      return;
    }

    switch (msg.event) {

      // ── start: call connected to stream ──────────────────────────────────
      case 'start': {
        callUUID  = msg.start?.callUUID  || msg.start?.call_uuid || 'unknown';
        sessionId = callUUID;

        sessions.set(sessionId, { sttWs, ttsWs });
        console.log(`[WS] Call started | UUID: ${callUUID}`);
        console.log(`[WS] Metadata:`, JSON.stringify(msg.start, null, 2));

        // Dial the agent's phone so they can join the call
        try {
          await dialAgent();
        } catch (err) {
          console.error('[Plivo] Failed to dial agent:', err.message);
        }
        break;
      }

      // ── media: audio chunk received ───────────────────────────────────────
      case 'media': {
        const track   = msg.media?.track;
        const payload = msg.media?.payload;

        if (!payload) break;

        // 'outbound' = audio flowing FROM your phone TO the stream
        // (i.e. the agent's voice — this is what we convert to female)
        if (track === 'outbound') {
          const mulawBuf = Buffer.from(payload, 'base64');
          const pcmBuf   = mulawToPcm16k(mulawBuf);

          if (sttWs.readyState === 1 /* OPEN */) {
            sttWs.send(pcmBuf);
          }
        }

        // 'inbound' = audio from the customer — Plivo routes this to
        // the agent's phone automatically; nothing to do here.
        break;
      }

      // ── stop: call ended ──────────────────────────────────────────────────
      case 'stop': {
        console.log(`[WS] Call stopped | UUID: ${callUUID}`);
        cleanup(sessionId, sttWs, ttsWs);
        break;
      }

      default:
        // Ignore unknown events (dtmf, mark, etc.)
        break;
    }
  });

  plivoWs.on('close', (code) => {
    console.log(`[WS] Connection closed (code=${code}) | UUID: ${callUUID}`);
    cleanup(sessionId, sttWs, ttsWs);
  });

  plivoWs.on('error', (err) => {
    console.error('[WS] Plivo WebSocket error:', err.message);
  });
});

// ─── Cleanup Helper ───────────────────────────────────────────────────────────

function cleanup(sessionId, sttWs, ttsWs) {
  if (sessionId) sessions.delete(sessionId);
  try { sttWs?.close(); } catch {}
  try { ttsWs?.close(); } catch {}
}

// ─── Start Server ─────────────────────────────────────────────────────────────

server.listen(PORT, () => {
  console.log('');
  console.log('╔══════════════════════════════════════════╗');
  console.log('║       Voice Modulation Server            ║');
  console.log(`║   HTTP : http://localhost:${PORT}          ║`);
  console.log(`║   WS   : ws://localhost:${PORT}/stream     ║`);
  console.log(`║   Voice: ${SPEAKER} (${LANG})          ║`);
  console.log('╚══════════════════════════════════════════╝');
  console.log('');
});
