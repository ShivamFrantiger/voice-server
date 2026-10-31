// server.js — Voice Modulation Server (Inverted Flow)
// I call Plivo → server dials the client dynamically → my voice is modulated → client hears fake voice
// Client speaks → raw audio passed through → I hear real client voice
//
// Flow:
//   MY Phone ──inbound──► /stream   ──► ElevenLabs S2S ──► modulated ulaw ──► ClientWS
//   ClientWS  ──inbound──► /client-stream ──► raw passthrough ──► MY Phone WS

require("dotenv").config();

// ─── Process-level guards ─────────────────────────────────────────────────────
process.on("uncaughtException", (err) => {
  if (
    err.code === "ECONNRESET" ||
    err.message?.includes("wsarecv") ||
    err.message?.includes("stream reading error") ||
    err.message?.includes("aborted")
  ) {
    console.warn("[Process] Suppressed network abort:", err.message);
  } else {
    console.error("[Process] Uncaught exception:", err);
  }
});

process.on("unhandledRejection", (reason) => {
  const msg = reason?.message || String(reason);
  if (
    msg.includes("wsarecv") ||
    msg.includes("stream reading error") ||
    msg.includes("ECONNRESET") ||
    msg.includes("aborted")
  ) {
    console.warn("[Process] Suppressed network abort (rejection):", msg);
  } else {
    console.error("[Process] Unhandled rejection:", reason);
  }
});

const express = require("express");
const http = require("http");
const { WebSocketServer } = require("ws");

const { dialClient, hangupCall } = require("./plivoClient");

// ─── Voice Modulation Engines Configuration ──────────────────────────────────
// Both engines loaded simultaneously so calls can switch dynamically at runtime!
const { createSarvamSTT } = require("./sarvamSTT");
const { createSarvamTTS } = require("./sarvamTTS");
const { ElevenLabsS2S } = require("./elevenLabsS2S");
const {
  mulawToPcm16k,
  pcm24kToMulaw,
  applyMulawNoiseGate,
  computeMulawRms,
} = require("./audioUtils");
const { fillerManager } = require("./fillerManager");

const PORT = process.env.PORT || 8080;
const MY_NUMBER = process.env.MY_PHONE_NUMBER; // MY hardcoded number (trusted caller)

// Sarvam Config
const SARVAM_KEY = process.env.SARVAM_API_KEY;
const SARVAM_SPEAKER =
  process.env.SARVAM_VOICE_ID || process.env.SARVAM_FEMALE_SPEAKER || "priya";
const SARVAM_LANG = process.env.SARVAM_LANGUAGE_CODE || "hi-IN";

// ElevenLabs Config & Low-Latency Tuning
const ELEVEN_KEY = process.env.ELEVEN_LABS_API;
const ELEVEN_VOICE = process.env.ELEVEN_LABS_VOICE_ID;
const ELEVEN_MODEL =
  process.env.ELEVEN_LABS_MODEL || "eleven_multilingual_sts_v2";
const SILENCE_MS = parseInt(process.env.ELEVEN_LABS_SILENCE_MS || "260", 10);
const SPEECH_THRESH = parseInt(
  process.env.ELEVEN_LABS_SPEECH_THRESHOLD || "100",
  10,
);
const NOISE_GATE = parseInt(
  process.env.ELEVEN_LABS_NOISE_GATE_RANGE || "0",
  10,
);
const MAX_SPEECH_MS = parseInt(
  process.env.ELEVEN_LABS_MAX_SPEECH_MS || "3500",
  10,
);
const MIN_SPEECH_MS = parseInt(
  process.env.ELEVEN_LABS_MIN_SPEECH_MS || "120",
  10,
);
const LATENCY_LEVEL = parseInt(
  process.env.ELEVEN_LABS_LATENCY_LEVEL || "3",
  10,
);
const SIMILARITY_BOOST = parseFloat(
  process.env.ELEVEN_LABS_SIMILARITY_BOOST || "0.80",
);
const STABILITY = parseFloat(process.env.ELEVEN_LABS_STABILITY || "0.70");
const PREBUFFER_MS = parseInt(
  process.env.AUDIO_STREAMER_PREBUFFER_MS || "160",
  10,
);

if (!SARVAM_KEY) console.warn("[Config] SARVAM_API_KEY not set");
if (!ELEVEN_KEY) console.warn("[Config] ELEVEN_LABS_API not set");
if (!ELEVEN_VOICE) console.warn("[Config] ELEVEN_LABS_VOICE_ID not set");
if (!MY_NUMBER)
  console.warn("[Config] MY_PHONE_NUMBER not set — caller validation disabled");

// Global default engine (can be changed on command via POST /api/engine)
let currentDefaultEngine = (
  process.env.DEFAULT_ENGINE || "elevenlabs"
).toLowerCase();
if (
  currentDefaultEngine !== "sarvam" &&
  currentDefaultEngine !== "elevenlabs"
) {
  currentDefaultEngine = "elevenlabs";
}
console.log(`[Config] Default Modulation Engine: ${currentDefaultEngine}`);

// ─── In-memory stores ─────────────────────────────────────────────────────────

/**
 * pendingCalls: keyed by my phone number (normalised) → target client number.
 * Set via POST /api/prepare-call from the VoiceModulator web UI.
 * Cleared once the call is initiated or after TTL.
 */
const pendingCalls = new Map();
const PENDING_TTL_MS = 5 * 60 * 1000; // 5 minutes TTL

/**
 * sessions: keyed by callUUID (sessionId).
 * { myWs, clientWs, s2s, streamer, clientNumber }
 */
const sessions = new Map();

// ─── Audio Streamer (Adaptive Jitter Buffer) ─────────────────────────────────
//
// Phase 2 improvements:
//   • Pre-buffer 80ms of audio before starting playback (absorbs jitter bursts)
//   • Larger 320-byte (40ms) chunks = fewer WebSocket sends = less overhead
//   • Comfort noise on buffer underrun (prevents audible "dead air" gaps)
//   • Buffer-depth monitoring to detect backpressure early
//
class AudioStreamer {
  constructor(ws, sessionId, label, options = {}) {
    this.ws = ws;
    this.sessionId = sessionId;
    this.label = label || "audio";
    this.queue = Buffer.alloc(0);
    this.timer = null;
    this.chunkSize = 320; // 40ms @ 8kHz μ-law (320 bytes)
    this.interval = 40; // 40ms send cadence
    const preMs = options.preBufferMs || PREBUFFER_MS || 160;
    // 160ms @ 8kHz μ-law = 1280 bytes
    this.preBuffer = Math.max(320, Math.round((preMs / 1000) * 8000));
    this._isStreaming = false;
    this._idleCount = 0;
  }

  addAudio(mulawBuffer) {
    if (!mulawBuffer || mulawBuffer.length === 0) return;
    this.queue = Buffer.concat([this.queue, mulawBuffer]);

    // Start streaming once pre-buffer threshold is reached
    if (!this._isStreaming && this.queue.length >= this.preBuffer) {
      this._startStreaming();
    }
  }

  _startStreaming() {
    if (this._isStreaming) return;
    this._isStreaming = true;
    this._idleCount = 0;

    if (this.timer) clearInterval(this.timer);
    this.timer = setInterval(() => {
      if (this.queue.length >= this.chunkSize) {
        // Send exactly one 40ms frame (320 bytes)
        const chunk = this.queue.subarray(0, this.chunkSize);
        this.queue = this.queue.subarray(this.chunkSize);
        this._sendChunk(chunk);
        this._idleCount = 0;
      } else if (this.queue.length > 0) {
        // Partial chunk in buffer (< 320 bytes).
        // CRITICAL FIX: DO NOT wipe or pad with silence while streaming!
        // Keep partial bytes so next incoming audio chunk appends seamlessly.
        this._idleCount++;
        // If no new audio arrives for 200ms (5 ticks), the utterance has finished
        if (this._idleCount >= 5) {
          const padded = Buffer.alloc(this.chunkSize, 255); // 255 = standard μ-law silence
          this.queue.copy(padded);
          this.queue = Buffer.alloc(0);
          this._sendChunk(padded);
          this._stopStreaming();
        }
      } else {
        // Buffer is completely empty (0 bytes)
        this._idleCount++;
        // If queue remains empty for 3 ticks (120ms), sleep until next audio arrives
        if (this._idleCount >= 3) {
          this._stopStreaming();
        }
      }
    }, this.interval);
  }

  _sendChunk(chunk) {
    if (this.ws && this.ws.readyState === 1) {
      this.ws.send(
        JSON.stringify({
          event: "playAudio",
          media: {
            contentType: "audio/x-mulaw",
            sampleRate: 8000,
            payload: chunk.toString("base64"),
          },
        }),
      );
    }
  }

  _stopStreaming() {
    if (this.timer) clearInterval(this.timer);
    this.timer = null;
    this._isStreaming = false;
    this._idleCount = 0;
  }

  stop() {
    this._stopStreaming();
    this.queue = Buffer.alloc(0);
  }
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

function normaliseNumber(num) {
  return (num || "").replace(/[\s\-()]/g, "").replace(/^\+/, "");
}

function numbersMatch(a, b) {
  const normA = normaliseNumber(a);
  const normB = normaliseNumber(b);
  if (!normA || !normB) return false;
  if (normA === normB) return true;
  // Match last 10 digits if both are at least 10 digits
  if (normA.length >= 10 && normB.length >= 10) {
    return normA.slice(-10) === normB.slice(-10);
  }
  return false;
}

function isTrustedCaller(fromNumber, pendingEntry) {
  if (pendingEntry) return true;
  if (!MY_NUMBER) return true;
  if (!fromNumber) return false;
  return numbersMatch(fromNumber, MY_NUMBER);
}

function getPendingCall(fromNumber) {
  const now = Date.now();
  for (const [key, val] of pendingCalls.entries()) {
    if (now - val.createdAt > PENDING_TTL_MS) pendingCalls.delete(key);
  }

  // 1. Try matching with fromNumber
  if (fromNumber) {
    const normFrom = normaliseNumber(fromNumber);
    if (pendingCalls.has(normFrom)) {
      const entry = pendingCalls.get(normFrom);
      pendingCalls.delete(normFrom);
      return entry;
    }
    for (const [key, val] of pendingCalls.entries()) {
      if (numbersMatch(key, normFrom)) {
        pendingCalls.delete(key);
        return val;
      }
    }
  }

  // 2. Try matching with configured MY_NUMBER
  const myNum = normaliseNumber(MY_NUMBER || "agent");
  if (pendingCalls.has(myNum)) {
    const entry = pendingCalls.get(myNum);
    pendingCalls.delete(myNum);
    return entry;
  }
  for (const [key, val] of pendingCalls.entries()) {
    if (numbersMatch(key, myNum)) {
      pendingCalls.delete(key);
      return val;
    }
  }

  // 3. Fallback: If only 1 pending call exists and it's fresh
  if (pendingCalls.size === 1) {
    const [key, val] = pendingCalls.entries().next().value;
    pendingCalls.delete(key);
    return val;
  }

  return null;
}

// ─── App Setup ───────────────────────────────────────────────────────────────

const app = express();
const server = http.createServer(app);

const myWss = new WebSocketServer({ noServer: true });
const clientWss = new WebSocketServer({ noServer: true });

server.on("upgrade", (req, socket, head) => {
  const pathname = new URL(req.url, "http://localhost").pathname;
  if (pathname === "/stream") {
    myWss.handleUpgrade(req, socket, head, (ws) =>
      myWss.emit("connection", ws, req),
    );
  } else if (pathname === "/client-stream") {
    clientWss.handleUpgrade(req, socket, head, (ws) =>
      clientWss.emit("connection", ws, req),
    );
  } else {
    socket.destroy();
  }
});

app.use(express.json());
app.use(express.urlencoded({ extended: true }));

// ─── HTTP Routes ─────────────────────────────────────────────────────────────

app.get("/", (req, res) => {
  res.json({
    status: "ok",
    version: "2.0.0",
    defaultEngine: currentDefaultEngine,
    availableEngines: ["elevenlabs", "sarvam"],
    voices: {
      elevenlabs: ELEVEN_VOICE || "not configured",
      sarvam: `${SARVAM_SPEAKER} (${SARVAM_LANG})`,
    },
    message: `Voice modulation server is running with ${currentDefaultEngine} as default`,
    pendingCalls: pendingCalls.size,
    activeCalls: sessions.size,
  });
});

/**
 * GET /api/engine
 * Returns current default engine, available engines, and config status.
 */
app.get("/api/engine", (req, res) => {
  res.json({
    currentEngine: currentDefaultEngine,
    availableEngines: ["elevenlabs", "sarvam"],
    config: {
      elevenlabs: {
        voiceId: ELEVEN_VOICE || null,
        model: ELEVEN_MODEL,
        silenceMs: SILENCE_MS,
        speechThreshold: SPEECH_THRESH,
        maxSpeechMs: MAX_SPEECH_MS,
        minSpeechMs: MIN_SPEECH_MS,
        latencyLevel: LATENCY_LEVEL,
        preBufferMs: PREBUFFER_MS,
        isConfigured: Boolean(ELEVEN_KEY && ELEVEN_VOICE),
      },
      sarvam: {
        speaker: SARVAM_SPEAKER,
        language: SARVAM_LANG,
        isConfigured: Boolean(SARVAM_KEY),
      },
    },
  });
});

/**
 * POST /api/engine
 * Switch default modulation engine on command!
 * Accepts { "engine": "elevenlabs" | "sarvam" } or ?engine=...
 */
app.post("/api/engine", (req, res) => {
  const reqEngine = (req.body?.engine || req.query?.engine || "")
    .trim()
    .toLowerCase();
  if (reqEngine !== "elevenlabs" && reqEngine !== "sarvam") {
    return res.status(400).json({
      error: "Invalid engine. Must be 'elevenlabs' or 'sarvam'.",
      currentEngine: currentDefaultEngine,
      availableEngines: ["elevenlabs", "sarvam"],
    });
  }

  const prev = currentDefaultEngine;
  currentDefaultEngine = reqEngine;
  console.log(
    `[Engine] Default modulation engine switched: ${prev} ➔ ${currentDefaultEngine}`,
  );

  return res.json({
    success: true,
    message: `Default modulation engine switched to ${currentDefaultEngine}`,
    previousEngine: prev,
    currentEngine: currentDefaultEngine,
  });
});

/**
 * POST /api/prepare-call
 * Called by VoiceModulator web UI before I dial the Plivo number.
 * Body: { clientNumber: "+91XXXXXXXXXX", callerNumber: "+91YYYYYYYYYY", engine?: "elevenlabs" | "sarvam" }
 */
app.post("/api/prepare-call", (req, res) => {
  const { clientNumber, callerNumber, engine } = req.body;

  if (!clientNumber || clientNumber.trim().length < 5) {
    return res.status(400).json({ error: "clientNumber is required" });
  }

  const effectiveCaller = callerNumber ? callerNumber.trim() : MY_NUMBER || "";
  if (!effectiveCaller) {
    return res.status(400).json({ error: "callerNumber is required" });
  }

  const normCaller = normaliseNumber(effectiveCaller);
  const now = Date.now();

  for (const [key, val] of pendingCalls.entries()) {
    if (now - val.createdAt > PENDING_TTL_MS) pendingCalls.delete(key);
  }

  // Determine engine: requested engine or fallback to server default
  const selectedEngine =
    engine &&
    (engine.toLowerCase() === "sarvam" || engine.toLowerCase() === "elevenlabs")
      ? engine.toLowerCase()
      : currentDefaultEngine;

  pendingCalls.set(normCaller, {
    clientNumber: clientNumber.trim(),
    callerNumber: effectiveCaller,
    engine: selectedEngine,
    createdAt: now,
  });
  console.log(
    `[PrepareCall] Registered caller ${normCaller} (${effectiveCaller}) ➔ client ${clientNumber.trim()} | Engine: ${selectedEngine}`,
  );

  return res.json({
    success: true,
    message: `Ready. Engine: ${selectedEngine}. Now dial the Plivo number from your phone.`,
    clientNumber: clientNumber.trim(),
    callerNumber: effectiveCaller,
    engine: selectedEngine,
    expiresInMs: PENDING_TTL_MS,
  });
});

/** GET /api/prepare-call/status */
app.get("/api/prepare-call/status", (req, res) => {
  const reqCaller = req.query.callerNumber;
  const callerKey = reqCaller
    ? normaliseNumber(reqCaller)
    : normaliseNumber(MY_NUMBER || "");

  let entry = callerKey ? pendingCalls.get(callerKey) : null;
  if (!entry && pendingCalls.size > 0) {
    entry = pendingCalls.values().next().value;
  }
  const active = sessions.size;

  if (!entry || Date.now() - entry.createdAt > PENDING_TTL_MS) {
    return res.json({
      pending: false,
      activeCalls: active,
      currentEngine: currentDefaultEngine,
    });
  }

  return res.json({
    pending: true,
    clientNumber: entry.clientNumber,
    callerNumber: entry.callerNumber,
    engine: entry.engine || currentDefaultEngine,
    activeCalls: active,
    expiresInMs: PENDING_TTL_MS - (Date.now() - entry.createdAt),
  });
});

/** POST /api/prepare-call/cancel & POST /api/end-call */
const terminateActiveCalls = () => {
  pendingCalls.clear();
  let endedCount = 0;
  for (const [sessionId, session] of sessions.entries()) {
    endedCount++;
    if (session.myCallUuid) hangupCall(session.myCallUuid);
    if (session.clientCallUuid) hangupCall(session.clientCallUuid);

    try {
      if (session.myWs && session.myWs.readyState === 1) session.myWs.close();
    } catch {}
    try {
      if (session.clientWs && session.clientWs.readyState === 1)
        session.clientWs.close();
    } catch {}

    cleanupSession(sessionId);
  }
  return endedCount;
};

app.post("/api/end-call", (req, res) => {
  console.log(
    "[EndCall] Terminating all active call legs and pending registrations...",
  );
  const endedCount = terminateActiveCalls();
  return res.json({
    success: true,
    message: `Ended ${endedCount} active calls`,
    endedCount,
  });
});

app.post("/api/prepare-call/cancel", (req, res) => {
  console.log("[CancelCall] Terminating pending calls and active call legs...");
  const endedCount = terminateActiveCalls();
  return res.json({ success: true, endedCount });
});

/**
 * GET/POST /api/plivo/answer
 * Direct Plivo answer URL fallback on voice-server.
 * Returns XML pointing to /stream WS with caller query params.
 */
app.all("/api/plivo/answer", (req, res) => {
  const serverUrl = process.env.SERVER_URL || `http://localhost:${PORT}`;
  let wsUrl = serverUrl.replace(/^https?/, "wss") + "/stream";

  const fromNumber =
    req.body?.From ||
    req.body?.from ||
    req.query?.From ||
    req.query?.from ||
    "";
  const callUUID =
    req.body?.CallUUID ||
    req.body?.callUUID ||
    req.query?.CallUUID ||
    req.query?.callUUID ||
    "";
  const toNumber =
    req.body?.To || req.body?.to || req.query?.To || req.query?.to || "";

  try {
    const urlObj = new URL(wsUrl);
    if (fromNumber) urlObj.searchParams.set("from", fromNumber);
    if (callUUID) urlObj.searchParams.set("callUUID", callUUID);
    if (toNumber) urlObj.searchParams.set("to", toNumber);
    wsUrl = urlObj.toString();
  } catch (_) {
    if (fromNumber)
      wsUrl +=
        (wsUrl.includes("?") ? "&" : "?") +
        `from=${encodeURIComponent(fromNumber)}`;
  }

  const escapedWsUrl = wsUrl.replace(/&/g, "&amp;");
  const extraHeadersAttr = fromNumber
    ? ` extraHeaders="from:${fromNumber}"`
    : "";

  const xml = `<?xml version="1.0" encoding="UTF-8"?>
<Response>
  <Stream streamTimeout="86400" keepCallAlive="true" bidirectional="true" contentType="audio/x-mulaw;rate=8000"${extraHeadersAttr}>
    ${escapedWsUrl}
  </Stream>
</Response>`;

  res.set("Content-Type", "text/xml");
  res.send(xml);
});

/**
 * POST /api/plivo/client-answer
 * Plivo hits this when the client picks up the outbound call.
 * Returns XML pointing to /client-stream WS.
 */
app.post("/api/plivo/client-answer", (req, res) => {
  const serverUrl = process.env.SERVER_URL || `http://localhost:${PORT}`;
  let wsUrl = serverUrl.replace(/^https?/, "wss") + "/client-stream";

  if (req.query.sessionId) {
    wsUrl += `?sessionId=${req.query.sessionId}`;
  }

  const xml = `<?xml version="1.0" encoding="UTF-8"?>
<Response>
  <Stream streamTimeout="86400" keepCallAlive="true" bidirectional="true" contentType="audio/x-mulaw;rate=8000">
    ${wsUrl}
  </Stream>
</Response>`;

  res.set("Content-Type", "text/xml");
  res.send(xml);
});

// ─── MY Phone WebSocket Handler (/stream) ─────────────────────────────────────
// I dial the Plivo number → Plivo opens this WS for MY audio.
// MY inbound audio → ElevenLabs S2S → modulated ulaw → client phone
// Client audio (via /client-stream) → raw passthrough → MY phone (this WS)

myWss.on("connection", (myWs, req) => {
  console.log("\n[MyWS] ─── New call from MY phone ─────────────────────");

  let queryFrom = "";
  let queryCallUUID = "";

  if (req && req.url) {
    try {
      const parsedUrl = new URL(req.url, "http://localhost");
      queryFrom = parsedUrl.searchParams.get("from") || "";
      queryCallUUID = parsedUrl.searchParams.get("callUUID") || "";
    } catch (_) {}
  }

  let callUUID = queryCallUUID || null;
  let sessionId = null;

  myWs.on("message", async (raw) => {
    let msg;
    try {
      msg = JSON.parse(raw.toString());
    } catch {
      return;
    }

    switch (msg.event) {
      case "start": {
        callUUID =
          msg.start?.callId ||
          msg.start?.callUUID ||
          msg.start?.call_uuid ||
          queryCallUUID ||
          "unknown";
        sessionId = callUUID;

        let fromNumber =
          queryFrom || msg.start?.from || msg.start?.callerName || "";

        // Check extra_headers from Plivo start event if available
        if (!fromNumber && msg.start?.extra_headers) {
          try {
            if (typeof msg.start.extra_headers === "object") {
              fromNumber = msg.start.extra_headers.from || "";
            } else if (typeof msg.start.extra_headers === "string") {
              const match = msg.start.extra_headers.match(/from:([^;]+)/i);
              if (match) fromNumber = match[1].trim();
            }
          } catch (_) {}
        }

        console.log(
          `[MyWS] Call started | UUID: ${callUUID} | from: ${fromNumber || "(not provided in stream)"}`,
        );
        console.log(`[MyWS] Metadata:`, JSON.stringify(msg.start, null, 2));

        const pending = getPendingCall(fromNumber);

        if (!pending) {
          console.warn(
            "[MyWS] No pending client number registered — hanging up",
          );
          myWs.close();
          return;
        }

        if (fromNumber && !isTrustedCaller(fromNumber, pending)) {
          console.warn(`[MyWS] Untrusted caller "${fromNumber}" — rejecting`);
          myWs.close();
          return;
        }

        if (fromNumber) {
          console.log(`[MyWS] Trusted caller verified: ${fromNumber}`);
        } else {
          console.log(
            `[MyWS] Using pending registration for caller ${pending.callerNumber || MY_NUMBER}`,
          );
        }

        const clientNumber = pending.clientNumber;
        const callEngine = (
          pending.engine ||
          currentDefaultEngine ||
          "elevenlabs"
        ).toLowerCase();

        sessions.set(sessionId, {
          myWs,
          clientWs: null,
          engine: callEngine,
          sttWs: null,
          ttsWs: null,
          s2s: null,
          streamer: new AudioStreamer(null, sessionId, "client", {
            preBufferMs: PREBUFFER_MS,
          }),
          clientNumber,
          myCallUuid: callUUID,
          clientCallUuid: null,
        });

        console.log(
          `[MyWS] Session ${sessionId} initialized | Engine: ${callEngine} | Will dial client: ${clientNumber}`,
        );

        try {
          const resp = await dialClient(sessionId, clientNumber);
          const currentSession = sessions.get(sessionId);
          if (currentSession && resp) {
            currentSession.clientCallUuid =
              resp.requestUuid || resp.callUuid || null;
          }
        } catch (err) {
          console.error("[Plivo] Failed to dial client:", err.message);
          cleanupSession(sessionId);
        }
        break;
      }

      case "media": {
        const { track, payload } = msg.media || {};
        if (!payload) break;

        // MY voice (inbound from my phone) → feed to modulation pipeline
        if (track === "inbound") {
          const session = sessions.get(sessionId);
          if (!session) break;

          if (session.engine === "sarvam") {
            // === SARVAM STT FLOW ===
            if (session.sttWs && session.sttWs.readyState === 1) {
              try {
                const mulawBuf = Buffer.from(payload, "base64");
                const rms = computeMulawRms(mulawBuf);
                if (rms > SPEECH_THRESH && session.streamer) {
                  fillerManager.onAgentSpeechStart(sessionId, session.streamer);
                }
                const pcmBuf = mulawToPcm16k(mulawBuf);
                session.sttWs.sendAudio(pcmBuf);
                if (Math.random() < 0.05) {
                  console.log(
                    `[MyWS→STT] Audio frame forwarded to Sarvam STT (session: ${sessionId})`,
                  );
                }
              } catch (err) {
                console.error("[MyWS→STT] Conversion/Send error:", err.message);
              }
            }
          } else {
            // === ELEVENLABS S2S FLOW ===
            if (session.s2s) {
              try {
                let mulawBuf = Buffer.from(payload, "base64");
                // Noise gate only applied if NOISE_GATE > 0 (disabled by default for pristine audio)
                if (NOISE_GATE > 0) {
                  mulawBuf = applyMulawNoiseGate(mulawBuf, NOISE_GATE);
                }
                session.s2s.addAudio(mulawBuf);
                if (Math.random() < 0.02) {
                  console.log(
                    `[MyWS→S2S] Audio frame → ElevenLabs WS (session: ${sessionId})`,
                  );
                }
              } catch (err) {
                console.error("[MyWS→S2S] Error:", err.message);
              }
            }
          }
        }
        break;
      }

      case "stop": {
        console.log(`[MyWS] Call stopped | UUID: ${callUUID}`);
        cleanupSession(sessionId);
        break;
      }
    }
  });

  myWs.on("close", (code) => {
    console.log(`[MyWS] Connection closed (code=${code}) | UUID: ${callUUID}`);
    cleanupSession(sessionId);
  });

  myWs.on("error", (err) => console.error("[MyWS] Error:", err.message));
});

// ─── Client WebSocket Handler (/client-stream) ────────────────────────────────
// Handles the client's phone audio AFTER I dial them.
// Client inbound audio → raw passthrough → MY phone (unmodified, I hear real client voice)

clientWss.on("connection", (clientWs, req) => {
  console.log("[ClientWS] ─── Client leg connected ──────────────────");

  const url = new URL(req.url, "http://localhost");
  const sessionId = url.searchParams.get("sessionId");
  const session = sessions.get(sessionId);

  if (!session) {
    console.warn(
      `[ClientWS] No active session found for ${sessionId} — closing`,
    );
    clientWs.close();
    return;
  }

  session.clientWs = clientWs;
  session.streamer.ws = clientWs;
  console.log(
    `[ClientWS] Linked to session ${sessionId} | client: ${session.clientNumber} | engine: ${session.engine}`,
  );

  if (session.engine === "sarvam") {
    // === INITIALIZE SARVAM (STT + TTS) ========================================
    console.log(
      `[ClientWS] Initializing Sarvam STT + TTS for session ${sessionId}...`,
    );

    // 1. Initialise Sarvam TTS — outputs modulated speech to client
    const ttsWs = createSarvamTTS(
      SARVAM_KEY,
      SARVAM_SPEAKER,
      SARVAM_LANG,
      (pcmBuffer) => {
        const cur = sessions.get(sessionId);
        if (!cur || !cur.clientWs || cur.clientWs.readyState !== 1) return;

        fillerManager.onModulatedChunkReceived(sessionId);

        if (cur.streamer && cur.streamer.ws !== cur.clientWs) {
          cur.streamer.ws = cur.clientWs;
        }

        try {
          // Resample Sarvam 24kHz linear16 PCM to 8kHz mu-law
          const mulawBuf = pcm24kToMulaw(pcmBuffer);
          cur.streamer.addAudio(mulawBuf);

          if (Math.random() < 0.05) {
            console.log(
              `[TTS→Client] Buffered Sarvam modulated audio (session: ${sessionId})`,
            );
          }
        } catch (err) {
          console.error("[TTS→Client] Audio downsampling error:", err.message);
        }
      },
    );
    session.ttsWs = ttsWs;

    // 2. Initialise Sarvam Realtime STT — transcribes MY speech and feeds into TTS
    const sttWs = createSarvamSTT(
      SARVAM_KEY,
      SARVAM_LANG,
      (transcript, isFinal) => {
        if (isFinal && transcript && transcript.trim().length > 0) {
          console.log(`[STT→TTS] Final transcript recognized: "${transcript}"`);
          const cur = sessions.get(sessionId);
          if (cur?.ttsWs) {
            cur.ttsWs.synthesize(transcript);
          } else {
            console.warn(
              "[STT→TTS] TTS not ready, dropping transcript:",
              transcript,
            );
          }
        }
      },
    );
    session.sttWs = sttWs;
  } else {
    // === INITIALIZE ELEVENLABS (S2S) ==========================================
    console.log(
      `[ClientWS] Initializing ElevenLabs S2S for session ${sessionId}...`,
    );

    const s2s = new ElevenLabsS2S(
      ELEVEN_KEY,
      ELEVEN_VOICE,
      (ulawChunk) => {
        const cur = sessions.get(sessionId);
        if (!cur || !cur.clientWs || cur.clientWs.readyState !== 1) return;

        fillerManager.onModulatedChunkReceived(sessionId);

        if (cur.streamer && cur.streamer.ws !== cur.clientWs) {
          cur.streamer.ws = cur.clientWs;
        }
        cur.streamer.addAudio(ulawChunk);

        if (Math.random() < 0.02) {
          console.log(
            `[S2S→Client] Received modulated chunk → AudioStreamer (session: ${sessionId})`,
          );
        }
      },
      {
        silenceDurationMs: SILENCE_MS,
        speechThreshold: SPEECH_THRESH,
        maxSpeechMs: MAX_SPEECH_MS,
        minSpeechMs: MIN_SPEECH_MS,
        latencyLevel: LATENCY_LEVEL,
        similarityBoost: SIMILARITY_BOOST,
        stability: STABILITY,
        removeBackgroundNoise: process.env.ELEVEN_LABS_REMOVE_NOISE === "true",
        modelId: ELEVEN_MODEL,
        onSpeechStart: () => {
          const cur = sessions.get(sessionId);
          if (cur?.streamer) {
            fillerManager.onAgentSpeechStart(sessionId, cur.streamer);
          }
        },
      },
    );
    session.s2s = s2s;
  }

  clientWs.on("message", (raw) => {
    let msg;
    try {
      msg = JSON.parse(raw.toString());
    } catch {
      return;
    }

    if (msg.event === "start") {
      const clientUuid =
        msg.start?.callId || msg.start?.callUUID || msg.start?.call_uuid;
      if (clientUuid && session) {
        session.clientCallUuid = clientUuid;
      }
      console.log(
        "[ClientWS] Client call started | UUID:",
        clientUuid || "unknown",
      );
    } else if (msg.event === "media") {
      const { track, payload } = msg.media || {};
      if (!payload) return;

      // Client inbound audio → raw passthrough → MY phone (I hear client's real voice)
      // Detect client speech to notify fillerManager and reset conversational turn
      if (track === "inbound") {
        const curSession = sessions.get(sessionId);
        if (curSession) {
          try {
            const clientMulaw = Buffer.from(payload, "base64");
            const clientRms = computeMulawRms(clientMulaw);
            if (clientRms > 80) {
              fillerManager.onClientSpeech(sessionId);
            }
          } catch (_) {}
        }
      }
      if (track === "inbound") {
        const cur = sessions.get(sessionId);
        if (cur?.myWs && cur.myWs.readyState === 1) {
          cur.myWs.send(
            JSON.stringify({
              event: "playAudio",
              media: {
                contentType: "audio/x-mulaw",
                sampleRate: 8000,
                payload,
              },
            }),
          );
          if (Math.random() < 0.05)
            console.log(
              `[Router] Client audio → MyWS raw passthrough (session: ${sessionId})`,
            );
        }
      }
    }
  });

  clientWs.on("close", (code) => {
    console.log(`[ClientWS] Closed (code=${code}) | session: ${sessionId}`);
    cleanupSession(sessionId);
    const cur = sessions.get(sessionId);
    if (cur) cur.clientWs = null;
  });

  clientWs.on("error", (err) =>
    console.error("[ClientWS] Error:", err.message),
  );
});

// ─── Cleanup Helpers ──────────────────────────────────────────────────────────

function cleanupSession(sessionId) {
  if (sessionId) fillerManager.cleanup(sessionId);
  if (!sessionId) return;
  const session = sessions.get(sessionId);
  if (session) {
    try {
      session.s2s?.destroy();
    } catch {}
    try {
      session.sttWs?.close();
    } catch {}
    try {
      session.ttsWs?.close();
    } catch {}
    try {
      session.streamer?.stop();
    } catch {}
    session.s2s = null;
    session.sttWs = null;
    session.ttsWs = null;
    session.clientWs = null;
    session.myWs = null;
  }
  sessions.delete(sessionId);
  console.log(`[Cleanup] Session ${sessionId} removed`);
}

// ─── Start ───────────────────────────────────────────────────────────────────

server.listen(PORT, () => {
  fillerManager.init();
  console.log("");
  console.log(
    "╔══════════════════════════════════════════════════════════════╗",
  );
  console.log(
    "║     Voice Modulation Server (Dynamic Dual Engine)            ║",
  );
  console.log(
    `║   HTTP : http://localhost:${PORT}                              ║`,
  );
  console.log(
    `║   WS   : ws://localhost:${PORT}/stream                         ║`,
  );
  console.log(
    `║   WS   : ws://localhost:${PORT}/client-stream                  ║`,
  );
  console.log(`║   Default Engine   : ${currentDefaultEngine.padEnd(39)} ║`);
  console.log(
    `║   ElevenLabs Voice : ${(ELEVEN_VOICE?.slice(0, 24) || "not set").padEnd(39)} ║`,
  );
  console.log(
    `║   Sarvam Speaker   : ${(SARVAM_SPEAKER?.slice(0, 24) || "not set").padEnd(39)} ║`,
  );
  console.log(
    `║   My #             : ${(MY_NUMBER || "NOT SET").padEnd(39)} ║`,
  );
  console.log(
    `║   Guruji Fillers   : ${(fillerManager.enabled ? `Active (thresh=${fillerManager.thresholdMs}ms, count=${fillerManager.fillerKeys.length})` : "Disabled").padEnd(39)} ║`,
  );
  console.log(
    "╚══════════════════════════════════════════════════════════════╝",
  );
  console.log("");
});
