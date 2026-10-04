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
const http    = require("http");
const { WebSocketServer } = require("ws");

const { dialClient, hangupCall } = require("./plivoClient");
const { ElevenLabsS2S } = require("./elevenLabsS2S");

// ─── Config ──────────────────────────────────────────────────────────────────

const PORT          = process.env.PORT || 8080;
const ELEVEN_KEY    = process.env.ELEVEN_LABS_API;
const ELEVEN_VOICE  = process.env.ELEVEN_LABS_VOICE_ID;
const ELEVEN_MODEL  = process.env.ELEVEN_LABS_MODEL || "eleven_multilingual_sts_v2";
const SILENCE_MS    = parseInt(process.env.ELEVEN_LABS_SILENCE_MS    || "300", 10);
const SPEECH_THRESH = parseInt(process.env.ELEVEN_LABS_SPEECH_THRESHOLD || "200", 10);
const MY_NUMBER     = process.env.MY_PHONE_NUMBER; // MY hardcoded number (trusted caller)

if (!ELEVEN_KEY)   console.warn("[Config] ELEVEN_LABS_API not set");
if (!ELEVEN_VOICE) console.warn("[Config] ELEVEN_LABS_VOICE_ID not set");
if (!MY_NUMBER)    console.warn("[Config] MY_PHONE_NUMBER not set — caller validation disabled");

// ─── In-memory stores ─────────────────────────────────────────────────────────

/**
 * pendingCalls: keyed by my phone number (normalised) → target client number.
 * Set via POST /api/prepare-call from the VoiceModulator web UI.
 * Cleared once the call is initiated or after TTL.
 */
const pendingCalls   = new Map();
const PENDING_TTL_MS = 5 * 60 * 1000; // 5 minutes TTL

/**
 * sessions: keyed by callUUID (sessionId).
 * { myWs, clientWs, s2s, streamer, clientNumber }
 */
const sessions = new Map();

// ─── Audio Streamer (Jitter Buffer) ──────────────────────────────────────────
class AudioStreamer {
  constructor(ws, sessionId, label) {
    this.ws        = ws;
    this.sessionId = sessionId;
    this.label     = label || "audio";
    this.queue     = Buffer.alloc(0);
    this.timer     = null;
    this.chunkSize = 160;
    this.interval  = 20;
  }

  addAudio(mulawBuffer) {
    this.queue = Buffer.concat([this.queue, mulawBuffer]);
    if (!this.timer) this.startStreaming();
  }

  startStreaming() {
    this.timer = setInterval(() => {
      if (this.queue.length >= this.chunkSize) {
        const chunk = this.queue.subarray(0, this.chunkSize);
        this.queue  = this.queue.subarray(this.chunkSize);
        this.sendChunk(chunk);
      } else if (this.queue.length === 0) {
        clearInterval(this.timer);
        this.timer = null;
      }
    }, this.interval);
  }

  sendChunk(chunk) {
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

  stop() {
    if (this.timer) clearInterval(this.timer);
    this.timer = null;
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

const app    = express();
const server = http.createServer(app);

const myWss     = new WebSocketServer({ noServer: true });
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
    status:       "ok",
    version:      "1.2.0",
    message:      "Voice modulation server (inverted flow) is running",
    pendingCalls: pendingCalls.size,
    activeCalls:  sessions.size,
  });
});

/**
 * POST /api/prepare-call
 * Called by VoiceModulator web UI before I dial the Plivo number.
 * Body: { clientNumber: "+91XXXXXXXXXX", callerNumber: "+91YYYYYYYYYY" }
 */
app.post("/api/prepare-call", (req, res) => {
  const { clientNumber, callerNumber } = req.body;

  if (!clientNumber || clientNumber.trim().length < 5) {
    return res.status(400).json({ error: "clientNumber is required" });
  }

  const effectiveCaller = callerNumber ? callerNumber.trim() : (MY_NUMBER || "");
  if (!effectiveCaller) {
    return res.status(400).json({ error: "callerNumber is required" });
  }

  const normCaller = normaliseNumber(effectiveCaller);
  const now = Date.now();

  for (const [key, val] of pendingCalls.entries()) {
    if (now - val.createdAt > PENDING_TTL_MS) pendingCalls.delete(key);
  }

  pendingCalls.set(normCaller, {
    clientNumber: clientNumber.trim(),
    callerNumber: effectiveCaller,
    createdAt: now,
  });
  console.log(`[PrepareCall] Registered caller ${normCaller} (${effectiveCaller}) → client ${clientNumber.trim()}`);

  return res.json({
    success:      true,
    message:      "Ready. Now dial the Plivo number from your phone.",
    clientNumber: clientNumber.trim(),
    callerNumber: effectiveCaller,
    expiresInMs:  PENDING_TTL_MS,
  });
});

/** GET /api/prepare-call/status */
app.get("/api/prepare-call/status", (req, res) => {
  const reqCaller = req.query.callerNumber;
  const callerKey = reqCaller ? normaliseNumber(reqCaller) : normaliseNumber(MY_NUMBER || "");
  
  let entry = callerKey ? pendingCalls.get(callerKey) : null;
  if (!entry && pendingCalls.size > 0) {
    entry = pendingCalls.values().next().value;
  }
  const active = sessions.size;

  if (!entry || Date.now() - entry.createdAt > PENDING_TTL_MS) {
    return res.json({ pending: false, activeCalls: active });
  }

  return res.json({
    pending:      true,
    clientNumber: entry.clientNumber,
    callerNumber: entry.callerNumber,
    activeCalls:  active,
    expiresInMs:  PENDING_TTL_MS - (Date.now() - entry.createdAt),
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

    try { if (session.myWs && session.myWs.readyState === 1) session.myWs.close(); } catch {}
    try { if (session.clientWs && session.clientWs.readyState === 1) session.clientWs.close(); } catch {}

    cleanupSession(sessionId);
  }
  return endedCount;
};

app.post("/api/end-call", (req, res) => {
  console.log("[EndCall] Terminating all active call legs and pending registrations...");
  const endedCount = terminateActiveCalls();
  return res.json({ success: true, message: `Ended ${endedCount} active calls`, endedCount });
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

  const fromNumber = req.body?.From || req.body?.from || req.query?.From || req.query?.from || "";
  const callUUID   = req.body?.CallUUID || req.body?.callUUID || req.query?.CallUUID || req.query?.callUUID || "";
  const toNumber   = req.body?.To || req.body?.to || req.query?.To || req.query?.to || "";

  try {
    const urlObj = new URL(wsUrl);
    if (fromNumber) urlObj.searchParams.set("from", fromNumber);
    if (callUUID)   urlObj.searchParams.set("callUUID", callUUID);
    if (toNumber)   urlObj.searchParams.set("to", toNumber);
    wsUrl = urlObj.toString();
  } catch (_) {
    if (fromNumber) wsUrl += (wsUrl.includes("?") ? "&" : "?") + `from=${encodeURIComponent(fromNumber)}`;
  }

  const escapedWsUrl = wsUrl.replace(/&/g, "&amp;");
  const extraHeadersAttr = fromNumber ? ` extraHeaders="from:${fromNumber}"` : "";

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

  let callUUID  = queryCallUUID || null;
  let sessionId = null;

  myWs.on("message", async (raw) => {
    let msg;
    try { msg = JSON.parse(raw.toString()); } catch { return; }

    switch (msg.event) {
      case "start": {
        callUUID  = msg.start?.callId || msg.start?.callUUID || msg.start?.call_uuid || queryCallUUID || "unknown";
        sessionId = callUUID;

        let fromNumber = queryFrom || msg.start?.from || msg.start?.callerName || "";

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

        console.log(`[MyWS] Call started | UUID: ${callUUID} | from: ${fromNumber || "(not provided in stream)"}`);
        console.log(`[MyWS] Metadata:`, JSON.stringify(msg.start, null, 2));

        const pending = getPendingCall(fromNumber);

        if (!pending) {
          console.warn("[MyWS] No pending client number registered — hanging up");
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
          console.log(`[MyWS] Using pending registration for caller ${pending.callerNumber || MY_NUMBER}`);
        }

        const clientNumber = pending.clientNumber;

        sessions.set(sessionId, {
          myWs,
          clientWs:       null,
          s2s:            null,
          streamer:       new AudioStreamer(null, sessionId, "client"),
          clientNumber,
          myCallUuid:     callUUID,
          clientCallUuid: null,
        });

        console.log(`[MyWS] Will dial client: ${clientNumber}`);

        try {
          const resp = await dialClient(sessionId, clientNumber);
          const currentSession = sessions.get(sessionId);
          if (currentSession && resp) {
            currentSession.clientCallUuid = resp.requestUuid || resp.callUuid || null;
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

        // MY voice (inbound from my phone) → feed to ElevenLabs S2S → modulated → client
        if (track === "inbound") {
          const session = sessions.get(sessionId);
          if (session?.s2s) {
            try {
              const mulawBuf = Buffer.from(payload, "base64");
              session.s2s.addAudio(mulawBuf);
            } catch (err) {
              console.error("[MyWS→S2S] Error:", err.message);
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

  const url       = new URL(req.url, "http://localhost");
  const sessionId = url.searchParams.get("sessionId");
  const session   = sessions.get(sessionId);

  if (!session) {
    console.warn(`[ClientWS] No active session found for ${sessionId} — closing`);
    clientWs.close();
    return;
  }

  session.clientWs = clientWs;
  console.log(`[ClientWS] Linked to session ${sessionId} | client: ${session.clientNumber}`);

  // Initialise ElevenLabs S2S — processes MY voice → modulated audio → plays to client
  const s2s = new ElevenLabsS2S(
    ELEVEN_KEY,
    ELEVEN_VOICE,
    (ulawChunk) => {
      const cur = sessions.get(sessionId);
      if (!cur || !cur.clientWs || cur.clientWs.readyState !== 1) return;

      if (cur.streamer && cur.streamer.ws !== cur.clientWs) {
        cur.streamer.ws = cur.clientWs;
      }
      cur.streamer.addAudio(ulawChunk);

      if (Math.random() < 0.05)
        console.log(`[S2S→Client] Buffered modulated audio (session: ${sessionId})`);
    },
    {
      silenceDurationMs: SILENCE_MS,
      speechThreshold:   SPEECH_THRESH,
      modelId:           ELEVEN_MODEL,
    },
  );

  session.streamer.ws = clientWs;
  session.s2s = s2s;

  clientWs.on("message", (raw) => {
    let msg;
    try { msg = JSON.parse(raw.toString()); } catch { return; }

    if (msg.event === "start") {
      const clientUuid = msg.start?.callId || msg.start?.callUUID || msg.start?.call_uuid;
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
            console.log(`[Router] Client audio → MyWS raw passthrough (session: ${sessionId})`);
        }
      }
    }
  });

  clientWs.on("close", (code) => {
    console.log(`[ClientWS] Closed (code=${code})`);
    cleanupElevenLabs(sessionId);
    const cur = sessions.get(sessionId);
    if (cur) cur.clientWs = null;
  });

  clientWs.on("error", (err) => console.error("[ClientWS] Error:", err.message));
});

// ─── Cleanup Helpers ──────────────────────────────────────────────────────────

function cleanupElevenLabs(sessionId) {
  const session = sessions.get(sessionId);
  if (!session) return;
  try { session.s2s?.destroy(); } catch {}
  if (session.streamer) session.streamer.stop();
  session.s2s = null;
}

function cleanupSession(sessionId) {
  if (!sessionId) return;
  cleanupElevenLabs(sessionId);
  sessions.delete(sessionId);
  console.log(`[Cleanup] Session ${sessionId} removed`);
}

// ─── Start ───────────────────────────────────────────────────────────────────

server.listen(PORT, () => {
  console.log("");
  console.log("╔══════════════════════════════════════════╗");
  console.log("║   Voice Modulation Server (Inverted)     ║");
  console.log(`║   HTTP : http://localhost:${PORT}          ║`);
  console.log(`║   WS   : ws://localhost:${PORT}/stream     ║`);
  console.log(`║   WS   : ws://localhost:${PORT}/client-stream ║`);
  console.log(`║   Voice: ElevenLabs ${ELEVEN_VOICE?.slice(0, 12)}...  ║`);
  console.log(`║   My # : ${MY_NUMBER || "NOT SET"}  ║`);
  console.log("╚══════════════════════════════════════════╝");
  console.log("");
});
