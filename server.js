// server.js — Voice Modulation Server
// Bridges Plivo WebSocket streaming with ElevenLabs Speech-to-Speech
// to convert the agent voice before sending to the customer.
//
// Flow:
//   Customer  ──inbound──► /stream   ──────────────────────────────► AgentWS  (raw, no mod)
//   AgentWS   ──inbound──► /agent-stream → ElevenLabs S2S → ulaw_8000 → CustomerWS

require("dotenv").config();

// ─── Process-level guards ─────────────────────────────────────────────────────
// Windows wsarecv TCP abort errors from dropped WebSocket connections
// surface as unhandled rejections or uncaught exceptions.
// Log them and keep the server alive rather than crashing.
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

const { dialAgent } = require("./plivoClient");
const { ElevenLabsS2S } = require("./elevenLabsS2S");

// ─── Config ──────────────────────────────────────────────────────────────────

const PORT           = process.env.PORT || 8080;
const ELEVEN_KEY     = process.env.ELEVEN_LABS_API;
const ELEVEN_VOICE   = process.env.ELEVEN_LABS_VOICE_ID;
const ELEVEN_MODEL   = process.env.ELEVEN_LABS_MODEL || "eleven_multilingual_sts_v2";
const SILENCE_MS     = parseInt(process.env.ELEVEN_LABS_SILENCE_MS    || "300", 10);
const SPEECH_THRESH  = parseInt(process.env.ELEVEN_LABS_SPEECH_THRESHOLD || "200", 10);

if (!ELEVEN_KEY)   console.warn("[Config] ELEVEN_LABS_API not set");
if (!ELEVEN_VOICE) console.warn("[Config] ELEVEN_LABS_VOICE_ID not set");

// ─── Audio Streamer (Jitter Buffer) ──────────────────────────────────────────
// Plivo prefers steady chunks of audio rather than large burst payloads.
class AudioStreamer {
  constructor(ws, sessionId) {
    this.ws = ws;
    this.sessionId = sessionId;
    this.queue = Buffer.alloc(0);
    this.timer = null;
    this.chunkSize = 160; // 20ms of 8kHz mulaw
    this.interval = 20;   // 20ms
  }

  addAudio(mulawBuffer) {
    this.queue = Buffer.concat([this.queue, mulawBuffer]);
    if (!this.timer) {
      this.startStreaming();
    }
  }

  startStreaming() {
    this.timer = setInterval(() => {
      if (this.queue.length >= this.chunkSize) {
        const chunk = this.queue.subarray(0, this.chunkSize);
        this.queue = this.queue.subarray(this.chunkSize);
        this.sendChunk(chunk);
      } else if (this.queue.length > 0) {
        // Optional: drain the last tiny bit, but usually we just wait for more.
      } else {
        clearInterval(this.timer);
        this.timer = null;
      }
    }, this.interval);
  }

  sendChunk(chunk) {
    if (this.ws.readyState === 1) {
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

// ─── App Setup ───────────────────────────────────────────────────────────────

const app = express();
const server = http.createServer(app);

// Use noServer:true + manual upgrade routing to avoid ws upgrade event
// conflicts when multiple WebSocketServer instances share the same HTTP server.
const wss      = new WebSocketServer({ noServer: true });
const agentWss = new WebSocketServer({ noServer: true });

server.on("upgrade", (req, socket, head) => {
  const pathname = new URL(req.url, "http://localhost").pathname;
  if (pathname === "/stream") {
    wss.handleUpgrade(req, socket, head, (ws) =>
      wss.emit("connection", ws, req),
    );
  } else if (pathname === "/agent-stream") {
    agentWss.handleUpgrade(req, socket, head, (ws) =>
      agentWss.emit("connection", ws, req),
    );
  } else {
    socket.destroy();
  }
});

app.use(express.json());
app.use(express.urlencoded({ extended: true }));

// ─── HTTP Routes ─────────────────────────────────────────────────────────────

// Health check
app.get("/", (req, res) => {
  res.json({ status: "ok", message: "Voice modulation server is running" });
});

// Plivo hits this when the agent picks up.
// Returns XML pointing to /agent-stream (NOT /stream, to avoid re-triggering dialAgent).
app.post("/api/plivo/agent-answer", (req, res) => {
  const serverUrl = process.env.SERVER_URL || `http://localhost:${PORT}`;
  let wsUrl = serverUrl.replace(/^https?/, "wss") + "/agent-stream";

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

// ─── Customer WebSocket Handler (/stream) ─────────────────────────────────────
// Handles the incoming call from the customer's phone.

const sessions = new Map();

wss.on("connection", (plivoWs) => {
  console.log("\n[WS] ─── New Plivo connection ─────────────────────");

  let callUUID  = null;
  let sessionId = null;

  plivoWs.on("message", async (raw) => {
    let msg;
    try {
      msg = JSON.parse(raw.toString());
    } catch {
      return;
    }

    switch (msg.event) {
      // ── start ─────────────────────────────────────────────────────────────
      case "start": {
        callUUID =
          msg.start?.callId ||
          msg.start?.callUUID ||
          msg.start?.call_uuid ||
          "unknown";
        sessionId = callUUID;
        console.log(`[WS] Call started | UUID: ${callUUID}`);
        console.log(`[WS] Metadata:`, JSON.stringify(msg.start, null, 2));

        // Register the session. agentWs / s2s will be filled in
        // later when the agent leg connects (/agent-stream).
        sessions.set(sessionId, {
          customerWs: plivoWs,
          agentWs:    null,
          s2s:        null,
          streamer:   new AudioStreamer(plivoWs, sessionId),
        });

        // Dial the agent once
        try {
          await dialAgent(sessionId);
        } catch (err) {
          console.error("[Plivo] Failed to dial agent:", err.message);
        }
        break;
      }

      // ── media ─────────────────────────────────────────────────────────────
      // Customer audio → forward raw (unmodified) to agent so they can hear.
      case "media": {
        const { track, payload } = msg.media || {};
        if (!payload) break;

        if (track === "inbound") {
          const session = sessions.get(sessionId);
          if (session && session.agentWs && session.agentWs.readyState === 1) {
            session.agentWs.send(
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
                `[Router] Routed customer audio → AgentWS (callId: ${sessionId})`,
              );
          }
        }
        break;
      }

      // ── stop ──────────────────────────────────────────────────────────────
      case "stop": {
        console.log(`[WS] Call stopped | UUID: ${callUUID}`);
        cleanupSession(sessionId);
        break;
      }
    }
  });

  plivoWs.on("close", (code) => {
    console.log(`[WS] Connection closed (code=${code}) | UUID: ${callUUID}`);
    cleanupSession(sessionId);
  });

  plivoWs.on("error", (err) => console.error("[WS] Error:", err.message));
});

// ─── Agent WebSocket Handler (/agent-stream) ──────────────────────────────────
// Handles the agent's phone audio.
// Agent inbound audio ──► ElevenLabs S2S ──► ulaw_8000 ──► customer
// Does NOT call dialAgent() — that would cause an infinite loop.

agentWss.on("connection", (agentWs, req) => {
  console.log("[AgentWS] ─── Agent leg connected ──────────────────");

  const url       = new URL(req.url, "http://localhost");
  const sessionId = url.searchParams.get("sessionId");
  const session   = sessions.get(sessionId);

  if (!session) {
    console.warn(
      `[AgentWS] No active session found for ${sessionId} — closing`,
    );
    agentWs.close();
    return;
  }

  session.agentWs = agentWs;
  console.log(`[AgentWS] Linked to session ${sessionId}`);

  // ── Initialise ElevenLabs S2S ─────────────────────────────────────────────
  const s2s = new ElevenLabsS2S(
    ELEVEN_KEY,
    ELEVEN_VOICE,
    (ulawChunk) => {
      // ulaw_8000 bytes arrive directly from ElevenLabs — no resampling needed.
      const currentSession = sessions.get(sessionId);
      if (
        !currentSession ||
        !currentSession.customerWs ||
        currentSession.customerWs.readyState !== 1
      )
        return;
      currentSession.streamer.addAudio(ulawChunk);
      if (Math.random() < 0.05)
        console.log(`[S2S→Customer] Buffered modulated audio (callId: ${sessionId})`);
    },
    {
      silenceDurationMs: SILENCE_MS,
      speechThreshold:   SPEECH_THRESH,
      modelId:           ELEVEN_MODEL,
    },
  );

  session.s2s = s2s;

  // ── Agent media messages ───────────────────────────────────────────────────
  agentWs.on("message", (raw) => {
    let msg;
    try {
      msg = JSON.parse(raw.toString());
    } catch {
      return;
    }

    if (msg.event === "start") {
      console.log(
        "[AgentWS] Agent call started | UUID:",
        msg.start?.callId || msg.start?.callUUID || "unknown",
      );
    } else if (msg.event === "media") {
      const { track, payload } = msg.media || {};
      if (!payload) return;

      // 'inbound' = audio coming FROM the agent's phone (what the agent speaks).
      // Feed raw mu-law directly into S2S — no conversion needed at this stage.
      if (track === "inbound") {
        const currentSession = sessions.get(sessionId);
        if (currentSession?.s2s) {
          try {
            const mulawBuf = Buffer.from(payload, "base64");
            currentSession.s2s.addAudio(mulawBuf).catch(err => {
              console.error("[Agent→S2S] Stream error:", err.message);
            });
          } catch (err) {
            console.error("[Agent→S2S] Error:", err.message);
          }
        }
      }
    }
  });

  agentWs.on("close", (code) => {
    console.log(`[AgentWS] Closed (code=${code})`);
    cleanupElevenLabs(sessionId);
    if (session) session.agentWs = null;
  });

  agentWs.on("error", (err) => console.error("[AgentWS] Error:", err.message));
});

// ─── Cleanup Helpers ──────────────────────────────────────────────────────────

/**
 * Destroy ElevenLabs S2S processor for a session without deleting the session.
 * Called when the agent leg drops (customer may still be on hold).
 */
function cleanupElevenLabs(sessionId) {
  const session = sessions.get(sessionId);
  if (!session) return;
  try {
    session.s2s?.destroy();
  } catch {}
  if (session.streamer) session.streamer.stop();
  session.s2s = null;
}

/**
 * Full session teardown. Called when the customer call ends.
 */
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
  console.log("║       Voice Modulation Server            ║");
  console.log(`║   HTTP : http://localhost:${PORT}          ║`);
  console.log(`║   WS   : ws://localhost:${PORT}/stream     ║`);
  console.log(`║   WS   : ws://localhost:${PORT}/agent-stream ║`);
  console.log(`║   Voice: ElevenLabs ${ELEVEN_VOICE?.slice(0, 12)}…  ║`);
  console.log("╚══════════════════════════════════════════╝");
  console.log("");
});
