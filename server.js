// server.js — Voice Modulation Server
// Bridges Plivo WebSocket streaming with Sarvam STT + TTS
// to convert the agent's voice to Sarvam Priya before sending to the customer.
//
// Flow:
//   Customer  ──inbound──► /stream   ──────────────────────────────► AgentWS  (raw, no mod)
//   AgentWS   ──inbound──► /agent-stream → STT → transcript → TTS → CustomerWS  (Priya voice)

require("dotenv").config();

const express = require("express");
const http = require("http");
const { WebSocketServer } = require("ws");
const fs = require("fs"); // Added for debugging

const { dialAgent } = require("./plivoClient");
const { createSarvamSTT } = require("./sarvamSTT");
const { createSarvamTTS } = require("./sarvamTTS");
const { mulawToPcm16k, pcm24kToMulaw } = require("./audioUtils");

// ─── Config ──────────────────────────────────────────────────────────────────

const PORT = process.env.PORT || 8080;
const SARVAM_KEY = process.env.SARVAM_API_KEY;
const SPEAKER =
  process.env.SARVAM_VOICE_ID || process.env.SARVAM_FEMALE_SPEAKER || "priya";
const LANG = process.env.SARVAM_LANGUAGE_CODE || "hi-IN";

// ─── Audio Streamer (Jitter Buffer) ──────────────────────────────────────────
// Plivo prefers steady chunks of audio rather than large burst payloads.
class AudioStreamer {
  constructor(ws, sessionId) {
    this.ws = ws;
    this.sessionId = sessionId;
    this.queue = Buffer.alloc(0);
    this.timer = null;
    this.chunkSize = 160; // 20ms of 8kHz mulaw
    this.interval = 20; // 20ms
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
const wss = new WebSocketServer({ noServer: true });
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

  let callUUID = null;
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

        // Register the session. agentWs / sttWs / ttsWs will be filled in
        // later when the agent leg connects (/agent-stream).
        sessions.set(sessionId, {
          customerWs: plivoWs,
          agentWs: null,
          sttWs: null,
          ttsWs: null,
          streamer: new AudioStreamer(plivoWs, sessionId),
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
// Agent inbound audio ──► Sarvam STT ──► transcript ──► Sarvam TTS (Priya) ──► customer
// Does NOT call dialAgent() — that would cause an infinite loop.

agentWss.on("connection", (agentWs, req) => {
  console.log("[AgentWS] ─── Agent leg connected ──────────────────");

  const url = new URL(req.url, "http://localhost");
  const sessionId = url.searchParams.get("sessionId");
  const session = sessions.get(sessionId);

  if (!session) {
    console.warn(
      `[AgentWS] No active session found for ${sessionId} — closing`,
    );
    agentWs.close();
    return;
  }

  session.agentWs = agentWs;
  console.log(`[AgentWS] Linked to session ${sessionId}`);

  // ── Initialise Sarvam TTS (Priya) ─────────────────────────────────────────
  // Create TTS first so it's ready when STT produces transcripts.
  const ttsWs = createSarvamTTS(SARVAM_KEY, SPEAKER, LANG, (pcmBuffer) => {
    // Sarvam is returning 24kHz linear16 PCM.
    // We MUST resample to 8kHz locally because Sarvam ignores sample_rate requests via WebSocket.
    const currentSession = sessions.get(sessionId);
    if (
      !currentSession ||
      !currentSession.customerWs ||
      currentSession.customerWs.readyState !== 1
    )
      return;

    try {
      // Resample 24000Hz PCM to 8000Hz mu-law locally
      const mulawBuf = pcm24kToMulaw(pcmBuffer);

      // Queue the resampled mu-law directly into the streamer to avoid bursting Plivo
      currentSession.streamer.addAudio(mulawBuf);

      if (Math.random() < 0.05)
        console.log(
          `[TTS→Customer] Buffered Priya audio to customer (callId: ${sessionId})`,
        );
    } catch (err) {
      console.error("[TTS→Plivo] Error:", err.message);
    }
  });

  session.ttsWs = ttsWs;

  // ── Initialise Sarvam STT ──────────────────────────────────────────────────
  // STT receives agent PCM audio and fires onTranscript on each recognised phrase.
  const sttWs = createSarvamSTT(SARVAM_KEY, LANG, (transcript, isFinal) => {
    console.log(`[STT] ${isFinal ? "FINAL" : "partial"}: "${transcript}"`);
    if (isFinal && transcript.trim().length > 0) {
      const currentSession = sessions.get(sessionId);
      if (currentSession?.ttsWs?.readyState === 1) {
        currentSession.ttsWs.synthesize(transcript);
      } else {
        console.warn("[STT→TTS] TTS not ready, dropping transcript");
      }
    }
  });

  session.sttWs = sttWs;

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
      // Send raw PCM to STT so we can recognise and re-speak in Priya's voice.
      if (track === "inbound") {
        const currentSession = sessions.get(sessionId);
        if (currentSession?.sttWs?.readyState === 1) {
          try {
            const pcmBuf = mulawToPcm16k(Buffer.from(payload, "base64"));
            // sendAudio() wraps PCM in { event:"audio_input", audio:<base64> } JSON
            // which is what Sarvam saaras:v3-realtime expects (NOT raw binary frames)
            currentSession.sttWs.sendAudio(pcmBuf);
            if (Math.random() < 0.05)
              console.log(
                `[Agent→STT] Routed agent audio to Sarvam STT (callId: ${sessionId})`,
              );
          } catch (err) {
            console.error("[Agent→STT] Conversion error:", err.message);
          }
        }
      }
    }
  });

  agentWs.on("close", (code) => {
    console.log(`[AgentWS] Closed (code=${code})`);
    // Close Sarvam connections tied to this agent leg
    cleanupSarvam(sessionId);
    if (session) session.agentWs = null;
  });

  agentWs.on("error", (err) => console.error("[AgentWS] Error:", err.message));
});

// ─── Cleanup Helpers ──────────────────────────────────────────────────────────

/**
 * Close Sarvam STT + TTS connections for a session without deleting the session.
 * Called when the agent leg drops (customer may still be on hold).
 */
function cleanupSarvam(sessionId) {
  const session = sessions.get(sessionId);
  if (!session) return;
  try {
    session.sttWs?.close();
  } catch {}
  try {
    session.ttsWs?.close();
  } catch {}
  if (session.streamer) session.streamer.stop();
  session.sttWs = null;
  session.ttsWs = null;
}

/**
 * Full session teardown. Called when the customer call ends.
 */
function cleanupSession(sessionId) {
  if (!sessionId) return;
  cleanupSarvam(sessionId);
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
  console.log(`║   Voice: ${SPEAKER} (${LANG})          ║`);
  console.log("╚══════════════════════════════════════════╝");
  console.log("");
});
