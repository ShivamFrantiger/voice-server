// server.js — Voice Modulation Server
// Bridges Plivo WebSocket streaming with Sarvam STT + TTS
// to convert the agent's voice to female before sending to the customer.
//

require("dotenv").config();

const express = require("express");
const http = require("http");
const { WebSocketServer } = require("ws");

const { dialAgent } = require("./plivoClient");
const { createSarvamSTT } = require("./sarvamSTT");
const { createSarvamTTS } = require("./sarvamTTS");
const { mulawToPcm16k, pcm16kToMulaw } = require("./audioUtils");

// ─── Config ──────────────────────────────────────────────────────────────────

const PORT = process.env.PORT || 8080;
const SARVAM_KEY = process.env.SARVAM_API_KEY;
const SPEAKER = process.env.SARVAM_FEMALE_SPEAKER || "priya";
const LANG = process.env.SARVAM_LANGUAGE_CODE || "hi-IN";

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

const sessions = new Map();

wss.on("connection", (plivoWs) => {
  console.log("\n[WS] ─── New Plivo connection ─────────────────────");

  let callUUID = null;
  let sessionId = null;
  let sttWs = null;
  let ttsWs = null;

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

        // --- SARVAM STT/TTS BYPASSED FOR TESTING ---
        // sttWs = createSarvamSTT(SARVAM_KEY, LANG, (transcript, isFinal) => {
        //   console.log(`[STT] ${isFinal ? 'FINAL' : 'partial'}: "${transcript}"`);
        //   if (isFinal && transcript.trim().length > 0) {
        //     const session = sessions.get(sessionId);
        //     if (session?.ttsWs) session.ttsWs.synthesize(transcript);
        //   }
        // });
        //
        // ttsWs = createSarvamTTS(SARVAM_KEY, SPEAKER, LANG, (pcmBuffer) => {
        //   const session = sessions.get(sessionId);
        //   if (!session || !session.customerWs || session.customerWs.readyState !== 1) return;
        //   try {
        //     const mulawBuf = pcm16kToMulaw(pcmBuffer);
        //     session.customerWs.send(JSON.stringify({
        //       event: 'playAudio',
        //       media: { contentType: 'audio/x-mulaw', sampleRate: 8000, payload: mulawBuf.toString('base64') },
        //     }));
        //     console.log(`[TTS] Sent modulated audio to CustomerWS (callId: ${sessionId})`);
        //   } catch (err) {
        //     console.error('[TTS→Plivo] Error:', err.message);
        //   }
        // });

        sessions.set(sessionId, {
          customerWs: plivoWs,
          sttWs: null,
          ttsWs: null,
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
      case "media": {
        const { track, payload } = msg.media || {};
        if (!payload) break;
        // Send customer audio to the agent so they can hear
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
                `[Router] Routed customer audio to AgentWS (callId: ${sessionId})`,
              );
          }
        }
        break;
      }

      // ── stop ──────────────────────────────────────────────────────────────
      case "stop": {
        console.log(`[WS] Call stopped | UUID: ${callUUID}`);
        cleanup(sessionId, sttWs, ttsWs);
        break;
      }
    }
  });

  plivoWs.on("close", (code) => {
    console.log(`[WS] Connection closed (code=${code}) | UUID: ${callUUID}`);
    cleanup(sessionId, sttWs, ttsWs);
  });

  plivoWs.on("error", (err) => console.error("[WS] Error:", err.message));
});

// ─── Agent WebSocket Handler (/agent-stream) ──────────────────────────────────
// Handles agent's phone audio. Does NOT call dialAgent() — prevents the loop.

agentWss.on("connection", (agentWs, req) => {
  console.log("[AgentWS] ─── Agent leg connected ──────────────────");

  const url = new URL(req.url, "http://localhost");
  const sessionId = url.searchParams.get("sessionId");
  let session = sessions.get(sessionId);

  if (session) {
    session.agentWs = agentWs;
    console.log(`[AgentWS] Linked to session ${sessionId}`);
  } else {
    console.warn(`[AgentWS] No active session found for ${sessionId}`);
  }

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
      // 'inbound' = agent's voice → send directly to customer
      // --- SARVAM ROUTING COMMENTED OUT ---
      // if (track === 'inbound' && session && session.sttWs && session.sttWs.readyState === 1) {
      //   session.sttWs.send(mulawToPcm16k(Buffer.from(payload, 'base64')));
      //   if (Math.random() < 0.05) console.log(`[Router] Routed agent audio to STT (callId: ${sessionId})`);
      // }

      if (
        track === "inbound" &&
        session &&
        session.customerWs &&
        session.customerWs.readyState === 1
      ) {
        session.customerWs.send(
          JSON.stringify({
            event: "playAudio",
            media: { contentType: "audio/x-mulaw", sampleRate: 8000, payload },
          }),
        );
        if (Math.random() < 0.05)
          console.log(
            `[Router] Routed raw agent audio directly to CustomerWS (callId: ${sessionId})`,
          );
      }
    }
  });

  agentWs.on("close", (code) => {
    console.log(`[AgentWS] Closed (code=${code})`);
    if (session) session.agentWs = null;
  });
  agentWs.on("error", (err) => console.error("[AgentWS] Error:", err.message));
});

// ─── Cleanup ─────────────────────────────────────────────────────────────────

function cleanup(sessionId, sttWs, ttsWs) {
  if (sessionId) sessions.delete(sessionId);
  try {
    sttWs?.close();
  } catch {}
  try {
    ttsWs?.close();
  } catch {}
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
