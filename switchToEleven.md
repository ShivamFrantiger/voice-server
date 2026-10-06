# Voice Modulation Engine Guide (`switchToEleven.md`)

> [!NOTE]
> **Manual Code Editing is No Longer Needed!**
> The system has been upgraded to a **Dynamic Dual-Engine Architecture**. Both ElevenLabs and Sarvam run side-by-side. You can select either engine on the frontend UI ([VoiceModulator](file:///c:/Drive/Baba/VoiceModulator/src/app/page.tsx)), via the CLI (`node switchEngine.js elevenlabs` / `sarvam`), or via the REST API (`POST /api/engine`).
> 
> The sections below are preserved for historical reference of how each engine operates internally.

---

## Quick Reference: Switching Engines on Command

### 1. From the Frontend UI (`VoiceModulator`)
Simply click the **ElevenLabs S2S** or **Sarvam AI** toggle card on the dialer screen before clicking "Initiate Guruji Voice Call". Your preference is auto-saved in your browser.

### 2. From the Command Line (CLI)
```bash
# In c:\Drive\Baba\voice-server
npm run switch:eleven       # Switch default engine to ElevenLabs
npm run switch:sarvam       # Switch default engine to Sarvam AI
npm run engine:status       # Check current engine status
```

### 3. Via REST API (Live on Render / Production)
```bash
# Switch to Sarvam
curl -X POST https://voice-server-i2v1.onrender.com/api/engine -H "Content-Type: application/json" -d '{"engine":"sarvam"}'

# Switch to ElevenLabs
curl -X POST https://voice-server-i2v1.onrender.com/api/engine -H "Content-Type: application/json" -d '{"engine":"elevenlabs"}'
```

---

There are **5 exact spots** in [server.js](file:///c:/Drive/Baba/voice-server/server.js) with paired Sarvam and ElevenLabs blocks:

| Spot | Location | What to Comment | What to Uncomment |
| :--- | :--- | :--- | :--- |
| **1** | Top Imports & Config (~Line 47) | Sarvam imports & config | ElevenLabs imports & config |
| **2** | Session Setup (~Line 480) | `sttWs: null, ttsWs: null` | `s2s: null` |
| **3** | Inbound Media Stream (~Line 513) | Sarvam STT send block | ElevenLabs `s2s.addAudio()` block |
| **4** | Client Connection (~Line 580) | Sarvam TTS + STT init | ElevenLabs `s2s` init |
| **5** | Teardown & Startup Banner (~Line 697) | `cleanupSarvam()` & Sarvam banner | `cleanupElevenLabs()` & ElevenLabs banner |

---

## Detailed Step-by-Step Instructions

### Step 1: Top Imports & Config (Lines ~47 to 72)

#### 🔴 Comment out Sarvam & 🟢 Uncomment ElevenLabs:
```javascript
// === SARVAM CONFIG (COMMENT OUT) ==============================================
/*
const { createSarvamSTT } = require("./sarvamSTT");
const { createSarvamTTS } = require("./sarvamTTS");
const { mulawToPcm16k, pcm24kToMulaw } = require("./audioUtils");

const SARVAM_KEY     = process.env.SARVAM_API_KEY;
const SARVAM_SPEAKER = process.env.SARVAM_VOICE_ID || process.env.SARVAM_FEMALE_SPEAKER || "priya";
const SARVAM_LANG    = process.env.SARVAM_LANGUAGE_CODE || "hi-IN";
if (!SARVAM_KEY)  console.warn("[Config] SARVAM_API_KEY not set");
*/

// === ELEVENLABS ENGINE CONFIG (ACTIVE - UNCOMMENTED) ==========================
const { ElevenLabsS2S } = require("./elevenLabsS2S");
const ELEVEN_KEY    = process.env.ELEVEN_LABS_API;
const ELEVEN_VOICE  = process.env.ELEVEN_LABS_VOICE_ID;
const ELEVEN_MODEL  = process.env.ELEVEN_LABS_MODEL || "eleven_multilingual_sts_v2";
const SILENCE_MS    = parseInt(process.env.ELEVEN_LABS_SILENCE_MS    || "300", 10);
const SPEECH_THRESH = parseInt(process.env.ELEVEN_LABS_SPEECH_THRESHOLD || "200", 10);
if (!ELEVEN_KEY)   console.warn("[Config] ELEVEN_LABS_API not set");
if (!ELEVEN_VOICE) console.warn("[Config] ELEVEN_LABS_VOICE_ID not set");
// ==============================================================================

const PORT      = process.env.PORT || 8080;
const MY_NUMBER = process.env.MY_PHONE_NUMBER; // MY hardcoded number (trusted caller)
if (!MY_NUMBER) console.warn("[Config] MY_PHONE_NUMBER not set — caller validation disabled");
```

---

### Step 2: Session Map Registration (Lines ~478 to 488)

In `myWss.on("connection")` -> `case "start"`:

```javascript
        sessions.set(sessionId, {
          myWs,
          clientWs:       null,
          // sttWs:       null, // (Sarvam handle - uncomment if reviving Sarvam)
          // ttsWs:       null, // (Sarvam handle - uncomment if reviving Sarvam)
          s2s:            null, // ElevenLabs handle (ACTIVE)
          streamer:       new AudioStreamer(null, sessionId, "client"),
          clientNumber,
          myCallUuid:     callUUID,
          clientCallUuid: null,
        });
```

---

### Step 3: Inbound Media Stream (Lines ~513 to 539)

In `myWss.on("connection")` -> `case "media"` -> `if (track === "inbound")`:

```javascript
        // MY voice (inbound from my phone) → feed to modulation pipeline
        if (track === "inbound") {
          const session = sessions.get(sessionId);

          /*
          // === SARVAM STT FLOW (COMMENTED - UNCOMMENT TO REVIVE) ===
          if (session?.sttWs && session.sttWs.readyState === 1) {
            try {
              const mulawBuf = Buffer.from(payload, "base64");
              const pcmBuf   = mulawToPcm16k(mulawBuf);
              session.sttWs.sendAudio(pcmBuf);
            } catch (err) {
              console.error("[MyWS→STT] Conversion/Send error:", err.message);
            }
          }
          // ========================================================
          */

          // === ELEVENLABS S2S FLOW (ACTIVE) =======================
          if (session?.s2s) {
            try {
              const mulawBuf = Buffer.from(payload, "base64");
              session.s2s.addAudio(mulawBuf);
            } catch (err) {
              console.error("[MyWS→S2S] Error:", err.message);
            }
          }
          // ========================================================
        }
```

---

### Step 4: Client Leg Initialization (Lines ~580 to 655)

In `clientWss.on("connection")`:

```javascript
  session.clientWs = clientWs;
  console.log(`[ClientWS] Linked to session ${sessionId} | client: ${session.clientNumber}`);

  /*
  // === SARVAM STT + TTS INITIALIZATION (COMMENTED - UNCOMMENT TO REVIVE) ======
  const ttsWs = createSarvamTTS(SARVAM_KEY, SARVAM_SPEAKER, SARVAM_LANG, (pcmBuffer) => {
    const cur = sessions.get(sessionId);
    if (!cur || !cur.clientWs || cur.clientWs.readyState !== 1) return;
    if (cur.streamer && cur.streamer.ws !== cur.clientWs) cur.streamer.ws = cur.clientWs;
    try {
      const mulawBuf = pcm24kToMulaw(pcmBuffer);
      cur.streamer.addAudio(mulawBuf);
    } catch (err) {
      console.error("[TTS→Client] Audio downsampling error:", err.message);
    }
  });
  session.ttsWs = ttsWs;

  const sttWs = createSarvamSTT(SARVAM_KEY, SARVAM_LANG, (transcript, isFinal) => {
    if (isFinal && transcript && transcript.trim().length > 0) {
      const cur = sessions.get(sessionId);
      if (cur?.ttsWs) {
        cur.ttsWs.synthesize(transcript);
      }
    }
  });
  session.sttWs = sttWs;
  // ============================================================================
  */

  // === ELEVENLABS S2S INITIALIZATION (ACTIVE) =================================
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
  // ============================================================================
```

---

### Step 5: Teardown, Cleanup & Startup Banner (Lines ~696 to 752)

#### In `clientWs.on("close")`:
```javascript
  clientWs.on("close", (code) => {
    console.log(`[ClientWS] Closed (code=${code})`);
    // cleanupSarvam(sessionId); // (Uncomment if using Sarvam)
    cleanupElevenLabs(sessionId); // (ACTIVE)
    const cur = sessions.get(sessionId);
    if (cur) cur.clientWs = null;
  });
```

#### In Cleanup Helpers:
```javascript
/*
// === SARVAM CLEANUP (COMMENTED - UNCOMMENT TO REVIVE) =========================
function cleanupSarvam(sessionId) {
  const session = sessions.get(sessionId);
  if (!session) return;
  try { session.sttWs?.close(); } catch {}
  try { session.ttsWs?.close(); } catch {}
  if (session.streamer) session.streamer.stop();
  session.sttWs = null;
  session.ttsWs = null;
}
*/

// === ELEVENLABS CLEANUP (ACTIVE) ==============================================
function cleanupElevenLabs(sessionId) {
  const session = sessions.get(sessionId);
  if (!session) return;
  try { session.s2s?.destroy(); } catch {}
  if (session.streamer) session.streamer.stop();
  session.s2s = null;
}
// ==============================================================================

function cleanupSession(sessionId) {
  if (!sessionId) return;
  // cleanupSarvam(sessionId); // (Uncomment if using Sarvam)
  cleanupElevenLabs(sessionId); // (ACTIVE)
  sessions.delete(sessionId);
  console.log(`[Cleanup] Session ${sessionId} removed`);
}
```

#### In Startup Banner:
```javascript
server.listen(PORT, () => {
  console.log("");
  console.log("╔══════════════════════════════════════════╗");
  console.log("║   Voice Modulation Server (Inverted)     ║");
  console.log(`║   HTTP : http://localhost:${PORT}          ║`);
  console.log(`║   WS   : ws://localhost:${PORT}/stream     ║`);
  console.log(`║   WS   : ws://localhost:${PORT}/client-stream ║`);
  console.log(`║   Engine: ElevenLabs Speech-to-Speech    ║`);
  console.log(`║   Voice : ElevenLabs ${ELEVEN_VOICE?.slice(0, 12)}...  ║`);
  console.log(`║   My # : ${MY_NUMBER || "NOT SET"}  ║`);
  console.log("╚══════════════════════════════════════════╝");
  console.log("");
});
```

---

## Verification & Sanity Checks

1. **Verify Environment Variables** in [.env](file:///c:/Drive/Baba/voice-server/.env):
   - `ELEVEN_LABS_API` must contain your valid ElevenLabs API key (`sk_...`).
   - `ELEVEN_LABS_VOICE_ID` must contain your ElevenLabs voice ID (e.g., `qzd9kLDZI6qcXC4JuIkJ`).
2. **Start the server**:
   ```bash
   node server.js
   ```
3. **Verify Healthcheck**:
   Open browser or terminal:
   ```bash
   curl http://localhost:8080/
   ```
   Should output:
   ```json
   {
     "status": "ok",
     "activeCalls": 0
   }
   ```

---

## How to Switch Back to Sarvam

To switch back from ElevenLabs to Sarvam, simply do the reverse of the 5 steps:
1. Comment ElevenLabs in Step 1, uncomment Sarvam.
2. In Step 2, set `sttWs: null, ttsWs: null` and comment `s2s`.
3. In Step 3, uncomment Sarvam STT block and comment ElevenLabs S2S block.
4. In Step 4, uncomment Sarvam STT+TTS block and comment ElevenLabs init.
5. In Step 5, switch `cleanupElevenLabs()` to `cleanupSarvam()`.
