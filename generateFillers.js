// generateFillers.js
// Pre-generates Guruji voice fillers using ElevenLabs TTS in 8kHz u-law format.
// Saves both .raw (ready for Plivo streaming) and .wav (for listening preview).

require("dotenv").config();
const fs = require("fs");
const path = require("path");
const alawmulaw = require("alawmulaw");
const { WaveFile } = require("wavefile");

const ELEVEN_KEY = process.env.ELEVEN_LABS_API;
const ELEVEN_VOICE = process.env.ELEVEN_LABS_VOICE_ID;
const MODEL_ID = process.env.ELEVEN_LABS_TTS_MODEL || "eleven_multilingual_v2";

if (!ELEVEN_KEY || !ELEVEN_VOICE) {
  console.error(
    "[generateFillers] Missing ELEVEN_LABS_API or ELEVEN_LABS_VOICE_ID in .env",
  );
  process.exit(1);
}

const FILLERS_DIR = path.join(__dirname, "fillers");
if (!fs.existsSync(FILLERS_DIR)) {
  fs.mkdirSync(FILLERS_DIR, { recursive: true });
}

// Phrases with spiritual, warm Guruji intonation
const PHRASES = [
  { id: "jai_guruji", text: "जय गुरुजी", label: "Jai Guruji" },
  { id: "shukrana_guruji", text: "शुकराना गुरुजी", label: "Shukrana Guruji" },
  { id: "hari_om", text: "हरि ॐ", label: "Hari Om" },
  { id: "hey_ram", text: "हे राम", label: "Hey Ram" },
  { id: "haan_beta", text: "हाँ बेटा", label: "Haan beta" },
  { id: "kalyan_ho", text: "कल्याण हो", label: "Kalyan ho" },
  { id: "aashirwad", text: "आशीर्वाद", label: "Aashirwad" },
];

// 120ms of comfort silence in 8kHz u-law (0xFF = 255 = standard u-law silence)
const COMFORT_TAIL_BYTES = Math.round((120 / 1000) * 8000); // 960 bytes
const COMFORT_TAIL = Buffer.alloc(COMFORT_TAIL_BYTES, 255);

async function generatePhrase(item) {
  const url = `https://api.elevenlabs.io/v1/text-to-speech/${ELEVEN_VOICE}?output_format=ulaw_8000`;

  console.log(
    `[generateFillers] Synthesizing "${item.label}" (${item.text})...`,
  );

  const resp = await fetch(url, {
    method: "POST",
    headers: {
      "xi-api-key": ELEVEN_KEY,
      "Content-Type": "application/json",
      Accept: "audio/basic",
    },
    body: JSON.stringify({
      text: item.text,
      model_id: MODEL_ID,
      voice_settings: {
        stability: 0.75,
        similarity_boost: 0.85,
        style: 0.0,
        use_speaker_boost: true,
      },
    }),
  });

  if (!resp.ok) {
    const errText = await resp.text();
    throw new Error(`ElevenLabs TTS failed (${resp.status}): ${errText}`);
  }

  const arrayBuffer = await resp.arrayBuffer();
  let mulawBuf = Buffer.from(arrayBuffer);

  // If header exists (some endpoints return wav container even for ulaw), strip it
  if (mulawBuf.length >= 44 && mulawBuf.toString("utf8", 0, 4) === "RIFF") {
    mulawBuf = mulawBuf.subarray(44);
  }

  // Append comfort tail for smooth conversational breathing pause
  const fullMulaw = Buffer.concat([mulawBuf, COMFORT_TAIL]);

  // Save .raw (ready for AudioStreamer)
  const rawPath = path.join(FILLERS_DIR, `${item.id}.raw`);
  fs.writeFileSync(rawPath, fullMulaw);

  // Also convert to 8kHz WAV for desktop preview
  const uint8 = new Uint8Array(
    fullMulaw.buffer,
    fullMulaw.byteOffset,
    fullMulaw.byteLength,
  );
  const pcm8k = alawmulaw.mulaw.decode(uint8);
  const wav = new WaveFile();
  wav.fromScratch(1, 8000, "16", pcm8k);
  const wavPath = path.join(FILLERS_DIR, `${item.id}.wav`);
  fs.writeFileSync(wavPath, wav.toBuffer());

  const durationMs = Math.round((fullMulaw.length / 8000) * 1000);
  console.log(
    `  ✓ Saved ${item.id}.raw and ${item.id}.wav (${durationMs}ms, ${fullMulaw.length} bytes)`,
  );
}

async function run() {
  console.log("=============================================================");
  console.log("   Guruji Voice Filler Generator (ElevenLabs TTS ulaw_8000)  ");
  console.log(`   Voice ID: ${ELEVEN_VOICE}`);
  console.log(`   Model   : ${MODEL_ID}`);
  console.log(`   Output  : ${FILLERS_DIR}`);
  console.log(
    "=============================================================\n",
  );

  for (const item of PHRASES) {
    try {
      await generatePhrase(item);
    } catch (err) {
      console.error(`  ✗ Error synthesizing ${item.label}:`, err.message);
    }
  }

  console.log("\nAll fillers generated successfully!");
}

run();
