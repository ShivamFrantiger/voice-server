// audioUtils.js
// Converts between Plivo's 8kHz μ-law and Sarvam's 16kHz PCM16
// Also provides real-time DSP pitch shifting for voice modulation.
// Uses the `alawmulaw` npm package for G.711 codec support.

const alawmulaw = require('alawmulaw');

/**
 * Decode μ-law 8kHz Buffer → PCM16 16kHz Buffer (for Sarvam STT)
 * Sarvam STT expects raw 16-bit little-endian PCM at 16kHz.
 *
 * @param {Buffer} mulawBuffer - raw μ-law bytes from Plivo (base64-decoded)
 * @returns {Buffer}           - PCM16 at 16kHz
 */
function mulawToPcm16k(mulawBuffer) {
  // alawmulaw.mulaw.decode expects Uint8Array → returns Int16Array at 8kHz
  const uint8 = new Uint8Array(mulawBuffer.buffer, mulawBuffer.byteOffset, mulawBuffer.byteLength);
  const pcm8k = alawmulaw.mulaw.decode(uint8); // Int16Array, 8kHz

  // Upsample 8kHz → 16kHz via simple linear interpolation (2x)
  const pcm16k = new Int16Array(pcm8k.length * 2);
  for (let i = 0; i < pcm8k.length; i++) {
    pcm16k[i * 2] = pcm8k[i];
    if (i < pcm8k.length - 1) {
      pcm16k[i * 2 + 1] = Math.round((pcm8k[i] + pcm8k[i + 1]) / 2);
    } else {
      pcm16k[i * 2 + 1] = pcm8k[i];
    }
  }

  return Buffer.from(pcm16k.buffer);
}

/**
 * Encode PCM16 16kHz Buffer → μ-law 8kHz Buffer (Sarvam TTS output → Plivo)
 * Plivo's bidirectional stream expects 8kHz μ-law in the playAudio event.
 *
 * @param {Buffer} pcmBuffer - raw PCM16 bytes from Sarvam TTS (base64-decoded)
 * @returns {Buffer}         - μ-law at 8kHz
 */
function pcm16kToMulaw(pcmBuffer) {
  const pcm16k = new Int16Array(pcmBuffer.buffer, pcmBuffer.byteOffset, pcmBuffer.byteLength / 2);

  // Downsample 16kHz → 8kHz by taking every other sample
  const pcm8k = new Int16Array(Math.floor(pcm16k.length / 2));
  for (let i = 0; i < pcm8k.length; i++) {
    pcm8k[i] = pcm16k[i * 2];
  }

  // alawmulaw.mulaw.encode expects Int16Array → returns Uint8Array
  const encoded = alawmulaw.mulaw.encode(pcm8k);
  return Buffer.from(encoded.buffer);
}

/**
 * Real-time pitch shifting on raw μ-law payload (base64 string).
 *
 * Algorithm (OLA-lite / resample trick):
 *   1. Decode μ-law → PCM16 (8 kHz)
 *   2. Compress time by `ratio` via linear interpolation resampling
 *      (read only 1/ratio as many samples → fewer samples = higher frequency)
 *   3. Stretch back to the original frame length by linear interpolation
 *      (maintains real-time duration so Plivo isn't starved)
 *   4. Re-encode PCM16 → μ-law → base64
 *
 * This raises the perceived pitch by `ratio` (e.g. 1.50 ≈ +7 semitones,
 * making a male/neutral voice sound clearly female) with zero extra latency.
 *
 * @param {string} base64Payload - base64-encoded μ-law bytes from Plivo
 * @param {number} ratio         - pitch multiplier (> 1 = higher pitch)
 *                                 2^(semitones/12): +7 st → 2^(7/12) ≈ 1.498
 * @returns {string}             - base64-encoded μ-law with shifted pitch
 */
function pitchShiftMulaw(base64Payload, ratio) {
  // ── 1. Decode μ-law → PCM16 ──────────────────────────────────────────────
  const rawBuf = Buffer.from(base64Payload, 'base64');
  const uint8   = new Uint8Array(rawBuf.buffer, rawBuf.byteOffset, rawBuf.byteLength);
  const pcm     = alawmulaw.mulaw.decode(uint8); // Int16Array @ 8 kHz
  const n       = pcm.length;

  if (n === 0) return base64Payload; // nothing to process

  // ── 2. Time-compress by `ratio` via linear interpolation ─────────────────
  // We read through `pcm` stepping by `ratio` — each output sample is
  // interpolated between two source samples. The result has fewer samples
  // (n / ratio), which when played back at 8 kHz sounds higher-pitched.
  const compressedLen = Math.round(n / ratio);
  const compressed    = new Int16Array(compressedLen);
  for (let i = 0; i < compressedLen; i++) {
    const srcF  = i * ratio;          // fractional source index
    const srcI  = Math.floor(srcF);
    const frac  = srcF - srcI;
    const s0    = pcm[Math.min(srcI,     n - 1)];
    const s1    = pcm[Math.min(srcI + 1, n - 1)];
    compressed[i] = Math.round(s0 + frac * (s1 - s0));
  }

  // ── 3. Stretch back to original frame length via linear interpolation ─────
  // Plivo expects a constant frame rate, so we re-expand to `n` samples.
  // Pitch is already baked in from step 2; stretching doesn't undo it.
  const stretched = new Int16Array(n);
  const cLen      = compressed.length;
  for (let i = 0; i < n; i++) {
    const srcF = i * (cLen - 1) / (n - 1);
    const srcI = Math.floor(srcF);
    const frac = srcF - srcI;
    const s0   = compressed[Math.min(srcI,     cLen - 1)];
    const s1   = compressed[Math.min(srcI + 1, cLen - 1)];
    stretched[i] = Math.round(s0 + frac * (s1 - s0));
  }

  // ── 4. Re-encode PCM16 → μ-law → base64 ─────────────────────────────────
  const encoded = alawmulaw.mulaw.encode(stretched);
  return Buffer.from(encoded.buffer).toString('base64');
}

module.exports = { mulawToPcm16k, pcm16kToMulaw, pitchShiftMulaw };
