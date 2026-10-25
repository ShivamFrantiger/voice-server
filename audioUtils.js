const alawmulaw = require('alawmulaw');
const { WaveFile } = require('wavefile');

// Noise gate: μ-law silence byte (127 = 0 PCM after decode)
const MULAW_SILENCE_BYTE = 127;

/**
 * Decode μ-law 8kHz Buffer → PCM16 16kHz Buffer (for Sarvam STT).
 * Sarvam STT expects raw 16-bit little-endian PCM at 16kHz.
 *
 * @param {Buffer} mulawBuffer - raw μ-law bytes from Plivo (base64-decoded)
 * @returns {Buffer}           - PCM16 at 16kHz
 */
function mulawToPcm16k(mulawBuffer) {
  const uint8 = new Uint8Array(mulawBuffer.buffer, mulawBuffer.byteOffset, mulawBuffer.byteLength);
  const pcm8k = alawmulaw.mulaw.decode(uint8); // Int16Array, 8kHz

  // Upsample 8kHz → 16kHz via wavefile resampler
  const wav = new WaveFile();
  wav.fromScratch(1, 8000, '16', pcm8k);
  wav.toSampleRate(16000);
  
  const pcm16k = wav.getSamples(false, Int16Array);
  return Buffer.from(pcm16k.buffer, pcm16k.byteOffset, pcm16k.byteLength);
}

/**
 * Decode μ-law 8kHz Buffer → PCM16 16kHz as Int16Array (fast path).
 * Used by the ElevenLabs WS streaming path — returns raw Int16Array
 * to avoid an extra Buffer allocation round-trip.
 *
 * @param {Buffer} mulawBuffer - raw μ-law bytes from Plivo (base64-decoded)
 * @returns {Int16Array}       - PCM16 at 16kHz
 */
function mulawToPcm16kRaw(mulawBuffer) {
  const uint8 = new Uint8Array(mulawBuffer.buffer, mulawBuffer.byteOffset, mulawBuffer.byteLength);
  const pcm8k = alawmulaw.mulaw.decode(uint8); // Int16Array, 8kHz

  // Simple 2x linear upsample (8k → 16k): insert interpolated sample between each pair
  // This is intentionally lightweight for the streaming path (each chunk is tiny ~20ms)
  const pcm16k = new Int16Array(pcm8k.length * 2);
  for (let i = 0; i < pcm8k.length; i++) {
    pcm16k[i * 2] = pcm8k[i];
    // Linear interpolation between adjacent samples
    const next = i + 1 < pcm8k.length ? pcm8k[i + 1] : pcm8k[i];
    pcm16k[i * 2 + 1] = Math.round((pcm8k[i] + next) / 2);
  }
  return pcm16k;
}

/**
 * Apply a noise gate to a μ-law buffer.
 * In G.711 μ-law, 255 (positive 0) and 127 (negative 0) represent zero amplitude.
 * Values near zero are [255 - gateRange .. 255] and [127 - gateRange .. 127].
 * Bytes 128..135 are high positive amplitudes (+32,000 PCM) and MUST NOT be zeroed.
 *
 * @param {Buffer} mulawBuffer
 * @param {number} [gateRange=3]
 * @returns {Buffer}
 */
function applyMulawNoiseGate(mulawBuffer, gateRange = 3) {
  const out = Buffer.from(mulawBuffer);
  for (let i = 0; i < out.length; i++) {
    const b = out[i];
    // Check if within gateRange of negative zero (127) or positive zero (255)
    if ((b <= 127 && b >= 127 - gateRange) || (b <= 255 && b >= 255 - gateRange)) {
      out[i] = 255; // 255 = standard μ-law silence
    }
  }
  return out;
}

/**
 * Checks if a buffer starts with a WAV RIFF header.
 */
function hasWavHeader(buffer) {
  return buffer.length >= 44 && buffer.toString('utf8', 0, 4) === 'RIFF';
}

/**
 * Strips the 44-byte WAV header if present.
 */
function stripWavHeader(buffer) {
  if (hasWavHeader(buffer)) {
    return buffer.subarray(44);
  }
  return buffer;
}

/**
 * Safely downsamples 24kHz PCM to 8kHz μ-law using wavefile resampling
 * which applies proper anti-aliasing filters.
 *
 * @param {Buffer} pcmBuffer - raw PCM16 bytes from Sarvam TTS at 24kHz (WAV or raw)
 * @returns {Buffer}         - μ-law at 8kHz
 */
function pcm24kToMulaw(pcmBuffer) {
  let rawPcm = stripWavHeader(pcmBuffer);
  if (rawPcm.byteLength % 2 !== 0) {
    rawPcm = rawPcm.subarray(0, rawPcm.byteLength - 1);
  }
  let alignedPcm = rawPcm;
  if (rawPcm.byteOffset % 2 !== 0) {
    alignedPcm = Buffer.from(rawPcm);
  }
  
  // Create a WAV in memory so wavefile can resample it
  const wav = new WaveFile();
  // 1 channel, 24000 Hz, 16-bit, samples
  wav.fromScratch(1, 24000, '16', new Int16Array(alignedPcm.buffer, alignedPcm.byteOffset, alignedPcm.byteLength / 2));
  
  // Resample properly to 8kHz (this avoids the aliasing distortion!)
  wav.toSampleRate(8000);
  
  // Extract the new 8kHz samples
  const pcm8k = wav.getSamples(false, Int16Array);
  
  // Encode to μ-law
  const encoded = alawmulaw.mulaw.encode(pcm8k);
  return Buffer.from(encoded.buffer, encoded.byteOffset, encoded.byteLength);
}

/**
 * Convert 8kHz PCM directly to μ-law (if Sarvam outputs 8kHz directly)
 */
function pcm8kToMulaw(pcmBuffer) {
  let rawPcm = stripWavHeader(pcmBuffer);
  if (rawPcm.byteLength % 2 !== 0) {
    rawPcm = rawPcm.subarray(0, rawPcm.byteLength - 1);
  }
  let alignedPcm = rawPcm;
  if (rawPcm.byteOffset % 2 !== 0) {
    alignedPcm = Buffer.from(rawPcm);
  }
  const pcm8k = new Int16Array(alignedPcm.buffer, alignedPcm.byteOffset, alignedPcm.byteLength / 2);
  const encoded = alawmulaw.mulaw.encode(pcm8k);
  return Buffer.from(encoded.buffer, encoded.byteOffset, encoded.byteLength);
}

/**
 * Convert raw μ-law 8kHz Buffer → 16kHz PCM16 WAV Buffer (for ElevenLabs S2S input).
 * ElevenLabs works better with 16kHz+ audio.
 *
 * @param {Buffer} mulawBuffer - raw μ-law bytes from Plivo
 * @returns {Buffer}           - WAV file (PCM16 @ 16kHz, mono)
 */
function mulawToWav16k(mulawBuffer) {
  const uint8 = new Uint8Array(mulawBuffer.buffer, mulawBuffer.byteOffset, mulawBuffer.byteLength);
  const pcm8k = alawmulaw.mulaw.decode(uint8); // Int16Array @ 8kHz
  
  // Upsample 8kHz → 16kHz via wavefile resampler
  const wav = new WaveFile();
  wav.fromScratch(1, 8000, '16', pcm8k);
  wav.toSampleRate(16000);

  return Buffer.from(wav.toBuffer());
}

module.exports = { mulawToPcm16k, mulawToPcm16kRaw, pcm24kToMulaw, pcm8kToMulaw, mulawToWav16k, hasWavHeader, stripWavHeader, applyMulawNoiseGate };
