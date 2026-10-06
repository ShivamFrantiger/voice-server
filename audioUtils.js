const alawmulaw = require('alawmulaw');
const { WaveFile } = require('wavefile');

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

module.exports = { mulawToPcm16k, pcm24kToMulaw, pcm8kToMulaw, mulawToWav16k, hasWavHeader, stripWavHeader };
