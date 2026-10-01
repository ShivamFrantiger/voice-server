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

  // Upsample 8kHz → 16kHz via linear interpolation
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
  
  // Create a WAV in memory so wavefile can resample it
  const wav = new WaveFile();
  // 1 channel, 24000 Hz, 16-bit, samples
  wav.fromScratch(1, 24000, '16', new Int16Array(rawPcm.buffer, rawPcm.byteOffset, rawPcm.byteLength / 2));
  
  // Resample properly to 8kHz (this avoids the aliasing distortion!)
  wav.toSampleRate(8000);
  
  // Extract the new 8kHz samples
  const pcm8k = wav.getSamples(false, Int16Array);
  
  // Encode to μ-law
  const encoded = alawmulaw.mulaw.encode(pcm8k);
  return Buffer.from(encoded.buffer);
}

/**
 * Convert 8kHz PCM directly to μ-law (if Sarvam outputs 8kHz directly)
 */
function pcm8kToMulaw(pcmBuffer) {
  const rawPcm = stripWavHeader(pcmBuffer);
  const pcm8k = new Int16Array(rawPcm.buffer, rawPcm.byteOffset, rawPcm.byteLength / 2);
  const encoded = alawmulaw.mulaw.encode(pcm8k);
  return Buffer.from(encoded.buffer);
}

module.exports = { mulawToPcm16k, pcm24kToMulaw, pcm8kToMulaw, hasWavHeader, stripWavHeader };
