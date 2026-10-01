// audioUtils.js
// Converts between Plivo's 8kHz μ-law and Sarvam's 16kHz PCM16.
// Uses the `alawmulaw` npm package for G.711 codec support.

const alawmulaw = require('alawmulaw');

/**
 * Decode μ-law 8kHz Buffer → PCM16 16kHz Buffer (for Sarvam STT).
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
 * Encode PCM16 16kHz Buffer → μ-law 8kHz Buffer (Sarvam TTS output → Plivo).
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
 * Encode PCM16 8kHz Buffer → μ-law 8kHz Buffer (Sarvam TTS output → Plivo).
 *
 * @param {Buffer} pcmBuffer - raw PCM16 bytes from Sarvam TTS at 8kHz
 * @returns {Buffer}         - μ-law at 8kHz
 */
function pcm8kToMulaw(pcmBuffer) {
  const pcm8k = new Int16Array(pcmBuffer.buffer, pcmBuffer.byteOffset, pcmBuffer.byteLength / 2);
  const encoded = alawmulaw.mulaw.encode(pcm8k);
  return Buffer.from(encoded.buffer);
}

module.exports = { mulawToPcm16k, pcm16kToMulaw, pcm8kToMulaw };
