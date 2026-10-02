// elevenLabsS2S.js
// ElevenLabs Speech-to-Speech processor
// Replaces sarvamSTT.js + sarvamTTS.js with a single, simpler pipeline.
//
// Flow:
//   Agent mu-law chunks → VAD buffer → silence detected →
//   mulawToWav16k() → POST /v1/speech-to-speech/{voice_id}/stream →
//   ulaw_8000 response chunks → onAudioChunk() → AudioStreamer → Customer

const alawmulaw = require('alawmulaw');
const { mulawToWav16k } = require('./audioUtils');

/**
 * ElevenLabsS2S — Voice Activity Detection + ElevenLabs Speech-to-Speech
 *
 * @param {string}   apiKey        - ELEVEN_LABS_API env var
 * @param {string}   voiceId       - ELEVEN_LABS_VOICE_ID env var
 * @param {function} onAudioChunk  - Called with each Buffer of ulaw_8000 audio
 * @param {object}   [options]
 * @param {number}   [options.silenceDurationMs=300]    - ms of silence before flush
 * @param {number}   [options.speechThreshold=200]      - RMS energy above = speech
 * @param {number}   [options.minSpeechMs=250]          - ignore utterances shorter than this
 * @param {number}   [options.maxSpeechMs=15000]        - force-flush after this long
 * @param {string}   [options.modelId]                  - ElevenLabs STS model
 */
class ElevenLabsS2S {
  constructor(apiKey, voiceId, onAudioChunk, options = {}) {
    this.apiKey       = apiKey;
    this.voiceId      = voiceId;
    this.onAudioChunk = onAudioChunk;

    // VAD config (Latency optimized to 300ms)
    this.silenceDurationMs  = options.silenceDurationMs  ?? 300;
    this.speechThreshold    = options.speechThreshold    ?? 200;
    this.minSpeechMs        = options.minSpeechMs        ?? 250;
    this.maxSpeechMs        = options.maxSpeechMs        ?? 15000;
    this.modelId            = options.modelId            ?? 'eleven_multilingual_sts_v2';

    // VAD state
    this._speechBuffers  = [];   // Array<Buffer> — accumulated mu-law chunks
    this._isSpeaking     = false;
    this._speechStart    = 0;    // Date.now() when speech began
    this._silenceTimer   = null;
    this._maxTimer       = null;

    // Sequential processing queue (prevents out-of-order audio)
    this._queue = Promise.resolve();

    console.log(`[S2S] Initialised | voice=${voiceId} | silence=${this.silenceDurationMs}ms`);
  }

  // ─── Public API ─────────────────────────────────────────────────────────────

  /**
   * Feed one mu-law chunk (raw bytes, NOT base64) from Plivo into the VAD.
   * Call this for every 'inbound' media frame from the agent WebSocket.
   *
   * @param {Buffer} mulawBuffer
   */
  async addAudio(mulawBuffer) {
    const rms = this._computeRms(mulawBuffer);
    const isSpeech = rms > this.speechThreshold;

    if (isSpeech) {
      this._onSpeechChunk(mulawBuffer);
    } else {
      this._onSilenceChunk(mulawBuffer);
    }
  }

  /**
   * Cancel any pending timers. Call when session ends.
   */
  destroy() {
    this._clearTimers();
    this._speechBuffers = [];
    this._isSpeaking = false;
    console.log('[S2S] Destroyed');
  }

  // ─── VAD internals ──────────────────────────────────────────────────────────

  _onSpeechChunk(mulawBuffer) {
    // Cancel any pending silence flush — agent is still speaking
    if (this._silenceTimer) {
      clearTimeout(this._silenceTimer);
      this._silenceTimer = null;
    }

    if (!this._isSpeaking) {
      // Transition to SPEAKING
      this._isSpeaking  = true;
      this._speechStart = Date.now();
      this._speechBuffers = [];
      console.log('[S2S] VAD: speech started');

      // Safety: force flush after maxSpeechMs
      this._maxTimer = setTimeout(() => {
        console.log('[S2S] VAD: max speech duration reached — force flush');
        this._flush();
      }, this.maxSpeechMs);
    }

    this._speechBuffers.push(mulawBuffer);
  }

  _onSilenceChunk(mulawBuffer) {
    if (!this._isSpeaking) return; // nothing to do during leading silence

    // Keep a little trailing silence so the utterance sounds natural
    this._speechBuffers.push(mulawBuffer);

    // Arm/reset the silence timer
    if (!this._silenceTimer) {
      this._silenceTimer = setTimeout(() => {
        console.log('[S2S] VAD: silence detected — flushing utterance');
        this._flush();
      }, this.silenceDurationMs);
    }
  }

  _flush() {
    this._clearTimers();

    if (!this._isSpeaking || this._speechBuffers.length === 0) return;

    const speechDurationMs = Date.now() - this._speechStart;
    const buffers = this._speechBuffers;

    // Reset VAD state immediately so new audio can accumulate
    this._isSpeaking    = false;
    this._speechBuffers = [];

    if (speechDurationMs < this.minSpeechMs) {
      console.log(`[S2S] VAD: utterance too short (${speechDurationMs}ms) — skipping`);
      return;
    }

    console.log(`[S2S] Flushing utterance (${speechDurationMs}ms, ${buffers.length} chunks)`);

    // Enqueue — ensures responses arrive in order even if API is slow
    this._queue = this._queue.then(() => this._sendToElevenLabs(buffers));
  }

  _clearTimers() {
    if (this._silenceTimer) { clearTimeout(this._silenceTimer); this._silenceTimer = null; }
    if (this._maxTimer)     { clearTimeout(this._maxTimer);     this._maxTimer     = null; }
  }

  // ─── ElevenLabs API call ────────────────────────────────────────────────────

  /**
   * Convert mu-law buffers → WAV → POST to ElevenLabs S2S → stream back ulaw_8000.
   * @param {Buffer[]} buffers
   */
  async _sendToElevenLabs(buffers) {
    let combined;
    try {
      combined = Buffer.concat(buffers);
    } catch (err) {
      console.error('[S2S] Buffer concat error:', err.message);
      return;
    }

    // Convert mu-law 8kHz → PCM WAV 16kHz (ElevenLabs works better with 16kHz)
    let wavBuffer;
    try {
      wavBuffer = mulawToWav16k(combined);
    } catch (err) {
      console.error('[S2S] WAV conversion error:', err.message);
      return;
    }

    const url =
      `https://api.elevenlabs.io/v1/speech-to-speech/${this.voiceId}/stream` +
      `?output_format=ulaw_8000`;

    // Use built-in FormData + Blob (Node 18+, guaranteed by Express v5)
    const form = new FormData();
    form.append('audio', new Blob([wavBuffer], { type: 'audio/wav' }), 'utterance.wav');
    form.append('model_id', this.modelId);
    form.append(
      'voice_settings',
      JSON.stringify({ stability: 0.5, similarity_boost: 0.8, style: 0, use_speaker_boost: true }),
    );

    let response;
    try {
      response = await fetch(url, {
        method: 'POST',
        headers: { 'xi-api-key': this.apiKey },
        body: form,
      });
    } catch (err) {
      console.error('[S2S] Network error:', err.message);
      return;
    }

    if (!response.ok) {
      let body = '';
      try { body = await response.text(); } catch {}
      console.error(`[S2S] API error ${response.status}: ${body.slice(0, 200)}`);
      return;
    }

    // Stream raw ulaw_8000 chunks back to the caller
    let bytesReceived = 0;
    try {
      for await (const chunk of response.body) {
        const buf = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
        bytesReceived += buf.length;
        this.onAudioChunk(buf);
      }
      console.log(`[S2S] Streamed ${bytesReceived} bytes of ulaw_8000 to customer`);
    } catch (err) {
      if (
        err.code === 'ECONNRESET' ||
        err.message?.includes('aborted') ||
        err.message?.includes('wsarecv')
      ) {
        console.warn('[S2S] Stream aborted (network):', err.message);
      } else {
        console.error('[S2S] Stream error:', err.message);
      }
    }
  }

  // ─── Helpers ─────────────────────────────────────────────────────────────────

  /**
   * Compute RMS energy of a mu-law buffer by decoding to PCM16 first.
   * This is cheap enough to run on every 20ms chunk.
   *
   * @param {Buffer} mulawBuffer
   * @returns {number} RMS value (0–32767)
   */
  _computeRms(mulawBuffer) {
    if (mulawBuffer.length === 0) return 0;
    try {
      const uint8  = new Uint8Array(mulawBuffer.buffer, mulawBuffer.byteOffset, mulawBuffer.byteLength);
      const pcm    = alawmulaw.mulaw.decode(uint8); // Int16Array
      let sumSq = 0;
      for (let i = 0; i < pcm.length; i++) sumSq += pcm[i] * pcm[i];
      return Math.sqrt(sumSq / pcm.length);
    } catch {
      return 0;
    }
  }
}

module.exports = { ElevenLabsS2S };
