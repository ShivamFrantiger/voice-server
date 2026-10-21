// elevenLabsS2S.js
// ElevenLabs Speech-to-Speech — Optimized HTTP Streaming Pipeline
//
// Architecture improvements over the original batch approach:
//
//  Phase 1 (Latency): Replaced single large-utterance batching with
//    short-segment streaming. Instead of waiting for a full sentence + 500ms
//    silence, we now split on shorter pauses (300ms) AND hard-cap each
//    segment at 4 seconds. Each segment is sent to ElevenLabs as soon as it
//    completes, with responses streaming back immediately. Multiple concurrent
//    requests are allowed (up to MAX_CONCURRENT) so new segments can be sent
//    while older ones are still being played back.
//
//  Phase 3 (VAD): Hangover logic prevents mid-word cuts. Speech threshold
//    lowered to 100 to catch soft phone audio. MinSpeechMs reduced to 100ms.
//
//  Phase 4 (Quality): remove_background_noise=true kept. Voice settings
//    tuned for stability (0.7) and slightly reduced similarity_boost (0.85)
//    to avoid artifacts on telephone-quality input.
//
// Flow:
//   Plivo μ-law chunks → addAudio() → VAD segments (short!) →
//   mulawToWav16k() → POST /v1/speech-to-speech/{voice_id}/stream →
//   ulaw_8000 response chunks → onAudioChunk() → AudioStreamer → Client

const alawmulaw = require("alawmulaw");
const { mulawToWav16k } = require("./audioUtils");

// VAD hangover: require this many consecutive below-threshold frames before
// switching to "silence" mode. At 20ms/frame: 6 frames = 120ms hangover.
// Prevents mid-word cuts on soft consonants and word endings.
const VAD_HANGOVER_FRAMES = 6;

// Max concurrent ElevenLabs API requests (pipeline parallelism).
// Allows sending the next segment while the previous is still streaming back.
const MAX_CONCURRENT = 2;

/**
 * ElevenLabsS2S — Optimized HTTP Speech-to-Speech with segment pipelining
 *
 * @param {string}   apiKey        - ELEVEN_LABS_API env var
 * @param {string}   voiceId       - ELEVEN_LABS_VOICE_ID env var
 * @param {function} onAudioChunk  - Called with each Buffer of μ-law 8kHz audio
 * @param {object}   [options]
 * @param {number}   [options.silenceDurationMs=300]    - ms of silence before flush
 * @param {number}   [options.speechThreshold=100]      - RMS energy above = speech
 * @param {number}   [options.minSpeechMs=100]          - ignore utterances shorter than this
 * @param {number}   [options.maxSpeechMs=4000]         - force-flush after this long
 * @param {string}   [options.modelId]                  - ElevenLabs STS model
 */
class ElevenLabsS2S {
  constructor(apiKey, voiceId, onAudioChunk, options = {}) {
    this.apiKey = apiKey;
    this.voiceId = voiceId;
    this.onAudioChunk = onAudioChunk;

    // VAD config — tuned for telephone-quality audio
    this.silenceDurationMs = options.silenceDurationMs ?? 300;
    this.speechThreshold = options.speechThreshold ?? 100; // was 200
    this.minSpeechMs = options.minSpeechMs ?? 100; // was 250
    this.maxSpeechMs = options.maxSpeechMs ?? 4000; // was 15000 — short segments!
    this.modelId = options.modelId ?? "eleven_multilingual_sts_v2";

    // VAD state
    this._speechBuffers = []; // Array<Buffer> — accumulated μ-law chunks
    this._isSpeaking = false;
    this._speechStart = 0; // Date.now() when speech began
    this._silenceTimer = null;
    this._maxTimer = null;
    this._hangoverCount = 0; // consecutive below-threshold frames

    // Pipeline: ordered queue to keep segments in sequence
    this._queue = Promise.resolve();
    this._activeRequests = 0;

    // Monitoring
    this._totalSegments = 0;
    this._sessionStart = Date.now();

    console.log(
      `[S2S] Initialised | voice=${voiceId} | silence=${this.silenceDurationMs}ms | threshold=${this.speechThreshold} | maxSegMs=${this.maxSpeechMs}`,
    );
  }

  // ─── Public API ─────────────────────────────────────────────────────────────

  /**
   * Feed one μ-law chunk (raw bytes, NOT base64) from Plivo into the VAD.
   * Call this for every 'inbound' media frame from the agent WebSocket.
   *
   * @param {Buffer} mulawBuffer
   */
  addAudio(mulawBuffer) {
    const rms = this._computeRms(mulawBuffer);
    const isSpeech = rms > this.speechThreshold;

    if (isSpeech) {
      this._hangoverCount = 0;
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
    const uptime = ((Date.now() - this._sessionStart) / 1000).toFixed(1);
    console.log(
      `[S2S] Destroyed | uptime=${uptime}s | segments=${this._totalSegments}`,
    );
  }

  // ─── VAD internals ──────────────────────────────────────────────────────────

  _onSpeechChunk(mulawBuffer) {
    // Cancel any pending silence flush — still speaking
    if (this._silenceTimer) {
      clearTimeout(this._silenceTimer);
      this._silenceTimer = null;
    }

    if (!this._isSpeaking) {
      // Transition to SPEAKING
      this._isSpeaking = true;
      this._speechStart = Date.now();
      this._speechBuffers = [];
      console.log("[S2S] VAD: speech started");

      // Hard cap: flush after maxSpeechMs even mid-sentence
      // Short segments (4s max) drastically reduce perceived latency
      this._maxTimer = setTimeout(() => {
        console.log(
          `[S2S] VAD: max segment duration (${this.maxSpeechMs}ms) — force flush`,
        );
        this._flush();
      }, this.maxSpeechMs);
    }

    this._speechBuffers.push(mulawBuffer);
  }

  _onSilenceChunk(mulawBuffer) {
    if (!this._isSpeaking) return;

    // Hangover: keep accumulating for VAD_HANGOVER_FRAMES before treating as silence
    // This prevents choppy cuts on soft consonants / word endings
    this._hangoverCount++;
    if (this._hangoverCount < VAD_HANGOVER_FRAMES) {
      this._speechBuffers.push(mulawBuffer); // keep audio during hangover
      return;
    }

    // Keep a little trailing silence so the utterance sounds natural
    this._speechBuffers.push(mulawBuffer);

    // Arm/reset the silence timer
    if (!this._silenceTimer) {
      this._silenceTimer = setTimeout(() => {
        console.log("[S2S] VAD: silence detected — flushing segment");
        this._flush();
      }, this.silenceDurationMs);
    }
  }

  _flush() {
    this._clearTimers();

    if (!this._isSpeaking || this._speechBuffers.length === 0) return;

    const speechDurationMs = Date.now() - this._speechStart;
    const buffers = this._speechBuffers;

    // Reset VAD state immediately so new audio can accumulate during API call
    this._isSpeaking = false;
    this._speechBuffers = [];
    this._hangoverCount = 0;

    if (speechDurationMs < this.minSpeechMs) {
      console.log(
        `[S2S] VAD: segment too short (${speechDurationMs}ms) — skipping`,
      );
      return;
    }

    this._totalSegments++;
    console.log(
      `[S2S] Flushing segment #${this._totalSegments} (${speechDurationMs}ms, ${buffers.length} chunks)`,
    );

    // Pipeline parallelism: if we're under MAX_CONCURRENT, fire immediately
    // Otherwise, enqueue to preserve ordering
    if (this._activeRequests < MAX_CONCURRENT) {
      // Slot available — run immediately alongside existing request(s)
      this._activeRequests++;
      this._sendToElevenLabs(buffers).finally(() => {
        this._activeRequests--;
      });
    } else {
      // Queue behind existing requests
      this._queue = this._queue.then(() => {
        this._activeRequests++;
        return this._sendToElevenLabs(buffers).finally(() => {
          this._activeRequests--;
        });
      });
    }
  }

  _clearTimers() {
    if (this._silenceTimer) {
      clearTimeout(this._silenceTimer);
      this._silenceTimer = null;
    }
    if (this._maxTimer) {
      clearTimeout(this._maxTimer);
      this._maxTimer = null;
    }
  }

  // ─── ElevenLabs API call ────────────────────────────────────────────────────

  /**
   * Convert μ-law buffers → WAV → POST to ElevenLabs S2S → stream back ulaw_8000.
   * @param {Buffer[]} buffers
   */
  async _sendToElevenLabs(buffers) {
    let combined;
    try {
      combined = Buffer.concat(buffers);
    } catch (err) {
      console.error("[S2S] Buffer concat error:", err.message);
      return;
    }

    // Convert μ-law 8kHz → PCM WAV 16kHz
    let wavBuffer;
    try {
      wavBuffer = mulawToWav16k(combined);
    } catch (err) {
      console.error("[S2S] WAV conversion error:", err.message);
      return;
    }

    const url =
      `https://api.elevenlabs.io/v1/speech-to-speech/${this.voiceId}/stream` +
      `?output_format=ulaw_8000&optimize_streaming_latency=3`;
    //                          ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    // optimize_streaming_latency=3 (0-4):
    //   0 = best quality (default), 4 = lowest latency.
    //   Level 3 gives a good balance — shaves ~200-400ms off API response time.

    const form = new FormData();
    form.append(
      "audio",
      new Blob([wavBuffer], { type: "audio/wav" }),
      "utterance.wav",
    );
    form.append("model_id", this.modelId);
    form.append(
      "voice_settings",
      JSON.stringify({
        stability: 0.7, // More stable → fewer artifacts on telephone audio
        similarity_boost: 1, // Slightly reduced → avoids over-fitting glitches
        style: 0,
        use_speaker_boost: true,
      }),
    );
    form.append("remove_background_noise", "true");

    const t0 = Date.now();
    let response;
    try {
      response = await fetch(url, {
        method: "POST",
        headers: { "xi-api-key": this.apiKey },
        body: form,
      });
    } catch (err) {
      console.error("[S2S] Network error:", err.message);
      return;
    }

    if (!response.ok) {
      let body = "";
      try {
        body = await response.text();
      } catch {}
      console.error(
        `[S2S] API error ${response.status}: ${body.slice(0, 200)}`,
      );
      return;
    }

    const ttfbMs = Date.now() - t0;
    console.log(`[S2S] API TTFB: ${ttfbMs}ms`);

    // Stream raw ulaw_8000 chunks back to the caller
    let bytesReceived = 0;
    try {
      for await (const chunk of response.body) {
        const buf = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
        bytesReceived += buf.length;
        this.onAudioChunk(buf);
      }
      console.log(
        `[S2S] Segment #${this._totalSegments} done | ${bytesReceived} bytes | TTFB ${ttfbMs}ms | total time ${Date.now() - t0}ms`,
      );
    } catch (err) {
      if (
        err.code === "ECONNRESET" ||
        err.message?.includes("aborted") ||
        err.message?.includes("wsarecv")
      ) {
        console.warn("[S2S] Stream aborted (network):", err.message);
      } else {
        console.error("[S2S] Stream error:", err.message);
      }
    }
  }

  // ─── Helpers ─────────────────────────────────────────────────────────────────

  /**
   * Compute RMS energy of a μ-law buffer by decoding to PCM16 first.
   * Cheap enough to run on every 20ms chunk.
   *
   * @param {Buffer} mulawBuffer
   * @returns {number} RMS value (0–32767)
   */
  _computeRms(mulawBuffer) {
    if (mulawBuffer.length === 0) return 0;
    try {
      const uint8 = new Uint8Array(
        mulawBuffer.buffer,
        mulawBuffer.byteOffset,
        mulawBuffer.byteLength,
      );
      const pcm = alawmulaw.mulaw.decode(uint8); // Int16Array
      let sumSq = 0;
      for (let i = 0; i < pcm.length; i++) sumSq += pcm[i] * pcm[i];
      return Math.sqrt(sumSq / pcm.length);
    } catch {
      return 0;
    }
  }
}

module.exports = { ElevenLabsS2S };
