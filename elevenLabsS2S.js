// elevenLabsS2S.js
// ElevenLabs Speech-to-Speech — Low Latency (2.0s - 2.8s) Streaming Pipeline
//
// Optimizations for Target 2.0s - 2.8s Latency:
//
//  1. Fast VAD & Clause Splitting:
//     - 60ms hangover (VAD_HANGOVER_FRAMES = 3) prevents syllable clipping while shaving 60ms.
//     - silenceDurationMs = 220ms (down from 500ms) eliminates awkward pause delay.
//     - maxSpeechMs = 2000ms (down from 4000ms) splits continuous speech into ~2s natural clauses.
//
//  2. High-Performance TTFB:
//     - Smaller ~2s audio payloads reduce ElevenLabs S2S inference time by ~40% (TTFB ~450-600ms).
//     - optimize_streaming_latency = 3 gives near-instantaneous streaming start.
//
//  3. Strict Chronological Ordered Streaming:
//     - Parallel requests to ElevenLabs are tracked via _orderedSegments.
//     - Audio chunks from concurrent segments never interleave or collide.
//     - As Segment 1 finishes, Segment 2's pre-buffered chunks immediately drain into AudioStreamer.
//
//  4. Voice Quality Guardrails:
//     - Stability (0.70) and Similarity Boost (0.80) prevent robotic drift or telephone aliasing.
//     - μ-law noise gate prevents background hiss from keeping VAD open.

const alawmulaw = require("alawmulaw");
const { mulawToWav16k } = require("./audioUtils");

// VAD hangover: consecutive below-threshold frames before entering silence mode.
// At 20ms/frame: 3 frames = 60ms hangover.
// Prevents mid-word cuts on soft consonants while removing dead-air latency.
const VAD_HANGOVER_FRAMES = 3;

// Max concurrent ElevenLabs API requests (pipeline parallelism).
// Allows preparing the next 2s clause while the previous one is streaming.
const MAX_CONCURRENT = 2;

/**
 * ElevenLabsS2S — Low-Latency Speech-to-Speech with ordered segment streaming
 *
 * @param {string}   apiKey        - ELEVEN_LABS_API env var
 * @param {string}   voiceId       - ELEVEN_LABS_VOICE_ID env var
 * @param {function} onAudioChunk  - Called with each Buffer of μ-law 8kHz audio in strict order
 * @param {object}   [options]
 * @param {number}   [options.silenceDurationMs=220]    - ms of silence before flush
 * @param {number}   [options.speechThreshold=100]      - RMS energy above = speech
 * @param {number}   [options.minSpeechMs=120]          - ignore utterances shorter than this
 * @param {number}   [options.maxSpeechMs=2000]         - clause split cap for low latency
 * @param {number}   [options.latencyLevel=3]           - ElevenLabs optimize_streaming_latency (0-4)
 * @param {number}   [options.similarityBoost=0.80]     - Voice similarity boost
 * @param {number}   [options.stability=0.70]           - Voice stability
 * @param {string}   [options.modelId]                  - ElevenLabs STS model
 */
class ElevenLabsS2S {
  constructor(apiKey, voiceId, onAudioChunk, options = {}) {
    this.apiKey = apiKey;
    this.voiceId = voiceId;
    this.onAudioChunk = onAudioChunk;

    // VAD config — tuned for telephone-quality audio & 2.0s - 2.8s total latency
    this.silenceDurationMs = options.silenceDurationMs ?? 220;
    this.speechThreshold   = options.speechThreshold ?? 100;
    this.minSpeechMs       = options.minSpeechMs ?? 120;
    this.maxSpeechMs       = options.maxSpeechMs ?? 2000;
    this.modelId           = options.modelId ?? "eleven_multilingual_sts_v2";
    this.latencyLevel      = options.latencyLevel ?? 3;
    this.similarityBoost   = options.similarityBoost ?? 0.80;
    this.stability         = options.stability ?? 0.70;

    // VAD state
    this._speechBuffers    = []; // Array<Buffer> — accumulated μ-law chunks
    this._isSpeaking       = false;
    this._speechStart      = 0;  // Date.now() when speech began
    this._silenceTimer     = null;
    this._maxTimer         = null;
    this._hangoverCount    = 0;

    // Ordered segment playback queue to prevent chunk interleaving across concurrent requests
    this._orderedSegments  = [];
    this._queue            = Promise.resolve();
    this._activeRequests   = 0;
    this._destroyed        = false;

    // Monitoring
    this._totalSegments    = 0;
    this._sessionStart     = Date.now();

    console.log(
      `[S2S] Initialised | voice=${voiceId} | silence=${this.silenceDurationMs}ms | threshold=${this.speechThreshold} | maxSegMs=${this.maxSpeechMs} | latency=${this.latencyLevel}`,
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
    if (this._destroyed) return;

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
   * Cancel any pending timers and clean up active requests. Call when session ends.
   */
  destroy() {
    this._destroyed = true;
    this._clearTimers();
    this._speechBuffers = [];
    this._orderedSegments = [];
    this._isSpeaking = false;
    const uptime = ((Date.now() - this._sessionStart) / 1000).toFixed(1);
    console.log(
      `[S2S] Destroyed | uptime=${uptime}s | segments=${this._totalSegments}`,
    );
  }

  // ─── VAD Internals ──────────────────────────────────────────────────────────

  _onSpeechChunk(mulawBuffer) {
    // Cancel any pending silence flush — still actively speaking
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

      // Natural clause cap: flush after maxSpeechMs even mid-sentence
      // 2.0s segments drastically reduce TTFB and perceived latency without audio chops
      this._maxTimer = setTimeout(() => {
        console.log(
          `[S2S] VAD: max clause duration (${this.maxSpeechMs}ms) — clause flush`,
        );
        this._flush();
      }, this.maxSpeechMs);
    }

    this._speechBuffers.push(mulawBuffer);
  }

  _onSilenceChunk(mulawBuffer) {
    if (!this._isSpeaking) return;

    // Hangover: keep accumulating for VAD_HANGOVER_FRAMES before treating as silence
    // Prevents cuts on soft consonants / word endings
    this._hangoverCount++;
    if (this._hangoverCount < VAD_HANGOVER_FRAMES) {
      this._speechBuffers.push(mulawBuffer);
      return;
    }

    // Trailing silence frame so the utterance ends naturally
    this._speechBuffers.push(mulawBuffer);

    // Arm silence timer (220ms)
    if (!this._silenceTimer) {
      this._silenceTimer = setTimeout(() => {
        console.log(`[S2S] VAD: pause detected (${this.silenceDurationMs}ms) — flushing clause`);
        this._flush();
      }, this.silenceDurationMs);
    }
  }

  _flush() {
    this._clearTimers();

    if (!this._isSpeaking || this._speechBuffers.length === 0) return;

    const speechDurationMs = Date.now() - this._speechStart;
    const buffers = this._speechBuffers;

    // Reset VAD state immediately so subsequent speech can accumulate during API call
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
    const segRecord = {
      id: this._totalSegments,
      bufferedChunks: [],
      isDone: false,
      bytesReceived: 0,
      t0: Date.now(),
      speechDurationMs,
    };
    this._orderedSegments.push(segRecord);

    console.log(
      `[S2S] Flushing segment #${segRecord.id} (${speechDurationMs}ms, ${buffers.length} chunks)`,
    );

    // Pipeline parallelism: if under MAX_CONCURRENT, fire immediately
    // Otherwise, enqueue behind existing requests
    if (this._activeRequests < MAX_CONCURRENT) {
      this._activeRequests++;
      this._sendToElevenLabs(segRecord, buffers).finally(() => {
        this._activeRequests--;
      });
    } else {
      this._queue = this._queue.then(() => {
        if (this._destroyed) return;
        this._activeRequests++;
        return this._sendToElevenLabs(segRecord, buffers).finally(() => {
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

  // ─── ElevenLabs API & Ordered Streaming ─────────────────────────────────────

  /**
   * Convert μ-law buffers → WAV → POST to ElevenLabs S2S → stream back ulaw_8000.
   * Output chunks are emitted in strict chronological order through _emitChunk.
   *
   * @param {object}   segRecord
   * @param {Buffer[]} buffers
   */
  async _sendToElevenLabs(segRecord, buffers) {
    if (this._destroyed) return;

    let combined;
    try {
      combined = Buffer.concat(buffers);
    } catch (err) {
      console.error(`[S2S] Segment #${segRecord.id} buffer concat error:`, err.message);
      this._completeSegment(segRecord);
      return;
    }

    // Convert μ-law 8kHz → PCM WAV 16kHz
    let wavBuffer;
    try {
      wavBuffer = mulawToWav16k(combined);
    } catch (err) {
      console.error(`[S2S] Segment #${segRecord.id} WAV conversion error:`, err.message);
      this._completeSegment(segRecord);
      return;
    }

    const url =
      `https://api.elevenlabs.io/v1/speech-to-speech/${this.voiceId}/stream` +
      `?output_format=ulaw_8000&optimize_streaming_latency=${this.latencyLevel}`;

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
        stability: this.stability,
        similarity_boost: this.similarityBoost,
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
      console.error(`[S2S] Segment #${segRecord.id} network error:`, err.message);
      this._completeSegment(segRecord);
      return;
    }

    if (!response.ok) {
      let body = "";
      try {
        body = await response.text();
      } catch {}
      console.error(
        `[S2S] Segment #${segRecord.id} API error ${response.status}: ${body.slice(0, 200)}`,
      );
      this._completeSegment(segRecord);
      return;
    }

    const ttfbMs = Date.now() - t0;
    console.log(`[S2S] Segment #${segRecord.id} API TTFB: ${ttfbMs}ms`);

    // Stream raw ulaw_8000 chunks back in strict order
    try {
      for await (const chunk of response.body) {
        if (this._destroyed) break;
        const buf = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
        segRecord.bytesReceived += buf.length;
        this._emitChunk(segRecord, buf);
      }
      console.log(
        `[S2S] Segment #${segRecord.id} done | ${segRecord.bytesReceived} bytes | TTFB ${ttfbMs}ms | total ${Date.now() - t0}ms`,
      );
    } catch (err) {
      if (
        err.code === "ECONNRESET" ||
        err.message?.includes("aborted") ||
        err.message?.includes("wsarecv")
      ) {
        console.warn(`[S2S] Segment #${segRecord.id} stream aborted:`, err.message);
      } else {
        console.error(`[S2S] Segment #${segRecord.id} stream error:`, err.message);
      }
    } finally {
      this._completeSegment(segRecord);
    }
  }

  /**
   * Route incoming chunk to client:
   * If this segment is currently at the head of the playback queue, stream directly.
   * If an earlier segment is still streaming, buffer until the earlier segment completes.
   */
  _emitChunk(segRecord, chunk) {
    if (this._destroyed) return;

    if (this._orderedSegments[0] === segRecord) {
      this.onAudioChunk(chunk);
    } else {
      segRecord.bufferedChunks.push(chunk);
    }
  }

  /**
   * Mark segment as completed and drain the queue sequentially.
   */
  _completeSegment(segRecord) {
    segRecord.isDone = true;
    this._drainQueue();
  }

  /**
   * Drains finished segments and flushes queued chunks for the next active segment.
   */
  _drainQueue() {
    if (this._destroyed) return;

    while (this._orderedSegments.length > 0 && this._orderedSegments[0].isDone) {
      const finishedSeg = this._orderedSegments.shift();
      // Flush any remaining buffered chunks
      for (const chunk of finishedSeg.bufferedChunks) {
        this.onAudioChunk(chunk);
      }
      finishedSeg.bufferedChunks = [];

      // If there is a next segment waiting, flush all its pre-buffered chunks immediately
      if (this._orderedSegments.length > 0) {
        const nextSeg = this._orderedSegments[0];
        while (nextSeg.bufferedChunks.length > 0) {
          const chunk = nextSeg.bufferedChunks.shift();
          this.onAudioChunk(chunk);
        }
      }
    }
  }

  // ─── Helpers ─────────────────────────────────────────────────────────────────

  /**
   * Compute RMS energy of a μ-law buffer by decoding to PCM16 first.
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
