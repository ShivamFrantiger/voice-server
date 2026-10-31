// audioStreamer.js — Adaptive Drift-Compensated Real-Time Audio Engine
//
// Key Features:
//   • High-Precision Self-Correcting Timer: eliminates Node.js setInterval drift
//     using performance.now() compensation (cadence locked to 40ms / 8000 B/s).
//   • Zero-Truncation Delivery: preserves 100% of the speech audio from the very
//     first syllable without dropping or slicing the beginning of sentences.
//   • High-Capacity Buffer: supports natural long sentences (up to 40 seconds)
//     without clipping or dropping bytes.
//   • Plivo clearAudio Support: flushes Plivo's hardware buffer on barge-in / reset.

class AudioStreamer {
  constructor(ws, sessionId, label, options = {}) {
    this.ws = ws;
    this.sessionId = sessionId;
    this.label = label || "audio";
    this.queue = Buffer.alloc(0);
    this.timer = null;
    this.chunkSize = 320; // 40ms @ 8kHz μ-law (320 bytes)
    this.interval = 40;  // 40ms send cadence = exactly 8000 bytes/sec
    const preMs = options.preBufferMs ?? (parseInt(process.env.AUDIO_STREAMER_PREBUFFER_MS || "40", 10));
    this.preBuffer = Math.max(320, Math.round((preMs / 1000) * 8000));
    // Generous 40-second safety limit to protect against orphaned connections
    // while NEVER dropping legitimate long sentences or phrase beginnings.
    this.maxQueueBytes = options.maxQueueBytes || 320000;

    this._isStreaming = false;
    this._idleCount = 0;
    this._expectedTime = 0;
    this._totalBytesSent = 0;
    this._framesSent = 0;
  }

  addAudio(mulawBuffer) {
    if (!mulawBuffer || mulawBuffer.length === 0) return;

    // Append incoming modulated audio to playback queue
    this.queue = Buffer.concat([this.queue, mulawBuffer]);

    // Safety guard against extreme runaway memory leaks (only if > 40 seconds)
    if (this.queue.length > this.maxQueueBytes) {
      console.warn(
        `[AudioStreamer:${this.label}] Extreme queue overflow (${this.queue.length}B > ${this.maxQueueBytes}B) — capping to prevent memory leak.`,
      );
      this.queue = this.queue.subarray(this.queue.length - this.maxQueueBytes);
    }

    // Start streaming immediately once we have at least one frame of audio
    if (!this._isStreaming && this.queue.length >= this.preBuffer) {
      this._startStreaming();
    }
  }

  _startStreaming() {
    if (this._isStreaming) return;
    this._isStreaming = true;
    this._idleCount = 0;
    this._expectedTime = performance.now() + this.interval;

    if (this.timer) clearTimeout(this.timer);
    this.timer = setTimeout(() => this._tick(), this.interval);
  }

  _tick() {
    if (!this._isStreaming) return;

    if (this.queue.length >= this.chunkSize) {
      // Send standard 40ms frame (320 bytes) at exact 1x real-time speed
      const chunk = this.queue.subarray(0, this.chunkSize);
      this.queue = this.queue.subarray(this.chunkSize);
      this._sendChunk(chunk);
      this._idleCount = 0;
    } else if (this.queue.length > 0) {
      // Partial trailing frame (< 320 bytes).
      // Keep partial bytes so next incoming audio chunk appends seamlessly.
      this._idleCount++;
      // If no new audio arrives for 200ms (5 ticks), the utterance has finished;
      // pad trailing bytes with silence and complete the stream.
      if (this._idleCount >= 5) {
        const padded = Buffer.alloc(this.chunkSize, 255); // 255 = standard μ-law silence
        this.queue.copy(padded);
        this.queue = Buffer.alloc(0);
        this._sendChunk(padded);
        this._stopStreaming();
        return;
      }
    } else {
      // Buffer is completely empty
      this._idleCount++;
      // If queue remains empty for 3 ticks (120ms), sleep until next audio arrives
      if (this._idleCount >= 3) {
        this._stopStreaming();
        return;
      }
    }

    // High-Precision Self-Correcting Schedule
    const now = performance.now();
    const drift = now - this._expectedTime;
    this._expectedTime += this.interval;
    const nextDelay = Math.max(0, this.interval - drift);

    this.timer = setTimeout(() => this._tick(), nextDelay);
  }

  _sendChunk(chunk) {
    if (this.ws && this.ws.readyState === 1) {
      this.ws.send(
        JSON.stringify({
          event: "playAudio",
          media: {
            contentType: "audio/x-mulaw",
            sampleRate: 8000,
            payload: chunk.toString("base64"),
          },
        }),
      );
      this._totalBytesSent += chunk.length;
      this._framesSent++;
    }
  }

  _stopStreaming() {
    if (this.timer) {
      clearTimeout(this.timer);
      this.timer = null;
    }
    this._isStreaming = false;
    this._idleCount = 0;
  }

  stop() {
    this._stopStreaming();
    this.queue = Buffer.alloc(0);
  }

  /**
   * Barge-in / Turn Reset:
   * Instantly purges local buffer and instructs Plivo to dump its audio buffer.
   */
  clear() {
    this.stop();
    if (this.ws && this.ws.readyState === 1) {
      try {
        this.ws.send(JSON.stringify({ event: "clearAudio" }));
        console.log(`[AudioStreamer:${this.label}] Sent clearAudio to Plivo`);
      } catch (err) {
        console.warn(`[AudioStreamer:${this.label}] clearAudio error:`, err.message);
      }
    }
  }
}

module.exports = { AudioStreamer };
