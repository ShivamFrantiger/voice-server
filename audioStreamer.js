// audioStreamer.js — Reliable Real-Time Audio Streaming Engine for Plivo
//
// Streams 8kHz μ-law audio at a steady, reliable cadence to Plivo's WebSocket.
// Guarantees zero truncation of speech (no head-dropping or clipping).
//
// [FIX 2] Pre-buffer: 1 frame (320 bytes = 40ms) — already at minimum via AUDIO_STREAMER_PREBUFFER_MS=40
// [FIX 3] Idle thresholds increased to prevent AudioStreamer stopping between consecutive
//         ElevenLabs segments. At 120ms idle stop, a ~200ms inter-segment gap caused
//         AudioStreamer to stop + restart, re-applying the pre-buffer delay every time.
//         Now stops after 500ms empty (12 ticks) so consecutive segments flow uninterrupted.

class AudioStreamer {
  constructor(ws, sessionId, label, options = {}) {
    this.ws = ws;
    this.sessionId = sessionId;
    this.label = label || "audio";
    this.queue = Buffer.alloc(0);
    this.timer = null;
    this.chunkSize = 320; // 40ms @ 8kHz μ-law (320 bytes)
    this.interval = 40;  // 40ms send cadence
    const preMs = options.preBufferMs || 40;
    this.preBuffer = Math.max(320, Math.round((preMs / 1000) * 8000));
    this._isStreaming = false;
    this._idleCount = 0;
  }

  addAudio(mulawBuffer) {
    if (!mulawBuffer || mulawBuffer.length === 0) return;
    this.queue = Buffer.concat([this.queue, mulawBuffer]);

    // Start streaming once pre-buffer threshold is reached (1 frame = 320 bytes)
    if (!this._isStreaming && this.queue.length >= this.preBuffer) {
      this._startStreaming();
    }
  }

  _sendNextChunk() {
    if (this.queue.length >= this.chunkSize) {
      // Send exactly one 40ms frame (320 bytes)
      const chunk = this.queue.subarray(0, this.chunkSize);
      this.queue = this.queue.subarray(this.chunkSize);
      this._sendChunk(chunk);
      this._idleCount = 0;
    } else if (this.queue.length > 0) {
      // Partial chunk — wait up to 400ms (10 ticks) for more audio before padding+flushing.
      this._idleCount++;
      if (this._idleCount >= 10) {
        const padded = Buffer.alloc(this.chunkSize, 255); // 255 = μ-law silence
        this.queue.copy(padded);
        this.queue = Buffer.alloc(0);
        this._sendChunk(padded);
        this._stopStreaming();
      }
    } else {
      // Buffer is completely empty — wait up to 500ms (12 ticks) before sleeping.
      this._idleCount++;
      if (this._idleCount >= 12) {
        this._stopStreaming();
      }
    }
  }

  _startStreaming() {
    if (this._isStreaming) return;
    this._isStreaming = true;
    this._idleCount = 0;

    // [FIX 7] Immediate first frame dispatch: sends frame 0 at t=0ms instead of waiting
    // 40ms for the first setInterval tick. Shaves 40ms of latency off every utterance start!
    this._sendNextChunk();

    if (this.timer) clearInterval(this.timer);
    this.timer = setInterval(() => {
      this._sendNextChunk();
    }, this.interval);
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
    }
  }

  _stopStreaming() {
    if (this.timer) clearInterval(this.timer);
    this.timer = null;
    this._isStreaming = false;
    this._idleCount = 0;
  }

  stop() {
    this._stopStreaming();
    this.queue = Buffer.alloc(0);
  }

  clear() {
    this.stop();
    if (this.ws && this.ws.readyState === 1) {
      try {
        this.ws.send(JSON.stringify({ event: "clearAudio" }));
      } catch (_) {}
    }
  }
}

module.exports = { AudioStreamer };
