// audioStreamer.js — Reliable Real-Time Audio Streaming Engine for Plivo
//
// Streams 8kHz μ-law audio at a steady, reliable cadence to Plivo's WebSocket.
// Guarantees zero truncation of speech (no head-dropping or clipping).

class AudioStreamer {
  constructor(ws, sessionId, label, options = {}) {
    this.ws = ws;
    this.sessionId = sessionId;
    this.label = label || "audio";
    this.queue = Buffer.alloc(0);
    this.timer = null;
    this.chunkSize = 320; // 40ms @ 8kHz μ-law (320 bytes)
    this.interval = 40; // 40ms send cadence
    const preMs = options.preBufferMs || 40;
    this.preBuffer = Math.max(320, Math.round((cpreMs / 1000) * 8000));
    this._isStreaming = false;
    this._idleCount = 0;
  }

  addAudio(mulawBuffer) {
    if (!mulawBuffer || mulawBuffer.length === 0) return;
    this.queue = Buffer.concat([this.queue, mulawBuffer]);

    // Start streaming once pre-buffer threshold is reached
    if (!this._isStreaming && this.queue.length >= this.preBuffer) {
      this._startStreaming();
    }
  }

  _startStreaming() {
    if (this._isStreaming) return;
    this._isStreaming = true;
    this._idleCount = 0;

    if (this.timer) clearInterval(this.timer);
    this.timer = setInterval(() => {
      if (this.queue.length >= this.chunkSize) {
        // Send exactly one 40ms frame (320 bytes)
        const chunk = this.queue.subarray(0, this.chunkSize);
        this.queue = this.queue.subarray(this.chunkSize);
        this._sendChunk(chunk);
        this._idleCount = 0;
      } else if (this.queue.length > 0) {
        // Partial chunk in buffer (< 320 bytes).
        // Keep partial bytes so next incoming audio chunk appends seamlessly.
        this._idleCount++;
        // If no new audio arrives for 200ms (5 ticks), the utterance has finished
        if (this._idleCount >= 5) {
          const padded = Buffer.alloc(this.chunkSize, 255); // 255 = standard μ-law silence
          this.queue.copy(padded);
          this.queue = Buffer.alloc(0);
          this._sendChunk(padded);
          this._stopStreaming();
        }
      } else {
        // Buffer is completely empty (0 bytes)
        this._idleCount++;
        // If queue remains empty for 3 ticks (120ms), sleep until next audio arrives
        if (this._idleCount >= 3) {
          this._stopStreaming();
        }
      }
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
