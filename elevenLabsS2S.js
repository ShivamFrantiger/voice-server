const WebSocket = require('ws');
const alawmulaw = require('alawmulaw');
const { mulawToPcm16k } = require('./audioUtils');

/**
 * ElevenLabsS2S — Voice Activity Detection + ElevenLabs WebSocket Speech-to-Speech
 *
 * Streams audio continuously to ElevenLabs to achieve near-zero latency.
 *
 * @param {string}   apiKey        - ELEVEN_LABS_API env var
 * @param {string}   voiceId       - ELEVEN_LABS_VOICE_ID env var
 * @param {function} onAudioChunk  - Called with each Buffer of ulaw_8000 audio
 * @param {object}   [options]
 * @param {number}   [options.silenceDurationMs=300]    - ms of silence before pausing stream
 * @param {number}   [options.speechThreshold=200]      - RMS energy above = speech
 * @param {string}   [options.modelId]                  - ElevenLabs STS model
 */
class ElevenLabsS2S {
  constructor(apiKey, voiceId, onAudioChunk, options = {}) {
    this.apiKey       = apiKey;
    this.voiceId      = voiceId;
    this.onAudioChunk = onAudioChunk;

    // VAD config
    this.silenceDurationMs  = options.silenceDurationMs  ?? 300;
    this.speechThreshold    = options.speechThreshold    ?? 200;
    this.modelId            = options.modelId            ?? 'eleven_multilingual_sts_v2';

    // VAD state
    this._isSpeaking     = false;
    this._silenceTimer   = null;

    // WebSocket state
    this.ws = null;
    this._connecting = null;

    console.log(`[S2S] Initialised | voice=${voiceId} | silence=${this.silenceDurationMs}ms`);
    
    // Pre-warm the connection so it's ready for the first word
    this._ensureConnection().catch(err => console.error('[S2S WS] Initial connection failed:', err.message));
  }

  // ─── Connection Management ──────────────────────────────────────────────────

  _connect() {
    return new Promise((resolve, reject) => {
      const url = `wss://api.elevenlabs.io/v1/speech-to-speech/${this.voiceId}/stream-input?model_id=${this.modelId}&output_format=ulaw_8000`;
      this.ws = new WebSocket(url, {
        headers: {
          'xi-api-key': this.apiKey
        }
      });

      this.ws.on('open', () => {
        console.log(`[S2S WS] Connected to ElevenLabs | Voice: ${this.voiceId}`);
        // Send initial config payload
        this.ws.send(JSON.stringify({
          text: " ", // Placeholder required by stream-input schema
          voice_settings: {
            stability: 0.5,
            similarity_boost: 0.8,
            style: 0,
            use_speaker_boost: true
          },
          xi_api_key: this.apiKey
        }));
        resolve();
      });

      this.ws.on('message', (data) => {
        try {
          const msg = JSON.parse(data.toString());
          if (msg.audio) {
            const audioBuf = Buffer.from(msg.audio, 'base64');
            this.onAudioChunk(audioBuf);
          }
          if (msg.error) {
            console.error('[S2S WS] API Error:', msg.error);
          }
        } catch (err) {
          console.error('[S2S WS] Error parsing message:', err.message);
        }
      });

      this.ws.on('error', (err) => {
        console.error('[S2S WS] Socket Error:', err.message);
        reject(err);
      });

      this.ws.on('close', (code) => {
        console.log(`[S2S WS] Connection closed (code=${code})`);
        this.ws = null;
      });
    });
  }

  async _ensureConnection() {
    if (!this.ws || this.ws.readyState === WebSocket.CLOSED) {
      if (!this._connecting) {
        this._connecting = this._connect();
      }
      await this._connecting;
      this._connecting = null;
    } else if (this.ws.readyState === WebSocket.CONNECTING) {
      await this._connecting;
    }
  }

  // ─── Public API ─────────────────────────────────────────────────────────────

  /**
   * Feed one mu-law chunk from Plivo into the VAD/Streamer.
   *
   * @param {Buffer} mulawBuffer
   */
  async addAudio(mulawBuffer) {
    const rms = this._computeRms(mulawBuffer);
    const isSpeech = rms > this.speechThreshold;

    if (isSpeech) {
      // Agent is speaking
      if (this._silenceTimer) {
        clearTimeout(this._silenceTimer);
        this._silenceTimer = null;
      }
      
      if (!this._isSpeaking) {
        console.log('[S2S WS] Speech started — streaming to ElevenLabs');
      }
      this._isSpeaking = true;

      await this._ensureConnection();

      // Convert 8kHz mu-law directly to 16kHz PCM buffer and send
      const pcm16kBuf = mulawToPcm16k(mulawBuffer);
      if (this.ws && this.ws.readyState === WebSocket.OPEN) {
        this.ws.send(JSON.stringify({
          user_audio_chunk: pcm16kBuf.toString('base64')
        }));
      }

    } else {
      // Silence detected
      if (!this._isSpeaking) return; // Ignore pure silence before speech begins

      await this._ensureConnection();

      // Keep sending the trailing silence so the word sounds natural
      const pcm16kBuf = mulawToPcm16k(mulawBuffer);
      if (this.ws && this.ws.readyState === WebSocket.OPEN) {
        this.ws.send(JSON.stringify({
          user_audio_chunk: pcm16kBuf.toString('base64')
        }));
      }

      // Arm silence flush timer
      if (!this._silenceTimer) {
        this._silenceTimer = setTimeout(() => {
          console.log('[S2S WS] Silence detected — paused sending audio');
          this._isSpeaking = false;
          this._silenceTimer = null;
          // We intentionally do NOT send {"user_audio_chunk": ""} because that would 
          // close the WebSocket. We just stop sending chunks to keep it alive for the next sentence.
        }, this.silenceDurationMs);
      }
    }
  }

  /**
   * Cancel any pending timers and close connections. Call when session ends.
   */
  destroy() {
    if (this._silenceTimer) {
      clearTimeout(this._silenceTimer);
      this._silenceTimer = null;
    }
    if (this.ws) {
      try {
        // Send EOS to gracefully close ElevenLabs if we want, or just terminate.
        this.ws.send(JSON.stringify({ user_audio_chunk: "" }));
        this.ws.close();
      } catch (e) {}
      this.ws = null;
    }
    this._isSpeaking = false;
    console.log('[S2S] Destroyed');
  }

  // ─── Helpers ─────────────────────────────────────────────────────────────────

  /**
   * Compute RMS energy of a mu-law buffer by decoding to PCM16 first.
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
