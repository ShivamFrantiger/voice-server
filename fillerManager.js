// fillerManager.js
// Dynamic latency-masking manager for Guruji voice fillers.
// Caches pre-generated 8kHz u-law fillers in memory and schedules smooth playback
// when voice modulation latency exceeds 1.0 second.

const fs = require("fs");
const path = require("path");

class FillerManager {
  constructor() {
    this.fillersMap = new Map(); // id -> Buffer (raw u-law 8kHz)
    this.fillerKeys = [];        // Array of IDs
    this.deck       = [];        // Shuffled deck of IDs
    this.lastFiller = null;      // Last played filler ID across all calls

    // Per-session tracking: sessionId -> state
    this.sessions = new Map();

    // Default configuration (can be customized via .env)
    this.enabled           = (process.env.FILLER_ENABLED || "true").toLowerCase() === "true";
    this.thresholdMs       = parseInt(process.env.FILLER_LATENCY_THRESHOLD_MS || "1000", 10);
    this.cooldownMs        = parseInt(process.env.FILLER_COOLDOWN_MS          || "8000", 10);
  }

  /**
   * Pre-load all .raw filler files from disk into in-memory Buffers.
   * Runs synchronously on server boot for zero-latency retrieval during calls.
   *
   * @param {string} [dirPath] - Path to directory containing .raw filler files
   */
  init(dirPath) {
    const targetDir = dirPath || path.join(__dirname, "fillers");
    if (!fs.existsSync(targetDir)) {
      console.warn(`[FillerManager] Fillers directory not found at: ${targetDir}`);
      return;
    }

    const files = fs.readdirSync(targetDir);
    let loadedCount = 0;

    for (const file of files) {
      if (file.endsWith(".raw")) {
        const id = path.basename(file, ".raw");
        const fullPath = path.join(targetDir, file);
        const buf = fs.readFileSync(fullPath);
        this.fillersMap.set(id, buf);
        this.fillerKeys.push(id);
        loadedCount++;
      }
    }

    console.log(
      `[FillerManager] Loaded ${loadedCount} fillers into memory (${this.fillerKeys.join(", ")}) | Enabled: ${this.enabled} | Threshold: ${this.thresholdMs}ms | Cooldown: ${this.cooldownMs}ms`
    );
  }

  /**
   * Refill and shuffle the deck (Fisher-Yates) without immediate back-to-back repeats.
   */
  _refillDeck() {
    if (this.fillerKeys.length === 0) return;
    const shuffled = [...this.fillerKeys];
    for (let i = shuffled.length - 1; i > 0; i--) {
      const j = Math.floor(Math.random() * (i + 1));
      [shuffled[i], shuffled[j]] = [shuffled[j], shuffled[i]];
    }

    // If first item in newly shuffled deck matches last played filler, swap with middle
    if (shuffled.length > 1 && shuffled[shuffled.length - 1] === this.lastFiller) {
      const swapIdx = Math.floor(shuffled.length / 2);
      [shuffled[shuffled.length - 1], shuffled[swapIdx]] = [shuffled[swapIdx], shuffled[shuffled.length - 1]];
    }

    this.deck = shuffled;
  }

  /**
   * Select the next random filler from the deck.
   *
   * @returns {{ id: string, buffer: Buffer } | null}
   */
  getNextFiller() {
    if (this.fillerKeys.length === 0) return null;
    if (this.deck.length === 0) {
      this._refillDeck();
    }
    const id = this.deck.pop();
    this.lastFiller = id;
    return { id, buffer: this.fillersMap.get(id) };
  }

  /**
   * Retrieve or initialize state for a call session.
   */
  _getSessionState(sessionId) {
    if (!this.sessions.has(sessionId)) {
      this.sessions.set(sessionId, {
        timer: null,
        isArmed: false,
        lastPlayedAt: 0,
        clientHasSpoken: true, // Allow filler on the very first turn
        activeFillerId: null,
      });
    }
    return this.sessions.get(sessionId);
  }

  /**
   * Called when the agent begins speaking (VAD speech detected on myWs).
   * Evaluates eligibility and arms the latency threshold timer.
   *
   * @param {string} sessionId
   * @param {object} streamer - Instance of AudioStreamer
   */
  onAgentSpeechStart(sessionId, streamer) {
    if (!this.enabled || this.fillerKeys.length === 0) return;

    const state = this._getSessionState(sessionId);

    // If timer is already armed and ticking, do not re-arm
    if (state.isArmed) return;

    const now = Date.now();
    const timeSinceLast = now - state.lastPlayedAt;

    // Check Cooldown
    if (timeSinceLast < this.cooldownMs) {
      console.log(`[FillerManager] Session ${sessionId}: Cooldown active (${timeSinceLast}ms < ${this.cooldownMs}ms) — skipping filler`);
      return;
    }

    // Check Turn Gating: Client must have spoken since last filler (prevents filler spam on pauses mid-sentence)
    if (!state.clientHasSpoken) {
      console.log(`[FillerManager] Session ${sessionId}: Client hasn't spoken yet since last turn — skipping filler`);
      return;
    }

    state.isArmed = true;
    console.log(`[FillerManager] Session ${sessionId}: Latency timer armed (${this.thresholdMs}ms)...`);

    if (state.timer) clearTimeout(state.timer);
    state.timer = setTimeout(() => {
      state.timer = null;
      state.isArmed = false;

      // Ensure streamer is available and ready
      if (!streamer || !streamer.ws || streamer.ws.readyState !== 1) {
        console.warn(`[FillerManager] Session ${sessionId}: Streamer WS not ready — cannot play filler`);
        return;
      }

      const filler = this.getNextFiller();
      if (!filler) return;

      console.log(
        `\n[FillerManager] >>> LATENCY CROSSED ${this.thresholdMs}ms! INJECTING GURUJI FILLER: "${filler.id}" (${filler.buffer.length} bytes) <<<\n`
      );

      // Inject into AudioStreamer queue. AudioStreamer handles 40ms pacing to Plivo.
      streamer.addAudio(filler.buffer);

      state.lastPlayedAt = Date.now();
      state.clientHasSpoken = false; // Turn consumed! Client must speak before next filler can trigger.
      state.activeFillerId = filler.id;
    }, this.thresholdMs);
  }

  /**
   * Called when modulated audio from ElevenLabs S2S / Sarvam TTS arrives.
   * If the latency timer was still waiting, cancels it immediately.
   *
   * @param {string} sessionId
   */
  onModulatedChunkReceived(sessionId) {
    const state = this.sessions.get(sessionId);
    if (!state) return;

    if (state.timer) {
      clearTimeout(state.timer);
      state.timer = null;
      state.isArmed = false;
      console.log(
        `[FillerManager] Session ${sessionId}: Modulated voice arrived before ${this.thresholdMs}ms threshold — filler cancelled.`
      );
    }
  }

  /**
   * Called when client voice is detected on /client-stream.
   * Marks that the client has spoken (unlocking the next turn for a filler)
   * and cancels any pending agent filler if client is actively speaking.
   *
   * @param {string} sessionId
   */
  onClientSpeech(sessionId) {
    const state = this._getSessionState(sessionId);
    state.clientHasSpoken = true;

    // If a filler was somehow armed while client was speaking, disarm it immediately
    if (state.timer) {
      clearTimeout(state.timer);
      state.timer = null;
      state.isArmed = false;
      console.log(`[FillerManager] Session ${sessionId}: Client speaking — disarmed pending filler`);
    }
  }

  /**
   * Clean up session state on call disconnect.
   *
   * @param {string} sessionId
   */
  cleanup(sessionId) {
    const state = this.sessions.get(sessionId);
    if (state) {
      if (state.timer) clearTimeout(state.timer);
      this.sessions.delete(sessionId);
      console.log(`[FillerManager] Session ${sessionId}: Cleaned up`);
    }
  }
}

const fillerManager = new FillerManager();

module.exports = { fillerManager, FillerManager };

