// test_fillerManager.js
// Verification script for Guruji Voice Filler state machine and audio queueing.

require("dotenv").config();
const { fillerManager } = require("./fillerManager");

async function runTest() {
  console.log("=== Testing FillerManager Initialization ===");
  fillerManager.init();

  if (fillerManager.fillerKeys.length !== 7) {
    throw new Error(`Expected 7 fillers, found ${fillerManager.fillerKeys.length}`);
  }
  console.log("✓ All 7 filler files successfully loaded into RAM.\n");

  console.log("=== Testing Non-Repeating Random Deck ===");
  const picked = [];
  for (let i = 0; i < 14; i++) {
    const filler = fillerManager.getNextFiller();
    picked.push(filler.id);
  }
  console.log("Picked sequence (14 picks):", picked.join(" -> "));
  // Check no adjacent duplicates
  for (let i = 1; i < picked.length; i++) {
    if (picked[i] === picked[i - 1]) {
      throw new Error(`Found consecutive duplicate filler: ${picked[i]}`);
    }
  }
  console.log("✓ No back-to-back repetitions detected in 14 picks.\n");

  console.log("=== Testing Latency Cancellation (< 1000ms) ===");
  const mockStreamer1 = {
    ws: { readyState: 1 },
    chunks: [],
    addAudio(buf) { this.chunks.push(buf); }
  };
  fillerManager.onAgentSpeechStart("session-1", mockStreamer1);
  console.log("Agent started speaking. Simulating fast response arriving at 300ms...");
  await new Promise(r => setTimeout(r, 300));
  fillerManager.onModulatedChunkReceived("session-1");
  await new Promise(r => setTimeout(r, 900)); // wait past 1000ms
  if (mockStreamer1.chunks.length > 0) {
    throw new Error("Filler was played despite modulated voice arriving early!");
  }
  console.log("✓ Fast response aborted the filler cleanly.\n");

  console.log("=== Testing Latency Trigger (> 1000ms) ===");
  const mockStreamer2 = {
    ws: { readyState: 1 },
    chunks: [],
    addAudio(buf) { this.chunks.push(buf); }
  };
  fillerManager.onAgentSpeechStart("session-2", mockStreamer2);
  console.log("Agent started speaking. Waiting 1100ms for latency threshold to fire...");
  await new Promise(r => setTimeout(r, 1150));
  if (mockStreamer2.chunks.length === 0) {
    throw new Error("Expected filler to be injected, but streamer queue is empty!");
  }
  console.log(`✓ Filler successfully injected into AudioStreamer queue (${mockStreamer2.chunks[0].length} bytes).\n`);

  console.log("=== Testing Turn Gating & Cooldown ===");
  // Trying to trigger immediately again without client speaking
  fillerManager.onAgentSpeechStart("session-2", mockStreamer2);
  await new Promise(r => setTimeout(r, 1150));
  if (mockStreamer2.chunks.length !== 1) {
    throw new Error("Filler triggered again without client speaking or cooldown!");
  }
  console.log("✓ Filler prevented from re-triggering mid-turn.\n");

  console.log("=== Testing Client Speech Turn Reset ===");
  fillerManager.onClientSpeech("session-2");
  // Set cooldown back artificially for fast test
  const state = fillerManager.sessions.get("session-2");
  state.lastPlayedAt = Date.now() - 9000; // 9 seconds ago
  fillerManager.onAgentSpeechStart("session-2", mockStreamer2);
  await new Promise(r => setTimeout(r, 1150));
  if (mockStreamer2.chunks.length !== 2) {
    throw new Error("Expected 2nd filler after client speech turn reset!");
  }
  console.log("✓ Client speech reset allowed the next turn filler.\n");

  fillerManager.cleanup("session-1");
  fillerManager.cleanup("session-2");

  console.log("=========================================");
  console.log("  ALL TESTS PASSED WITH 100% SUCCESS!    ");
  console.log("=========================================");
}

runTest().catch(err => {
  console.error("Test failed:", err);
  process.exit(1);
});

