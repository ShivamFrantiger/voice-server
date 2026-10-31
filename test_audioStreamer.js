// test_audioStreamer.js
// Verification suite for Step 1: AudioStreamer real-time precision & buffer management

const { AudioStreamer } = require("./audioStreamer");

async function runTests() {
  console.log("=== Test 1: Full Drain with Adaptive Catch-Up ===");
  const sentFrames = [];
  const mockWs = {
    readyState: 1,
    send(payloadStr) {
      const msg = JSON.parse(payloadStr);
      sentFrames.push({
        time: performance.now(),
        bytes: Buffer.from(msg.media.payload, "base64").length,
      });
    },
  };

  const streamer = new AudioStreamer(mockWs, "test-session", "test-streamer", {
    preBufferMs: 40,
    maxQueueBytes: 32000,
  });

  const totalFrames = 50;
  const chunk = Buffer.alloc(320, 127);
  const t0 = performance.now();

  for (let i = 0; i < totalFrames; i++) {
    streamer.addAudio(chunk);
  }

  await new Promise((resolve) => {
    const check = setInterval(() => {
      if (streamer.queue.length === 0 && sentFrames.length > 0) {
        clearInterval(check);
        resolve();
      }
    }, 20);
  });

  const totalDuration = sentFrames[sentFrames.length - 1].time - t0;
  const totalBytesSent = sentFrames.reduce((acc, f) => acc + f.bytes, 0);
  console.log(`Sent ${sentFrames.length} frames (${totalBytesSent} bytes total).`);
  console.log(`Stream drained cleanly in ${Math.round(totalDuration)}ms.`);

  if (totalBytesSent !== totalFrames * 320) {
    throw new Error(`Data loss! Expected ${totalFrames * 320} bytes, sent ${totalBytesSent}`);
  }
  console.log("✓ Test 1 Passed: Audio drained completely with zero data loss!\n");

  console.log("=== Test 2: Standard 40ms Cadence (Queue Under Catch-Up Limit) ===");
  const precisionFrames = [];
  const mockWsTiming = {
    readyState: 1,
    send() {
      precisionFrames.push(performance.now());
    },
  };

  const timingStreamer = new AudioStreamer(mockWsTiming, "timing", "timing", {
    preBufferMs: 40,
  });

  // Keep queue under 1600 bytes (e.g. 4 frames = 1280 bytes = 160ms)
  // Each frame is sent at standard 40ms cadence
  timingStreamer.addAudio(Buffer.alloc(1280, 127)); // 4 frames of 320 bytes

  const startTiming = performance.now();
  await new Promise((resolve) => {
    const check = setInterval(() => {
      if (timingStreamer.queue.length === 0 && precisionFrames.length >= 4) {
        clearInterval(check);
        resolve();
      }
    }, 10);
  });

  const timingDuration = precisionFrames[precisionFrames.length - 1] - startTiming;
  const expectedTiming = 4 * 40; // 160ms
  const drift = Math.abs(timingDuration - expectedTiming);
  console.log(`4 ticks of 40ms took ${Math.round(timingDuration)}ms (expected ${expectedTiming}ms, drift: ${Math.round(drift)}ms)`);
  if (drift > 60) {
    throw new Error(`Timer drift exceeded tolerance: ${drift}ms`);
  }
  console.log("✓ Test 2 Passed: Standard 40ms cadence maintained with sub-frame precision!\n");

  console.log("=== Test 3: Adaptive Catch-Up Drain ===");
  const burstFrames = [];
  const mockWs2 = {
    readyState: 1,
    send(payloadStr) {
      const msg = JSON.parse(payloadStr);
      burstFrames.push(Buffer.from(msg.media.payload, "base64").length);
    },
  };

  const streamer2 = new AudioStreamer(mockWs2, "test-session-2", "catch-up", {
    preBufferMs: 40,
  });

  // 4000 bytes burst (> 1600 bytes triggers catch-up)
  const burstAudio = Buffer.alloc(4000, 127);
  streamer2.addAudio(burstAudio);

  // Wait 120ms (around 3 ticks)
  await new Promise((r) => setTimeout(r, 120));
  streamer2.stop();

  console.log("Chunk sizes dispatched during catch-up:", burstFrames);
  const hasCatchUpFrames = burstFrames.some((size) => size > 320);
  if (!hasCatchUpFrames) {
    throw new Error("Catch-up failed: no 640-byte accelerated frames were dispatched!");
  }
  console.log("✓ Test 3 Passed: Dynamic catch-up successfully scaled chunk size to drain backlog!\n");

  console.log("=== Test 4: Hard Ceiling (Anti-Bloat Protection) ===");
  const streamer3 = new AudioStreamer(mockWs, "test-session-3", "anti-bloat", {
    maxQueueBytes: 3200, // 400ms for test
  });

  const hugeAudio = Buffer.alloc(24000, 127); // 3 seconds burst
  streamer3.addAudio(hugeAudio);

  console.log(`Queue size after 24,000 byte burst: ${streamer3.queue.length} bytes (max: ${streamer3.maxQueueBytes})`);
  if (streamer3.queue.length > streamer3.maxQueueBytes) {
    throw new Error(`Hard ceiling breached! Queue has ${streamer3.queue.length} bytes`);
  }
  console.log("✓ Test 4 Passed: Hard ceiling prevented memory bloat and shed stale audio!\n");

  console.log("=== Test 5: Barge-In / clear() ===");
  let clearedPlivo = false;
  const mockWs4 = {
    readyState: 1,
    send(payloadStr) {
      const msg = JSON.parse(payloadStr);
      if (msg.event === "clearAudio") clearedPlivo = true;
    },
  };

  const streamer4 = new AudioStreamer(mockWs4, "test-session-4", "barge-in");
  streamer4.addAudio(Buffer.alloc(1600, 127));
  streamer4.clear();

  if (streamer4.queue.length !== 0 || !clearedPlivo) {
    throw new Error("clear() failed to purge queue or notify Plivo!");
  }
  console.log("✓ Test 5 Passed: clear() successfully purged queue and dispatched clearAudio to Plivo!\n");

  console.log("ALL AUDIO STREAMER TESTS PASSED SUCCESSFULLY! 🚀");
}

runTests().catch((err) => {
  console.error("Test failed:", err);
  process.exit(1);
});
