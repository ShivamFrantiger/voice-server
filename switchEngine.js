#!/usr/bin/env node
/**
 * switchEngine.js
 * CLI command to switch or query the voice modulation engine on voice-server at runtime.
 *
 * Usage:
 *   node switchEngine.js elevenlabs
 *   node switchEngine.js sarvam
 *   node switchEngine.js status
 */

require("dotenv").config();

const target = (process.argv[2] || "").trim().toLowerCase();
const port   = process.env.PORT || 8080;
const isExplicitLocal = process.argv.includes("--local") || process.argv.includes("-l");
let host = isExplicitLocal ? `http://localhost:${port}` : (process.env.SERVER_URL || `http://localhost:${port}`);

for (const arg of process.argv) {
  if (arg.startsWith("--url=")) host = arg.split("=")[1];
}

async function requestWithFallback(path, options = {}) {
  try {
    const res = await fetch(`${host}${path}`, options);
    return { res, usedHost: host };
  } catch (err) {
    if (host !== `http://localhost:${port}`) {
      try {
        const fallbackRes = await fetch(`http://localhost:${port}${path}`, options);
        return { res: fallbackRes, usedHost: `http://localhost:${port}` };
      } catch (_) {}
    }
    throw err;
  }
}

async function main() {
  if (!target || target === "status" || target === "-s" || target === "--status") {
    try {
      const { res, usedHost } = await requestWithFallback("/api/engine");
      if (!res.ok) throw new Error(`HTTP ${res.status}: ${res.statusText}`);
      const data = await res.json();
      console.log("\n Voice Modulation Engine Status:");
      console.log(`   Connected Host: ${usedHost}`);
      console.log(`   Default Engine: ${data.currentEngine}`);
      console.log(`   Available:      ${data.availableEngines?.join(", ")}`);
      console.log(`   ElevenLabs:     Voice ID: ${data.config?.elevenlabs?.voiceId || "none"}`);
      console.log(`   Sarvam:         Speaker: ${data.config?.sarvam?.speaker || "none"}, Lang: ${data.config?.sarvam?.language || "none"}\n`);
    } catch (err) {
      console.error(`\n❌ Failed to connect to server: ${err.message}\n`);
    }
    return;
  }

  if (target !== "elevenlabs" && target !== "sarvam") {
    console.error(`\n❌ Invalid engine: "${target}". Choose "elevenlabs" or "sarvam".\n`);
    console.log("Usage:");
    console.log("  node switchEngine.js elevenlabs");
    console.log("  node switchEngine.js sarvam");
    console.log("  node switchEngine.js status\n");
    process.exit(1);
  }

  try {
    const { res, usedHost } = await requestWithFallback("/api/engine", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ engine: target }),
    });

    const data = await res.json();
    if (!res.ok) {
      console.error(`\n❌ Failed to switch engine: ${data.error || res.statusText}\n`);
      process.exit(1);
    }

    console.log(`\n✅ Modulation engine successfully changed!`);
    console.log(`   Previous Engine: ${data.previousEngine}`);
    console.log(`   Active Engine:   ${data.currentEngine}\n`);
  } catch (err) {
    console.error(`\n❌ Failed to connect to server at ${host}: ${err.message}\n`);
    process.exit(1);
  }
}

main();
