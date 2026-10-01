require('dotenv').config();
const { createSarvamTTS } = require('./sarvamTTS');
const fs = require('fs');
const { hasWavHeader } = require('./audioUtils');

const apiKey = process.env.SARVAM_API_KEY;
if (!apiKey) {
    console.error("No SARVAM_API_KEY found in .env");
    process.exit(1);
}

console.log("Connecting to Sarvam TTS...");
const ws = createSarvamTTS(apiKey, 'priya', 'hi-IN', (chunk) => {
    console.log(`Received chunk of size ${chunk.length}`);
    if (hasWavHeader(chunk)) {
        console.log("Chunk HAS WAV HEADER!");
        // Parse WAV header manually
        const numChannels = chunk.readUInt16LE(22);
        const sampleRate = chunk.readUInt32LE(24);
        const byteRate = chunk.readUInt32LE(28);
        const blockAlign = chunk.readUInt16LE(32);
        const bitsPerSample = chunk.readUInt16LE(34);
        console.log(`WAV Format: ${numChannels} channels, ${sampleRate} Hz, ${bitsPerSample}-bit`);
    } else {
        console.log("Chunk does NOT have WAV header.");
    }
    fs.appendFileSync('test_output.bin', chunk);
});

setTimeout(() => {
    console.log("Sending synthesize request...");
    ws.synthesize("Hello, this is a test to check the audio format.");
}, 2000);

setTimeout(() => {
    console.log("Closing...");
    process.exit(0);
}, 6000);
