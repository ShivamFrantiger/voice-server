const fs = require('fs');
require('dotenv').config();

async function run() {
    console.log('API KEY:', process.env.ELEVEN_LABS_API ? 'Exists' : 'Missing');
    console.log('VOICE ID:', process.env.ELEVEN_LABS_VOICE_ID);
}
run();
