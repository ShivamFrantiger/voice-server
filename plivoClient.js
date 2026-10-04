// plivoClient.js
// Plivo REST API wrapper — dials the client's phone as a second call leg

const plivo = require('plivo');

const client = new plivo.Client(
  process.env.PLIVO_AUTH_ID,
  process.env.PLIVO_AUTH_TOKEN
);

/**
 * Dials the client's phone number dynamically.
 * When the client answers, Plivo hits CLIENT_ANSWER_URL which returns
 * a <Stream> XML so voice-server can receive the client's audio.
 *
 * @param {string} sessionId   - session UUID (appended to answerUrl as query param)
 * @param {string} toNumber    - client's phone number to dial (E.164 format)
 * @returns {Promise<object>}  - Plivo call response
 */
async function dialClient(sessionId, toNumber) {
  const from = process.env.PLIVO_FROM_NUMBER;
  const to   = toNumber;

  let answerUrl = process.env.CLIENT_ANSWER_URL;
  if (!answerUrl) {
    throw new Error('CLIENT_ANSWER_URL env var is not set');
  }

  if (sessionId) {
    try {
      const url = new URL(answerUrl);
      url.searchParams.set('sessionId', sessionId);
      answerUrl = url.toString();
    } catch (e) {
      answerUrl += (answerUrl.includes('?') ? '&' : '?') + `sessionId=${sessionId}`;
    }
  }

  console.log(`[Plivo] Dialing client: ${from} → ${to}`);

  const response = await client.calls.create(
    from,
    to,
    answerUrl,
    {
      answer_method: 'POST',
    }
  );

  console.log('[Plivo] Outbound call created, requestUuid:', response.requestUuid);
  return response;
}

module.exports = { dialClient };
