// plivoClient.js
// Plivo REST API wrapper — dials the agent's phone as a second call leg

const plivo = require('plivo');

const client = new plivo.Client(
  process.env.PLIVO_AUTH_ID,
  process.env.PLIVO_AUTH_TOKEN
);

/**
 * Dials the agent's phone number.
 * When the agent answers, Plivo hits AGENT_ANSWER_URL which returns
 * a <Stream> XML so voice-server can receive the agent's audio.
 *
 * @returns {Promise<object>} Plivo call response
 */
async function dialAgent() {
  const from = process.env.PLIVO_FROM_NUMBER;
  const to   = process.env.AGENT_PHONE_NUMBER;
  const answerUrl = process.env.AGENT_ANSWER_URL;

  console.log(`[Plivo] Dialing agent: ${from} → ${to}`);

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

module.exports = { dialAgent };
