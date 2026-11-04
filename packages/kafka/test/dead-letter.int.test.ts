import { randomUUID } from 'node:crypto';
import {
  TOPICS,
  createEnvelope,
  dlqTopicName,
  topicName,
  type AnyEnvelope,
} from '@ticketsurge/events';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import {
  createTestTopics,
  testKafka,
  testLogger,
  waitForRawMessages,
} from '../../../scripts/src/testing/kafka.js';
import {
  createProducer,
  PermanentError,
  startConsumer,
  type Producer,
  type RunningConsumer,
} from '../src/index.js';

const GROUP = 'dlq-test';
let topics: Awaited<ReturnType<typeof createTestTopics>>;
let producer: Producer;
let consumer: RunningConsumer;
let source: string;
let dlq: string;

// Behaviour per bookingId, set by each test.
const behaviour = new Map<string, 'ok' | 'flaky' | 'always-fail' | 'permanent'>();
const attempts = new Map<string, number>();
const handled: string[] = [];

beforeAll(async () => {
  topics = await createTestTopics();
  source = topicName(topics.prefix, TOPICS.bookingConfirmed);
  dlq = dlqTopicName(topics.prefix, GROUP);
  const kafka = testKafka('dlq-test');
  producer = await createProducer(kafka);
  // The DLQ topic for this ad-hoc group is not in CONSUMER_GROUPS, so create it here.
  const admin = kafka.admin();
  await admin.connect();
  await admin.createTopics({ topics: [{ topic: dlq, numPartitions: 1, replicationFactor: 1 }] });
  await admin.disconnect();

  consumer = await startConsumer({
    kafka,
    groupId: `${topics.prefix}${GROUP}`,
    topics: [source],
    logger: testLogger,
    deadLetter: { topic: dlq, producer },
    // One partition at a time, so "later messages continue" is meaningful.
    concurrency: 1,
    retry: { maxAttempts: 3, initialDelayMs: 20, maxDelayMs: 50 },
    handle: async (envelope: AnyEnvelope) => {
      const id = envelope.data.bookingId;
      const n = (attempts.get(id) ?? 0) + 1;
      attempts.set(id, n);
      const mode = behaviour.get(id) ?? 'ok';
      if (mode === 'flaky' && n < 3) throw new Error(`transient failure ${n}`);
      if (mode === 'always-fail') throw new Error('database unreachable');
      if (mode === 'permanent') throw new PermanentError('seat belongs to another event');
      handled.push(id);
      await Promise.resolve();
    },
  });
  await consumer.waitForAssignment();
}, 60_000);

afterAll(async () => {
  await consumer.stop();
  await producer.disconnect();
  await topics.cleanup();
});

/** All test messages use one key, so they share a partition and keep their order. */
const KEY = randomUUID();

function confirmed(bookingId: string) {
  return createEnvelope(
    TOPICS.bookingConfirmed,
    {
      bookingId,
      eventId: randomUUID(),
      userId: randomUUID(),
      holdId: randomUUID(),
      seatIds: [randomUUID()],
      amountMinor: 100,
    },
    { traceId: 'dlq-test' },
  );
}

async function send(value: string, headers: Record<string, string> = {}) {
  await producer.send({ topic: source, messages: [{ key: KEY, value, headers }] });
}

async function waitHandled(bookingId: string) {
  const deadline = Date.now() + 20_000;
  while (!handled.includes(bookingId)) {
    if (Date.now() > deadline) throw new Error(`${bookingId} was not handled`);
    await new Promise((resolve) => setTimeout(resolve, 50));
  }
}

describe('retries and dead letters', () => {
  it('a malformed message reaches the dead-letter topic while later messages continue', async () => {
    const after = randomUUID();
    await send('{ this is not json', { traceId: 'bad-json' });
    await send(JSON.stringify({ type: 'booking.confirmed', version: 1, data: {} }), {
      traceId: 'bad-schema',
    });
    await send(JSON.stringify(confirmed(after)));

    const dead = await waitForRawMessages(dlq, (m) => m.key === KEY, { count: 2 });
    expect(dead.map((m) => m.value)).toEqual([
      '{ this is not json',
      JSON.stringify({ type: 'booking.confirmed', version: 1, data: {} }),
    ]);
    expect(dead[0]?.headers).toMatchObject({
      traceId: 'bad-json',
      'dlq-original-topic': source,
      'dlq-consumer-group': `${topics.prefix}${GROUP}`,
      'dlq-attempts': '0',
    });
    expect(dead[0]?.headers['dlq-error']).toMatch(/SyntaxError/);
    expect(dead[1]?.headers['dlq-error']).toMatch(/invalid message/);
    expect(dead[0]?.headers['dlq-original-offset']).toMatch(/^\d+$/);

    // The valid message behind them was processed.
    await waitHandled(after);
  });

  it('retries transient failures and succeeds without dead-lettering', async () => {
    const id = randomUUID();
    behaviour.set(id, 'flaky');
    await send(JSON.stringify(confirmed(id)));
    await waitHandled(id);
    expect(attempts.get(id)).toBe(3);
  });

  it('dead-letters after the retry budget is spent, and moves on', async () => {
    const failing = randomUUID();
    const permanent = randomUUID();
    const after = randomUUID();
    behaviour.set(failing, 'always-fail');
    behaviour.set(permanent, 'permanent');
    await send(JSON.stringify(confirmed(failing)));
    await send(JSON.stringify(confirmed(permanent)));
    await send(JSON.stringify(confirmed(after)));

    await waitHandled(after);
    const dead = await waitForRawMessages(
      dlq,
      (m) => m.value.includes(failing) || m.value.includes(permanent),
      { count: 2 },
    );
    const byId = (id: string) => dead.find((m) => m.value.includes(id));
    expect(byId(failing)?.headers).toMatchObject({ 'dlq-attempts': '3' });
    expect(byId(failing)?.headers['dlq-error']).toContain('database unreachable');
    // Permanent errors are not retried.
    expect(byId(permanent)?.headers).toMatchObject({ 'dlq-attempts': '1' });
    expect(attempts.get(permanent)).toBe(1);
    // The original value is preserved exactly, so the message can be replayed after a fix.
    expect(JSON.parse(byId(failing)?.value ?? '{}')).toMatchObject({
      data: { bookingId: failing },
    });
  });
});
