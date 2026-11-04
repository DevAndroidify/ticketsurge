// k6 flash-sale scenario: every iteration is one distinct user competing for the 500 seats.
//
//   join queue -> poll until admitted -> load seat map -> hold 1-2 random available seats
//   (retry up to 3 times on conflicts) -> abandon some holds -> book the rest
//
// Run through load-tests/run.sh, which prepares data/run.json and starts every service.
import { check, sleep } from 'k6';
import { SharedArray } from 'k6/data';
import exec from 'k6/execution';
import http from 'k6/http';
import { Counter, Trend } from 'k6/metrics';

const BASE = __ENV.BASE_URL || 'http://localhost:3000';
const USERS = Number(__ENV.USERS || 10000);
const VUS = Number(__ENV.VUS || 400);
const ABANDON_RATE = Number(__ENV.ABANDON_RATE || 0.1);
const ADMISSION_TIMEOUT_MS = Number(__ENV.ADMISSION_TIMEOUT_MS || 180000);

const run = JSON.parse(open('./data/run.json'));
const tokens = new SharedArray('tokens', () => JSON.parse(open('./data/run.json')).tokens);
const EVENT = run.eventId;

const holdLatency = new Trend('hold_latency', true);
const bookingLatency = new Trend('booking_latency', true);
const seatMapLatency = new Trend('seat_map_latency', true);
const admissionWait = new Trend('admission_wait', true);
// One counter per outcome, so the end-of-test summary reports each total.
const OUTCOMES = [
  'booked',
  'sold_out',
  'abandoned_hold',
  'hold_lost_race',
  'admission_timeout',
  'error',
];
const counters = Object.fromEntries(OUTCOMES.map((name) => [name, new Counter(`outcome_${name}`)]));

export const options = {
  scenarios: {
    flash_sale: {
      // Exactly USERS distinct users; at most VUS of them active at the same moment.
      executor: 'shared-iterations',
      vus: VUS,
      iterations: USERS,
      maxDuration: '20m',
    },
  },
  summaryTrendStats: ['avg', 'med', 'p(95)', 'p(99)', 'max'],
  discardResponseBodies: false,
};

function outcome(name) {
  (counters[name] || counters.error).add(1, { detail: name });
}

function pick(list, n) {
  const copy = list.slice();
  const chosen = [];
  while (chosen.length < n && copy.length > 0) {
    chosen.push(copy.splice(Math.floor(Math.random() * copy.length), 1)[0]);
  }
  return chosen;
}

export default function () {
  const token = tokens[exec.scenario.iterationInTest];
  const auth = { Authorization: `Bearer ${token}` };
  const json = { ...auth, 'Content-Type': 'application/json' };

  // 1. Join the virtual queue.
  let res = http.post(`${BASE}/events/${EVENT}/queue`, null, {
    headers: auth,
    tags: { name: 'queue_join' },
  });
  if (res.status !== 200) return outcome(`queue_join_${res.status}`);
  let admitted = res.json('admitted');

  // 2. Wait to be admitted.
  const waitStart = Date.now();
  while (!admitted) {
    if (Date.now() - waitStart > ADMISSION_TIMEOUT_MS) return outcome('admission_timeout');
    sleep(0.5);
    res = http.get(`${BASE}/events/${EVENT}/queue/me`, {
      headers: auth,
      tags: { name: 'queue_me' },
    });
    admitted = res.status === 200 && res.json('admitted');
  }
  admissionWait.add(Date.now() - waitStart);

  // 3. Load the seat map.
  res = http.get(`${BASE}/events/${EVENT}/seats`, { tags: { name: 'seat_map' } });
  seatMapLatency.add(res.timings.duration);
  if (res.status !== 200) return outcome(`seat_map_${res.status}`);
  let available = res
    .json('seats')
    .filter((s) => s.status === 'available')
    .map((s) => s.id);
  if (available.length === 0) return outcome('sold_out');

  // 4. Hold 1 seat (70%) or 2 adjacent-in-list seats (30%), retrying on conflicts.
  const want = Math.random() < 0.7 ? 1 : 2;
  let holdId;
  for (let attempt = 0; attempt < 3 && !holdId; attempt++) {
    const seatIds = pick(available, want);
    if (seatIds.length < want) break;
    res = http.post(`${BASE}/events/${EVENT}/holds`, JSON.stringify({ seatIds }), {
      headers: json,
      tags: { name: 'hold' },
    });
    holdLatency.add(res.timings.duration);
    if (res.status === 201) {
      holdId = res.json('holdId');
    } else if (res.status === 409) {
      const taken = new Set(res.json('error.details.seatIds') || seatIds);
      available = available.filter((id) => !taken.has(id));
    } else {
      return outcome(`hold_${res.status}`);
    }
  }
  if (!holdId) return outcome(available.length === 0 ? 'sold_out' : 'hold_lost_race');

  // 5. Some users walk away; their hold expires on its own.
  if (Math.random() < ABANDON_RATE) return outcome('abandoned_hold');
  sleep(0.2 + Math.random() * 0.8);

  // 6. Book.
  res = http.post(`${BASE}/bookings`, JSON.stringify({ holdId }), {
    headers: { ...json, 'Idempotency-Key': `${run.runId}-${exec.scenario.iterationInTest}` },
    tags: { name: 'booking' },
  });
  bookingLatency.add(res.timings.duration);
  check(res, { 'booking accepted': (r) => r.status === 202 });
  return outcome(res.status === 202 ? 'booked' : `booking_${res.status}`);
}

export function handleSummary(data) {
  return { [`/results/${__ENV.RESULT_NAME || 'summary'}.json`]: JSON.stringify(data, null, 2) };
}
