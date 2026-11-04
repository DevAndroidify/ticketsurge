// Prepares a flash-sale load test: a fresh 500-seat event on sale now, N users, and an access token
// per user, written to load-tests/data/run.json for k6.
//
// Users are inserted directly and their tokens signed with the API's JWT secret, because creating
// 10,000 accounts through /auth/register would load-test scrypt, not the flash sale. Their password
// hash is a placeholder that can never match, so these accounts cannot log in.
// Usage: npm run loadtest:prepare -- [users=10000]
import { mkdirSync, writeFileSync } from 'node:fs';
import { randomUUID } from 'node:crypto';
import { loadConfig, loadEnvFile, postgresUrlSchema } from '@ticketsurge/config';
import { SignJWT } from 'jose';
import { z } from 'zod';
import { createPool } from '../lib/db-config.js';
import { DEMO_ROWS, DEMO_SEATS_PER_ROW, DEMO_SECTIONS } from '../lib/seed.js';

loadEnvFile();
const config = loadConfig(
  z.object({
    DATABASE_URL: postgresUrlSchema,
    JWT_SECRET: z.string().min(32),
    JWT_ISSUER: z.string().default('ticketsurge-api'),
    JWT_AUDIENCE: z.string().default('ticketsurge'),
  }),
);
const userCount = Number(process.argv[2] ?? 10_000);
const runId = new Date()
  .toISOString()
  .replace(/[-:T.Z]/g, '')
  .slice(0, 14);
const pool = createPool(config.DATABASE_URL);

try {
  const eventId = randomUUID();
  await pool.query(
    `INSERT INTO booking.events (id, name, venue, starts_at, sale_starts_at, status)
     VALUES ($1, $2, 'Load Test Arena', now() + interval '30 days', now() - interval '1 minute', 'PUBLISHED')`,
    [eventId, `Flash sale ${runId}`],
  );
  await pool.query(
    `INSERT INTO booking.seats (event_id, section, row_label, seat_number, price_minor)
     SELECT $1, s.name, r.label, n, s.price
     FROM unnest($2::text[], $3::int[]) AS s(name, price)
     CROSS JOIN unnest($4::text[]) AS r(label)
     CROSS JOIN generate_series(1, $5::int) AS n`,
    [
      eventId,
      DEMO_SECTIONS.map((s) => s.name),
      DEMO_SECTIONS.map((s) => s.priceMinor),
      DEMO_ROWS,
      DEMO_SEATS_PER_ROW,
    ],
  );

  const emails = Array.from({ length: userCount }, (_, i) => `load-${runId}-${i}@load.test`);
  const { rows: users } = await pool.query<{ id: string; email: string }>(
    `INSERT INTO booking.users (email, password_hash)
     SELECT e, 'disabled:load-test' FROM unnest($1::text[]) AS e
     RETURNING id, email`,
    [emails],
  );

  const key = new TextEncoder().encode(config.JWT_SECRET);
  const now = Math.floor(Date.now() / 1000);
  const tokens = await Promise.all(
    users.map((u) =>
      new SignJWT({ email: u.email })
        .setProtectedHeader({ alg: 'HS256', typ: 'JWT' })
        .setSubject(u.id)
        .setIssuer(config.JWT_ISSUER)
        .setAudience(config.JWT_AUDIENCE)
        .setIssuedAt(now)
        .setExpirationTime(now + 4 * 3600)
        .sign(key),
    ),
  );

  const outDir = new URL('../../../load-tests/data/', import.meta.url);
  mkdirSync(outDir, { recursive: true });
  writeFileSync(
    new URL('run.json', outDir),
    JSON.stringify({ runId, eventId, seats: 500, tokens }),
  );
  console.log(JSON.stringify({ runId, eventId, users: users.length }));
} catch (error) {
  console.error((error as Error).message);
  process.exitCode = 1;
} finally {
  await pool.end();
}
