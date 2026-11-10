// Prepares a flash-sale load test: a fresh 500-seat event on sale now, N users, and an access token
// per user, written to load-tests/data/run.json for k6.
//
// Users are inserted directly and their tokens signed with the API's JWT secret, because creating
// 10,000 accounts through /auth/register would load-test scrypt, not the flash sale. Their password
// hash is a placeholder that can never match, so these accounts cannot log in.
// Usage: npm run loadtest:prepare -- [users=10000]
import { mkdirSync, writeFileSync } from 'node:fs';
import { loadConfig, loadEnvFile, postgresUrlSchema } from '@ticketsurge/config';
import { SignJWT } from 'jose';
import { z } from 'zod';
import { createPool } from '../lib/db-config.js';
import { createSaleEvent } from '../lib/seed.js';

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
  const { eventId } = await createSaleEvent(pool, {
    name: `Flash sale ${runId}`,
    venue: 'Load Test Arena',
  });

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
