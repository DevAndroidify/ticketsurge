import { mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import pg from 'pg';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { runMigrations } from '../src/lib/migrations.js';
import { createSaleEvent, DEMO_EVENT_ID, seedDemoEvent } from '../src/lib/seed.js';
import { useTestEnv } from '../src/testing/test-env.js';

const { databaseUrl } = useTestEnv();
const pool = new pg.Pool({ connectionString: databaseUrl, max: 2 });

afterAll(async () => {
  await pool.end();
});

describe('runMigrations', () => {
  it('is a no-op when the database is up to date', async () => {
    // Global setup already migrated the test database.
    expect(await runMigrations(pool)).toEqual([]);
  });

  describe('with a scratch schema', () => {
    let dir: string;
    beforeAll(async () => {
      dir = mkdtempSync(join(tmpdir(), 'ticketsurge-mig-'));
      await pool.query('DROP TABLE IF EXISTS public.mig_probe');
      await pool.query("DELETE FROM public.schema_migrations WHERE name LIKE '9%'");
    });
    afterAll(async () => {
      rmSync(dir, { recursive: true, force: true });
      await pool.query('DROP TABLE IF EXISTS public.mig_probe');
      await pool.query("DELETE FROM public.schema_migrations WHERE name LIKE '9%'");
    });

    it('applies new files once and rejects edits to applied files', async () => {
      writeFileSync(join(dir, '9001_probe.sql'), 'CREATE TABLE public.mig_probe (id int);');
      expect(await runMigrations(pool, dir)).toEqual(['9001_probe.sql']);
      expect(await runMigrations(pool, dir)).toEqual([]);

      writeFileSync(join(dir, '9001_probe.sql'), 'CREATE TABLE public.mig_probe (id bigint);');
      await expect(runMigrations(pool, dir)).rejects.toThrow(/modified after it was applied/);
    });

    it('rolls back a failing migration completely', async () => {
      writeFileSync(join(dir, '9001_probe.sql'), 'CREATE TABLE public.mig_probe (id int);');
      writeFileSync(
        join(dir, '9002_bad.sql'),
        'CREATE TABLE public.mig_half (id int); SELECT nope;',
      );
      await expect(runMigrations(pool, dir)).rejects.toThrow(/9002_bad.sql failed/);
      const { rowCount } = await pool.query("SELECT 1 FROM pg_tables WHERE tablename = 'mig_half'");
      expect(rowCount).toBe(0);
    });
  });
});

describe('seedDemoEvent', () => {
  it('creates 500 seats and is idempotent', async () => {
    // Other test files may leave bookings that reference the demo event.
    await pool.query('TRUNCATE booking.events, booking.users, booking.outbox CASCADE');
    expect(await seedDemoEvent(pool)).toEqual({ eventId: DEMO_EVENT_ID, seats: 500 });
    expect(await seedDemoEvent(pool)).toEqual({ eventId: DEMO_EVENT_ID, seats: 500 });
  });
});

describe('createSaleEvent', () => {
  it('opens a new 500-seat event on sale now, each time', async () => {
    const first = await createSaleEvent(pool, { name: 'Flash sale A', venue: 'Demo Arena' });
    const second = await createSaleEvent(pool, { name: 'Flash sale B', venue: 'Demo Arena' });
    expect(first.seats).toBe(500);
    expect(second.eventId).not.toBe(first.eventId);
    const { rows } = await pool.query<{ on_sale: boolean }>(
      `SELECT status = 'PUBLISHED' AND sale_starts_at <= now() AS on_sale
       FROM booking.events WHERE id = $1`,
      [second.eventId],
    );
    expect(rows[0]?.on_sale).toBe(true);
  });
});
