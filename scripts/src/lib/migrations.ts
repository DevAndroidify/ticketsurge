import { createHash } from 'node:crypto';
import { readdir, readFile } from 'node:fs/promises';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import type pg from 'pg';

export const MIGRATIONS_DIR = fileURLToPath(new URL('../../../migrations', import.meta.url));

// Arbitrary constant; serialises concurrent migration runs (e.g. two services starting at once).
const MIGRATION_LOCK_ID = 7_402_211;

export interface MigrationFile {
  name: string;
  sql: string;
  checksum: string;
}

export async function readMigrations(dir: string = MIGRATIONS_DIR): Promise<MigrationFile[]> {
  const names = (await readdir(dir)).filter((name) => /^\d{4}_[\w-]+\.sql$/.test(name)).sort();
  return Promise.all(
    names.map(async (name) => {
      const sql = await readFile(join(dir, name), 'utf8');
      return { name, sql, checksum: createHash('sha256').update(sql).digest('hex') };
    }),
  );
}

/**
 * Applies pending SQL migrations in filename order, each in its own transaction.
 * Already-applied files must be unchanged: editing a shipped migration is an error, because
 * databases that already ran it would silently differ from new ones.
 */
export async function runMigrations(
  pool: pg.Pool,
  dir: string = MIGRATIONS_DIR,
  log: (message: string) => void = () => undefined,
): Promise<string[]> {
  const migrations = await readMigrations(dir);
  const client = await pool.connect();
  const applied: string[] = [];
  try {
    await client.query('SELECT pg_advisory_lock($1)', [MIGRATION_LOCK_ID]);
    await client.query(`
      CREATE TABLE IF NOT EXISTS public.schema_migrations (
        name       TEXT PRIMARY KEY,
        checksum   TEXT NOT NULL,
        applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
      )`);

    const { rows } = await client.query<{ name: string; checksum: string }>(
      'SELECT name, checksum FROM public.schema_migrations',
    );
    const done = new Map(rows.map((row) => [row.name, row.checksum]));

    for (const migration of migrations) {
      const previous = done.get(migration.name);
      if (previous !== undefined) {
        if (previous !== migration.checksum) {
          throw new Error(`migration ${migration.name} was modified after it was applied`);
        }
        continue;
      }
      await client.query('BEGIN');
      try {
        await client.query(migration.sql);
        await client.query(
          'INSERT INTO public.schema_migrations (name, checksum) VALUES ($1, $2)',
          [migration.name, migration.checksum],
        );
        await client.query('COMMIT');
      } catch (error) {
        await client.query('ROLLBACK');
        throw new Error(`migration ${migration.name} failed: ${(error as Error).message}`, {
          cause: error,
        });
      }
      applied.push(migration.name);
      log(`applied ${migration.name}`);
    }
  } finally {
    await client.query('SELECT pg_advisory_unlock($1)', [MIGRATION_LOCK_ID]).catch(() => undefined);
    client.release();
  }
  return applied;
}
