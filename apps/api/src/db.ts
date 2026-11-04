import pg from 'pg';

// pg returns BIGINT/COUNT(*) as strings to avoid precision loss. Our values (counts, minor-unit
// amounts) stay far below 2^53, so parse them as numbers.
pg.types.setTypeParser(pg.types.builtins.INT8, (value) => Number.parseInt(value, 10));

export type Db = pg.Pool;

export function createPool(connectionString: string, connectionTimeoutMillis: number): pg.Pool {
  return new pg.Pool({ connectionString, connectionTimeoutMillis, max: 20 });
}

/** Runs `fn` in a transaction on one pooled connection, rolling back if it throws. */
export async function withTransaction<T>(
  pool: pg.Pool,
  fn: (client: pg.PoolClient) => Promise<T>,
): Promise<T> {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    const result = await fn(client);
    await client.query('COMMIT');
    return result;
  } catch (error) {
    await client.query('ROLLBACK').catch(() => undefined);
    throw error;
  } finally {
    client.release();
  }
}

/** PostgreSQL error with SQLSTATE code and the violated constraint, if any. */
export function pgErrorInfo(error: unknown): { code?: string; constraint?: string } {
  if (typeof error === 'object' && error !== null) {
    const { code, constraint } = error as { code?: unknown; constraint?: unknown };
    return {
      ...(typeof code === 'string' ? { code } : {}),
      ...(typeof constraint === 'string' ? { constraint } : {}),
    };
  }
  return {};
}

export const PG_UNIQUE_VIOLATION = '23505';
