import pg from "pg";

const globalParsers = globalThis as { __relayPgParsers?: boolean };
if (!globalParsers.__relayPgParsers) {
  pg.types.setTypeParser(20, (value) => value);
  globalParsers.__relayPgParsers = true;
}

export function createPool(connectionString: string): pg.Pool {
  return new pg.Pool({ connectionString, max: 8 });
}

export async function withTransaction<T>(
  pool: pg.Pool,
  run: (client: pg.PoolClient) => Promise<T>,
): Promise<T> {
  const client = await pool.connect();
  try {
    await client.query("BEGIN ISOLATION LEVEL READ COMMITTED");
    const value = await run(client);
    await client.query("COMMIT");
    return value;
  } catch (error) {
    try {
      await client.query("ROLLBACK");
    } catch {
      // The connection may already be aborted.
    }
    throw error;
  } finally {
    client.release();
  }
}

export async function callJson(
  pool: pg.Pool,
  sql: string,
  params: unknown[],
): Promise<unknown> {
  return withTransaction(pool, async (client) => {
    const result = await client.query(sql, params);
    return result.rows[0]?.result;
  });
}
