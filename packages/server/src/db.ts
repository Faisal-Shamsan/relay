import { AsyncLocalStorage } from "node:async_hooks";
import pg from "pg";

const tenantContext = new AsyncLocalStorage<string>();

export function currentTenant(): string | undefined {
  return tenantContext.getStore();
}

export function withTenant<T>(tenantId: string, run: () => Promise<T>): Promise<T> {
  return tenantContext.run(tenantId, run);
}

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
    await bindTenant(client);
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

async function bindTenant(client: pg.PoolClient): Promise<void> {
  const tenantId = tenantContext.getStore();
  if (!tenantId) return;
  await client.query(`SELECT set_config('app.tenant_id', $1, true)`, [tenantId]);
}
