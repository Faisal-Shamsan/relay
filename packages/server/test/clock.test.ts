import assert from "node:assert/strict";
import test from "node:test";
import pg from "pg";
import { adminBaseUrl } from "../src/testing.js";

test("now() stays at transaction start while clock_timestamp() moves", async () => {
  const client = new pg.Client({ connectionString: adminBaseUrl() });
  await client.connect();
  try {
    await client.query("BEGIN");
    const first = await client.query<{ n: Date; c: Date }>("SELECT now() AS n, clock_timestamp() AS c");
    await client.query("SELECT pg_sleep(0.2)");
    const second = await client.query<{ n: Date; c: Date }>("SELECT now() AS n, clock_timestamp() AS c");
    await client.query("COMMIT");
    assert.equal(second.rows[0]?.n.getTime(), first.rows[0]?.n.getTime());
    assert.ok(second.rows[0]!.c.getTime() > second.rows[0]!.n.getTime());
    assert.ok(second.rows[0]!.c.getTime() > first.rows[0]!.c.getTime());
  } finally {
    await client.end();
  }
});
