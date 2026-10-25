import { createPool } from "./db.js";

export async function verifyProjections(
  databaseUrl: string,
  runId: string,
): Promise<{ ok: boolean; mismatches: unknown[] }> {
  const pool = createPool(databaseUrl);
  try {
    const result = await pool.query("SELECT relay_verify_projections($1::uuid) AS result", [runId]);
    const body = result.rows[0]?.result as { ok?: unknown; mismatches?: unknown } | undefined;
    if (typeof body !== "object" || body === null || typeof body.ok !== "boolean" || !Array.isArray(body.mismatches)) {
      throw new Error("projection verification returned an unexpected result");
    }
    return { ok: body.ok, mismatches: body.mismatches };
  } finally {
    await pool.end();
  }
}
