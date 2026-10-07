import { readdirSync, readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import pg from "pg";

export async function migrate(adminUrl: string): Promise<void> {
  const here = dirname(fileURLToPath(import.meta.url));
  const directory = join(here, "../../sql");
  const files = readdirSync(directory).filter((file) => file.endsWith(".sql")).sort();
  const client = new pg.Client({ connectionString: adminUrl });
  await client.connect();
  try {
    for (const file of files) {
      await client.query(readFileSync(join(directory, file), "utf8"));
    }
  } finally {
    await client.end();
  }
}
