import { randomBytes } from "node:crypto";
import pg from "pg";
import { migrate } from "./migrate.js";

export function adminBaseUrl(): string {
  return process.env.RELAY_ADMIN_URL ?? "postgres://postgres@127.0.0.1:54329/postgres";
}

export async function createTestDatabase(): Promise<{
  name: string;
  adminUrl: string;
  appUrl: string;
  drop: () => Promise<void>;
}> {
  const name = `relay_${randomBytes(6).toString("hex")}`;
  const root = new pg.Client({ connectionString: adminBaseUrl() });
  await root.connect();
  try {
    await root.query(`CREATE DATABASE ${name}`);
  } finally {
    await root.end();
  }
  const admin = new URL(adminBaseUrl());
  admin.pathname = `/${name}`;
  const adminUrl = admin.toString();
  await migrate(adminUrl);
  const app = new URL(adminUrl);
  app.username = "relay_app";
  app.password = "relay_app_dev";
  return {
    name,
    adminUrl,
    appUrl: app.toString(),
    drop: async () => {
      const client = new pg.Client({ connectionString: adminBaseUrl() });
      await client.connect();
      try {
        await client.query(
          `SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = $1 AND pid <> pg_backend_pid()`,
          [name],
        );
        await client.query(`DROP DATABASE ${name}`);
      } finally {
        await client.end();
      }
    },
  };
}
