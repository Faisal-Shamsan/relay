import assert from "node:assert/strict";
import { existsSync, readdirSync, readFileSync, statSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import test from "node:test";

test("the dashboard source stays on the read API", () => {
  const root = dashboardRoot();
  const files = walk(root);
  assert.equal(files.length > 0, true);
  const forbidden = [
    /from ["']pg["']/,
    /require\(["']pg["']\)/,
    /@relay\/sdk/,
    /@relay\/server/,
    /relay-server/,
    /DATABASE_URL/,
  ];
  for (const file of files) {
    const text = readFileSync(file, "utf8");
    for (const pattern of forbidden) {
      assert.equal(pattern.test(text), false, `${file} matches ${pattern}`);
    }
  }
});

function dashboardRoot(): string {
  let dir = dirname(fileURLToPath(import.meta.url));
  for (let i = 0; i < 6; i += 1) {
    const candidate = join(dir, "packages", "dashboard");
    if (existsSync(candidate)) return candidate;
    dir = dirname(dir);
  }
  throw new Error("dashboard package was not found");
}

function walk(dir: string): string[] {
  const found: string[] = [];
  for (const name of readdirSync(dir)) {
    if (name === "node_modules" || name === ".next") continue;
    const path = join(dir, name);
    if (statSync(path).isDirectory()) found.push(...walk(path));
    else if (/\.(ts|tsx|js|jsx|css|json)$/.test(name)) found.push(path);
  }
  return found;
}
