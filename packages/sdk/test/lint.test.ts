import assert from "node:assert/strict";
import test from "node:test";
import { lintWorkflow, type LintRule } from "../src/lint.js";

test("direct workflow violations are reported and callback effects are not", () => {
  const source = `
    async (ctx) => {
      // Date.now()
      const label = "Math.random()";
      /* process.env.REGION */
      const clock = Date.now();
      const roll = Math.random();
      const id = crypto.randomUUID();
      const region = process.env.REGION;
      await fetch("https://example.invalid");
      globalThis.shared = 1;
      await Promise.all([1]);
      await ctx.now("t");
      await ctx.random("n");
      await ctx.uuid("id");
      await ctx.step("load", async () => {
        Date.now();
        Math.random();
        crypto.randomUUID();
        process.env.REGION;
        await fetch("https://example.invalid");
        globalThis.inside = 1;
        await Promise.race([1]);
        return { ok: true };
      });
      await ctx.step("priced", { input: { n: 1 } }, async (_step, _input) => {
        performance.now();
        return { ok: true };
      });
      await ctx.step.parallel("searches", [
        { name: "search:a", run: async () => Math.random() },
      ]);
      const via = "\${Date.now()}";
      return { clock, roll, id, region, via };
    }
  `;
  const rules = lintWorkflow(source).map((finding) => finding.rule);
  assert.deepEqual(rules, ["clock", "random", "uuid", "env", "io", "shared", "concurrency"]);
});

test("a template interpolation is checked and a hidden helper call is not", () => {
  const visible = lintWorkflow("async (ctx) => `time ${Date.now()}`");
  assert.deepEqual(visible.map((finding) => finding.rule), ["clock"]);
  assert.equal(visible[0]?.line, 1);

  const hidden = lintWorkflow("async (ctx) => hidden()");
  assert.deepEqual(hidden, []);

  const local = lintWorkflow("async (ctx) => {\n  function hidden() { return Date.now(); }\n  return hidden();\n}");
  assert.deepEqual(local.map((finding) => finding.rule), ["clock"]);
  assert.equal(local[0]?.line, 2);
});

test("lintWorkflow reads a function's own source", () => {
  const workflow = async () => Date.now();
  const rules: LintRule[] = lintWorkflow(workflow).map((finding) => finding.rule);
  assert.deepEqual(rules, ["clock"]);
});
