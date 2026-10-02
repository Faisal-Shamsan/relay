import assert from "node:assert/strict";
import test from "node:test";
import { canonicalRelayJson, parseRelayJson, RelayJsonError, validateRelayJson } from "../src/json.js";

test("canonicalizes object keys by UTF-16 order and normalizes negative zero", () => {
  const canonical = canonicalRelayJson({ b: 1, a: -0 });
  assert.equal(canonical, '{"a":0,"b":1}');
  assert.deepEqual(validateRelayJson({ b: 1, a: -0 }), { a: 0, b: 1 });
  assert.equal(Object.is(JSON.parse(canonical).a, -0), false);
});

test("rejects values the specification excludes", () => {
  const samples: unknown[] = [
    undefined,
    Number.NaN,
    Number.POSITIVE_INFINITY,
    2 ** 53,
    BigInt(1),
    new Date(),
    new Map(),
    new Set(),
    Buffer.from("x"),
    new Uint8Array(),
    () => 1,
    Symbol("x"),
    Object.assign(Object.create({ inherited: true }), { a: 1 }),
  ];
  for (const sample of samples) {
    assert.throws(() => validateRelayJson(sample), RelayJsonError);
  }
  const cycle: { self?: unknown } = {};
  cycle.self = cycle;
  assert.throws(() => validateRelayJson(cycle), RelayJsonError);
});

test("rejects duplicate raw object keys before ordinary parsing", () => {
  assert.throws(() => parseRelayJson('{"a":1,"a":2}'), /duplicate object key/);
});

test("requires an explicit null and rejects a sparse array", () => {
  assert.deepEqual(validateRelayJson(null), null);
  const sparse: unknown[] = [];
  sparse.length = 1;
  assert.throws(() => validateRelayJson(sparse), /sparse/);
});

test("enforces the recorded depth choice: root counts as depth 1", () => {
  let allowed: unknown = "leaf";
  for (let i = 0; i < 63; i += 1) allowed = [allowed];
  assert.doesNotThrow(() => validateRelayJson(allowed));
  let rejected: unknown = "leaf";
  for (let i = 0; i < 64; i += 1) rejected = [rejected];
  assert.throws(() => validateRelayJson(rejected), /depth/);
});
