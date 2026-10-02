import assert from "node:assert/strict";
import test from "node:test";
import { logicalStepIdempotencyKey } from "../src/idempotency.js";

test("logical step key ignores attempt and fence", () => {
  const first = logicalStepIdempotencyKey(
    "11111111-1111-4111-8111-111111111111",
    "22222222-2222-4222-8222-222222222222",
    "only",
  );
  const second = logicalStepIdempotencyKey(
    "11111111-1111-4111-8111-111111111111",
    "22222222-2222-4222-8222-222222222222",
    "only",
  );
  assert.equal(first, second);
  assert.match(first, /^relay-step-v1:[0-9a-f]{64}$/);
  assert.equal(first.includes("attempt"), false);
  assert.equal(first.includes("fence"), false);
});
