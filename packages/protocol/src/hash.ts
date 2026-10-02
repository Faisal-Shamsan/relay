import { createHash } from "node:crypto";
import { canonicalRelayJson, type Json } from "./json.js";

/** SHA-256 is the only digest named by the specification. Receipt hashes are 64 hex digits. */
export function sha256Hex(utf8: string): string {
  return createHash("sha256").update(utf8, "utf8").digest("hex");
}

export function canonicalHash(value: Json): string {
  return sha256Hex(canonicalRelayJson(value));
}
