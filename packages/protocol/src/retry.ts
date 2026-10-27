import { RelayJsonError, type Json } from "./json.js";

export interface RetryPolicy {
  maxAttempts: number;
  initialDelayMs: number;
  maxDelayMs: number;
  multiplier: number;
  jitter: "none" | "full";
  retryableCodes: string[];
  [key: string]: Json;
}

const POLICY_KEYS = ["maxAttempts", "initialDelayMs", "maxDelayMs", "multiplier", "jitter", "retryableCodes"];
const MAX_ATTEMPTS = 10_000;

export function canonicalRetryPolicy(value: Json): RetryPolicy {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw new RelayJsonError("retry policy is invalid", "retry");
  }
  const keys = Object.keys(value);
  if (keys.length !== POLICY_KEYS.length || POLICY_KEYS.some((key) => !Object.hasOwn(value, key))) {
    throw new RelayJsonError("retry policy is invalid", "retry");
  }
  const maxAttempts = value.maxAttempts;
  const initialDelayMs = value.initialDelayMs;
  const maxDelayMs = value.maxDelayMs;
  const multiplier = value.multiplier;
  const jitter = value.jitter;
  const codes = value.retryableCodes;
  if (typeof maxAttempts !== "number" || !Number.isSafeInteger(maxAttempts) || maxAttempts < 1 || maxAttempts > MAX_ATTEMPTS) {
    throw new RelayJsonError("maxAttempts must be from 1 to 10000", "retry.maxAttempts");
  }
  if (typeof initialDelayMs !== "number" || !Number.isSafeInteger(initialDelayMs) || initialDelayMs < 0) {
    throw new RelayJsonError("initialDelayMs must be a nonnegative safe integer", "retry.initialDelayMs");
  }
  if (typeof maxDelayMs !== "number" || !Number.isSafeInteger(maxDelayMs) || maxDelayMs < 0) {
    throw new RelayJsonError("maxDelayMs must be a nonnegative safe integer", "retry.maxDelayMs");
  }
  if (typeof multiplier !== "number" || !Number.isFinite(multiplier) || multiplier < 1 || multiplier > Number.MAX_SAFE_INTEGER) {
    throw new RelayJsonError("multiplier must be a finite number >= 1", "retry.multiplier");
  }
  if (jitter !== "none" && jitter !== "full") {
    throw new RelayJsonError("jitter must be none or full", "retry.jitter");
  }
  if (!Array.isArray(codes) || codes.length > 32) {
    throw new RelayJsonError("retryableCodes must be an array of at most 32 strings", "retry.retryableCodes");
  }
  const seen = new Set<string>();
  const retryableCodes: string[] = [];
  for (const code of codes) {
    if (typeof code !== "string" || code.length === 0 || Buffer.byteLength(code) > 64 || seen.has(code)) {
      throw new RelayJsonError("retryableCodes must be unique non-empty strings", "retry.retryableCodes");
    }
    seen.add(code);
    retryableCodes.push(code);
  }
  return { maxAttempts, initialDelayMs, maxDelayMs, multiplier, jitter, retryableCodes };
}
