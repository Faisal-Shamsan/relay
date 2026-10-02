/**
 * Relay JSON (spec §3.1).
 *
 * Canonical form is RFC 8785 JCS over a value that already passed Relay's
 * stricter allowed-value rules. JSON.stringify is used only to encode an
 * already validated string or finite number, never to decide whether a value
 * is allowed.
 *
 * Depth: the root value counts as depth 1. Each nested array element or
 * object property is one level deeper. Depth 64 is allowed; 65 is rejected.
 * The specification does not say whether the root counts. This file records
 * that choice. Values are never truncated to fit the limit.
 *
 * Unsafe integers are rejected. They are not rewritten into decimal strings.
 * An integer is unsafe when it is outside Number.MIN_SAFE_INTEGER ..
 * Number.MAX_SAFE_INTEGER. The specification does not give a second cutoff.
 * Non-integer finite numbers are allowed. No Unicode normalization is applied.
 */

export type Json = null | boolean | number | string | Json[] | { [key: string]: Json };

export const CANONICAL_UTF8_LIMIT = 1024 * 1024;
export const DEPTH_LIMIT = 64;
export const HTTP_BODY_LIMIT = 4 * 1024 * 1024;
export const STEP_KEY_BYTE_LIMIT = 256;

export class RelayJsonError extends Error {
  readonly code = "Serialization";

  constructor(
    message: string,
    readonly path: string,
  ) {
    super(message);
    this.name = "RelayJsonError";
  }
}

const MAX_SAFE = BigInt(Number.MAX_SAFE_INTEGER);
const MIN_SAFE = BigInt(Number.MIN_SAFE_INTEGER);

export function parseRelayJson(text: string): Json {
  const parser = new Parser(text);
  const value = parser.parseValue("$");
  parser.skipWs();
  if (!parser.done()) {
    throw new RelayJsonError("trailing data", "$");
  }
  return finish(value);
}

export function validateRelayJson(value: unknown): Json {
  const canonical = canonicalizeUnknown(value, "$", 1, new WeakSet());
  return finishFromCanonical(canonical);
}

export function canonicalRelayJson(value: Json): string {
  return canonicalizeUnknown(value, "$", 1, new WeakSet());
}

export function stepKeyError(key: string): RelayJsonError | null {
  if (Buffer.byteLength(key, "utf8") < 1 || Buffer.byteLength(key, "utf8") > STEP_KEY_BYTE_LIMIT) {
    return new RelayJsonError("step key must be 1 to 256 bytes", "$.stepKey");
  }
  if (key.includes("\u0000")) {
    return new RelayJsonError("step key must not contain U+0000", "$.stepKey");
  }
  return null;
}

function finish(value: Json): Json {
  const canonical = canonicalRelayJson(value);
  return finishFromCanonical(canonical);
}

function finishFromCanonical(canonical: string): Json {
  if (Buffer.byteLength(canonical, "utf8") > CANONICAL_UTF8_LIMIT) {
    throw new RelayJsonError("canonical UTF-8 value exceeds 1 MiB", "$");
  }
  return JSON.parse(canonical) as Json;
}

function canonicalizeUnknown(value: unknown, path: string, depth: number, seen: WeakSet<object>): string {
  if (depth > DEPTH_LIMIT) {
    throw new RelayJsonError("depth limit exceeded", path);
  }
  if (value === null) return "null";
  if (value === undefined) {
    throw new RelayJsonError("undefined is not Relay JSON", path);
  }
  switch (typeof value) {
    case "boolean":
      return value ? "true" : "false";
    case "string":
      assertRelayString(value, path);
      return JSON.stringify(value);
    case "number":
      return canonicalizeNumber(value, path);
    case "bigint":
      throw new RelayJsonError("bigint is not Relay JSON", path);
    case "function":
      throw new RelayJsonError("function is not Relay JSON", path);
    case "symbol":
      throw new RelayJsonError("symbol is not Relay JSON", path);
    case "object":
      return canonicalizeContainer(value, path, depth, seen);
    default:
      throw new RelayJsonError("unsupported value", path);
  }
}

function canonicalizeNumber(value: number, path: string): string {
  if (Object.is(value, -0)) return "0";
  if (!Number.isFinite(value)) {
    throw new RelayJsonError("non-finite number is not Relay JSON", path);
  }
  if (Number.isInteger(value) && !Number.isSafeInteger(value)) {
    throw new RelayJsonError("unsafe integer is not Relay JSON", path);
  }
  return JSON.stringify(value);
}

function canonicalizeContainer(value: object, path: string, depth: number, seen: WeakSet<object>): string {
  if (seen.has(value)) {
    throw new RelayJsonError("cycle is not Relay JSON", path);
  }
  if (value instanceof Date) throw new RelayJsonError("Date is not Relay JSON", path);
  if (value instanceof Map) throw new RelayJsonError("Map is not Relay JSON", path);
  if (value instanceof Set) throw new RelayJsonError("Set is not Relay JSON", path);
  if (value instanceof Error) throw new RelayJsonError("Error is not Relay JSON", path);
  if (Buffer.isBuffer(value)) throw new RelayJsonError("Buffer is not Relay JSON", path);
  if (ArrayBuffer.isView(value)) throw new RelayJsonError("typed array is not Relay JSON", path);
  if (value instanceof ArrayBuffer) throw new RelayJsonError("ArrayBuffer is not Relay JSON", path);

  if (Array.isArray(value)) {
    if (Object.getPrototypeOf(value) !== Array.prototype) {
      throw new RelayJsonError("custom array prototype is not Relay JSON", path);
    }
    if (Object.getOwnPropertySymbols(value).length > 0) {
      throw new RelayJsonError("symbol keys are not Relay JSON", path);
    }
    for (let i = 0; i < value.length; i += 1) {
      if (!Object.hasOwn(value, i)) {
        throw new RelayJsonError("sparse array is not Relay JSON", path);
      }
    }
    seen.add(value);
    const parts = value.map((item, index) => canonicalizeUnknown(item, `${path}[${index}]`, depth + 1, seen));
    seen.delete(value);
    return `[${parts.join(",")}]`;
  }

  if (!isPlainDataObject(value)) {
    throw new RelayJsonError("object is not a plain JSON object", path);
  }
  seen.add(value);
  const keys = Object.keys(value).sort(compareUtf16);
  const parts = keys.map((key) => {
    assertRelayString(key, path);
    const child = canonicalizeUnknown((value as Record<string, unknown>)[key], `${path}.${key}`, depth + 1, seen);
    return `${JSON.stringify(key)}:${child}`;
  });
  seen.delete(value);
  return `{${parts.join(",")}}`;
}

function isPlainDataObject(value: object): boolean {
  const proto = Object.getPrototypeOf(value);
  if (proto !== Object.prototype && proto !== null) return false;
  if (Object.getOwnPropertySymbols(value).length > 0) return false;
  for (const key of Object.getOwnPropertyNames(value)) {
    const desc = Object.getOwnPropertyDescriptor(value, key);
    if (!desc || !desc.enumerable || typeof desc.get === "function" || typeof desc.set === "function") {
      return false;
    }
  }
  return true;
}

function assertRelayString(value: string, path: string): void {
  for (let i = 0; i < value.length; i += 1) {
    const code = value.charCodeAt(i);
    if (code === 0) throw new RelayJsonError("U+0000 is not Relay JSON", path);
    if (code >= 0xd800 && code <= 0xdbff) {
      const next = value.charCodeAt(i + 1);
      if (!(next >= 0xdc00 && next <= 0xdfff)) {
        throw new RelayJsonError("unpaired surrogate is not Relay JSON", path);
      }
      i += 1;
      continue;
    }
    if (code >= 0xdc00 && code <= 0xdfff) {
      throw new RelayJsonError("unpaired surrogate is not Relay JSON", path);
    }
  }
}

function compareUtf16(left: string, right: string): number {
  if (left < right) return -1;
  if (left > right) return 1;
  return 0;
}

class Parser {
  private index = 0;

  constructor(private readonly text: string) {}

  done(): boolean {
    return this.index >= this.text.length;
  }

  parseValue(path: string): Json {
    this.skipWs();
    if (this.done()) throw new RelayJsonError("unexpected end", path);
    const char = this.text[this.index];
    if (char === "{") return this.parseObject(path);
    if (char === "[") return this.parseArray(path);
    if (char === '"') return this.parseString(path);
    if (char === "t") return this.consumeLiteral("true", true, path);
    if (char === "f") return this.consumeLiteral("false", false, path);
    if (char === "n") return this.consumeLiteral("null", null, path);
    if (char === "-" || (char !== undefined && char >= "0" && char <= "9")) return this.parseNumber(path);
    throw new RelayJsonError("invalid JSON", path);
  }

  skipWs(): void {
    while (this.index < this.text.length) {
      const char = this.text[this.index];
      if (char !== " " && char !== "\n" && char !== "\r" && char !== "\t") break;
      this.index += 1;
    }
  }

  private parseObject(path: string): Json {
    this.index += 1;
    this.skipWs();
    const result: { [key: string]: Json } = {};
    const seen = new Set<string>();
    if (this.peek("}")) {
      this.index += 1;
      return result;
    }
    while (true) {
      if (this.text[this.index] !== '"') throw new RelayJsonError("object key must be a string", path);
      const key = this.parseString(path);
      if (seen.has(key)) throw new RelayJsonError("duplicate object key", `${path}.${key}`);
      seen.add(key);
      this.skipWs();
      if (!this.peek(":")) throw new RelayJsonError("expected colon", path);
      this.index += 1;
      result[key] = this.parseValue(`${path}.${key}`);
      this.skipWs();
      if (this.peek("}")) {
        this.index += 1;
        return result;
      }
      if (!this.peek(",")) throw new RelayJsonError("expected comma", path);
      this.index += 1;
      this.skipWs();
    }
  }

  private parseArray(path: string): Json {
    this.index += 1;
    this.skipWs();
    const result: Json[] = [];
    if (this.peek("]")) {
      this.index += 1;
      return result;
    }
    while (true) {
      result.push(this.parseValue(`${path}[${result.length}]`));
      this.skipWs();
      if (this.peek("]")) {
        this.index += 1;
        return result;
      }
      if (!this.peek(",")) throw new RelayJsonError("expected comma", path);
      this.index += 1;
      this.skipWs();
    }
  }

  private parseString(path: string): string {
    this.index += 1;
    let out = "";
    while (this.index < this.text.length) {
      const char = this.text[this.index];
      if (char === '"') {
        this.index += 1;
        assertRelayString(out, path);
        return out;
      }
      if (char === "\\") {
        this.index += 1;
        const escaped = this.text[this.index];
        this.index += 1;
        if (escaped === '"' || escaped === "\\" || escaped === "/") out += escaped;
        else if (escaped === "b") out += "\b";
        else if (escaped === "f") out += "\f";
        else if (escaped === "n") out += "\n";
        else if (escaped === "r") out += "\r";
        else if (escaped === "t") out += "\t";
        else if (escaped === "u") {
          const hex = this.text.slice(this.index, this.index + 4);
          if (!/^[0-9a-fA-F]{4}$/.test(hex)) throw new RelayJsonError("invalid unicode escape", path);
          this.index += 4;
          out += String.fromCharCode(Number.parseInt(hex, 16));
        } else {
          throw new RelayJsonError("invalid escape", path);
        }
        continue;
      }
      if ((char !== undefined && char.charCodeAt(0) < 0x20) || char === undefined) {
        throw new RelayJsonError("unescaped control character", path);
      }
      out += char;
      this.index += 1;
    }
    throw new RelayJsonError("unterminated string", path);
  }

  private parseNumber(path: string): number {
    const start = this.index;
    if (this.peek("-")) this.index += 1;
    if (this.peek("0")) {
      this.index += 1;
    } else if (this.text[this.index] !== undefined && this.text[this.index]! >= "1" && this.text[this.index]! <= "9") {
      while (this.isDigit()) this.index += 1;
    } else {
      throw new RelayJsonError("invalid number", path);
    }
    if (this.peek(".")) {
      this.index += 1;
      if (!this.isDigit()) throw new RelayJsonError("invalid number", path);
      while (this.isDigit()) this.index += 1;
    }
    if (this.peek("e") || this.peek("E")) {
      this.index += 1;
      if (this.peek("+") || this.peek("-")) this.index += 1;
      if (!this.isDigit()) throw new RelayJsonError("invalid number", path);
      while (this.isDigit()) this.index += 1;
    }
    const literal = this.text.slice(start, this.index);
    if (/^-?(0|[1-9][0-9]*)$/.test(literal)) {
      const integer = BigInt(literal);
      if (integer > MAX_SAFE || integer < MIN_SAFE) {
        throw new RelayJsonError("unsafe integer is not Relay JSON", path);
      }
    }
    const value = Number(literal);
    if (!Number.isFinite(value)) throw new RelayJsonError("non-finite number is not Relay JSON", path);
    if (Number.isInteger(value) && !Number.isSafeInteger(value)) {
      throw new RelayJsonError("unsafe integer is not Relay JSON", path);
    }
    return Object.is(value, -0) ? 0 : value;
  }

  private consumeLiteral<T extends Json>(literal: string, value: T, path: string): T {
    if (this.text.slice(this.index, this.index + literal.length) !== literal) {
      throw new RelayJsonError("invalid JSON", path);
    }
    this.index += literal.length;
    return value;
  }

  private peek(char: string): boolean {
    return this.text[this.index] === char;
  }

  private isDigit(): boolean {
    const char = this.text[this.index];
    return char !== undefined && char >= "0" && char <= "9";
  }
}
