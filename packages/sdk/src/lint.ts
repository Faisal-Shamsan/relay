/**
 * Syntactic checks for workflow code outside step callbacks (spec §4.4).
 *
 * The check reads source text. It does not follow calls, so a helper that
 * hides `Date.now()` is not reported. Clock, random, UUID, environment, I/O,
 * shared globals, and Promise fan-out inside a `ctx.step` callback or a
 * parallel child's `run` function are allowed, because those callbacks are
 * the place for effects.
 */

export type LintRule = "clock" | "random" | "uuid" | "env" | "io" | "shared" | "concurrency";

export interface LintFinding {
  rule: LintRule;
  line: number;
  column: number;
  message: string;
}

const RULES: ReadonlyArray<{ rule: LintRule; pattern: RegExp; message: string }> = [
  {
    rule: "clock",
    pattern: /\bnew\s+Date\b|\bDate\s*\.\s*now\s*\(|\bDate\s*\(|\bperformance\s*\.\s*now\b/g,
    message: "workflow code must not read the clock; use ctx.now(key)",
  },
  {
    rule: "random",
    pattern: /\bMath\s*\.\s*random\b/g,
    message: "workflow code must not call Math.random; use ctx.random(key)",
  },
  {
    rule: "uuid",
    pattern: /\brandomUUID\s*\(/g,
    message: "workflow code must not generate a UUID; use ctx.uuid(key)",
  },
  {
    rule: "env",
    pattern: /\bprocess\s*\.\s*env\b/g,
    message: "workflow code must not read the environment",
  },
  {
    rule: "io",
    pattern: /\bfetch\s*\(|\bsetTimeout\s*\(|\bsetInterval\s*\(/g,
    message: "workflow code must not perform I/O; put it in a step",
  },
  {
    rule: "shared",
    pattern: /\bglobalThis\b|\bglobal\s*\./g,
    message: "workflow code must not touch shared global state",
  },
  {
    rule: "concurrency",
    pattern: /\bPromise\s*\.\s*(?:all|allSettled|race|any)\s*\(/g,
    message: "workflow code must not race commands; use ctx.step.parallel",
  },
];

export function lintWorkflow(input: string | ((...args: never[]) => unknown)): LintFinding[] {
  const source = typeof input === "string" ? input : Function.prototype.toString.call(input);
  const masked = mask(source);
  const ignored = callbackRanges(masked);
  const findings: LintFinding[] = [];
  for (const rule of RULES) {
    rule.pattern.lastIndex = 0;
    let match: RegExpExecArray | null;
    while ((match = rule.pattern.exec(masked)) !== null) {
      const index = match.index;
      if (ignored.some(([start, end]) => index >= start && index < end)) continue;
      const at = position(source, index);
      findings.push({ rule: rule.rule, line: at.line, column: at.column, message: rule.message });
    }
  }
  findings.sort((a, b) => a.line - b.line || a.column - b.column || a.rule.localeCompare(b.rule));
  return findings;
}

function callbackRanges(source: string): Array<[number, number]> {
  const ranges: Array<[number, number]> = [];
  const step = /\.step\s*\(/g;
  let match: RegExpExecArray | null;
  while ((match = step.exec(source)) !== null) {
    const open = match.index + match[0].length - 1;
    const body = lastFunctionArgument(source, open);
    if (body) ranges.push(body);
    step.lastIndex = open + 1;
  }
  const run = /\brun\s*:/g;
  while ((match = run.exec(source)) !== null) {
    const found = functionBodyAt(source, skipWs(source, match.index + match[0].length));
    if (found) ranges.push(found.range);
  }
  return ranges;
}

function lastFunctionArgument(source: string, openParen: number): [number, number] | null {
  let i = openParen + 1;
  let depth = 1;
  let last: [number, number] | null = null;
  while (i < source.length && depth > 0) {
    if (depth === 1) {
      const found = functionBodyAt(source, i);
      if (found) {
        last = found.range;
        i = found.end;
        continue;
      }
    }
    const ch = source[i];
    if (ch === "(" || ch === "{" || ch === "[") depth += 1;
    else if (ch === ")" || ch === "}" || ch === "]") {
      depth -= 1;
      if (depth === 0) break;
    }
    i += 1;
  }
  return last;
}

function functionBodyAt(source: string, i: number): { range: [number, number]; end: number } | null {
  let j = i;
  if (source.startsWith("async", j) && isBoundary(source, j, 5)) {
    j = skipWs(source, j + 5);
  }
  if (source.startsWith("function", j) && isBoundary(source, j, 8)) {
    j = skipWs(source, j + 8);
    if (isIdentStart(source[j] ?? "")) {
      j = skipIdent(source, j);
      j = skipWs(source, j);
    }
    if (source[j] !== "(") return null;
    j = skipBalanced(source, j, "(", ")");
    j = skipWs(source, j);
    if (source[j] !== "{") return null;
    const end = skipBalanced(source, j, "{", "}");
    return { range: [j + 1, end - 1], end };
  }
  if (source[j] === "(") {
    const afterParams = skipBalanced(source, j, "(", ")");
    const arrow = skipWs(source, afterParams);
    if (!source.startsWith("=>", arrow)) return null;
    return arrowBody(source, arrow + 2);
  }
  if (isIdentStart(source[j] ?? "")) {
    const after = skipIdent(source, j);
    const arrow = skipWs(source, after);
    if (!source.startsWith("=>", arrow)) return null;
    return arrowBody(source, arrow + 2);
  }
  return null;
}

function arrowBody(source: string, afterArrow: number): { range: [number, number]; end: number } {
  const body = skipWs(source, afterArrow);
  if (source[body] === "{") {
    const end = skipBalanced(source, body, "{", "}");
    return { range: [body + 1, end - 1], end };
  }
  const end = expressionEnd(source, body);
  return { range: [body, end], end };
}

function expressionEnd(source: string, start: number): number {
  let depth = 0;
  for (let i = start; i < source.length; i += 1) {
    const ch = source[i];
    if (ch === "(" || ch === "{" || ch === "[") depth += 1;
    else if (ch === ")" || ch === "}" || ch === "]") {
      if (depth === 0) return i;
      depth -= 1;
    } else if (ch === "," && depth === 0) return i;
  }
  return source.length;
}

function skipBalanced(source: string, openIndex: number, open: string, close: string): number {
  let depth = 0;
  for (let i = openIndex; i < source.length; i += 1) {
    if (source[i] === open) depth += 1;
    else if (source[i] === close) {
      depth -= 1;
      if (depth === 0) return i + 1;
    }
  }
  return source.length;
}

function skipWs(source: string, i: number): number {
  while (i < source.length && /\s/.test(source[i] ?? "")) i += 1;
  return i;
}

function skipIdent(source: string, i: number): number {
  while (i < source.length && isIdent(source[i] ?? "")) i += 1;
  return i;
}

function isBoundary(source: string, index: number, length: number): boolean {
  const before = index === 0 ? "" : source[index - 1] ?? "";
  const after = source[index + length] ?? "";
  return !isIdent(before) && !isIdent(after);
}

function isIdentStart(ch: string): boolean {
  return /[A-Za-z_$]/.test(ch);
}

function isIdent(ch: string): boolean {
  return /[A-Za-z0-9_$]/.test(ch);
}

function position(source: string, index: number): { line: number; column: number } {
  let line = 1;
  let column = 1;
  for (let i = 0; i < index; i += 1) {
    if (source[i] === "\n") {
      line += 1;
      column = 1;
    } else {
      column += 1;
    }
  }
  return { line, column };
}

function mask(source: string): string {
  const chars = Array.from(source);
  maskRegion(chars, 0, false);
  return chars.join("");
}

function maskRegion(chars: string[], start: number, interpolation: boolean): number {
  let i = start;
  while (i < chars.length) {
    if (chars[i] === "/" && chars[i + 1] === "/") {
      while (i < chars.length && chars[i] !== "\n") {
        chars[i] = " ";
        i += 1;
      }
      continue;
    }
    if (chars[i] === "/" && chars[i + 1] === "*") {
      chars[i] = " ";
      chars[i + 1] = " ";
      i += 2;
      while (i < chars.length && !(chars[i] === "*" && chars[i + 1] === "/")) {
        if (chars[i] !== "\n") chars[i] = " ";
        i += 1;
      }
      if (i < chars.length) {
        chars[i] = " ";
        chars[i + 1] = " ";
        i += 2;
      }
      continue;
    }
    if (chars[i] === "'" || chars[i] === '"') {
      i = maskQuoted(chars, i, chars[i] ?? "");
      continue;
    }
    if (chars[i] === "`") {
      i = maskTemplate(chars, i);
      continue;
    }
    if (interpolation && chars[i] === "}") return i + 1;
    if (interpolation && chars[i] === "{") {
      i = maskRegion(chars, i + 1, true);
      continue;
    }
    i += 1;
  }
  return i;
}

function maskQuoted(chars: string[], start: number, quote: string): number {
  chars[start] = " ";
  let i = start + 1;
  while (i < chars.length && chars[i] !== quote) {
    if (chars[i] === "\\") {
      chars[i] = " ";
      i += 1;
      if (i < chars.length && chars[i] !== "\n") chars[i] = " ";
      i += 1;
      continue;
    }
    if (chars[i] !== "\n") chars[i] = " ";
    i += 1;
  }
  if (i < chars.length) {
    chars[i] = " ";
    i += 1;
  }
  return i;
}

function maskTemplate(chars: string[], start: number): number {
  chars[start] = " ";
  let i = start + 1;
  while (i < chars.length && chars[i] !== "`") {
    if (chars[i] === "\\") {
      chars[i] = " ";
      i += 1;
      if (i < chars.length && chars[i] !== "\n") chars[i] = " ";
      i += 1;
      continue;
    }
    if (chars[i] === "$" && chars[i + 1] === "{") {
      i += 2;
      i = maskRegion(chars, i, true);
      continue;
    }
    if (chars[i] !== "\n") chars[i] = " ";
    i += 1;
  }
  if (i < chars.length) {
    chars[i] = " ";
    i += 1;
  }
  return i;
}
