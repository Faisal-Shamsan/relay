import { DEV_CLIENT_KEY, DEV_WORKER_KEY } from "../packages/server/dist/src/dev-keys.js";

const original = globalThis.fetch;

function relayPath(input) {
  const url = input instanceof Request ? input.url : String(input);
  let parsed;
  try {
    parsed = new URL(url);
  } catch {
    return null;
  }
  if (parsed.hostname !== "127.0.0.1" || parsed.port === "1") return null;
  if (parsed.pathname.startsWith("/v1/") || parsed.pathname.startsWith("/worker/")) return parsed.pathname;
  return null;
}

globalThis.fetch = async (input, init) => {
  const path = relayPath(input);
  const headers = new Headers(init?.headers ?? (input instanceof Request ? input.headers : undefined));
  if (path && headers.has("x-relay-test-anonymous")) {
    headers.delete("x-relay-test-anonymous");
    headers.delete("authorization");
  } else if (path && !headers.has("authorization")) {
    const key = path.startsWith("/worker/") ? DEV_WORKER_KEY : DEV_CLIENT_KEY;
    headers.set("authorization", `Bearer ${key}`);
  }
  return original(input, { ...init, headers });
};
