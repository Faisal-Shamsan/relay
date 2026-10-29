import { clientKey, serverUrl } from "../../../lib/api";

export const dynamic = "force-dynamic";

export async function GET(request: Request, context: { params: Promise<{ path: string[] }> }): Promise<Response> {
  const { path } = await context.params;
  const incoming = new URL(request.url);
  const target = new URL(`${serverUrl()}/${path.join("/")}${incoming.search}`);
  const upstream = await fetch(target, {
    cache: "no-store",
    headers: {
      accept: request.headers.get("accept") ?? "text/event-stream",
      authorization: `Bearer ${clientKey()}`,
    },
  });
  const headers = new Headers();
  const type = upstream.headers.get("content-type");
  if (type) headers.set("content-type", type);
  headers.set("cache-control", "no-cache");
  return new Response(upstream.body, { status: upstream.status, headers });
}
