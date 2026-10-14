export const dynamic = 'force-dynamic';

async function proxy(request, context) {
  const { path } = await context.params;
  const route = path.join('/');
  const read = /^(stats|workers|jobs|dead-letter|health|ready|metrics)$/.test(route) || /^jobs\/[a-f0-9]{32}$/.test(route);
  const write = route === 'jobs' || /^dead-letter\/[a-f0-9]{32}\/retry$/.test(route);
  if (!(request.method === 'GET' ? read : write)) return Response.json({ error: 'Unknown endpoint' }, { status: 404 });
  // Next may normalize request.url to localhost behind its internal server.
  // Compare to the incoming Host, which browsers cannot override in a fetch.
  const origin = request.headers.get('origin');
  let sameOrigin = true;
  try { if (origin) sameOrigin = new URL(origin).host === request.headers.get('host'); }
  catch { sameOrigin = false; }
  if (request.method === 'POST' && !sameOrigin) {
    return Response.json({ error: 'Cross-origin writes are not allowed' }, { status: 403 });
  }
  const backend = process.env.DJQ_API_URL || 'http://127.0.0.1:5010';
  const url = new URL('/' + route, backend);
  url.search = new URL(request.url).search;
  const headers = { 'Content-Type': 'application/json' };
  const authorization = request.headers.get('authorization');
  if (authorization) headers.Authorization = authorization;
  try {
    const body = request.method === 'POST' ? await request.text() : undefined;
    if (body && new TextEncoder().encode(body).length > 256 * 1024) {
      return Response.json({ error: 'Payload exceeds 256 KiB' }, { status: 413 });
    }
    const response = await fetch(url, { method: request.method, headers, body, cache: 'no-store', signal: AbortSignal.timeout(8000) });
    return new Response(await response.text(), {
      status: response.status,
      headers: { 'Content-Type': response.headers.get('content-type') || 'application/json', 'Cache-Control': 'no-store' },
    });
  } catch {
    return Response.json({ error: 'Broker unavailable. Start the queue server and try again.' }, { status: 503 });
  }
}
export const GET = proxy;
export const POST = proxy;
