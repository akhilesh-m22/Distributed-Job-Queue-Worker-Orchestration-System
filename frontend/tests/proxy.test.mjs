import { after, before, test } from 'node:test';
import assert from 'node:assert/strict';
import { createServer } from 'node:http';
import { GET, POST } from '../app/api/[...path]/route.js';

let upstream;
let base;
before(async () => {
  upstream = createServer(async (req, res) => {
    const chunks = [];
    for await (const chunk of req) chunks.push(chunk);
    res.setHeader('Content-Type', 'application/json');
    if (req.url === '/jobs/missing') { res.statusCode = 404; res.end('{}'); return; }
    if (req.headers.authorization !== 'Bearer test-key') {
      res.statusCode = 401; res.end(JSON.stringify({ error: 'Unauthorized' })); return;
    }
    res.statusCode = req.method === 'POST' ? 202 : 200;
    res.end(JSON.stringify({ url: req.url, body: Buffer.concat(chunks).toString() }));
  });
  await new Promise(resolve => upstream.listen(0, '127.0.0.1', resolve));
  base = `http://127.0.0.1:${upstream.address().port}`;
  process.env.DJQ_API_URL = base;
});
after(async () => { await new Promise(resolve => upstream.close(resolve)); delete process.env.DJQ_API_URL; });

function context(path) { return { params: Promise.resolve({ path: path.split('/') }) }; }
function request(path, options = {}) {
  return new Request(`http://localhost:3000/api/${path}`, {
    ...options,
    headers: { host: '127.0.0.1:3000', authorization: 'Bearer test-key', ...options.headers },
  });
}

test('forwards queries at runtime without caching', async () => {
  const result = await GET(request('jobs?limit=10&offset=20'), context('jobs'));
  assert.equal(result.status, 200);
  assert.equal((await result.json()).url, '/jobs?limit=10&offset=20');
  assert.equal(result.headers.get('cache-control'), 'no-store');
});

test('same-origin writes work when Next normalizes request URL to localhost', async () => {
  const result = await POST(request('jobs', { method: 'POST', headers: { origin: 'http://127.0.0.1:3000' }, body: '{"task_type":"x"}' }), context('jobs'));
  assert.equal(result.status, 202);
  assert.equal((await result.json()).body, '{"task_type":"x"}');
});

test('cross-origin and opaque-origin writes are rejected', async () => {
  for (const origin of ['https://untrusted.example', 'null']) {
    const response = await POST(request('jobs', { method: 'POST', headers: { origin }, body: '{}' }), context('jobs'));
    assert.equal(response.status, 403);
  }
});

test('proxy forwards authentication failure without injecting secrets', async () => {
  const response = await GET(new Request('http://localhost:3000/api/stats'), context('stats'));
  assert.equal(response.status, 401);
});

test('unknown paths and write targets are not proxied', async () => {
  assert.equal((await GET(request('private'), context('private'))).status, 404);
  assert.equal((await POST(request('stats', { method: 'POST' }), context('stats'))).status, 404);
});

test('oversized writes are rejected', async () => {
  const response = await POST(request('jobs', { method: 'POST', body: 'x'.repeat(262145) }), context('jobs'));
  assert.equal(response.status, 413);
});
