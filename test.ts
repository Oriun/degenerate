import test from 'node:test';
import assert from 'node:assert/strict';
import { createRateLimitedClient } from '@oriun/degenerate';

test('endpoint patterns map to API groups; customKey maps to custom groups', async (t) => {
  const fetchMock = new FetchMock();
  fetchMock.queue('https://api.test/v1/auth/login', 200, { ok: true });
  fetchMock.queue('https://api.test/v1/search/items', 200, { ok: true });

  const seen: Array<{ apiGroup: string; customGroup: string }> = [];

  const client = createRateLimitedClient({
    limits: {
      api: { default: { max: 10, intervalMs: 100 } },
      custom: { default: { max: 10, intervalMs: 100 } },
    },
    endpointGroups: [
      { method: 'POST', url: '/v1/auth/**', group: 'auth' },
      { method: 'GET', url: '/v1/search/**', group: 'search' },
    ],
    customKey: ({ init }) =>
      (init?.headers instanceof Headers
        ? init.headers.get('X-User-Tier')
        : (init?.headers as Record<string, string> | undefined)?.['X-User-Tier']) || 'free',
    fetchImpl: fetchMock.fetch,
    hooks: {
      beforeRequest: ({ apiGroup, customGroup }) => {
        seen.push({ apiGroup, customGroup });
      },
    },
  });

  await client.request('https://api.test/v1/auth/login', {
    method: 'POST',
    init: { headers: { 'X-User-Tier': 'premium' } },
  });

  await client.request('https://api.test/v1/search/items', {
    method: 'GET',
    init: { headers: { 'X-User-Tier': 'free' } },
  });

  assert.equal(seen.length, 2);
  assert.deepEqual(seen[0], { apiGroup: 'auth', customGroup: 'premium' });
  assert.deepEqual(seen[1], { apiGroup: 'search', customGroup: 'free' });
});


test('rate limiter enforces sliding window (serializes start times)', async () => {
  const fetchMock = new FetchMock();
  // Respond quickly so we can observe spacing from limiter, not network
  fetchMock.queue('https://api.test/v1/a', 200, { ok: true }, 5);
  fetchMock.queue('https://api.test/v1/a', 200, { ok: true }, 5);

  const starts: number[] = [];

  const client = createRateLimitedClient({
    limits: {
      // allow 1 request per 120ms at API level; custom allows many
      api: { default: { max: 1, intervalMs: 120 } },
      custom: { default: { max: 10, intervalMs: 10 } },
    },
    endpointGroups: [{ method: '*', url: '/v1/**', group: 'g' }],
    fetchImpl: fetchMock.fetch,
    hooks: {
      beforeRequest: () => starts.push(now()),
    },
  });

  // Fire concurrently; second should be delayed by ~interval
  const p1 = client.request('https://api.test/v1/a', { method: 'GET' });
  const p2 = client.request('https://api.test/v1/a', { method: 'GET' });
  await Promise.all([p1, p2]);

  assert.equal(starts.length, 2);
  const delta = starts[1] - starts[0];
  assert.ok(
    delta >= 100, // allow a bit of slack for timing
    `expected >=100ms spacing between starts, got ${delta}ms`
  );
});


test('retries on 429 then succeeds', async () => {
  const fetchMock = new FetchMock();
  fetchMock.queue('https://api.test/v1/retry', 429, { err: 'rate' });
  fetchMock.queue('https://api.test/v1/retry', 200, { ok: true });

  let attempts = 0;

  const client = createRateLimitedClient({
    limits: {
      api: { default: { max: 10, intervalMs: 10 } },
      custom: { default: { max: 10, intervalMs: 10 } },
    },
    endpointGroups: [{ method: '*', url: '/v1/**', group: 'default' }],
    fetchImpl: async (url, init) => {
      attempts++;
      return fetchMock.fetch(url, init);
    },
    retry: { retries: 2, baseDelayMs: 10, maxDelayMs: 20 },
  });

  const res = await client.request('https://api.test/v1/retry', { method: 'GET' });
  assert.equal(attempts >= 2, true);
  assert.equal(res.status, 200);
});


test('per-request timeout aborts with AbortError (no retries)', async () => {
  const fetchMock = new FetchMock();
  // Single slow response (200) but with delay so we trigger timeout
  fetchMock.queue('https://api.test/v1/slow', 200, { ok: true }, 200);

  const client = createRateLimitedClient({
    limits: {
      api: { default: { max: 10, intervalMs: 10 } },
      custom: { default: { max: 10, intervalMs: 10 } },
    },
    endpointGroups: [{ method: '*', url: '/v1/**', group: 'default' }],
    fetchImpl: fetchMock.fetch,
    retry: { retries: 0 },
    defaultTimeoutMs: 50,
  });

  await assert.rejects(
    () => client.request('https://api.test/v1/slow', { method: 'GET' }),
    (err: any) => err?.name === 'AbortError',
  );
});


class FetchMock {
  calls: Array<{ url: string; init?: RequestInit; at: number }>; 
  private handlers: Map<string, Array<() => Promise<Response>>>;

  constructor() {
    this.calls = [];
    this.handlers = new Map();
  }

  /** Queue a response for a URL that resolves after `delayMs`. */
  queue(url: string, status: number, body: unknown, delayMs = 0) {
    const q = this.handlers.get(url) ?? [];
    q.push(async () => {
      if (delayMs) await new Promise((r) => setTimeout(r, delayMs));
      const res = new Response(
        body == null ? null : JSON.stringify(body),
        {
          status,
          headers: body == null ? undefined : { 'content-type': 'application/json' },
        }
      );
      return res;
    });
    this.handlers.set(url, q);
  }

  fetch = async (url: string | URL, init?: RequestInit): Promise<Response> => {
    const key = typeof url === 'string' ? url : url.toString();
    this.calls.push({ url: key, init, at: Date.now() });
    const q = this.handlers.get(key) ?? [];
    const handler = q.shift();
    this.handlers.set(key, q);
    if (!handler) {
      // default 200 OK empty
      return new Response(null, { status: 200 });
    }
    return handler();
  };
}

function now() {
  return Date.now();
}
