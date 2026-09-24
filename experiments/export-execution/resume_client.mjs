// Actual TypeScript client against installed HTTP. Node supplies a bounded
// in-memory cookie jar and fixed Origin; this is not browser qualification.
import assert from 'node:assert/strict';
import { createInterface } from 'node:readline';
import { createManagementClient, ManagementFailure } from '../../packages/api-client/src/management.ts';

assert.equal(process.version, 'v26.4.0');
const job = process.argv[2];
assert.match(job, /^[0-9a-f]{32}$/);
const origin = 'http://127.0.0.1:18080';
const cookies = new Map();
let context;
const traffic = [];
const lifetime = setTimeout(() => { console.error('typed resume fixture lifetime exceeded'); process.exit(3); }, 90_000);
const client = createManagementClient({
  cookie: () => [...cookies].map(([k, v]) => `${k}=${v}`).join('; '),
  fetch: async (path, init) => {
    assert.equal(typeof path, 'string');
    assert(['/management/v1/sessions', '/management/v1/me', `/management/v1/jobs/${job}/resume`].includes(path));
    assert.equal(init.method, path.endsWith('/me') ? 'GET' : 'POST');
    assert(traffic.length < 16);
    const headers = new Headers(init.headers);
    headers.set('Origin', origin);
    if (cookies.size) headers.set('Cookie', [...cookies].map(([k, v]) => `${k}=${v}`).join('; '));
    // Count the client's real fetch submissions even when no headers arrive.
    // This does not claim visibility into transport-internal packet attempts.
    const observation = { path, method: init.method, status: null };
    traffic.push(observation);
    const response = await fetch(origin + path, { ...init, headers });
    observation.status = response.status;
    const changed = response.headers.getSetCookie();
    assert(changed.length <= 2);
    if (path.endsWith('/resume')) {
      assert.equal(changed.length, 0);
      assert.equal(response.headers.get('cache-control'), 'no-store');
    }
    for (const header of changed) {
      const pair = header.split(';', 1)[0];
      const equals = pair.indexOf('=');
      const name = pair.slice(0, equals), value = pair.slice(equals + 1);
      assert(['BigBrotrLocalSession', 'BigBrotrLocalCsrf'].includes(name));
      assert.match(value, /^(?:[0-9a-f]{64})?$/);
      if (value) cookies.set(name, value); else cookies.delete(name);
    }
    return response;
  }
});
const emit = value => console.log(JSON.stringify(value));
const input = createInterface({ input: process.stdin, crlfDelay: Infinity });
const names = new Set();
let stopped = false;
emit({ event: 'ready', node: process.version });
try {
  for await (const line of input) {
    assert(Buffer.byteLength(line) <= 2048 && names.size < 16);
    const request = JSON.parse(line);
    assert(typeof request.name === 'string' && /^[a-z-]{1,64}$/.test(request.name) && !names.has(request.name));
    names.add(request.name);
    if (request.operation === 'stop') {
      assert.deepEqual(Object.keys(request).sort(), ['name', 'operation']);
      emit({ event: request.name, traffic }); stopped = true; break;
    }
    const count = traffic.length;
    try {
      let value;
      if (request.operation === 'login') {
        assert.deepEqual(Object.keys(request).sort(), ['name', 'operation']);
        value = context = await client.login({ username: 'alice', password: 'Independent owner password 2026' });
      } else if (request.operation === 'inspect') {
        assert.deepEqual(Object.keys(request).sort(), ['name', 'operation']);
        value = context = await client.inspect();
      } else if (request.operation === 'resume-current') {
        assert.deepEqual(Object.keys(request).sort(), ['key', 'name', 'operation', 'revision']);
        assert(context);
        value = await client.resumeExport(job, request.key, {
          expected_job_revision: request.revision,
          expected_config_revision: context.config_revision,
          expected_policy_revision: context.policy_revision
        });
      } else {
        assert.equal(request.operation, 'resume');
        assert.deepEqual(Object.keys(request).sort(), ['command', 'key', 'name', 'operation']);
        value = await client.resumeExport(job, request.key, request.command);
      }
      assert.equal(traffic.length, count + 1);
      emit({ event: request.name, status: traffic.at(-1).status, observation: value, requests: 1 });
    } catch (error) {
      if (!(error instanceof ManagementFailure)) throw error;
      assert.equal(traffic.length, count + 1);
      emit({ event: request.name, status: error.api?.status ?? null, failure: error.kind, requests: 1 });
    }
  }
  assert(stopped, 'missing explicit stop');
} finally {
  cookies.clear(); context = undefined; input.close(); clearTimeout(lifetime);
}
