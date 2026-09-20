import assert from 'node:assert/strict';
import { test } from 'node:test';
import { WireStatuses } from './wire-status.mjs';

test('selected wire statuses survive either CDP event order without unrelated statuses', () => {
  for (const responseFirst of [false, true]) {
    const wire = new WireStatuses();
    const request = () => { wire.request('document', false); wire.request('command', true); };
    const response = () => { wire.response('document', 200); wire.response('command', 403); };
    if (responseFirst) { response(); request(); } else { request(); response(); }
    assert.deepEqual(wire.statuses, [403]);
    assert.equal(wire.overflow, false);
  }
});

test('pending requests and selected output have finite capacities', () => {
  const pending = new WireStatuses();
  for (let i = 0; i < 33; i++) pending.response(String(i), 200);
  pending.request('32', true);
  assert.equal(pending.overflow, true);
  assert.deepEqual(pending.statuses, []);
  const output = new WireStatuses();
  for (let i = 0; i < 9; i++) {
    output.request(String(i), true);
    output.response(String(i), 403);
  }
  assert.equal(output.overflow, true);
  assert.deepEqual(output.statuses, Array(8).fill(403));
});
