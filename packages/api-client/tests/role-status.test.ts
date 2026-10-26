import assert from 'node:assert/strict';
import test from 'node:test';
import { readFileSync } from 'node:fs';
import { validateRoles } from '../src/generated/validators.js';

const fixture = JSON.parse(readFileSync(new URL('../../../tests/fixtures/management/role-status.json', import.meta.url), 'utf8'));

test('role observation preserves unknown source and the older exact verified cut', () => {
  assert.equal(validateRoles(fixture), true);
  assert.equal(fixture.identity.config_revision, '18446744073709551615');
  assert.equal(fixture.capabilities[0].published.cut.bounds[0].last_complete_sequence, '18446744073709551615');
  assert.equal(fixture.source.cut, null);
  assert.equal(fixture.capabilities[0].state, 'stale');
});

test('role observation is a closed metadata profile without invented readiness or metrics', () => {
  for (const change of [
    (v: typeof fixture) => { v.pending_bytes = 0; },
    (v: typeof fixture) => { v.authority = 'execute'; },
    (v: typeof fixture) => { v.identity.config_revision = 9007199254740992; },
    (v: typeof fixture) => { v.roles[0].role = 'arbitrary_plugin'; },
    (v: typeof fixture) => { v.roles[0].state = 'healthy'; },
    (v: typeof fixture) => { v.roles[0].reason = 'raw connection secret'; },
    (v: typeof fixture) => { v.roles = []; },
    (v: typeof fixture) => { v.source.freshness = 'fresh'; },
    (v: typeof fixture) => { v.capabilities[0].state = 'ready'; },
    (v: typeof fixture) => { v.capabilities[0].time.age_us = -1; },
    (v: typeof fixture) => { v.capabilities[0].capability = 'private_history'; },
    (v: typeof fixture) => { v.capabilities[0].published.cut.bounds[0].owner_epoch = '01'; },
    (v: typeof fixture) => { v.capabilities[0].published.cut.bounds = Array(1025).fill(v.capabilities[0].published.cut.bounds[0]); }
  ]) {
    const value = structuredClone(fixture); change(value);
    assert.equal(validateRoles(value), false, JSON.stringify(value));
  }
});

test('role route is GET only and requires explicit CSRF with existing session security', () => {
  const contract = JSON.parse(readFileSync(new URL('../../../contracts/management/v1/openapi.yaml', import.meta.url), 'utf8'));
  const route = contract.paths['/management/v1/roles'];
  assert.deepEqual(Object.keys(route), ['get']);
  assert.deepEqual(contract.security, [{ localSession: [] }]);
  assert.equal(route.get.parameters[0].name, 'X-CSRF-Token');
  assert.equal(route.get.parameters[0].required, true);
  assert.equal(route.get.responses['200'].content['application/json'].schema.$ref, '#/components/schemas/RoleStatusObservation');
});
