import assert from 'node:assert/strict';
import test from 'node:test';
import { readFileSync } from 'node:fs';
import Ajv2020 from 'ajv/dist/2020.js';

const contract = JSON.parse(readFileSync(new URL('../../../contracts/management/v1/openapi.yaml', import.meta.url), 'utf8'));
const validate = new Ajv2020({ strict: true }).compile(contract.components.schemas.AcquisitionJobCommand);
const command = { kind: 'acquisition', scope_id: '04'.repeat(16), deadline_us: '120000000', expected_config_revision: '1', expected_policy_revision: '1' };

test('finite command schema preserves the closed bounded string duration', () => {
  for (const duration of ['1', '9999999999', '10000000000', '79999999999', '85999999999', '86000000000', '86399999999', '86400000000']) {
    assert.equal(validate({ ...command, deadline_us: duration }), true, duration);
  }
  for (const duration of ['0', '-1', '01', '86400000001', '87000000000', '99999999999', 1, null]) {
    assert.equal(validate({ ...command, deadline_us: duration }), false, String(duration));
  }
  for (const patch of [{ kind: 'export' }, { scope_id: '00'.repeat(16) }, { scope_id: 'AB'.repeat(16) }, { endpoint: 'wss://unselected.example' }, { destination: '/tmp/private' }]) {
    assert.equal(validate({ ...command, ...patch }), false);
  }
  for (const field of Object.keys(command)) {
    const incomplete: Record<string, unknown> = { ...command }; delete incomplete[field];
    assert.equal(validate(incomplete), false, field);
  }
});
