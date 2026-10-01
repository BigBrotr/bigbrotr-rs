import assert from 'node:assert/strict';
import test from 'node:test';
import { readFileSync } from 'node:fs';
import Ajv2020 from 'ajv/dist/2020.js';

const contract = JSON.parse(readFileSync(new URL('../../../contracts/management/v1/openapi.yaml', import.meta.url), 'utf8'));
const definitions = JSON.parse(JSON.stringify(contract.components.schemas).replaceAll('#/components/schemas/', '#/$defs/'));
const ajv = new Ajv2020({ strict: true });
ajv.addSchema({ $id: 'scope-fixture', $defs: definitions });
const commandValid = ajv.compile({ $ref: 'scope-fixture#/$defs/AcquisitionScopeCommand' });
const receiptValid = ajv.compile({ $ref: 'scope-fixture#/$defs/AcquisitionScopeReceipt' });
const command = {
  endpoint_id: '44'.repeat(16), access_scope_id: '55'.repeat(16),
  visibility: { dataset_id: '11'.repeat(16), dictionary_kind: 4, allocation_domain: '66'.repeat(16), ordinal: '7' },
  selector: { version: 1, filters: [{ kinds: [2, 1, 1], '#t': ['β', 'a'] }], history_since: 0, ephemeral: 'retain' },
  expected_config_revision: '2', expected_policy_revision: '3'
};

test('scope schema accepts profile1 shape and rejects independent invalid controls', () => {
  assert.equal(commandValid(command), true, JSON.stringify(commandValid.errors));
  for (const selector of [
    [], { ...command.selector, version: 2 }, { ...command.selector, filters: [] },
    { ...command.selector, history_since: -1 }, { ...command.selector, history_since: 2 ** 64 },
    { ...command.selector, filters: [{ kinds: [65536] }] },
    { ...command.selector, filters: [{ ids: ['a'] }] },
    { ...command.selector, filters: [{ '#e': ['NOT A FULL ID'] }] },
    { ...command.selector, filters: [{ '#multi': ['x'] }] },
    { ...command.selector, filters: [{ search: 'unsupported' }] },
    { ...command.selector, filters: [{ '#t': [] }] }
  ]) assert.equal(commandValid({ ...command, selector }), false);
  assert.equal(commandValid({ ...command, actor_id: '22'.repeat(16) }), false);
  assert.equal(commandValid({ ...command, visibility: ['11'.repeat(16), 4, '66'.repeat(16), '7'] }), false);
  assert.equal(commandValid({ ...command, expected_policy_revision: '03' }), false);
});

test('scope receipt schema retains exact strings and identity-only authority', () => {
  const receipt = { format: 'bigbrotr-acquisition-scope-registration/1', command_key: '08'.repeat(16),
    scope_id: '77'.repeat(16), accepted_at_us: '9223372036854775807', created: true, authority: 'identity-only' };
  assert.equal(receiptValid(receipt), true);
  assert.equal(receiptValid({ ...receipt, created: false }), true);
  for (const patch of [{ scope_id: '0'.repeat(32) }, { accepted_at_us: 42 }, { authority: 'execute' },
    { selector: command.selector }, { endpoint_id: command.endpoint_id }]) {
    assert.equal(receiptValid({ ...receipt, ...patch }), false);
  }
});
