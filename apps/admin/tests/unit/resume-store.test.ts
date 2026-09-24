import assert from 'node:assert/strict';
import test from 'node:test';
import { decodeResumeRecord } from '../../src/lib/resume-store.ts';

const record = {
  format: 'bigbrotr-browser-resume/1', principal_id: '01'.repeat(16), job_id: '02'.repeat(16),
  key: '03'.repeat(16), command: { expected_job_revision: '9007199254740993', expected_config_revision: '18446744073709551615', expected_policy_revision: '2' },
  outcome: 'pending'
};

test('saved resume metadata retains exact original identity and revisions without aliasing', () => {
  const decoded = decodeResumeRecord(record, record.principal_id);
  assert.deepEqual(decoded, record);
  assert.notEqual(decoded, record);
  assert.notEqual(decoded.command, record.command);
  for (const outcome of ['accepted', 'refused']) assert.equal(decodeResumeRecord({ ...record, outcome }, record.principal_id).outcome, outcome);
});

test('saved metadata refuses other accounts, extra data, malformed identities and revisions', () => {
  for (const value of [null, [], {}, { ...record, principal_id: '04'.repeat(16) },
    { ...record, key: '0'.repeat(32) }, { ...record, job_id: '02'.repeat(16) + '\n' },
    { ...record, session: 'must never be stored' }, { ...record, outcome: 'running' },
    { ...record, outcome: ['pending'] }, { ...record, outcome: { toString: () => 'accepted' } },
    { ...record, format: 'bigbrotr-browser-resume/2' },
    ...['0', '01', '1\n', '18446744073709551616', 1].map(expected_config_revision => ({ ...record, command: { ...record.command, expected_config_revision } })),
    { ...record, command: { ...record.command, destination: '/private' } }
  ]) assert.throws(() => decodeResumeRecord(value, record.principal_id));
});
