#!/usr/bin/env python3
"""Offline independent replay of the two-origin 0038 SQL experiment.

Only standard Python is used to construct expected rows, manifests, handoffs and
closure digests. No production codec, SQL fixture helper, database or native store
is called. Random identities, actual times and transaction IDs are observations;
their relations and every other field are checked independently. The pinned SQL
fixture supplies named execution assertions (including top-level xid identity).
These are SQL shapes, not evidence of native effects or a successor writer.

Normal invocation requires a completed PASS for BOTH origins. For retrospective
inspection of a complete origin in a failed run, explicitly select --origin and
--allow-failed-run. That mode never reclassifies the original run as successful.
"""
import argparse
from copy import deepcopy
import datetime as dt
import hashlib
import json
from pathlib import Path
import re
import struct
import tomllib


FIXTURE_SHA256 = '1a134c6bd3cc8daccc288a4ccbbf3566f4fea179a26ca4aff90a41e3fc829fe7'
ORIGINS = {
    'activated': dict(kind=1, version=37,
        run='ingest-activated-retirement-engine-9bdc86a',
        database='bb_scope_bind_9bdc86a_installed_activated_retirement_off',
        snapshot='installed-activated-retirement-off-retry-after-sql.json',
        snapshot_sha256='7ac690057de370a00144ffd415bb1f504b05ff4b5599d6458b728820a98acbad',
        result_sha256='48506a152b393854f2f50cbedfedb71b66e53ed9578753d7e6b4df3bd76c8e09',
        status='PASS_INSTALLED_ACTIVATED_RETIREMENT_FIRST_WAVE'),
    'empty': dict(kind=2, version=31, run='ingest-empty-installed-engine-788f971',
        database='bb_scope_bind_788f971_installed_empty_normal',
        snapshot='installed-empty-normal-terminal-after.json',
        snapshot_sha256='5cc9803df1c4f2093f56694ca08857bbdf62066c51f260a35637ce2208aa7579',
        result_sha256='ff1d2e111dc42ff4026c7e42985a7814dbf058eb0bd1d7ba92c5e269e7642d34',
        status='PASS_INSTALLED_EMPTY_RETIREMENT'),
}
MIGRATIONS = {
    32: ('initial_activation', '1af4c64b3c1a311cac185d7ec8f015974b339ab60138a7454a3047d2d50b072f'),
    33: ('activated_initial_recovery', '4f22eb019e831126bf3f5c712d0ddf1e8213dfdfcdf9afaf5b38c80418cb5804'),
    34: ('activated_control_retirement', 'd3ccd19efd29a5c5db849c8bd21db45e4699076122d0bb6cd659fdd736d0b87a'),
    35: ('activated_source_closure', '80b5441cbb2de1f782ce6458e1c2b26a2f3730f51e61619af49f1e8d8adc375a'),
    36: ('activated_acquisition_abandonment', '19e2390b46d9eba7e4eac765eafc30fc7b2cac03e03c0918df64ed0b87642e54'),
    37: ('activated_initial_retirement', 'ea882fd887d0800ab1880646026ac85eeb6485f5fa4fd3323eee09b4bf96eac8'),
    38: ('repeatable_ingest_succession', 'bc5c90d25e5ae533e67dd328fbe1da32af3c19e8c03615241fa2359572aac087'),
}
STAGES = ('original', 'prepared3', 'active3', 'closed3', 'retired3', 'prepared4',
          'empty_terminal4', 'active5', 'final', 'released')
POSITIVES = set('activation_top_level_xid actual_exclusive_control_released '
    'actual_exclusive_control_session exact_activated_terminal_delta exact_empty_terminal_delta '
    'guards_origin historical_empty_after_future_claim manifest_coordinates_are_not_epoch_count '
    'preparation_top_level_xid retirement_top_level_xid selected_genesis terminal_genesis_flat '
    'unsigned_full_width'.split())
NEGATIVES = {
    'activation_admission_subtransaction': ('require_ingest_admission', 'lineage admission requires previously committed activation'),
    'activation_control_fence': ('guard_ingest_lineage_transaction', 'lineage preparation/activation authority and target must remain unchanged through commit'),
    'activation_target_fence': ('guard_ingest_lineage_transaction', 'lineage preparation/activation authority and target must remain unchanged through commit'),
    'anchored_genesis_role1': ('guard_ingest_lineage_role', 'anchored lineage role permits only exact terminal retirement'),
    'claim_after_activation_savepoint_release': ('guard_ingest_terminal_claim', 'lineage workflow claims require previously committed activation'),
    'claim_before_activation': ('guard_ingest_terminal_claim', 'lineage workflow claims require previously committed activation'),
    'domain_truncate': ('guard_ingest_lineage_truncate', 'anchored lineage registry history cannot be truncated'),
    'empty_terminal_lease_rewrite': ('lock_ingest_lineage', 'empty lineage scheduling metadata differs from preparation'),
    'epoch256_before_history': ('lock_ingest_lineage', 'lineage metadata or membership profile exhausted'),
    'genesis_creation_before_anchor': ('lock_ingest_lineage', 'lineage prepared identities or creation times differ'),
    'partial_terminal': ('require_ingest_successor', 'lineage retirement requires exactly ready or complete terminal metadata'),
    'prepare_activation_distinct_commit': ('guard_ingest_activation', 'lineage activation requires previously committed preparation'),
    'receipt_cascade_truncate': ('guard_ingest_succession', 'lineage preparations are retained and immutable'),
    'terminal_receipt_requires_retirement': ('guard_ingest_terminal_receipt', 'lineage terminal transaction requires its retired producer'),
    'terminal_role_delete': ('guard_ingest_lineage_role', 'anchored lineage role history cannot be deleted'),
    'terminal_role_early_constraints': ('guard_ingest_lineage_role', 'anchored lineage role permits only exact terminal retirement'),
    'unsigned_overflow': ('ingest_lineage_u64', 'invalid lineage u64'),
}


def require(answer, message):
    if not answer:
        raise ValueError(message)


def only(rows, **keys):
    found = [r for r in rows if all(r.get(k) == v for k, v in keys.items())]
    require(len(found) == 1, f'expected one row: {keys}, found {len(found)}')
    return found[0]


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=True)


def same(actual, expected, label):
    require(set(actual) == set(expected), label + ': table census')
    for table in expected:
        require(isinstance(actual[table], list), label + ': row array')
        require(sorted(map(canonical, actual[table])) == sorted(map(canonical, expected[table])),
                label + ': full rows differ in ' + table)


def raw(value, length=None):
    require(isinstance(value, str) and re.fullmatch(r'\\x(?:[0-9a-f]{2})*', value), 'bytea encoding')
    answer = bytes.fromhex(value[2:])
    require(length is None or len(answer) == length, 'bytea length')
    return answer


def hexbytes(value):
    return '\\x' + value.hex()


def sha(value):
    return hashlib.sha256(value).digest()


def u64(value):
    require(type(value) is int and 0 <= value < 2**64, 'u64')
    return struct.pack('>Q', value)


def instant(value):
    parsed = dt.datetime.fromisoformat(value.replace('Z', '+00:00'))
    require(parsed.utcoffset() == dt.timedelta(), 'UTC timestamp')
    delta = parsed - dt.datetime(1970, 1, 1, tzinfo=dt.timezone.utc)
    return delta.days * 86400000000 + delta.seconds * 1000000 + delta.microseconds


def decimal(value, bits):
    require(isinstance(value, str) and re.fullmatch(r'[1-9][0-9]*', value), 'transaction encoding')
    number = int(value)
    require(number < 2**bits, 'transaction range')
    return number


def xmins(rows):
    return {r['xmin'] for values in rows.values() for r in values}


def named_cases(cases, kind):
    errors = dict(NEGATIVES)
    errors['anchored_genesis_role2'] = (
        ('guard_activated_retirement_role', 'activated target role permits only exact retirement')
        if kind == 1 else ('guard_ingest_lineage_role', 'anchored lineage role permits only exact terminal retirement'))
    require(len(cases) == 31 and {r['name'] for r in cases} == POSITIVES | errors.keys(), '31 named cases')
    for item in cases:
        require(set(item) == {'name', 'sqlstate', 'message', 'context'}, 'diagnostic fields')
        if item['name'] in POSITIVES:
            require(item == dict(name=item['name'], sqlstate=None, message=None, context=None), 'positive assertion')
        else:
            function, message = errors[item['name']]
            require(item['sqlstate'] == '23514' and item['message'] == message, 'named refusal message')
            require(isinstance(item['context'], str) and re.match(
                r'PL/pgSQL function control\.' + function + r'\([^\n]*\) line [1-9][0-9]* at RAISE(?:\n|$)',
                item['context']), 'named refusal origin')


class Model:
    """Complete expected rowsets; observations supply only fresh runtime coordinates."""
    def __init__(self, rows, kind, window):
        require(len(rows) == 75, '75 tables')
        self.rows = deepcopy(rows)
        self.kind, self.window = kind, window
        self.dataset = only(self.rows['datasets'])
        self.legacy = only(self.rows['initial_ingest_successions'])
        self.resource = self.legacy['owner_resource_id']
        self.slot = self.legacy['producer_slot_id']
        self.dataset_id = self.dataset['dataset_id']
        self.migration_floor = max(r['applied_at_us'] for r in rows['schema_versions'])
        self.clock = max(window[0], self.migration_floor)
        self.transaction = None
        self.full_transaction = None
        self.used = xmins(rows)
        # Fresh IDs cannot alias any retained 16-byte identity, including domains.
        self.identities = set(re.findall(r'\\\\x([0-9a-f]{32})"', canonical(rows)))
        require(all(not rows[t] for t in ('ingest_successions', 'ingest_activations', 'ingest_terminal_transactions')), 'empty new registries')
        require(bool(rows['initial_ingest_activations']) == (kind == 1), 'genesis kind')
        require(all(r['state'] == 'released' for r in rows['owner_epochs']
                    if r['role'] in ('control', 'ingest')), 'terminal genesis owners')
        self.control = None

    def time(self, value, lower=None):
        require(type(value) is int and max(self.clock, lower or self.window[0]) <= value <= self.window[1], 'phase UTC ordering')
        self.clock = value
        return value

    def identity(self, value):
        parsed = raw(value, 16).hex()
        require(parsed != '00' * 16 and parsed not in self.identities, 'fresh identity')
        self.identities.add(parsed)
        return value

    def tx(self, value, top=None, sub=False):
        n = decimal(value, 32)
        full = decimal(top, 64) if top is not None else None
        t = full % 2**32 if full is not None else n
        require(value not in self.used and str(t) not in self.used, 'fresh transaction versus all retained rows')
        require((0 < (n - t) % 2**32 < 2**31) if sub else n == t, 'top-level versus subtransaction xid')
        if self.transaction is not None:
            require(0 < (t - self.transaction) % 2**32 < 2**31, 'phase transaction order')
            if self.full_transaction is not None:
                expected_top = self.full_transaction + (t - self.transaction) % 2**32
                require(full is None or full == expected_top, 'consistent full-width transaction epoch')
                full = expected_top
        if full is not None:
            self.full_transaction = full + (n - t) % 2**32
        self.transaction = n
        self.used.update((value, str(t)))
        return value

    def append(self, table, **row):
        self.rows[table].append(row)
        return row

    def role(self, role_id, role, at, xid):
        return self.append('role_instances', role_instance_id=role_id, dataset_id=self.dataset_id,
                           role=role, created_at_us=at, retired_at_us=None, xmin=xid)

    def owner(self, resource, epoch, role_id, incarnation, role, at, xid, pid=None, start=None):
        return self.append('owner_epochs', resource_id=resource, epoch=epoch, dataset_id=self.dataset_id,
            role=role, role_instance_id=role_id, process_incarnation=incarnation, acquired_at_us=at,
            lease_expires_at_us=at + 60000000, state='active', released_at_us=None,
            exclusion_method=None, exclusion_digest=None, control_fence_version=1 if role == 'control' else None,
            control_backend_pid=pid, control_backend_start=start, archive_frontier_protocol=None, xmin=xid)

    def release(self, observed, xid):
        c = self.control
        out = only(observed['owner_epochs'], resource_id=c['resource_id'], epoch=c['epoch'])
        at = self.time(out['released_at_us'])
        require(at < c['lease_expires_at_us'], 'release before Control expiry')
        c.update(state='released', released_at_us=at, exclusion_method='storage_fenced',
                 exclusion_digest=hexbytes(sha(raw(c['process_incarnation']))), xmin=xid)
        only(self.rows['role_instances'], role_instance_id=c['role_instance_id']).update(retired_at_us=at, xmin=xid)

    def claim(self, observed, switch=False):
        last = max((r for r in self.rows['owner_epochs'] if r['role'] == 'control'), key=lambda r: r['epoch'])
        c = only(observed['owner_epochs'], role='control', state='active')
        xid = self.tx(c['xmin'])
        if switch:
            self.release(observed, xid)
            require((c['control_backend_pid'], c['control_backend_start']) ==
                    (last['control_backend_pid'], last['control_backend_start']), 'same actual psql backend')
        at = self.time(c['acquired_at_us'])
        require(type(c['control_backend_pid']) is int and c['control_backend_pid'] > 0, 'positive backend PID')
        require(max(self.window[0], self.migration_floor) <= instant(c['control_backend_start']) <= at,
                'backend starts after migration in this invocation')
        role_id, incarnation = self.identity(c['role_instance_id']), self.identity(c['process_incarnation'])
        self.role(role_id, 'control', at, xid)
        self.control = self.owner(last['resource_id'], last['epoch'] + 1, role_id, incarnation, 'control', at,
                                  xid, c['control_backend_pid'], c['control_backend_start'])

    def prepare(self, observed, epoch):
        spec = only(observed['ingest_successions'], target_epoch=epoch)
        xid = self.tx(spec['xmin'], spec['preparation_transaction'], sub=epoch == 3)
        at = self.time(spec['created_at_us'])
        c = self.control
        require(at < c['lease_expires_at_us'], 'preparation current Control')
        incarnation, role_id = self.identity(spec['target_incarnation']), self.identity(spec['target_role_instance_id'])
        previous = self.legacy if epoch == 3 else only(self.rows['ingest_successions'], target_epoch=epoch - 1)
        prev_manifest, parent = raw(previous['target_manifest'], 266), raw(previous['handoff'])
        source = only(self.rows['source_streams'], owner_resource_id=self.resource, owner_epoch=epoch - 1)
        producer = only(self.rows['producer_epochs'], producer_slot_id=self.slot, producer_epoch=epoch - 1)
        require(source['state'] == 'closed' and producer['state'] == 'retired', 'terminal predecessor')
        topology = self.dataset['topology_generation'] + 1
        kind = self.kind if epoch == 3 else (1 if epoch == 4 else 2)
        manifest = (bytes.fromhex('01010003000100010001') + raw(self.dataset_id, 16) + u64(topology)
                    + raw(self.resource, 16) + struct.pack('>I', self.legacy['shard_id']) + u64(epoch)
                    + raw(incarnation, 16) + prev_manifest[78:94] + u64(self.dataset['policy_revision']) + b'\x01\x09')
        self.role(role_id, 'ingest', at, xid)
        self.owner(self.resource, epoch, role_id, incarnation, 'ingest', at, xid)
        self.append('source_streams', dataset_id=self.dataset_id, shard_id=self.legacy['shard_id'], owner_epoch=epoch,
            owner_resource_id=self.resource, owner_role='ingest', owner_incarnation=incarnation, opened_at_us=at,
            state='open', closed_at_us=None, last_complete_sequence=None, closure_digest=None, xmin=xid)
        for k in range(1, 10):
            domain = only(observed['allocation_domains'], owner_resource_id=self.resource, owner_epoch=epoch, dictionary_kind=k)
            domain_id = self.identity(domain['allocation_domain'])
            manifest += struct.pack('>H', k) + raw(domain_id, 16)
            self.append('allocation_domains', dataset_id=self.dataset_id, dictionary_kind=k, allocation_domain=domain_id,
                owner_resource_id=self.resource, owner_epoch=epoch, owner_role='ingest', owner_incarnation=incarnation,
                created_at_us=at, state='active', closed_at_us=None, closure_digest=None, xmin=xid)
        self.append('producer_epochs', producer_slot_id=self.slot, producer_epoch=epoch, dataset_id=self.dataset_id,
            producer_role_instance_id=role_id, producer_role='ingest', producer_incarnation=incarnation,
            owner_resource_id=self.resource, owner_epoch=epoch, owner_role='ingest', owner_incarnation=incarnation,
            outstanding_request_limit=2, opened_at_us=at, state='open', retired_at_us=None, closure_digest=None,
            reserved_through_request=0, xmin=xid)
        self.append('topology_generations', dataset_id=self.dataset_id, generation=topology,
            previous_generation=self.dataset['topology_generation'], created_at_us=at, sealed=True, xmin=xid)
        for stream in self.rows['source_streams']:
            require(stream['owner_resource_id'] == self.resource, 'single-lineage fixture')
            self.append('topology_streams', dataset_id=self.dataset_id, topology_generation=topology,
                shard_id=self.legacy['shard_id'], owner_epoch=stream['owner_epoch'],
                handoff_last_complete_sequence=stream['last_complete_sequence'], xmin=xid)
        old_topology = self.dataset['topology_generation']
        self.dataset.update(topology_generation=topology, xmin=xid)
        handoff = (b'\x02\x0a' + bytes([kind]) + raw(self.slot, 16) + u64(epoch - 1)
                   + u64(producer['reserved_through_request']) + u64(source['last_complete_sequence'])
                   + raw(source['closure_digest'], 32) + sha(parent) + struct.pack('>H', len(prev_manifest))
                   + prev_manifest + struct.pack('>H', len(manifest)) + manifest)
        require(len(manifest) == 266 and len(handoff) == 643, 'connected wire sizes')
        self.append('ingest_successions', dataset_id=self.dataset_id, owner_resource_id=self.resource, owner_role='ingest',
            target_epoch=epoch, previous_epoch=epoch - 1, terminal_kind=kind, target_incarnation=incarnation,
            target_role_instance_id=role_id, producer_slot_id=self.slot, producer_epoch=epoch, shard_id=self.legacy['shard_id'],
            previous_topology=old_topology, target_topology=topology, issued_through=producer['reserved_through_request'],
            last_complete_sequence=source['last_complete_sequence'], closure_digest=source['closure_digest'],
            previous_manifest=hexbytes(prev_manifest), target_manifest=hexbytes(manifest),
            previous_manifest_sha256=hexbytes(sha(prev_manifest)), target_manifest_sha256=hexbytes(sha(manifest)),
            handoff=hexbytes(handoff), handoff_sha256=hexbytes(sha(handoff)), parent_handoff_sha256=hexbytes(sha(parent)),
            config_revision=self.dataset['config_revision'], policy_revision=self.dataset['policy_revision'],
            config_digest=only(self.rows['config_revisions'], revision=self.dataset['config_revision'])['document_digest'],
            policy_digest=only(self.rows['policy_revisions'], revision=self.dataset['policy_revision'])['document_digest'],
            outstanding_request_limit=2, created_at_us=at, lease_expires_at_us=at + 60000000,
            prepared_by_resource=c['resource_id'], prepared_by_epoch=c['epoch'], prepared_by_role='control',
            prepared_by_incarnation=c['process_incarnation'], preparation_transaction=spec['preparation_transaction'], xmin=xid)

    def activate(self, observed, epoch):
        a = only(observed['ingest_activations'], target_epoch=epoch)
        s = only(self.rows['ingest_successions'], target_epoch=epoch)
        xid = self.tx(a['xmin'], a['activation_transaction'], sub=epoch == 3)
        at = self.time(a['activated_at_us'])
        require(at < min(s['lease_expires_at_us'], self.control['lease_expires_at_us']), 'activation live leases')
        self.append('ingest_activations', **{k: s[k] for k in ('dataset_id', 'owner_resource_id', 'target_epoch',
            'target_incarnation', 'target_role_instance_id', 'producer_slot_id', 'producer_epoch', 'target_manifest_sha256', 'handoff_sha256')},
            activated_by_resource=self.control['resource_id'], activated_by_epoch=self.control['epoch'],
            activated_by_incarnation=self.control['process_incarnation'], activated_at_us=at,
            activation_transaction=a['activation_transaction'], xmin=xid)

    def closure(self, epoch, empty):
        s = only(self.rows['ingest_successions'], target_epoch=epoch)
        domain = b'bigbrotr/ingest-successor-' + (b'empty-closure' if empty else b'source-closure') + b'/1\0'
        return hexbytes(sha(domain + sha(raw(s['handoff'])) + sha(raw(s['target_manifest']))
                            + raw(self.slot, 16) + u64(epoch) + u64(0) + u64(0)))

    def close(self, observed):
        src = only(observed['source_streams'], owner_resource_id=self.resource, owner_epoch=3)
        xid, at = self.tx(src['xmin']), self.time(src['closed_at_us'])
        require(at < self.control['lease_expires_at_us'], 'close live Control')
        only(self.rows['producer_epochs'], producer_slot_id=self.slot, producer_epoch=3).update(state='closing', xmin=xid)
        only(self.rows['source_streams'], owner_resource_id=self.resource, owner_epoch=3).update(
            state='closed', closed_at_us=at, last_complete_sequence=0, closure_digest=self.closure(3, False), xmin=xid)

    def retire(self, observed, epoch, empty):
        receipt = only(observed['ingest_terminal_transactions'], target_epoch=epoch)
        out = only(observed['producer_epochs'], producer_slot_id=self.slot, producer_epoch=epoch)
        xid = self.tx(out['xmin'], receipt['terminal_transaction'], sub=not empty)
        at = self.time(out['retired_at_us'])
        require(at < self.control['lease_expires_at_us'], 'terminal live Control')
        digest = self.closure(epoch, empty)
        s = only(self.rows['ingest_successions'], target_epoch=epoch)
        only(self.rows['producer_epochs'], producer_slot_id=self.slot, producer_epoch=epoch).update(
            state='retired', retired_at_us=at, closure_digest=digest, xmin=xid)
        domains = [r for r in self.rows['allocation_domains'] if r['owner_resource_id'] == self.resource and r['owner_epoch'] == epoch]
        require(len(domains) == 9, 'nine terminal domains')
        for row in domains:
            row.update(state='closed', closed_at_us=at, closure_digest=digest, xmin=xid)
        only(self.rows['owner_epochs'], resource_id=self.resource, epoch=epoch).update(
            state='released', released_at_us=at, exclusion_method='storage_fenced', exclusion_digest=digest, xmin=xid)
        only(self.rows['role_instances'], role_instance_id=s['target_role_instance_id']).update(retired_at_us=at, xmin=xid)
        if empty:
            only(self.rows['source_streams'], owner_resource_id=self.resource, owner_epoch=epoch).update(
                state='closed', closed_at_us=at, last_complete_sequence=0, closure_digest=digest, xmin=xid)
        self.append('ingest_terminal_transactions', owner_resource_id=self.resource, target_epoch=epoch,
            terminal_transaction=receipt['terminal_transaction'], control_resource=self.control['resource_id'],
            control_epoch=self.control['epoch'], control_incarnation=self.control['process_incarnation'], xmin=xid)

    def job(self, observed):
        job = only(observed['jobs'], request_key=hexbytes(b'lineage-history-fixture'))
        xid, at = self.tx(job['xmin']), self.time(job['created_at_us'])
        require(at < self.control['lease_expires_at_us'], 'job live Control')
        job_id, principal = self.identity(job['job_id']), self.identity(job['principal_id'])
        self.append('jobs', job_id=job_id, dataset_id=self.dataset_id, principal_id=principal,
            request_key=hexbytes(b'lineage-history-fixture'), request_digest=hexbytes(sha(b'\x01')),
            kind='import', config_revision=self.dataset['config_revision'], policy_revision=self.dataset['policy_revision'],
            created_at_us=at, state='queued', job_revision=3, checkpoint_revision=0, owner_role='control',
            owner_resource_id=self.control['resource_id'], owner_epoch=self.control['epoch'],
            owner_incarnation=self.control['process_incarnation'], input_complete=True, input_topology_generation=None,
            reason_code=None, completed_at_us=None, result_manifest_digest=None,
            acquisition_job_protocol=None, export_job_protocol=None, xmin=xid)
        ordinal = max(r['admission_ordinal'] for r in self.rows['job_admissions']) + 1
        self.append('job_admissions', job_id=job_id, admission_ordinal=ordinal, xmin=xid)


def verify_summary(summary, migrated, kind, window):
    require(set(summary) == {'kind', 'cases', 'snapshots'} and summary['kind'] == 'SQL_ONLY_NO_NATIVE_PROOF', 'SQL-only summary')
    named_cases(summary['cases'], kind)
    s = summary['snapshots']
    require(set(s) == set(STAGES), 'ten exact stages')
    same(s['original'], migrated, 'migrated anchor')
    m = Model(migrated, kind, window)
    m.claim(s['prepared3'])
    m.prepare(s['prepared3'], 3)
    same(s['prepared3'], m.rows, 'prepared3')
    m.activate(s['active3'], 3)
    same(s['active3'], m.rows, 'active3')
    m.claim(s['closed3'], switch=True)
    m.close(s['closed3'])
    same(s['closed3'], m.rows, 'closed3')
    m.retire(s['retired3'], 3, False)
    same(s['retired3'], m.rows, 'retired3: twelve targets plus receipt')
    m.prepare(s['prepared4'], 4)
    same(s['prepared4'], m.rows, 'prepared4')
    m.retire(s['empty_terminal4'], 4, True)
    same(s['empty_terminal4'], m.rows, 'empty_terminal4: thirteen targets plus receipt')
    m.prepare(s['active5'], 5)
    m.activate(s['active5'], 5)
    same(s['active5'], m.rows, 'active5')
    m.job(s['final'])
    same(s['final'], m.rows, 'raw import claim')
    out = only(s['released']['owner_epochs'], resource_id=m.control['resource_id'], epoch=m.control['epoch'])
    m.release(s['released'], m.tx(out['xmin']))
    same(s['released'], m.rows, 'released')
    return dict(snapshots=10, tables_per_snapshot=75, named_cases=31,
                terminal_target_rows=[12, 13], terminal_receipts=2,
                handoff_sha256=[raw(r['handoff_sha256']).hex() for r in m.rows['ingest_successions']])


def migrated_checked(before, after, case, origin, window):
    expected = deepcopy(before)
    for table in ('ingest_successions', 'ingest_activations', 'ingest_terminal_transactions'):
        require(table not in expected, 'new table already in seed')
        expected[table] = []
    if origin['version'] == 31:
        require('initial_ingest_activations' not in expected, 'legacy31 activation census')
        expected['initial_ingest_activations'] = []
    added = []
    original_xmins = xmins(before)
    migration_xmin = None
    previous_time = window[0]
    for version in range(origin['version'] + 1, 39):
        suffix, checksum = MIGRATIONS[version]
        name = f'{version:04d}_{suffix}.sql'
        row = only(after['schema_versions'], migration=version)
        decimal(row['xmin'], 32)
        require(row['xmin'] not in original_xmins, 'fresh migration transaction')
        migration_xmin = migration_xmin or row['xmin']
        require(row['xmin'] == migration_xmin, 'single migration invocation transaction')
        at = row['applied_at_us']
        require(type(at) is int and previous_time <= at <= window[1], 'migration time')
        previous_time = at
        expected['schema_versions'].append(dict(engine='postgres-control', migration=version, name=name,
            checksum='\\x' + checksum, applied_at_us=at, applied_by='bigbrotr_migrator',
            reader_min=1, reader_max=1, writer_floor=1, xmin=row['xmin']))
        added.append(dict(name=name, sha256=checksum, version=str(version)))
    same(after, expected, 'exact additive migration')
    require(len(case['migrations']) == 3, 'plan/apply/reapply')
    for index, observed in enumerate(case['migrations']):
        require(set(observed) == {'action', 'exit_code', 'stdout', 'stderr'}, 'migration response envelope')
        require(observed['action'] == ('plan' if index == 0 else 'apply') and observed['exit_code'] == 0 and observed['stderr'] == '', 'migration terminal response')
        wanted = dict(format='bigbrotr-control-migration/1', scope='postgres-control', target_version='38',
            source_version=str(origin['version']) if index < 2 else '38',
            current_version=str(origin['version']) if index == 0 else '38',
            status=('planned', 'applied', 'current')[index], applied_count=str(len(added)) if index == 1 else '0',
            steps=added if index < 2 else [], dataset_initialization='not-performed', release_upgrade_qualification='not-performed')
        require(json.loads(observed['stdout']) == wanted, 'exact migration report')


def negative_controls(summary, migrated, kind, window):
    """Mutate complete captured evidence in memory; never touch its files."""
    mutations = []

    def propagated(table, predicate, key, value):
        def change(s):
            for stage in STAGES:
                for row in s['snapshots'][stage][table]:
                    if predicate(row):
                        row[key] = value(row) if callable(value) else value
        return change

    mutations.extend([
        ('old-source-cut', propagated('source_streams', lambda r: r['owner_epoch'] == 1, 'last_complete_sequence', 0)),
        ('old-row-xmin', propagated('role_instances', lambda r: True, 'xmin', '1')),
        ('empty-lease', propagated('owner_epochs', lambda r: r['role'] == 'ingest' and r['epoch'] == 4, 'lease_expires_at_us', lambda r: r['lease_expires_at_us'] + 1)),
        ('activated-source-rewritten-at-retirement', lambda s: only(s['snapshots']['retired3']['source_streams'], owner_epoch=3).update(xmin='1')),
        ('missing-terminal-domain', lambda s: s['snapshots']['retired3']['allocation_domains'].pop()),
        ('terminal-control-attribution', propagated('ingest_terminal_transactions', lambda r: True, 'control_epoch', 1)),
        ('preparation-subxid-as-top', propagated('ingest_successions', lambda r: r['target_epoch'] == 3, 'preparation_transaction', lambda r: r['xmin'])),
        ('activation-subxid-as-top', propagated('ingest_activations', lambda r: r['target_epoch'] == 3, 'activation_transaction', lambda r: r['xmin'])),
        ('terminal-subxid-as-top', propagated('ingest_terminal_transactions', lambda r: r['target_epoch'] == 3, 'terminal_transaction', lambda r: r['xmin'])),
        ('activation-xid8-epoch', propagated('ingest_activations', lambda r: r['target_epoch'] == 3,
            'activation_transaction', lambda r: str(int(r['activation_transaction']) + 2**32))),
        ('topology-cut', propagated('topology_streams', lambda r: r['owner_epoch'] == 1 and r['topology_generation'] >= 2, 'handoff_last_complete_sequence', 0)),
        ('job-revision', propagated('jobs', lambda r: r['request_key'] == hexbytes(b'lineage-history-fixture'), 'job_revision', 2)),
        ('missing-case', lambda s: s['cases'].pop()),
        ('wrong-named-function', lambda s: only(s['cases'], name='claim_before_activation').update(context='PL/pgSQL function control.unrelated() line 1 at RAISE')),
        ('extra-table', lambda s: s['snapshots']['released'].update(unchecked=[])),
        ('duplicate-row', lambda s: s['snapshots']['released']['ingest_activations'].append(deepcopy(s['snapshots']['released']['ingest_activations'][0]))),
    ])

    def rehashed_parent(s):
        for state in s['snapshots'].values():
            for row in state['ingest_successions']:
                if row['target_epoch'] == 5:
                    handoff = bytearray(raw(row['handoff']))
                    handoff[75] ^= 1  # First parent-hash byte, after kind/slot/three u64/digest.
                    row.update(handoff=hexbytes(handoff), handoff_sha256=hexbytes(sha(handoff)),
                               parent_handoff_sha256=hexbytes(handoff[75:107]))
                    for activation in state['ingest_activations']:
                        if activation['target_epoch'] == 5:
                            activation['handoff_sha256'] = row['handoff_sha256']
    mutations.append(('coherently-rehashed-parent', rehashed_parent))

    def terminal_claim_xmin(s):
        states = s['snapshots']
        epoch = only(states['retired3']['ingest_terminal_transactions'], target_epoch=3)['control_epoch']
        claim_xmin = only(states['closed3']['owner_epochs'], role='control', epoch=epoch)['xmin']
        terminal_xmin = only(states['retired3']['ingest_terminal_transactions'], target_epoch=3)['xmin']
        for state in states.values():
            for rows in state.values():
                for row in rows:
                    if row['xmin'] == terminal_xmin:
                        row['xmin'] = claim_xmin
    mutations.append(('coherent-terminal-uses-Control-claim-xmin', terminal_claim_xmin))
    rejected = []
    for label, mutate in mutations:
        altered = deepcopy(summary)
        mutate(altered)
        try:
            verify_summary(altered, migrated, kind, window)
        except (ValueError, KeyError, TypeError, IndexError):
            rejected.append(label)
        else:
            raise ValueError('accepted counterexample: ' + label)
    return dict(count=len(rejected), rejected=rejected)


def replay(root, selected=None, allow_failed=False):
    root = Path(root).resolve(strict=True)
    output = root / 'sql'
    files = {}

    def read(path, expected=None, text=False):
        data = path.read_bytes()
        digest = sha(data).hex()
        require(expected is None or digest == expected, 'pinned file: ' + str(path))
        files[str(path)] = digest
        return data.decode() if text else json.loads(data)

    report = read(output / 'result.json')
    require(isinstance(report['source'], str) and re.fullmatch('[0-9a-f]{40}', report['source']), 'source revision')
    require(not allow_failed or selected in ORIGINS, 'failed-run mode requires one explicit origin')
    require(report['status'] == 'PASS_SQL_SHAPES_ONLY' or (allow_failed and report['status'] == 'FAIL'), 'terminal run status')
    require(all(report[k] == [] for k in ('cleanup_errors', 'unrelated_changed', 'unexpected_containers')), 'reported custody completion')
    window = (instant(report['started_at']), instant(report['finished_at']))
    require(window[0] < window[1], 'run window')
    read(root / 'source/experiments/control-migrations/repeatable_succession_cases.sql', FIXTURE_SHA256, text=True)
    require([c['name'] for c in report['cases']] == ['activated', 'empty'], 'two origin attempts in run')
    require(len(set(report['databases'])) == 2, 'two distinct clone databases')
    require({c['database'] for c in report['cases']} == set(report['databases']), 'distinct origin database mapping')
    results = {}
    for name in ([selected] if selected else ORIGINS):
        origin = ORIGINS[name]
        basis = root.parent / origin['run'] / 'bindings'
        old_report = read(basis / 'result.json', origin['result_sha256'])
        require(old_report['status'] == origin['status'], 'qualified original status')
        before = read(basis / origin['snapshot'], origin['snapshot_sha256'])
        retained = report['origins'][name]
        require(all(retained[k] == origin[k] for k in ('kind', 'run', 'database', 'snapshot', 'snapshot_sha256', 'result_sha256', 'status')) and retained['preserved'] is True, 'origin attribution/preservation')
        same(retained['rows'], before, 'original observed rows')
        same(read(output / (name + '-copy-before.json')), before, 'clone before migration')
        case = only(report['cases'], name=name)
        require(case['status'] == 'PASS_SQL_SHAPE' and case['fixture_exit'] == 0 and case['declared_cases'] == 31, 'complete selected origin')
        require(case['backend_absence'] == case['final_backend_absence'] == [], 'observed clone backend absence')
        require(case['database'] == 'bb_succession_' + report['source'][:7] + '_' + name
                and case['database'] != origin['database'], 'copy database attribution')
        config = tomllib.loads(read(output / (case['database'] + '.toml'), text=True))
        require(config['postgres']['control'] == dict(database=case['database'], endpoint='postgres:5432',
            password_ref='file:/run/secrets/bigbrotr-control', user='bigbrotr_control',
            tls=dict(local_boundary='private-bridge', mode='local-plaintext')), 'migration configuration database/identity')
        require(case['legacy_grants'] == dict(exit_code=0, stdout='LEGACY_ACL_ROLLBACK\n', stderr=''), 'ordinary legacy ACL rollback')
        migrated = read(output / (name + '-migrated.json'))
        migrated_checked(before, migrated, case, origin, window)
        for version in range(origin['version'] + 1, 39):
            suffix, checksum = MIGRATIONS[version]
            read(root / f'source/migrations/postgres/{version:04d}_{suffix}.sql', checksum, text=True)
        summary = read(output / (name + '-summary.json'))
        stdout = read(output / (name + '.stdout'), text=True)
        require(all(not line or line.startswith('{') for line in stdout.splitlines()), 'only void results and JSON on raw stdout')
        summaries = [json.loads(line) for line in stdout.splitlines() if line.startswith('{')]
        require(summaries == [summary], 'raw stdout to saved summary binding')
        stderr = read(output / (name + '.stderr'), text=True)
        require(stderr == 'NOTICE:  truncate cascades to table "ingest_activations"\nNOTICE:  truncate cascades to table "ingest_terminal_transactions"\n', 'only expected cascade notices')
        results[name] = verify_summary(summary, migrated, origin['kind'], window)
        same(read(output / (name + '-external.json')), summary['snapshots']['released'], 'external final state')
        same(read(output / (name + '-final.json')), summary['snapshots']['released'], 'cleanup final state')
        results[name]['counterexamples'] = negative_controls(summary, migrated, origin['kind'], window)
    return dict(status='PASS_SELECTED_SQL_REPLAY' if selected else 'PASS_BOTH_ORIGINS_SQL_REPLAY',
        original_run_status=report['status'], source=report['source'], root=str(root), origins=results,
        scope='Offline full-row SQL shape replay; no native positive proof, writer, concurrency or installed succession qualification',
        evidence_limits=['Execution/ACL/top-level-xid assertions bind the pinned SQL fixture and its raw stdout.',
                         'Original preservation and backend absence are retained observations, not new queries.',
                         'No PostgreSQL 255-epoch capacity/performance or loss/expiry schedule is inferred.'],
        files_sha256=files)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('run_root', type=Path)
    parser.add_argument('--origin', choices=ORIGINS)
    parser.add_argument('--allow-failed-run', action='store_true')
    args = parser.parse_args()
    print(json.dumps(replay(args.run_root, args.origin, args.allow_failed_run), indent=2, sort_keys=True))
