"""Independent installed cycle state model. No product or engine operations.

The startup encoder is a scoped adaptation of the qualified installed-family
oracle, extended explicitly to epochs4/5 and no Sync. It independently derives
new rows/portable bytes; no Rust-produced receipt is used as an expectation.
"""
import copy
import hashlib
import re
from installed_family_replay import clock, fresh_xmin, exclusion, leases
import history_replay as history
import transition_replay as transition

encoded, sha, binary, be = history.encoded, history.sha, history.binary, history.be
only, same, instant = history.only, history.same, history.instant
native_map, native_rows, bx = transition.native_map, transition.native_rows, transition.bx
digest_bytes = lambda value: hashlib.sha256(value).digest()

PINS = {'activated': {'name': 'activated',
               'run': 'ingest-succession-installed-family-engine-9359721',
               'subdirectory': 'installed-family',
               'database': 'bb_installed_successor_9359721_activated',
               'status': 'PASS_INSTALLED_SUCCESSOR_FAMILY',
               'source': '93597217aa9f23a99090a280837c0bc12ae90d34',
               'owner': 'afd74d59318d8316fea633aa1f238058',
               'label': 'org.bigbrotr.installed-successor',
               'client_id': '680de19d6254cda4ce7eb856b0a9c49f9f8f6c1fe65c26a2b40d81dbbc7064ac',
               'source_clone': 'activated',
               'final_case': 'activated-live3',
               'catalog_sha256': '6aad32f83a7c62b071ec37852994df068afdcb7c334635b94aacf1d225a5718e',
               'result_sha256': '43e9f2883e65d5d4156bea011695e649268f0b349fb6c6aaad6c56c6e225db0c',
               'snapshot': 'activated-live3-after.json',
               'snapshot_sha256': 'c0dccd1bef852b32922145391b1723c4f65134aac400d5c5190ef97afe0d9997',
               'native_file': 'activated-live3-after-native.json',
               'native_sha256': '536593b9831d7c0bafb39b4519abf06d8e653e3d8afc5f0cc299b6b5ccd5a222',
               'inputs_file': 'activated-live3-after-inputs.json',
               'inputs_sha256': '713ec780a8fc393ea136f16e8eb020ff019dd95d87c923f0b0030d28b375bc98',
               'store': 'activated/store'},
 'empty': {'name': 'empty',
           'run': 'ingest-succession-installed-family-engine-9359721',
           'subdirectory': 'installed-family',
           'database': 'bb_installed_successor_9359721_empty',
           'status': 'PASS_INSTALLED_SUCCESSOR_FAMILY',
           'source': '93597217aa9f23a99090a280837c0bc12ae90d34',
           'owner': 'afd74d59318d8316fea633aa1f238058',
           'label': 'org.bigbrotr.installed-successor',
           'client_id': '680de19d6254cda4ce7eb856b0a9c49f9f8f6c1fe65c26a2b40d81dbbc7064ac',
           'source_clone': 'empty',
           'final_case': 'empty-live3',
           'catalog_sha256': '062b2dcee8d28ce48461cca668dd3c9d20f94e3035041f1a817925fafa77263e',
           'result_sha256': '43e9f2883e65d5d4156bea011695e649268f0b349fb6c6aaad6c56c6e225db0c',
           'snapshot': 'empty-live3-after.json',
           'snapshot_sha256': 'de49f2b01fffe9ff0e6abeaf3db64ea2d0beaf991b1ba439e0124d352b8ef7dd',
           'native_file': 'empty-live3-after-native.json',
           'native_sha256': '269b0ea9905e9f3fba80ba84b0ce5ebcb0a533590c0268deedacbec5e3acd539',
           'inputs_file': 'empty-live3-after-inputs.json',
           'inputs_sha256': '9f0e5a13447660ea374c1b847c544dc0f0da13179f1c2d4e2cde75d90cfda834',
           'store': 'empty/store'},
 'later': {'name': 'later',
           'run': 'ingest-succession-installed-family-engine-9359721',
           'subdirectory': 'installed-family',
           'database': 'bb_installed_successor_9359721_later',
           'status': 'PASS_INSTALLED_SUCCESSOR_FAMILY',
           'source': '93597217aa9f23a99090a280837c0bc12ae90d34',
           'owner': 'afd74d59318d8316fea633aa1f238058',
           'label': 'org.bigbrotr.installed-successor',
           'client_id': '680de19d6254cda4ce7eb856b0a9c49f9f8f6c1fe65c26a2b40d81dbbc7064ac',
           'source_clone': 'later',
           'final_case': 'later-live4',
           'catalog_sha256': '062b2dcee8d28ce48461cca668dd3c9d20f94e3035041f1a817925fafa77263e',
           'result_sha256': '43e9f2883e65d5d4156bea011695e649268f0b349fb6c6aaad6c56c6e225db0c',
           'snapshot': 'later-live4-after.json',
           'snapshot_sha256': '87780bb8b3c0e8cfccd6d1ea35abf4b6358991c230224baafeb017bc260a4e70',
           'native_file': 'later-live4-after-native.json',
           'native_sha256': '3a6cfcd2ce481c9634cc2b97e87f87c3550a47d8a64b3d4d3d1bab1a667beaba',
           'inputs_file': 'later-live4-after-inputs.json',
           'inputs_sha256': 'fa63a211046e0f7c67912f6f469df73c5828f46993d1acff59305144440aead9',
           'store': 'later/store'}}

class Model:
    def __init__(self, before, native, inputs, window):
        self.pg, self.native, self.inputs = copy.deepcopy((before, native, inputs))
        self.ds = only(self.pg['datasets'])
        self.legacy = only(self.pg['initial_ingest_successions'])
        self.resource, self.slot = (self.legacy[k] for k in ('owner_resource_id', 'producer_slot_id'))
        self.window = window
        self.ids = set(re.findall(r'\\\\x([0-9a-f]{32})"', encoded(before).decode()))

    def identity(self, value):
        assert re.fullmatch('[0-9a-f]{32}', value) and int(value, 16)
        assert value not in self.ids
        self.ids.add(value)
        return '\\x' + value

    def startup(self, observed, report):
        before, ds = copy.deepcopy(self.pg), self.ds['dataset_id']
        previous = max((r for r in before['owner_epochs'] if r['role'] == 'control'), key=lambda r: r['epoch'])
        assert previous['state'] == 'released'
        actual = only(observed['owner_epochs'], role='control', epoch=previous['epoch'] + 1)
        role = only(observed['role_instances'], role_instance_id=actual['role_instance_id'])
        rid, incarnation = self.identity(role['role_instance_id'][2:]), self.identity(actual['process_incarnation'][2:])
        for at in (role['created_at_us'], actual['acquired_at_us'], actual['lease_expires_at_us'] - 6_000_000):
            clock(at, self.window)
        assert role['created_at_us'] <= actual['acquired_at_us'] <= actual['lease_expires_at_us'] - 6_000_000
        fresh_xmin(role['xmin'], before); fresh_xmin(actual['xmin'], before)
        assert type(actual['control_backend_pid']) is int and actual['control_backend_pid'] > 0
        assert self.window['before_us'] <= instant(actual['control_backend_start']) <= actual['acquired_at_us']
        self.pg['role_instances'].append(dict(role_instance_id=rid, dataset_id=ds, role='control',
            created_at_us=role['created_at_us'], retired_at_us=None, xmin=role['xmin']))
        self.control = dict(previous, epoch=previous['epoch'] + 1, role_instance_id=rid,
            process_incarnation=incarnation, acquired_at_us=actual['acquired_at_us'],
            lease_expires_at_us=actual['lease_expires_at_us'], state='active', released_at_us=None,
            exclusion_method=None, exclusion_digest=None, control_backend_pid=actual['control_backend_pid'],
            control_backend_start=actual['control_backend_start'], xmin=actual['xmin'])
        self.pg['owner_epochs'].append(self.control)
        epoch = max(r['producer_epoch'] for r in before['producer_epochs']) + 1
        assert epoch in (4, 5)
        intent = only(observed['ingest_successions'], target_epoch=epoch)
        at, deadline, tx = intent['created_at_us'], intent['lease_expires_at_us'], intent['xmin']
        clock(at, self.window)
        assert actual['acquired_at_us'] <= at < deadline <= at + 5_000_000
        fresh_xmin(tx, self.pg)
        assert re.fullmatch('[1-9][0-9]*', intent['preparation_transaction'])
        assert int(intent['preparation_transaction']) < 2**64 and int(intent['preparation_transaction']) % 2**32 == int(tx)
        role_id, inc = self.identity(report['role_instance_id']), self.identity(report['process_incarnation'])
        domains = [self.identity(only(observed['allocation_domains'], owner_epoch=epoch,
            dictionary_kind=k)['allocation_domain'][2:]) for k in range(1, 10)]
        previous_intent = self.legacy if epoch == 3 else only(before['ingest_successions'], target_epoch=epoch - 1)
        previous_manifest, parent = binary(previous_intent['target_manifest']), binary(previous_intent['handoff'])
        source = only(before['source_streams'], owner_epoch=epoch - 1)
        producer = only(before['producer_epochs'], producer_epoch=epoch - 1)
        assert source['state'] == 'closed' and producer['state'] == 'retired'
        assert at >= producer['retired_at_us']
        kind = 1 if (bool(before['initial_ingest_activations']) if epoch == 3 else
            any(r['target_epoch'] == epoch - 1 for r in before['ingest_activations'])) else 2
        topology = self.ds['topology_generation'] + 1
        manifest = (bytes.fromhex('01010003000100010001') + binary(ds) + be(topology)
            + binary(self.resource) + be(0, 4) + be(epoch) + binary(inc) + previous_manifest[78:94]
            + be(self.ds['policy_revision']) + b'\x01\x09'
            + b''.join(be(k, 2) + binary(domain) for k, domain in enumerate(domains, 1)))
        record = (b'\x02\x0a' + bytes([kind]) + binary(self.slot) + be(epoch - 1)
            + be(producer['reserved_through_request']) + be(source['last_complete_sequence'])
            + binary(source['closure_digest']) + digest_bytes(parent)
            + be(len(previous_manifest), 2) + previous_manifest + be(len(manifest), 2) + manifest)
        assert len(manifest) == 266 and len(record) == 643
        policy = only(before['policy_revisions'], revision=self.ds['policy_revision'])
        limit = policy['document']['settings']['ingest.producer_outstanding_requests']
        assert type(limit) is int and limit in (2, 32)
        config = only(before['config_revisions'], revision=self.ds['config_revision'])
        self.plan = dict(epoch=epoch, created=at, deadline=deadline, role=role_id, incarnation=inc,
            domains=domains, topology=topology, manifest=manifest, record=record, limit=limit)
        self.pg['role_instances'].append(dict(role_instance_id=role_id, dataset_id=ds, role='ingest',
            created_at_us=at, retired_at_us=None, xmin=tx))
        target = only(observed['owner_epochs'], role='ingest', epoch=epoch)
        target_deadline = target['lease_expires_at_us']
        assert deadline <= target_deadline
        if target['xmin'] == tx:
            assert target_deadline == deadline
        else:
            fresh_xmin(target['xmin'], self.pg); clock(target_deadline - 6_000_000, self.window)
        self.pg['owner_epochs'].append(dict(resource_id=self.resource, epoch=epoch, dataset_id=ds,
            role='ingest', role_instance_id=role_id, process_incarnation=inc, acquired_at_us=at,
            lease_expires_at_us=target_deadline, state='active', released_at_us=None, exclusion_method=None,
            exclusion_digest=None, control_fence_version=None, control_backend_pid=None,
            control_backend_start=None, archive_frontier_protocol=None, xmin=target['xmin']))
        self.pg['source_streams'].append(dict(dataset_id=ds, shard_id=0, owner_epoch=epoch,
            owner_resource_id=self.resource, owner_role='ingest', owner_incarnation=inc, opened_at_us=at,
            state='open', closed_at_us=None, last_complete_sequence=None, closure_digest=None, xmin=tx))
        for number, domain in enumerate(domains, 1):
            self.pg['allocation_domains'].append(dict(dataset_id=ds, dictionary_kind=number,
                allocation_domain=domain, owner_resource_id=self.resource, owner_epoch=epoch, owner_role='ingest',
                owner_incarnation=inc, created_at_us=at, state='active', closed_at_us=None, closure_digest=None, xmin=tx))
        self.pg['producer_epochs'].append(dict(producer_slot_id=self.slot, producer_epoch=epoch, dataset_id=ds,
            producer_role_instance_id=role_id, producer_role='ingest', producer_incarnation=inc,
            owner_resource_id=self.resource, owner_epoch=epoch, owner_role='ingest', owner_incarnation=inc,
            outstanding_request_limit=limit, opened_at_us=at, state='open', retired_at_us=None,
            closure_digest=None, reserved_through_request=0, xmin=tx))
        self.pg['topology_generations'].append(dict(dataset_id=ds, generation=topology,
            previous_generation=self.ds['topology_generation'], created_at_us=at, sealed=True, xmin=tx))
        for e in range(1, epoch + 1):
            stream = only(self.pg['source_streams'], owner_epoch=e)
            self.pg['topology_streams'].append(dict(dataset_id=ds, topology_generation=topology, shard_id=0,
                owner_epoch=e, handoff_last_complete_sequence=stream['last_complete_sequence'], xmin=tx))
        old_topology = self.ds['topology_generation']
        self.ds.update(topology_generation=topology, xmin=tx)
        expected_intent = dict(dataset_id=ds, owner_resource_id=self.resource, owner_role='ingest',
            target_epoch=epoch, previous_epoch=epoch - 1, terminal_kind=kind, target_incarnation=inc,
            target_role_instance_id=role_id, producer_slot_id=self.slot, producer_epoch=epoch, shard_id=0,
            previous_topology=old_topology, target_topology=topology,
            issued_through=producer['reserved_through_request'], last_complete_sequence=source['last_complete_sequence'],
            closure_digest=source['closure_digest'], previous_manifest=previous_intent['target_manifest'],
            target_manifest=bx(manifest), previous_manifest_sha256=bx(digest_bytes(previous_manifest)),
            target_manifest_sha256=bx(digest_bytes(manifest)), handoff=bx(record), handoff_sha256=bx(digest_bytes(record)),
            parent_handoff_sha256=bx(digest_bytes(parent)), config_revision=self.ds['config_revision'],
            policy_revision=self.ds['policy_revision'], config_digest=config['document_digest'],
            policy_digest=policy['document_digest'], outstanding_request_limit=limit, created_at_us=at,
            lease_expires_at_us=deadline, prepared_by_resource=self.control['resource_id'],
            prepared_by_epoch=self.control['epoch'], prepared_by_role='control',
            prepared_by_incarnation=self.control['process_incarnation'],
            preparation_transaction=intent['preparation_transaction'], xmin=tx)
        self.pg['ingest_successions'].append(expected_intent)
        activation = only(observed['ingest_activations'], target_epoch=epoch)
        ax, activated = activation['xmin'], activation['activated_at_us']
        fresh_xmin(ax, self.pg); clock(activated, self.window)
        assert at <= activated < deadline
        assert re.fullmatch('[1-9][0-9]*', activation['activation_transaction'])
        assert int(activation['activation_transaction']) < 2**64 and int(activation['activation_transaction']) % 2**32 == int(ax)
        assert int(activation['activation_transaction']) > int(intent['preparation_transaction'])
        self.pg['ingest_activations'].append(dict({k: expected_intent[k] for k in ('dataset_id',
            'owner_resource_id', 'target_epoch', 'target_incarnation', 'target_role_instance_id',
            'producer_slot_id', 'producer_epoch', 'target_manifest_sha256', 'handoff_sha256')},
            activated_by_resource=self.control['resource_id'], activated_by_epoch=self.control['epoch'],
            activated_by_incarnation=self.control['process_incarnation'], activated_at_us=activated,
            activation_transaction=activation['activation_transaction'], xmin=ax))
        assert report == dict(format='bigbrotr-ingest-successor/1', state='activated', dataset_id=ds[2:],
            owner_resource_id=self.resource[2:], producer_slot_id=self.slot[2:], producer_epoch=str(epoch),
            process_incarnation=inc[2:], role_instance_id=role_id[2:], activated_at_us=str(activated),
            prepared_at_us=str(at), lease_expires_at_us=str(deadline), config_revision=str(self.ds['config_revision']),
            policy_revision=str(self.ds['policy_revision']), outstanding_request_limit=str(limit),
            predecessor=dict(producer_epoch=str(epoch - 1), resolved_requests=str(producer['reserved_through_request']),
                last_complete_sequence=str(source['last_complete_sequence']), sha256=source['closure_digest'][2:]),
            acquisition='not-started', ready=False, listeners=[dict(kind='private-session-http',
                host='127.0.0.1', port=18081, profile='initial-source-commands')])
        values = native_map(self.native)
        for cf, key, value in [('default', b'\x01\x01', manifest),
            ('default', b'\x01\x0a' + binary(ds) + binary(self.resource) + be(epoch - 1), record),
            ('default', b'\x01\x02' + binary(ds) + be(0, 4) + be(epoch), b'\x01\x02' + be(0)),
            ('receipts', b'\x01\x31' + binary(self.slot) + be(epoch), b'\x01\x31' + bytes(24))]:
            assert key == b'\x01\x01' or (cf, key.hex()) not in values
            values[cf, key.hex()] = value.hex()
        self.native.update(rows=native_rows(values), sequence=self.native['sequence'] + 4)
        same(self.pg, observed)

    def completed_control(self, observed):
        """Model only observable final authority; do not invent refresh history."""
        before = copy.deepcopy(self.pg)
        previous = max((r for r in before['owner_epochs'] if r['role'] == 'control'), key=lambda r:r['epoch'])
        assert previous['state'] == 'released'
        actual = only(observed['owner_epochs'], role='control', epoch=previous['epoch'] + 1)
        role = only(observed['role_instances'], role_instance_id=actual['role_instance_id'])
        rid, inc = self.identity(role['role_instance_id'][2:]), self.identity(actual['process_incarnation'][2:])
        for at in (role['created_at_us'], actual['acquired_at_us'], actual['released_at_us'],
                   role['retired_at_us'], actual['lease_expires_at_us'] - 6_000_000):
            clock(at, self.window)
        assert role['created_at_us'] <= actual['acquired_at_us'] <= actual['lease_expires_at_us'] - 6_000_000
        assert actual['lease_expires_at_us'] - 6_000_000 <= actual['released_at_us'] <= role['retired_at_us']
        assert actual['released_at_us'] < actual['lease_expires_at_us']
        assert type(actual['control_backend_pid']) is int and actual['control_backend_pid'] > 0
        assert self.window['before_us'] <= instant(actual['control_backend_start']) <= actual['acquired_at_us']
        fresh_xmin(actual['xmin'], before)
        self.control = dict(previous, epoch=previous['epoch'] + 1, role_instance_id=rid,
            process_incarnation=inc, acquired_at_us=actual['acquired_at_us'],
            lease_expires_at_us=actual['lease_expires_at_us'], state='released', released_at_us=actual['released_at_us'],
            exclusion_method='storage_fenced', control_backend_pid=actual['control_backend_pid'],
            control_backend_start=actual['control_backend_start'], xmin=actual['xmin'])
        self.control['exclusion_digest'] = exclusion(self.control)
        self.pg['owner_epochs'].append(self.control)
        self.pg['role_instances'].append(dict(role_instance_id=rid, dataset_id=self.ds['dataset_id'],
            role='control', created_at_us=role['created_at_us'], retired_at_us=role['retired_at_us'], xmin=actual['xmin']))

    def mutation(self, observed, operation, known):
        head, issued, cut, digest = (known[k] for k in ('head', 'issued', 'cut', 'digest'))
        producer = only(self.pg['producer_epochs'], producer_epoch=head)
        source = only(self.pg['source_streams'], owner_epoch=head)
        def coordinates(row, field, lower):
            at, xid = row[field], row['xmin']
            clock(at, self.window); fresh_xmin(xid, self.pg)
            assert max(lower, self.control['acquired_at_us']) <= at <= self.control['released_at_us']
            assert at < self.control['lease_expires_at_us']
            return at, xid
        if operation == 'recover':
            assert source['state'] == 'open' and producer['state'] in ('open', 'closing')
            if producer['state'] == 'open':
                row = only(observed['producer_epochs'], producer_epoch=head)
                fresh_xmin(row['xmin'], self.pg)
                producer.update(state='closing', xmin=row['xmin'])
                values = native_map(self.native)
                key = ('receipts', (b'\x01\x31' + binary(self.slot) + be(head)).hex())
                assert values[key] == (b'\x01\x31' + be(issued) + bytes(16)).hex()
                values[key] = (b'\x02\x31' + be(issued) + bytes(16) + b'\x01').hex()
                self.native.update(rows=native_rows(values), sequence=self.native['sequence'] + 1)
        elif operation == 'source':
            assert producer['state'] == 'closing'
            if source['state'] == 'open':
                row = only(observed['source_streams'], owner_epoch=head)
                at, tx = coordinates(row, 'closed_at_us', source['opened_at_us'])
                source.update(state='closed', closed_at_us=at, last_complete_sequence=cut,
                    closure_digest='\\x' + digest, xmin=tx)
            else:
                assert source['state'] == 'closed' and source['closure_digest'] == '\\x' + digest
                assert source['last_complete_sequence'] == cut
        elif operation == 'abandon':
            assert producer['state'] == 'closing' and source['state'] == 'closed'
            job = only(self.pg['jobs'], request_key='\\x' + 'e4' * 16)
            if job['state'] == 'queued':
                assert job['job_revision'] == 2 and job['checkpoint_revision'] == 0
                assert job['input_complete'] is True and job['result_manifest_digest'] is None
                activation = only(self.pg['ingest_activations'], target_epoch=head)
                assert tuple(job[k] for k in ('owner_resource_id','owner_epoch','owner_incarnation')) == tuple(
                    activation[k] for k in ('activated_by_resource','activated_by_epoch','activated_by_incarnation'))
                row = only(observed['acquisition_gaps'], job_id=job['job_id'])
                at, tx = coordinates(row, 'abandoned_at_us', source['closed_at_us'])
                self.pg['acquisition_gaps'].append(dict(xmin=tx, job_id=job['job_id'], reason='ingest_successor_abandoned',
                    shard_id=0, dataset_id=self.ds['dataset_id'], owner_role='control', issuer_epoch=self.control['epoch'],
                    producer_epoch=head, abandoned_at_us=at, issuer_resource=self.control['resource_id'],
                    producer_slot_id=self.slot, issuer_incarnation=self.control['process_incarnation'],
                    source_owner_epoch=head, previous_owner_epoch=job['owner_epoch'], previous_job_revision=2,
                    source_closure_digest='\\x' + digest, last_complete_sequence=cut,
                    previous_owner_resource=job['owner_resource_id'], previous_owner_incarnation=job['owner_incarnation']))
                job.update(state='cancelled', job_revision=4, reason_code='ingest_successor_abandoned', completed_at_us=at,
                    owner_resource_id=self.control['resource_id'], owner_epoch=self.control['epoch'],
                    owner_incarnation=self.control['process_incarnation'], xmin=tx)
            else:
                assert job['state'] == 'cancelled' and job['job_revision'] == 4
        else:
            assert operation == 'retire' and source['state'] == 'closed'
            job = only(self.pg['jobs'], request_key='\\x' + 'e4' * 16)
            gap = only(self.pg['acquisition_gaps'], job_id=job['job_id'])
            assert job['state'] == 'cancelled' and gap['producer_epoch'] == head
            if producer['state'] == 'closing':
                row = only(observed['producer_epochs'], producer_epoch=head)
                at, tx = coordinates(row, 'retired_at_us', max(source['closed_at_us'], gap['abandoned_at_us']))
                receipt = only(observed['ingest_terminal_transactions'], target_epoch=head)
                assert re.fullmatch('[1-9][0-9]*', receipt['terminal_transaction'])
                assert int(receipt['terminal_transaction']) < 2**64 and int(receipt['terminal_transaction']) % 2**32 == int(tx)
                producer.update(state='retired', retired_at_us=at, closure_digest='\\x' + digest, xmin=tx)
                owner = only(self.pg['owner_epochs'], resource_id=self.resource, epoch=head)
                owner.update(state='released', released_at_us=at, exclusion_method='storage_fenced',
                    exclusion_digest='\\x' + digest, xmin=tx)
                only(self.pg['role_instances'], role_instance_id=owner['role_instance_id']).update(retired_at_us=at, xmin=tx)
                domains = [r for r in self.pg['allocation_domains'] if r['owner_epoch'] == head]
                assert len(domains) == 9
                for row in domains:
                    row.update(state='closed', closed_at_us=at, closure_digest='\\x' + digest, xmin=tx)
                self.pg['ingest_terminal_transactions'].append(dict(owner_resource_id=self.resource, target_epoch=head,
                    terminal_transaction=receipt['terminal_transaction'], control_resource=self.control['resource_id'],
                    control_epoch=self.control['epoch'], control_incarnation=self.control['process_incarnation'], xmin=tx))
            else:
                assert producer['state'] == 'retired'
        same(self.pg, observed)


def science(seed):
    """Pinned, previously qualified receipts; derive the new closure literally."""
    pg, native, inputs = seed
    head = max(r['producer_epoch'] for r in pg['producer_epochs'])
    producer = only(pg['producer_epochs'], producer_epoch=head)
    intent = only(pg['ingest_successions'], target_epoch=head)
    values = native_map(native)
    ds, slot = binary(producer['dataset_id']), binary(producer['producer_slot_id'])
    issued = producer['reserved_through_request']
    assert head in (3, 4) and issued in (2, 3) and producer['state'] == 'open'
    cut = int.from_bytes(bytes.fromhex(values['default', (b'\x01\x02' + ds + bytes(4) + be(head)).hex()])[2:], 'big')
    assert cut in (5, 10)
    preimage = (b'bigbrotr/ingest-successor-source-closure/1\0' + digest_bytes(binary(intent['handoff']))
        + digest_bytes(binary(intent['target_manifest'])) + slot + be(head) + be(issued))
    for seq in range(1, issued + 1):
        request = only(pg['producer_requests'], producer_epoch=head, request_sequence=seq)
        outcome = only(pg['producer_outcomes'], producer_epoch=head, request_sequence=seq)
        assert outcome['outcome_sha256'] == bx(digest_bytes(binary(outcome['outcome_bytes'])))
        assert outcome['request_digest'] == request['request_digest']
        preimage += be(seq) + binary(request['request_digest']) + binary(outcome['outcome_sha256'])
    return dict(head=head, issued=issued, cut=cut, digest=sha(preimage + be(cut)))

SEED_HASHES = {'activated': ['5164627148d56ad4de192e8bdec994163edb527212a1037766b0ba6227acd9f7', 'db3031a072f2642f1f27a0d0fdd8576e7adfe412393bf73f1514fe3a0a532099', 'fedeae1f6ee5a679f33eb1b23a0b9130893cd38090cdcc8ef3957dfb270a0d5c'], 'empty': ['b9f01d9bd285e8488bca65d747a7c45471322dd88fa13cbfa7f6519c79de88a8', '3bd5b1813b08330d99475327e6fe979703e65aaea94fadac8c9d861d2f25493f', 'a7cb5f1407c842f33499bebb8f1b63abc95283af7a7641b71ec936c0be08429f'], 'later': ['4e2b5833f4090de7714811f89c8b2836b21d28cd075d8d5b1c60e17b9b0342fa', '94068026657928a7dfadc5337fe13f61d5d2996922d185c6a629a380e543553e', '0c2b19593469676cdaed215a7b79943e97752f494557b126e8fd2f4bd9dae185']}
