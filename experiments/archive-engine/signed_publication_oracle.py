"""Independent literal signed corpus; no product codecs, Arrow or observed templates.

CBOR comes from the existing separately authored bootstrap oracle. Scientific
fields and descriptor constants come from the closed v1 contracts. This is a
fixed experiment, not a generic native reader or a new product export format.
"""
import hashlib
import json
import struct
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'experiments/ingest-bootstrap'))
from oracle import cbor

DATASET = bytes([1]) * 16
MODES = ('normal', 'before', 'after', 'owner_first', 'held_commit', 'held_rollback')
EVENT = json.loads((ROOT / 'experiments/identity-contract/signed-fixture.json').read_text())['events']['E_public']
TABLES = ('dictionaries', 'events', 'event_eligibility_facts', 'event_receive_facts', 'provenance_facts', 'archive_commits')
DESCRIPTOR_FIELDS = ('event_id class url clock vantage software method_id public_key route_class '
                     'software_version label visibility_scope_id input algorithm capability output_unit '
                     'failure_taxonomy algorithm_version protocol_revision format format_version '
                     'source_dataset_id source_artifact_digest').split()


def digest(prefix, value):
    return hashlib.sha256(prefix + cbor(value)).digest()


def reference(kind):
    return [DATASET, kind, bytes([kind]) * 16, 1]


def ref_row(kind):
    return dict(zip(('dataset_id', 'dictionary_kind', 'allocation_domain', 'ordinal'),
                    (DATASET.hex().upper(), kind, (bytes([kind]) * 16).hex().upper(), 1)))


def source(sequence):
    return [DATASET, 0, 1, sequence]


def corpus(event=EVENT, history=False):
    body = [bytes.fromhex(event['id']), bytes.fromhex(event['pubkey']), event['created_at'],
            event['kind'], event['tags'], event['content']]
    body_digest = digest(b'bigbrotr/body/1\0', body)
    clock = [reference(2), 1, None, 0]
    descriptors = [
        (4, 'visibility', {'class': 'public'}),
        (1, 'endpoint', {'url': 'wss://public.example/'}),
        (2, 'observer', dict(clock='system-utc', vantage='fixture-vantage', software=None,
                             method_id=None, public_key=None, route_class=None, software_version=None)),
        (3, 'access-scope', dict(class_='public', label=None, visibility_scope_id=reference(4))),
        (5, 'event-verification', dict(input='nip01-event/1', algorithm='sha256-bip340',
            capability='event-id-and-signature', output_unit='verification-result',
            failure_taxonomy='bigbrotr-nip01-verifier/1', algorithm_version=1,
            protocol_revision='a2494f4f81d46684e5814a9bf35e2b1df978f955')),
        (8, 'event', {'event_id': body[0]}),
    ]
    descriptors[3][2]['class'] = descriptors[3][2].pop('class_')
    payloads = [('dictionary', [reference(k), 1, schema, 1, value]) for k, schema, value in descriptors]
    payloads += [
        ('event-payload', [*body, reference(8)]),
        ('event-eligibility', [body[0], body_digest, bytes.fromhex(event['sig']), 10_000_000, 'network', None]),
        ('event-receive', [body[0], 10_000_000, reference(2), clock, None]),
        ('provenance', [body[0], reference(1), reference(2), reference(3), 10_000_000, clock, reference(5), None]),
    ]
    if history:
        payloads += [
            ('event-receive', [body[0], 5_000_000, reference(2), clock, source(9)]),
            ('provenance', [body[0], reference(1), reference(2), reference(3), 5_000_000, clock, reference(5), source(10)]),
        ]
    records, tables = [], {name: [] for name in TABLES}
    clock_row = dict(clock_id=ref_row(2), resolution_us=1, uncertainty_us=None, anomaly_flags=0)
    for sequence, (kind, payload) in enumerate(payloads, 1):
        time = 0 if sequence <= 5 else (20_000_000 if sequence > 10 else 10_000_000)
        preimage = b'bigbrotr/fact/1\0' + cbor([source(sequence), kind, 1, reference(4), time, payload])
        fact_digest = hashlib.sha256(preimage).digest()
        records.append(dict(sequence=sequence, digest=fact_digest.hex(), preimage_hex=preimage.hex()))
        row = dict(source_record_id=dict(dataset_id=DATASET.hex().upper(), shard_id=0, owner_epoch=1, sequence=sequence),
                   record_schema_version=1, record_kind=kind, visibility_scope_id=ref_row(4),
                   payload_digest=fact_digest.hex().upper(), recorded_at=time)
        if kind == 'dictionary':
            k, schema, value = descriptors[sequence - 1]
            descriptor = dict.fromkeys(DESCRIPTOR_FIELDS)
            descriptor.update(value)
            if k == 3:
                descriptor['visibility_scope_id'] = ref_row(4)
            if k == 8:
                descriptor['event_id'] = event['id'].upper()
            row.update(reference=ref_row(k), normalization_version=1, descriptor_schema=schema,
                       descriptor_version=1, descriptor_value=descriptor)
            table = 'dictionaries'
        elif kind == 'event-payload':
            row.update(event_id=event['id'].upper(), pubkey=event['pubkey'].upper(), created_at=event['created_at'],
                       kind=event['kind'], tags=event['tags'], content=event['content'], event_ordinal=ref_row(8))
            table = 'events'
        elif kind == 'event-eligibility':
            row.update(event_id=event['id'].upper(), body_digest=body_digest.hex().upper(), sig=event['sig'].upper(),
                       first_eligible_at=10_000_000, origin='network', import_lineage=None)
            table = 'event_eligibility_facts'
        else:
            corrects = dict(dataset_id=DATASET.hex().upper(), shard_id=0, owner_epoch=1, sequence=sequence-2) if sequence > 10 else None
            row.update(event_id=event['id'].upper(), received_at=5_000_000 if sequence > 10 else 10_000_000, observer_id=ref_row(2),
                       clock_quality=clock_row, corrects=corrects)
            table = 'event_receive_facts'
            if kind == 'provenance':
                row.update(endpoint_id=ref_row(1), access_scope_id=ref_row(3), verification_method_id=ref_row(5))
                table = 'provenance_facts'
        tables[table].append(row)
    batches = []
    for first, last in [(1, 5), (6, 10)] + ([(11, 12)] if history else []):
        facts = records[first - 1:last]
        references = [[source(r['sequence']), bytes.fromhex(r['digest'])] for r in facts]
        batches.append(dict(first=first, last=last, facts=facts,
            admission_digest=digest(b'bigbrotr/admission-batch/1\0', [DATASET, 0, 1, first, last, references]).hex(),
            archive_digest=digest(b'bigbrotr/archive-batch/1\0', [DATASET, 1, 0, [[0, 1, first, last]], references]).hex()))
    return batches, tables


def expected(mode, bootstrap_only=False):
    assert mode in MODES or mode == 'history'
    batches, tables = corpus(history=mode == 'history')
    empty_event = mode == 'before' or bootstrap_only
    if empty_event:
        for name in tables:
            tables[name] = [r for r in tables[name] if r['source_record_id']['sequence'] <= 5]
    for index, batch in enumerate(batches[:1] if empty_event else batches):
        ordinal = 2 if index == 0 else 5 if index == 2 else 4 if mode in ('owner_first', 'held_rollback') else 3
        n = 180 + (8 if mode == 'history' else MODES.index(mode)) * 8 + ordinal
        tables['archive_commits'].append(dict(dataset_id=DATASET.hex().upper(), schema_version=1,
            topology_generation=0, batch_digest=batch['archive_digest'].upper(), record_count=batch['last']-batch['first']+1,
            input_ranges=[dict(shard_id=0, owner_epoch=1, first_sequence=batch['first'], last_sequence=batch['last'])],
            attempt_id=(bytes([n]) * 16).hex().upper(), publication_ordinal=ordinal))
    return dict(event='readback', mode=mode, tables=tables, signed_event=None if empty_event else EVENT)


def input_expectations(mode):
    batches, _ = corpus(history=mode == 'history')
    result = []
    for ordinal in range(1, 6 if mode == 'history' else 4 if mode in ('before', 'after') else 5):
        batch = batches[0 if ordinal == 2 else 2 if ordinal == 5 else 1]
        n = 180 + (8 if mode == 'history' else MODES.index(mode)) * 8 + ordinal
        result.append(dict(dataset_id=DATASET.hex(), schema_version=1, topology_generation='0',
            batch_digest=batch['archive_digest'], record_count=batch['last']-batch['first']+1,
            input_ranges=[dict(shard_id=0, owner_epoch=1, first_sequence=batch['first'], last_sequence=batch['last'])],
            attempt_id=[n] * 16, publication_ordinal=ordinal))
    return result


def restart_expectations(mode):
    committed = expected(mode)['tables']['archive_commits']
    result = []
    for wanted in input_expectations(mode):
        same_digest = [r for r in committed if r['batch_digest'].lower() == wanted['batch_digest']]
        assert len(same_digest) <= 1
        status = 'absent'
        if same_digest:
            row = same_digest[0]
            status = ('present' if row['attempt_id'].lower() == bytes(wanted['attempt_id']).hex()
                      and row['publication_ordinal'] == wanted['publication_ordinal'] else 'conflict')
        result.append(dict(event='inputs-restart', expected=wanted, receipt=status))
    return result


def input_files(home, mode):
    # Original initialization bytes have host-specific inode/path fields. All
    # source, request and header fields below come from the independent corpus.
    initialization = hashlib.sha256((home/'kind=7/.bigbrotr-initialization/intent.json').read_bytes()).digest()
    batches, _ = corpus()
    result = []
    for expected in input_expectations(mode):
        ordinal = expected['publication_ordinal']
        n = expected['attempt_id'][0]
        batch = batches[0 if ordinal == 2 else 1]
        body = (b'BBINPUT\x01' + initialization + bytes([n])*16 + DATASET + bytes([90])*16
                + bytes([80])*16 + bytes([n+1])*16 + struct.pack('>QQ', ordinal, 0)
                + bytes.fromhex(batch['archive_digest']) + struct.pack('>I', 1)
                + struct.pack('>IQQ', 0, 1, batch['first']) + b'\x01\x21'
                + struct.pack('>Q', batch['last']) + bytes.fromhex(batch['admission_digest']))
        wanted = body + hashlib.sha256(body).digest()
        path = home/'archive-state'/((bytes([n])*16).hex()+'.inputs')
        assert path.read_bytes() == wanted, 'independent complete retained input bytes'
        assert path.stat().st_mode & 0o777 == 0o600 and path.stat().st_nlink == 1
        result.append(dict(name=path.name, bytes=len(wanted), sha256=hashlib.sha256(wanted).hexdigest()))
    assert sorted(p.name for p in (home/'archive-state').glob('*.inputs')) == sorted(r['name'] for r in result)
    return result


def producer_input_files(home):
    """Exact original mutation bytes, independently authored from the fixed corpus."""
    descriptors = [
        (4, 'visibility', {'class': 'public'}),
        (1, 'endpoint', {'url': 'wss://public.example/'}),
        (2, 'observer', dict(clock='system-utc', vantage='fixture-vantage', software=None,
                             method_id=None, public_key=None, route_class=None, software_version=None)),
        (3, 'access-scope', {'class': 'public', 'label': None, 'visibility_scope_id': reference(4)}),
        (5, 'event-verification', dict(input='nip01-event/1', algorithm='sha256-bip340',
            capability='event-id-and-signature', output_unit='verification-result',
            failure_taxonomy='bigbrotr-nip01-verifier/1', algorithm_version=1,
            protocol_revision='a2494f4f81d46684e5814a9bf35e2b1df978f955')),
    ]
    dictionaries = [['dictionary', 1, [reference(k), 1, schema, 1, value]]
                    for k, schema, value in descriptors]
    inputs = [('admit-evidence', [1, reference(4), dictionaries])]
    event = EVENT
    for sequence in [2, 3, 4]:
        signed = [bytes(32) if sequence == 3 else bytes.fromhex(event['id']),
                  bytes.fromhex(event['pubkey']), event['created_at'], event['kind'],
                  event['tags'], event['content'],
                  bytes(64) if sequence == 2 else bytes.fromhex(event['sig'])]
        observation = [10_000_000, reference(2), [reference(2), 1, None, 0],
                       [reference(1), reference(3), reference(5)]]
        inputs.append(('admit', [1, reference(4), signed, 'network', None, observation]))
    spool = home / 'rocks/producer-inputs-v1'
    assert spool.stat().st_mode & 0o777 == 0o700 and not spool.is_symlink()
    result = []
    for sequence, (operation, intent) in enumerate(inputs, 1):
        slot = bytes([66]) * 16
        wanted = b'bigbrotr/producer-request/1\0' + cbor([[slot, 1, sequence], operation, 1, intent])
        path = spool / f'{slot.hex()}-0000000000000001-{sequence:016x}.cbor'
        assert path.is_file() and not path.is_symlink()
        assert path.read_bytes() == wanted, 'independent original producer input bytes'
        assert path.stat().st_mode & 0o777 == 0o600 and path.stat().st_nlink == 1
        result.append(dict(name=path.name, bytes=len(wanted), sha256=hashlib.sha256(wanted).hexdigest()))
    assert sorted(p.name for p in spool.iterdir()) == sorted(r['name'] for r in result)
    return result


def check(mode, parent, reader, installed=False):
    reopened = [r['expected'] for r in parent if r['event'] == 'inputs-reopened']
    assert reopened == (input_expectations(mode) if installed else [])
    parent = [r for r in parent if r['event'] != 'inputs-reopened']
    if installed and mode == 'normal':
        assert parent[2:6] == [
            dict(event='connect-ready'),
            dict(event='connect-refused',error='UnsafeRole',commit_started=False),
            dict(event='usage-ready'),
            dict(event='usage-refused',error='UnsafeRole',commit_started=False),
        ]
        parent = parent[:2] + parent[6:]
    stages = ['signed-reopened', 'retained-inputs']
    if mode in ('owner_first', 'held_commit', 'held_rollback'):
        stages += ['activation-begin', 'activation-complete', 'old-excluded']
        assert parent[2]['backend_pid'] > 0
        assert parent[3] == dict(event='activation-complete', ordinal=4)
        assert parent[4] == dict(event='old-excluded', worker_active_permits=0)
    assert [r['event'] for r in parent] == stages + ['terminal']
    before, pending, terminal = parent[0], parent[1], parent[-1]
    assert before['exact_admission_retry'] and before['rejections'] == ['Signature', 'EventId']
    assert before['rocks_rows'] > 10 and before['rocks_rows'] == terminal['rocks_rows']
    assert before['rocks_digest'] == terminal['rocks_digest'] and len(before['rocks_digest']) == 64
    assert pending['batches'] == corpus(history=mode == 'history')[0], 'independent retained fact/admission/archive identities'
    assert terminal['worker_active_permits'] == 0 and terminal['mode'] == mode
    outcomes = []
    for ordinal, first in [(1, 6), (2, 1), (3, 6), (4, 6), (5, 11)][:5 if mode == 'history' else 3 if mode in ('before', 'after') else 4]:
        n = 180 + (8 if mode == 'history' else MODES.index(mode)) * 8 + ordinal
        snapshots, files = ((5, 11) if ordinal == 5 else (2, 0) if ordinal == 1 else (3, 2) if ordinal == 2 or
                            ordinal == 3 and mode in ('before', 'owner_first', 'held_rollback') else (4, 8))
        success = ordinal == 2 or ordinal == 3 and mode in ('normal', 'history', 'held_commit') or ordinal == 4 and mode in ('owner_first', 'held_rollback') or ordinal == 5
        receipt = ('conflict' if ordinal == 4 and not success else
                   'present' if success or ordinal == 3 and mode == 'after' else 'absent')
        outcomes.append(dict(ordinal=ordinal, first=first, success=success,
            snapshots=snapshots, files=files, attempt_id=(bytes([n]) * 16).hex(), receipt=receipt))
    assert terminal['outcomes'] == outcomes, 'publication outcomes'
    assert reader == expected(mode), 'complete scientific rows and signed event'
