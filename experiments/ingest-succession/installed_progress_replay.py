"""Independent finite producer-progress encoding for the installed fixture.

Only literal expected records may establish semantic success. Hashing an
observed head is an integrity check, never its own expected classification.
This module is stdlib-only apart from the existing independent CBOR encoder.
No product helper, SQL function or native decoder supplies expected bytes.
"""
import copy
import hashlib
import json
import re

from live_replay import body, cbor

DOMAIN = b'bigbrotr/producer-progress/1\0'
TABLES = {'acquisition_progress', 'producer_checkpoint_dependencies'}
IDENTITY = {'dataset_id', 'producer_slot_id', 'producer_epoch'}
JOB_FIELDS = IDENTITY | {'job_id', 'scope_id', 'progress_revision', 'request_id',
    'event_ordinal', 'wire_sequence', 'input_sha256', 'intent_digest',
    'classification', 'mutation_request', 'classified_events', 'successful_events',
    'rejected_events', 'observed_at_us', 'clock_resolution_us',
    'clock_anomaly_flags', 'xmin'}
DEPENDENCY_FIELDS = IDENTITY | {'source_shard', 'source_epoch',
    'source_sequence', 'payload_digest', 'xmin'}
HEAD_ADDITIONS = {'progress_format', 'manifest_sha256', 'progress_jobs',
    'dependency_count', 'issuer_resource', 'issuer_epoch', 'issuer_incarnation'}
HEAD_FIELDS = IDENTITY | HEAD_ADDITIONS | {'checkpoint_revision',
    'complete_through_request', 'checkpoint_digest', 'committed_at_us', 'xmin'}
REQUEST_ADDITIONS = {'progress_request_id', 'progress_event_ordinal',
    'progress_wire_sequence', 'progress_input_sha256'}
JOB_BYTES, DEPENDENCY_BYTES = 216, 72
MAX_JOBS, MAX_DEPENDENCIES = 64, 16384

# Literal unions independently derived from NetworkScience's dictionary/NEW
# receipts and OLD proof (off) or witness receipt (on). The native reducer adds
# no implicit context references. These are not obtained from captured SQL.
CASE_EXPECTATIONS = {
    'activated': {
        'dependency_coordinates': ((1, 3), (1, 4), (1, 8), (1, 9), (1, 10), (1, 11),
            (3, 1), (3, 2), (3, 3), (3, 4), (3, 5)),
        'split': {
            'mutation': dict(outcome='COMMITTED', jobs=1, checkpoint_revision=3,
                complete_through_request=2, dependency_count=11,
                job=dict(progress_revision=2, event_ordinal=2, classification=5,
                    mutation_request=None, classified_events=2, successful_events=2, rejected_events=0)),
            'proof-only': dict(outcome='COMMITTED', jobs=1, checkpoint_revision=4,
                complete_through_request=2, dependency_count=11,
                job=dict(progress_revision=3, event_ordinal=1, classification=5,
                    mutation_request=None, classified_events=3, successful_events=3, rejected_events=0)),
        },
        'hole': {
            'mutation': dict(outcome='COMMITTED', jobs=0, checkpoint_revision=1,
                complete_through_request=1, dependency_count=2),
            'proof-only': dict(outcome='REFUSAL'),
        },
    },
    'empty': {
        'dependency_coordinates': ((1, 3), (1, 4), (1, 8), (1, 9), (1, 10), (1, 11),
            (3, 1), (3, 2), (3, 3), (3, 4), (3, 5), (3, 6), (3, 7), (3, 8), (3, 9), (3, 10)),
        'split': {
            'mutation': dict(outcome='COMMITTED', jobs=1, checkpoint_revision=3,
                complete_through_request=3, dependency_count=16,
                job=dict(progress_revision=2, event_ordinal=2, classification=2,
                    mutation_request=3, classified_events=2, successful_events=2, rejected_events=0)),
            'proof-only': dict(outcome='COMMITTED', jobs=1, checkpoint_revision=4,
                complete_through_request=3, dependency_count=16,
                job=dict(progress_revision=3, event_ordinal=1, classification=5,
                    mutation_request=None, classified_events=3, successful_events=3, rejected_events=0)),
        },
        'hole': {
            'mutation': dict(outcome='COMMITTED', jobs=0, checkpoint_revision=1,
                complete_through_request=1, dependency_count=5),
            'proof-only': dict(outcome='REFUSAL'),
        },
    },
    'later': {
        'dependency_coordinates': ((1, 3), (1, 4), (1, 8), (1, 9), (1, 10), (1, 11),
            (4, 1), (4, 2), (4, 3), (4, 4), (4, 5), (4, 6), (4, 7), (4, 8), (4, 9), (4, 10)),
        'split': {
            'mutation': dict(outcome='COMMITTED', jobs=1, checkpoint_revision=3,
                complete_through_request=3, dependency_count=16,
                job=dict(progress_revision=2, event_ordinal=2, classification=2,
                    mutation_request=3, classified_events=2, successful_events=2, rejected_events=0)),
            'proof-only': dict(outcome='COMMITTED', jobs=1, checkpoint_revision=4,
                complete_through_request=3, dependency_count=16,
                job=dict(progress_revision=3, event_ordinal=1, classification=5,
                    mutation_request=None, classified_events=3, successful_events=3, rejected_events=0)),
        },
        'hole': {
            'mutation': dict(outcome='COMMITTED', jobs=0, checkpoint_revision=1,
                complete_through_request=1, dependency_count=5),
            'proof-only': dict(outcome='REFUSAL'),
        },
    },
}

# Frozen to the reviewed migration and qualified component build. product_source
# names that exact build, not an isolated author packet or a later fixture-only
# runner revision. No caller can replace the golden or turn a valid digest into PASS.
FROZEN_PROFILE = {
    'migration_sha256': '267045f6f5c9ee44c1a9dc3335c7092c056606662bd4016cb7c2a81b19c421f0',
    'product_source': 'd4e1b85566dee30994017834b763fe04396bcf1c',
    'cases': CASE_EXPECTATIONS,
}

# Literal finite ACK recipe. No value is obtained from an observed head/ACK.
# This keeps the historical0039 profile intact while exact0040/build identity
# is pinned to the qualified component packet below. Installed outcomes remain
# NOT_RUN; epoch4/window32 and full-window proof are separate gates.
ACK_EXPECTATIONS = {
    'initial': dict(epoch=1,request=6,mutation_revision=6,
        dependencies=tuple((1,n) for n in range(1,23)),key='b1'),
    'activated': dict(epoch=2,request=4,mutation_revision=4,
        dependencies=((1,3),*tuple((2,n) for n in range(1,20))),key='f4'),
    'successor': dict(epoch=3,request=4,mutation_revision=4,
        dependencies=((1,3),*tuple((3,n) for n in range(1,14))),key='e4'),
}
ACK_MIGRATION_SHA256 = '47eaef37cc8bb475a7b597da0814c5a9dc3d4f983ed9506fdfd734d35fb64af6'
ACK_PROFILE = dict(migration_sha256=ACK_MIGRATION_SHA256,
    product_source='fcad4c9a01ad1aa2cc5dd419707414c9516e97ce',cases=ACK_EXPECTATIONS)
ACK_EXECUTABLES_SHA256 = {'bigbrotr': '4c1840a4957e0939ac4b2679e9d3813ac962f3ba7f6750f7a55511cc73a339fa',
 'control_schema': '82b81eb6eb09433795e3e06940f6723c09ab9e72fae913c182c68c7761c979d4',
 'evidence_intent': '9f50cfe4d853f113d3e6b445ed72b98d9e371c174883a24223e6a58ec1634cdf',
 'fact_codec': '11458f28cbbf95a3e942f2a629666d4de62ff58245cf8784269d7c4158db9c07',
 'ingest_bootstrap_probe': '71696873eb80665817b81961f5f16cb053c491af1b19b9b5e1b3e47b69175e5e',
 'migrate_entrypoint': '82ccd47da1bf342a41421083fea51a8fe0da9aec6853e07b74c7232514412d57',
 'native_components': 'bc330c1b78c47b1a768af54ef8db58bf1ded798ef3c9a1af135933b495df1a0c',
 'sync_progress': '72cdf877d436cbb520d233f4c0e150647058b26c959ae796c9f35b6ee35ea02e'}

# Additive initial-only scenario, pinned to root's measured262-test/19-group/
# 34-control component packet. Installed qualification is still a separate run.
# Historical ACK_PROFILE and ACK_EXECUTABLES_SHA256 retain their exact meaning.
PREPARATION_HOLE_EXPECTATIONS = {
    'initial': dict(epoch=1,request=7,mutation_revision=5,continuation_revision=8,
        dependencies=tuple((1,n) for n in range(1,28)),key='b1'),
}
PREPARATION_HOLE_PROFILE = dict(migration_sha256=ACK_MIGRATION_SHA256,
    product_source='9cef25f13db44cac1cdd55e1862229ba7298f3b0',cases=PREPARATION_HOLE_EXPECTATIONS)
PREPARATION_HOLE_BUILD = dict(tree='a473399f2eca0c953af9a2d61abfb4efb4434924',
    source_manifest_sha256='6b173bd0e36b12d0a7d23510c710d8dae2bf345dae42100d666c43e59a23cc57',
    lockfile_sha256='5e84342a2b6ef96ddfe243cb003ac07e8faa1d8f603064b01300d171a65c0db9')
PREPARATION_HOLE_COMPONENT_CHECKS_SHA256 = 'e090daa0be760e2ca32775137ae501a83d1801b876ef47db0a0def34de21dbef'
PREPARATION_HOLE_EXECUTABLES_SHA256 = {
    'native_components':'e5401bf206fab743bf859a626d70b332f254bb891673b944c2a84c6c6fbc69de',
    'sync_progress':'fac5b64eabeeee26d8c0da23a53a27f9fadbb36e772bf130e29fb5901ad227d0',
    'fact_codec':'11458f28cbbf95a3e942f2a629666d4de62ff58245cf8784269d7c4158db9c07',
    'evidence_intent':'9f50cfe4d853f113d3e6b445ed72b98d9e371c174883a24223e6a58ec1634cdf',
    'control_schema':'3299f5197abf2fc4e3b397ee1dc10157faec4eccb6e2d9060c16447a4657d9ee',
    'migrate_entrypoint':'82ccd47da1bf342a41421083fea51a8fe0da9aec6853e07b74c7232514412d57',
    'ingest_bootstrap_probe':'654d33a8a2ee26b8be221df31ad60ab66fc815dcae3f093788b9f3b855a37f34',
    'bigbrotr':'2398dfc457f643de16ee8e6e73171eef9efaf2c31c0b5ce7e028b1d195a6bc6c',
}

# A distinct recipe on the same measured product. Historical recipe/build
# profiles retain their exact meaning; this is not an alias for an old capture.
POST_HOLE_PROOF_EXPECTATIONS = {
    'initial': dict(epoch=1,request=6,mutation_revision=6,proof_revision=7,
        dependencies=tuple((1,n) for n in range(1,23)),key='b1'),
}
POST_HOLE_PROOF_PROFILE = dict(migration_sha256=ACK_MIGRATION_SHA256,
    product_source='9cef25f13db44cac1cdd55e1862229ba7298f3b0',cases=POST_HOLE_PROOF_EXPECTATIONS)


def preparation_hole_qualification(built, component, qualification, *, post_hole_proof=False):
    profile=require_frozen_profile(ack=True,preparation_hole=not post_hole_proof,post_hole_proof=post_hole_proof)
    assert component['status']=='PASS' and qualification['status']=='PASS_PRODUCER_ACK_COMPONENTS'
    assert built['source']==component['source']==qualification['source']==profile['product_source']
    assert all({key:value[key] for key in PREPARATION_HOLE_BUILD}==PREPARATION_HOLE_BUILD
        for value in (built,component,qualification))
    assert component['executables_sha256']==ack_executables(preparation_hole=True)
    assert component['test_counts']==qualification['test_counts']
    selection={key:component[key] for key in ('test_counts','test_names','required_controls','missing_controls')}
    encoded=json.dumps(selection,sort_keys=True,separators=(',',':'),ensure_ascii=False).encode()
    assert hashlib.sha256(encoded).hexdigest()==PREPARATION_HOLE_COMPONENT_CHECKS_SHA256


def phase_names(preparation_hole=False):
    assert type(preparation_hole) is bool
    return ('mutation','continuation' if preparation_hole else 'proof-only')


def ack_executables(*, preparation_hole=False, post_hole_proof=False):
    assert type(post_hole_proof) is bool and not (preparation_hole and post_hole_proof)
    if not (preparation_hole or post_hole_proof):
        return ACK_EXECUTABLES_SHA256
    pins=PREPARATION_HOLE_EXECUTABLES_SHA256
    assert pins is not None, 'preparation-hole fixture NOT_READY: measured executable pins pending'
    assert set(pins)==set(ACK_EXECUTABLES_SHA256)
    assert all(re.fullmatch('[0-9a-f]{64}',value) for value in pins.values())
    return pins

# Additive authority only in the explicit0039 fixture. The ordinary0038
# runner retains its original two grant statements.
PROGRESS_GRANTS = """GRANT SELECT,INSERT,UPDATE ON
    control.producer_checkpoints,control.acquisition_progress TO bigbrotr_control;
GRANT SELECT,INSERT ON control.producer_checkpoint_dependencies TO bigbrotr_control;
GRANT EXECUTE ON FUNCTION
    control.acquisition_progress_bytes(control.acquisition_progress),
    control.producer_progress_digest(control.producer_checkpoints),
    control.require_producer_progress_capacity(bytea,numeric,bigint,integer,integer),
    control.require_retained_producer_progress(bytea,numeric)
    TO bigbrotr_control;
"""


def require_frozen_profile(*, ack=False, preparation_hole=False, post_hole_proof=False):
    assert type(ack) is bool
    assert type(preparation_hole) is bool and (not preparation_hole or ack)
    assert type(post_hole_proof) is bool and (not post_hole_proof or ack and not preparation_hole)
    if post_hole_proof:
        assert POST_HOLE_PROOF_PROFILE==dict(migration_sha256=ACK_MIGRATION_SHA256,
            product_source='9cef25f13db44cac1cdd55e1862229ba7298f3b0',cases=POST_HOLE_PROOF_EXPECTATIONS)
        assert POST_HOLE_PROOF_PROFILE['product_source']==PREPARATION_HOLE_PROFILE['product_source']
        ack_executables(post_hole_proof=True)
        return POST_HOLE_PROOF_PROFILE
    if preparation_hole:
        profile=PREPARATION_HOLE_PROFILE
        assert profile is not None, 'preparation-hole fixture NOT_READY: reviewed source/build pins pending'
        assert set(profile)=={'migration_sha256','product_source','cases'}
        assert profile['migration_sha256']==ACK_MIGRATION_SHA256
        assert re.fullmatch('[0-9a-f]{40}',profile['product_source'])
        assert profile['product_source']!=ACK_PROFILE['product_source']
        assert profile['cases']==PREPARATION_HOLE_EXPECTATIONS
        ack_executables(preparation_hole=True)
        return profile
    if ack:
        assert ACK_PROFILE is not None, 'producer ACK fixture NOT_READY: exact reviewed0040/build/profile pending'
        assert set(ACK_PROFILE)=={'migration_sha256','product_source','cases'}
        assert ACK_PROFILE['migration_sha256']==ACK_MIGRATION_SHA256
        assert re.fullmatch('[0-9a-f]{40}',ACK_PROFILE['product_source'])
        assert ACK_PROFILE['cases']==ACK_EXPECTATIONS
        return ACK_PROFILE
    assert FROZEN_PROFILE is not None, (
        'producer progress fixture NOT_READY: final schema/source/expectations pending')
    assert set(FROZEN_PROFILE) == {'migration_sha256', 'product_source', 'cases'}
    assert re.fullmatch('[0-9a-f]{64}', FROZEN_PROFILE['migration_sha256'])
    assert re.fullmatch('[0-9a-f]{40}', FROZEN_PROFILE['product_source'])
    assert FROZEN_PROFILE['cases'] == CASE_EXPECTATIONS
    return FROZEN_PROFILE


def ack_event_sequence(*, successor=False):
    """Independent pinned literals, retaining their original JSON member order."""
    from pathlib import Path
    from live_replay import NEW_EVENT
    root=Path(__file__).resolve().parents[2]
    raw=(root/'tests/fixtures/acquisition/selector-events.json').read_bytes()
    assert hashlib.sha256(raw).hexdigest()=='64f3d2ad64cf6960aaa9908e8b85b21205ca89786cb2c21dacb3c4e8e4d1938f'
    items=json.loads(raw)['events']
    assert type(successor) is bool
    names=('wrong-first-tag','wrong-author') if successor else ('included','wrong-author')
    events=[NEW_EVENT]+[next(e['event'] for e in items if e['name']==name) for name in names]
    assert [event['id'] for event in events]==[
        '57b20f44c0095dfc789765ed38caa8dd54ae37635dab32237c563146c8865b02',
        ('7815649941b6434d7c525bb633ff461fb7ee341954f1bcb0bce2c9535c2bb7f7' if successor else
         '32be96c6ec271705225aad466eebc4f8352c65960e619164be86eae0d442a791'),
        '3ac1dd47ad27c66939e5ed24e2002e71fcfac11c63254b622b3d54ea8491ce58']
    return copy.deepcopy(events)


def preparation_hole_events():
    """Four frozen signed bodies; malformed frame is a separate wire-only input."""
    events=ack_event_sequence()
    events.append(ack_event_sequence(successor=True)[1])
    assert len({event['id'] for event in events})==4
    return events


def preparation_hole_recipe():
    return dict(format='bigbrotr-ack-preparation-hole-events/1',events=preparation_hole_events())


def phase_events(phase, *, preparation_hole=False, successor=False, post_hole_proof=False):
    assert phase in phase_names(preparation_hole)
    assert type(post_hole_proof) is bool and not (post_hole_proof and (preparation_hole or successor))
    if post_hole_proof:
        a,b,c=ack_event_sequence()
        return [a,b,c] if phase=='mutation' else [a,{'id':a['id']},a]
    if not preparation_hole:
        return ack_event_sequence(successor=successor)
    assert not successor
    a,b,c,d=preparation_hole_events()
    return [a,{'id':a['id']},b] if phase=='mutation' else [c,d,b]


def uint(value, bits=64, minimum=0):
    assert type(value) is int and minimum <= value < 1 << bits, 'unsigned scalar'
    return value


def integer(value):
    assert type(value) is int and -(1 << 63) <= value < 1 << 63, 'signed scalar'
    return value


def bytea(value, length):
    assert type(value) is str and re.fullmatch(r'\\x[0-9a-f]{' + str(2 * length) + '}', value), 'closed bytea'
    return bytes.fromhex(value[2:])


def counter(value, width=8):
    return uint(value, width * 8).to_bytes(width, 'big')


def xid(value):
    assert type(value) is str and re.fullmatch('[1-9][0-9]*', value)
    assert int(value) < 1 << 32
    return value


def identity(row):
    dataset, slot = bytea(row['dataset_id'], 16), bytea(row['producer_slot_id'], 16)
    assert any(dataset) and any(slot)
    return dataset, slot, uint(row['producer_epoch'], minimum=1)


def job_record(row):
    assert set(row) == JOB_FIELDS, 'closed acquisition progress fields'
    identity(row); xid(row['xmin'])
    job, scope = bytea(row['job_id'], 16), bytea(row['scope_id'], 16)
    assert any(job) and any(scope)
    request = bytea(row['request_id'], 48)
    assert re.fullmatch(b'[0-9a-f]{48}', request) and int(request[:32], 16)
    ordinal = uint(row['event_ordinal'], 16, 1)
    assert ordinal <= 3
    revision = uint(row['progress_revision'], minimum=1)
    classified = uint(row['classified_events'], minimum=1)
    successful, rejected = uint(row['successful_events']), uint(row['rejected_events'])
    assert revision == classified == successful + rejected and classified >= ordinal
    kind = uint(row['classification'], 16)
    assert kind in (2, 3, 4, 5, 6)
    mutation = row['mutation_request']
    if kind in (2, 3, 4):
        assert 1 <= uint(mutation) <= 64
    else:
        assert mutation is None
    assert (successful > 0 if kind in (2, 3, 5) else rejected > 0)
    flags = uint(row['clock_anomaly_flags'], 32)
    resolution = uint(row['clock_resolution_us'], minimum=1)
    assert flags in (0, 1, 4, 5) and bool(flags & 4) == (resolution > 1)
    raw = (job + scope + counter(revision) + request + counter(ordinal, 2)
        + counter(uint(row['wire_sequence'], minimum=1))
        + bytea(row['input_sha256'], 32) + bytea(row['intent_digest'], 32)
        + counter(kind, 2) + counter(0 if mutation is None else mutation)
        + counter(classified) + counter(successful) + counter(rejected)
        + integer(row['observed_at_us']).to_bytes(8, 'big', signed=True)
        + counter(resolution) + counter(flags, 4))
    assert len(raw) == JOB_BYTES
    return raw


def dependency_record(row):
    assert set(row) == DEPENDENCY_FIELDS, 'closed dependency fields'
    dataset, _, _ = identity(row); xid(row['xmin'])
    raw = (dataset + counter(uint(row['source_shard'], 32))
        + counter(uint(row['source_epoch'], minimum=1))
        + counter(uint(row['source_sequence'], minimum=1))
        + bytea(row['payload_digest'], 32))
    assert len(raw) == DEPENDENCY_BYTES
    return raw


def head_digest(head, jobs, dependencies, native_check):
    """Stream ordered independent records, retaining no union of fact payloads.

    native_check must compare a reference with the independently reconstructed
    complete native state. A digest over observed rows alone is insufficient.
    This helper returns a digest, never a qualification result.
    """
    assert set(head) == HEAD_FIELDS, 'closed producer checkpoint fields'
    dataset, slot, epoch = identity(head); xid(head['xmin'])
    assert type(head['progress_format']) is int and head['progress_format'] == 1
    revision = uint(head['checkpoint_revision'], minimum=1)
    complete = uint(head['complete_through_request'])
    assert complete <= 64
    job_count, dep_count = uint(head['progress_jobs']), uint(head['dependency_count'])
    assert job_count <= MAX_JOBS and dep_count <= MAX_DEPENDENCIES
    integer(head['committed_at_us'])
    assert any(bytea(head['issuer_resource'], 16)) and any(bytea(head['issuer_incarnation'], 16))
    uint(head['issuer_epoch'], minimum=1)
    digest = hashlib.sha256(DOMAIN + dataset + slot + counter(epoch)
        + bytea(head['manifest_sha256'], 32) + counter(revision) + counter(complete)
        + counter(job_count) + counter(dep_count))
    count, previous = 0, None
    for row in jobs:
        assert identity(row) == (dataset, slot, epoch), 'foreign job producer'
        raw = job_record(row)
        key = raw[:16]
        assert previous is None or previous < key, 'job order or duplicate'
        previous = key
        count += 1
        assert count <= MAX_JOBS
        digest.update(raw)
    assert count == job_count, 'job count'
    count, previous = 0, None
    for row in dependencies:
        assert identity(row) == (dataset, slot, epoch), 'foreign dependency producer'
        raw = dependency_record(row)
        key = raw[16:40]
        assert previous is None or previous < key, 'dependency order or duplicate'
        previous = key
        count += 1
        assert count <= MAX_DEPENDENCIES
        native_check(row)
        digest.update(raw)
    assert count == dep_count, 'dependency count'
    return digest.digest()


def native_reference(row, values):
    """Exact key/frame/source/full-preimage check in an already modeled census."""
    from installed_family_science import decode
    dataset, _, _ = identity(row)
    source = dataset + counter(row['source_shard'], 4) + counter(row['source_epoch']) + counter(row['source_sequence'])
    key = ('facts', (b'\x01\x20' + source).hex())
    value = bytes.fromhex(values[key])
    expected = bytea(row['payload_digest'], 32)
    assert 34 < len(value) <= 2 * 1024 * 1024 + 34
    assert value[:2] == b'\x01\x20' and value[2:34] == expected
    preimage = value[34:]
    assert preimage.startswith(b'bigbrotr/fact/1\0')
    assert hashlib.sha256(preimage).digest() == expected
    fact = decode(preimage.split(b'\0', 1)[1])
    assert fact[0] == [dataset, row['source_shard'], row['source_epoch'], row['source_sequence']]


def certification_digest(event, scope, observer, clock, witness, head, wire):
    """Reconstruct the complete proof-only request from literal event/attribution."""
    received = integer(head['observed_at_us'])
    resolution = uint(head['clock_resolution_us'], minimum=1)
    flags = uint(head['clock_anomaly_flags'], 32)
    assert resolution == wire['clock_resolution_us']
    assert flags in (0, 1, 4, 5) and bool(flags & 4) == (resolution > 1)
    assert wire['sent_at_us'] - resolution <= received <= wire['closed_at_us'] + resolution
    observation = [received, observer, [clock, resolution, None, flags], witness]
    intent = [1, scope, [*body(event), bytes.fromhex(event['sig'])], 'network', None, observation]
    preimage = b'bigbrotr/producer-request/1\0' + cbor([[bytes(16), 0, 0], 'certify-existing', 1, intent])
    assert bytea(head['input_sha256'], 32) == hashlib.sha256(json.dumps(
        event, ensure_ascii=False, separators=(',', ':')).encode()).digest()
    return hashlib.sha256(preimage).digest()


def exact_snapshot(head, jobs, dependencies, expected_head, expected_jobs, expected_dependencies, native):
    """Compare literal/reconstructed semantics first; digest comparison is last."""
    assert head == expected_head, 'expected checkpoint head'
    assert jobs == expected_jobs, 'expected acquisition heads'
    assert dependencies == expected_dependencies, 'expected cumulative dependencies'
    actual = head_digest(head, iter(jobs), iter(dependencies), lambda row: native_reference(row, native))
    assert bytea(head['checkpoint_digest'], 32) == actual, 'checkpoint digest'
    return dict(status='VERIFIED_TYPED_RECORDS', jobs=len(jobs), dependencies=len(dependencies),
        checkpoint_sha256=actual.hex(), qualification='NOT_RUN')


def proof_only_boundary(mutation, proof, expected_revision):
    """No extra mutation reservation/input/outcome or fabricated job completion."""
    first, later = mutation['control'], proof['control']
    assert mutation['inputs'] == proof['inputs']
    for table in ('producer_requests', 'producer_outcomes', 'producer_epochs',
                  'jobs', 'acquisition_jobs', 'job_admissions'):
        assert first[table] == later[table], ('proof-only changed', table)
    before, = first['acquisition_progress']
    after, = later['acquisition_progress']
    job, = [row for row in later['jobs'] if row['request_key'] == '\\x' + 'e4' * 16]
    assert before['job_id'] == after['job_id'] == job['job_id']
    assert job['checkpoint_revision'] == 0
    assert before['request_id'] != after['request_id'] and after['event_ordinal'] == 1
    assert after['classification'] == 5 and after['mutation_request'] is None
    assert after['progress_revision'] == after['classified_events'] == expected_revision
    assert after['successful_events'] == before['successful_events'] + 1
    assert after['rejected_events'] == before['rejected_events'] == 0
    assert first['producer_checkpoint_dependencies'] == later['producer_checkpoint_dependencies']


def phase_spec(origin, hole, phase):
    profile = require_frozen_profile()
    assert origin in ('activated', 'empty', 'later')
    assert type(hole) is bool and phase in ('mutation', 'proof-only')
    # This is a source literal, never a caller-supplied expected-file path.
    return profile['cases'][origin]['hole' if hole else 'split'][phase]


def phase_ready(state, origin, hole, phase, epoch, wire):
    expected = phase_spec(origin, hole, phase)
    assert len(state) == 77 and TABLES <= set(state)
    if expected['outcome'] == 'REFUSAL':
        # A refusal is never reclassified as successful capture by polling.
        return False
    heads = [h for h in state['producer_checkpoints'] if h['producer_epoch'] == epoch]
    jobs = [h for h in state['acquisition_progress'] if h['producer_epoch'] == epoch]
    if len(heads) != 1 or len(jobs) != expected['jobs']:
        return False
    head, = heads
    if any(head[key] != expected[key] for key in
           ('checkpoint_revision', 'complete_through_request', 'dependency_count')):
        return False
    if jobs:
        job, = jobs
        wanted = expected['job']
        if any(job[key] != value for key, value in wanted.items()):
            return False
        if bytea(job['request_id'], 48) != wire['request'][1].encode():
            return False
    return True


def require_phase(state, origin, hole, phase, epoch, wire):
    assert phase_ready(state, origin, hole, phase, epoch, wire), 'expected durable phase'
    for row in state['acquisition_progress']:
        job_record(row)
    for row in state['producer_checkpoint_dependencies']:
        dependency_record(row)


def legacy_state(state, *, ack=False):
    """Exact schema extension projection; qualification checks every removed row."""
    profile = require_frozen_profile(ack=ack)
    assert len(state) == 77 and TABLES <= set(state)
    result = copy.deepcopy(state)
    if ack:
        receipt, = [r for r in result['schema_versions'] if r['migration']==40]
        integer(receipt['applied_at_us']);xid(receipt['xmin'])
        assert receipt==dict(migration=40,name='0040_producer_ack.sql',checksum='\\x'+profile['migration_sha256'],
            engine='postgres-control',applied_by='bigbrotr_migrator',reader_min=1,reader_max=1,writer_floor=1,
            applied_at_us=receipt['applied_at_us'],xmin=receipt['xmin'])
        assert all(type(receipt[k]) is int for k in ('migration','reader_min','reader_max','writer_floor'))
        assert max(r['applied_at_us'] for r in result['schema_versions'] if r['migration']<40)<=receipt['applied_at_us']
        result['schema_versions'].remove(receipt)
    for table in TABLES:
        result.pop(table)
    ledger, = [r for r in result['schema_versions'] if r['migration'] == 39]
    integer(ledger['applied_at_us'])
    assert max(r['applied_at_us'] for r in result['schema_versions']
        if r['migration'] < 39) <= ledger['applied_at_us']
    xid(ledger['xmin'])
    assert ledger == dict(migration=39, name='0039_producer_progress.sql',
        checksum='\\x' + (FROZEN_PROFILE if ack else profile)['migration_sha256'], engine='postgres-control',
        applied_by='bigbrotr_migrator', reader_min=1, reader_max=1, writer_floor=1,
        applied_at_us=ledger['applied_at_us'], xmin=ledger['xmin'])
    assert all(type(ledger[k]) is int for k in
        ('migration', 'reader_min', 'reader_max', 'writer_floor'))
    result['schema_versions'].remove(ledger)
    retained = []
    for head in result['producer_checkpoints']:
        assert set(head) == HEAD_FIELDS
        if head['progress_format'] is None:
            assert all(head[k] is None for k in HEAD_ADDITIONS)
            retained.append({k: v for k, v in head.items() if k not in HEAD_ADDITIONS})
        else:
            assert head['progress_format'] == 1
    result['producer_checkpoints'] = retained
    for row in result['producer_requests']:
        assert REQUEST_ADDITIONS <= set(row)
        for field in REQUEST_ADDITIONS:
            row.pop(field)
    assert len(result) == 75
    return result


def legacy_context(context, *, ack=False):
    require_frozen_profile(ack=ack)
    result = copy.deepcopy(context)
    mode = result.pop('progress_profile')
    assert mode == ('ack' if ack else 'split'), 'hole/refusal has a separate unqualified custody outcome'
    if ack:
        assert json.loads(result.pop('ack_event_recipe'))==dict(format='bigbrotr-ack-events/1',events=ack_event_sequence(successor=True))
        result.pop('progress_gates')
    for field in ('before', 'after'):
        result[field] = legacy_state(result[field],ack=ack)
    result['snapshots'] = [s for s in result['snapshots'] if not s['stage'].startswith('progress-')]
    for snapshot in result['snapshots']:
        snapshot['control'] = legacy_state(snapshot['control'],ack=ack)
    result['fixture']['renewal']['current'] = copy.deepcopy(next(
        s for s in result['snapshots'] if s['stage'] == 'renewed'))
    return result


def verify_records(context, model, science, pairs):
    """Reuse literal typed expectations at actual observed cuts, including loss.

    The caller must independently reconstruct full science and complete legacy
    SQL first. This helper does not establish lifecycle/custody or COMMIT outcome.
    """
    from installed_family_replay import clock, fresh_xmin
    from history_replay import only
    from live_replay import OLD_EVENT, NEW_EVENT
    from installed_family_science import decode
    from transition_replay import native_map
    profile = require_frozen_profile()
    origin, epoch = context['case']['origin'], science.epoch
    phases = {row['progress_phase']: row for row in context['journey']['closed']
        if 'progress_phase' in row}
    assert set(phases) == {'mutation', 'proof-only'}
    assert [phase for phase, _ in pairs] in (['mutation'], ['mutation', 'proof-only'])
    mutation = pairs[0][1]
    snapshots = context['snapshots']
    manifest = hashlib.sha256(science.manifest).digest()
    common = dict(dataset_id='\\x' + science.ds.hex(), producer_slot_id='\\x' + science.slot.hex(),
        producer_epoch=epoch)
    old_requests = {(r['producer_slot_id'], r['producer_epoch'], r['request_sequence'])
        for r in context['before']['producer_requests']}
    for state in [context['before'], *(s['control'] for s in snapshots), context['after']]:
        assert set(state) == set(context['before']) and len(state) == 77
        for request in state['producer_requests']:
            assert REQUEST_ADDITIONS <= set(request)
            key = (request['producer_slot_id'], request['producer_epoch'], request['request_sequence'])
            if key in old_requests or request['request_sequence'] == 1:
                assert all(request[f] is None for f in REQUEST_ADDITIONS)
            else:
                n = request['request_sequence']
                assert request['producer_epoch'] == epoch and n in (2, 3)
                event = NEW_EVENT if n == 2 else OLD_EVENT
                ordinal = n - 1
                assert request['progress_request_id'] == '\\x' + phases['mutation']['request'][1].encode().hex()
                assert request['progress_event_ordinal'] == request['progress_wire_sequence'] == ordinal
                assert bytea(request['progress_input_sha256'], 32) == hashlib.sha256(
                    json.dumps(event, ensure_ascii=False, separators=(',', ':')).encode()).digest()
    # There is no classified prefix before the job's first actual EVENT.
    for state in (context['before'], snapshots[0]['control'], snapshots[1]['control']):
        assert all(state[t] == [] for t in (*TABLES, 'producer_checkpoints'))
    original_ledger = context['before']['schema_versions']
    for state in [*(s['control'] for s in snapshots), context['after']]:
        assert state['schema_versions'] == original_ledger
    modeled_native = native_map(model.native)
    results = []
    for phase, snapshot in pairs:
        state, wire = snapshot['control'], phases[phase]
        spec = phase_spec(origin, False, phase)
        head, = state['producer_checkpoints']
        actual_job, = state['acquisition_progress']
        accepted = only(state['jobs'], request_key='\\x' + 'e4' * 16)
        job_scope = only(state['acquisition_jobs'], job_id=accepted['job_id'])['scope_id']
        event = OLD_EVENT if phase == 'mutation' else NEW_EVENT
        ordinal = 2 if phase == 'mutation' else 1
        request_number = spec['job']['mutation_request']
        expected_job = dict(common, job_id=accepted['job_id'], scope_id=job_scope,
            **spec['job'], request_id='\\x' + wire['request'][1].encode().hex(),
            wire_sequence=ordinal, input_sha256='\\x' + hashlib.sha256(json.dumps(event,
                ensure_ascii=False, separators=(',', ':')).encode()).hexdigest(),
            observed_at_us=actual_job['observed_at_us'],
            clock_resolution_us=actual_job['clock_resolution_us'],
            clock_anomaly_flags=actual_job['clock_anomaly_flags'], xmin=actual_job['xmin'])
        clock(expected_job['observed_at_us'], context['server_window'])
        xid(expected_job['xmin'])
        if request_number is None:
            # A proof-only head is its own transaction, not the preceding
            # reservation/outcome transaction relabeled as observed progress.
            fresh_xmin(head['xmin'], legacy_state(state))
            witness = [science.ref(1), science.ref(3), science.ref(5)] if origin != 'activated' else None
            intent = certification_digest(event, science.scope, science.ref(2),
                science.ref(2), witness, expected_job, wire)
        else:
            raw = science.inputs[request_number]
            # Complete original bytes were already independently reconstructed.
            intent = hashlib.sha256(raw).digest()
            observation = decode(raw.split(b'\0', 1)[1])[3][5]
            assert observation[0] == expected_job['observed_at_us']
            assert observation[2][1] == expected_job['clock_resolution_us']
            assert observation[2][3] == expected_job['clock_anomaly_flags']
            outcome = only(state['producer_outcomes'], producer_epoch=epoch,
                request_sequence=request_number)
            assert expected_job['xmin'] == outcome['xmin']
        expected_job['intent_digest'] = '\\x' + intent.hex()
        assert actual_job == expected_job
        assert head['xmin'] == expected_job['xmin']
        fresh_xmin(head['xmin'], context['before'])
        clock(head['committed_at_us'], context['server_window'])
        assert wire['sent_at_us'] <= head['committed_at_us'] <= snapshot['observed_by_us']
        control = model.control
        expected_head = dict(common, progress_format=1, manifest_sha256='\\x' + manifest.hex(),
            checkpoint_revision=spec['checkpoint_revision'],
            complete_through_request=spec['complete_through_request'], progress_jobs=1,
            dependency_count=spec['dependency_count'], issuer_resource=control['resource_id'],
            issuer_epoch=control['epoch'], issuer_incarnation=control['process_incarnation'],
            committed_at_us=head['committed_at_us'], xmin=head['xmin'])
        # Coordinates are a frozen fixture literal, never selected from SQL.
        coordinates = profile['cases'][origin]['dependency_coordinates']
        dependencies = []
        for source_epoch, sequence in coordinates:
            reference = science.refs[source_epoch, sequence]
            observed = only(state['producer_checkpoint_dependencies'], source_shard=0,
                source_epoch=source_epoch, source_sequence=sequence)
            first_transaction = None
            for n in sorted(science.receipts):
                receipt = decode(science.receipts[n].split(b'\0', 1)[1])
                if reference in receipt[4] + receipt[5]:
                    first_transaction = only(state['producer_outcomes'],
                        producer_epoch=epoch, request_sequence=n)['xmin']
                    break
            if first_transaction is None:
                # The predecessor ExistingProof in mutation phase first adds
                # these historical references, without any mutation request.
                assert origin == 'activated'
                first_transaction = only(mutation['control']['acquisition_progress'])['xmin']
            dependencies.append(dict(common, source_shard=0, source_epoch=source_epoch,
                source_sequence=sequence, payload_digest='\\x' + reference[1].hex(),
                xmin=first_transaction))
            assert observed == dependencies[-1]
        assert len(dependencies) == spec['dependency_count']
        assert len(state['producer_checkpoint_dependencies']) == len(dependencies)
        expected_head['checkpoint_digest'] = '\\x' + head_digest(
            dict(expected_head, checkpoint_digest='\\x' + '00' * 32),
            [expected_job], iter(dependencies),
            lambda row: native_reference(row, modeled_native)).hex()
        actual_dependencies = sorted(state['producer_checkpoint_dependencies'],
            key=lambda r: (r['source_shard'], r['source_epoch'], r['source_sequence']))
        results.append(exact_snapshot(head, [actual_job], actual_dependencies,
            expected_head, [expected_job], dependencies, modeled_native))
    return results


def verify(context, *, ack=False):
    """Full base science plus explicit typed heads; never a structural-only PASS."""
    profile = require_frozen_profile(ack=ack)
    if ack:
        return verify_ack(context)
    from installed_family_replay import verify as base_verify, clock, fresh_xmin
    from history_replay import only
    from live_replay import OLD_EVENT, NEW_EVENT
    from installed_family_science import decode
    from transition_replay import native_map
    assert context['progress_profile'] == 'split', 'refusal is not positive qualification'
    base, model, science = base_verify(context, progress=True)
    origin, epoch = context['case']['origin'], science.epoch
    phases = {row['progress_phase']: row for row in context['journey']['closed']
        if 'progress_phase' in row}
    assert set(phases) == {'mutation', 'proof-only'}
    snapshots = context['snapshots']
    assert [s['stage'] for s in snapshots] == ['activated', 'prepared',
        'progress-mutation', 'progress-proof-only', 'submitted', 'renewed', 'before-stop']
    mutation, proof = snapshots[2:4]
    # Each extra cut receives the same complete75-table scientific model as the
    # ordinary submitted cut. Removing new relations for the legacy model must
    # not conceal unrelated SQL changes in either intermediate observation.
    for snapshot in (mutation, proof):
        phase_context = copy.deepcopy(context)
        phase_context['snapshots'][4] = dict(copy.deepcopy(snapshot), stage='submitted')
        base_verify(phase_context, progress=True)
    require_phase(mutation['control'], origin, False, 'mutation', epoch, phases['mutation'])
    require_phase(proof['control'], origin, False, 'proof-only', epoch, phases['proof-only'])
    proof_only_boundary(mutation, proof, phase_spec(origin, False, 'proof-only')['job']['progress_revision'])
    proof_head, = proof['control']['producer_checkpoints']
    mutation_head, = mutation['control']['producer_checkpoints']
    fresh_xmin(proof_head['xmin'], mutation['control'])
    assert 0 < (int(proof_head['xmin']) - int(mutation_head['xmin'])) % 2**32 < 2**31
    results = verify_records(context, model, science, [('mutation', mutation), ('proof-only', proof)])
    for snapshot in snapshots[4:] + [dict(control=context['after'])]:
        for table in (*TABLES, 'producer_checkpoints'):
            assert snapshot['control'][table] == proof['control'][table], ('late progress rewrite', table)
    assert mutation['observed_by_us'] <= phases['proof-only']['sent_at_us']
    assert mutation['inputs'] == proof['inputs'] == context['inputs_after']
    assert only(context['after']['jobs'], request_key='\\x' + 'e4' * 16)['checkpoint_revision'] == 0
    return dict(status='PASS_INSTALLED_PRODUCER_PROGRESS', base=base, phases=results,
        sql_tables=77, native_ack=0, native_science='complete independent native/input reconstruction',
        scope='One real mutation quantum and a distinct proof-only REQ; no EOSE/job completion or ACK/GC')


def controls(context, *, ack=False):
    """Baseline first, including attacks with a recomputed valid head checksum."""
    from transition_replay import native_map
    from history_replay import only
    verify(context,ack=ack)
    checks = []
    def reject(name, mutate):
        other = copy.deepcopy(context)
        mutate(other)
        if ack and name in {'same-request','proof-reserves-request','proof-advances-complete','proof-intent',
                'self-consistent-forged-intent','self-consistent-incomplete-closure','proof-transaction-reuse',
                'proof-observation','missing-dependency','intermediate-unrelated-table'}:
            from installed_progress_context_oracle import rebind_gates
            rebind_gates(other)
        try:
            verify(other,ack=ack)
        except (AssertionError, KeyError, ValueError, TypeError, IndexError, OverflowError):
            checks.append(name)
            return
        raise AssertionError('negative control accepted: ' + name)
    def rehash(state, native):
        head, = state['producer_checkpoints']
        jobs = sorted(state['acquisition_progress'], key=lambda r: bytea(r['job_id'], 16))
        dependencies = sorted(state['producer_checkpoint_dependencies'],
            key=lambda r: (r['source_shard'], r['source_epoch'], r['source_sequence']))
        head['checkpoint_digest'] = '\\x' + head_digest(head, jobs, dependencies,
            lambda row: native_reference(row, native)).hex()
    def forged_intent(c):
        native = native_map(c['native_after'])
        for state in [s['control'] for s in c['snapshots'][3:]] + [c['after']]:
            state['acquisition_progress'][0]['intent_digest'] = '\\x' + '00' * 32
            rehash(state, native)
    def forged_closure(c):
        native = native_map(c['native_after'])
        for state in [s['control'] for s in c['snapshots'][2:]] + [c['after']]:
            deps = state['producer_checkpoint_dependencies']
            # Missing dependency with internally consistent counts/checksum.
            deps.pop()
            state['producer_checkpoints'][0]['dependency_count'] -= 1
            rehash(state, native)
    def reused_transaction(c):
        tx = c['snapshots'][2]['control']['producer_checkpoints'][0]['xmin']
        for state in [s['control'] for s in c['snapshots'][3:]] + [c['after']]:
            state['acquisition_progress'][0]['xmin'] = tx
            state['producer_checkpoints'][0]['xmin'] = tx
    reject('unknown-process', lambda c: c['response'].update(outcome='OUTCOME_UNKNOWN'))
    reject('native-changed', lambda c: c['native_after'].update(sequence=c['native_after']['sequence'] + 1))
    reject('missing-old-input', lambda c: c['inputs_after'].pop(next(iter(c['inputs_before']))))
    reject('missing-phase', lambda c: c['snapshots'].pop(2))
    reject('same-request', lambda c: c['snapshots'][3]['control']['acquisition_progress'][0].update(
        request_id=c['snapshots'][2]['control']['acquisition_progress'][0]['request_id']))
    reject('proof-reserves-request', lambda c: c['snapshots'][3]['control']['producer_requests'].append(
        copy.deepcopy(c['snapshots'][3]['control']['producer_requests'][-1])))
    reject('proof-advances-complete', lambda c: c['snapshots'][3]['control']['producer_checkpoints'][0].update(
        complete_through_request=64))
    reject('proof-intent', lambda c: c['snapshots'][3]['control']['acquisition_progress'][0].update(
        intent_digest='\\x' + '00' * 32))
    reject('self-consistent-forged-intent', forged_intent)
    reject('self-consistent-incomplete-closure', forged_closure)
    reject('proof-transaction-reuse', reused_transaction)
    reject('proof-observation', lambda c: c['snapshots'][3]['control']['acquisition_progress'][0].update(
        observed_at_us=c['server_window']['before_us'] - 1))
    reject('unknown-head-field', lambda c: c['after']['producer_checkpoints'][0].update(extra=1))
    reject('missing-dependency', lambda c: c['snapshots'][2]['control']['producer_checkpoint_dependencies'].pop())
    reject('request-frame-mismatch', lambda c: c['after']['producer_requests'][-1].update(
        progress_event_ordinal=1 if ack else 3))
    reject('job-completion', lambda c: only(c['after']['jobs'],
        request_key='\\x' + 'e4' * 16).update(checkpoint_revision=1))
    reject('missing-native-fact', lambda c: c['native_after']['rows'].pop())
    reject('present-backend', lambda c: c['backend']['exact'][0].update(present=True))
    reject('catalog-changed', lambda c: c.update(catalog_after=c['catalog_after'] + '\\n'))
    reject('new-table', lambda c: c['after'].update(unexpected=[]))
    def historical_job_change(c):
        original = c['before']['jobs'][0]
        only(c['snapshots'][2]['control']['jobs'], job_id=original['job_id']).update(
            checkpoint_revision=original['checkpoint_revision'] + 1)
    reject('intermediate-unrelated-table', historical_job_change)
    if ack:
        checks.extend(ack_counterexamples(context,lambda value:verify(value,ack=True),'successor'))
    return dict(status='PASS', controls=len(checks), rejected=checks,
        scope='offline retained-observation mutations; no engine fault injection')


def verify_ack(context):
    """One real window2 epoch3 case; cumulative typed closure plus full old science."""
    require_frozen_profile(ack=True)
    from installed_family_replay import verify as base_verify
    from installed_progress_context_oracle import typed_checked, wires_checked
    assert context['progress_profile']=='ack' and context['case']['origin']=='activated'
    assert [s['stage'] for s in context['snapshots']]==['activated','prepared','progress-mutation',
        'progress-proof-only','submitted','renewed','before-stop']
    base,model,science=base_verify(context,progress=True,ack=True)
    for cut in context['snapshots'][2:4]:
        intermediate=copy.deepcopy(context)
        intermediate['snapshots'][4]=dict(copy.deepcopy(cut),stage='submitted')
        base_verify(intermediate,progress=True,ack=True)
    phases=wires_checked(context['journey']['opened'],context['journey']['closed'],context['snapshots'],
        ack_event_sequence(successor=True)[-1],context['progress_gates'],ack=True,successor=True)
    result=typed_checked('successor',context['before'],context['after'],context['snapshots'],phases,
        ack_event_sequence(successor=True)[-1],science.values,science.inputs,science.receipts,
        science.scope,science.ref(2),None,model.control,ack=True)
    return dict(status='PASS_INSTALLED_PRODUCER_ACK',base=base,typed=result,sql_tables=77,native_ack=4,
        native_science='complete independent native/input reconstruction',
        scope='Three mutations through actual window2 then a distinct three-proof REQ; retained originals, noGC')


def ack_counterexamples(context, checker, kind, *, preparation_hole=False, post_hole_proof=False):
    """Exact native state/write accounting and three-frame/window discriminators."""
    spec=POST_HOLE_PROOF_EXPECTATIONS[kind] if post_hole_proof else PREPARATION_HOLE_EXPECTATIONS[kind] if preparation_hole else ACK_EXPECTATIONS[kind]
    checks=[]
    def reject(name,change,reason=None):
        value=copy.deepcopy(context);change(value)
        if name=='coherent-window32-adoption':
            from installed_progress_context_oracle import rebind_gates
            rebind_gates(value)
        if kind=='initial':
            value['native_readback']['stdout']=json.dumps(value['native'])
        try:
            checker(value)
        except (AssertionError,KeyError,ValueError,TypeError,IndexError,StopIteration) as error:
            if reason is not None:
                assert isinstance(error,AssertionError) and str(error)==reason, (name,'wrong rejection',str(error))
            checks.append(name)
        else:
            raise AssertionError('ACK counterexample accepted: '+name)
    def native(c): return c['native'] if kind=='initial' else c['native_after']
    def state(c,field,value):
        slot=next(r['producer_slot_id'] for r in c['after']['producer_epochs'] if r['producer_epoch']==spec['epoch'])
        key='0131'+slot[2:]+spec['epoch'].to_bytes(8,'big').hex()
        row,=[r for r in native(c)['rows'] if r['cf']=='receipts' and r['key']==key]
        raw=bytearray.fromhex(row['value'])
        assert len(raw)==26 and raw[:2]==b'\x01\x31'
        offset={'issued':2,'acknowledged':10,'retired':18}[field]
        raw[offset:offset+8]=value.to_bytes(8,'big');row['value']=raw.hex()
    reject('ack-missing',lambda c:state(c,'acknowledged',0),'complete ACK native bytes differ')
    reject('ack-beyond-issue',lambda c:state(c,'acknowledged',spec['request']+1),'complete ACK native bytes differ')
    reject('ack-reclaimed-detail',lambda c:state(c,'retired',1),'complete ACK native bytes differ')
    reject('ack-extra-issued-request',lambda c:state(c,'issued',spec['request']+1),'complete ACK native bytes differ')
    reject('proof-native-extra-write',lambda c:native(c).update(sequence=native(c)['sequence']+1),'ACK native write count differs')
    reject('missing-synced-ack-writes',lambda c:native(c).update(sequence=native(c)['sequence']-spec['request']),'ACK native write count differs')
    reject('wrong-three-event-order',lambda c:c.update(ack_event_recipe=json.dumps(dict(
        format='bigbrotr-ack-post-hole-proof-events/1' if post_hole_proof else 'bigbrotr-ack-events/1',
        events=list(reversed(ack_event_sequence(successor=kind=='successor')))))))
    def full_window(c):
        for snapshot in c['snapshots']:
            for row in snapshot['control']['producer_epochs']:
                if row['producer_epoch']==spec['epoch']: row['outstanding_request_limit']=32
        for row in c['after']['producer_epochs']:
            if row['producer_epoch']==spec['epoch']: row['outstanding_request_limit']=32
    reject('coherent-window32-adoption',full_window)
    reject('third-event-unclassified',lambda c:c['after']['acquisition_progress'][0].update(
        classified_events=5,successful_events=5,progress_revision=5))
    reject('proof-only-new-request',lambda c:c['after']['producer_requests'].append(
        dict(c['after']['producer_requests'][-1],request_sequence=spec['request']+1)))
    return checks
