#!/usr/bin/env python3
"""Light fixture checks only; these cannot qualify the installed product journey."""
import ast
import copy
import errno
import hashlib
import importlib.util
import json
from pathlib import Path
import sys
import socket
import tempfile
import threading
import time

sys.dont_write_bytecode = True
SOURCE = Path(__file__).resolve().parents[2]
sys.path[:0] = [str(SOURCE/'experiments/control-migrations'), str(Path(__file__).parent)]
spec = importlib.util.spec_from_file_location('bootstrap_oracle', Path(__file__).with_name('oracle.py'))
bootstrap = importlib.util.module_from_spec(spec)
sys.modules['bootstrap_oracle'] = bootstrap
spec.loader.exec_module(bootstrap)
from installed_sync_oracle import decode, expected
from installed_sync_client import REFUSED_VISIBILITIES, no_connections, read_frame, refusal_frames, relay_listener
from installed_sync_expiry import DURATION_US, KEY, preserve_expired, validate_expired_acceptance
from installed_sync import validate_queued
from installed_sync_window import window_frames

files = [*Path(__file__).parent.glob('installed_sync*.py'), Path(__file__).with_name('installed_client.py'),
    Path(__file__).with_name('session_client.py'), SOURCE/'experiments/acquisition-scope/bindings.py']
embedded = 0
for path in files:
    tree = ast.parse(path.read_text())
    if path.name == 'installed_sync.py':
        for node in ast.walk(tree):
            if isinstance(node, ast.Assign) and isinstance(node.value, ast.Constant) and isinstance(node.value.value, str):
                if any(isinstance(target, ast.Name) and target.id in ('script', 'wrapper') for target in node.targets):
                    ast.parse(node.value.value); embedded += 1
assert embedded == 2

state = dict(datasets=[dict(dataset_id='\\x'+'01'*16)], owner_epochs=[dict(role='ingest',
    resource_id='\\x'+'02'*16, process_incarnation='\\x'+'03'*16, acquired_at_us=100)],
    allocation_domains=[dict(dictionary_kind=n, allocation_domain='\\x'+f'{n+16:02x}'*16) for n in range(1, 10)])
public = dict(dataset_id='01'*16, dictionary_kind=4, allocation_domain='14'*16, ordinal='1')
report = dict(owner_resource_id='02'*16, producer_slot_id='04'*16, recovery_generation='05'*16,
    topology_generation='0', dictionary_count=3, public_scope=public, private_scope=dict(public, ordinal='3'))
# The focused refusal cases select either an absent identity or the original
# private descriptor whose fact has a public envelope. Never relabel private2.
bootstrap_rows, _, bootstrap_facts = bootstrap.expected(bytes.fromhex('01'*16), True,
    domain=bytes.fromhex('14'*16))
fact = decode(bootstrap_facts[1].split(b'\0', 1)[1])
assert fact[3] == [bytes.fromhex('01'*16), 4, bytes.fromhex('14'*16), 1]
assert fact[5] == [[bytes.fromhex('01'*16), 4, bytes.fromhex('14'*16), 2],
    1, 'visibility', 1, {'class': 'private'}]
assert {int.from_bytes(key[-8:], 'big') for cf, key, _ in bootstrap_rows
    if cf == 'default' and key.startswith(b'\x01\x04')} == {1, 2}
assert REFUSED_VISIBILITIES == {'sync-public-visibility': 1, 'sync-absent-visibility': 99,
    'sync-bootstrap-private-visibility': 2}
# Literal acceptance corpus exercises the500ms expiry oracle and the ordinal2
# projection into the existing full120s accepted-job oracle. This is not SQL.
ident = lambda value: '\\x'+value*16
dataset, actor, scope, session, job_id, audit_id = map(ident, ('01', '10', '20', '30', '40', '50'))
def job_digest(key, duration):
    return '\\x'+hashlib.sha256(b'bigbrotr/acquisition-job/1\0'+bytes.fromhex(dataset[2:])
        +bytes.fromhex(actor[2:])+bytes.fromhex(key)+bytes.fromhex(scope[2:])
        +duration.to_bytes(8, 'big')+(1).to_bytes(8, 'big')*2).hexdigest()
receipt = dict(format='bigbrotr-job-acceptance/1', job_id=job_id[2:], command_key=KEY,
    accepted_at_us='1000', created=True, execution='unobserved', authority='acceptance-only')
job = dict(job_id=job_id, dataset_id=dataset, principal_id=actor, request_key='\\x'+KEY,
    request_digest=job_digest(KEY, DURATION_US), kind='acquisition', config_revision=1, policy_revision=1,
    created_at_us=1000, state='accepted', job_revision=1, checkpoint_revision=0, reason_code=None,
    result_manifest_digest=None, owner_resource_id=None, owner_epoch=None, owner_role='control',
    owner_incarnation=None, completed_at_us=None, input_topology_generation=None, input_complete=True,
    export_job_protocol=None, acquisition_job_protocol=1, xmin='42')
detail = dict(job_id=job_id, dataset_id=dataset, actor_id=actor, request_key='\\x'+KEY,
    scope_id=scope, duration_us=DURATION_US, deadline_at_us=1000+DURATION_US, session_id=session,
    actor_credential_revision=1, actor_authentication_revision=1, actor_grant_revision=1, xmin='42')
command = dict(dataset_id=dataset, actor_id=actor, request_key='\\x'+KEY,
    request_digest=job['request_digest'], action='job.acquisition.accept', accepted_at_us=1000,
    expected_config_revision=1, policy_revision=1, audit_id=audit_id, authority_mode=None,
    document_digest=None, xmin='42')
audit = dict(dataset_id=dataset, actor_id=actor, audit_id=audit_id, action='job.acquisition.accept',
    outcome='accepted', subject_id='\\x'+KEY, created_at_us=1000, config_revision=1, policy_revision=1,
    reason=None, requester_database_role=None, requester_os_uid=None, xmin='42')
accepted = dict(datasets=[dict(dataset_id=dataset)], jobs=[job], acquisition_jobs=[detail],
    job_admissions=[dict(job_id=job_id, admission_ordinal=1, xmin='42')],
    management_commands=[command], management_audit=[audit], producer_requests=[],
    acquisition_scopes=[dict(scope_id=scope)],
    local_accounts=[dict(principal_id=actor, credential_revision=1, authentication_revision=1, grant_revision=1)],
    browser_sessions=[dict(session_id=session, dataset_id=dataset, principal_id=actor,
        issued_at_us=900, revoked_at_us=None, expires_at_us=200000000, idle_expires_at_us=200000000)])
assert validate_expired_acceptance(accepted, receipt) == 501000
advanced = copy.deepcopy(accepted)
later, later_key, later_audit = ident('41'), 'b1'*16, ident('51')
owner = dict(role='control', resource_id=ident('60'), epoch=1, process_incarnation=ident('61'))
advanced['owner_epochs'] = [owner]
advanced['jobs'].append(dict(job, job_id=later, request_key='\\x'+later_key,
    request_digest=job_digest(later_key, 120000000), state='queued', job_revision=2,
    owner_resource_id=owner['resource_id'], owner_epoch=1, owner_incarnation=owner['process_incarnation']))
advanced['acquisition_jobs'].append(dict(detail, job_id=later, request_key='\\x'+later_key,
    duration_us=120000000, deadline_at_us=120001000))
advanced['job_admissions'].append(dict(job_id=later, admission_ordinal=2, xmin='43'))
advanced['management_commands'].append(dict(command, request_key='\\x'+later_key,
    request_digest=job_digest(later_key, 120000000), audit_id=later_audit))
advanced['management_audit'].append(dict(audit, audit_id=later_audit, subject_id='\\x'+later_key))
observations = [dict(path='jobs', key=KEY, body=receipt), dict(path='jobs', key=later_key,
    body=dict(receipt, job_id=later[2:], command_key=later_key))]
validate_queued(advanced, observations, ordinal=2)
preserve_expired(accepted, advanced)
for fault in ('claimed', 'xmin', 'attribution', 'ordinal'):
    corrupted = copy.deepcopy(advanced if fault != 'claimed' else accepted)
    try:
        if fault == 'claimed':
            corrupted['jobs'][0]['state'] = 'queued'
            validate_expired_acceptance(corrupted, receipt)
        elif fault == 'ordinal':
            corrupted['job_admissions'][1]['admission_ordinal'] = 1
            validate_queued(corrupted, observations, ordinal=2)
        else:
            if fault == 'xmin': corrupted['jobs'][0]['xmin'] = '43'
            else: corrupted['producer_requests'] = [dict(acquisition_job_id=job_id)]
            preserve_expired(accepted, corrupted)
    except AssertionError:
        pass
    else:
        raise AssertionError('expiry oracle accepted corruption: '+fault)
event = json.loads((SOURCE/'tests/fixtures/ingest/import-event.jsonl').read_text())
selector_artifact = (SOURCE/'tests/fixtures/acquisition/selector-events.json').read_bytes()
original_event = json.dumps(event, sort_keys=True)
refusals = refusal_frames(event, selector_artifact)
assert refusals[0] == {'id': event['id']} and refusals[1]['kind'] == 22222
assert refusals[2] is event and json.dumps(event, sort_keys=True) == original_event
assert refusals[1] == next(entry['event'] for entry in json.loads(selector_artifact)['events']
    if entry['name'] == 'ephemeral')
try:
    refusal_frames(event, selector_artifact+b'\n')
except AssertionError:
    pass
else:
    raise AssertionError('modified frozen selector artifact accepted')
for distinct in (True, False):
    values, labels = window_frames(event, selector_artifact, distinct=distinct)
    assert len(values) == len(labels) == 3
    assert values[0] is event and values[2] is event
    assert values[1]['kind'] == 1
    assert (values[1]['id'] != event['id']) == distinct
    assert labels == (['signed', 'distinct-signed', 'duplicate'] if distinct else ['signed', 'duplicate', 'duplicate'])
assert json.dumps(event, sort_keys=True) == original_event
counts = []
for received in [None, 300]:
    rows, inputs, receipts = expected(state, report, event, 200, received, 400 if received else None)
    assert len(rows) == (49 if received else 35)
    assert len(inputs) == len(receipts) == (4 if received else 3)
    for raw in inputs+receipts:
        content = raw.split(b'\0', 1)[1]
        assert bootstrap.cbor(decode(content)) == content
    counts.append(dict(native_rows=len(rows), inputs=len(inputs), receipts=len(receipts)))

malformed = [b'\x18\x01', b'\xa2\x01\x02\x01\x03', b'\x9f\xff', b'\x41', b'\x80\x00']
for bad in malformed:
    try:
        decode(bad)
    except (AssertionError, ValueError):
        pass
    else:
        raise AssertionError('malformed/noncanonical CBOR accepted')


class Stream:
    def __init__(self, value):
        self.value = value

    def recv(self, count):
        part, self.value = self.value[:count], self.value[count:]
        return part


assert read_frame(Stream(bytes.fromhex('81840102030453475233'))) == (1, b'REQ7')
assert read_frame(Stream(bytes.fromhex('888001020304'))) == (8, b'')
for bad in [bytes.fromhex('8100'), bytes.fromhex('818001'), bytes.fromhex('81807f')]:
    try:
        read_frame(Stream(bad))
    except (AssertionError, EOFError):
        pass
    else:
        raise AssertionError('unmasked/truncated WebSocket frame accepted')
# Complete a real server-initiated TCP close before immediately reusing the
# exact endpoint. Merely closing an unused listener cannot expose TIME_WAIT.
address = ('127.0.0.1', 0)
for _ in range(3):
    with relay_listener(address) as listener:
        address = listener.getsockname()
        with socket.create_connection(address, timeout=1) as peer:
            with listener.accept()[0] as accepted:
                accepted.settimeout(1)
                accepted.sendall(b'x')
                assert peer.recv(1) == b'x'
                accepted.shutdown(socket.SHUT_WR)
                assert peer.recv(1) == b''
                peer.shutdown(socket.SHUT_WR)
                assert accepted.recv(1) == b''

# Reuse must never hide a live listener, whether or not that listener also
# opted into address reuse. Preserve it and prove it still accepts afterward.
for incumbent_reuse in (False, True):
    with socket.socket() as incumbent:
        if incumbent_reuse:
            incumbent.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        incumbent.bind(('127.0.0.1', 0)); incumbent.listen(1); incumbent.settimeout(1)
        address = incumbent.getsockname()
        try:
            unexpected = relay_listener(address)
        except OSError as error:
            assert error.errno == errno.EADDRINUSE, error
        else:
            unexpected.close()
            raise AssertionError('fixture hid a live listener conflict')
        with socket.create_connection(address, timeout=1) as peer:
            with incumbent.accept()[0] as accepted:
                accepted.sendall(b'y')
                assert peer.recv(1) == b'y'

# Exercise the refusal observer itself on local ephemeral sockets. Injecting
# one TCP connection must fail even without a WebSocket handshake or EVENT.
for connect in (False, True):
    with tempfile.TemporaryDirectory(prefix='bigbrotr-sync-observer-') as temp:
        directory = Path(temp)
        listener = socket.socket()
        listener.bind(('127.0.0.1', 0)); listener.listen(1); listener.settimeout(.01)
        address = listener.getsockname()
        finished, errors = threading.Event(), []
        worker = threading.Thread(target=no_connections, args=(directory, listener, finished, errors))
        worker.start()
        deadline = time.monotonic()+2
        while not (directory/'relay-monitor-started.json').exists():
            assert time.monotonic() < deadline
            time.sleep(.001)
        if connect:
            with socket.create_connection(address, timeout=1):
                pass
        else:
            finished.set()
        worker.join(2)
        assert not worker.is_alive()
        observed = json.loads((directory/'relay-monitor-ended.json').read_text())
        assert observed['connections'] == int(connect)
        assert observed['finished_at_us'] >= observed['started_at_us']
        assert bool(errors) == connect and (directory/'relay-error.json').exists() == connect

print(json.dumps(dict(status='PASS_FIXTURE_ONLY', ast_files=len(files), embedded_python=embedded, constructed=counts,
    malformed_cbor_cases=len(malformed), websocket_cases=5, no_dial_observer_cases=2,
    peer_refusal_sequence_cases=2,
    sequential_listener_cases=3, live_listener_conflicts=2,
    focused_visibility_cases=2,
    expiry_oracle_cases=6,
    full_window_frame_cases=2,
    not_run=['actual CLI', 'PostgreSQL', 'RocksDB', 'product WebSocket', 'renewal', 'signal shutdown',
        'exact-owned process suspension and natural SQL expiry'])))
