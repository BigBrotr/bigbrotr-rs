#!/usr/bin/env python3
"""Light fixture checks only; these cannot qualify the installed product journey."""
import ast
import errno
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
from installed_sync_client import no_connections, read_frame, refusal_frames, relay_listener

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
    not_run=['actual CLI', 'PostgreSQL', 'RocksDB', 'product WebSocket', 'renewal', 'signal shutdown'])))
