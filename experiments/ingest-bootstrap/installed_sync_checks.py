#!/usr/bin/env python3
"""Light fixture checks only; these cannot qualify the installed product journey."""
import ast
import importlib.util
import json
from pathlib import Path
import sys

sys.dont_write_bytecode = True
SOURCE = Path(__file__).resolve().parents[2]
sys.path[:0] = [str(SOURCE/'experiments/control-migrations'), str(Path(__file__).parent)]
spec = importlib.util.spec_from_file_location('bootstrap_oracle', Path(__file__).with_name('oracle.py'))
bootstrap = importlib.util.module_from_spec(spec)
sys.modules['bootstrap_oracle'] = bootstrap
spec.loader.exec_module(bootstrap)
from installed_sync_oracle import decode, expected
from installed_sync_client import read_frame

files = [*Path(__file__).parent.glob('installed_sync*.py'), Path(__file__).with_name('installed_client.py'),
    Path(__file__).with_name('session_client.py'), SOURCE/'experiments/acquisition-scope/bindings.py']
for path in files:
    ast.parse(path.read_text())

state = dict(datasets=[dict(dataset_id='\\x'+'01'*16)], owner_epochs=[dict(role='ingest',
    resource_id='\\x'+'02'*16, process_incarnation='\\x'+'03'*16, acquired_at_us=100)],
    allocation_domains=[dict(dictionary_kind=n, allocation_domain='\\x'+f'{n+16:02x}'*16) for n in range(1, 10)])
public = dict(dataset_id='01'*16, dictionary_kind=4, allocation_domain='14'*16, ordinal='1')
report = dict(owner_resource_id='02'*16, producer_slot_id='04'*16, recovery_generation='05'*16,
    topology_generation='0', dictionary_count=3, public_scope=public, private_scope=dict(public, ordinal='3'))
event = json.loads((SOURCE/'tests/fixtures/ingest/import-event.jsonl').read_text())
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
print(json.dumps(dict(status='PASS_FIXTURE_ONLY', ast_files=len(files), constructed=counts,
    malformed_cbor_cases=len(malformed), websocket_cases=5,
    not_run=['actual CLI', 'PostgreSQL', 'RocksDB', 'product WebSocket', 'renewal', 'signal shutdown'])))
