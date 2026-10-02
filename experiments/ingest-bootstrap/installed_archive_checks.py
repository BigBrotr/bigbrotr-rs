#!/usr/bin/env python3
"""Pure fixture checks; not installed Archive, PostgreSQL or worker qualification."""
import ast
import hashlib
import importlib.util
import json
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace

sys.dont_write_bytecode = True
SOURCE = Path(__file__).resolve().parents[2]
sys.path[:0] = [str(SOURCE / 'experiments/control-migrations'), str(Path(__file__).parent)]
spec = importlib.util.spec_from_file_location('bootstrap_oracle', Path(__file__).with_name('oracle.py'))
bootstrap = importlib.util.module_from_spec(spec)
sys.modules['bootstrap_oracle'] = bootstrap
spec.loader.exec_module(bootstrap)
from installed_archive import CASES, verify_file
from installed_archive_oracle import expected
from installed_archive_frontier_checks import check as check_frontiers

checked = [*Path(__file__).parent.glob('installed_archive*.py'),
    Path(__file__).with_name('installed_client.py'), SOURCE / 'experiments/acquisition-scope/bindings.py']
embedded = 0
for path in checked:
    parsed = ast.parse(path.read_text())
    for node in ast.walk(parsed):
        if isinstance(node, ast.Assign) and any(isinstance(t, ast.Name) and t.id == 'wrapper' for t in node.targets):
            ast.parse(ast.literal_eval(node.value)); embedded += 1
from installed_archive_journey import CASES as PUBLICATION_CASES
assert PUBLICATION_CASES == ['archive-wrong-reader', 'archive-missing-reader', 'archive-reader-grant', 'archive-signed']
runner = ast.parse((SOURCE / 'experiments/acquisition-scope/bindings.py').read_text())
start = next(i for i,n in enumerate(runner.body) if isinstance(n,ast.Assign) and any(isinstance(t,ast.Name) and t.id=='guards' for t in n.targets))
end = next(i for i,n in enumerate(runner.body) if isinstance(n,ast.Assign) and any(isinstance(t,ast.Name) and t.id=='revision' for t in n.targets))
flags = compile(ast.Module(body=runner.body[start:end],type_ignores=[]), '<runner flags>', 'exec')
for mode in ('--installed-archive', '--installed-archive-setup', '--installed-sync', '--installed-sync-window'):
    selected = {'sys':SimpleNamespace(argv=['bindings.py','/unused',mode])}
    exec(flags,selected)
    assert selected['installed_archive'] == mode.startswith('--installed-archive')
    assert selected['installed_archive_publication'] == (mode=='--installed-archive')
    assert selected['installed_archive_setup'] == (mode=='--installed-archive-setup')
    assert selected['installed_sync'] == mode.startswith('--installed-sync')
assert CASES == ['archive-setup-refusal']
state = dict(datasets=[dict(dataset_id='\\x' + '01'*16)], owner_epochs=[dict(role='ingest',
    resource_id='\\x' + '02'*16, process_incarnation='\\x' + '03'*16, acquired_at_us=100)],
    allocation_domains=[dict(dictionary_kind=n, allocation_domain='\\x' + f'{n+16:02x}'*16) for n in range(1, 10)])
public = dict(dataset_id='01'*16, dictionary_kind=4, allocation_domain='14'*16, ordinal='1')
report = dict(owner_resource_id='02'*16, producer_slot_id='04'*16, recovery_generation='05'*16,
    topology_generation='0', dictionary_count=3, public_scope=public, private_scope=dict(public, ordinal='3'))
event = json.loads((SOURCE / 'tests/fixtures/ingest/import-event.jsonl').read_text())
value = expected(state, report, event, 200, 300, 400)
assert len(value['native']) == 49 and len(value['inputs']) == len(value['receipts']) == 4
assert [(b['first'], b['last']) for b in value['batches']] == [(1, 2), (3, 3), (4, 7), (8, 12)]
assert [len(b['facts']) for b in value['batches']] == [2, 1, 4, 5]
assert len({b['archive_digest'] for b in value['batches']}) == 4
assert all(b['admission_digest'] != b['archive_digest'] for b in value['batches'])
frontier_checks = check_frontiers(value)
rows = value['tables']
assert [r['visibility_scope_id']['ordinal'] for r in rows['dictionaries']] == [1, 1, 3, 3, 3, 3, 3, 3]
assert rows['dictionaries'][1]['descriptor_value']['class'] == 'private'
assert rows['dictionaries'][1]['reference']['ordinal'] == 2
assert rows['dictionaries'][2]['reference']['ordinal'] == 3
assert rows['events'][0]['tags'] == event['tags'] and rows['events'][0]['content'] == event['content']
assert rows['event_eligibility_facts'][0]['sig'] == event['sig'].upper()
assert rows['event_receive_facts'][0]['received_at'] == rows['provenance_facts'][0]['received_at'] == 300
# Semantic Archive identity consumes actual current topology; native fact bytes
# are unchanged when the explicit parameter changes. Never hardcode generation0.
other = expected(state, dict(report, topology_generation='7'), event, 200, 300, 400)
assert [b['facts'] for b in other['batches']] == [b['facts'] for b in value['batches']]
assert all(a['archive_digest'] != b['archive_digest'] for a, b in zip(value['batches'], other['batches']))
rejected = 0
with tempfile.TemporaryDirectory(prefix='bigbrotr-archive-fixture-') as temp:
    path = Path(temp) / 'worker'
    path.write_bytes(b'fixture-only-binary')
    path.chmod(0o500)
    identity = dict(bytes=path.stat().st_size, sha256=hashlib.sha256(path.read_bytes()).hexdigest())
    verify_file(path, identity, executable=True)
    for fault in ('size', 'hash', 'writable', 'not-executable', 'symlink'):
        candidate, wanted = path, dict(identity)
        if fault == 'size': wanted['bytes'] += 1
        elif fault == 'hash': wanted['sha256'] = '0'*64
        elif fault == 'writable': path.chmod(0o522)
        elif fault == 'not-executable': path.chmod(0o400)
        else:
            candidate = Path(temp) / 'alias'
            candidate.symlink_to(path)
        try:
            verify_file(candidate, wanted, executable=True)
        except AssertionError:
            rejected += 1
        else:
            raise AssertionError('asset mismatch accepted: ' + fault)
        finally:
            path.chmod(0o500)
assert rejected == 5
print(json.dumps(dict(status='PASS_FIXTURE_CHECKS_ONLY', ast_files=len(checked), embedded_python=embedded, runner_modes=4, native_rows=49,
    independent_complete_batches=4, scientific_facts=12, asset_negative_controls=rejected, **frontier_checks,
    not_run=['installed CLI', 'PostgreSQL catalog initialization', 'authenticated native worker',
        'role-selected process refusal', 'Archive publication/frontier/read/export'])))
