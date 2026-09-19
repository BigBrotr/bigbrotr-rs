#!/usr/bin/env python3
"""Actual signed existing-effect reads, independent proof bytes and retained rows."""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import sys

sys.dont_write_bytecode = True
SOURCE = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(SOURCE/'experiments/archive-engine'))
sys.path.insert(0, str(SOURCE/'experiments/ingest-bootstrap'))
from signed_publication_cases import command, execute
from event_oracle import expected_event
from existing_outcome import check_all_outcome_copies
from import_originals import check as check_originals
from oracle import cbor


def sha(value):
    return hashlib.sha256(value).hexdigest()


def compact(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False).encode()


def expected_proof(wanted):
    dataset = bytes([1])*16
    event = json.loads((SOURCE/'tests/fixtures/ingest/import-event.jsonl').read_bytes())
    signed = [bytes.fromhex(event['id']), bytes.fromhex(event['pubkey']), event['created_at'],
              event['kind'], event['tags'], event['content'], bytes.fromhex(event['sig'])]
    scope, lineage = [dataset, 4, bytes([64])*16, 1], [dataset, 9, bytes([69])*16, 1]
    intent = b'bigbrotr/producer-request/1\0'+cbor([[bytes(16), 0, 0], 'certify-existing', 1,
                                                  [1, scope, signed, 'import', lineage, None]])
    digest = hashlib.sha256(intent).digest()
    rows = {(cf, key): value for cf, key, value in wanted}
    dependencies = []
    for sequence in [1, 3, 4, 5, 6]:
        key = b'\x01\x20'+dataset+bytes(4)+(1).to_bytes(8, 'big')+sequence.to_bytes(8, 'big')
        dependencies.append([[dataset, 0, 1, sequence], rows['facts', key][2:34]])
    preimage = b'bigbrotr/existing-proof/1\0'+cbor([dataset, 0, 'local-synced', bytes([67])*16,
                                                1, digest, dependencies])
    return dict(result='existing', context=dict(dataset_id=dataset.hex(), topology_generation='0',
        protection_class='local-synced', recovery_generation=(bytes([67])*16).hex(), policy_epoch='1'),
        intent_digest=digest.hex(), proof_digest=sha(preimage), dependencies=[dict(
            source_record_id=dict(dataset_id=dataset.hex(), shard_id=0, owner_epoch='1', sequence=str(s[3])),
            payload_digest=d.hex()) for s, d in dependencies])


def check_messages(messages, scenario, wanted, originals, table_names):
    before = [m['state'] for m in messages if m['event'] == 'certification-before']
    after = [m['state'] for m in messages if m['event'] == 'certification-after']
    assert len(before) == len(after) == 1 and before == after
    state = before[0]
    rows = [dict(cf=cf, key=key.hex(), value=value.hex()) for cf, key, value in wanted]
    assert state['native_rows'] == 26 and state['native_sha256'] == sha(compact(rows))
    assert isinstance(state['sequence'], int) and state['sequence'] > 0
    assert [t[0] for t in state['control_tables']] == table_names
    assert state['control_sha256'] == sha(compact(state['control_tables']))
    tables = {name: json.loads(raw) for name, raw in state['control_tables']}
    assert all('xmin' in row for values in tables.values() for row in values)
    assert len(tables['producer_requests']) == len(tables['producer_outcomes']) == 2
    assert len(tables['producer_epochs']) == 1
    assert tables['producer_epochs'][0]['reserved_through_request'] == 2
    assert tables['producer_epochs'][0]['state'] == ('closing' if scenario == 'retired' else 'open')
    for name in ['producer_input_bindings', 'import_record_closures', 'import_checkpoints',
                 'producer_checkpoints', 'jobs']:
        assert tables[name] == []
    assert state['originals'] == [[name, value] for name, value in sorted(originals.items())]
    expected = expected_proof(wanted)
    if scenario == 'normal':
        outputs = [('existing', expected), ('existing', expected),
            ('missing-scope', dict(result='needs-admission')),
            ('missing-lineage', dict(result='needs-admission')),
            ('invalid', dict(result='rejected', reason='Signature')),
            ('size', dict(result='rejected', reason='EventSize')),
            ('limit', dict(result='refused', error='Native(Limit)')),
            ('cancelled', dict(result='refused', error='Native(AuthorityUnavailable)')),
            ('existing', expected)]
    else:
        error = {'expired':'Control(Unavailable)', 'policy':'Control(InvalidPolicy)',
                 'retired':'Registry(InvalidMetadata)'}[scenario]
        outputs = [(scenario, dict(result='refused', error=error))]
    observed = [(m['case'], m['output']) for m in messages if m['event'] == 'certification']
    assert observed == outputs
    return dict(reads=len(outputs), existing=sum(o['result'] == 'existing' for _, o in outputs),
                native_rows=26, control_tables=len(table_names), native_sequence=state['sequence'])


def main(root, tag, scenario):
    resources = json.loads((root/'resources.json').read_text())
    revision = command('git', '-C', str(SOURCE), 'rev-parse', 'HEAD')
    assert not command('git', '-C', str(SOURCE), 'status', '--porcelain', '--untracked-files=all')
    assert not command('git', '-C', str(SOURCE), 'diff', resources['source'], revision, '--',
                       'apps', 'crates', 'migrations', 'Cargo.lock', 'Cargo.toml', 'rust-toolchain.toml')
    ready = json.loads((root/'out/native-tests/ready.json').read_text())
    assert ready['status'] == 'READY' and ready['source'] == resources['source']
    assert sha((root/'out/native-tests/ingest-bootstrap-probe').read_bytes()) == ready['ingest_probe']
    for identity in [resources['container_id'], resources['postgres_id']]:
        owned = json.loads(command('docker', 'inspect', identity))[0]
        assert owned['Config']['Labels']['org.bigbrotr.publication'] == resources['owner']
        assert owned['State']['Running'] and not owned['State']['OOMKilled']
    assert tag.isalnum() and 1 <= len(tag) <= 8
    target = root/('certification-'+tag+'.json')
    assert not target.exists()
    report = dict(status='INCOMPLETE', source=resources['source'], harness_revision=revision,
                  artifacts=ready, scenario=scenario,
                  processes={}, sql=[])
    def save(): target.write_text(json.dumps(report, indent=2)+'\n')
    def sql(db, text):
        observation = dict(database=db, sql_sha256=sha(text.encode()), status='INCOMPLETE',
                           remote_exclusion='UNKNOWN')
        report['sql'].append(observation); save()
        try:
            result = subprocess.run(['docker', 'exec', '-i', resources['postgres_id'], 'psql', '-XqAt',
                '-v', 'ON_ERROR_STOP=1', '-U', 'postgres', '-d', db],
                input=text, text=True, capture_output=True, timeout=30)
            observation.update(status='RETURNED', exit=result.returncode,
                               stdout=result.stdout, stderr=result.stderr)
            result.check_returncode()
            return result.stdout.strip()
        except subprocess.TimeoutExpired as error:
            decode = lambda v: v.decode(errors='replace') if isinstance(v, bytes) else (v or '')
            observation.update(status='TIMEOUT', stdout=decode(error.stdout), stderr=decode(error.stderr))
            raise
        finally: save()
    binary = ['docker', 'exec', '-i', resources['container_id'], '/out/native-tests/ingest-bootstrap-probe']
    def run(label, args):
        observation = report['processes'][label] = {}; save()
        try: execute(binary+args, 40, observation)
        finally: save()
        return observation
    db, directory = 'bbcertification_'+tag, '/out/certification-'+tag
    save(); sql('postgres', 'CREATE DATABASE '+db)
    setup = run('setup', ['setup-14', db, '/secrets/postgres', 'unused'])
    assert setup['exit'] == 0 and not setup['stderr']
    command('docker', 'exec', resources['container_id'], 'mkdir', '-m', '700', directory)
    mode = 'event-certification'+('' if scenario == 'normal' else '-'+scenario)
    result = run('evaluate', [mode, db, '/secrets/bigbrotr_control', directory+'/db'])
    # Preserve and inspect durable state even when the application fixture fails.
    control = report['control'] = json.loads(sql(db, "SELECT jsonb_build_object('requests',(SELECT jsonb_agg(to_jsonb(r) ORDER BY request_sequence) FROM control.producer_requests r),'outcomes',(SELECT jsonb_agg(to_jsonb(o) ORDER BY request_sequence) FROM control.producer_outcomes o),'bindings',(SELECT coalesce(jsonb_agg(to_jsonb(b)),'[]'::jsonb) FROM control.producer_input_bindings b),'producer',(SELECT jsonb_agg(to_jsonb(p)) FROM control.producer_epochs p),'streams',(SELECT jsonb_agg(to_jsonb(s)) FROM control.source_streams s),'closures',(SELECT count(*) FROM control.import_record_closures),'heads',(SELECT count(*) FROM control.import_checkpoints),'jobs',(SELECT count(*) FROM control.jobs),'checkpoints',(SELECT count(*) FROM control.producer_checkpoints))")); save()
    dump = run('dump', ['dump', directory+'/db'])
    assert dump['exit'] == 0 and not dump['stderr']
    native = report['native'] = json.loads(dump['stdout']); save()
    wanted = expected_event(bytes([1])*16, True, 1)[0]
    assert [(r['cf'], bytes.fromhex(r['key']), bytes.fromhex(r['value'])) for r in native['rows']] == wanted
    check_all_outcome_copies(control, wanted, request_count=2, outcome_count=2)
    files = {p.name:p.read_bytes() for p in (root/directory.lstrip('/')/'db/producer-inputs-v1').iterdir()}
    report['originals'] = {name:dict(bytes=len(value), sha256=sha(value)) for name, value in files.items()}; save()
    check_originals(control, files, wanted, 2, repetitions=1)
    assert control['bindings'] == []
    assert control['closures'] == control['heads'] == control['jobs'] == control['checkpoints'] == 0
    assert len(control['producer']) == len(control['streams']) == 1
    assert control['producer'][0]['reserved_through_request'] == 2
    table_names = json.loads(sql(db, "SELECT jsonb_agg(tablename ORDER BY tablename) FROM pg_tables WHERE schemaname='control'"))
    report['remaining_client_backends'] = int(sql(db, 'SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid()'))
    save(); assert report['remaining_client_backends'] == 0
    assert result['exit'] == 0 and not result['stderr']
    messages = [json.loads(line) for line in result['stdout'].splitlines()]
    report['checked'] = check_messages(messages, scenario, wanted, report['originals'], table_names)
    report['status'] = 'PASS'; save(); print(json.dumps(dict(status='PASS', **report['checked'])))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, required=True)
    parser.add_argument('--run-tag', required=True)
    parser.add_argument('--scenario', choices=['normal', 'expired', 'policy', 'retired'], default='normal')
    args = parser.parse_args()
    main(args.root.resolve(strict=True), args.run_tag, args.scenario)
