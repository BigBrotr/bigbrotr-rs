#!/usr/bin/env python3
"""Actual closed-producer recovery after native success and original SQL rollback.

This first connected case is a prerequisite, not full recovery qualification.
Fault/restart/resource cases and independent altered-observation controls remain
required before reporting that larger capability complete.
"""
import argparse
import json
from pathlib import Path
import subprocess
import sys

sys.dont_write_bytecode = True
SOURCE = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(SOURCE/'experiments/archive-engine'))
sys.path.insert(0, str(SOURCE/'experiments/ingest-bootstrap'))
from signed_publication_cases import command, execute
from closing import sha, check_state, expected_native, same_window
from import_originals import check as check_originals


def main(root, tag):
    resources = json.loads((root/'resources.json').read_text())
    revision = command('git', '-C', str(SOURCE), 'rev-parse', 'HEAD')
    assert not command('git', '-C', str(SOURCE), 'status', '--porcelain', '--untracked-files=all')
    assert revision == resources['source']
    ready = json.loads((root/'out/native-tests/ready.json').read_text())
    assert ready['source'] == revision and ready['status'] == 'READY'
    assert sha((root/'out/native-tests/ingest-bootstrap-probe').read_bytes()) == ready['ingest_probe']
    for identity in [resources['container_id'], resources['postgres_id']]:
        owned = json.loads(command('docker', 'inspect', identity))[0]
        assert owned['Config']['Labels']['org.bigbrotr.publication'] == resources['owner']
        assert owned['State']['Running'] and not owned['State']['OOMKilled']
    assert tag.isalnum() and 1 <= len(tag) <= 8
    target = root/('recovery-'+tag+'.json')
    assert not target.exists()
    report = dict(status='INCOMPLETE', source=revision, artifacts=ready, processes={}, sql=[])
    def save(): target.write_text(json.dumps(report, indent=2)+'\n')
    def sql(db, text):
        observation = dict(database=db, sql_sha256=sha(text.encode()), status='INCOMPLETE')
        report['sql'].append(observation)
        save()
        try:
            result = subprocess.run(['docker', 'exec', '-i', resources['postgres_id'], 'psql', '-XqAt', '-v', 'ON_ERROR_STOP=1', '-U', 'postgres', '-d', db], input=text, text=True, capture_output=True, timeout=30)
            observation.update(status='RETURNED', exit=result.returncode, stdout=result.stdout, stderr=result.stderr)
            result.check_returncode()
            return result.stdout.strip()
        finally: save()
    binary = ['docker', 'exec', '-i', resources['container_id'], '/out/native-tests/ingest-bootstrap-probe']
    def run(label, args):
        observation = report['processes'][label] = {}
        save()
        try: execute(binary+args, 45, observation)
        finally: save()
        return observation
    db, directory = 'bbrecovery_'+tag, '/out/recovery-'+tag
    save()
    sql('postgres', 'CREATE DATABASE '+db)
    setup = run('setup', ['setup-14', db, '/secrets/postgres', 'unused'])
    assert setup['exit'] == 0 and not setup['stderr']
    # This exception runs after the actual native sync. A new recording Control
    # epoch can copy the result; the original reservation is never rewritten.
    sql(db, "CREATE FUNCTION public.fixture_abort_original_copy() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'fixture rollback of original event outcome copy'; END $$; CREATE TRIGGER zz_fixture_abort_original_copy AFTER INSERT ON control.producer_outcomes FOR EACH ROW WHEN (NEW.recorder_epoch=1 AND NEW.request_sequence=2) EXECUTE FUNCTION public.fixture_abort_original_copy();")
    command('docker', 'exec', resources['container_id'], 'mkdir', '-m', '700', directory)
    result = run('recover', ['event-recovery-normal', db, '/secrets/bigbrotr_control', directory+'/db'])
    # Read durable state before interpreting successful exit or fixture assertions.
    control = report['control'] = json.loads(sql(db, "SELECT jsonb_build_object('requests',(SELECT jsonb_agg(to_jsonb(r) ORDER BY request_sequence) FROM control.producer_requests r),'outcomes',(SELECT jsonb_agg(to_jsonb(o) ORDER BY request_sequence) FROM control.producer_outcomes o),'bindings',(SELECT coalesce(jsonb_agg(to_jsonb(b)),'[]'::jsonb) FROM control.producer_input_bindings b))"))
    save()
    dump = run('dump', ['dump', directory+'/db'])
    assert dump['exit'] == 0 and not dump['stderr']
    report['native'] = json.loads(dump['stdout'])
    wanted = expected_native(False, True)
    assert [(r['cf'], bytes.fromhex(r['key']), bytes.fromhex(r['value'])) for r in report['native']['rows']] == wanted
    files = {p.name:p.read_bytes() for p in (root/directory.lstrip('/')/'db/producer-inputs-v1').iterdir()}
    originals = report['originals'] = {name:dict(bytes=len(value), sha256=sha(value)) for name, value in files.items()}
    check_originals(control, files, wanted, 2, repetitions=1)
    names = json.loads(sql(db, "SELECT jsonb_agg(tablename ORDER BY tablename) FROM pg_tables WHERE schemaname='control'"))
    report['remaining_backends'] = int(sql(db, 'SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid()'))
    save()
    assert report['remaining_backends'] == 0
    assert result['exit'] == 0 and not result['stderr']
    messages = [json.loads(line) for line in result['stdout'].splitlines()]
    one = lambda event: next(m['state'] for m in messages if m['event'] == event)
    closing = one('closing-before')
    before, after = one('recovery-before'), one('recovery-after')
    ct = check_state(closing, expected_native(False, False), names, originals)
    bt = check_state(before, wanted, names, originals)
    at = check_state(after, wanted, names, originals)
    same_window(ct, bt, True)
    assert before['sequence'] == closing['sequence']+1
    assert after['sequence'] == before['sequence']
    assert len(bt['producer_outcomes']) == 1 and len(at['producer_outcomes']) == 2
    previous = bt['producer_outcomes'][0]
    assert previous['request_sequence'] == 1 and previous in at['producer_outcomes']
    for table in names:
        if table != 'producer_outcomes': assert bt[table] == at[table], table
    rows = sorted(at['producer_outcomes'], key=lambda row:row['request_sequence'])
    native = {(cf, key):value for cf, key, value in wanted}
    for n, row in enumerate(rows, 1):
        value = native['receipts', b'\x01\x30'+bytes([66])*16+(1).to_bytes(8, 'big')+n.to_bytes(8, 'big')]
        assert row['outcome_kind'] == 1 and row['policy_epoch'] == 1
        assert row['outcome_bytes'] == '\\x'+value.hex() and row['outcome_sha256'] == '\\x'+sha(value)
        assert row['recorder_epoch'] == n
        assert row['recorder_incarnation'] == '\\x'+(bytes([41 if n == 1 else 43])*16).hex()
        assert row['recorder_resource'] == control['requests'][n-1]['issuer_resource']
        assert row['recorder_role'] == 'control' and row['recorded_at_us'] >= control['requests'][n-1]['reserved_at_us']
    resolved = [m for m in messages if m['event'] == 'producer-resolved']
    assert [m['request'] for m in resolved] == [2, 2, 1]
    for message in resolved:
        row = rows[message['request']-1]
        assert message['bytes'] == row['outcome_bytes'][2:]
        assert message['current_control_epoch'] == 2 and message['historical_control_epoch'] == 1
    assert at['producer_epochs'][0]['state'] == 'closing'
    assert at['producer_epochs'][0]['reserved_through_request'] == 2
    assert at['source_streams'][0]['state'] == 'open'
    for table in ['jobs', 'producer_checkpoints', 'producer_input_bindings', 'import_record_closures', 'import_checkpoints']:
        assert at[table] == []
    report.update(status='PASS', checked=dict(native_rows=len(wanted), control_tables=len(names), originals=len(files), sequence=after['sequence'], resolved=[2, 2, 1]))
    save()
    print(json.dumps(dict(status='PASS', **report['checked'])))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, required=True)
    parser.add_argument('--run-tag', required=True)
    args = parser.parse_args()
    main(args.root.resolve(strict=True), args.run_tag)
