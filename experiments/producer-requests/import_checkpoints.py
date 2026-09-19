#!/usr/bin/env python3
"""Actual signed reverse-order logical closure; finite prerequisite, not import acceptance."""
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
from existing_outcome import expected, check_all_outcome_copies


def main(root, tag, red):
    state = json.loads((root/'resources.json').read_text())
    revision = command('git', '-C', str(SOURCE), 'rev-parse', 'HEAD')
    assert not command('git', '-C', str(SOURCE), 'status', '--porcelain', '--untracked-files=all')
    assert not command('git', '-C', str(SOURCE), 'diff', state['source'], revision, '--',
                       'apps', 'crates', 'migrations', 'Cargo.lock', 'Cargo.toml')
    ready = json.loads((root/'out/native-tests/ready.json').read_text())
    assert ready['status'] == 'READY' and ready['source'] == state['source']
    artifact = root/'out/native-tests/ingest-bootstrap-probe'
    assert hashlib.sha256(artifact.read_bytes()).hexdigest() == ready['ingest_probe']
    for identity in [state['container_id'], state['postgres_id']]:
        owned = json.loads(command('docker', 'inspect', identity))[0]
        assert owned['Config']['Labels']['org.bigbrotr.publication'] == state['owner']
        assert owned['State']['Running'] and not owned['State']['OOMKilled']
    assert tag.isalnum() and 1 <= len(tag) <= 8
    target = root/('import-checkpoints-'+tag+'.json')
    assert not target.exists()
    report = dict(status='INCOMPLETE', source=state['source'], harness_revision=revision,
                  artifacts=ready, expected_missing_progress=red, processes={}, sql=[])
    def save(): target.write_text(json.dumps(report, indent=2)+'\n')
    def sql(db, text):
        observation = dict(database=db, sql_sha256=hashlib.sha256(text.encode()).hexdigest(),
                           status='INCOMPLETE', remote_exclusion='UNKNOWN')
        report['sql'].append(observation); save()
        try:
            result = subprocess.run(['docker', 'exec', '-i', state['postgres_id'], 'psql', '-XqAt',
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
    binary = ['docker', 'exec', '-i', state['container_id'], '/out/native-tests/ingest-bootstrap-probe']
    def run(label, args):
        observation = report['processes'][label] = {}; save()
        try: execute(binary+args, 40, observation)
        finally: save()
        return observation
    db = 'bbcheckpoint_'+tag
    directory = '/out/checkpoints-'+tag
    save()
    sql('postgres', 'CREATE DATABASE '+db)
    setup = run('setup', ['setup-14', db, '/secrets/postgres', 'unused'])
    assert setup['exit'] == 0 and not setup['stderr']
    command('docker', 'exec', state['container_id'], 'mkdir', '-m', '700', directory)
    result = run('admit', ['event-bindings-checkpoints', db, '/secrets/bigbrotr_control', directory+'/db'])
    control = report['control'] = json.loads(sql(db, "SELECT jsonb_build_object('requests',(SELECT jsonb_agg(to_jsonb(r) ORDER BY request_sequence) FROM control.producer_requests r),'outcomes',(SELECT jsonb_agg(to_jsonb(o) ORDER BY request_sequence) FROM control.producer_outcomes o),'bindings',(SELECT jsonb_agg(to_jsonb(b) ORDER BY logical_record) FROM control.producer_input_bindings b),'closures',(SELECT coalesce(jsonb_agg(to_jsonb(c) ORDER BY logical_record),'[]'::jsonb) FROM control.import_record_closures c),'heads',(SELECT coalesce(jsonb_agg(to_jsonb(h)),'[]'::jsonb) FROM control.import_checkpoints h),'jobs',(SELECT jsonb_agg(to_jsonb(j)) FROM control.jobs j),'producer_checkpoints',(SELECT count(*) FROM control.producer_checkpoints))")); save()
    dump = run('dump', ['dump', directory+'/db'])
    assert dump['exit'] == 0 and not dump['stderr']
    native = report['native'] = json.loads(dump['stdout']); save()
    wanted = expected_event(bytes([1])*16, True, 2)[0] if red else expected(2)[0]
    assert [(r['cf'], bytes.fromhex(r['key']), bytes.fromhex(r['value'])) for r in native['rows']] == wanted
    check_all_outcome_copies(control, wanted, request_count=2 if red else 3, outcome_count=2 if red else 3)
    assert control['producer_checkpoints'] == 0
    if red:
        # A measured failing assertion, after native admission and its PG copy.
        assert result['exit'] == 101
        assert 'left: 0' in result['stderr'] and 'right: 1' in result['stderr']
        assert len(control['bindings']) == 1 and control['bindings'][0]['logical_record'] == 2
        assert control['closures'] == control['heads'] == []
        assert control['jobs'][0]['job_revision'] == 3 and control['jobs'][0]['checkpoint_revision'] == 0
    else:
        assert result['exit'] == 0 and not result['stderr']
        assert [(c['logical_record'], c['request_sequence']) for c in control['closures']] == [(1, 3), (2, 2)]
        assert len(control['heads']) == 1
        assert (control['heads'][0]['revision'], control['heads'][0]['complete_through_record'],
                control['heads'][0]['byte_offset']) == (1, 2, 706)
        assert (control['jobs'][0]['job_revision'], control['jobs'][0]['checkpoint_revision']) == (4, 1)
        messages = [json.loads(line) for line in result['stdout'].splitlines()]
        progress = [m for m in messages if m['event'] == 'logical-checkpoint']
        assert [(m['request_sequence'], m['retry']) for m in progress] == [(2, False), (2, True), (3, False), (3, True)]
        for original, retry in [(progress[0], progress[1]), (progress[2], progress[3])]:
            assert original['state'] == retry['state'] and original['receipt'] == retry['receipt']
        assert progress[0]['state']['heads'] == [] and len(progress[0]['state']['closures']) == 1
        for closure in control['closures']:
            receipt = control['outcomes'][closure['request_sequence']-1]
            binding = next(b for b in control['bindings'] if b['logical_record'] == closure['logical_record'])
            for key in ['job_id', 'dataset_id', 'shard_id', 'logical_record', 'producer_slot_id', 'producer_epoch', 'request_sequence']:
                assert closure[key] == binding[key]
            for key in ['outcome_sha256', 'recorder_resource', 'recorder_epoch', 'recorder_incarnation']:
                assert closure[key] == receipt[key]
    report['remaining_client_backends'] = int(sql(db, 'SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid()'))
    assert report['remaining_client_backends'] == 0
    report['status'] = 'EXPECTED_RED' if red else 'PASS'
    save(); print(json.dumps(dict(status=report['status'], native_rows=len(wanted))))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, required=True)
    parser.add_argument('--run-tag', required=True)
    parser.add_argument('--expect-missing-progress', action='store_true')
    args = parser.parse_args()
    main(args.root.resolve(strict=True), args.run_tag, args.expect_missing_progress)
