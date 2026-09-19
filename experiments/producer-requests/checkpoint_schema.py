#!/usr/bin/env python3
"""SQL consistency on a clone of the completed signed checkpoint fixture."""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import sys

sys.dont_write_bytecode = True
SOURCE = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(SOURCE/'experiments/archive-engine'))
from signed_publication_cases import command


def main(root, tag):
    state = json.loads((root/'resources.json').read_text())
    harness = command('git', '-C', str(SOURCE), 'rev-parse', 'HEAD')
    assert not command('git', '-C', str(SOURCE), 'status', '--porcelain', '--untracked-files=all')
    assert not command('git', '-C', str(SOURCE), 'diff', state['source'], harness, '--', 'apps', 'crates', 'migrations', 'Cargo.lock', 'Cargo.toml')
    native = json.loads((root/'import-checkpoints-reverse1.json').read_text())
    assert native['status'] == 'PASS' and native['source'] == state['source']
    owned = json.loads(command('docker', 'inspect', state['postgres_id']))[0]
    assert owned['Config']['Labels']['org.bigbrotr.publication'] == state['owner']
    assert owned['State']['Running'] and not owned['State']['OOMKilled']
    assert tag.isalnum() and 1 <= len(tag) <= 8
    db = 'bbcheckpoint_sql_'+tag
    target = root/('checkpoint-schema-'+tag+'.json'); assert not target.exists()
    prefix = (SOURCE/'experiments/producer-requests/binding_schema_cases.sql').read_text()
    marker = 'SELECT pg_temp.job(1001);'
    assert prefix.count(marker) == 1
    source = prefix.split(marker)[0]+(SOURCE/'experiments/producer-requests/checkpoint_schema_cases.sql').read_text()
    report = dict(status='INCOMPLETE', source=state['source'], harness_revision=harness,
                  fixture_sha256=hashlib.sha256((root/'import-checkpoints-reverse1.json').read_bytes()).hexdigest(),
                  sql_sha256=hashlib.sha256(source.encode()).hexdigest(), database=db, process={})
    def save(): target.write_text(json.dumps(report, indent=2)+'\n')
    def sql(database, text):
        return command('docker', 'exec', '-i', state['postgres_id'], 'psql', '-XqAt', '-v', 'ON_ERROR_STOP=1', '-U', 'postgres', '-d', database, input=text)
    save()
    assert sql('postgres', "SELECT count(*) FROM pg_stat_activity WHERE datname='bbcheckpoint_reverse1'") == '0'
    sql('postgres', 'CREATE DATABASE '+db+' TEMPLATE bbcheckpoint_reverse1')
    try:
        result = subprocess.run(['docker', 'exec', '-i', state['postgres_id'], 'psql', '-XqAt',
            '-v', 'ON_ERROR_STOP=1', '-U', 'postgres', '-d', db], input=source,
            text=True, capture_output=True, timeout=45)
        report['process'].update(exit=result.returncode, stdout=result.stdout, stderr=result.stderr)
        save(); result.check_returncode()
    except subprocess.TimeoutExpired as error:
        decode = lambda value: value.decode(errors='replace') if isinstance(value, bytes) else (value or '')
        report['process'].update(exit=None, remote_exclusion='UNKNOWN', stdout=decode(error.stdout), stderr=decode(error.stderr))
        raise
    finally: save()
    results = [json.loads(line) for line in result.stdout.splitlines() if line.startswith('{')]
    assert len(results) == 1 and results[0]['status'] == 'PASS'
    expected = {'nonempty_native_fixture', 'closure_without_outcome', 'wrong_outcome_digest',
        'hole_preserves_zero', 'gap_fill_one_revision', 'identical_insert_no_rewrite',
        'closure_immutable_update', 'closure_immutable_delete', 'closure_truncate_fk',
        'closure_truncate_guard', 'retained_outcome_delete', 'retained_outcome_truncate',
        'head_delete', 'head_noop_revision', 'forged_global_revision', 'head_without_contiguous_members',
        'head_wrong_byte_boundary', 'first_shard_revision', 'second_shard_global_revision',
        'local_revisions_remain_independent', 'rejected_is_not_event_closure',
        'multirow_after_visibility', 'closure_head_job_rollback', 'paused_job_closure_refused',
        'producer_watermarks_unchanged'}
    assert {case['label'] for case in results[0]['cases']} == expected
    report['result'] = results[0]
    report['remaining_client_backends'] = int(sql(db, 'SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid()'))
    assert report['remaining_client_backends'] == 0
    report['status'] = 'PASS'; save(); print(json.dumps(dict(status='PASS', cases=len(expected))))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, required=True)
    parser.add_argument('--run-tag', required=True)
    args = parser.parse_args()
    main(args.root.resolve(strict=True), args.run_tag)
