#!/usr/bin/env python3
"""Actual native outcome handoff and fourth-COMMIT faults in an isolated Tux lab."""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import sys

sys.dont_write_bytecode = True
SOURCE = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(SOURCE / 'experiments/archive-engine'))
sys.path.insert(0, str(SOURCE / 'experiments/ingest-bootstrap'))
from signed_publication_cases import command, execute, event, proxy_observation
from fault_cases import Process
from oracle import expected


def main(root):
    state = json.loads((root / 'resources.json').read_text())
    assert SOURCE == root / 'source'
    assert command('git', '-C', str(SOURCE), 'rev-parse', 'HEAD') == state['source']
    assert not command('git', '-C', str(SOURCE), 'status', '--porcelain', '--untracked-files=all')
    runner, pg = state['container_id'], state['postgres_id']
    for identity in [runner, pg]:
        record = json.loads(command('docker', 'inspect', identity))[0]
        assert record['State']['Running'] and not record['State']['OOMKilled']
        assert record['Config']['Labels']['org.bigbrotr.publication'] == state['owner']
    ready = json.loads((root / 'out/native-tests/ready.json').read_text())
    assert ready['source'] == state['source']
    assert hashlib.sha256((root/'out/native-tests/ingest-bootstrap-probe').read_bytes()).hexdigest() == ready['ingest_probe']
    evidence = root / 'producer-outcome-cases.json'
    assert not evidence.exists()
    report = dict(status='INCOMPLETE', source=state['source'], tree=state['tree'],
                  artifacts=ready, harness_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(), cases=[])
    def save(): evidence.write_text(json.dumps(report, indent=2)+'\n')
    def sql(db, text):
        return command('docker','exec','-i',pg,'psql','-XAt','-v','ON_ERROR_STOP=1','-U','postgres','-d',db,input=text)
    def rows(db):
        return json.loads(sql(db,"SELECT json_build_object('requests',(SELECT coalesce(json_agg(t),'[]'::json) FROM control.producer_requests t),'outcomes',(SELECT coalesce(json_agg(t),'[]'::json) FROM control.producer_outcomes t),'checkpoints',(SELECT coalesce(json_agg(t),'[]'::json) FROM control.producer_checkpoints t))"))
    binary = ['docker', 'exec', runner, '/out/native-tests/ingest-bootstrap-probe']
    wanted, receipt, _ = expected(bytes([1])*16, True)
    save()
    for label in ['write', 'outcome-refused', 'outcome-rollback', 'before-forward', 'after-complete']:
        case = dict(name=label, status='INCOMPLETE', process={})
        report['cases'].append(case); save()
        db = 'bboutcome_'+label.replace('-', '_')
        sql('postgres', f'CREATE DATABASE {db}')
        setup = {}; execute(binary+['setup-12', db, '/secrets/postgres', 'unused'], 30, setup)
        case['setup'] = setup; save()
        assert setup['exit'] == 0 and not setup['stderr']
        if label == 'outcome-refused':
            sql(db, 'REVOKE INSERT ON control.producer_outcomes FROM bigbrotr_control')
        if label == 'outcome-rollback':
            sql(db, "CREATE FUNCTION public.fixture_abort() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'fixture rollback'; END $$; CREATE TRIGGER zz_fixture_abort AFTER INSERT ON control.producer_outcomes FOR EACH ROW EXECUTE FUNCTION public.fixture_abort();")
        mode = 'outcome-commit-lost' if label in ['before-forward', 'after-complete'] else label
        home = '/out/outcome_'+label
        command('docker','exec',runner,'mkdir','-m','700',home)
        proxy = None
        try:
            if mode == 'outcome-commit-lost':
                proxy = Process(['docker','exec','-i',runner,'python3','-B','/source/experiments/control-migrations/fault_proxy.py','--upstream-port','5432','--listen-port','6543','--mode',label,'--commit-number','4','--deadline-seconds','35'], limit=65536)
                event(proxy, 'ready')
            execute(binary+[mode,db,'/secrets/bigbrotr_control',home+'/db'],35,case['process'])
            save(); assert case['process']['exit'] == 0 and not case['process']['stderr']
            if proxy:
                assert proxy.wait(5) == 0
                case['proxy'] = proxy_observation(proxy); save()
                fault = json.loads(case['proxy']['stdout'].splitlines()[-1])
                assert fault['event'] == 'receipt' and fault['commits_seen'] == 4 and fault['faults'] == 1
                assert fault['server_commit_complete'] == (4 if label == 'after-complete' else 3)
                assert fault['client_commit_complete'] == 3
        finally:
            if proxy:
                proxy.stop(); case['proxy'] = proxy_observation(proxy)
            save()
        case['native'] = json.loads(command(*binary,'dump',home+'/db'))
        assert [(r['cf'],bytes.fromhex(r['key']),bytes.fromhex(r['value'])) for r in case['native']['rows']] == wanted
        case['control'] = rows(db); save()
        copied = label in ['write', 'after-complete']
        assert len(case['control']['requests']) == 1 and not case['control']['checkpoints']
        assert len(case['control']['outcomes']) == int(copied)
        if copied:
            row = case['control']['outcomes'][0]
            reservation = case['control']['requests'][0]
            for field in ['producer_slot_id','producer_epoch','request_sequence','dataset_id','request_digest','manifest_sha256']:
                assert row[field] == reservation[field]
            assert row['outcome_kind'] == 1 and row['policy_epoch'] == 1
            assert row['outcome_bytes'] == '\\x'+receipt.hex()
            assert row['outcome_sha256'] == '\\x'+hashlib.sha256(receipt).hexdigest()
        assert sql(db,"SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid()") == '0'
        inspection = {}; execute(binary+['inspect',db,'/secrets/bigbrotr_control',home+'/db'],30,inspection)
        case['inspection'] = inspection; save()
        assert inspection['exit'] == 0 and not inspection['stderr']
        observed = json.loads(inspection['stdout'])
        assert observed['control_epoch'] == 2 and observed['present']
        assert (observed['outcome'] is not None) == copied
        if copied: assert observed['outcome']['sha256'] == hashlib.sha256(receipt).hexdigest()
        assert rows(db) == case['control']
        case['status'] = 'PASS'; save()
    fixture = SOURCE/'experiments/producer-requests/outcome_schema_cases.sql'
    run = subprocess.run(['docker','exec','-i',pg,'psql','-XAt','-U','postgres','-d','bboutcome_write'],
                         input=fixture.read_text(), capture_output=True, text=True, timeout=60)
    report['schema'] = dict(exit=run.returncode, stdout=run.stdout, stderr=run.stderr,
                            fixture_sha256=hashlib.sha256(fixture.read_bytes()).hexdigest()); save()
    assert run.returncode == 0
    result, _ = json.JSONDecoder().raw_decode(run.stdout[run.stdout.index('{'):])
    assert result['status'] == 'PASS' and len(result['cases']) == 19
    report['status'] = 'PASS'; save()
    print(json.dumps(dict(status='PASS', cases=len(report['cases']))))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, required=True)
    main(parser.parse_args().root.resolve(strict=True))
