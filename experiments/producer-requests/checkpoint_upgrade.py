#!/usr/bin/env python3
"""Migration14 against actual migration13 clients and retained bound native outcomes.

Any failed/unknown docker-exec requires an owner-verified terminal laboratory audit;
collecting its local CLI is not exclusion of a remote native owner.
"""
import argparse
import hashlib
import json
from pathlib import Path
import shutil
import subprocess
import sys

sys.dont_write_bytecode = True
SOURCE = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(SOURCE/'experiments/control-migrations'))
sys.path.insert(0, str(SOURCE/'experiments/ingest-bootstrap'))
from lab import change
from oracle import expected
from existing_outcome import expected as signed_expected, check_all_outcome_copies
from import_originals import check as check_originals


def command(*args, input=None):
    p = subprocess.run(args, input=input, text=True, capture_output=True, timeout=40)
    assert p.returncode == 0, p.stderr
    return p.stdout.strip()


def main(root, old_root):
    state = json.loads((root/'resources.json').read_text())
    assert state['root'] == str(root)
    harness = command('git', '-C', str(SOURCE), 'rev-parse', 'HEAD')
    assert not command('git', '-C', str(SOURCE), 'status', '--porcelain', '--untracked-files=all')
    assert command('git', '-C', str(root/'source'), 'rev-parse', 'HEAD') == state['source']
    assert not command('git', '-C', str(root/'source'), 'status', '--porcelain', '--untracked-files=all')
    assert (SOURCE/'migrations/postgres/0014_import_checkpoints.sql').read_bytes() == (root/'source/migrations/postgres/0014_import_checkpoints.sql').read_bytes()
    runner, pg = state['container_id'], state['postgres_id']
    for identity in [runner, pg]:
        c = json.loads(command('docker', 'inspect', identity))[0]
        assert c['Config']['Labels']['org.bigbrotr.publication'] == state['owner']
        assert c['State']['Running'] and not c['State']['OOMKilled']
    ready = json.loads((root/'out/native-tests/ready.json').read_text())
    old = json.loads((old_root/'out/native-tests/ready.json').read_text())
    assert ready['source'] == state['source'] and ready['status'] == 'READY'
    assert old['source'] == '8b0660e580a61503686abab6a02944258e995bb0' and old['status'] == 'READY'
    assert old['ingest_probe'] == '19abd7e1a156b7d65145e965bbf5645eeff377ca5db78d2161bb0dd154844a9a'
    old_artifact = old_root/'out/native-tests/ingest-bootstrap-probe'
    assert hashlib.sha256(old_artifact.read_bytes()).hexdigest() == old['ingest_probe']
    for path, key in [('bigbrotr', 'control'), ('ingest-bootstrap-probe', 'ingest_probe')]:
        assert hashlib.sha256((root/'out/native-tests'/path).read_bytes()).hexdigest() == ready[key]
    work = root/'out/checkpoint-upgrade-r3'; work.mkdir(mode=0o700)
    shutil.copyfile(old_artifact, work/'old-ingest-probe'); (work/'old-ingest-probe').chmod(0o500)
    assert hashlib.sha256((work/'old-ingest-probe').read_bytes()).hexdigest() == old['ingest_probe']
    credential = work/'migrator-secret'
    credential.write_bytes((root/'secrets/postgres').read_bytes()); credential.chmod(0o400)
    report_path = root/'import-checkpoint-upgrade-r3.json'; assert not report_path.exists()
    report = dict(status='INCOMPLETE', source=state['source'], harness_revision=harness,
                  harness_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                  artifacts=ready, old_build=old, cases=[], operations=[])
    def save(): report_path.write_text(json.dumps(report, indent=2)+'\n')
    def observe(label, args, input=None):
        item = dict(label=label, exit=None, remote_exclusion='UNKNOWN')
        report['operations'].append(item); save()
        try:
            p = subprocess.run(args, input=input, capture_output=True, text=True, timeout=40)
            item.update(exit=p.returncode, stdout=p.stdout, stderr=p.stderr)
            return p
        except subprocess.TimeoutExpired as error:
            partial = lambda b: b.decode(errors='replace') if isinstance(b, bytes) else b or ''
            item.update(failure='TimeoutExpired', stdout=partial(error.stdout), stderr=partial(error.stderr))
            raise
        except BaseException as error:
            item.update(failure=type(error).__name__)
            raise
        finally:
            save()
    def checked(label, args, input=None):
        p = observe(label, args, input)
        assert p.returncode == 0, p.stderr
        return p.stdout.strip()
    def sql(db, text):
        return checked('sql:'+db, ['docker','exec','-i',pg,'psql','-XqAt','-v','ON_ERROR_STOP=1','-U','postgres','-d',db], input=text)
    new = ['docker','exec',runner,'/out/native-tests/ingest-bootstrap-probe']
    old_command = ['docker','exec',runner,'/out/checkpoint-upgrade-r3/old-ingest-probe']
    main_command = ['docker','exec',runner,'/out/native-tests/bigbrotr']
    checked('config-init',main_command+['config','init','--recipe','archive-only','--output','/out/checkpoint-upgrade-r3/template.toml'])
    def config(db):
        text = change((work/'template.toml').read_text(),'postgres.control','database',db)
        text = change(text,'postgres.control','endpoint','127.0.0.1:5432')
        text = change(text,'postgres.control.tls','local_boundary','loopback')
        (work/(db+'.toml')).write_text(text)
        return '/out/checkpoint-upgrade-r3/'+db+'.toml'
    def snapshot(db):
        names = json.loads(sql(db,"SELECT jsonb_agg(tablename ORDER BY tablename) FROM pg_tables WHERE schemaname='control'"))
        assert 1 <= len(names) <= 64 and all(n.replace('_','').isalnum() for n in names)
        return {n:json.loads(sql(db,'SELECT coalesce(jsonb_agg(to_jsonb(t) ORDER BY to_jsonb(t)::text),\'[]\'::jsonb) FROM control.'+n+' t')) for n in names}
    def schema_digest(db):
        dump = checked('schema-dump:'+db, ['docker','exec',pg,'pg_dump','-U','postgres','-d',db,
                       '--schema=control','--schema-only','--no-owner','--no-privileges'])
        # PostgreSQL randomizes these psql guards on each otherwise identical dump.
        dump = '\n'.join(line for line in dump.splitlines() if not line.startswith(('\\restrict ', '\\unrestrict ')))
        return hashlib.sha256(dump.encode()).hexdigest()
    def execute(case, label, args):
        p = observe(case['name']+':'+label, args)
        case[label] = dict(exit=p.returncode, stdout=p.stdout, stderr=p.stderr); save()
        return p
    save()
    for mode in ['empty', 'retained', 'new_on_old', 'nonzero_checkpoint']:
        case = dict(name=mode, status='INCOMPLETE'); report['cases'].append(case); save()
        db = 'bbcheckpoint_upgrade_r3_'+mode
        sql('postgres','CREATE DATABASE '+db)
        checked('setup:'+db,new+['setup-13',db,'/secrets/postgres','unused'])
        home = '/out/checkpoint-upgrade-r3/'+mode+'/db'; (work/mode).mkdir(mode=0o700)
        if mode in ['retained', 'nonzero_checkpoint']:
            p = execute(case,'old_write',old_command+['event-bindings',db,'/secrets/bigbrotr_control',home])
            assert p.returncode == 0 and not p.stderr
            native = json.loads(checked('native-dump:'+db,new+['dump',home])); case['native'] = native
            wanted, _, _ = signed_expected(2)
            assert [(r['cf'],bytes.fromhex(r['key']),bytes.fromhex(r['value'])) for r in native['rows']] == wanted
            spool = work/mode/'db/producer-inputs-v1'
            inputs = {p.name:p.read_bytes() for p in spool.iterdir()}
            retained = snapshot(db)
            ordered = dict(requests=sorted(retained['producer_requests'],key=lambda r:r['request_sequence']),
                           outcomes=sorted(retained['producer_outcomes'],key=lambda r:r['request_sequence']),
                           bindings=retained['producer_input_bindings'])
            check_originals(ordered,inputs,wanted,3)
            check_all_outcome_copies(ordered,wanted)
            case['inputs'] = {name:dict(bytes=len(value),sha256=hashlib.sha256(value).hexdigest()) for name,value in inputs.items()}
        if mode == 'nonzero_checkpoint':
            # Valid prior SQL metadata, deliberately unsupported by migration14.
            # No native owner/replay is created by this synthetic job update.
            fixture = (SOURCE/'experiments/producer-requests/binding_schema_cases.sql').read_text()
            prefix = fixture[:fixture.index('SELECT pg_temp.job(1001);')]
            release = fixture[fixture.index("UPDATE control.owner_epochs SET state='released'"):]
            sql(db,prefix+"UPDATE control.jobs j SET checkpoint_revision=1,job_revision=j.job_revision+1,owner_resource_id=o.resource_id,owner_epoch=o.epoch,owner_incarnation=o.process_incarnation FROM control.owner_epochs o WHERE o.role='control' AND o.state='active';\n"+release)
        before = snapshot(db); case['before'] = before
        case['schema_before'] = schema_digest(db); save()
        if mode == 'new_on_old':
            p = execute(case,'new_refusal',new+['write',db,'/secrets/bigbrotr_control',home])
            assert p.returncode == 101 and 'Control(IncompatibleDeployment)' in p.stderr
            after = snapshot(db)
            assert not after['producer_requests'] and not after['producer_outcomes']
            assert not after['producer_checkpoints'] and len(after['producer_epochs']) == 1
            assert after['producer_epochs'][0]['reserved_through_request'] == 0
            assert len(after['owner_epochs']) == 2
            changed={'datasets','owner_epochs','role_instances','producer_slots','producer_epochs','source_streams','topology_generations','topology_streams','allocation_domains'}
            assert {k:v for k,v in after.items() if k not in changed} == {k:v for k,v in before.items() if k not in changed}
            assert len(before['datasets'])==len(after['datasets'])==1
            assert before['datasets'][0]['topology_generation'] is None and after['datasets'][0]['topology_generation']==0
            assert {k:v for k,v in before['datasets'][0].items() if k!='topology_generation'} == {k:v for k,v in after['datasets'][0].items() if k!='topology_generation'}
            native = json.loads(checked('native-refused:'+db,new+['dump',home]))
            wanted,_,_ = expected(bytes([1])*16,False)
            assert [(r['cf'],bytes.fromhex(r['key']),bytes.fromhex(r['value'])) for r in native['rows']] == wanted
            assert not (work/mode/'db/producer-inputs-v1').exists()
            case['native'] = native
            assert schema_digest(db) == case['schema_before']
        else:
            p = execute(case,'migration',main_command+['migrate','apply','--file',config(db),'--user','postgres','--password-ref','file:/out/checkpoint-upgrade-r3/migrator-secret','--target','14'])
            after = snapshot(db); case['after'] = after; save()
            if mode == 'nonzero_checkpoint':
                assert p.returncode == 1 and not p.stdout
                assert after == before and schema_digest(db) == case['schema_before']
                assert json.loads(checked('native-dump-after:'+db,new+['dump',home])) == case['native']
                assert {p.name:p.read_bytes() for p in spool.iterdir()} == inputs
                case['rollback_observed'] = True; save()
                # Required concrete recovery diagnostic; the current candidate
                # is expected to expose the missing mapping before correction.
                assert 'migration 14 refuses import checkpoints without typed closure' in p.stderr
            else:
                assert p.returncode == 0 and not p.stderr
                assert json.loads(p.stdout)['status'] == 'applied'
                assert len(after['schema_versions']) == len(before['schema_versions'])+1
                assert not after['import_record_closures'] and not after['import_checkpoints']
                assert {k:v for k,v in after.items() if k not in ['schema_versions','import_record_closures','import_checkpoints']} == {k:v for k,v in before.items() if k != 'schema_versions'}
                migrated_schema = schema_digest(db)
                old_home = '/out/checkpoint-upgrade-r3/'+mode+'/old-refused'
                p = execute(case,'old_refusal',old_command+['write',db,'/secrets/bigbrotr_control',old_home])
                assert p.returncode == 101 and 'IncompatibleDeployment' in p.stderr
                assert snapshot(db) == after and not (work/mode/'old-refused').exists()
                assert schema_digest(db) == migrated_schema
            if mode in ['retained', 'nonzero_checkpoint']:
                assert json.loads(checked('native-dump-after:'+db,new+['dump',home])) == case['native']
                assert {p.name:p.read_bytes() for p in spool.iterdir()} == inputs
        case['after'] = after
        case['schema_after'] = schema_digest(db)
        assert sql(db,"SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid()") == '0'
        case['status'] = 'PASS'; save()
    report['status'] = 'PASS'; save()
    print(json.dumps(dict(status='PASS',cases=len(report['cases']))))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, required=True)
    parser.add_argument('--old-root', type=Path, required=True)
    args = parser.parse_args()
    main(args.root.resolve(strict=True), args.old_root.resolve(strict=True))
