#!/usr/bin/env python3
"""Migration13 against actual migration12 clients and retained original outcomes.

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
    assert (SOURCE/'migrations/postgres/0013_input_bindings.sql').read_bytes() == (root/'source/migrations/postgres/0013_input_bindings.sql').read_bytes()
    runner, pg = state['container_id'], state['postgres_id']
    for identity in [runner, pg]:
        c = json.loads(command('docker', 'inspect', identity))[0]
        assert c['Config']['Labels']['org.bigbrotr.publication'] == state['owner']
        assert c['State']['Running'] and not c['State']['OOMKilled']
    ready = json.loads((root/'out/native-tests/ready.json').read_text())
    old = json.loads((old_root/'out/native-tests/ready.json').read_text())
    assert ready['source'] == state['source'] and ready['status'] == 'READY'
    assert old['source'] == 'd376771c69dd48c88d2defeb128a42e58de53390' and old['status'] == 'READY'
    assert old['ingest_probe'] == '99d66d02af30423345106944afbbec0714a001ba480504775d69844ff2e448ff'
    old_artifact = old_root/'out/native-tests/ingest-bootstrap-probe'
    assert hashlib.sha256(old_artifact.read_bytes()).hexdigest() == old['ingest_probe']
    for path, key in [('bigbrotr', 'control'), ('ingest-bootstrap-probe', 'ingest_probe')]:
        assert hashlib.sha256((root/'out/native-tests'/path).read_bytes()).hexdigest() == ready[key]
    work = root/'out/binding-upgrade'; work.mkdir(mode=0o700)
    shutil.copyfile(old_artifact, work/'old-ingest-probe'); (work/'old-ingest-probe').chmod(0o500)
    assert hashlib.sha256((work/'old-ingest-probe').read_bytes()).hexdigest() == old['ingest_probe']
    credential = work/'migrator-secret'
    credential.write_bytes((root/'secrets/postgres').read_bytes()); credential.chmod(0o400)
    report_path = root/'input-binding-upgrade.json'; assert not report_path.exists()
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
    old_command = ['docker','exec',runner,'/out/binding-upgrade/old-ingest-probe']
    main_command = ['docker','exec',runner,'/out/native-tests/bigbrotr']
    checked('config-init',main_command+['config','init','--recipe','archive-only','--output','/out/binding-upgrade/template.toml'])
    def config(db):
        text = change((work/'template.toml').read_text(),'postgres.control','database',db)
        text = change(text,'postgres.control','endpoint','127.0.0.1:5432')
        text = change(text,'postgres.control.tls','local_boundary','loopback')
        (work/(db+'.toml')).write_text(text)
        return '/out/binding-upgrade/'+db+'.toml'
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
    for mode in ['empty', 'retained', 'new_on_old']:
        case = dict(name=mode, status='INCOMPLETE'); report['cases'].append(case); save()
        db = 'bbbinding_upgrade_'+mode
        sql('postgres','CREATE DATABASE '+db)
        checked('setup:'+db,new+['setup-12',db,'/secrets/postgres','unused'])
        home = '/out/binding-upgrade/'+mode+'/db'; (work/mode).mkdir()
        if mode == 'retained':
            p = execute(case,'old_write',old_command+['write',db,'/secrets/bigbrotr_control',home])
            assert p.returncode == 0 and not p.stderr
            native = json.loads(checked('native-dump:'+db,new+['dump',home])); case['native'] = native
            wanted, _, _ = expected(bytes([1])*16,True)
            assert [(r['cf'],bytes.fromhex(r['key']),bytes.fromhex(r['value'])) for r in native['rows']] == wanted
            inputs = list((work/mode/'db/producer-inputs-v1').iterdir())
            assert len(inputs) == 1
            original = inputs[0].read_bytes()
            row = snapshot(db)['producer_requests'][0]
            assert row['input_bytes'] == len(original) and row['request_digest'] == '\\x'+hashlib.sha256(original).hexdigest()
            case['input'] = dict(name=inputs[0].name, bytes=len(original), sha256=hashlib.sha256(original).hexdigest())
        before = snapshot(db); case['before'] = before
        case['schema_before'] = schema_digest(db); save()
        if mode == 'new_on_old':
            p = execute(case,'new_refusal',new+['write',db,'/secrets/bigbrotr_control',home])
            assert p.returncode == 101 and 'IncompatibleSchema' in p.stderr
            after = snapshot(db)
            assert not after['producer_requests'] and not after['producer_outcomes']
            assert not after['producer_checkpoints'] and len(after['producer_epochs']) == 1
            assert after['producer_epochs'][0]['reserved_through_request'] == 0
            assert len(after['owner_epochs']) == 2
            native = json.loads(checked('native-refused:'+db,new+['dump',home]))
            wanted,_,_ = expected(bytes([1])*16,False)
            assert [(r['cf'],bytes.fromhex(r['key']),bytes.fromhex(r['value'])) for r in native['rows']] == wanted
            assert not list((work/mode/'db').glob('producer-inputs-v1/*.cbor'))
            case['native'] = native
            assert schema_digest(db) == case['schema_before']
        else:
            p = execute(case,'migration',main_command+['migrate','apply','--file',config(db),'--user','postgres','--password-ref','file:/out/binding-upgrade/migrator-secret','--target','13'])
            after = snapshot(db)
            assert p.returncode == 0 and not p.stderr
            assert json.loads(p.stdout)['status'] == 'applied'
            assert len(after['schema_versions']) == len(before['schema_versions'])+1
            assert not after['producer_input_bindings']
            assert {k:v for k,v in after.items() if k not in ['schema_versions','producer_input_bindings']} == {k:v for k,v in before.items() if k != 'schema_versions'}
            migrated_schema = schema_digest(db)
            # Old startup rejects unknown13 before any new owner/native root.
            old_home = '/out/binding-upgrade/'+mode+'/old-refused'
            p = execute(case,'old_refusal',old_command+['write',db,'/secrets/bigbrotr_control',old_home])
            assert p.returncode == 101 and 'IncompatibleDeployment' in p.stderr
            assert snapshot(db) == after and not (work/mode/'old-refused').exists()
            assert schema_digest(db) == migrated_schema
            if mode == 'retained':
                assert json.loads(checked('native-dump-after:'+db,new+['dump',home])) == case['native']
                assert inputs[0].read_bytes() == original
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
