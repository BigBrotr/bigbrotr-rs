#!/usr/bin/env python3
"""Actual installed migrator and synthetic frontier constraints in an owned lab.

Native observation is exercised separately by event_cases --capture-cut. This
matrix does not infer scientific truth from privileged synthetic SQL fixtures.
"""
import argparse
import json
from pathlib import Path
import re
import sys

sys.dont_write_bytecode = True
from signed_publication_cases import command, execute, sha
from lab import change


def main(root, namespace):
    assert not namespace or re.fullmatch('[a-z]{1,12}', namespace)
    prefix = 'bbfrontier_'+(namespace+'_' if namespace else '')
    directory_name = 'frontier-sql'+('-'+namespace if namespace else '')
    source = Path(__file__).resolve().parents[2]
    state = json.loads((root/'resources.json').read_text())
    assert source in (root/'source', root/'harness-source')
    revision = command('git', '-C', str(source), 'rev-parse', 'HEAD')
    assert not command('git', '-C', str(source), 'status', '--porcelain', '--untracked-files=all')
    changed = command('git', '-C', str(source), 'diff', '--name-only', state['source'], revision).splitlines()
    assert all(p.startswith(('docs/', 'tests/fixtures/storage/archive-frontier'))
               or p == 'experiments/archive-engine/frontier_schema_cases.py' for p in changed)
    for key in ['container_id', 'postgres_id']:
        c = json.loads(command('docker', 'inspect', state[key]))[0]
        assert c['State']['Running'] and not c['State']['OOMKilled']
        assert c['Config']['Labels']['org.bigbrotr.publication'] == state['owner']
    ready = json.loads((root/'out/native-tests/ready.json').read_text())
    assert ready['source'] == state['source']
    assert sha(root/'out/native-tests/bigbrotr') == ready['control']
    directory = root/'out'/directory_name
    directory.mkdir(mode=0o700)
    report_path = root/('frontier-schema-cases'+('-'+namespace if namespace else '')+'.json')
    assert not report_path.exists()
    report = dict(status='INCOMPLETE', source=state['source'], harness_source=revision,
                  changed_paths=changed, artifact=ready['control'], cases=[], commands=[],
                  fixture_sha256={p.name: sha(p) for p in sorted((source/'tests/fixtures/storage').glob('archive-frontier*.sql'))},
                  scope='Installed migration and synthetic SQL constraints; native truth is a separate fixture')

    def save():
        report_path.write_text(json.dumps(report, indent=2)+'\n')

    def sql(database, text):
        return command('docker', 'exec', '-i', state['postgres_id'], 'psql', '-XqAt',
                       '-v', 'ON_ERROR_STOP=1', '-U', 'postgres', '-d', database, input=text)

    def migrate(label, target):
        record = dict(case=label, target=target)
        report['commands'].append(record)
        try:
            execute(['docker', 'exec', state['container_id'], '/out/native-tests/bigbrotr',
                     'migrate', 'apply', '--file', '/out/'+directory_name+'/'+label+'/instance.toml',
                     '--target', str(target), '--user', 'postgres', '--password-ref',
                     'file:/out/'+directory_name+'/'+label+'/postgres'], 75, record)
        finally:
            save()
        assert record['exit'] == 0 and not record['stderr'] and not record['overflow']
        result = json.loads(record['stdout'])
        assert result['current_version'] == str(target)
        entries = json.loads((source/'migrations/manifest.json').read_text())['application_order']
        entries = entries[:next(i+1 for i,e in enumerate(entries) if e['version']==target)]
        ledger = json.loads(sql(prefix+label, "SELECT json_agg(json_build_array(migration,name,encode(checksum,'hex')) ORDER BY migration) FROM control.schema_versions"))
        assert ledger == sorted([[e['version'], e['name'], sha(source/'migrations/postgres'/e['name'])] for e in entries])
        record['ledger'] = ledger
        save()

    def fresh(label, target):
        home = directory/label
        home.mkdir(mode=0o700)
        for role in ['postgres', 'bigbrotr_control']:
            p = home/role
            p.write_bytes((root/'secrets'/role).read_bytes())
            p.chmod(0o600)
        text = (source/'config/recipes/archive-only.toml').read_text()
        for section,key,value in [('postgres.control','endpoint','127.0.0.1:5432'),
                                  ('postgres.control','database',prefix+label),
                                  ('postgres.control','password_ref','file:/out/'+directory_name+'/'+label+'/bigbrotr_control')]:
            text = change(text,section,key,value)
        (home/'instance.toml').write_text(text)
        sql('postgres','CREATE DATABASE '+prefix+label)
        migrate(label,target)
        return prefix+label

    def observe(database):
        return json.loads(sql(database, """SELECT json_build_object(
          'owners',(SELECT json_agg(to_jsonb(t)-'archive_frontier_protocol') FROM (SELECT o.*,xmin::text AS xmin FROM control.owner_epochs o)t),
          'revisions',(SELECT json_agg(t) FROM (SELECT r.*,xmin::text AS xmin FROM control.consumer_frontier_revisions r)t),
          'heads',(SELECT json_agg(t) FROM (SELECT c.*,xmin::text AS xmin FROM control.consumers c)t))"""))

    save()
    try:
        for label in ['unbound', 'constraints']:
            database = fresh(label,16)
            fixture = source/'tests/fixtures/storage'/('archive-frontier-'+label+'.sql')
            record = dict(name=label,status='INCOMPLETE')
            report['cases'].append(record)
            try:
                record['output'] = sql(database, fixture.read_text())
                record['rows'] = observe(database)
                assert ('PASS: unbound empty archive frontier refused atomically' in record['output']
                        if label=='unbound' else json.loads(record['output'])['status']=='PASS')
                record['status'] = 'PASS'
            finally:
                save()
        database = fresh('upgrade',15)
        setup = (source/'tests/fixtures/storage/archive-frontier-unbound.sql').read_text().split('DO $$')[0]
        sql(database, setup+"""
        BEGIN;
        INSERT INTO control.consumer_frontier_revisions(consumer_id,frontier_revision,dataset_id,
          consumer_role,topology_generation,owner_resource_id,owner_epoch,owner_incarnation,created_at_us)
          VALUES(pg_temp.id(4),1,pg_temp.id(1),'archive',0,pg_temp.id(4),1,pg_temp.id(5),0);
        UPDATE control.consumer_frontier_revisions SET sealed=true;
        COMMIT;
        """)
        before = observe(database)
        assert len(before['revisions'])==1 and before['revisions'][0]['sealed'] is True
        assert before['heads'][0]['current_revision'] is None
        migrate('upgrade',16)
        assert observe(database)==before, 'migration changed old values or xmin'
        result = sql(database, """DO $$ DECLARE observed text; BEGIN
          BEGIN UPDATE control.consumers SET current_revision=1,current_topology_generation=0;
          EXCEPTION WHEN OTHERS THEN GET STACKED DIAGNOSTICS observed=RETURNED_SQLSTATE; END;
          IF observed IS DISTINCT FROM '23514' THEN RAISE EXCEPTION 'old unbound head accepted or wrong refusal: %',observed; END IF;
        END $$; SELECT 'old unbound head refused';""")
        assert result=='old unbound head refused' and observe(database)==before
        report['cases'].append(dict(name='old-unbound-history-upgrade',status='PASS',rows=before))
        report['status']='PASS'
    finally:
        save()
    print(json.dumps(dict(status=report['status'], report=str(report_path))))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('root', type=Path)
    parser.add_argument('--namespace', default='')
    args = parser.parse_args()
    main(args.root, args.namespace)
