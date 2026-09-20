#!/usr/bin/env python3
"""Exact installed pin migration, legacy preservation and native consumer oracle.

Run after event_cases --capture-cut in a separately inventoried trusted lab.
SQL-only cases establish constraints, never native scientific truth or delivery.
"""
import argparse
import json
from pathlib import Path
import sys

sys.dont_write_bytecode = True
from signed_publication_cases import command, execute, sha
from lab import change


def main(root):
    source = Path(__file__).resolve().parents[2]
    state = json.loads((root/'resources.json').read_text())
    assert source == root/'source'
    assert command('git', '-C', str(source), 'rev-parse', 'HEAD') == state['source']
    assert not command('git', '-C', str(source), 'status', '--porcelain', '--untracked-files=all')
    for key in ['container_id', 'postgres_id']:
        container = json.loads(command('docker', 'inspect', state[key]))[0]
        assert container['State']['Running'] and not container['State']['OOMKilled']
        assert container['Config']['Labels']['org.bigbrotr.publication'] == state['owner']
    ready = json.loads((root/'out/native-tests/ready.json').read_text())
    assert ready['source'] == state['source']
    assert sha(root/'out/native-tests/bigbrotr') == ready['control']
    directory = root/'out/pin-schema'
    directory.mkdir(mode=0o700)
    report_path = root/'pin-schema-cases.json'
    assert not report_path.exists()
    report = dict(status='INCOMPLETE', source=state['source'], artifact=ready['control'],
                  fixtures={p.name: sha(p) for p in (source/'tests/fixtures/storage').glob('archive-pin*.sql')},
                  commands=[], cases=[])

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
                     'migrate', 'apply', '--file', '/out/pin-schema/'+label+'/instance.toml',
                     '--target', str(target), '--user', 'postgres', '--password-ref',
                     'file:/out/pin-schema/'+label+'/postgres'], 75, record)
        finally:
            save()
        assert record['exit'] == 0 and not record['stderr'] and not record['overflow']
        assert json.loads(record['stdout'])['current_version'] == str(target)
        entries = json.loads((source/'migrations/manifest.json').read_text())['application_order']
        entries = entries[:next(i+1 for i, entry in enumerate(entries) if entry['version'] == target)]
        ledger = json.loads(sql('bbpin_'+label, "SELECT json_agg(json_build_array(migration,name,encode(checksum,'hex')) ORDER BY migration) FROM control.schema_versions"))
        assert ledger == sorted([[e['version'], e['name'], sha(source/'migrations/postgres'/e['name'])] for e in entries])
        record['ledger'] = ledger
        save()

    def fresh(label, target):
        home = directory/label
        home.mkdir(mode=0o700)
        secret = home/'postgres'
        secret.write_bytes((root/'secrets/postgres').read_bytes())
        secret.chmod(0o600)
        text = (source/'config/recipes/archive-only.toml').read_text()
        for key, value in [('endpoint', '127.0.0.1:5432'), ('database', 'bbpin_'+label)]:
            text = change(text, 'postgres.control', key, value)
        (home/'instance.toml').write_text(text)
        sql('postgres', 'CREATE DATABASE bbpin_'+label)
        migrate(label, target)
        return 'bbpin_'+label

    setup = (source/'tests/fixtures/storage/archive-frontier-constraints.sql').read_text()
    save()
    try:
        database = fresh('constraints', 17)
        text = setup+(source/'tests/fixtures/storage/archive-pin-unqualified.sql').read_text()
        text += (source/'tests/fixtures/storage/archive-pin-constraints.sql').read_text()
        record = dict(name='qualified-empty-pin-constraints', status='INCOMPLETE')
        report['cases'].append(record)
        try:
            record['output'] = sql(database, text)
        finally:
            save()
        observed = json.loads(record['output'].splitlines()[-1])
        expected = {'pin_missing_binding','pin_wrong_incarnation','pin_wrong_frontier',
                    'pin_empty_cut_with_retired_publisher','pin_binding_mutable','pin_binding_delete',
                    'pin_binding_truncate','pin_lease_truncate','pin_claim_truncate',
                    'pin_protocol_removed','pin_stale_revision','pin_live_renewal',
                    'pin_released_cannot_renew','pin_expired_control'}
        assert {c['name'] for c in observed['cases'] if c['name'].startswith('pin_')} == expected
        assert observed['status'] == 'PASS'
        record['status'] = 'PASS'
        database = fresh('legacy', 16)
        sql(database, setup+"""
          INSERT INTO control.snapshot_leases(lease_id,dataset_id,consumer_id,frontier_revision,
            topology_generation,holder_kind,holder_role_instance_id,holder_role,holder_incarnation,
            acquired_at_us,updated_at_us,expires_at_us)
          VALUES(pg_temp.id(301),pg_temp.id(1),pg_temp.id(9),1,0,'role_instance',pg_temp.id(3),
            'control',pg_temp.id(6),pg_temp.now_us(),pg_temp.now_us(),pg_temp.now_us()+30000000);
        """)
        def legacy_rows():
            return json.loads(sql(database, "SELECT json_agg(to_jsonb(t)-'archive_pin_protocol') FROM (SELECT *,xmin::text AS xmin FROM control.snapshot_leases)t"))
        before = legacy_rows()
        assert len(before) == 1 and before[0]['state'] == 'active'
        migrate('legacy', 17)
        assert legacy_rows() == before
        assert sql(database, 'SELECT archive_pin_protocol IS NULL FROM control.snapshot_leases') == 't'
        result = sql(database, """DO $$ DECLARE observed text; BEGIN
          BEGIN UPDATE control.snapshot_leases SET archive_pin_protocol=1,lease_revision=lease_revision+1,
            updated_at_us=updated_at_us+1,expires_at_us=expires_at_us+1;
          EXCEPTION WHEN OTHERS THEN GET STACKED DIAGNOSTICS observed=RETURNED_SQLSTATE; END;
          IF observed IS DISTINCT FROM '23514' THEN RAISE EXCEPTION 'legacy adoption accepted: %',observed; END IF;
        END $$; SELECT 'legacy adoption refused';""")
        assert result == 'legacy adoption refused' and legacy_rows() == before
        report['cases'].append(dict(name='legacy-empty-lease-unchanged-not-adopted', status='PASS', rows=before))

        native = json.loads((root/'cut-cases.json').read_text())
        assert native['source'] == state['source'] and native['status'] == 'PASS'
        events = [json.loads(line) for line in native['history_publisher']['stdout'].splitlines()]
        histories = [event for event in events if event.get('event') == 'pin-history']
        assert len(histories) == 1
        history = histories[0]
        assert history['registered'] == history['retried'], 'retry must preserve values and xmin'
        assert history['released'] == history['released_retry'], 'retry must not resurrect or rewrite'
        before, after = history['registered'], history['released']
        assert set(before) == set(after) == {'leases', 'bindings', 'claims'}
        assert all(len(rows) == 1 for rows in list(before.values())+list(after.values()))
        assert before['bindings'] == after['bindings'], 'original issuer and request binding are immutable'
        lease, binding, claim = (before[key][0] for key in ['leases', 'bindings', 'claims'])
        assert lease['lease_id'] == binding['lease_id'] == claim['lease_id'] == '\\x'+'f2'*16
        assert lease['consumer_id'] == binding['consumer_id'] == '\\x'+'5a'*16
        assert lease['dataset_id'] == binding['dataset_id'] == claim['dataset_id']
        assert lease['holder_incarnation'] == binding['issuer_incarnation']
        assert lease['frontier_revision'] == binding['frontier_revision'] == 1
        assert [claim[k] for k in ['shard_id','owner_epoch','first_sequence','last_sequence','purpose']] == [0,1,1,10,'reader']
        assert lease['state'] == claim['state'] == 'active' and lease['lease_revision'] == 1
        assert lease['archive_pin_protocol'] == 1 and binding['requested_ttl_us'] == 10000000
        assert lease['released_at_us'] is None and claim['released_at_us'] is None
        assert lease['expires_at_us'] > lease['acquired_at_us']
        for table, mutable in [('leases', {'lease_revision','state','updated_at_us','released_at_us','release_reason','xmin'}),
                               ('claims', {'state','released_at_us','release_reason','xmin'})]:
            old, new = before[table][0], after[table][0]
            assert {k:v for k,v in old.items() if k not in mutable} == {k:v for k,v in new.items() if k not in mutable}
            assert new['state'] == 'released' and new['release_reason'] == 'reader_joined'
            assert new['released_at_us'] >= lease['acquired_at_us'] and new['xmin'] != old['xmin']
        assert after['leases'][0]['lease_revision'] == 2
        assert after['leases'][0]['updated_at_us'] == after['leases'][0]['released_at_us']
        final = {}
        for key, table in [('leases','snapshot_leases'), ('bindings','archive_snapshot_pins'),
                           ('claims','retention_claims')]:
            final[key] = json.loads(sql('bbpub_control_history',
                'SELECT json_agg(t) FROM (SELECT *,xmin::text AS xmin FROM control.'+table+')t'))
        assert final == after, 'independent final database must match the retained release observation'
        observed = json.loads(sql('bbpub_control_history', """SELECT json_build_object(
          'lease',(SELECT json_build_array(encode(l.lease_id,'hex'),encode(l.consumer_id,'hex'),
            l.frontier_revision,l.topology_generation,l.holder_kind,l.holder_role,l.lease_revision,
            l.state,l.archive_pin_protocol,p.purpose,p.requested_ttl_us,f.snapshot_id,
            l.acquired_at_us<=l.updated_at_us,l.expires_at_us>l.acquired_at_us,
            l.expires_at_us-l.acquired_at_us<=p.requested_ttl_us,
            l.holder_incarnation=p.issuer_incarnation,l.holder_role_instance_id=o.role_instance_id)
            FROM control.snapshot_leases l JOIN control.archive_snapshot_pins p USING(lease_id)
            JOIN control.archive_frontier_snapshots f ON f.consumer_id=p.consumer_id AND f.frontier_revision=p.frontier_revision
            JOIN control.owner_epochs o ON o.resource_id=p.issuer_resource AND o.epoch=p.issuer_epoch),
          'claims',(SELECT json_agg(json_build_array(shard_id,owner_epoch,first_sequence,last_sequence,purpose,state))
            FROM control.retention_claims));"""))
        assert observed == dict(lease=['f2'*16,'5a'*16,1,0,'role_instance','control',2,
            'released',1,'reader',10000000,3,True,True,True,True,True],
            claims=[[0,1,1,10,'reader','released']]), 'actual native reader must retain and release exact old source prefix'
        report['cases'].append(dict(name='native-history-pin-durable-effects', status='PASS', rows=observed,
                                   retry_states=history))
        report['status'] = 'PASS'
    finally:
        save()
    print(json.dumps(dict(status=report['status'], source=state['source'], cases=len(report['cases']))))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('root', type=Path)
    main(parser.parse_args().root.resolve())
