#!/usr/bin/env python3
"""Owned Linux/PG qualification of blocked history delivery and finite authority.

Only the isolated network namespace has bounded tcp_wmem. The actual application,
native reader and PostgreSQL run normally; SQL shortens only newly issued fixture
pins, preserving all production triggers and the originally requested lifetime.
"""
import hashlib
import json
from pathlib import Path
import sys
import time
sys.dont_write_bytecode = True
from signed_publication_cases import command, event, execute, send, sha, Process
from signed_publication_oracle import corpus
from event_cases import cut_digest
from private_history_body_client import large_event


PROCESS = """import json,os,sys
from pathlib import Path
pid,port=map(int,sys.argv[1:]); home=Path('/proc',str(pid)); children=[]
stat=(home/'stat').read_text(); fields=stat[stat.rfind(')')+2:].split()
for p in Path('/proc').iterdir():
 if not p.name.isdigit():continue
 try:
  raw=(p/'stat').read_text(); f=raw[raw.rfind(')')+2:].split()
  if int(f[1])==pid:children.append(dict(pid=int(p.name),state=f[0]))
 except (FileNotFoundError,ProcessLookupError):pass
owned={os.readlink(fd) for fd in (home/'fd').iterdir()}
sockets=[]
for line in Path('/proc/net/tcp').read_text().splitlines()[1:]:
 f=line.split()
 if f[1]=='0100007F:46A0' and f[2]==f'0100007F:{port:04X}':
  tx,rx=(int(v,16) for v in f[4].split(':'))
  sockets.append(dict(state=f[3],tx=tx,rx=rx,inode=f[9],owned=f'socket:[{f[9]}]' in owned))
print(json.dumps(dict(pid=pid,state=fields[0],start_ticks=int(fields[19]),children=children,sockets=sockets)))
"""


def main(root):
    resources = json.loads((root / 'resources.json').read_text())
    source = root / 'source'
    assert Path(__file__).resolve().parents[2] == source
    assert command('git', '-C', str(source), 'rev-parse', 'HEAD') == resources['source']
    assert not command('git', '-C', str(source), 'status', '--porcelain', '--untracked-files=all')
    runner, pg = resources['container_id'], resources['postgres_id']
    for identity in [runner, pg]:
        item = json.loads(command('docker', 'inspect', identity))[0]
        assert item['Config']['Labels'][resources['owner_label']] == resources['owner']
        assert item['State']['Running'] and not item['State']['OOMKilled']
    ready = json.loads((root / 'out/native-tests/ready.json').read_text())
    for key, name in [('control','native-tests/bigbrotr'), ('worker','archive-worker/bigbrotr-archive-worker'),
                      ('publisher','native-tests/archive-publication-probe')]:
        assert sha(root / 'out' / name) == ready[key]
    result = dict(source=resources['source'], tree=resources['tree'], status='INCOMPLETE', cases=[])
    output = root / 'private-history-body-cases.json'
    assert not output.exists()
    def save(): output.write_text(json.dumps(result, indent=2) + '\n')
    def sql(database, query):
        return command('docker', 'exec', '-i', pg, 'psql', '-XAtq', '-v', 'ON_ERROR_STOP=1', '-U', 'postgres', '-d', database, input=query)
    def control(query): return json.loads(sql('bbpub_control_large', query))
    def counts():
        return control("SELECT json_build_object('pins',(SELECT count(*) FROM control.snapshot_leases WHERE state='active'),'claims',(SELECT count(*) FROM control.retention_claims WHERE state='active'),'controls',(SELECT count(*) FROM control.owner_epochs WHERE role='control' AND state='active'))")
    def leases():
        return control("SELECT json_build_object('now_us',(EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint,'owner',(SELECT json_build_object('epoch',epoch,'expires',lease_expires_at_us) FROM control.owner_epochs WHERE role='control' AND state='active'),'pins',(SELECT json_agg(json_build_object('id',encode(l.lease_id,'hex'),'state',l.state,'acquired',l.acquired_at_us,'expires',l.expires_at_us,'released',l.released_at_us,'requested',p.requested_ttl_us)) FROM control.snapshot_leases l JOIN control.archive_snapshot_pins p USING(lease_id) WHERE l.state='active'))")
    def files():
        return {str(p.relative_to(root / 'out')):sha(p) for p in sorted((root / 'out/signed_large').rglob('*.parquet'))}
    policy = json.loads((source / 'experiments/archive-engine/receipt-policy.json').read_text())
    policy['settings'].update({'administration.enabled':True, 'read.execution.max_memory_bytes':1073741824})
    def accept(revision, ttl):
        assert counts() == dict(pins=0, claims=0, controls=0)
        document = json.dumps({**policy,'settings':{**policy['settings'],'read.authorization.lease_ttl':ttl}}, sort_keys=True, separators=(',', ':'))
        digest = hashlib.sha256(b'bigbrotr/product-policy/1\0' + document.encode()).hexdigest()
        sql('bbpub_control_large', f"BEGIN; INSERT INTO control.policy_revisions(revision,dataset_id,previous_revision,config_revision,schema_version,document,document_digest,accepted_at_us,actor_id) SELECT {revision},dataset_id,{revision-1},config_revision,schema_version,$policy${document}$policy$::jsonb,decode('{digest}','hex'),accepted_at_us+1,actor_id FROM control.policy_revisions WHERE revision={revision-1}; UPDATE control.datasets SET policy_revision={revision}; COMMIT;")
    native = ['docker', 'exec', '--env', 'LD_LIBRARY_PATH=/out/archive-worker/lib', runner]
    script = '/out/build/source/experiments/archive-engine/private_history_body_client.py'
    try:
        result['tcp_wmem'] = command('docker', 'exec', runner, 'cat', '/proc/sys/net/ipv4/tcp_wmem')
        assert result['tcp_wmem'].split() == ['4096', '8192', '16384']
        for database in ['bbpub_large', 'bbpub_control_large']:
            sql('postgres', f'CREATE DATABASE {database};')
            sql(database, f'REVOKE ALL ON DATABASE {database} FROM PUBLIC; REVOKE ALL ON SCHEMA public FROM PUBLIC;')
        sql('bbpub_control_large', 'GRANT CONNECT ON DATABASE bbpub_control_large TO bigbrotr_control;')
        publisher = result['publisher'] = {}
        execute(native + ['/out/native-tests/archive-publication-probe', 'large', '5432', '--capture-cut'], 180, publisher); save()
        assert publisher['exit'] == 0 and not publisher['stderr'] and not publisher['overflow']
        values = [json.loads(line) for line in publisher['stdout'].splitlines()]
        batches, _ = corpus(large_event(source))
        assert next(v['batches'] for v in values if v['event']=='retained-inputs') == batches
        receipts = []
        for ordinal in range(1, 5):
            batch = batches[0 if ordinal == 2 else 1]
            receipts.append(dict(dataset_id='01'*16, schema_version=1, topology_generation='0',
                batch_digest=batch['archive_digest'], record_count=batch['last']-batch['first']+1,
                input_ranges=[dict(shard_id=0, owner_epoch=1, first_sequence=batch['first'], last_sequence=batch['last'])],
                attempt_id=[180+7*8+ordinal]*16, publication_ordinal=ordinal))
        assert [v['expected'] for v in values if v['event']=='inputs-reopened'] == receipts
        read = dict(dataset_id='01'*16, topology_generation='0', bounds=[dict(shard_id=0,owner_epoch=1,complete_through=10)],
                    receipts=receipts[1:3], snapshot_id=3)
        expected = dict(event='captured-cut', observation=dict(format='bigbrotr-native-cut/1', snapshot_id=3,
            request_digest=cut_digest(read), family_counts=[6,1,1,1,1]), read=read,
            frontier=dict(operation_id='f1'*16, revision=1, snapshot_id=3, request_digest=cut_digest(read)), pin='not-granted')
        assert [v for v in values if v['event']=='captured-cut'] == [expected]
        result['files_before'] = files()
        assert len(result['files_before']) == 8
        client = ['docker', 'exec', runner, 'python3', '-B', '/out/build/source/experiments/archive-engine/private_grant_http.py']
        command(*client, 'prepare-large')
        config = root / 'out/private-grant-http/instance.toml'
        # Fixture-only faster heartbeat supplies actual renewals during the
        # blocked body. The unchanged60s owner lease still has ample safety margin.
        config.write_text(config.read_text() + '\n[runtime]\nheartbeat_interval="500ms"\n')
        sql('bbpub_control_large', 'GRANT SELECT,INSERT,UPDATE ON ALL TABLES IN SCHEMA control TO bigbrotr_control; GRANT DELETE ON control.browser_sessions,control.reauthentication_proofs TO bigbrotr_control; GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA control TO bigbrotr_control;')
        accept(2, 5000000)
        command(*client, 'enroll')
        assert json.loads(command('docker', 'exec', runner, 'python3', '-B', script, 'setup')) == dict(event='setup',status='PASS')
        for revision, mode in [(2,'pass'), (3,'action'), (4,'pin')]:
            if revision > 2: accept(revision, 2000000 if mode=='action' else 5000000)
            if mode == 'pin':
                sql('bbpub_control_large', "CREATE FUNCTION control.fixture_short_pin() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN NEW.expires_at_us:=least(NEW.expires_at_us,(EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint+2000000); RETURN NEW; END $$; CREATE TRIGGER aa_fixture_short_pin BEFORE INSERT ON control.snapshot_leases FOR EACH ROW EXECUTE FUNCTION control.fixture_short_pin();")
            record = dict(case='history-blocked-body-' + mode, status='INCOMPLETE')
            result['cases'].append(record); save()
            process = None
            try:
                process = Process(['docker', 'exec', '-i', runner, 'python3', '-B', script, mode, str(revision)], input_pipe=True)
                header = record['headers'] = event(process, 'headers', timeout=15)
                assert header['header_ms'] < 1500
                record['blocked'] = []
                for _ in range(2):
                    observed = json.loads(command('docker','exec',runner,'python3','-c',PROCESS,str(header['parent_pid']),str(header['client_port'])))
                    record['blocked'].append(observed); save()
                    assert observed['state'] not in ('Z','X') and not observed['children']
                    assert len(observed['sockets']) == 1
                    s = observed['sockets'][0]
                    assert s['state']=='01' and s['owned'] and 0 < s['tx'] < header['content_length']
                    assert counts() == dict(pins=1, claims=1, controls=1)
                    time.sleep(.1)
                before = record['authority_before'] = leases(); save()
                assert len(before['pins']) == 1 and before['pins'][0]['expires'] > before['now_us']
                # Same schedule in all cases, earlier than the ordinary5s HTTP
                # ceiling. No reading until the host observes this exact barrier.
                delay = 3 - (time.monotonic_ns()-header['started_ns']) / 1e9
                assert delay > 0
                time.sleep(delay)
                after = record['authority_before_drain'] = leases(); save()
                assert after['owner']['epoch'] == before['owner']['epoch']
                assert after['owner']['expires'] > before['owner']['expires'] + 500000
                assert after['owner']['expires'] > after['now_us']
                assert after['pins'] == before['pins'], 'expiry alone must not release the retained pin'
                assert counts() == dict(pins=1, claims=1, controls=1)
                pin = after['pins'][0]
                if mode == 'pin':
                    assert 1000000 < pin['expires']-pin['acquired'] <= 2000000
                    assert pin['expires'] < after['now_us'] and pin['requested'] > 3000000
                else:
                    assert pin['expires'] > after['now_us'] + 500000
                send(process, 'drain\n')
                record['healthy'] = event(process, 'healthy', timeout=8); save()
                assert counts() == dict(pins=0, claims=0, controls=1)
                send(process, 'stop\n')
                record['terminal'] = event(process, 'terminal')
                assert process.wait(5)==0 and not process.stderr and not process.overflow
                assert counts() == dict(pins=0, claims=0, controls=0)
                record['files_unchanged'] = files() == result['files_before']
                assert record['files_unchanged']
                record['status'] = 'PASS'; save()
            finally:
                if process is not None: process.stop()
        sql('bbpub_control_large', 'DROP TRIGGER aa_fixture_short_pin ON control.snapshot_leases; DROP FUNCTION control.fixture_short_pin();')
        result['terminal_backends'] = {}
        for database in ['bbpub_large','bbpub_control_large']:
            count = int(sql(database, 'SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid();'))
            result['terminal_backends'][database] = count
            assert count == 0
        result['status'] = 'PASS'
    except BaseException as error:
        result['failure'] = type(error).__name__
        raise
    finally:
        save()
    print(json.dumps(dict(status=result['status'], cases=len(result['cases']))))


if __name__ == '__main__': main(Path(sys.argv[1]).resolve())
