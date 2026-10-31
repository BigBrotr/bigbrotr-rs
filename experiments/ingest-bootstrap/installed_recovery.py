"""Installed positive recovery: independent complete retained-state comparisons."""
import hashlib
import json
from pathlib import Path
import shutil
import lab
from installed import run as initialize, set_value


def run(pg, client, output, database, name, rows, clone):
    assert name in ('positive-recovery','positive-recovery-interruptions')
    initialize(pg, client, output, database, 'window-local', rows)
    original = rows(database)
    cases = []

    def dump(label, seed):
        result = lab.docker('exec', client, '/build/ingest_bootstrap_probe', 'dump', '/lab/'+seed+'/store')
        assert result.returncode == 0 and not result.stderr
        (output/(label+'-native.json')).write_text(result.stdout)
        return json.loads(result.stdout)

    base_native = dump('recovery-base', 'window-local')
    assert len(original) == 69 and len(base_native['rows']) == 17
    original_inputs = {p.name:p.read_bytes() for p in (output/'window-local/store/producer-inputs-v1').iterdir()}
    assert len(original_inputs) == 2

    def prepare(label):
        db = clone(label, database)
        shutil.copytree(output/'window-local', output/label)
        state = dict(pg, client=client)
        config = lab.config(output, state, db, endpoint='127.0.0.1:5432', boundary='loopback')
        text = (output/Path(config).name).read_text()
        for section, key, value in [
            ('topology','dataset_id',original['datasets'][0]['dataset_id'][2:]),
            ('topology','instance_id',original['datasets'][0]['instance_id'][2:]),
            ('topology','local_roles',['control','ingest','archive']),
            ('postgres.control','password_ref','file:/run/secrets/application'),
            ('storage','ingest_root','/lab/'+label+'/store'),
            ('runtime','lease_ttl','6s'), ('runtime','renewal_timeout','1s'),
            ('runtime','heartbeat_interval','1s'), ('runtime','renewal_safety_margin','1s'),
            ('runtime','authorization_ttl','5s'), ('runtime','graceful_shutdown_timeout','2s'),
            ('runtime.processes.application','memory_max_bytes','1536MiB'),
            ('runtime.processes.application','baseline_bytes','128MiB'),
            ('runtime.processes.application','payload_bytes','1GiB'),
            ('runtime.processes.application','native_bytes','256MiB'),
            ('runtime.processes.application','completion_reserve_bytes','32MiB'),
        ]:
            text = set_value(text, section, key, value)
        (output/Path(config).name).write_text(text)
        valid = lab.cli(state, 'config','validate','--file',config)
        assert valid.returncode == 0, valid.stderr
        return db, config

    def invoke(db, config, label, seed, error=None, closure=True, copied=False, held=False, blocked_output=False):
        before = rows(db)
        native = dump(label+'-before', seed)
        inputs = {p.name:p.read_bytes() for p in (output/seed/'store/producer-inputs-v1').iterdir()}
        lab.write_json(output/(label+'-before.json'), before)
        if held:
            # Both processes use the actual RocksDB binding. Hold exclusion until
            # the attempted CLI has terminated; there is no fabricated lock file.
            script = '''import json,subprocess
hold=subprocess.Popen(['/build/ingest_bootstrap_probe','history-hold',%r,
 '/run/secrets/application',%r,%r,%r],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
try:
 assert json.loads(hold.stdout.readline())=={'event':'native-held'}
 result=subprocess.run(['/build/bigbrotr','ingest','recover','--file',%r],capture_output=True,timeout=10)
 print(json.dumps(dict(returncode=result.returncode,stdout=result.stdout.decode(),stderr=result.stderr.decode())))
finally:
 out,err=hold.communicate(b'release\\n',timeout=5)
 assert hold.returncode==0 and not out and not err
''' % (db, '/lab/'+seed+'/store', before['datasets'][0]['dataset_id'][2:],
        before['datasets'][0]['instance_id'][2:], config)
            result = lab.docker('exec',client,'python3','-c',script,check=False,timeout=20)
            assert result.returncode == 0 and not result.stderr, result.stderr
            captured = json.loads(result.stdout)
        elif blocked_output:
            script = '''import fcntl,json,os,subprocess,time
read,write=os.pipe()
fcntl.fcntl(write,fcntl.F_SETFL,os.O_NONBLOCK)
filled=0
try:
 while True: filled+=os.write(write,b'x'*4096)
except BlockingIOError: pass
fcntl.fcntl(write,fcntl.F_SETFL,0)
started=time.monotonic()
result=subprocess.run(['/build/bigbrotr','ingest','recover','--file',%r],stdout=write,stderr=subprocess.PIPE,timeout=8)
elapsed=time.monotonic()-started
os.close(write)
data=b''
while chunk:=os.read(read,65536): data+=chunk
os.close(read)
assert data==b'x'*filled and filled>0
print(json.dumps(dict(returncode=result.returncode,stdout='',stderr=result.stderr.decode(),
 filled_bytes=filled,elapsed_seconds=elapsed)))
''' % config
            result = lab.docker('exec',client,'python3','-c',script,check=False,timeout=12)
            assert result.returncode==0 and not result.stderr, result.stderr
            captured = json.loads(result.stdout)
        else:
            result = lab.cli(dict(pg,client=client),'ingest','recover','--file',config)
            captured = dict(returncode=result.returncode,stdout=result.stdout,stderr=result.stderr)
        lab.write_json(output/(label+'-response.json'), captured)
        after = rows(db)
        final = dump(label+'-after', seed)
        lab.write_json(output/(label+'-after.json'), after)
        assert inputs == {p.name:p.read_bytes() for p in (output/seed/'store/producer-inputs-v1').iterdir()}
        # Independently change only the fixed native producer barrier encoding.
        wanted = json.loads(json.dumps(native))
        if closure:
            marker, = [r for r in wanted['rows'] if r['cf']=='receipts' and r['key'].startswith('0131')]
            raw = bytes.fromhex(marker['value'])
            assert raw[:2] in (b'\x01\x31',b'\x02\x31')
            marker['value'] = (b'\x02\x31'+raw[2:26]+b'\x01').hex()
        assert final == wanted, (label, 'native effects')
        if error:
            assert captured['returncode'] in (3,4) and not captured['stdout'] and error in captured['stderr'], captured
        elif blocked_output:
            assert captured['returncode']==1 and not captured['stdout'] and not captured['stderr'], captured
            assert captured['elapsed_seconds']<8
        else:
            assert captured['returncode'] == 0 and not captured['stderr'], captured
            actual, = [json.loads(line) for line in captured['stdout'].splitlines()]
            owner, = [r for r in before['owner_epochs'] if r['role']=='ingest']
            producer, = before['producer_epochs']
            assert actual == dict(format='bigbrotr-ingest-recovery/1',state='positive-outcomes-resolved',
                dataset_id=before['datasets'][0]['dataset_id'][2:],owner_resource_id=owner['resource_id'][2:],
                producer_slot_id=producer['producer_slot_id'][2:],producer_epoch='1',issued_through='2',
                reserved_through='2',resolved_requests='2',native='closed',owner_release='committed',ready=False), actual
        changed = {'owner_epochs','role_instances','producer_epochs'} if closure else set()
        if copied: changed.add('producer_outcomes')
        for table in before.keys()-changed:
            assert before[table] == after[table], (label, table)
        if closure:
            old, new = dict(before['producer_epochs'][0]), dict(after['producer_epochs'][0])
            assert new.pop('state') == 'closing'
            old_state = old.pop('state')
            if old_state == 'closing': assert old['xmin'] == new['xmin']
            old.pop('xmin'); new.pop('xmin')
            assert old == new
            assert all(row in after['owner_epochs'] for row in before['owner_epochs'])
            assert all(row in after['role_instances'] for row in before['role_instances'])
            assert len(after['owner_epochs']) == len(before['owner_epochs'])+1
            assert len(after['role_instances']) == len(before['role_instances'])+1
            current, = [r for r in after['owner_epochs'] if r not in before['owner_epochs']]
            assert current['role']=='control' and current['epoch']==max(r['epoch'] for r in before['owner_epochs'] if r['role']=='control')+1
            assert current['state']==('active' if error else 'released')
            if copied:
                assert all(row in after['producer_outcomes'] for row in before['producer_outcomes'])
                added, = [r for r in after['producer_outcomes'] if r not in before['producer_outcomes']]
                wanted = dict(next(r for r in original['producer_outcomes'] if r['request_sequence']==2))
                for key in ('recorded_at_us','recorder_resource','recorder_epoch','recorder_incarnation','xmin'):
                    wanted[key] = added[key]
                assert added == wanted
                assert added['recorder_epoch']==current['epoch']
                assert added['recorder_incarnation']==current['process_incarnation']
        assert lab.sql(pg,db,"SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND usename='bigbrotr_control'") == '0'
        cases.append(dict(name=label,status='PASS',expected=error or 'positive-outcomes-resolved',control_tables=len(after),native_rows=len(final['rows'])))

    if name == 'positive-recovery':
        db, config = prepare('recovery_pass')
        invoke(db,config,'recovery-pass','recovery_pass')
        invoke(db,config,'recovery-retry','recovery_pass')
        db, config = prepare('recovery_copy')
        lab.sql(pg,db,'BEGIN; SET LOCAL session_replication_role=replica; DELETE FROM control.producer_outcomes WHERE request_sequence=2; COMMIT;')
        invoke(db,config,'recovery-copy','recovery_copy',copied=True)
        db, config = prepare('recovery_pending')
        lab.sql(pg,db,"""BEGIN; SET LOCAL session_replication_role=replica;
            UPDATE control.producer_epochs SET reserved_through_request=3;
            INSERT INTO control.producer_requests SELECT (jsonb_populate_record(NULL::control.producer_requests,
                to_jsonb(r)||jsonb_build_object('request_sequence',3))).* FROM control.producer_requests r WHERE request_sequence=2;
            COMMIT;""")
        invoke(db,config,'recovery-pending','recovery_pending',error='Unresolved')
        db, config = prepare('recovery_hole')
        lab.sql(pg,db,'BEGIN; SET LOCAL session_replication_role=replica; DELETE FROM control.producer_requests WHERE request_sequence=1; COMMIT;')
        invoke(db,config,'recovery-hole','recovery_hole',error='Unresolved')
        db, config = prepare('recovery_input')
        original_file = sorted((output/'recovery_input/store/producer-inputs-v1').iterdir())[1]
        original_file.rename(output/'recovery-missing-input-original.cbor')
        invoke(db,config,'recovery-input','recovery_input',error='Input(')
        db, config = prepare('recovery_held')
        invoke(db,config,'recovery-held','recovery_held',error='exclusive open failed',closure=False,held=True)
        db, config = prepare('recovery_policy')
        lab.sql(pg,db,"BEGIN; SET LOCAL session_replication_role=replica; UPDATE control.policy_revisions SET document_digest=decode(repeat('ae',32),'hex'); COMMIT;")
        invoke(db,config,'recovery-policy','recovery_policy',error='InvalidPolicy',closure=False)
        db, config = prepare('recovery_heads')
        statements = []
        for table in ['config_revisions','policy_revisions']:
            document = json.loads(json.dumps(original[table][0]['document']))
            document['settings']['ingest.producer_outstanding_requests'] = 1
            encoded = json.dumps(document,sort_keys=True,separators=(',',':'),ensure_ascii=False).encode()
            patch = dict(revision=2,previous_revision=1,document=document,
                document_digest='\\x'+hashlib.sha256(b'bigbrotr/product-policy/3\0'+encoded).hexdigest())
            if table=='policy_revisions': patch['config_revision']=2
            encoded_patch = json.dumps(patch,separators=(',',':')).encode().hex()
            statements.append(f"INSERT INTO control.{table} SELECT (jsonb_populate_record(NULL::control.{table},to_jsonb(t)||convert_from(decode('{encoded_patch}','hex'),'UTF8')::jsonb)).* FROM control.{table} t WHERE revision=1")
        statements.append('UPDATE control.datasets SET config_revision=2,policy_revision=2')
        lab.sql(pg,db,'BEGIN; SET LOCAL session_replication_role=replica; '+';'.join(statements)+'; COMMIT;')
        invoke(db,config,'recovery-heads','recovery_heads')
    else:
        db, config = prepare('recovery_stdout')
        invoke(db,config,'recovery-stdout','recovery_stdout',blocked_output=True)
        from recovery_interruptions import run as interruptions
        cases.extend(interruptions(pg,client,output,rows,prepare,dump))
    assert rows(database) == original and dump('recovery-base-after','window-local') == base_native
    assert original_inputs == {p.name:p.read_bytes() for p in (output/'window-local/store/producer-inputs-v1').iterdir()}
    return dict(name=name,status='PASS',cases=cases,scope='installed positive recovery; source closure and successor remain separate')
