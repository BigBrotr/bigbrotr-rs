"""Actual explicit CLI lifetime, independent native bytes and retained SQL state."""
import hashlib
import json
from pathlib import Path
import re
import shutil
import time
import tomllib
from bootstrap_oracle import cbor, expected
import lab


def set_value(text, section, key, value):
    parsed = tomllib.loads(text)
    table = parsed
    for part in section.split('.'):
        table = table.get(part, {})
    if key in table:
        if isinstance(table[key], list):
            # Generated recipes render role arrays across several lines. The
            # older migration helper intentionally replaces scalar lines only.
            section_match = re.search(r'(?ms)(^\['+re.escape(section)+r'\]\n)(.*?)(?=^\[|\Z)', text)
            assert section_match is not None
            body, count = re.subn(r'(?ms)^'+re.escape(key)+r'\s*=\s*\[.*?\]',
                                  key+' = '+json.dumps(value), section_match[2])
            assert count==1
            return text[:section_match.start()]+section_match[1]+body+text[section_match.end():]
        return lab.change(text, section, key, value)
    marker = '['+section+']\n'
    line = key+' = '+json.dumps(value)+'\n'
    if marker in text:
        return text.replace(marker, marker+line, 1)
    return text+'\n'+marker+line


def policy_setup(pg, database, name, rows, output):
    before = rows(database)
    statements = []
    for table in ['config_revisions', 'policy_revisions']:
        document = before[table][0]['document']
        document['settings']['administration.enabled'] = name == 'policy-admin'
        document['settings']['ingest.producer_outstanding_requests'] = 64 if table == 'config_revisions' else 32
        encoded = json.dumps(document, sort_keys=True, separators=(',', ':'), ensure_ascii=False).encode()
        digest = hashlib.sha256(b'bigbrotr/product-policy/2\0'+encoded).hexdigest()
        statements.append(f"UPDATE control.{table} SET document=convert_from(decode('{encoded.hex()}','hex'),'UTF8')::jsonb, document_digest=decode('{digest}','hex')")
    if name == 'zero-topology':
        statements.extend([
            'INSERT INTO control.topology_generations(dataset_id,generation,created_at_us,sealed) SELECT dataset_id,0,1,true FROM control.datasets',
            'UPDATE control.datasets SET topology_generation=0',
        ])
    lab.sql(pg,database,'BEGIN; SET LOCAL session_replication_role=replica; '+'; '.join(statements)+'; COMMIT;')
    lab.write_json(output/(name+'-fixture.json'), rows(database))


def wait_for(condition, label, seconds=20):
    deadline = time.monotonic()+seconds
    while True:
        value = condition()
        if value:
            return value
        assert time.monotonic() < deadline, label
        time.sleep(.05)


def start(client, config, output, name):
    prefix = output/name
    lab.docker('exec','-d',client,'python3','/lab/installed_client.py',config,'/lab/'+name)
    wait_for(lambda: Path(str(prefix)+'.pid.json').exists(), 'CLI-start')
    return prefix


def messages(prefix):
    path = Path(str(prefix)+'.stdout')
    if not path.exists():
        return []
    # A last partial line is not a message; the next poll reads its completion.
    return [json.loads(line) for line in path.read_text().splitlines(keepends=True) if line.endswith('\n')]


def stop(client, prefix):
    identity = json.loads(Path(str(prefix)+'.pid.json').read_text())
    # PID is scoped to our owned client container. Check incarnation and exact
    # executable/arguments before signalling; never discover a process by name.
    script = '''import os,signal,json
from pathlib import Path
r=json.loads(%r)
p=Path('/proc')/str(r['pid'])
assert p.joinpath('stat').read_text().split()[21]==r['start_ticks']
assert p.joinpath('cmdline').read_bytes().split(b'\\0')[:4]==[b'/build/bigbrotr',b'serve',b'--file',r['config'].encode()]
os.kill(r['pid'],signal.SIGTERM)
''' % json.dumps(identity)
    lab.docker('exec',client,'python3','-c',script)


def terminal(prefix):
    path = Path(str(prefix)+'.terminal.json')
    wait_for(path.exists, 'CLI-reap', 8)
    result = json.loads(path.read_text())
    assert result['reaped'] and not result['forced'], result
    return result


def native_oracle(pg, client, output, database, name, state, report, suffix=''):
    owner = next(r for r in state['owner_epochs'] if r['role']=='ingest')
    assert report['owner_resource_id']==owner['resource_id'][2:]
    dataset = bytes.fromhex(state['datasets'][0]['dataset_id'][2:])
    domains = sorted((r['dictionary_kind'],bytes.fromhex(r['allocation_domain'][2:])) for r in state['allocation_domains'])
    assert [kind for kind,_ in domains] == list(range(1,10))
    assert len(set(value for _,value in domains)) == 9
    assert len(state['producer_requests']) == len(state['producer_outcomes']) == 1
    public = report['public_scope']
    assert public['dataset_id']==dataset.hex() and public['dictionary_kind']==4 and public['ordinal']=='1'
    assert public['allocation_domain']==dict(domains)[4].hex()
    slot = bytes.fromhex(report['producer_slot_id'])
    recovery = bytes.fromhex(report['recovery_generation'])
    assert len(recovery)==16 and any(recovery)
    wanted, receipt, _ = expected(dataset, True, domain=dict(domains)[4],slot=slot,
        owner=bytes.fromhex(owner['resource_id'][2:]),incarnation=bytes.fromhex(owner['process_incarnation'][2:]),
        recovery=recovery,topology=int(report['topology_generation']),namespaces=domains,
        recorded_at=owner['acquired_at_us'])
    dumped = lab.docker('exec',client,'/build/ingest_bootstrap_probe','dump','/lab/'+name+'/store')
    with (output/(name+suffix+'-native.json')).open('x') as capture:
        capture.write(dumped.stdout)
    actual = [(r['cf'],bytes.fromhex(r['key']),bytes.fromhex(r['value'])) for r in json.loads(dumped.stdout)['rows']]
    assert actual==wanted and len(actual)==12
    reference = lambda n: [dataset,4,dict(domains)[4],n]
    entries = [['dictionary',1,[reference(n),1,'visibility',1,{'class':label}]] for n,label in [(1,'public'),(2,'private')]]
    request = b'bigbrotr/producer-request/1\0'+cbor([[slot,1,1],'admit-evidence',1,[1,reference(1),entries]])
    inputs = list((output/name/'store/producer-inputs-v1').glob('*.cbor'))
    assert len(inputs)==1 and inputs[0].read_bytes()==request
    assert state['producer_requests'][0]['request_digest']=='\\x'+hashlib.sha256(request).hexdigest()
    assert state['producer_outcomes'][0]['outcome_bytes']=='\\x'+receipt.hex()
    assert state['producer_epochs'][0]['outstanding_request_limit']==32
    assert state['source_streams'][0]['state']=='open'  # signal is not source closure


def configure(text,before,name):
    for section,key,value in [
        ('topology','dataset_id',before['datasets'][0]['dataset_id'][2:]),
        ('topology','instance_id',before['datasets'][0]['instance_id'][2:]),
        ('topology','local_roles',['control','ingest','archive']),
        ('administration','enabled',False), ('archive.reader','enabled',False),
        ('postgres.control','password_ref','file:/run/secrets/application'),
        ('storage','ingest_root','/lab/'+name+'/store'),
        ('ingest','producer_outstanding_requests',65),
        ('runtime','lease_ttl','6s'),('runtime','heartbeat_interval','1s'),
        ('runtime','renewal_timeout','1s'),('runtime','renewal_safety_margin','1s'),
        ('runtime','graceful_shutdown_timeout','2s'),('runtime','authorization_ttl','5s'),
        ('runtime.processes.application','memory_max_bytes','1GiB'),
        ('runtime.processes.application','baseline_bytes','128MiB'),
        ('runtime.processes.application','payload_bytes','576MiB'),
        ('runtime.processes.application','native_bytes','256MiB'),
        ('runtime.processes.application','completion_reserve_bytes','32MiB'),
    ]:
        text = set_value(text,section,key,value)
    return text


def run(pg,client,output,database,name,rows):
    script = output/'installed_client.py'
    if not script.exists():
        shutil.copy2(Path(__file__).with_name('installed_client.py'),script)
    if name!='registered-only':
        policy_setup(pg,database,name,rows,output)
    before = rows(database)
    lab.write_json(output/(name+'-before.json'),before)
    state = dict(pg,client=client)
    config = lab.config(output,state,database,endpoint='127.0.0.1:5432',boundary='loopback')
    path = output/Path(config).name
    text = path.read_text()
    text = configure(text,before,name)
    path.write_text(text)
    validated = lab.cli(state,'config','validate','--file',config)
    (output/(name+'-validate.stdout')).write_text(validated.stdout)
    (output/(name+'-validate.stderr')).write_text(validated.stderr)
    assert validated.returncode==0, 'initial-config-invalid'
    (output/name).mkdir(mode=0o700)
    if name=='existing-directory':
        (output/name/'store').mkdir()
        (output/name/'store/sentinel').write_bytes(b'original partial state')
    prefix = start(client,config,output,name)
    if name in ['pass','zero-topology','expired']:
        first = wait_for(lambda: messages(prefix) or Path(str(prefix)+'.terminal.json').exists(), 'initialized')
        assert isinstance(first,list) and len(first)==1 and first[0]['state']=='initialized', first
        initialized = first[0]
        assert initialized['ready'] is False and initialized['acquisition']=='not-started'
        initial = rows(database)
        original_deadline = next(r['lease_expires_at_us'] for r in initial['owner_epochs'] if r['role']=='ingest')
        if name=='expired':
            lab.sql(pg,database,"UPDATE control.owner_epochs SET lease_expires_at_us=(EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint-1 WHERE role='ingest'")
        else:
            # Actual server time and persisted renewed deadlines, not a log or
            # mere sleep, establish survival beyond the previous scheduling TTL.
            observed = wait_for(lambda: json.loads(lab.sql(pg,database,
                "SELECT json_build_object('now',(EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint,'leases',(SELECT json_agg(lease_expires_at_us) FROM control.owner_epochs WHERE state='active'))")), 'server-time')
            while observed['now'] <= original_deadline:
                assert not Path(str(prefix)+'.terminal.json').exists(), 'owner-ended-early'
                time.sleep(.1)
                observed = json.loads(lab.sql(pg,database,"SELECT json_build_object('now',(EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint,'leases',(SELECT json_agg(lease_expires_at_us) FROM control.owner_epochs WHERE state='active'))"))
            assert len(observed['leases'])==2 and min(observed['leases'])>observed['now']
            lab.write_json(output/(name+'-renewed.json'),dict(original_deadline=original_deadline,observed=observed))
            stop(client,prefix)
        ended = terminal(prefix)
        assert ended['exit_code']==(4 if name=='expired' else 0), ended
        error = Path(str(prefix)+'.stderr').read_text()
        assert ('Control backend operation failed: Unavailable' in error) if name=='expired' else not error
        final = messages(prefix)
        if name!='expired':
            assert final[-1]['native']=='closed' and final[-1]['owner_release']=='committed'
        after = rows(database)
        if name!='expired':
            control = next(r for r in after['owner_epochs'] if r['role']=='control')
            role = next(r for r in after['role_instances'] if r['role_instance_id']==control['role_instance_id'])
            assert control['state']=='released' and control['released_at_us']>=control['acquired_at_us']
            assert control['exclusion_method']=='storage_fenced' and control['exclusion_digest'] is not None
            assert role['retired_at_us'] is not None
        native_oracle(pg,client,output,database,name,after,initialized)
        if name=='pass':
            retry = lab.cli(state,'serve','--file',config,'--initialize-ingest','--roles','control,ingest')
            (output/'pass-retry.stdout').write_text(retry.stdout)
            (output/'pass-retry.stderr').write_text(retry.stderr)
            assert retry.returncode==3 and 'target exists' in retry.stderr and not retry.stdout
            assert rows(database)==after
            native_oracle(pg,client,output,database,name,after,initialized,suffix='-retry')
        assert len(after['owner_epochs'])==2 and len(after['role_instances'])==2
        assert len(after['topology_streams'])==1 and len(after['source_streams'])==1
        assert len(after['topology_generations'])==(2 if name=='zero-topology' else 1)
        assert after['datasets'][0]['topology_generation']==(1 if name=='zero-topology' else 0)
        changed_tables = {'datasets','owner_epochs','role_instances','topology_generations','topology_streams',
            'source_streams','allocation_domains','producer_slots','producer_epochs','producer_requests','producer_outcomes'}
        for table in before.keys()-changed_tables:
            assert after[table]==before[table], table
    else:
        ended = terminal(prefix)
        assert ended['exit_code']==3 and not messages(prefix)
        reason = {'policy-admin':'Control backend operation failed: InvalidPolicy',
                  'registered-only':'Control backend operation failed: IncompatibleDeployment',
                  'existing-directory':'initial Ingest target exists or cannot be inspected'}[name]
        assert reason in Path(str(prefix)+'.stderr').read_text(), 'wrong-refusal-reason'
        after = rows(database)
        if name=='registered-only':
            for table in before:
                if table not in ['owner_epochs','role_instances']:
                    assert before[table]==after[table], table
            assert len(after['owner_epochs'])==len(before['owner_epochs'])+1
            assert len(after['role_instances'])==len(before['role_instances'])+1
            for table in ['owner_epochs','role_instances']:
                assert all(row in after[table] for row in before[table]), table
            assert [r for r in after['owner_epochs'] if r['role']=='ingest']==[r for r in before['owner_epochs'] if r['role']=='ingest']
            assert not (output/name/'store').exists()
        else:
            assert after==before, 'pre-mutation-refusal'
        if name=='existing-directory':
            assert list((output/name/'store').iterdir())==[output/name/'store/sentinel']
            assert (output/name/'store/sentinel').read_bytes()==b'original partial state'
        if name=='policy-admin':
            assert not (output/name/'store').exists()
    lab.write_json(output/(name+'-after.json'),after)
    assert lab.sql(pg,database,"SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND usename='bigbrotr_control'")=='0'
    return dict(name=name,status='PASS',actual_cli=True,actual_pg_rocks=name in ['pass','zero-topology','expired'],
                control_tables=len(after),terminal=ended)
