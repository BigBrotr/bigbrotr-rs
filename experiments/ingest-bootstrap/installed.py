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


def policy_setup(pg, database, name, rows, output, administration=False):
    before = rows(database)
    statements = []
    for table in ['config_revisions', 'policy_revisions']:
        document = before[table][0]['document']
        document['settings']['administration.enabled'] = administration
        if name == 'archive-export':
            # Explicit fixture policy for the documented native read/export profile.
            # This does not qualify the separately required256MiB acceptance.
            document['settings']['export.memory.max_bytes'] = 1024**3
            document['settings']['read.execution.max_memory_bytes'] = 1024**3
        if name == 'policy-archive':
            document['settings']['archive.reader.enabled'] = True
        document['settings']['ingest.producer_outstanding_requests'] = 64 if table == 'config_revisions' else 32
        if (name == 'window-config' and table == 'config_revisions') or (name == 'window-policy' and table == 'policy_revisions'):
            document['settings']['ingest.producer_outstanding_requests'] = 1
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


def append_private_expected(wanted, dataset, domain, slot, recovery, recorded_at, topology):
    """Extend independently encoded bootstrap rows, never actual native output."""
    # Preserve the original public-envelope two-map request. The new explicit
    # private identity has its own self-contained fact and request2 receipt.
    be = lambda n: n.to_bytes(8, 'big')
    private = [dataset, 4, domain, 3]
    source = [dataset, 0, 1, 3]
    source_key = dataset + (0).to_bytes(4, 'big') + be(1) + be(3)
    reference_key = dataset + (4).to_bytes(2, 'big') + domain + be(3)
    descriptor = [1, 'visibility', 1, {'class':'private'}]
    mapping = [private, *descriptor]
    fact = b'bigbrotr/fact/1\0' + cbor([source, 'dictionary', 1, private,
        recorded_at, mapping])
    digest = hashlib.sha256(fact).digest()
    refs = [[source, digest]]
    private_input = b'bigbrotr/producer-request/1\0' + cbor([[slot,1,2], 'admit-evidence',1,
        [1,private,[['dictionary',1,mapping]]]])
    private_receipt = b'bigbrotr/receipt/1\0' + cbor([1,[slot,1,2],hashlib.sha256(private_input).digest(),
        [dataset,topology,1,recovery,1],refs,refs])
    wanted = {(cf,k):v for cf,k,v in wanted}
    wanted['facts',b'\x01\x20'+source_key] = b'\x01\x20'+digest+fact
    wanted['default',b'\x01\x04'+reference_key] = b'\x01\x04'+source_key+digest
    canonical = cbor(descriptor)
    wanted['default',b'\x01\x05'+dataset+(4).to_bytes(2,'big')+hashlib.sha256(canonical).digest()+reference_key] = b'\x01\x05'+canonical
    wanted['default',b'\x01\x03'+dataset+(4).to_bytes(2,'big')+domain] = b'\x01\x03'+be(3)
    wanted['default',b'\x01\x02'+dataset+(0).to_bytes(4,'big')+be(1)] = b'\x01\x02'+be(3)
    wanted['facts',b'\x01\x21'+source_key] = b'\x01\x21'+be(3)+hashlib.sha256(
        b'bigbrotr/admission-batch/1\0'+cbor([dataset,0,1,3,3,refs])).digest()
    wanted['receipts',b'\x01\x30'+slot+be(1)+be(2)] = private_receipt
    wanted['receipts',b'\x01\x31'+slot+be(1)] = b'\x01\x31'+be(2)+be(0)+be(0)
    wanted = sorted((cf,k,v) for (cf,k),v in wanted.items())
    return wanted, private_input, private_receipt


def native_oracle(pg, client, output, database, name, state, report, suffix='',
                  probe='/build/ingest_bootstrap_probe'):
    owner = next(r for r in state['owner_epochs'] if r['role']=='ingest')
    assert report['owner_resource_id']==owner['resource_id'][2:]
    dataset = bytes.fromhex(state['datasets'][0]['dataset_id'][2:])
    domains = sorted((r['dictionary_kind'],bytes.fromhex(r['allocation_domain'][2:])) for r in state['allocation_domains'])
    assert [kind for kind,_ in domains] == list(range(1,10))
    assert len(set(value for _,value in domains)) == 9
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
    wanted, private_input, private_receipt = append_private_expected(wanted, dataset,
        dict(domains)[4], slot, recovery, owner['acquired_at_us'], int(report['topology_generation']))
    dumped = lab.docker('exec',client,probe,'dump','/lab/'+name+'/store')
    with (output/(name+suffix+'-native.json')).open('x') as capture:
        capture.write(dumped.stdout)
    actual = [(r['cf'],bytes.fromhex(r['key']),bytes.fromhex(r['value'])) for r in json.loads(dumped.stdout)['rows']]
    assert actual==wanted and len(actual)==17, 'complete-original-bootstrap-and-private-append'
    assert report['private_scope']==dict(public,ordinal='3') and report['dictionary_count']==3
    reference = lambda n: [dataset,4,dict(domains)[4],n]
    entries = [['dictionary',1,[reference(n),1,'visibility',1,{'class':label}]] for n,label in [(1,'public'),(2,'private')]]
    request = b'bigbrotr/producer-request/1\0'+cbor([[slot,1,1],'admit-evidence',1,[1,reference(1),entries]])
    inputs = sorted((output/name/'store/producer-inputs-v1').glob('*.cbor'))
    assert len(inputs)==2 and [p.read_bytes() for p in inputs]==[request,private_input]
    requests=sorted(state['producer_requests'],key=lambda r:int(r['request_sequence']))
    outcomes=sorted(state['producer_outcomes'],key=lambda r:int(r['request_sequence']))
    assert len(requests)==len(outcomes)==2
    for sequence,row,original in zip((1,2),requests,(request,private_input)):
        assert int(row['request_sequence'])==sequence and int(row['input_bytes'])==len(original)
        assert row['request_digest']=='\\x'+hashlib.sha256(original).hexdigest()
        assert all(row[k] is None for k in ('relay_endpoint_id','relay_access_scope_id',
            'relay_enrollment_generation','relay_route_id'))
    assert [r['outcome_bytes'] for r in outcomes]==['\\x'+v.hex() for v in (receipt,private_receipt)]
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
    if name == 'window-local':
        text = set_value(text, 'ingest', 'producer_outstanding_requests', 1)
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
    if name in ['pass','zero-topology','expired','window-local']:
        first = wait_for(lambda: messages(prefix) or Path(str(prefix)+'.terminal.json').exists(), 'initialized')
        if not isinstance(first,list):
            # Retain the full refusal census even when the expected initialization
            # fails, so the discriminating baseline is independently inspectable.
            lab.write_json(output/(name+'-after.json'),rows(database))
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
        lab.write_json(output/(name+'-after.json'),after)
        native_oracle(pg,client,output,database,name,after,initialized)
        if name in ('pass','window-local'):
            retry = lab.cli(state,'serve','--file',config,'--initialize-ingest','--roles','control,ingest')
            (output/(name+'-retry.stdout')).write_text(retry.stdout)
            (output/(name+'-retry.stderr')).write_text(retry.stderr)
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
        reason = {'policy-archive':'Control backend operation failed: InvalidPolicy',
                  'registered-only':'Control backend operation failed: IncompatibleDeployment',
                  'existing-directory':'initial Ingest target exists or cannot be inspected',
                  'window-config':'Control backend operation failed: InvalidPolicy',
                  'window-policy':'Control backend operation failed: InvalidPolicy'}[name]
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
        if name in ('policy-archive','window-config','window-policy'):
            assert not (output/name/'store').exists()
    lab.write_json(output/(name+'-after.json'),after)
    assert lab.sql(pg,database,"SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND usename='bigbrotr_control'")=='0'
    return dict(name=name,status='PASS',actual_cli=True,actual_pg_rocks=name in ['pass','zero-topology','expired','window-local'],
                control_tables=len(after),terminal=ended)
