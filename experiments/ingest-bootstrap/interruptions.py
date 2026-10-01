"""Real installed native/driver custody, using an independently qualified tracer."""
import hashlib
import json
from pathlib import Path
import shutil
import lab
from installed import configure, messages, native_oracle, policy_setup, set_value, wait_for


INSTRUMENT = {
    'strace-exit-fixed':'509ea4b8a7eb303990155f013e1e288df1054d63e5c81b11bc3c31b5905b7458',
    'result.json':'cf0317fc8934eb70f3cf2d74ac6ba0680be09366a4ade586aabd95991c6c3abc',
}


def prepare_instrument(root, prior, source):
    retained=root.parent/'strace-exit-9b30c35'
    state=json.loads((retained/'resources.json').read_text())
    assert state['source']=='9b30c3535c1407294fb95b40df8019ebda6860f9'
    assert state['owner']=='84ac8cbf5292679b8ef7043fed7a2799' and state['state']=='stopped-retained'
    for entry in state['containers']:
        item=next(c for c in prior if c['Id']==entry['id'])
        assert item['Config']['Labels']['org.bigbrotr.strace-exit']==state['owner']
        assert not item['State']['Running'] and item['State']['Pid']==0 and item['State']['ExitCode']==0
    target=root/'instrument';target.mkdir()
    for name,digest in INSTRUMENT.items():
        assert hashlib.sha256((retained/'out'/name).read_bytes()).hexdigest()==digest
        shutil.copy2(retained/'out'/name,target/name)
        assert hashlib.sha256((target/name).read_bytes()).hexdigest()==digest
    proof=json.loads((target/'result.json').read_text())
    assert proof['status']=='PASS'
    patch=source/'experiments/archive-engine/strace-6.1-exit-delay.patch'
    assert hashlib.sha256(patch.read_bytes()).hexdigest()==proof['source_inputs'][patch.name]
    lab.write_json(root/'instrument.json',dict(source=state['source'],owner=state['owner'],root=str(retained),files=INSTRUMENT))
    return target


def run(pg,client,output,database,name,rows,*,sessions=False):
    script=output/'interruption_client.py'
    if not script.exists():shutil.copy2(Path(__file__).with_name(script.name),script)
    assert not sessions or name == 'close-signal'
    policy_setup(pg,database,name,rows,output,administration=sessions)
    before=rows(database);lab.write_json(output/(name+'-before.json'),before)
    state=dict(pg,client=client)
    config=lab.config(output,state,database,endpoint='127.0.0.1:5432',boundary='loopback')
    path=output/Path(config).name
    if sessions:
        from sessions import configuration
        text=configuration(path.read_text(),before,name)
    else:
        text=configure(path.read_text(),before,name)
    for key,value in [('lease_ttl','30s'),('renewal_timeout','5s')]:text=set_value(text,'runtime',key,value)
    path.write_text(text)
    validated=lab.cli(state,'config','validate','--file',config)
    (output/(name+'-validate.stdout')).write_text(validated.stdout)
    (output/(name+'-validate.stderr')).write_text(validated.stderr)
    assert validated.returncode==0,'interruption-config-invalid'
    competing_config='/lab/'+name+'-competing.toml'
    (output/Path(competing_config).name).write_text(set_value(text,'storage','ingest_root','/lab/'+name+'/competitor'))
    assert lab.cli(state,'config','validate','--file',competing_config).returncode==0
    (output/name).mkdir(mode=0o700)
    prefix=output/name
    arguments = ['--session-http'] if sessions else []
    lab.docker('exec','-d',client,'python3','/lab/interruption_client.py',config,'/lab/'+name,name,*arguments)
    if name!='trace-pass':
        barrier=Path(str(prefix)+'.waiting.json');terminal=Path(str(prefix)+'.terminal.json')
        wait_for(lambda:barrier.exists() or terminal.exists(),'native barrier',10)
        assert barrier.exists(), terminal.read_text() if terminal.exists() else 'missing barrier'
        # Actual backend/session fence while the native thread has not returned.
        observed=json.loads(lab.sql(pg,database,"""
            SELECT json_agg(json_build_object('pid',a.pid,'backend_start',a.backend_start,
                'state',a.state,'granted_advisory',
                (SELECT count(*) FROM pg_locks l WHERE l.pid=a.pid AND l.locktype='advisory' AND l.granted)))
              FROM pg_stat_activity a
             WHERE a.datname=current_database() AND a.usename='bigbrotr_control'
        """))
        assert len(observed)==1 and observed[0]['granted_advisory']>=1
        held_rows=rows(database)
        held_control=next(r for r in held_rows['owner_epochs'] if r['role']=='control')
        assert observed[0]['pid']==held_control['control_backend_pid']
        assert observed[0]['backend_start']==held_control['control_backend_start']
        # Same real initial entrypoint with a distinct absent target reaches
        # Control acquire without opening an unrelated Archive coordinator.
        competitor='''import subprocess,sys
r=subprocess.run(['/build/bigbrotr','serve','--file',sys.argv[1],'--initialize-ingest','--roles','control,ingest'],env={},timeout=.75)
sys.exit(r.returncode)
'''
        conflict=lab.docker('exec',client,'python3','-c',competitor,competing_config,check=False,timeout=3)
        (output/(name+'-competing.stdout')).write_text(conflict.stdout)
        (output/(name+'-competing.stderr')).write_text(conflict.stderr)
        assert conflict.returncode==3 and 'Control backend operation failed: Busy' in conflict.stderr
        assert not (output/name/'competitor').exists() and rows(database)==held_rows
        lab.write_json(output/(name+'-held-sql.json'),dict(backend=observed,rows=held_rows))
        with Path(str(prefix)+'.continue').open('x') as stream:stream.write('observed\n')
    terminal=Path(str(prefix)+'.terminal.json')
    wait_for(terminal.exists,'native terminal',12)
    result=json.loads(terminal.read_text())
    assert result['status']=='PASS' and not result['cleanup']['forced'],result
    after=rows(database);lab.write_json(output/(name+'-after.json'),after)
    assert len(after['owner_epochs'])==len(after['role_instances'])==2
    control=next(r for r in after['owner_epochs'] if r['role']=='control')
    if name=='trace-pass':
        assert control['state']=='released'
    else:
        assert control['state']=='active' and control['released_at_us'] is None and control['exclusion_digest'] is None
        held=json.loads((output/(name+'-held-sql.json')).read_text())['rows']
        assert held==after,'state changed after held native call'
    if name in ['trace-pass','close-signal']:
        native_oracle(pg,client,output,database,name,after,messages(prefix)[0])
    else:
        assert not after['producer_requests'] and not after['producer_outcomes'] and not messages(prefix)
        assert (output/name/'store').is_dir() and not (output/name/'store/CURRENT').exists()
    allowed={'datasets','role_instances','owner_epochs','topology_generations','topology_streams','source_streams','allocation_domains','producer_slots','producer_epochs','producer_requests','producer_outcomes'}
    assert {t for t in before if before[t]!=after[t]}<=allowed
    retry=lab.cli(state,'serve','--file',config,'--initialize-ingest','--roles','control,ingest')
    (output/(name+'-retry.stderr')).write_text(retry.stderr)
    assert retry.returncode==3 and 'target exists' in retry.stderr and not retry.stdout
    assert rows(database)==after
    assert lab.sql(pg,database,"SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND usename='bigbrotr_control'")=='0'
    return dict(name=name,status='PASS',actual_cli=True,actual_native_thread=True,control_tables=len(after),terminal=result)
