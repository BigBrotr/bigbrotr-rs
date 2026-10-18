"""Genuine initial -> terminal3 -> Archive catch-up -> read/export/restart.

One newly initialized physical root and catalog survive every process. No SQL,
RocksDB or Archive copy substitutes for a live owner or coordinator identity.
Execution belongs to the isolated acquisition-scope runner, never import time.
"""
import copy
import json
from pathlib import Path
import secrets
import shutil
import sys
import time

SOURCE = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(SOURCE / 'experiments/ingest-succession'))
import lab
from installed import messages, set_value, wait_for
from installed_archive import catalog_rows, digest, files, invoke, global_roles
from installed_empty import exact_absence, signal_child
from initial_history import capture, inputs_at, observe_backend_absence, server_clock
from installed_archive_catch_up_client import argv

STATUS = 'PASS_INSTALLED_ARCHIVE_CATCH_UP_JOURNEY'
STEPS = ('abandon1','retire1','live2','abandon2','retire2','live3',
         'source3','abandon3','retire3','catchup','catchup-retry','ordinary','ordinary-restart')


def read(path):
    return json.loads(path.read_text())


def upgrade(pg, client, output, database, name, rows):
    """Explicit setup before any actual owner or physical coordinator exists."""
    from initial_live import FUNCTIONS
    from activated_retirement_oracle import GRANTS
    before = rows(database)
    assert not before['owner_epochs'] and not before['producer_requests']
    config = lab.config(output, dict(pg,client=client), database, endpoint='127.0.0.1:5432',boundary='loopback')
    observations=[]
    for action in ('plan','apply','apply'):
        answer=lab.cli(dict(pg,client=client),'migrate',action,'--file',config,'--user','bigbrotr_migrator',
            '--password-ref','file:/run/secrets/migrator','--target','38')
        observations.append(dict(action=action,returncode=answer.returncode,stdout=answer.stdout,stderr=answer.stderr))
        lab.write_json(output/(name+'-lineage-migrations.json'),observations)
        assert answer.returncode==0 and not answer.stderr
        if len(observations)==2: after=rows(database)
    assert rows(database)==after and len(after)==75
    assert all(after[t]==before[t] for t in before if t!='schema_versions')
    assert all(not after[t] for t in after.keys()-before.keys())
    assert all(r in after['schema_versions'] for r in before['schema_versions'])
    added=[r for r in after['schema_versions'] if r not in before['schema_versions']]
    assert {r['migration'] for r in added}==set(range(27,39))
    for entry in added:
        path=SOURCE/'migrations/postgres'/entry['name']
        assert entry['checksum']=='\\x'+digest(path)
    statements=[(SOURCE/'experiments/ingest-succession/schema_grants.sql').read_text(),
        'GRANT SELECT,INSERT,UPDATE ON control.initial_ingest_successions TO bigbrotr_control;',
        'GRANT SELECT,INSERT ON control.initial_ingest_activations,control.acquisition_gaps TO bigbrotr_control;',
        'GRANT UPDATE ON control.producer_epochs TO bigbrotr_control;',
        'GRANT INSERT ON control.producer_outcomes TO bigbrotr_control;',
        'GRANT EXECUTE ON FUNCTION '+','.join('control.'+f for f in FUNCTIONS)+' TO bigbrotr_control;',
        'GRANT EXECUTE ON FUNCTION control.guard_acquisition_gap(),control.require_acquisition_gap_complete(),'
        'control.require_initial_retirement(bytea,numeric,bytea),control.require_activated_initial_recovery(bytea),'
        'control.require_activated_initial_source(bytea) TO bigbrotr_control;',*GRANTS]
    granted=[]
    for statement in statements:
        value=lab.sql(pg,database,statement); granted.append(dict(sql=statement,stdout=value))
        lab.write_json(output/(name+'-lineage-grants.json'),granted)
        assert value==''
    assert rows(database)==after
    lab.write_json(output/(name+'-lineage-upgrade.json'),dict(before=before,after=after,grant_statements=statements))


def physical(home):
    result={}
    for name in ('store','data','state','scratch'):
        path=home/name; st=path.lstat()
        assert path.is_dir() and not path.is_symlink()
        result[name]=dict(device=st.st_dev,inode=st.st_ino,mode=st.st_mode,files=files(path))
    return result


def dump(client, output, label, home):
    answer,_=capture(client,output,label+'-dump','dump-state','/lab/'+home.name+'/store')
    assert answer.returncode==0 and not answer.stderr
    value=json.loads(answer.stdout); lab.write_json(output/(label+'-native.json'),value)
    return value


def snapshot(pg,client,output,label,database,rows,setup,native=True):
    value=dict(control=rows(database),inputs=inputs_at(setup['home']/'store'),
        catalog=catalog_rows(pg,setup['resource']['catalog_database']),system_roles=global_roles(pg),physical=physical(setup['home']))
    lab.write_json(output/(label+'-state.json'),value)
    if native: value['native']=dump(client,output,label,setup['home'])
    value['contents']={p.name:p.read_bytes().hex() for p in sorted((setup['home']/'store/producer-inputs-v1').glob('*.cbor'))}
    value['retained']={str(p.relative_to(setup['home'])):p.read_bytes().hex() for p in
        [setup['home']/'data/.bigbrotr-initialization/intent.json',*sorted((setup['home']/'state').glob('*.inputs'))]}
    assert all(len(bytes.fromhex(raw))<=1024*1024 for raw in value['retained'].values())
    value['physical']=physical(setup['home'])
    lab.write_json(output/(label+'-state.json'),value)
    return value


def response(prefix):
    result=dict(returncode=None,stdout='',stderr='',outcome='OUTCOME_UNKNOWN',terminal=None)
    for stream in ('stdout','stderr'):
        p=Path(str(prefix)+'.'+stream)
        if p.exists():
            raw=p.read_bytes(); assert len(raw)<=131072
            result[stream]=raw.decode()
    p=Path(str(prefix)+'.terminal.json')
    if p.exists():
        terminal=read(p); result['terminal']=terminal
        assert type(terminal) is dict and type(terminal['reaped']) is bool and type(terminal['forced']) is bool
        assert terminal['exit_code'] is None or type(terminal['exit_code']) is int
        if terminal['reaped'] and not terminal['forced']:
            result.update(returncode=terminal['exit_code'],outcome='PROCESS_EXITED')
    return result


def process_closed(answer,operation,config,prefix):
    assert answer['outcome']=='PROCESS_EXITED' and answer['returncode']==0 and not answer['stderr']
    terminal=answer['terminal']
    assert terminal['command']==argv(operation,Path(config)) and terminal['prefix']=='/lab/'+prefix.name
    assert terminal['config']==config and terminal['reaped'] is True and terminal['forced'] is False
    assert type(terminal['pid']) is int and terminal['pid']>0 and type(terminal['start_ticks']) is int and terminal['start_ticks']>0
    reports=[json.loads(s) for s in answer['stdout'].splitlines()]
    last=reports[-1]
    # Every installed success here is delivered only after the actual owned
    # runtime's native/worker close, Control release and parent driver join.
    if operation == 'catchup':
        assert len(reports)==1 and last['format']=='bigbrotr-archive-catch-up/1'
    elif not operation.startswith('ordinary'):
        assert last['native']=='closed' and last['owner_release']=='committed'
        assert last['ready'] is False
    if operation.startswith('live') or operation.startswith('ordinary'):
        assert len(reports)==2 and last['state']=='stopped'
    else: assert len(reports)==1
    return reports


def execute(pg,client,output,database,rows,setup,step,before,action=None):
    name='catch-up-'+step; prefix=output/name; operation='catchup' if step=='catchup-retry' else step
    config_text=(output/Path(setup['config']).name).read_text()
    (output/(name+'-config.toml')).write_text(config_text)
    actual=snapshot(pg,client,output,name+'-before',database,rows,setup)
    assert {k:v for k,v in actual.items() if k!='physical'}=={k:v for k,v in before.items() if k!='physical'}, 'phase boundary changed before dispatch'
    assert all({k:v for k,v in actual['physical'][n].items() if k!='files'}=={k:v for k,v in before['physical'][n].items() if k!='files'} for n in actual['physical']), 'physical root identity changed'
    before=actual
    window=dict(before_us=server_clock(pg,database)); fixture=dict(errors=[],snapshots=[])
    answer=dict(returncode=None,stdout='',stderr='',outcome='OUTCOME_UNKNOWN',terminal=None)
    failure=None; after=None; closed=False; backend={}
    def observe(stage):
        value=dict(stage=stage,before_us=server_clock(pg,database),control=rows(database),
            inputs=inputs_at(setup['home']/'store'))
        value['after_us']=server_clock(pg,database)
        fixture['snapshots'].append(value)
        lab.write_json(output/(name+'-observations.json'),fixture['snapshots'])
        return value
    try:
        lab.docker('exec','-d',client,'python3','-B','/lab/installed_archive_catch_up_client.py',
            operation,setup['config'],'/lab/'+name,timeout=5)
        wait_for(lambda:Path(str(prefix)+'.pid.json').exists() or Path(str(prefix)+'.terminal.json').exists(),name+' identity',5)
        if operation.startswith('live') or operation.startswith('ordinary'):
            first=wait_for(lambda:messages(prefix) or Path(str(prefix)+'.terminal.json').exists(),name+' report',20)
            assert type(first) is list and len(first)==1
            initial=observe('started'); fixture['initial_report']=first[0]
            if action: action(prefix,observe,fixture,first[0])
            else:
                owners=[r for r in initial['control']['owner_epochs'] if r['state']=='active' and r['role'] in ('control','ingest')]
                initial_deadline=max(r['lease_expires_at_us'] for r in owners)
                wait_for(lambda:server_clock(pg,database)>initial_deadline,name+' renewal',15)
                fixture['initial_deadline_us']=initial_deadline
                observe('renewed'); observe('before-stop')
                signal_child(client,read(Path(str(prefix)+'.pid.json')))
        wait_for(Path(str(prefix)+'.terminal.json').exists,name+' actual join',110 if operation=='catchup' else 55)
    except BaseException as error:
        failure=error
    finally:
        try:
            if not Path(str(prefix)+'.terminal.json').exists() and Path(str(prefix)+'.pid.json').exists():
                signal_child(client,read(Path(str(prefix)+'.pid.json')))
                wait_for(Path(str(prefix)+'.terminal.json').exists,name+' cleanup join',5)
        except BaseException as error: fixture['errors'].append(dict(stage='join',error=repr(error)))
        try:
            answer=response(prefix); process_closed(answer,operation,setup['config'],prefix); closed=True
        except BaseException as error: fixture['errors'].append(dict(stage='custody',error=repr(error)))
        try: window['after_us']=server_clock(pg,database)
        except BaseException as error: fixture['errors'].append(dict(stage='clock',error=repr(error)))
        for key,value in [('response',answer),('clock',window),('fixture',fixture)]: lab.write_json(output/(name+'-'+key+'.json'),value)
        # Preserve SQL/input/physical observations even if the process wrapper
        # was malformed or transport failed. These never authorize Rocks open.
        try: snapshot(pg,client,output,name+'-provisional',database,rows,setup,native=False)
        except BaseException as error: fixture['errors'].append(dict(stage='provisional',error=repr(error)))
        try:
            observe_backend_absence(pg,database,output,name+'-control')
            current=rows(database)
            old={(r['resource_id'],r['epoch']) for r in before['control']['owner_epochs']}
            expected=[dict(pid=r['control_backend_pid'],backend_start=r['control_backend_start'],database=database)
                for r in current['owner_epochs'] if r['role']=='control' and (r['resource_id'],r['epoch']) not in old]
            assert len(expected)==1
            backend['exact']=exact_absence(pg,database,expected)
            backend['all']={db:lab.sql(pg,db,'SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid()')
                for db in (database,setup['resource']['catalog_database'])}
            assert all(v=='0' for v in backend['all'].values())
            if closed and fixture.get('client_joined',True): after=snapshot(pg,client,output,name+'-after',database,rows,setup)
        except BaseException as error: fixture['errors'].append(dict(stage='external',error=repr(error)))
        lab.write_json(output/(name+'-backends.json'),backend)
        lab.write_json(output/(name+'-fixture.json'),fixture)
        lab.write_json(output/(name+'-custody.json'),dict(status='OBSERVED_CLOSED_AND_JOINED' if closed and after is not None else 'UNKNOWN',errors=fixture['errors']))
    if failure: raise failure
    assert closed and after is not None and not fixture['errors']
    value=dict(step=step,operation=operation,database=database,config=setup['config'],config_text=config_text,before=before,after=after,
        response=answer,window=window,fixture=fixture,backend=backend)
    lab.write_json(output/(name+'-verification.json'),value)
    return value


def run(pg,client,output,database,name,rows,*,continuation=None,**context):
    from installed_archive_journey import run as initial
    assert name=='archive-export'
    built=context['built']; build=Path(context['build'])
    qualification=read(build.parent/'qualification.json')
    assert qualification['status']=='PASS_ARCHIVE_CATCH_UP_INSTALLED_COMPONENTS'
    assert qualification['source']==built['source']
    try:
        return initial(pg,client,output,database,name,rows,setup_upgrade=upgrade,
            preserve_diagnostics=True,
            continuation=lambda *args: (continuation or continue_journey)(pg,client,output,database,rows,*args),**context)
    except BaseException as error:
        # This boundary is inside bindings.run: its owned PostgreSQL is still
        # alive. Capture SQL only; neither process closure nor native custody is
        # inferred from a failed CLI or a successful observer query.
        try:
            failure_diagnostics(pg,output,database,name,rows,context['resources'],error)
        except BaseException as capture_error:
            error.add_note('Archive failure diagnostics unavailable: '+type(capture_error).__name__)
        raise


def failure_diagnostics(pg,output,database,name,rows,resources,original):
    report=dict(format='bigbrotr-archive-failure-diagnostics/1',
        original_error_type=type(original).__name__,database=database,
        process_custody='NOT_ASSERTED',native_opened=False,captures=[],errors=[])
    def save():
        try:
            target=output/(name+'-failure-diagnostics.json')
            partial=target.with_suffix('.partial')
            lab.write_json(partial,report); partial.replace(target)
        except BaseException as error:
            if len(report['errors'])<8:
                report['errors'].append(dict(stage='report',error_type=type(error).__name__))
    def capture_sql(label,read):
        entry=dict(stage=label,started_monotonic_ns=time.monotonic_ns(),status='UNKNOWN')
        report['captures'].append(entry); save()
        try:
            value=read()
            entry['completed_monotonic_ns']=time.monotonic_ns()
            target=output/(name+'-failure-'+label+'.json')
            partial=target.with_suffix('.partial')
            lab.write_json(partial,value); partial.replace(target)
            entry.update(status='CAPTURED',file=target.name)
        except BaseException as error:
            entry.update(status='FAILED',error_type=type(error).__name__)
        save()
    capture_sql('control',lambda:rows(database))
    try:
        selected=[r for r in resources.get('archive_resources',[])
            if r['control_database']==database and r['home']==str(output/name)]
        assert len(selected)==1, 'exact Archive resource unavailable'
        catalog=selected[0]['catalog_database']
        assert catalog==database+'_cat', 'unexpected Archive catalog'
    except BaseException as error:
        report['errors'].append(dict(stage='catalog-identity',error_type=type(error).__name__))
        save()
    else:
        report['catalog_database']=catalog
        capture_sql('catalog',lambda:catalog_rows(pg,catalog))


def continue_journey(pg,client,output,database,rows,setup,initialized,initial_after,wanted,initial_result):
    from installed_archive_catch_up_oracle import verify, negative_controls
    home=setup['home']; cfg=output/Path(setup['config']).name
    for folder,filename in [('ingest-bootstrap','installed_archive_catch_up_client.py'),
        ('ingest-succession','installed_family_client.py')]:
        shutil.copy2(SOURCE/'experiments'/folder/filename,output/filename)
    baseline=snapshot(pg,client,output,'catch-up-initial',database,rows,setup)
    assert baseline['control']==initial_after
    value=dict(status='RUNNING',database=database,config=setup['config'],initial=initial_result,
        initialized=initialized,setup_resource=setup['resource'],initial_setup={k:setup[k] for k in ('control','catalog','resource')},initial_state=baseline,
        initial_science={k:wanted[k] for k in ('tables','batches')},
        initial_event_hex=(home/'event.jsonl').read_bytes().hex(),
        initial_transcript=[read(p) for p in sorted(home.glob('relay-closed-*.json'))],steps=[])
    def save(): lab.write_json(output/'archive-catch-up-journey.json',value)
    save(); before=baseline
    try:
        for step in STEPS:
            action=None
            if step=='live3': action=family_action(pg,client,output,database,rows,setup)
            if step=='ordinary':
                value['read_setup']=prepare_read(pg,client,output,database,rows,setup,initialized,before); save()
            if step in ('ordinary','ordinary-restart'): action=read_action(client,output,setup,step)
            if step=='live3':
                from installed_family_replay import ENDPOINT
                text=cfg.read_text()
                # Local transport authorization only. Accepted scientific policy
                # remains the original revision1 (including provenance=true).
                rules='[{endpoint="'+ENDPOINT+'",cidr="127.0.0.1/32",port=7447}]'
                text=set_value(text,'network.private','rules',[]).replace('rules = []','rules = '+rules)
                cfg.write_text(text)
                invoke(client,output,'catch-up-live3-config','config','validate','--file',setup['config'])
            phase=execute(pg,client,output,database,rows,setup,step,before,action)
            if step in ('catchup','catchup-retry','ordinary','ordinary-restart'):
                result=lab.docker('exec',client,'python3','-B','/lab/installed_archive_read.py',setup['config'],
                    '/lab/'+home.name+'/census-'+step,'--full-stream-order',check=False,timeout=40)
                phase['readback']=dict(returncode=result.returncode,stdout=result.stdout,stderr=result.stderr)
                lab.write_json(output/('catch-up-'+step+'-readback.json'),phase['readback'])
                assert result.returncode==0 and not result.stderr
                assert rows(database)==phase['after']['control']
                assert catalog_rows(pg,setup['resource']['catalog_database'])==phase['after']['catalog']
                assert physical(home)['data']==phase['after']['physical']['data']
                assert physical(home)['state']==phase['after']['physical']['state']
            if step in ('ordinary','ordinary-restart'):
                phase['artifact_files']=files(home/'destination')
                phase['artifact_contents']={str(p.relative_to(home/'destination')):p.read_bytes().hex()
                    for p in sorted((home/'destination').rglob('*')) if p.is_file()}
                lab.write_json(output/('catch-up-'+step+'-artifacts.json'),dict(files=phase['artifact_files'],contents=phase['artifact_contents']))
            value['steps'].append(phase); save(); before=phase['after']
        value['oracle']=verify(value)
        value['negative_controls']=negative_controls(value)
        value['status']='PASS'; save()
        return dict(name='archive-export',status='PASS',scope='same physical root initial-to-terminal3 Archive/read/export/restart',
            installed_processes=14,scientific_records=22,archive_receipts=7,final_snapshot=8,
            oracle=value['oracle'],negative_controls=value['negative_controls'],initial=initial_result)
    except BaseException as error:
        value.update(status='FAIL',error=repr(error)); save(); raise


def family_action(pg,client,output,database,rows,setup):
    from live_replay import OLD_EVENT,NEW_EVENT
    directory=output/'catch-up-network'; directory.mkdir(mode=0o700)
    ds=rows(database)['datasets'][0]
    scope=next(r for r in rows(database)['allocation_domains'] if r['owner_epoch']==1 and r['dictionary_kind']==4)
    lab.write_json(directory/'fixture.json',dict(config_revision=str(ds['config_revision']),policy_revision=str(ds['policy_revision']),
        visibility=dict(dataset_id=ds['dataset_id'][2:],dictionary_kind=4,allocation_domain=scope['allocation_domain'][2:],ordinal='3')))
    lab.write_json(directory/'events.json',[OLD_EVENT,NEW_EVENT])
    def action(prefix,observe,fixture,report):
        from installed_live import stop_quiet
        wrapper="""import subprocess,sys,json
from pathlib import Path
p=Path(sys.argv[1])
with (p/'client.stdout').open('xb') as out,(p/'client.stderr').open('xb') as err:
 r=subprocess.run(['python3','-B','/lab/installed_family_client.py','journey',str(p)],stdout=out,stderr=err,timeout=50)
(p/'client-exit.json').write_text(json.dumps(dict(exit_code=r.returncode,reaped=True,outcome='PROCESS_EXITED')))
"""
        fixture['client_joined']=False
        try:
            lab.docker('exec','-d',client,'python3','-c',wrapper,'/lab/'+directory.name)
            for stage in ('prepared','submitted','commands-finished'):
                wait_for(lambda:(directory/(stage+'.json')).exists() or (directory/'client-exit.json').exists(),stage,25)
                assert (directory/(stage+'.json')).exists()
                if stage=='submitted':
                    wait_for(lambda:len([r for r in rows(database)['producer_outcomes'] if r['producer_epoch']==3])==3,'three real successor requests',25)
                observe(stage)
                if stage!='commands-finished': (directory/(stage+'.continue')).write_text('observed\n')
            deadline=int(report['lease_expires_at_us'])
            wait_for(lambda:server_clock(pg,database)>deadline,'new owner renewed',15)
            fixture['initial_deadline_us']=deadline; observe('renewed'); observe('before-stop')
            fixture['signal']=stop_quiet(client,directory,read(Path(str(prefix)+'.pid.json')),deadline)
            wait_for(Path(str(prefix)+'.terminal.json').exists,'new owner stopped',8)
            (directory/'commands-finished.continue').write_text('owner joined\n')
            wait_for((directory/'client-exit.json').exists,'finite relay client joined',5)
            fixture['journey']=dict(result=read(directory/'client-result.json'),terminal=read(directory/'client-exit.json'),
                opened=[read(p) for p in sorted(directory.glob('relay-open-*.json'))],
                closed=[read(p) for p in sorted(directory.glob('relay-closed-*.json'))])
            assert fixture['journey']['terminal']==dict(exit_code=0,reaped=True,outcome='PROCESS_EXITED')
            assert not (directory/'client.stderr').read_text() and not (directory/'client.stdout').read_text()
        finally:
            try:
                # Gates only release the finite transport client; they grant no
                # native or Control authority. A timeout remains unknown custody.
                for stage in ('prepared','submitted','commands-finished'):
                    path=directory/(stage+'.continue')
                    if not path.exists(): path.write_text('cleanup observation\n')
                wait_for((directory/'client-exit.json').exists,'transport cleanup join',8)
                terminal=read(directory/'client-exit.json')
                assert type(terminal) is dict and terminal.get('reaped') is True
                assert terminal.get('outcome')=='PROCESS_EXITED' and type(terminal.get('exit_code')) is int
                fixture['client_terminal']=terminal
                fixture['client_joined']=True
            except BaseException as error:
                fixture['errors'].append(dict(stage='transport-custody',error=repr(error)))
    return action


def prepare_read(pg,client,output,database,rows,setup,initialized,before):
    home=setup['home']; destination=home/'destination'; destination.mkdir(mode=0o700)
    # Login plus three real reauthentications consume four attempts. Observe the
    # previous fixed 60s window expire naturally; no SQL timing rewrite is used.
    expiry=max(row['window_started_at_us']+60_000_000 for row in before['control']['login_buckets'])
    started=time.monotonic(); observations=[]
    while True:
        now=server_clock(pg,database); observations.append(dict(server_us=now,elapsed_seconds=time.monotonic()-started))
        lab.write_json(output/'catch-up-login-window.json',dict(expires_at_us=expiry,observations=observations))
        if now>=expiry: break
        assert time.monotonic()-started<65
        time.sleep(min(1,max(.01,(expiry-now)/1_000_000)))
    assert rows(database)==before['control']
    destination_id=secrets.token_hex(16); path=output/Path(setup['config']).name
    text=path.read_text()
    for section,key,v in [('archive.reader','enabled',True),('export.destination','id',destination_id),
        ('export.destination','root','/lab/'+home.name+'/destination')]: text=set_value(text,section,key,v)
    path.write_text(text)
    invoke(client,output,'catch-up-read-config','config','validate','--file',setup['config'])
    marker=invoke(client,output,'catch-up-destination','export','destination-initialize','--file',setup['config'])
    assert marker['binding_sha256']==digest(destination/'destination.json')
    assert rows(database)==before['control']
    shown=invoke(client,output,'catch-up-read-settings','config','show','--file',setup['config'])
    reserve=int(shown['settings']['runtime.disk_completion_reserve_bytes']['value'])
    capacity=dict(reserve=reserve,growth=577*1024**2,free=shutil.disk_usage(home).free)
    lab.write_json(output/'catch-up-read-capacity.json',capacity)
    assert reserve==64*1024**3 and capacity['free']>=reserve+capacity['growth']
    from live_replay import OLD_EVENT,NEW_EVENT
    lab.write_json(home/'events.json',[OLD_EVENT,NEW_EVENT])
    consumer,=before['control']['consumers']
    ds,=before['control']['datasets']
    selection=dict(scope=initialized['private_scope'],catalog_id=setup['resource']['catalog_id'],
        frontier_revision=consumer['current_revision'],destination_id=destination_id,binding_sha256=marker['binding_sha256'],
        config_revision=str(ds['config_revision']),policy_revision=str(ds['policy_revision']))
    lab.write_json(home/'read-input.json',selection)
    return dict(selection=selection,capacity=capacity,login_window=dict(expires_at_us=expiry,observations=observations),
        marker=marker,config=path.read_text())


def read_action(client,output,setup,step):
    def action(prefix,observe,fixture,report):
        mode='read-restart' if step=='ordinary-restart' else 'read'
        answer=lab.docker('exec',client,'python3','-B','/lab/installed_archive_catch_up_client.py',mode,
            '/lab/'+setup['home'].name,check=False,timeout=40)
        fixture['http_response']=dict(returncode=answer.returncode,stdout=answer.stdout,stderr=answer.stderr)
        lab.write_json(output/('catch-up-'+step+'-http-response.json'),fixture['http_response'])
        observe('http-complete')
        assert answer.returncode==0 and not answer.stderr and not answer.stdout
        label='restart' if step=='ordinary-restart' else 'read'
        fixture['http']=read(setup['home']/(label+'-observations.json'))
        fixture['http_result']=read(setup['home']/(label+'-client-result.json'))
        observe('before-stop'); signal_child(client,read(Path(str(prefix)+'.pid.json')))
    return action
