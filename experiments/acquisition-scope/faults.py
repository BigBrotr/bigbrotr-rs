#!/usr/bin/env python3
"""Actual registration COMMIT/lock schedules on new clones of qualified scopes."""
import datetime
import hashlib
import json
import os
from pathlib import Path
import re
import secrets
import shutil
import sys
import time

sys.dont_write_bytecode = True
SOURCE = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(SOURCE/'experiments/control-migrations'))
import lab
from fault_cases import Process
from concurrency_cases import Session
sys.path.insert(0, str(SOURCE/'experiments/relay-source'))
from locks import blocked
sys.path.insert(0, str(SOURCE/'experiments/network-transport'))
from inventory import projection

root = Path(sys.argv[1]).resolve(strict=True)
assert len(sys.argv) == 2 and SOURCE == root/'source'
revision = lab.command(['git','-C',str(SOURCE),'rev-parse','HEAD']).stdout.strip()
assert not lab.command(['git','-C',str(SOURCE),'status','--porcelain','--untracked-files=all']).stdout
assert not lab.command(['git','-C',str(SOURCE),'diff','--name-only','54d1a18',revision,
    '--','apps/bigbrotr/src','crates','migrations','Cargo.toml','Cargo.lock']).stdout
build = root/'out/artifacts'
receipt = json.loads((build/'build-result.json').read_text())
assert receipt['source'] == revision and receipt['probe_name'] == 'acquisition_scope_probe'
sha = lambda value: hashlib.sha256(value).hexdigest()
assert sha((build/receipt['probe_name']).read_bytes()) == receipt['probe_sha256']
pg_root = root.parent/'relay-source-recheck-e06f580/pg'
pg = lab.load(pg_root)
assert pg['owner'] == '5e69b0f060b1607c25230d1dbd3e5608' and pg['state'] == 'stopped-retained'
basis_root = root.parent/'acquisition-scope-54d1a18/engine'
basis_report = (basis_root/'result.json').read_bytes()
assert json.loads(basis_report)['status'] == 'PASS_SCOPED_ACQUISITION_REGISTRATION'
output = root/'faults'; output.mkdir(mode=0o700)
owner, label = secrets.token_hex(16), 'org.bigbrotr.acquisition-scope-faults'
client = 'bigbrotr-acquisition-scope-faults-'+revision[:7]
report = dict(format='bigbrotr-acquisition-scope-faults/1',source=revision,build=receipt,
    status='RUNNING',owner=owner,label=label,client=None,cases=[],databases=[],
    postgres=pg['postgres'],postgres_owner=pg['owner'],
    not_run=['raw relational negative matrix','installed Sync, scope loading/admission, progress/M1',
             'load/aged-state','arm64/release'])
processes, sessions = [], []


def save(): lab.write_json(output/'result.json',report)


def require(condition, reason):
    if not condition: raise RuntimeError(reason)


def record(name, **details):
    report['cases'].append(dict(name=name,status='PASS',**details)); save()


def containers():
    return json.loads(lab.docker('inspect',*lab.docker('ps','-aq').stdout.split()).stdout)


def rows(db):
    names = json.loads(lab.sql(pg,db,"SELECT json_agg(tablename ORDER BY tablename) FROM pg_tables WHERE schemaname='control'"))
    require(len(names)==66 and all(re.fullmatch('[a-z_]+',n) for n in names),'table-set')
    parts=[f"SELECT '{n}' AS name,coalesce(jsonb_agg(to_jsonb(t) ORDER BY to_jsonb(t)::text),'[]') AS rows "
           f"FROM (SELECT *,xmin::text AS xmin FROM control.{n}) t" for n in names]
    return json.loads(lab.sql(pg,db,'SELECT jsonb_object_agg(name,rows) FROM ('+' UNION ALL '.join(parts)+') t'))


def prepare(name, template='basic'):
    db='bb_scope_fault_'+revision[:7]+'_'+name
    require(re.fullmatch('[a-z0-9_]{1,63}',db),'database-name')
    lab.sql(pg,'postgres',f'CREATE DATABASE {db} OWNER bigbrotr_migrator TEMPLATE bb_scope_54d1a18_{template}')
    report['databases'].append(db);save()
    before=rows(db)
    require(before==json.loads((basis_root/(template+'-after.json')).read_text()),'exact-retained-clone')
    lab.write_json(output/(name+'-before.json'),before)
    return db,before


def unchanged(name, db, before, policy=False, registration=True):
    after=rows(db)
    allowed={'owner_epochs','role_instances'}
    if registration: allowed.update(['acquisition_selectors','acquisition_scopes'])
    if policy: allowed.update(['datasets','config_revisions','policy_revisions'])
    require(before.keys()==after.keys() and all(before[t]==after[t] for t in before if t not in allowed),'unrelated-table-change')
    for table in ['acquisition_selectors','acquisition_scopes']:
        require(all(row in after[table] for row in before[table]),'immutable-original-registration')
    added=[r for r in after['acquisition_scopes'] if r not in before['acquisition_scopes']]
    access=next(r for r in before['relay_access_contexts'] if r['ordinal']==1)
    require(all(r['dataset_id']==before['datasets'][0]['dataset_id'] and
        r['endpoint_id']==before['relay_endpoints'][0]['endpoint_id'] and
        r['access_scope_id']==access['access_scope_id'] and r['visibility_kind']==4 and
        r['visibility_domain']==access['allocation_domain'] and r['visibility_ordinal']==1
        for r in added),'complete-new-scope-dimensions')
    lab.write_json(output/(name+'-after.json'),after)
    return after


def selected(db):
    # Independent exact literal document; never use the product encoder as oracle.
    document=b'{"version":1,"filters":[{"kinds":[10]}],"history_since":0,"ephemeral":"retain"}'
    return json.loads(lab.sql(pg,db,f"""
        SELECT coalesce(jsonb_agg(to_jsonb(s)||jsonb_build_object('selector_document',encode(v.document,'hex')) ORDER BY s.policy_revision),'[]')
        FROM control.acquisition_scopes s JOIN control.acquisition_selectors v USING(dataset_id,selector_id)
        WHERE v.document=decode('{document.hex()}','hex')
    """))


def app(db, mode, nonce, port=5432):
    token=db+'-'+str(nonce)
    p=Process(['docker','exec',client,'/build/acquisition_scope_probe',db,
        '/run/secrets/application','/lab/policy.json','fault-'+mode,str(port),str(nonce),'/lab/'+token])
    processes.append((token,p));return p,token


def event(p, wanted, seconds=10):
    deadline=time.monotonic()+seconds
    with p.condition:
        while True:
            require(not p.overflow,'probe-output-overflow')
            end=p.stdout.find(b'\n',p.offset)
            if end>=0:
                value=json.loads(bytes(p.stdout[p.offset:end]));p.offset=end+1
                require(value.get('event')==wanted,'unexpected-probe-event');return value
            require(time.monotonic()<deadline and (p.process.poll() is None or p.threads[0].is_alive()),'missing-probe-event-'+wanted)
            p.condition.wait(.02)


def finish(p):
    value=event(p,'done')
    require(p.wait(10)==0 and not p.stderr and value['driver_joined'],'probe-terminal')
    return value['outcome']


def direct(db, mode, nonce):
    p,_=app(db,mode,nonce)
    if mode=='busy':
        value=event(p,'busy');require(p.wait(10)==0 and not p.stderr and not value['commit_started'],'busy-probe');return value
    return dict(ready=event(p,'ready'),outcome=finish(p))


def observe(db, pid):
    value=json.loads(lab.sql(pg,db,f"""SELECT json_build_object(
        'pid',a.pid,'backend_start',a.backend_start::text,'state',a.state,
        'expired',o.lease_expires_at_us <= (EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint,
        'epoch',o.epoch,'fenced',EXISTS(SELECT 1 FROM pg_locks WHERE pid=a.pid
            AND locktype='advisory' AND classid=1111651442::oid AND objid=1129599564::oid
            AND objsubid=2 AND mode='ExclusiveLock' AND granted))
        FROM pg_stat_activity a JOIN control.owner_epochs o ON o.control_backend_pid=a.pid
            AND o.control_backend_start=a.backend_start
        WHERE a.pid={int(pid)} AND a.datname='{db}' AND o.state='active'"""))
    require(value and value['fenced'] and value['pid']==pid,'actual-owner-fence');return value


def expire(db, pid, observations):
    deadline=time.monotonic()+5
    while True:
        value=observe(db,pid)
        require(not observations or value['backend_start']==observations[0]['backend_start'],'owner-incarnation-changed')
        observations.append(value)
        if value['expired']: return
        require(time.monotonic()<deadline,'owner-not-expired');time.sleep(.05)


ADVANCE_POLICY="""
    INSERT INTO control.config_revisions
        SELECT 2,dataset_id,1,2,authority_mode,document,document_digest,accepted_source_digest,
            accepted_at_us+1,actor_id FROM control.config_revisions WHERE revision=1;
    INSERT INTO control.policy_revisions
        SELECT 2,dataset_id,1,2,2,document,document_digest,accepted_at_us+1,actor_id
        FROM control.policy_revisions WHERE revision=1;
    UPDATE control.datasets SET config_revision=2,policy_revision=2;
"""


def verify(value, durable, created, epoch=1):
    wanted=[r for r in durable if r['policy_revision']==epoch]
    require(len(wanted)==1,'scope-policy-count');row=wanted[0]
    require(value==dict(scope=row['scope_id'][2:],selector=row['selector_id'][2:],policy=epoch,
                       created=created,commit_started=created),'exact-recovered-identity')


def proxy_start(db, mode):
    p=Process(['docker','exec','-i',client,'python3','-B','/source/experiments/control-migrations/fault_proxy.py',
        '--upstream-port','5432','--mode',mode,'--commit-number','2','--deadline-seconds','35'],input_pipe=True)
    processes.append((db+'-proxy',p));ready=p.event('ready',10)
    require(ready['proxy_sha256']==sha((SOURCE/'experiments/control-migrations/fault_proxy.py').read_bytes()),'proxy-source')
    return p,ready['listen_port']


prior=containers();target=next(c for c in prior if c['Name']=='/'+pg['postgres'])
assert target['Config']['Labels'][lab.LABEL]==pg['owner'] and not target['State']['Running']
assert not any(c['State']['Running'] and 'bigbrotr' in c['Name'] for c in prior)
assert lab.docker('inspect',client,check=False).returncode!=0
report.update(started_at=datetime.datetime.now(datetime.timezone.utc).isoformat(),postgres_id=target['Id'],free_bytes_before=shutil.disk_usage(root).free)
assert report['free_bytes_before']>2*1024**3
lab.write_json(output/'preflight.json',dict(containers=[projection(c) for c in prior],
    processes=lab.command(['ps','-eo','pid,ppid,stat,comm']).stdout,ports=lab.command(['ss','-lntup']).stdout,
    volumes=lab.docker('volume','ls').stdout,directories=sorted(p.name for p in root.parent.iterdir())))
save()
try:
    lab.docker('start',target['Id']);deadline=time.monotonic()+30
    while lab.docker('exec',target['Id'],'pg_isready','-U','postgres',check=False).returncode:
        require(time.monotonic()<deadline,'postgres-readiness');time.sleep(.1)
    for name in ['basic','collision']:
        require(rows('bb_scope_54d1a18_'+name)==json.loads((basis_root/(name+'-after.json')).read_text()),'retained-basis-before')
    shutil.copyfile(basis_root/'policy.json',output/'policy.json')
    ident=lab.docker('create','--init','--name',client,'--label',label+'='+owner,
        '--network','container:'+pg['postgres'],'--user',f'{os.getuid()}:{os.getgid()}',
        '--cpus','2','--memory','1g','--memory-swap','1g','--pids-limit','128','--read-only',
        '--cap-drop','ALL','--security-opt','no-new-privileges',
        '--mount',f'type=bind,src={output},dst=/lab',
        '--mount',f'type=bind,src={pg_root}/secrets,dst=/run/secrets,readonly',
        '--mount',f'type=bind,src={build},dst=/build,readonly',
        '--mount',f'type=bind,src={SOURCE},dst=/source,readonly',pg['client_image'],
        'python3','-c','import time; time.sleep(600)').stdout.strip()
    report['client']=dict(id=ident,name=client);save();lab.docker('start',ident)
    for name,proxy_mode in [('pass','pass'),('before','before-forward'),('after','after-complete'),
                            ('cancel_close','hold'),('cancel_release','hold'),('expiry','hold')]:
        db,before=prepare(name);proxy,port=proxy_start(db,proxy_mode)
        mode='unknown' if name in ('before','after') else 'cancel' if name.startswith('cancel_') else 'held-expired' if name=='expiry' else 'insert'
        p,token=app(db,mode,11,port);ready=event(p,'ready');observations=[]
        if proxy_mode=='hold':
            proxy.event('held',10);observations.append(observe(db,ready['pid']))
            require(not selected(db),'uncommitted-scope-visible')
            if name.startswith('cancel_'): require(event(p,'cancelled',5)['commit_started'],'not-cancelled')
            else:
                require(not observations[0]['expired'],'expired-before-hold-observation');expire(db,ready['pid'],observations)
            observations.append(observe(db,ready['pid']));state=rows(db)
            direct(db,'busy',12);require(rows(db)==state,'busy-owner-created-rows')
            proxy.send('close' if name=='cancel_close' else 'release')
            if name.startswith('cancel_'): (output/(token+'.finish')).touch(exist_ok=False)
        outcome=finish(p);wire=proxy.event('receipt',10)
        require(proxy.wait(5)==0 and not proxy.stderr,'proxy-terminal')
        counts=[wire[k] for k in ['commits_seen','commits_forwarded','server_commit_complete','client_commit_complete','faults']]
        expected={'pass':[2,2,2,2,0],'before':[2,1,1,1,1],'after':[2,2,2,1,1],
                  'cancel_close':[2,1,1,1,1],'cancel_release':[2,2,2,2,1],'expiry':[2,2,1,1,1]}[name]
        require(counts==expected,'selected-COMMIT-counts')
        original=selected(db);committed=name in ('pass','after','cancel_release')
        require(len(original)==int(committed),'independent-durable-outcome')
        if committed: require(original[0]['owner_epoch']==ready['epoch'] and original[0]['policy_revision']==1,'original-attribution')
        if name=='pass': verify(outcome,original,True)
        if name=='after': lab.sql(pg,db,'BEGIN; SELECT dataset_id FROM control.datasets FOR UPDATE;'+ADVANCE_POLICY+'COMMIT;')
        recovered=direct(db,'insert',13);durable=selected(db)
        require(recovered['ready']['epoch']==ready['epoch']+1,'successor-epoch')
        verify(recovered['outcome'],durable,not committed)
        require(durable==original if committed else durable[0]['owner_epoch']==recovered['ready']['epoch'],'original-or-successor-attribution')
        after=unchanged(name,db,before,policy=name=='after')
        require(len(after['acquisition_selectors'])==len(before['acquisition_selectors'])+1 and
                len(after['acquisition_scopes'])==len(before['acquisition_scopes'])+1,'atomic-recovery-row-counts')
        record('COMMIT-'+name,outcome=outcome,wire=wire,observations=observations,original=original,recovery=recovered,durable=durable)

    db,before=prepare('policy_first');p,token=app(db,'policy',11);ready=event(p,'ready')
    original_owner=observe(db,ready['pid']);writer=Session(pg,db,'scope-policy-writer',sessions)
    writer.execute('BEGIN; SELECT dataset_id FROM control.datasets FOR UPDATE;');writer.execute(ADVANCE_POLICY)
    (output/(token+'.go')).touch(exist_ok=False)
    barrier=blocked(pg,db,original_owner,writer.backend);writer.execute('COMMIT;')
    outcome=finish(p);require(not selected(db),'stale-head-created-scope')
    fresh=direct(db,'current',12);durable=selected(db);verify(fresh['outcome'],durable,True,2)
    require(len(durable)==1,'stale-head-created-old-identity')
    unchanged('policy_first',db,before,policy=True)
    record('policy-first-lock-order',barrier=barrier,outcome=outcome,fresh=fresh,durable=durable)
    writer.close(lambda:lab.load(pg_root));sessions.remove(writer)

    db,before=prepare('registration_first');proxy,port=proxy_start(db,'hold')
    p,_=app(db,'insert',11,port);ready=event(p,'ready');proxy.event('held',10)
    original_owner=observe(db,ready['pid']);writer=Session(pg,db,'scope-registration-writer',sessions)
    writer.start('BEGIN; SELECT dataset_id FROM control.datasets FOR UPDATE;')
    barrier=blocked(pg,db,writer.backend,original_owner)
    require(not selected(db),'held-registration-visible');proxy.send('release')
    outcome=finish(p);wire=proxy.event('receipt',10);require(proxy.wait(5)==0 and not proxy.stderr,'proxy-terminal')
    writer.expect(writer.finish());writer.execute(ADVANCE_POLICY);writer.execute('COMMIT;')
    original=selected(db);verify(outcome,original,True)
    historical=direct(db,'insert',12);verify(historical['outcome'],original,False)
    stale=direct(db,'stale',13)
    fresh=direct(db,'current',14);durable=selected(db);verify(fresh['outcome'],durable,True,2)
    require(len(durable)==2 and durable[0]==original[0] and durable[0]['selector_id']==durable[1]['selector_id'],'separate-epoch-identity')
    after=unchanged('registration_first',db,before,policy=True)
    require(len(after['acquisition_selectors'])==len(before['acquisition_selectors'])+1 and
            len(after['acquisition_scopes'])==len(before['acquisition_scopes'])+2,'epoch-recovery-row-counts')
    record('registration-first-lock-order',barrier=barrier,wire=wire,original=original,historical=historical,stale=stale,fresh=fresh,durable=durable)
    writer.close(lambda:lab.load(pg_root));sessions.remove(writer)

    db,before=prepare('overfull','collision')
    lab.sql(pg,db,"""BEGIN; ALTER TABLE control.acquisition_selectors DISABLE TRIGGER USER;
        INSERT INTO control.acquisition_selectors(selector_id,dataset_id,profile_version,document,registered_at_us,
            config_revision,policy_revision,owner_resource_id,owner_epoch,owner_incarnation)
        SELECT decode(repeat('ee',16),'hex'),dataset_id,1,
            convert_to('{"version":1,"filters":[{"kinds":[888]}],"history_since":0,"ephemeral":"retain"}','UTF8'),
            registered_at_us,config_revision,policy_revision,owner_resource_id,owner_epoch,owner_incarnation
        FROM control.acquisition_selectors LIMIT 1;
        ALTER TABLE control.acquisition_selectors ENABLE TRIGGER USER; COMMIT;""")
    require(lab.sql(pg,db,"SELECT count(*) FROM control.acquisition_selectors")=='65','overfull-fixture-count')
    require(lab.sql(pg,db,"SELECT count(*) FROM pg_trigger WHERE tgrelid='control.acquisition_selectors'::regclass AND tgenabled<>'O'")=='0','guards-not-restored')
    corrupt=rows(db);lab.write_json(output/'overfull-injected.json',corrupt)
    outcomes=[direct(db,mode,nonce) for mode,nonce in [('overfull',11),('overfull-new',12)]]
    unchanged('overfull',db,corrupt,registration=False);record('corrupt-overfull-existing-and-absent',outcomes=outcomes)

    db,before=prepare('lookup_expired');p,token=app(db,'existing-expired',11);ready=event(p,'ready');observations=[]
    expire(db,ready['pid'],observations);(output/(token+'.go')).touch(exist_ok=False)
    outcome=finish(p);require(not outcome['created'] and not outcome['commit_started'],'expired-lookup-created')
    require(any(r['scope_id']=='\\x'+outcome['scope'] and r['selector_id']=='\\x'+outcome['selector'] for r in before['acquisition_scopes']),'expired-original-identity')
    unchanged('lookup_expired',db,before,registration=False);record('expired-identity-lookup-only',observations=observations,outcome=outcome)
    for name in ['basic','collision']:
        require(rows('bb_scope_54d1a18_'+name)==json.loads((basis_root/(name+'-after.json')).read_text()),'retained-basis-after')
    require((basis_root/'result.json').read_bytes()==basis_report,'original-report-changed')
    require(lab.sql(pg,'postgres',"SELECT count(*) FROM pg_stat_activity WHERE usename='bigbrotr_control'")=='0','unjoined-application-backend')
    report['status']='PASS_SCOPED_ACQUISITION_FAULTS'
except BaseException as error:
    report.update(status='FAIL',failure=type(error).__name__+': '+str(error));raise
finally:
    errors=[]
    for session in sessions:
        try: session.close(lambda:lab.load(pg_root))
        except BaseException as error: errors.append('sql-session-'+type(error).__name__)
    if report['client']:
        try:
            item=json.loads(lab.docker('inspect',report['client']['id']).stdout)[0]
            require(item['Config']['Labels'][label]==owner,'client-ownership')
            if item['State']['Running']:lab.docker('stop','--time','3',item['Id'])
        except BaseException as error:errors.append('client-'+type(error).__name__)
    for name,p in processes:
        try:p.stop()
        except BaseException as error:errors.append('process-'+type(error).__name__)
        for suffix, data in [('.stdout',p.stdout),('.stderr',p.stderr)]:
            try: (output/(name+suffix)).write_bytes(data)
            except BaseException as error: errors.append('transcript-'+type(error).__name__)
    try:
        item=json.loads(lab.docker('inspect',target['Id']).stdout)[0]
        require(item['Config']['Labels'][lab.LABEL]==pg['owner'],'postgres-ownership')
        if item['State']['Running']:lab.docker('stop','--time','10',target['Id'])
    except BaseException as error:errors.append('postgres-'+type(error).__name__)
    current={c['Id']:projection(c) for c in containers()};owned=[target['Id']]+([report['client']['id']] if report['client'] else [])
    report.update(cleanup_errors=errors,missing_owned=[i for i in owned if i not in current],terminal=[current[i] for i in owned if i in current],
        unrelated_changed=[c['Id'] for c in prior if c['Id']!=target['Id'] and current.get(c['Id'])!=projection(c)],
        finished_at=datetime.datetime.now(datetime.timezone.utc).isoformat(),free_bytes_after=shutil.disk_usage(root).free)
    if errors or report['missing_owned'] or report['unrelated_changed'] or any(c['State']['Running'] or c['State']['Pid'] or c['State']['OOMKilled'] for c in report['terminal']):
        report['status']='INCOMPLETE_LAB_AUDIT'
    save()
require(report['status']=='PASS_SCOPED_ACQUISITION_FAULTS','incomplete-scope-faults')
print(json.dumps(dict(status=report['status'],cases=len(report['cases']))))
