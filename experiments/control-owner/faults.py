"""Additional real-process Control recovery cases; imported by the owned runner."""
import hashlib
import json
import os
import time

import lab
from fault_cases import Process


def require(value, reason):
    if not value: raise RuntimeError(reason)


def first_event(process, timeout):
    deadline = time.monotonic()+timeout
    while time.monotonic()<deadline:
        with process.condition: data = bytes(process.stdout)
        if b'\n' in data: return json.loads(data.splitlines()[0])
        require(process.process.poll() is None, 'probe-ended-before-event')
        time.sleep(.02)
    raise RuntimeError('probe-event-timeout')


def run(root, state, build, source, report, prepare, rows, save):
    report['fault_containers'] = []
    def probe(database, mode):
        result = lab.docker('exec',state['client'],'/build/control_owner_probe',mode,database,'/lab/secrets/application',check=False)
        (root/(database+'-'+mode+'.stdout')).write_text(result.stdout)
        (root/(database+'-'+mode+'.stderr')).write_text(result.stderr)
        require(result.returncode == 0 and not result.stderr, 'direct-probe-'+mode)
        return json.loads(result.stdout)
    def container(suffix):
        name=state['client']+'-'+suffix
        require(lab.docker('container','inspect',name,check=False).returncode != 0, 'existing-owned-container')
        record={'name':name,'owner':state['owner'],'state':'creating'}
        report['fault_containers'].append(record);save()
        lab.docker('run','-d','--init','--name',name,'--label','org.bigbrotr.control-owner-fault='+state['owner'],
            '--network','container:'+state['postgres'],'--user',f'{os.getuid()}:{os.getgid()}',
            '--cpus','2','--memory','1g','--memory-swap','1g','--pids-limit','128','--read-only',
            '--cap-drop','ALL','--security-opt','no-new-privileges',
            '--mount',f'type=bind,src={root},dst=/lab','--mount',f'type=bind,src={build},dst=/build,readonly',
            '--mount',f'type=bind,src={source},dst=/source,readonly',state['client_image'],'python3','-c','import time; time.sleep(240)')
        record['id']=json.loads(lab.docker('inspect',name).stdout)[0]['Id'];record['state']='running';save()
        return record
    def stop(record, processes):
        info=json.loads(lab.docker('inspect',record['name']).stdout)[0]
        require(info['Id']==record['id'] and info['Config']['Labels']['org.bigbrotr.control-owner-fault']==state['owner'],'owned-container-changed')
        if info['State']['Paused']:lab.docker('unpause',record['name'])
        if info['State']['Running']:lab.docker('stop','--time','3',record['name'])
        for process in processes:process.stop()
        record['state']='stopped-retained';record['terminal']=json.loads(lab.docker('inspect',record['name']).stdout)[0]['State'];save()
    def owners(database, count, active):
        value=rows(database)
        control=sorted((r for r in value['owner_epochs'] if r['role']=='control'),key=lambda r:r['epoch'])
        require(len(control)==count and [r['epoch'] for r in control]==list(range(1,count+1)), 'complete-owner-epochs')
        require(sum(r['state']=='active' for r in control)==active, 'active-owner-count')
        require(len(value['role_instances'])==count and sum(r['retired_at_us'] is None for r in value['role_instances'])==active, 'complete-role-retirement')
        if count:require(len({r['resource_id'] for r in control})==1, 'resource-reused')
        return value
    for scenario,mode in [('before','before-forward'),('after','after-complete'),('cancel','hold')]:
        db,_=prepare('fault_'+scenario)
        initial=rows(db)
        record=container('fault-'+scenario);processes=[]
        try:
            proxy=Process(['docker','exec','-i',record['name'],'python3','-B','/source/experiments/control-migrations/fault_proxy.py','--upstream-port','5432','--mode',mode,'--deadline-seconds','60'],input_pipe=True);processes.append(proxy)
            ready=proxy.event('ready',10)
            require(ready['proxy_sha256']==hashlib.sha256((source/'experiments/control-migrations/fault_proxy.py').read_bytes()).hexdigest(),'proxy-source')
            app_mode='cancel-held' if scenario=='cancel' else 'lost-'+scenario
            app=Process(['docker','exec',record['name'],'/build/control_owner_probe',app_mode,db,'/lab/secrets/application',str(ready['listen_port'])]);processes.append(app)
            if scenario=='cancel':
                proxy.event('held',15)
                require(rows(db)==initial,'uncommitted-owner-visible')
                require(first_event(app,5)=={'event':'cancelled','commit_started':True},'actual-acquisition-cancellation')
                require(probe(db,'busy')=={'event':'busy','commit_started':False},'cancelled-driver-still-excluded')
                proxy.send('release')
            require(app.wait(15)==0 and not app.stderr, 'fault-probe-terminal')
            expected={'event':'cancelled' if scenario=='cancel' else 'unknown','commit_started':True}
            require(json.loads(app.stdout)==expected,'complete-fault-response')
            receipt=proxy.event('receipt',10)
            expected_counts={'before':[1,0,0,0,1],'after':[1,1,1,0,1],'cancel':[1,1,1,1,1]}[scenario]
            require([receipt[k] for k in ['commits_seen','commits_forwarded','server_commit_complete','client_commit_complete','faults']]==expected_counts,'exact-COMMIT-boundary')
            require(proxy.wait(5)==0 and not proxy.stderr,'proxy-terminal')
            count=0 if scenario=='before' else 1
            current=owners(db,count,count)
            require(all(current[n]==initial[n] for n in initial if n not in ['owner_epochs','role_instances']), 'unknown-effect-set')
            recovered=probe(db,'recover');require(recovered=={'event':'complete','mode':'recover','epoch':count+1},'recovered-epoch')
            owners(db,count+1,0)
            report['cases'].append({'name':'owner-COMMIT-'+scenario,'result':'PASS','proxy':receipt,'recovery':recovered});save()
        finally:stop(record,processes)
    for killed in [False,True]:
        suffix='killed' if killed else 'live';db,_=prepare('paused_'+suffix)
        record=container('paused-'+suffix);processes=[]
        try:
            app=Process(['docker','exec',record['name'],'/build/control_owner_probe','hold-'+suffix,db,'/lab/secrets/application','5432']);processes.append(app)
            require(first_event(app,15)=={'event':'holding','epoch':1,'ttl_seconds':60},'actual-held-owner')
            original=owners(db,1,1);old=original['owner_epochs'][0]
            lab.docker('pause',record['name'])
            require(json.loads(lab.docker('inspect',record['name']).stdout)[0]['State']['Paused'], 'process-not-paused')
            paused_started=time.monotonic()
            deadline=paused_started+75
            while lab.sql(state,db,"SELECT lease_expires_at_us < (EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint FROM control.owner_epochs WHERE role='control';")!='t' or time.monotonic()-paused_started<61:
                require(time.monotonic()<deadline,'scheduling-TTL-not-expired');time.sleep(.5)
            require(probe(db,'busy')=={'event':'busy','commit_started':False},'expired-owner-was-stolen')
            require(rows(db)==original,'expired-claim-mutated-history')
            if killed:
                require(lab.sql(state,db,f"SELECT pg_terminate_backend({old['control_backend_pid']},5000);")=='t','backend-not-excluded')
                require(probe(db,'recover')=={'event':'complete','mode':'recover','epoch':2},'paused-successor-claim')
            paused_seconds=time.monotonic()-paused_started
            lab.docker('unpause',record['name'])
            require(app.wait(20)==0 and not app.stderr,'old-process-resume')
            require(json.loads(bytes(app.stdout).splitlines()[1])=={'event':'complete','mode':'hold-'+suffix,'epoch':1},'complete-resume-output')
            owners(db,2 if killed else 1,0)
            report['cases'].append({'name':'paused-beyond-TTL-'+suffix,'result':'PASS','old_backend':{'pid':old['control_backend_pid'],'start':old['control_backend_start']},'lease_expires_at_us':old['lease_expires_at_us'],'successor_while_paused':killed,'paused_seconds':paused_seconds});save()
        finally:stop(record,processes)
    db,_=prepare('crash')
    require(probe(db,'claim')=={'event':'complete','mode':'claim','epoch':1},'initial-crash-owner')
    owners(db,1,1)
    names=json.loads(lab.sql(state,'postgres',"SELECT json_agg(datname ORDER BY datname) FROM pg_database WHERE datname LIKE 'bb_owner_%';"))
    before={name:lab.schema_digest(state,name) for name in names}
    lab.docker('kill','--signal','KILL',state['postgres']);lab.docker('start',state['postgres'])
    deadline=time.monotonic()+20
    while lab.docker('exec',state['postgres'],'pg_isready','-U','postgres',check=False).returncode:
        require(time.monotonic()<deadline,'PostgreSQL-restart-timeout');time.sleep(.1)
    require(before=={name:lab.schema_digest(state,name) for name in names},'crash-history-changed')
    require(probe(db,'recover')=={'event':'complete','mode':'recover','epoch':2},'post-crash-owner')
    owners(db,2,0)
    report['cases'].append({'name':'PostgreSQL-crash-and-successor','result':'PASS','preserved_dumps':before});save()
    require(lab.sql(state,'postgres',"SELECT count(*) FROM pg_stat_activity WHERE usename IN ('bigbrotr_control','bigbrotr_migrator');")=='0','remaining-application-backends')
    report['remaining_backends']=0;save()
