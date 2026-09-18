#!/usr/bin/env python3
"""Actual Control executable, isolated native PG; never an in-memory substitute."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import time
sys.dont_write_bytecode=True
SOURCE=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(SOURCE/'experiments/control-migrations'))
import lab


def require(ok, reason):
    if not ok: raise RuntimeError(reason)


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root',type=Path,required=True)
    root=parser.parse_args().root.resolve(strict=True)
    state=lab.load(root)
    require(not lab.command(['git','-C',str(SOURCE),'status','--porcelain','--untracked-files=all']).stdout,'dirty-source')
    revision=lab.command(['git','-C',str(SOURCE),'rev-parse','HEAD']).stdout.strip()
    require(revision==state['build']['build_info']['source']['revision'],'source-identity')
    require(json.loads(lab.cli(state,'build-info').stdout)==state['build']['build_info'],'artifact-identity')
    mounts=json.loads(lab.docker('inspect',state['client']).stdout)[0]['Mounts']
    build=Path(next(m['Source'] for m in mounts if m['Destination']=='/build' and not m['RW']))
    require(hashlib.sha256((build/'bigbrotr').read_bytes()).hexdigest()==state['binary_sha256'],'artifact-bytes')
    output=root/'control-runtime.json';require(not output.exists(),'existing-report')
    report={'format':'bigbrotr-control-runtime-acceptance/1','source':revision,'binary_sha256':state['binary_sha256'],'status':'RUNNING','cases':[],'containers':[],'observations':{}}
    def save():lab.write_json(output,report)
    def check(name,**detail):report['cases'].append(dict(name=name,result='PASS',**detail));save()
    def rows(db):
        names=json.loads(lab.sql(state,db,"SELECT json_agg(tablename ORDER BY tablename) FROM pg_tables WHERE schemaname='control';"))
        require(len(names)==30,'complete-relation-inventory')
        return {n:json.loads(lab.sql(state,db,f"SELECT coalesce(json_agg(t),'[]') FROM control.{n} t;")) for n in names}
    def prepare(name,active=True):
        db='bb_runtime_'+name;lab.sql(state,'postgres',f'CREATE DATABASE {db} OWNER bigbrotr_migrator;')
        path=lab.config(root,state,db);local=root/Path(path).name
        local.write_text(lab.change(local.read_text(),'postgres.control','password_ref','file:/lab/secrets/application'))
        if name!='enabled_administration':local.write_text(lab.change(local.read_text(),'administration','enabled',False))
        require(lab.migrate(state,'apply',path,target=7).returncode==0,'migration')
        lab.sql(state,db,'GRANT USAGE ON SCHEMA control TO bigbrotr_control; GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA control TO bigbrotr_control; GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA control TO bigbrotr_control;')
        require(lab.cli(state,'config','initialize','--file',path).returncode==0,'initialize')
        if active:require(lab.cli(state,'config','activate-initial','--file',path,'--request-id','51'*16,'--expected-revision','1').returncode==0,'activate')
        return db,path,local
    def info(record):
        value=json.loads(lab.docker('inspect',record['name']).stdout)[0]
        require(value['Id']==record['id'] and value['Config']['Labels'].get('org.bigbrotr.control-runtime')==state['owner'],'changed-container-owner')
        return value
    def start(path,label):
        name=state['client']+'-'+label;require(lab.docker('inspect',name,check=False).returncode!=0,'existing-container')
        record={'name':name,'state':'creating'};report['containers'].append(record);save()
        cid=lab.docker('run','-d','--init','--name',name,'--label','org.bigbrotr.control-runtime='+state['owner'],
            '--network',state['network'],'--user',f'{os.getuid()}:{os.getgid()}',
            '--cpus','2','--memory','1g','--memory-swap','1g','--pids-limit','128','--read-only','--cap-drop','ALL','--security-opt','no-new-privileges',
            '--mount',f'type=bind,src={root},dst=/lab,readonly','--mount',f'type=bind,src={build},dst=/build,readonly',
            state['client_image'],'/build/bigbrotr','serve','--file',path,'--roles','control').stdout.strip()
        record.update(id=cid,state='running');save();return record
    def logs(record):
        result=lab.docker('logs',record['name']);return result.stdout,result.stderr
    def idle(record):
        deadline=time.monotonic()+12
        while time.monotonic()<deadline:
            out,err=logs(record)
            if out:
                value=json.loads(out.splitlines()[0]);require(value['state']=='idle' and value['listeners']==[] and value['native_stores']==[] and not err,'idle-output');return value
            require(info(record)['State']['Running'],'early-exit');time.sleep(.05)
        raise RuntimeError('idle-timeout')
    def terminal(record,code,seconds=12):
        deadline=time.monotonic()+seconds
        while time.monotonic()<deadline:
            value=info(record)
            if not value['State']['Running']:break
            time.sleep(.05)
        require(not value['State']['Running'] and value['State']['ExitCode']==code and not value['State']['OOMKilled'],'unexpected-terminal-'+record['name'])
        out,err=logs(record);(root/(record['name']+'.stdout')).write_text(out);(root/(record['name']+'.stderr')).write_text(err)
        record.update(state='stopped-retained',terminal=value['State']);save();return out,err
    def released(record,signal='TERM'):
        lab.docker('kill','--signal',signal,record['name']);out,err=terminal(record,0)
        require(not err and [json.loads(line)['state'] for line in out.splitlines()]==['idle','stopped'],'release-output')
        require(json.loads(out.splitlines()[1])['release']=='committed','release-not-committed')
    def history(db,count,active):
        observed=rows(db);owners=sorted(observed['owner_epochs'],key=lambda r:r['epoch'])
        require(len(owners)==count and len(observed['role_instances'])==count,'owner-cardinality')
        require([r['epoch'] for r in owners]==list(range(1,count+1)),'epoch-progression')
        require(len({r['resource_id'] for r in owners})==1 and len({r['process_incarnation'] for r in owners})==count,'resource-incarnation')
        require(sum(r['state']=='active' for r in owners)==active,'active-owner-count')
        require(sum(r['retired_at_us'] is None for r in observed['role_instances'])==active,'role-retirement')
        return observed
    save()
    try:
        for case in ['no_policy','wrong_dataset','wrong_instance','wrong_mode','invalid_config','invalid_policy','enabled_administration']:
            db,path,local=prepare(case,active=case!='no_policy')
            if case.startswith('wrong_'):
                section,key,value={'wrong_dataset':('topology','dataset_id','61'*16),'wrong_instance':('topology','instance_id','62'*16),'wrong_mode':('config','authority_mode','declarative')}[case]
                local.write_text(lab.change(local.read_text(),section,key,value))
            if case.startswith('invalid_'):
                table='config_revisions' if case=='invalid_config' else 'policy_revisions'
                row=json.loads(lab.sql(state,db,f"SELECT json_build_object('document',document,'digest',encode(document_digest,'hex')) FROM control.{table};"))
                encode=lambda d:json.dumps(d,ensure_ascii=False,separators=(',',':'),sort_keys=True)
                digest=lambda d:hashlib.sha256(b'bigbrotr/product-policy/1\0'+encode(d).encode()).hexdigest()
                require(digest(row['document'])==row['digest'],'independent-policy-encoding')
                row['document']['settings']['ingest.max_event_bytes']=0
                doc=encode(row['document']).replace("'","''")
                lab.sql(state,db,f"ALTER TABLE control.{table} DISABLE TRIGGER ALL; UPDATE control.{table} SET document='{doc}'::jsonb,document_digest=decode('{digest(row['document'])}','hex'); ALTER TABLE control.{table} ENABLE TRIGGER ALL;")
            before=lab.schema_digest(state,db);result=lab.cli(state,'serve','--file',path,'--roles','control')
            (root/(case+'.stderr')).write_text(result.stderr)
            require(result.returncode==3 and not result.stdout and 'Control backend operation failed' in result.stderr,'startup-refusal-'+case)
            require(before==lab.schema_digest(state,db),'startup-mutated-'+case);check(case,exit_code=result.returncode)
        db,path,local=prepare('lifecycle');baseline=rows(db)
        # Managed edits stay seed input; stored accepted policy still wins.
        local.write_text(local.read_text()+"\n[ingest]\nmax_event_bytes='512KiB'\n")
        owner=start(path,'lifecycle');first=idle(owner);before=history(db,1,1)
        duplicate=lab.cli(state,'serve','--file',path,'--roles','control')
        require(duplicate.returncode==3 and 'Busy' in duplicate.stderr and not duplicate.stdout,'concurrent-owner')
        require(rows(db)==before,'concurrent-mutated-history');check('concurrent-owner-refused')
        time.sleep(3);require(rows(db)==before,'policy-poll-renewed-scheduling-row');check('policy-poll-without-owner-renewal')
        deadline=time.monotonic()+24
        while time.monotonic()<deadline:
            renewed=rows(db)
            if renewed['owner_epochs'][0]['lease_expires_at_us']>before['owner_epochs'][0]['lease_expires_at_us']:break
            time.sleep(.2)
        require(renewed['owner_epochs'][0]['lease_expires_at_us']>before['owner_epochs'][0]['lease_expires_at_us'],'heartbeat-not-renewed')
        normalized=json.loads(json.dumps(renewed));normalized['owner_epochs'][0]['lease_expires_at_us']=before['owner_epochs'][0]['lease_expires_at_us'];require(normalized==before,'heartbeat-extra-effects')
        released(owner);history(db,1,0);check('managed-drift-heartbeat-sigterm',initial=first)
        owner=start(path,'restart');second=idle(owner);require(second['owner_epoch']=='2' and first['process_incarnation']!=second['process_incarnation'],'restart-identity');released(owner,'INT');history(db,2,0);check('restart-sigint',initial=second)
        owner=start(path,'paused');idle(owner);before=rows(db);lab.docker('pause',owner['name']);time.sleep(6)
        require(rows(db)==before,'paused-effects');lab.docker('unpause',owner['name']);out,err=terminal(owner,1)
        require('policy authority expired' in err and len(out.splitlines())==1 and rows(db)==before,'paused-owner-renewed');check('paused-past-policy-ttl')
        owner=start(path,'successor');idle(owner);released(owner);history(db,4,0);check('paused-owner-successor')
        owner=start(path,'cancelled');idle(owner)
        with (root/'blocker.stdout').open('w') as out, (root/'blocker.stderr').open('w') as err:
            blocker=subprocess.Popen(['docker','exec','-i',state['postgres'],'psql','-X','-q','-v','ON_ERROR_STOP=1','-U','postgres','-d',db],stdin=subprocess.PIPE,stdout=out,stderr=err,text=True)
            blocker.stdin.write("BEGIN; SET LOCAL application_name='bigbrotr-runtime-blocker'; SELECT 1 FROM control.datasets FOR UPDATE; SELECT pg_sleep(60); ROLLBACK;\n");blocker.stdin.close()
            try:
                deadline=time.monotonic()+8
                while time.monotonic()<deadline:
                    waiting=lab.sql(state,db,"SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND usename='bigbrotr_control' AND wait_event_type='Lock';")
                    if waiting=='1':break
                    time.sleep(.02)
                require(waiting=='1','refresh-not-blocked')
                lab.docker('kill','--signal','TERM',owner['name']);terminal(owner,4,6)
            finally:
                lab.sql(state,db,"SELECT pg_terminate_backend(pid,5000) FROM pg_stat_activity WHERE datname=current_database() AND application_name='bigbrotr-runtime-blocker';")
                blocker.wait(timeout=8)
        require(lab.sql(state,db,"SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND usename='bigbrotr_control';")=='0','cancelled-backend-retained');check('cancelled-policy-refresh')
        owner=start(path,'database-loss');idle(owner);before=rows(db)
        lab.docker('kill','--signal','KILL',state['postgres']);out,err=terminal(owner,1)
        require('Backend' in err and len(out.splitlines())==1,'database-loss-output')
        lab.docker('start',state['postgres']);deadline=time.monotonic()+20
        while lab.docker('exec',state['postgres'],'pg_isready','-U','postgres',check=False).returncode:
            require(time.monotonic()<deadline,'database-restart-timeout');time.sleep(.1)
        require(rows(db)==before,'database-crash-history');check('database-crash-fail-closed')
        owner=start(path,'recovered');idle(owner);released(owner);final=history(db,7,0)
        require(all(final[n]==baseline[n] for n in baseline if n not in ['owner_epochs','role_instances']),'runtime-unrelated-mutations');check('database-recovery-explicit-restart')
        names=json.loads(lab.sql(state,'postgres',"SELECT json_agg(datname ORDER BY datname) FROM pg_database WHERE datname LIKE 'bb_runtime_%';"))
        complete={}
        for db in names:
            require(lab.sql(state,db,"SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND usename IN ('bigbrotr_control','bigbrotr_migrator');")=='0','retained-application-backends')
            actual=rows(db);complete[db]=actual;report['observations'][db]={n:{'rows':len(v),'sha256':hashlib.sha256(json.dumps(v,sort_keys=True).encode()).hexdigest()} for n,v in actual.items()}
        descriptor=os.open(root/'final-rows.json',os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600)
        with os.fdopen(descriptor,'w') as stored:json.dump(complete,stored,sort_keys=True)
        report['status']='PASS'
    except BaseException as error:
        report.update(status='FAIL',failure=str(error));raise
    finally:
        for record in report['containers']:
            if 'id' not in record:continue
            current=info(record)
            if current['State']['Paused']:lab.docker('unpause',record['name'])
            if current['State']['Running']:lab.docker('stop','--time','3',record['name'])
            record.update(state='stopped-retained',terminal=info(record)['State'])
        save()
    print(json.dumps({'status':report['status'],'cases':len(report['cases'])}))

if __name__=='__main__':main()
