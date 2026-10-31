#!/usr/bin/env python3
"""Actual initialization, replay, refusal and COMMIT faults in one new owned PG lab."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import sys
import time

sys.dont_write_bytecode = True
SOURCE = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(SOURCE / 'experiments/control-migrations'))
import lab
from fault_cases import Process


def require(condition, reason):
    if not condition:
        raise RuntimeError(reason)


def sha(data):
    return hashlib.sha256(data).hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, required=True)
    root = parser.parse_args().root.resolve(strict=True)
    state = lab.load(root)
    revision = lab.command(['git', '-C', str(SOURCE), 'rev-parse', 'HEAD']).stdout.strip()
    require(not lab.command(['git','-C',str(SOURCE),'status','--porcelain','--untracked-files=all']).stdout, 'dirty-source')
    require(revision == state['build']['build_info']['source']['revision'] and Path(state['source']) == SOURCE, 'source-mismatch')
    actual = lab.cli(state, 'build-info')
    require(actual.returncode == 0 and json.loads(actual.stdout) == state['build']['build_info'], 'mounted-build-identity')
    client_info=json.loads(lab.docker('inspect',state['client']).stdout)[0]
    build_mounts=[m for m in client_info['Mounts'] if m['Destination']=='/build']
    require(len(build_mounts)==1 and not build_mounts[0]['RW'],'build-mount')
    build=Path(build_mounts[0]['Source'])
    mounted = lab.docker('exec',state['client'],'sha256sum','/build/bigbrotr').stdout.split()[0]
    require(mounted == state['binary_sha256'], 'mounted-binary-hash')
    output = root / 'control-initialization.json'
    require(not output.exists(), 'existing-report')
    report = {'format':'bigbrotr-control-initialization-native/1','source':revision,'binary_sha256':mounted,
              'status':'RUNNING','cases':[],'fault_containers':[], 'server':state['server']}
    policy = json.loads((SOURCE/'tests/fixtures/configuration/policy-v3.json').read_text())
    fingerprint = '77324b5f6aea113a4e4b81e11d946c0c4e113b051851d398c240de49a7a6322b'
    canonical = json.dumps(policy,sort_keys=True,ensure_ascii=False,separators=(',',':')).encode()
    require(sha(b'bigbrotr/product-policy/3\0'+canonical) == fingerprint,'frozen-policy-hash')
    databases = []

    def save(): lab.write_json(output,report)

    def prepare(name, target=801, declarative=False):
        database = 'bb_init_'+name
        lab.sql(state,'postgres',f'CREATE DATABASE {database} OWNER bigbrotr_migrator;')
        databases.append(database)
        path = lab.config(root,state,database)
        local = root/Path(path).name
        text = lab.change(local.read_text(),'postgres.control','password_ref','file:/lab/secrets/application')
        if declarative: text = lab.change(text,'config','authority_mode','declarative')
        local.write_text(text)
        result = lab.migrate(state,'apply',path,target=target)
        require(result.returncode == 0 and json.loads(result.stdout)['current_version'] == str(target),'migration')
        lab.sql(state,database,'GRANT USAGE ON SCHEMA control TO bigbrotr_control; GRANT SELECT ON ALL TABLES IN SCHEMA control TO bigbrotr_control; GRANT INSERT ON control.datasets, control.config_revisions TO bigbrotr_control; GRANT UPDATE(config_revision) ON control.datasets TO bigbrotr_control;')
        if target == 801:
            lab.sql(state,database,'GRANT INSERT ON control.principals, control.management_audit TO bigbrotr_control;')
        return database,path,local

    def contents(database):
        names = json.loads(lab.sql(state,database,"SELECT json_agg(tablename ORDER BY tablename) FROM pg_tables WHERE schemaname='control';"))
        return {name:json.loads(lab.sql(state,database,f"SELECT coalesce(json_agg(t),'[]') FROM control.{name} t;")) for name in names}

    def verify(database, local, present, mode='managed'):
        tables = contents(database)
        require(len(tables)==25,'relation-inventory')
        require([r['migration'] for r in sorted(tables['schema_versions'],key=lambda r:r['migration'])] == [1,2,801],'schema-history')
        for row in tables['schema_versions']:
            require(row['checksum']=='\\x'+sha((SOURCE/'migrations/postgres'/row['name']).read_bytes()),'migration-bytes')
            require(row['reader_min']==row['reader_max']==row['writer_floor']==1,'compatibility')
            require(row['applied_by']=='bigbrotr_migrator','migration-actor')
        populated = {'schema_versions','datasets','config_revisions','principals','management_audit'} if present else {'schema_versions'}
        require(all(not rows for name,rows in tables.items() if name not in populated),'unintended-effects')
        if present:
            require(all(len(tables[n])==1 for n in populated-{'schema_versions'}),'initial-row-counts')
            d,c,p,a=(tables[n][0] for n in ['datasets','config_revisions','principals','management_audit'])
            import tomllib
            roots=tomllib.loads(local.read_text())['topology']
            require(d['dataset_id']=='\\x'+roots['dataset_id'] and d['instance_id']=='\\x'+roots['instance_id'],'deployment-identity')
            require(d['singleton'] and d['config_revision']==1 and d['policy_revision'] is None and d['topology_generation'] is None,'no-runtime-heads')
            require(c['dataset_id']==p['dataset_id']==a['dataset_id']==d['dataset_id'],'dataset-binding')
            require(c['revision']==1 and c['previous_revision'] is None and c['schema_version']==3 and c['authority_mode']==mode,'initial-config')
            require(c['document']==policy and c['document_digest']=='\\x'+fingerprint and c['accepted_source_digest']=='\\x'+sha(local.read_bytes()),'complete-frozen-policy')
            require(c['actor_id']==p['principal_id']==a['actor_id'] and p['kind']=='installation' and p['database_role']=='bigbrotr_control' and p['local_os_uid']==os.getuid(),'attribution')
            require(d['created_at_us']==c['accepted_at_us']==p['created_at_us']==a['created_at_us'],'atomic-time')
            require(a['action']=='dataset.initialize' and a['outcome']=='accepted' and a['config_revision']==1,'audit-binding')
            require(len(p['principal_id'])==len(a['audit_id'])==34 and p['principal_id']!='\\x'+'00'*16 and a['audit_id']!='\\x'+'00'*16,'random-identities')
        return tables

    def call(name,database,path,code,status=None,unchanged=False):
        before=lab.schema_digest(state,database)
        result=lab.cli(state,'config','initialize','--file',path)
        value=json.loads(result.stdout) if result.stdout else None
        after=lab.schema_digest(state,database)
        passed=result.returncode==code and (status is None or value['status']==status) and (not unchanged or before==after)
        if code==0:
            passed=passed and not result.stderr and value['initial_revision']=='1' and value['initial_policy_sha256']==fingerprint and value['owner_enrollment']=='not-performed' and value['runtime_authorization']=='not-granted' and value['applied_revision'] is None and value['security_revision'] is None
        report['cases'].append({'name':name,'status':'PASS' if passed else 'FAIL','exit':result.returncode,'stdout':value,'stderr':result.stderr,'before':before,'after':after});save()
        require(passed,'case-'+name)
        return value

    try:
        database,path,local=prepare('basic')
        initial=call('initial',database,path,0,'initialized')
        report['initial_tables']=verify(database,local,True)
        again=call('retry',database,path,0,'already-initialized',True)
        require(initial['installation_actor']==again['installation_actor'] and initial['audit_id']==again['audit_id'],'stable-receipt')
        original=local.read_text()
        for name,suffix,drift in [('comment','\n# comment\n',False),('policy','\n[ingest]\nmax_event_bytes="512KiB"\n',True)]:
            local.write_text(original+suffix)
            value=call(name,database,path,0,'already-initialized',True)
            require(value['source_file_differs'] and value['candidate_policy_differs']==drift,'drift')
        for field in ['dataset_id','instance_id']:
            local.write_text(lab.change(original,'topology',field,'ff'*16))
            call('wrong-'+field,database,path,1,unchanged=True)
        local.write_text(lab.change(original,'config','authority_mode','declarative'))
        call('mode-mismatch',database,path,1,unchanged=True)
        local.write_text(lab.change(lab.change(original,'postgres.control','user','bigbrotr_migrator'),'postgres.control','password_ref','file:/lab/secrets/migrator'))
        call('migration-privileges',database,path,1,unchanged=True)
        local.write_text(original)
        for table in ['principals','management_audit']:
            for verb in ['UPDATE','DELETE']:
                before=lab.schema_digest(state,database)
                statement=f'UPDATE control.{table} SET created_at_us=created_at_us+1' if verb=='UPDATE' else f'DELETE FROM control.{table}'
                lab.sql(state,database,f"DO $case$ BEGIN BEGIN {statement}; RAISE EXCEPTION 'unexpected mutation'; EXCEPTION WHEN check_violation THEN NULL; END; END $case$;")
                require(before==lab.schema_digest(state,database),'immutable-'+table)
                report['cases'].append({'name':table+'-'+verb.lower(),'status':'PASS','expected_sqlstate':'23514','unchanged_dump':before});save()
        inspected=lab.cli(state,'config','inspect','--file',path)
        require(inspected.returncode==0 and json.loads(inspected.stdout)['accepted_policy_sha256']==fingerprint,'actual-reader')
        lab.docker('restart',state['postgres'])
        end=time.monotonic()+15
        while lab.docker('exec',state['postgres'],'pg_isready','-U','postgres',check=False).returncode:
            require(time.monotonic()<end,'restart-timeout');time.sleep(.1)
        call('restart',database,path,0,'already-initialized',True)
        require(contents(database)==report['initial_tables'],'restart-exact-state')
        database,path,local=prepare('declarative',declarative=True)
        call('declarative',database,path,0,'initialized');verify(database,local,True,'declarative')
        database,path,local=prepare('old',target=2)
        call('missing-migration',database,path,1,unchanged=True)
        database,path,local=prepare('denied')
        lab.sql(state,database,'REVOKE INSERT ON control.management_audit FROM bigbrotr_control;')
        call('denied-audit-insert',database,path,1,unchanged=True);verify(database,local,False)
        database,path,local=prepare('partial')
        import tomllib
        roots=tomllib.loads(local.read_text())['topology']
        lab.sql(state,database,f"INSERT INTO control.datasets(dataset_id,instance_id,created_at_us) VALUES(decode('{roots['dataset_id']}','hex'),decode('{roots['instance_id']}','hex'),123);")
        call('partial-existing',database,path,1,unchanged=True)
        # Each fault uses a distinct owned process/container in the PG network namespace.
        for scenario,mode in [('before','before-forward'),('after','after-complete'),('race','hold'),('killed','hold')]:
            database,path,local=prepare(scenario)
            container=state['client']+'-'+scenario
            require(lab.docker('container','inspect',container,check=False).returncode!=0,'existing-fault-container')
            record={'name':container,'owner':state['owner'],'state':'creating'};report['fault_containers'].append(record);save()
            lab.docker('run','-d','--init','--name',container,'--label','org.bigbrotr.initialization-fault='+state['owner'],
                '--network','container:'+state['postgres'],'--user',f'{os.getuid()}:{os.getgid()}',
                '--cpus','2','--memory','1g','--memory-swap','1g','--pids-limit','128','--read-only','--cap-drop','ALL','--security-opt','no-new-privileges',
                '--mount',f'type=bind,src={root},dst=/lab','--mount',f'type=bind,src={build},dst=/build,readonly',
                '--mount',f'type=bind,src={SOURCE},dst=/source,readonly',state['client_image'],'python3','-c','import time; time.sleep(180)')
            record['id']=json.loads(lab.docker('inspect',container).stdout)[0]['Id'];record['state']='running';save()
            processes=[]
            try:
                proxy=Process(['docker','exec','-i',container,'python3','-B','/source/experiments/control-migrations/fault_proxy.py','--upstream-port','5432','--mode',mode,'--deadline-seconds','60'],input_pipe=True);processes.append(proxy)
                ready=proxy.event('ready',10)
                require(ready['proxy_sha256']==sha((SOURCE/'experiments/control-migrations/fault_proxy.py').read_bytes()),'proxy-source')
                proxyfile=root/(database+'_proxy.toml')
                proxyfile.write_text(lab.change(lab.change(local.read_text(),'postgres.control','endpoint',f'127.0.0.1:{ready["listen_port"]}'),'postgres.control.tls','local_boundary','loopback'))
                app=Process(['docker','exec',container,'/build/bigbrotr','config','initialize','--file','/lab/'+proxyfile.name]);processes.append(app)
                case={'name':scenario,'status':'RUNNING'};report['cases'].append(case);save()
                if mode=='hold':
                    proxy.event('held',15)
                    verify(database,local,False)
                    if scenario=='race':
                        other=Process(['docker','exec',state['client'],'/build/bigbrotr','config','initialize','--file',path]);processes.append(other)
                        end=time.monotonic()+3
                        while True:
                            locks=json.loads(lab.sql(state,database,"SELECT coalesce(json_agg(json_build_object('pid',pid,'mode',mode,'granted',granted)),'[]') FROM pg_locks WHERE locktype='advisory' AND classid=1111651442 AND objid=1229867348;"))
                            if sum(r['granted'] and r['mode']=='ExclusiveLock' for r in locks)==1 and any(not r['granted'] for r in locks):break
                            require(time.monotonic()<end,'initializer-wait-not-observed');time.sleep(.05)
                        case['held_locks']=locks
                        proxy.send('release')
                        require(other.wait(15)==0,'race-exit')
                        case['other']=json.loads(other.stdout)
                        require(case['other']['status']=='already-initialized' and not other.stderr,'race-replay')
                    else:
                        lab.docker('kill',container)
                code=app.wait(15)
                case['exit']=code
                if scenario!='killed':
                    case['output']=json.loads(app.stdout);require(not app.stderr,'fault-cli-stderr')
                    require(code==(0 if scenario=='race' else 4),'fault-exit')
                    require(case['output']['status']==('initialized' if scenario=='race' else 'outcome-unknown'),'fault-outcome')
                    receipt=proxy.event('receipt',10);case['proxy']=receipt
                    counts=[receipt[k] for k in ['commits_seen','commits_forwarded','server_commit_complete','client_commit_complete','faults']]
                    require(counts=={'before':[1,0,0,0,1],'after':[1,1,1,0,1],'race':[1,1,1,1,1]}[scenario],'actual-commit-boundary')
                    require(proxy.wait(5)==0 and not proxy.stderr,'proxy-completion')
                    if scenario=='race':require(case['other']['audit_id']==case['output']['audit_id'],'race-audit')
                else:require(code!=0,'killed-process-success')
                present=scenario in ['after','race']
                # Drain/rollback may finish after transport closure, so serialize
                # with the real initializer before relying on a fresh state read.
                call('resolve-'+scenario,database,path,0,'already-initialized' if present else 'initialized',present)
                # Fault path changes only the deployment endpoint/source digest.
                accepted_local=proxyfile if present else local
                verify(database,accepted_local,True)
                case['status']='PASS';save()
            finally:
                info=json.loads(lab.docker('inspect',container).stdout)[0]
                require(info['Id']==record['id'] and info['Config']['Labels']['org.bigbrotr.initialization-fault']==state['owner'],'fault-owner')
                if info['State']['Running']:lab.docker('stop','--time','3',container)
                for process in processes:process.stop()
                info=json.loads(lab.docker('inspect',container).stdout)[0]
                record['state']='stopped-retained';record['terminal']=info['State'];save()
        for database in databases:
            require(int(lab.sql(state,database,"SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND usename IN ('bigbrotr_control','bigbrotr_migrator');"))==0,'remaining-backend')
        require(len(report['cases'])==25,'case-count')
        report['status']='PASS'
    except Exception as error:
        report['status']='FAIL';report['failure']=str(error);raise
    finally:save()
    print(json.dumps({'status':report['status'],'cases':len(report['cases']),'report':str(output)}))


if __name__=='__main__':main()
