#!/usr/bin/env python3
"""Actual credential/native/fault qualification in an explicitly owned Tux lab.

No secrets or SQL payloads enter the report. Host controller owns all docker exec
wrappers; only the enclosing owner may retire the runner/PG/network afterwards.
"""
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import time

sys.dont_write_bytecode = True
SOURCE = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(SOURCE / 'experiments/control-migrations'))
from fault_cases import Process


def require(value, reason):
    if not value:
        raise RuntimeError(reason)


def command(*args, input=None, timeout=60):
    result = subprocess.run(args, input=input, text=True, capture_output=True, timeout=timeout)
    require(result.returncode == 0, 'command-refused')
    require(len(result.stdout) <= 1024 * 1024, 'command-output-bound')
    return result.stdout.strip()


def main():
    root = Path(sys.argv[1]).resolve()
    state = json.loads((root / 'resources.json').read_text())
    runner, pg = state['container_id'], state['postgres_id']
    require(SOURCE == root / 'source', 'source-root')
    require(command('git', '-C', str(SOURCE), 'rev-parse', 'HEAD') == state['source'], 'revision')
    require(not command('git', '-C', str(SOURCE), 'status', '--porcelain', '--untracked-files=all'), 'dirty-source')
    for identifier in [runner, pg]:
        info = json.loads(command('docker', 'inspect', identifier))[0]
        require(info['Config']['Labels']['org.bigbrotr.catalog-credentials'] == state['owner'] and info['State']['Running'], 'resource-owner')
    ready = json.loads((root / 'out/native-tests/ready.json').read_text())
    require(ready['source'] == state['source'], 'artifact-source')
    report = {'source':state['source'], 'artifacts':ready, 'status':'RUNNING', 'cases':[], 'scope':'actual SCRAM credential lifetime, supervised finite native attach, role/owner atomicity and seal; synthetic input, no science/ACK or persistent process recovery'}
    path = root / 'credentials.json'
    require(not path.exists(), 'existing-report')
    children = []
    database = 'bbcat_credentials'
    def save():
        temporary = path.with_suffix('.new')
        temporary.write_text(json.dumps(report, indent=2) + '\n')
        temporary.replace(path)
    def sql(text):
        return command('docker','exec','-i',pg,'psql','-XAtq','-v','ON_ERROR_STOP=1','-U','postgres','-d',database,input=text)
    def login(ordinal):
        return 'bb_archive_' + bytes([18+2*ordinal]*16).hex()
    def snapshot():
        value = json.loads(sql("SELECT json_build_object('owner',(SELECT row_to_json(o) FROM bigbrotr_archive.owner o),'attempts',(SELECT coalesce(json_agg(a ORDER BY ordinal),'[]') FROM bigbrotr_archive.attempts a),'roles',(SELECT coalesce(json_agg(r ORDER BY rolname),'[]') FROM (SELECT oid,rolname,rolcanlogin,rolpassword IS NULL AS password_absent,coalesce(rolpassword LIKE 'SCRAM-SHA-256$4096:%',false) AS password_scram,NOT rolsuper AND NOT rolcreaterole AND NOT rolcreatedb AND NOT rolreplication AND NOT rolbypassrls AND NOT rolinherit AS restricted,rolconnlimit FROM pg_authid WHERE rolname LIKE 'bb_archive_%') r));"))
        return value
    def expected(ordinals, head, enabled):
        value = snapshot()
        require(value['owner'] == {'singleton':True, 'ordinal':head, 'enabled':enabled}, 'owner-state')
        require([a['ordinal'] for a in value['attempts']] == ordinals, 'retained-attempt-set')
        require(len(value['roles']) == len(ordinals), 'role-set')
        roles = {r['rolname']:r for r in value['roles']}
        for attempt in value['attempts']:
            n = attempt['ordinal']; role = roles[login(n)]
            require(attempt['login_name'] == login(n) and int(attempt['login_oid']) == int(role['oid']), 'role-oid')
            require(attempt['attempt_id'] == '\\x'+bytes([18+2*n]*16).hex() and attempt['worker_incarnation'] == '\\x'+bytes([19+2*n]*16).hex() and attempt['coordinator_incarnation'] == '\\x'+'50'*16, 'retained-identity')
            require(attempt['topology_generation'] == 0 and attempt['batch_digest'] == '\\x6617e55295d367ffc9453a93ceb054623a952d289dcbf8e6ad5f5332db9134d0', 'independent-input-identity')
            require(role['restricted'] and role['rolconnlimit'] == 8, 'role-restrictions')
            active = n == head and enabled
            require(role['rolcanlogin'] == active and role['password_scram'] == active and role['password_absent'] != active, 'credential-seal-state')
            rights = json.loads(sql("SELECT json_build_object('dml',(SELECT count(*) FROM pg_tables t CROSS JOIN (VALUES('SELECT'),('INSERT'),('UPDATE'),('DELETE'))p(v) WHERE t.schemaname='public' AND has_table_privilege('"+login(n)+"',quote_ident(t.schemaname)||'.'||quote_ident(t.tablename),p.v)), 'forbidden',(SELECT count(*) FROM pg_tables t CROSS JOIN (VALUES('TRIGGER'),('TRUNCATE'))p(v) WHERE t.schemaname='public' AND has_table_privilege('"+login(n)+"',quote_ident(t.schemaname)||'.'||quote_ident(t.tablename),p.v)), 'members',(SELECT json_agg(json_build_object('member',p.rolname,'admin',m.admin_option,'inherit',m.inherit_option,'set',m.set_option)) FROM pg_auth_members m JOIN pg_roles p ON p.oid=m.member WHERE m.roleid='"+login(n)+"'::regrole));"))
            require(rights == {'dml':112, 'forbidden':0, 'members':[{'member':'CatalogManager','admin':True,'inherit':False,'set':False}]}, 'complete-role-privileges-membership')
        require(sql("SELECT count(*)=28 AND bool_and(c.relowner='\"CatalogManager\"'::regrole AND t.tgenabled='O') FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace JOIN pg_trigger t ON t.tgrelid=c.oid AND t.tgname='bigbrotr_writer_guard' WHERE n.nspname='public';") == 't', 'all-native-owned-and-guarded')
        require(sql("SELECT NOT has_schema_privilege('BootstrapMixed','public','CREATE') AND count(*)=0 FROM pg_tables t WHERE schemaname='public' AND has_table_privilege('BootstrapMixed',quote_ident(t.schemaname)||'.'||quote_ident(t.tablename),'INSERT,UPDATE,DELETE,TRIGGER,TRUNCATE');") == 't', 'bootstrap-revoked')
        return value
    def start(args, pipe=False):
        p = Process(['docker','exec']+(['-i'] if pipe else [])+[runner]+args,input_pipe=pipe)
        children.append(p)
        return p
    def event(app, index, timeout=15):
        end = time.monotonic()+timeout
        while time.monotonic()<end:
            with app.condition: lines = bytes(app.stdout).splitlines()
            require(not app.overflow, 'output-bound')
            if len(lines)>index:
                value=json.loads(lines[index]); require(set(value) in [{'event','pid','start'}, {'event','outcome','commit_started'}], 'closed-probe-output')
                return value
            require(app.process.poll() is None, 'missing-probe-event')
            time.sleep(.01)
        raise RuntimeError('event-deadline')
    def probe(mode, ordinal=0, port=5432):
        p=start(['/out/native-tests/catalog-credentials-probe',mode,database,str(ordinal),str(port)])
        return p,event(p,0)
    def finish(p, outcome, committed):
        require(p.wait(20)==0 and not p.stderr, 'probe-terminal')
        require(event(p,1)=={'event':'outcome','outcome':outcome,'commit_started':committed}, 'guard-outcome')
    def direct(mode, ordinal, outcome, committed):
        p,_=probe(mode,ordinal); finish(p,outcome,committed)
    def proxy(mode):
        p=start(['python3','-B','/source/experiments/control-migrations/fault_proxy.py','--upstream-port','5432','--mode',mode,'--deadline-seconds','60'],True)
        ready=p.event('ready',10)
        require(ready['proxy_sha256']==hashlib.sha256((SOURCE/'experiments/control-migrations/fault_proxy.py').read_bytes()).hexdigest(),'proxy-source')
        return p,ready['listen_port']
    def receipt(p, counts):
        value=p.event('receipt',10); require(p.wait(5)==0 and not p.stderr,'proxy-terminal')
        require([value[k] for k in ['commits_seen','commits_forwarded','server_commit_complete','client_commit_complete','faults']]==counts,'actual-wire-boundary')
        return value
    def blocked(holder, waiter):
        end=time.monotonic()+3
        while time.monotonic()<end:
            rows=json.loads(sql("SELECT coalesce(json_agg(t),'[]') FROM (SELECT pid,backend_start::text,state,wait_event_type,pg_blocking_pids(pid) AS blockers FROM pg_stat_activity WHERE pid IN ("+str(holder['pid'])+','+str(waiter['pid'])+'))t;'))
            by_pid={r['pid']:r for r in rows}
            if waiter['pid'] in by_pid and holder['pid'] in by_pid[waiter['pid']]['blockers']:
                require(by_pid[holder['pid']]['backend_start']==holder['start'] and by_pid[waiter['pid']]['backend_start']==waiter['start'],'backend-incarnations')
                require(by_pid[holder['pid']]['state']=='idle in transaction' and by_pid[waiter['pid']]['wait_event_type']=='Lock','actual-owner-wait')
                return rows
            time.sleep(.02)
        raise RuntimeError('no-blocking-observation')
    def record(item): report['cases'].append(item); save()
    save()
    try:
        command('docker','exec','-i',pg,'psql','-Xq','-v','ON_ERROR_STOP=1','-U','postgres',input='CREATE DATABASE '+database+'; ALTER ROLE "CatalogManager" SUPERUSER;')
        (root/'out'/('archive_'+database)).mkdir()
        sql('GRANT CREATE ON SCHEMA public TO "BootstrapMixed";')
        native=json.loads(command('docker','exec',runner,'/out/native-tests/catalog-credentials-probe','bootstrap',database,'0','5432'))
        require(native=={'event':'bootstrap','status':'PASS'},'supervised-bootstrap')
        direct('install',0,'installed',True)
        sql('ALTER ROLE "CatalogManager" NOSUPERUSER CREATEROLE;')
        lifecycle=command('docker','exec',runner,'/out/native-tests/catalog-credentials-probe','lifecycle',database,'0','5432')
        events=[json.loads(line) for line in lifecycle.splitlines()]
        require([e['event'] for e in events]==['authenticated']+['membership-refused']*4+['sealed','native-reaped','lifecycle'],'lifecycle-events')
        record({'name':'lifecycle','status':'PASS','events':events,'observed':expected([1,2],2,False)})
        ordinals=[1,2]
        for ordinal,mode,fault in [(3,'provision','before-forward'),(4,'provision','after-complete'),(5,'seal','before-forward'),(6,'seal','after-complete')]:
            report['active']=f'{mode}_{fault}'; save()
            if mode=='seal':
                direct('provision',ordinal,'provisioned-discarded',True); ordinals.append(ordinal)
            before=snapshot()
            p,port=proxy(fault); app,backend=probe(mode,ordinal,port); finish(app,'unknown',True)
            wire=receipt(p,[1,0,0,0,1] if fault=='before-forward' else [1,1,1,0,1])
            observed=snapshot()
            if fault=='before-forward': require(observed==before,'rollback-was-not-atomic')
            if mode=='provision':
                if fault=='after-complete':
                    ordinals.append(ordinal)
                    require(expected(ordinals,ordinal,True)==observed,'completed-provision-state')
                # Compare without retaining any verifier or its fingerprint.
                fingerprint=sql("SELECT md5(rolpassword) FROM pg_authid WHERE rolname='"+login(ordinal)+"';")
                direct('provision',ordinal,'provisioned-discarded' if fault=='before-forward' else 'conflict',fault=='before-forward')
                if fault=='after-complete': require(fingerprint==sql("SELECT md5(rolpassword) FROM pg_authid WHERE rolname='"+login(ordinal)+"';"),'retry-reset-password')
                else: ordinals.append(ordinal)
                active=expected(ordinals,ordinal,True)
                direct('seal',ordinal,'sealed',True)
            else:
                if fault=='after-complete': require(expected(ordinals,ordinal,False)==observed,'completed-seal-state')
                direct('seal',ordinal,'sealed' if fault=='before-forward' else 'historical',True)
                if fault=='after-complete': require(snapshot()==observed,'seal-retry-repaired-state')
                active=None
            record({'name':report['active'],'status':'PASS','backend':backend,'wire':wire,'observed':observed,'active':active,'resolved':expected(ordinals,ordinal,False)})
        for ordinal,action in [(7,'release'),(8,'close')]:
            report['active']='same-intent-'+action; save()
            before=expected(ordinals,ordinal-1,False)
            p,port=proxy('hold'); first,holder=probe('provision',ordinal,port); p.event('held',10)
            second,waiter=probe('provision',ordinal)
            observation=blocked(holder,waiter)
            require(snapshot()==before,'uncommitted-attempt-visible')
            p.send(action)
            finish(first,'provisioned-discarded' if action=='release' else 'unknown',True)
            finish(second,'conflict' if action=='release' else 'provisioned-discarded',action=='close')
            wire=receipt(p,[1,1,1,1,1] if action=='release' else [1,0,0,0,1])
            ordinals.append(ordinal); active=expected(ordinals,ordinal,True)
            direct('seal',ordinal,'sealed',True)
            record({'name':report['active'],'status':'PASS','blocking':observation,'wire':wire,'active':active,'resolved':expected(ordinals,ordinal,False)})
        # Existing cluster-wide role collision cannot be adopted or reset.
        sql('CREATE ROLE '+login(9)+' NOLOGIN;')
        before=snapshot(); direct('provision',9,'conflict',False); require(snapshot()==before,'preexisting-role-mutated')
        sql('DROP ROLE '+login(9)+';')  # exact newly owned, no grants/client ever issued
        record({'name':'preexisting-role','status':'PASS','resolved':expected(ordinals,8,False)})
        require(sql("SELECT count(*) FROM pg_stat_activity WHERE application_name IN ('bigbrotr-catalog-recovery-probe','bigbrotr-credential-auth-probe');")=='0','remaining-probe-backends')
        report.update(status='PASS',active=None,remaining_probe_backends=0)
        save()
    except BaseException:
        report['status']='FAIL_OR_UNKNOWN'; save(); raise
    finally:
        for child in children: child.stop()


if __name__=='__main__': main()
