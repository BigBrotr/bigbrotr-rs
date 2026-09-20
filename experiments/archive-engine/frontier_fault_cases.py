#!/usr/bin/env python3
"""Real restart, transport and manager-loss cases on retained signed native state.

Only the closed qualification fixture is rebuilt; product/artifact inputs remain
at the baseline. This consumes no pin and gives no source reclamation authority.
"""
import argparse
import hashlib
import json
from pathlib import Path
import sys
import time
sys.dont_write_bytecode = True
from signed_publication_cases import command, event, execute, send, sha, Process, proxy_observation
from event_cases import cut_expectation


def verify_added_frontier(case, op, revision):
    """Exact additions plus only declared head/owner/role transitions."""
    before,after=case['before'],case['after']; expected=cut_expectation('normal')
    dataset,catalog='\\x'+'01'*16,'\\x'+'5a'*16
    issuer=case['issuer']; incarnation='\\x'+case['ready']['incarnation']
    assert issuer['process_incarnation']==incarnation and issuer['role']=='control' and issuer['state']=='active'
    previous=max(before['owners'],key=lambda o:o['epoch'])
    epoch=previous['epoch']+1
    owner=[o for o in after['owners'] if o['epoch']==epoch]
    assert len(owner)==1; owner=owner[0]; xmin=owner['xmin']
    assert owner==dict(resource_id=catalog,epoch=epoch,dataset_id=dataset,role='archive',
        role_instance_id=owner['role_instance_id'],process_incarnation=incarnation,
        acquired_at_us=owner['acquired_at_us'],lease_expires_at_us=issuer['lease_expires_at_us'],
        state='active',released_at_us=None,exclusion_method=None,exclusion_digest=None,
        archive_frontier_protocol=1,xmin=xmin)
    assert issuer['acquired_at_us']<=owner['acquired_at_us']<owner['lease_expires_at_us']
    for old in before['owners']:
        actual=next(o for o in after['owners'] if o['epoch']==old['epoch'])
        if old!=previous:assert actual==old
        else:
            digest=hashlib.sha256(b'bigbrotr/archive-frontier-owner-exclusion/1\0'+bytes.fromhex('01'*16+'5a'*16)+
                old['epoch'].to_bytes(8,'big')+bytes.fromhex(old['process_incarnation'][2:])+
                bytes.fromhex(issuer['resource_id'][2:])+issuer['epoch'].to_bytes(8,'big')+
                bytes.fromhex(incarnation[2:])+bytes.fromhex(expected['observation']['request_digest'])).hexdigest()
            assert actual==dict(old,state='released',released_at_us=actual['released_at_us'],
                exclusion_method='storage_fenced',exclusion_digest='\\x'+digest,xmin=xmin)
            assert old['acquired_at_us']<=actual['released_at_us']<=owner['acquired_at_us']
    assert len(after['owners'])==len(before['owners'])+1
    roles={r['role_instance_id']:r for r in after['roles']}
    for old in before['roles']:
        actual=roles.pop(old['role_instance_id'])
        if old['role_instance_id']==previous['role_instance_id']:
            assert actual==dict(old,retired_at_us=actual['retired_at_us'],xmin=xmin)
            assert old['created_at_us']<=actual['retired_at_us']<=owner['acquired_at_us']
        else:assert actual==old
    assert len(roles)==1; role=roles[owner['role_instance_id']]
    assert role==dict(role_instance_id=owner['role_instance_id'],dataset_id=dataset,role='archive',
        created_at_us=role['created_at_us'],retired_at_us=None,xmin=xmin)
    assert issuer['acquired_at_us']<=role['created_at_us']<=owner['acquired_at_us']
    assert len(before['consumers'])==len(after['consumers'])==1
    assert before['consumers'][0]['current_revision']==revision-1
    assert after['consumers']==[dict(before['consumers'][0],current_revision=revision,current_topology_generation=0,xmin=xmin)]
    def added(table,key):
        old={tuple(r[k] for k in key):r for r in before[table]}
        new={tuple(r[k] for k in key):r for r in after[table]}
        for k,r in old.items():assert new.pop(k)==r
        return list(new.values())
    sessions=added('archive_owner_sessions',['catalog_id','owner_epoch'])
    assert sessions==[dict(dataset_id=dataset,catalog_id=catalog,owner_epoch=epoch,owner_role='archive',
        owner_incarnation=incarnation,coordinator_binding_sha256=before['archive_owner_sessions'][0]['coordinator_binding_sha256'],
        persistence_protocol=1,issuer_resource=issuer['resource_id'],issuer_epoch=issuer['epoch'],
        issuer_role='control',issuer_incarnation=incarnation,xmin=xmin)]
    revisions=added('consumer_frontier_revisions',['consumer_id','frontier_revision'])
    assert len(revisions)==1; at=revisions[0]['created_at_us']
    assert owner['acquired_at_us']<=at<owner['lease_expires_at_us']
    common=dict(consumer_id=catalog,frontier_revision=revision,dataset_id=dataset,topology_generation=0)
    assert revisions==[dict(common,consumer_role='archive',previous_revision=revision-1,
        owner_resource_id=catalog,owner_epoch=epoch,owner_incarnation=incarnation,created_at_us=at,sealed=True,xmin=xmin)]
    assert added('archive_frontier_snapshots',['consumer_id','frontier_revision'])==[
        dict(common,operation_id='\\x'+f'{op:02x}'*16,catalog_id=catalog,owner_epoch=epoch,
             owner_incarnation=incarnation,snapshot_id=3,request_digest='\\x'+expected['observation']['request_digest'],
             dictionary_count=6,payload_count=1,eligibility_count=1,receive_count=1,provenance_count=1,xmin=xmin)]
    assert added('consumer_frontiers',['consumer_id','frontier_revision','shard_id','owner_epoch'])==[
        dict(common,shard_id=0,owner_epoch=1,last_complete_sequence=10,attestation_kind='archive-batch',
             attestation_digest='\\x'+expected['read']['receipts'][-1]['batch_digest'],xmin=xmin)]
    batches=added('archive_frontier_batches',['consumer_id','frontier_revision','attempt_id'])
    assert sorted(batches,key=lambda r:r['attempt_id'])==[
        dict(consumer_id=catalog,frontier_revision=revision,attempt_id='\\x'+bytes(r['attempt_id']).hex(),xmin=xmin)
        for r in expected['read']['receipts']]


def main(root, namespace, base_revision, operation_base, recovery_operation):
    assert namespace.isalpha() and namespace.islower() and len(namespace)<=12
    assert base_revision>=1 and 1<=operation_base<=249
    source = Path(__file__).resolve().parents[2]
    state = json.loads((root/'resources.json').read_text())
    assert source == root/'harness-source'
    assert not command('git','-C',str(source),'status','--porcelain','--untracked-files=all')
    build = root/'out/frontier-fault-build'
    ready = json.loads((build/'ready.json').read_text())
    harness_revision=command('git','-C',str(source),'rev-parse','HEAD')
    changed=command('git','-C',str(source),'diff','--name-only',ready['source'],harness_revision).splitlines()
    assert all(p.startswith('docs/') or p=='experiments/archive-engine/frontier_fault_cases.py' for p in changed)
    assert ready['product_source'] == state['source']
    assert sha(build/'archive-publication-probe') == ready['artifact_sha256']
    for key in ['container_id','postgres_id']:
        c = json.loads(command('docker','inspect',state[key]))[0]
        assert c['Config']['Labels']['org.bigbrotr.archive-event']==state['owner']
        assert c['State']['Running'] and not c['State']['OOMKilled']
    assert json.loads((root/'cut-cases.json').read_text())['status']=='PASS'
    runner,pg = state['container_id'],state['postgres_id']
    directory = root/'out'/('frontier-fault-'+namespace)
    directory.mkdir(mode=0o700)
    report_path = root/('frontier-fault-cases-'+namespace+'.json')
    assert not report_path.exists()
    report = dict(status='INCOMPLETE',source=state['source'],harness=ready,cases=[],
                  harness_source=harness_revision,changed_paths=changed,base_revision=base_revision,
                  operation_base=operation_base,recovery_operation=recovery_operation,
                  proxy_sha256=sha(source/'experiments/archive-engine/native_commit_proxy.py'))
    def save(): report_path.write_text(json.dumps(report,indent=2)+'\n')
    def sql(query,db='bbpub_control_normal'):
        return command('docker','exec','-i',pg,'psql','-XAtq','-v','ON_ERROR_STOP=1','-U','postgres','-d',db,input=query)
    def rows():
        tables = ['consumers','consumer_frontier_revisions','consumer_frontiers',
                  'archive_frontier_snapshots','archive_frontier_batches','archive_owner_sessions']
        result = {t:json.loads(sql(f"SELECT coalesce(jsonb_agg(to_jsonb(t) ORDER BY to_jsonb(t)::text),'[]') FROM (SELECT *,xmin::text AS xmin FROM control.{t})t")) for t in tables}
        result['owners'] = json.loads(sql("SELECT coalesce(jsonb_agg(to_jsonb(t) ORDER BY to_jsonb(t)::text),'[]') FROM (SELECT *,xmin::text AS xmin FROM control.owner_epochs WHERE role='archive')t"))
        result['roles'] = json.loads(sql("SELECT coalesce(jsonb_agg(to_jsonb(t) ORDER BY to_jsonb(t)::text),'[]') FROM (SELECT *,xmin::text AS xmin FROM control.role_instances WHERE role='archive')t"))
        return result
    def no_clients():
        until = time.monotonic()+6
        while True:
            value = json.loads(sql("SELECT coalesce(json_agg(json_build_object('pid',pid,'user',usename,'database',datname,'state',state)),'[]') FROM pg_stat_activity WHERE backend_type='client backend' AND pid<>pg_backend_pid()",'postgres'))
            if not value: return
            assert time.monotonic()<until,value
            time.sleep(.03)
    def fixture(label,op,revision,*,lookup=False,proxy_mode=None,manager_loss=False,blocked=False,cancel=False):
        label=namespace+'-'+label
        case=dict(name=label,status='INCOMPLETE',before=rows());report['cases'].append(case);save()
        proxy=holder=None
        try:
            port=5432
            if proxy_mode:
                proxy=Process(['docker','exec','-i',runner,'python3','-B',
                    '/out/frontier-fault-source/experiments/archive-engine/native_commit_proxy.py',
                    '--target','control-frontier','--mode',proxy_mode],input_pipe=True)
                started=case['proxy_ready']=event(proxy,'ready')
                assert started['source_sha256']==report['proxy_sha256']
                port=started['port']
            holder_pid=None
            if blocked:
                holder=Process(['docker','exec','-i',pg,'psql','-XAtq','-v','ON_ERROR_STOP=1','-U','postgres','-d','bbpub_control_normal'],input_pipe=True)
                send(holder,"SET idle_in_transaction_session_timeout='30s'; SET statement_timeout='30s'; BEGIN; LOCK TABLE control.archive_frontier_snapshots IN SHARE MODE; SELECT json_build_object('event','locked','pid',pid,'backend_start',backend_start::text) FROM pg_stat_activity WHERE pid=pg_backend_pid();\n")
                case['holder_ready']=event(holder,'locked');save()
                holder_pid=case['holder_ready']['pid']
            request=dict(case=label,operation=op,expected_revision=revision,control_port=port,lookup=lookup,wait=not lookup,cancel=cancel)
            path=directory/(label+'.json');path.write_text(json.dumps(request)+'\n')
            process=case['process']={}
            def interact(p):
                nonlocal holder
                if lookup:return
                case['ready']=event(p,'frontier-ready');save()
                for kind,user,database in [('control','bigbrotr_control','bbpub_control_normal'),('manager','CatalogManager','bbpub_normal')]:
                    observed=json.loads(sql(f"SELECT row_to_json(t) FROM (SELECT pid,backend_start::text,usename,datname FROM pg_stat_activity WHERE pid={case['ready'][kind+'_pid']})t"))
                    assert observed==dict(pid=case['ready'][kind+'_pid'],backend_start=case['ready'][kind+'_start'],usename=user,datname=database)
                case['issuer']=json.loads(sql("SELECT row_to_json(o) FROM control.owner_epochs o WHERE role='control' AND state='active'"));save()
                (root/'out/signed_normal'/('frontier-'+label)/'continue').touch(exist_ok=False)
                if blocked:
                    until=time.monotonic()+8
                    while True:
                        waiting=json.loads(sql(f"SELECT coalesce(json_agg(json_build_object('pid',pid,'user',usename,'blockers',pg_blocking_pids(pid),'wait',wait_event_type)),'[]') FROM pg_stat_activity WHERE pid={case['ready']['control_pid']} AND {holder_pid}=ANY(pg_blocking_pids(pid))"))
                        if waiting:break
                        assert time.monotonic()<until
                        time.sleep(.02)
                    assert waiting[0]['user']=='bigbrotr_control' and waiting[0]['wait']=='Lock'
                    case['blocked']=waiting;save()
                if proxy_mode=='hold-commit':
                    case['held']=event(proxy,'held');save()
                    assert case['held']['stage']=='commit' and case['held']['backend_pid']==case['ready']['control_pid']
                if manager_loss:
                    pid=case['ready']['manager_pid']
                    start=case['ready']['manager_start'];assert "'" not in start
                    assert sql(f"SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE pid={pid} AND backend_start='{start}'::timestamptz AND usename='CatalogManager' AND datname='bbpub_normal'")=='t'
                    until=time.monotonic()+5
                    while sql(f'SELECT count(*) FROM pg_stat_activity WHERE pid={pid}')!='0':
                        assert time.monotonic()<until;time.sleep(.01)
                    case['manager_excluded']=True;save()
                if cancel:
                    (root/'out/signed_normal'/('frontier-'+label)/'cancel').touch(exist_ok=False)
                    case['cancelled']=event(p,'frontier-cancelled')
                    assert not case['cancelled']['commit_started']
                    save()
                    # The actual future has been dropped while SQL was blocked.
                    # Release the blocker so its queued rollback/driver can join.
                if holder:
                    send(holder,'ROLLBACK;\n\\q\n')
                    assert holder.wait(5)==0 and not holder.stderr
                    case['holder']=proxy_observation(holder);holder.stop();holder=None
                if proxy_mode=='hold-commit':send(proxy,'release\n')
            execute(['docker','exec','--env','LD_LIBRARY_PATH=/out/archive-worker/lib',runner,
                     '/out/frontier-fault-build/archive-publication-probe','normal','frontier',
                     '/out/'+directory.name+'/'+path.name],75,process,interact)
            assert process['exit']==0 and not process['stderr'] and not process['overflow'],process
            values=[json.loads(line) for line in process['stdout'].splitlines()]
            observed=values[-1];assert observed['drivers_joined']
            case['observed']=observed;save()
            if proxy:
                send(proxy,'stop\n');assert proxy.wait(5)==0 and not proxy.stderr and not proxy.overflow
                case['proxy']=proxy_observation(proxy)
                terminal=event(proxy,'terminal');assert terminal['unexpected']==0
                assert terminal['data_queries']==terminal['data_commits']==1
                assert terminal['faults']==(0 if proxy_mode=='hold-commit' else 1)
                assert terminal['server_complete']==(0 if proxy_mode=='before-forward' else 1)
                proxy.stop();proxy=None
            no_clients();case['after']=rows();case['observed']=observed;save()
            return case
        finally:
            for label,p in [('proxy',proxy),('holder',holder)]:
                if p:
                    try:
                        send(p,'stop\n' if label=='proxy' else 'ROLLBACK;\n\\q\n')
                        assert p.wait(5)==0
                        if label=='proxy':assert event(p,'terminal')['unexpected']==0
                    except BaseException as error:
                        case[label+'_cleanup_error']=type(error).__name__
                    p.stop();case[label+'_incomplete']=proxy_observation(p)
            if holder and 'holder_ready' in case:
                identity=case['holder_ready'];start=identity['backend_start'];assert "'" not in start
                try:
                    result=sql(f"SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE pid={identity['pid']} AND backend_start='{start}'::timestamptz AND usename='postgres' AND datname='bbpub_control_normal'")
                    case['holder_cleanup_termination']=result
                    no_clients()
                except BaseException as error:case['remote_cleanup_unresolved']=type(error).__name__
            if 'after' not in case:
                try:case['after']=rows()
                except BaseException as error:case['post_state_unavailable']=type(error).__name__
            save()
    def passed(case):case['status']='PASS';save()
    def unchanged(case):assert case['after']==case['before']
    def published(case,op,revision,created):
        expected=cut_expectation('normal')
        assert case['observed']['receipt']==dict(operation_id=f'{op:02x}'*16,revision=revision,
            snapshot_id=3,request_digest=expected['observation']['request_digest'],
            family_counts=[6,1,1,1,1],created=created)
    save()
    try:
        original=fixture('historical',0xf1,0,lookup=True);published(original,0xf1,1,False);unchanged(original);passed(original)
        assert original['after']['consumers'][0]['current_revision']==base_revision
        if recovery_operation is not None:
            recovered=fixture('prior-interruption',recovery_operation,base_revision-1,lookup=True)
            published(recovered,recovery_operation,base_revision,False);unchanged(recovered);passed(recovered)
        revision=base_revision
        for name,op,options,wanted in [
            ('manager-before',operation_base,dict(manager_loss=True,blocked=True),'Unavailable'),
            ('commit-before',operation_base+1,dict(proxy_mode='before-forward'),'CommitUnknown'),
            ('commit-after',operation_base+2,dict(proxy_mode='after-complete'),'CommitUnknown'),
        ]:
            case=fixture(name,op,revision,**options)
            assert case['observed']['outcome']==wanted
            assert case['observed']['commit_started']==(name!='manager-before')
            committed=name=='commit-after'
            if not committed:unchanged(case)
            else:verify_added_frontier(case,op,revision+1)
            passed(case)
            lookup=fixture(name+'-lookup',op,revision,lookup=True);unchanged(lookup)
            if committed:published(lookup,op,revision+1,False)
            else:assert lookup['observed']['receipt'] is None
            passed(lookup)
            retry=fixture(name+'-retry',op,revision);published(retry,op,revision+1,not committed)
            if committed:unchanged(retry)
            else:verify_added_frontier(retry,op,revision+1)
            passed(retry);revision+=1
        case=fixture('manager-after',operation_base+3,revision,manager_loss=True,proxy_mode='hold-commit')
        # A historical result can commit under retained local exclusion. The
        # dead capture is reported; it confers no pin or read authority.
        assert case['observed']['outcome']=='published' and case['observed']['commit_started']
        assert not case['observed']['capture_closed'] and not case['observed']['manager_closed']
        published(case,operation_base+3,revision+1,True);verify_added_frontier(case,operation_base+3,revision+1);passed(case);revision+=1
        retry=fixture('manager-after-retry',operation_base+3,revision-1);published(retry,operation_base+3,revision,False);unchanged(retry);passed(retry)
        case=fixture('cancel-blocked',operation_base+4,revision,blocked=True,cancel=True)
        assert case['observed']['outcome']=='cancelled' and not case['observed']['commit_started']
        unchanged(case);passed(case)
        case=fixture('stale-head',operation_base+5,0)
        assert case['observed']['outcome']=='Conflict' and not case['observed']['commit_started']
        unchanged(case);passed(case)
        report['status']='PASS'
    finally:save()
    print(json.dumps(dict(status=report['status'],cases=len(report['cases']),report=str(report_path))))


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('root',type=Path);parser.add_argument('--namespace',required=True)
    parser.add_argument('--base-revision',type=int,default=1)
    parser.add_argument('--operation-base',type=lambda v:int(v,0),default=0xe1)
    parser.add_argument('--recovery-operation',type=lambda v:int(v,0))
    args=parser.parse_args();main(args.root,args.namespace,args.base_revision,args.operation_base,args.recovery_operation)
