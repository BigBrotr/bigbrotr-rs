#!/usr/bin/env python3
"""Actual populated17 upgrade and positively excluded pin recovery on native state."""
import argparse
import json
from pathlib import Path
import sys
import time
sys.dont_write_bytecode = True
from signed_publication_cases import command, event, execute, send, sha, Process, proxy_observation
from signed_publication_oracle import EVENT, corpus
from lab import change


def main(root):
    source = Path(__file__).resolve().parents[2]
    state = json.loads((root/'resources.json').read_text())
    assert source == root/'source'
    assert command('git','-C',str(source),'rev-parse','HEAD') == state['source']
    assert not command('git','-C',str(source),'status','--porcelain','--untracked-files=all')
    ready = json.loads((root/'out/native-tests/ready.json').read_text())
    assert ready['source']==state['source']
    for label,path in [('publisher','out/native-tests/archive-publication-probe'),('control','out/native-tests/bigbrotr'),
                       ('event_reader','out/native-tests/archive-event-probe'),('worker','out/archive-worker/bigbrotr-archive-worker')]:
        assert sha(root/path)==ready[label]
    assert json.loads((root/'cut-cases.json').read_text())['status']=='PASS'
    runner,pg = state['container_id'],state['postgres_id']
    for identity in [runner,pg]:
        observed=json.loads(command('docker','inspect',identity))[0]
        assert observed['State']['Running'] and not observed['State']['OOMKilled']
        assert observed['Config']['Labels']['org.bigbrotr.publication']==state['owner']
    directory=root/'out/pin-recovery-cases';directory.mkdir(mode=0o700)
    report_path=root/'pin-recovery-cases.json';assert not report_path.exists()
    report=dict(status='INCOMPLETE',source=state['source'],artifacts=ready,cases=[],
                proxy_sha256=sha(source/'experiments/archive-engine/native_commit_proxy.py'))
    def save():report_path.write_text(json.dumps(report,indent=2)+'\n')
    def sql(text,db='bbpub_control_normal'):
        return command('docker','exec','-i',pg,'psql','-XAtq','-v','ON_ERROR_STOP=1','-U','postgres','-d',db,input=text)
    def rows():
        names=['snapshot_leases','archive_snapshot_pins','retention_claims']
        result={n:json.loads(sql("SELECT coalesce(jsonb_agg(to_jsonb(t) ORDER BY to_jsonb(t)::text),'[]') FROM (SELECT *,xmin::text AS xmin FROM control."+n+")t")) for n in names}
        result['archive_pin_recoveries']=json.loads(sql("SELECT coalesce(jsonb_agg(to_jsonb(t) ORDER BY to_jsonb(t)::text),'[]') FROM (SELECT *,xmin::text AS xmin FROM control.archive_pin_recoveries)t")) if sql("SELECT to_regclass('control.archive_pin_recoveries') IS NOT NULL")=='t' else []
        return result
    def native():
        names=json.loads(sql("SELECT json_agg(tablename ORDER BY tablename) FROM pg_tables WHERE schemaname='public'",'bbpub_normal'))
        assert all(n.startswith('ducklake_') and n.replace('_','').isalnum() for n in names)
        metadata={n:json.loads(sql(f"SELECT coalesce(jsonb_agg(to_jsonb(t) ORDER BY to_jsonb(t)::text),'[]') FROM public.{n} t",'bbpub_normal')) for n in names}
        home=root/'out/signed_normal/kind=7'
        files={str(p.relative_to(home)):dict(bytes=p.stat().st_size,sha256=sha(p)) for p in sorted(home.rglob('*.parquet'))}
        assert len(files)==8
        return dict(metadata=metadata,files=files)
    def no_clients():
        until=time.monotonic()+5
        while True:
            clients=json.loads(sql("SELECT coalesce(json_agg(json_build_object('pid',pid,'user',usename,'database',datname)),'[]') FROM pg_stat_activity WHERE backend_type='client backend' AND pid<>pg_backend_pid()",'postgres'))
            if not clients:return
            assert time.monotonic()<until,clients
            time.sleep(.02)
    def fixture(label,operation,action,proxy_mode=None,manager_loss=False,wait=True):
        case=dict(name=label,status='INCOMPLETE',before=rows());report['cases'].append(case);save()
        proxy=None
        try:
            port=5432
            if proxy_mode:
                proxy=Process(['docker','exec','-i',runner,'python3','-B','/source/experiments/archive-engine/native_commit_proxy.py',
                    '--target','control-pin-recovery','--mode',proxy_mode],input_pipe=True)
                started=case['proxy_ready']=event(proxy,'ready')
                assert started['source_sha256']==report['proxy_sha256'];port=started['port']
            request=dict(case=label,operation=operation,control_port=port,action=action,ttl_ms=10000,wait=wait,cancel=False)
            path=directory/(label+'.json');path.write_text(json.dumps(request)+'\n')
            def interact(p):
                if not wait:return
                case['ready']=event(p,'pin-ready')
                case['issuer']=json.loads(sql("SELECT row_to_json(t) FROM control.owner_epochs t WHERE role='control' AND state='active'"))
                assert case['issuer']['process_incarnation']=='\\x'+case['ready']['incarnation']
                assert case['issuer']['role_instance_id']=='\\x'+case['ready']['role_instance_id']
                (root/'out/signed_normal'/('pin-'+label)/'continue').touch(exist_ok=False)
                if proxy_mode=='hold-commit':
                    case['held']=event(proxy,'held');assert case['held']['backend_pid']==case['ready']['control_pid']
                if manager_loss:
                    pid,start=case['ready']['manager_pid'],case['ready']['manager_start'];assert "'" not in start
                    assert sql(f"SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE pid={pid} AND backend_start='{start}'::timestamptz AND usename='CatalogManager' AND datname='bbpub_normal'")=='t'
                    until=time.monotonic()+5
                    while sql(f"SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE pid={pid} AND backend_start='{start}'::timestamptz)")=='t':
                        assert time.monotonic()<until,'manager backend did not exit'
                        time.sleep(.02)
                    case['manager_excluded']=dict(pid=pid,backend_start=start,observed_absent=True)
                if proxy_mode=='hold-commit':send(proxy,'release\n')
                save()
            process=case['process']={}
            execute(['docker','exec','--env','LD_LIBRARY_PATH=/out/archive-worker/lib',runner,
                '/out/native-tests/archive-publication-probe','normal','pin','/out/pin-recovery-cases/'+path.name],75,process,interact)
            assert process['exit']==0 and not process['stderr'] and not process['overflow'],process
            case['observed']=json.loads(process['stdout'].splitlines()[-1])
            if wait:
                assert case['observed']['event']=='pin-result' and case['observed']['drivers_joined']
                if action=='recover':assert case['observed']['ordinary_reacquired']
            if proxy:
                send(proxy,'stop\n');assert proxy.wait(5)==0 and not proxy.stderr and not proxy.overflow
                terminal=case['proxy_terminal']=event(proxy,'terminal');case['proxy']=proxy_observation(proxy)
                assert terminal['unexpected']==0 and terminal['data_queries']==1
                expected={'before-forward':(1,1,0,0),'after-complete':(1,1,1,0),'hold-commit':(0,1,1,1)}[proxy_mode]
                assert tuple(terminal[k] for k in ['faults','data_commits','server_complete','client_complete'])==expected
                proxy.stop();proxy=None
            if wait:no_clients()
            case['after']=rows();save();return case
        finally:
            if proxy:
                try:send(proxy,'stop\n');assert proxy.wait(5)==0
                except BaseException as error:case['proxy_cleanup_error']=type(error).__name__
                proxy.stop();case['proxy_incomplete']=proxy_observation(proxy)
            if 'after' not in case:
                try:case['after']=rows()
                except BaseException as error:case['post_state_unavailable']=type(error).__name__
            save()
    def passed(case):case['status']='PASS';save()
    def unchanged(case):assert case['after']==case['before']
    def seed(label,operation):
        case=fixture(label,operation,'register')
        assert case['observed']['outcome']=='registered' and case['observed']['receipt']['created']
        identifier='\\x'+f'{operation:02x}'*16
        for table in ['snapshot_leases','archive_snapshot_pins','retention_claims']:
            added=[r for r in case['after'][table] if r['lease_id']==identifier]
            assert len(added)==1
            assert [r for r in case['after'][table] if r['lease_id']!=identifier]==case['before'][table]
        lease=next(r for r in case['after']['snapshot_leases'] if r['lease_id']==identifier)
        claim=next(r for r in case['after']['retention_claims'] if r['lease_id']==identifier)
        binding=next(r for r in case['after']['archive_snapshot_pins'] if r['lease_id']==identifier)
        issuer=case['issuer']
        assert [binding[k] for k in ['issuer_resource','issuer_epoch','issuer_role','issuer_incarnation','purpose','requested_ttl_us']]==[issuer['resource_id'],issuer['epoch'],'control',issuer['process_incarnation'],'reader',10000000]
        assert [lease[k] for k in ['holder_kind','holder_role_instance_id','holder_role','holder_incarnation']]==['role_instance',issuer['role_instance_id'],'control',issuer['process_incarnation']]
        assert case['after']['archive_pin_recoveries']==case['before']['archive_pin_recoveries']
        assert lease['state']=='active' and lease['archive_pin_protocol']==lease['frontier_revision']==lease['lease_revision']==1
        assert lease['dataset_id']=='\\x'+'01'*16 and lease['consumer_id']=='\\x'+'5a'*16
        assert [claim[k] for k in ['shard_id','owner_epoch','first_sequence','last_sequence','purpose','state']]==[0,1,1,10,'reader','active']
        passed(case);return case
    def recovered(case,operation):
        identifier='\\x'+f'{operation:02x}'*16;before,after=case['before'],case['after']
        assert before['archive_snapshot_pins']==after['archive_snapshot_pins']
        for table,mutable in [('snapshot_leases',{'lease_revision','state','updated_at_us','released_at_us','release_reason','xmin'}),
                              ('retention_claims',{'state','released_at_us','release_reason','xmin'})]:
            assert [r for r in before[table] if r['lease_id']!=identifier]==[r for r in after[table] if r['lease_id']!=identifier]
            old=next(r for r in before[table] if r['lease_id']==identifier);new=next(r for r in after[table] if r['lease_id']==identifier)
            assert {k:v for k,v in old.items() if k not in mutable}=={k:v for k,v in new.items() if k not in mutable}
            assert old['state']=='active' and new['state']=='released' and new['release_reason']=='old_reader_excluded'
            assert old['xmin']!=new['xmin']
        lease=next(r for r in after['snapshot_leases'] if r['lease_id']==identifier)
        claim=next(r for r in after['retention_claims'] if r['lease_id']==identifier)
        assert lease['lease_revision']==2 and lease['updated_at_us']==lease['released_at_us']
        assert [r for r in after['archive_pin_recoveries'] if r['lease_id']!=identifier]==before['archive_pin_recoveries']
        binding=next(r for r in before['archive_snapshot_pins'] if r['lease_id']==identifier)
        r=next(r for r in after['archive_pin_recoveries'] if r['lease_id']==identifier);issuer=case['issuer']
        assert r==dict(lease_id=identifier,dataset_id='\\x'+'01'*16,consumer_id='\\x'+'5a'*16,expected_lease_revision=1,
            coordinator_binding_sha256=report['coordinator_binding'],exclusion_protocol=1,recoverer_resource=issuer['resource_id'],
            recoverer_epoch=issuer['epoch'],recoverer_role='control',recoverer_incarnation=issuer['process_incarnation'],
            recovered_at_us=r['recovered_at_us'],xmin=lease['xmin'])
        assert r['xmin']==claim['xmin'] and r['recovered_at_us']<=claim['released_at_us']<=lease['released_at_us']
        assert (binding['issuer_resource'],binding['issuer_epoch'],binding['issuer_incarnation'])!=(r['recoverer_resource'],r['recoverer_epoch'],r['recoverer_incarnation'])
        assert issuer['acquired_at_us']<=r['recovered_at_us']
    def receipt_matches(case,operation):
        lease=next(r for r in case['after']['snapshot_leases'] if r['lease_id']=='\\x'+f'{operation:02x}'*16)
        assert case['observed']['receipt']==dict(lease_id=f'{operation:02x}'*16,catalog_id='5a'*16,
            frontier_revision=1,snapshot_id=3,lease_revision=2,acquired_at_us=lease['acquired_at_us'],
            updated_at_us=lease['updated_at_us'],expires_at_us=lease['expires_at_us'],released=True,created=False)
    def blocked_reader():
        case=dict(name='actual-reader-excludes-recovery',status='INCOMPLETE',before=rows())
        report['cases'].append(case);save()
        records,_=corpus(EVENT)
        def reference(sequence):
            row=next(r for b in records for r in b['facts'] if r['sequence']==sequence)
            return dict(source_record_id=dict(dataset_id='01'*16,shard_id=0,owner_epoch='1',sequence=str(sequence)),payload_digest=row['digest'])
        request=dict(expected=dict(dataset='01'*16,event_id=EVENT['id'],
            scope=dict(dataset_id='01'*16,dictionary_kind=4,allocation_domain='04'*16,ordinal='1'),
            payload=reference(7),eligibility=reference(8),snapshot_id=3),catalog_id='5a'*16,
            data_root='/out/signed_normal/kind=7/',database='bbpub_normal',max_row_bytes=1024*1024,deadline_ms=15000,cancel_file=None)
        path=directory/'blocked-reader.json';path.write_text(json.dumps(request)+'\n')
        (directory/'reader-temp').mkdir(mode=0o700)
        holder=Process(['docker','exec','-i',pg,'psql','-XAtq','-v','ON_ERROR_STOP=1','-U','postgres','-d','bbpub_normal'],input_pipe=True)
        closed=False
        def close_holder():
            nonlocal closed
            if closed:return
            send(holder,'ROLLBACK;\n\\q\n');assert holder.wait(5)==0 and not holder.stderr and not holder.overflow
            closed=True
        try:
            send(holder,"BEGIN; LOCK TABLE public.ducklake_snapshot IN ACCESS EXCLUSIVE MODE; SELECT json_build_object('event','locked','pid',pg_backend_pid());\n")
            pid=event(holder,'locked')['pid']
            def interact(_):
                until=time.monotonic()+5
                while True:
                    blocked=json.loads(sql(f"SELECT coalesce(json_agg(json_build_object('pid',pid,'user',usename,'wait_type',wait_event_type,'blockers',pg_blocking_pids(pid))),'[]') FROM pg_stat_activity WHERE datname='bbpub_normal' AND {pid}=ANY(pg_blocking_pids(pid))"))
                    if blocked:break
                    assert time.monotonic()<until,'actual reader did not block'
                    time.sleep(.02)
                assert all(r['user']=='bigbrotr_receipt_reader' and r['wait_type']=='Lock' for r in blocked)
                case['blocking']=blocked
                case['custody']=json.loads(command('docker','exec','-i',runner,'python3','-B',
                    '/source/experiments/archive-engine/reader_custody_probe.py','inspect','/out/native-tests/archive-event-probe',
                    '/out/pin-recovery-cases/blocked-reader.json',request['data_root']))
                assert len(case['custody']['custody'])==1 and 'FLOCK' in case['custody']['custody'][0]['info']
                refused=fixture('recover-reader-held',0xa0,'recover',wait=False)
                assert refused['observed']==dict(event='pin-recovery-refused',outcome='Occupied',commit_started=False)
                unchanged(refused);passed(refused);close_holder();save()
            case['process']={}
            execute(['docker','exec','--env','LD_LIBRARY_PATH=/out/archive-worker/lib',runner,
                '/out/native-tests/archive-event-probe','/out/pin-recovery-cases/blocked-reader.json',
                '/out/pin-recovery-cases/reader-temp'],35,case['process'],interact)
            observed=case['process'];assert observed['exit']==0 and not observed['stderr'] and not observed['overflow']
            read=json.loads(observed['stdout']);assert read['active_permits']==0 and not read['cancelled']
            assert read['result']==dict(status='observed',snapshot_id='3',event=EVENT)
            no_clients();case['after']=rows();unchanged(case);passed(case)
        finally:
            try:close_holder()
            finally:
                holder.stop();case['holder']=dict(closed=closed,exit=holder.process.poll(),
                    stdout=holder.stdout.decode(errors='replace'),stderr=holder.stderr.decode(errors='replace'));save()
    save()
    try:
        report['native_before']=native()
        seed('pre-upgrade',0xa0)
        before=rows();report['upgrade_before']=before
        secret=directory/'postgres';secret.write_bytes((root/'secrets/postgres').read_bytes());secret.chmod(0o600)
        config=(source/'config/recipes/archive-only.toml').read_text()
        for key,value in [('endpoint','127.0.0.1:5432'),('database','bbpub_control_normal')]:config=change(config,'postgres.control',key,value)
        (directory/'instance.toml').write_text(config)
        migration=report['migration']={}
        execute(['docker','exec',runner,'/out/native-tests/bigbrotr','migrate','apply','--file','/out/pin-recovery-cases/instance.toml',
            '--target','18','--user','postgres','--password-ref','file:/out/pin-recovery-cases/postgres'],75,migration)
        assert migration['exit']==0 and not migration['stderr'] and json.loads(migration['stdout'])['current_version']=='18'
        assert rows()==before
        ledger=json.loads(sql("SELECT json_agg(json_build_array(migration,name,encode(checksum,'hex')) ORDER BY migration) FROM control.schema_versions"))
        entries=json.loads((source/'migrations/manifest.json').read_text())['application_order']
        assert ledger==sorted([[e['version'],e['name'],sha(source/'migrations/postgres'/e['name'])] for e in entries])
        report['ledger']=ledger
        sql('GRANT SELECT,INSERT ON control.archive_pin_recoveries TO bigbrotr_control; GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA control TO bigbrotr_control;')
        report['coordinator_binding']=sql("SELECT encode(coordinator_binding_sha256,'hex') FROM control.archive_persistence")
        report['coordinator_binding']='\\x'+report['coordinator_binding'];save()
        blocked_reader()
        case=fixture('recover-original',0xa0,'recover');assert case['observed']['outcome']=='recovered'
        recovered(case,0xa0);receipt_matches(case,0xa0);passed(case)
        retry=fixture('recover-original-retry',0xa0,'recover');unchanged(retry)
        receipt_matches(retry,0xa0);assert not retry['observed']['commit_started'];passed(retry)
        for i,(mode,committed,wanted) in enumerate([('before-forward',False,'CommitUnknown'),('after-complete',True,'CommitUnknown'),('hold-commit',True,'Unavailable')]):
            operation=0xa1+i;seed('seed-'+mode,operation)
            case=fixture('recover-'+mode,operation,'recover',proxy_mode=mode,manager_loss=mode=='hold-commit')
            assert case['observed']['outcome']==wanted and case['observed']['commit_started']
            if committed:recovered(case,operation)
            else:unchanged(case)
            passed(case)
            retry=fixture('retry-'+mode,operation,'recover');assert retry['observed']['outcome']=='recovered'
            if committed:unchanged(retry);assert not retry['observed']['commit_started']
            else:recovered(retry,operation);assert retry['observed']['commit_started']
            receipt_matches(retry,operation)
            passed(retry)
        report['native_after']=native();assert report['native_after']==report['native_before']
        no_clients();report['retained']=rows();report['status']='PASS'
    finally:save()
    print(json.dumps(dict(status=report['status'],source=state['source'],cases=len(report['cases']))))


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('root',type=Path)
    main(parser.parse_args().root.resolve())
