#!/usr/bin/env python3
"""Real pin transactions, missing claims, expiry, lock orders and lost replies.

Uses retained signed native state and a separately built exact-Git fixture.
No live-delivery or recovery-release qualification follows from these cases.
"""
import argparse
import json
from pathlib import Path
import sys
import time

sys.dont_write_bytecode = True
from signed_publication_cases import command, event, execute, send, sha, Process, proxy_observation


def main(root):
    source = Path(__file__).resolve().parents[2]
    state = json.loads((root/'resources.json').read_text())
    assert source == root/'harness-source'
    revision = command('git','-C',str(source),'rev-parse','HEAD')
    assert not command('git','-C',str(source),'status','--porcelain','--untracked-files=all')
    ready = json.loads((root/'out/pin-fault-build/ready.json').read_text())
    assert revision == ready['source'] and state['source'] == ready['product_source']
    assert sha(root/'out/pin-fault-build/archive-publication-probe') == ready['artifact_sha256']
    assert json.loads((root/'cut-cases.json').read_text())['status'] == 'PASS'
    runner, pg = state['container_id'], state['postgres_id']
    for identity in [runner, pg]:
        c = json.loads(command('docker','inspect',identity))[0]
        assert c['Config']['Labels']['org.bigbrotr.publication'] == state['owner']
        assert c['State']['Running'] and not c['State']['OOMKilled']
    directory = root/'out/pin-fault-cases'
    directory.mkdir(mode=0o700)
    report_path = root/'pin-fault-cases.json'
    assert not report_path.exists()
    report = dict(status='INCOMPLETE', source=state['source'], harness=ready, cases=[],
                  proxy_sha256=sha(source/'experiments/archive-engine/native_commit_proxy.py'))

    def save():
        report_path.write_text(json.dumps(report,indent=2)+'\n')

    def sql(query, db='bbpub_control_normal'):
        return command('docker','exec','-i',pg,'psql','-XAtq','-v','ON_ERROR_STOP=1',
                       '-U','postgres','-d',db,input=query)

    def rows():
        return {name: json.loads(sql("SELECT coalesce(jsonb_agg(to_jsonb(t) ORDER BY to_jsonb(t)::text),'[]') FROM (SELECT *,xmin::text AS xmin FROM control."+name+")t"))
                for name in ['snapshot_leases','archive_snapshot_pins','retention_claims']}

    def no_clients():
        until = time.monotonic()+6
        while True:
            observed = json.loads(sql("SELECT coalesce(json_agg(json_build_object('pid',pid,'database',datname,'user',usename)),'[]') FROM pg_stat_activity WHERE backend_type='client backend' AND pid<>pg_backend_pid()", 'postgres'))
            if not observed:
                return
            assert time.monotonic() < until, observed
            time.sleep(.03)

    def blocked(pid, holder):
        until = time.monotonic()+8
        while True:
            observed = json.loads(sql(f"SELECT coalesce(json_agg(json_build_object('pid',pid,'blockers',pg_blocking_pids(pid),'wait',wait_event_type)),'[]') FROM pg_stat_activity WHERE pid={pid} AND {holder}=ANY(pg_blocking_pids(pid))"))
            if observed:
                assert observed[0]['wait'] == 'Lock'
                return observed
            assert time.monotonic() < until
            time.sleep(.02)

    def fixture(label, operation, *, action='register', proxy_mode=None,
                manager_loss=False, maintenance=None, cancel=False, ttl_ms=10000, expire=False):
        case = dict(name=label,status='INCOMPLETE',before=rows())
        report['cases'].append(case)
        proxy = holder = None
        save()
        try:
            port = 5432
            if proxy_mode:
                proxy = Process(['docker','exec','-i',runner,'python3','-B',
                    '/out/pin-fault-source/experiments/archive-engine/native_commit_proxy.py',
                    '--target','control-pin','--mode',proxy_mode],input_pipe=True)
                started = case['proxy_ready'] = event(proxy,'ready')
                assert started['source_sha256'] == report['proxy_sha256']
                port = started['port']
            if maintenance:
                holder = Process(['docker','exec','-i',pg,'psql','-XAtq','-v','ON_ERROR_STOP=1',
                    '-U','postgres','-d','bbpub_control_normal'],input_pipe=True)
                send(holder,"SET idle_in_transaction_session_timeout='25s'; SET statement_timeout='20s'; BEGIN; SELECT json_build_object('event','maintenance-ready','pid',pid,'backend_start',backend_start::text) FROM pg_stat_activity WHERE pid=pg_backend_pid();\n")
                case['holder_ready'] = event(holder,'maintenance-ready')
                if maintenance == 'first':
                    send(holder,"DO $$ BEGIN PERFORM control.lock_archive_pins(decode(repeat('01',16),'hex'),decode(repeat('5a',16),'hex')); END $$; SELECT json_build_object('event','maintenance-scan','leases',(SELECT count(*) FROM control.snapshot_leases WHERE state='active'));\n")
                    case['first_scan'] = event(holder,'maintenance-scan')
                    assert case['first_scan']['leases'] == sum(r['state']=='active' for r in case['before']['snapshot_leases'])
            request = dict(case=label,operation=operation,control_port=port,action=action,
                           ttl_ms=ttl_ms,wait=action=='register',cancel=cancel)
            path = directory/(label+'.json')
            path.write_text(json.dumps(request)+'\n')
            process = case['process'] = {}

            def interact(p):
                nonlocal holder
                if action != 'register':
                    return
                case['ready'] = event(p,'pin-ready')
                for kind,user,database in [('control','bigbrotr_control','bbpub_control_normal'),
                                           ('manager','CatalogManager','bbpub_normal')]:
                    observed = json.loads(sql(f"SELECT row_to_json(t) FROM (SELECT pid,backend_start::text,usename,datname FROM pg_stat_activity WHERE pid={case['ready'][kind+'_pid']})t"))
                    assert observed == dict(pid=case['ready'][kind+'_pid'],backend_start=case['ready'][kind+'_start'],usename=user,datname=database)
                case['issuer'] = json.loads(sql("SELECT row_to_json(t) FROM control.owner_epochs t WHERE role='control' AND state='active'"))
                save()
                (root/'out/signed_normal'/('pin-'+label)/'continue').touch(exist_ok=False)
                if maintenance == 'first':
                    case['blocked'] = blocked(case['ready']['control_pid'],case['holder_ready']['pid'])
                if proxy_mode == 'hold-commit':
                    case['held'] = event(proxy,'held')
                    assert case['held']['stage']=='commit' and case['held']['backend_pid']==case['ready']['control_pid']
                if maintenance == 'second':
                    send(holder,"DO $$ BEGIN PERFORM control.lock_archive_pins(decode(repeat('01',16),'hex'),decode(repeat('5a',16),'hex')); END $$; SELECT json_build_object('event','maintenance-scan','leases',(SELECT count(*) FROM control.snapshot_leases WHERE state='active'),'claims',(SELECT count(*) FROM control.retention_claims WHERE state='active'));\n")
                    case['blocked'] = blocked(case['holder_ready']['pid'],case['ready']['control_pid'])
                if manager_loss:
                    pid, start = case['ready']['manager_pid'],case['ready']['manager_start']
                    assert "'" not in start
                    assert sql(f"SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE pid={pid} AND backend_start='{start}'::timestamptz AND usename='CatalogManager' AND datname='bbpub_normal'") == 't'
                    until = time.monotonic()+5
                    while sql(f'SELECT count(*) FROM pg_stat_activity WHERE pid={pid}') != '0':
                        assert time.monotonic()<until
                        time.sleep(.01)
                    case['manager_excluded'] = True
                if cancel:
                    (root/'out/signed_normal'/('pin-'+label)/'cancel').touch(exist_ok=False)
                    case['cancelled'] = event(p,'pin-cancelled')
                    assert not case['cancelled']['commit_started']
                if expire:
                    # Hold the actual transaction after lease insertion until
                    # its PG-clock deadline has passed. This is deliberate input.
                    time.sleep(ttl_ms/1000+.2)
                if proxy_mode == 'hold-commit':
                    send(proxy,'release\n')
                if maintenance == 'second':
                    case['second_scan'] = event(holder,'maintenance-scan')
                    assert case['second_scan']['leases'] == 1+sum(r['state']=='active' for r in case['before']['snapshot_leases'])
                    assert case['second_scan']['claims'] == 1+sum(r['state']=='active' for r in case['before']['retention_claims'])
                if holder:
                    send(holder,'ROLLBACK;\n\\q\n')
                    assert holder.wait(5)==0 and not holder.stderr
                    case['holder'] = proxy_observation(holder)
                    holder.stop()
                    holder = None
                save()

            execute(['docker','exec','--env','LD_LIBRARY_PATH=/out/archive-worker/lib',runner,
                     '/out/pin-fault-build/archive-publication-probe','normal','pin',
                     '/out/pin-fault-cases/'+path.name],75,process,interact)
            assert process['exit']==0 and not process['stderr'] and not process['overflow'], process
            case['observed'] = json.loads(process['stdout'].splitlines()[-1])
            assert case['observed']['drivers_joined']
            if proxy:
                send(proxy,'stop\n')
                assert proxy.wait(5)==0 and not proxy.stderr and not proxy.overflow
                case['proxy'] = proxy_observation(proxy)
                terminal = case['proxy_terminal'] = event(proxy,'terminal')
                assert terminal['unexpected']==0 and terminal['data_queries']==1
                expected = {
                    'before-forward': (1,1,0,0),
                    'after-complete': (1,1,1,0),
                    'hold-commit': (0,1,0,0) if expire else (0,1,1,1),
                    'omit-pin-claim': (0,1,0,0),
                    'wrong-pin-prefix': (0,0,0,0),
                    'wrong-pin-purpose': (0,0,0,0),
                }[proxy_mode]
                assert tuple(terminal[k] for k in ['faults','data_commits','server_complete','client_complete']) == expected
                proxy.stop()
                proxy = None
            no_clients()
            case['after'] = rows()
            save()
            return case
        finally:
            for label,p in [('proxy',proxy),('holder',holder)]:
                if p:
                    try:
                        send(p,'stop\n' if label=='proxy' else 'ROLLBACK;\n\\q\n')
                        assert p.wait(5)==0
                    except BaseException as error:
                        case[label+'_cleanup_error'] = type(error).__name__
                    p.stop()
                    case[label+'_incomplete'] = proxy_observation(p)
            if holder and 'holder_ready' in case:
                identity = case['holder_ready']
                start = identity['backend_start']
                assert "'" not in start
                case['holder_cleanup_termination'] = sql(f"SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE pid={identity['pid']} AND backend_start='{start}'::timestamptz AND usename='postgres' AND datname='bbpub_control_normal'")
            if 'after' not in case:
                try:
                    case['after'] = rows()
                except BaseException as error:
                    case['post_state_unavailable'] = type(error).__name__
            save()

    def unchanged(case):
        assert case['before'] == case['after']

    def registered(case, operation, ttl_ms):
        before, after = case['before'],case['after']
        added = {}
        for table in before:
            key = 'claim_id' if table=='retention_claims' else 'lease_id'
            remaining = {row[key]:row for row in after[table]}
            for old in before[table]:
                assert remaining.pop(old[key]) == old
            assert len(remaining) == 1
            added[table] = next(iter(remaining.values()))
        lease, pin, claim = (added[t] for t in ['snapshot_leases','archive_snapshot_pins','retention_claims'])
        issuer = case['issuer']
        assert issuer['process_incarnation']=='\\x'+case['ready']['incarnation']
        assert issuer['role_instance_id']=='\\x'+case['ready']['role_instance_id']
        dataset,catalog,identifier='\\x'+'01'*16,'\\x'+'5a'*16,'\\x'+f'{operation:02x}'*16
        xmin = lease['xmin']
        assert lease == dict(lease_id=identifier,dataset_id=dataset,consumer_id=catalog,
            frontier_revision=1,topology_generation=0,holder_kind='role_instance',holder_job_id=None,
            holder_role_instance_id=issuer['role_instance_id'],holder_role='control',holder_incarnation=issuer['process_incarnation'],
            lease_revision=1,acquired_at_us=lease['acquired_at_us'],updated_at_us=lease['acquired_at_us'],
            expires_at_us=lease['expires_at_us'],state='active',released_at_us=None,release_reason=None,archive_pin_protocol=1,xmin=xmin)
        assert 0 < lease['expires_at_us']-lease['acquired_at_us'] <= ttl_ms*1000
        assert issuer['acquired_at_us']<=lease['acquired_at_us']<lease['expires_at_us']<=issuer['lease_expires_at_us']
        assert pin == dict(lease_id=identifier,dataset_id=dataset,consumer_id=catalog,frontier_revision=1,
            issuer_resource=issuer['resource_id'],issuer_epoch=issuer['epoch'],issuer_role='control',
            issuer_incarnation=issuer['process_incarnation'],purpose='reader',requested_ttl_us=ttl_ms*1000,xmin=xmin)
        assert claim == dict(claim_id=claim['claim_id'],lease_id=identifier,dataset_id=dataset,topology_generation=0,
            shard_id=0,owner_epoch=1,first_sequence=1,last_sequence=10,purpose='reader',created_at_us=claim['created_at_us'],
            state='active',released_at_us=None,release_reason=None,xmin=xmin)
        assert lease['acquired_at_us']<=claim['created_at_us']<lease['expires_at_us']
        return dict(lease_id=f'{operation:02x}'*16,catalog_id='5a'*16,frontier_revision=1,snapshot_id=3,
            lease_revision=1,acquired_at_us=lease['acquired_at_us'],updated_at_us=lease['updated_at_us'],
            expires_at_us=lease['expires_at_us'],released=False,created=True)

    def passed(case):
        case['status'] = 'PASS'
        save()

    def resolution(label, operation, receipt):
        lookup = fixture(label+'-lookup',operation,action='lookup')
        unchanged(lookup)
        assert lookup['observed']['receipt'] == (dict(receipt,created=False) if receipt else None)
        passed(lookup)
        return lookup

    def metadata():
        names = json.loads(sql("SELECT json_agg(tablename ORDER BY tablename) FROM pg_tables WHERE schemaname='public'",'bbpub_normal'))
        assert all(n.startswith('ducklake_') and n.replace('_','').isalnum() for n in names)
        return {n:json.loads(sql("SELECT coalesce(jsonb_agg(to_jsonb(t) ORDER BY to_jsonb(t)::text),'[]') FROM public."+n+' t','bbpub_normal')) for n in names}

    native_before = metadata()
    files_before = {str(p.relative_to(root)):sha(p) for p in (root/'out/signed_normal/kind=7').rglob('*.parquet')}
    assert len(files_before)==8
    save()
    try:
        for offset,(mode,wanted,rejection) in enumerate([
            ('omit-pin-claim','CommitUnknown','missing-prefix'),
            ('wrong-pin-prefix','Backend','wrong-prefix'),
            ('wrong-pin-purpose','Backend','wrong-prefix')]):
            case = fixture(mode,0xb0+offset,proxy_mode=mode)
            assert case['observed']['outcome']==wanted
            assert case['observed']['commit_started']==(mode=='omit-pin-claim')
            assert case['proxy_terminal']['pin_claim_injections']==1
            assert case['proxy_terminal']['pin_rejections']==[rejection]
            unchanged(case)
            passed(case)
        for offset,(label,options,committed,outcome) in enumerate([
            ('manager-before',dict(manager_loss=True,maintenance='first'),False,'Unavailable'),
            ('commit-before',dict(proxy_mode='before-forward'),False,'CommitUnknown'),
            ('commit-after',dict(proxy_mode='after-complete'),True,'CommitUnknown'),
            ('manager-after',dict(proxy_mode='hold-commit',manager_loss=True),True,'Unavailable')]):
            operation = 0xc0+offset
            case = fixture(label,operation,**options)
            assert case['observed']['outcome']==outcome
            assert case['observed']['commit_started']==(label!='manager-before')
            if committed:
                receipt = registered(case,operation,10000)
            else:
                receipt = None
                unchanged(case)
            passed(case)
            resolution(label,operation,receipt)
            retry = fixture(label+'-retry',operation)
            assert retry['observed']['outcome']=='registered'
            if committed:
                unchanged(retry)
                assert retry['observed']['receipt']==dict(receipt,created=False)
            else:
                assert retry['observed']['receipt']==registered(retry,operation,10000)
            passed(retry)
        for operation,order in [(0xd0,'first'),(0xd1,'second')]:
            case = fixture('maintenance-'+order,operation,maintenance=order,
                           proxy_mode='hold-commit' if order=='second' else None)
            assert case['observed']['outcome']=='registered'
            assert case['observed']['receipt']==registered(case,operation,10000)
            passed(case)
        expired = fixture('expired-before-commit',0xd2,ttl_ms=500,expire=True,proxy_mode='hold-commit')
        assert expired['observed']['outcome']=='CommitUnknown' and expired['observed']['commit_started']
        assert expired['proxy_terminal']['pin_rejections']==['expired']
        unchanged(expired)
        passed(expired)
        cancelled = fixture('cancel-blocked',0xd3,maintenance='first',cancel=True)
        assert cancelled['observed']['outcome']=='cancelled' and not cancelled['observed']['commit_started']
        unchanged(cancelled)
        passed(cancelled)
        short = fixture('short',0xd4,ttl_ms=500)
        original = registered(short,0xd4,500)
        assert short['observed']['receipt']==original
        passed(short)
        time.sleep(.6)
        retry = fixture('expired-retry',0xd4,ttl_ms=500)
        unchanged(retry)
        assert retry['observed']['receipt']==dict(original,created=False)
        assert retry['observed']['outcome']=='registered'
        passed(retry)
        release = fixture('other-holder-release',0xd4,action='release')
        assert release['observed']['outcome']=='Conflict' and not release['observed']['commit_started']
        unchanged(release)
        passed(release)
        assert metadata()==native_before
        assert {str(p.relative_to(root)):sha(p) for p in (root/'out/signed_normal/kind=7').rglob('*.parquet')}==files_before
        no_clients()
        report['native_metadata_unchanged'] = True
        report['native_files_unchanged'] = files_before
        report['retained'] = rows()
        report['status'] = 'PASS'
    finally:
        save()
    print(json.dumps(dict(status=report['status'],cases=len(report['cases']),report=str(report_path))))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('root',type=Path)
    main(parser.parse_args().root.resolve())
