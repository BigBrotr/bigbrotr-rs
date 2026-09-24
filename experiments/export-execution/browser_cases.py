#!/usr/bin/env python3
"""Host-owned browser/SQL oracle over retained jobs, never a simulated API."""
import hashlib
import json
from pathlib import Path
import shutil
import tarfile
import time
from signed_publication_cases import Process, command, event, send

IMAGE='mcr.microsoft.com/playwright@sha256:bc6ab0d6d44ff4826e4cb8c1e6d801e185bfc42bb0753f8e2a30efc70db054c7'

def sha(p): return hashlib.sha256(p.read_bytes()).hexdigest()
def write(p,v): p.write_text(json.dumps(v,indent=2)+'\n')


def prepare(root, prior, start, finish):
    old=root.parent/'private-panel-no-preload-b6d9d7e'
    retained=json.loads((old/'resources.json').read_text())
    assert retained['owner']=='f062862798d1c5a5606e31e87618683e' and retained['state']=='stopped-retained'
    for entry in retained['containers']:
        c=next(c for c in prior if c['Id']==entry['id'])
        assert c['Config']['Labels']['org.bigbrotr.private-panel']==retained['owner']
        assert not c['State']['Running'] and c['State']['Pid']==0
    tools=old/'tools'
    archives={'node.tar.xz':('node','5c4286dcd5bbd5acb1ccc7eb0e088bd5eb1e3affad671ee9364004f8f6a4a431'),
              'pnpm.tgz':('pnpm','cf86a7ad764406395d4286a6d09d730711720acc6d93e9dce9ac7ac4dc4a28a7')}
    # Read-only reuse only after comparing every extracted tool file/link with
    # the pinned original archive, not just the executable entry point.
    import os
    for name,(directory,digest) in archives.items():
        assert sha(tools/name)==digest
        with tarfile.open(tools/name) as tar:
            for member in tar:
                relative=Path(*Path(member.name).parts[1:])
                assert not relative.is_absolute() and '..' not in relative.parts
                path=tools/directory/relative
                if member.isfile(): assert path.is_file() and not path.is_symlink() and sha(path)==hashlib.sha256(tar.extractfile(member).read()).hexdigest()
                elif member.issym(): assert path.is_symlink() and os.readlink(path)==member.linkname
                else: assert member.isdir() and path.is_dir()
    identity=json.loads(command('docker','image','inspect',IMAGE))[0]
    assert identity['Architecture']=='amd64' and identity['Os']=='linux' and IMAGE in identity['RepoDigests']
    for name,digest in [('browser-manifest.json',IMAGE.split(':')[-1]),('browser-config.json','2c1f4e0fd6450f43ddb46d60c2a6df30855a8588e165b1f2559fb0eda8d7ff35')]:
        assert sha(old/name)==digest;shutil.copy2(old/name,root/name)
    write(root/'browser-image.json',identity)
    write(root/'browser-tools.json',dict(root=str(old),owner=retained['owner'],archives={k:v[1] for k,v in archives.items()},node_sha256=sha(tools/'node/bin/node'),pnpm_sha256=sha(tools/'pnpm/bin/pnpm.cjs')))
    out=root/'out';shutil.copytree(out/'build/source',out/'frontend-source')
    def mount(p,target,ro=False): return ['--mount',f'type=bind,src={p},dst={target}'+(',readonly' if ro else '')]
    ident=start('frontend',IMAGE,['python3','-B','/out/build/source/experiments/private-panel/native.py','frontend'],
                mount(out,'/out')+mount(out/'build/source','/out/build/source',True)+mount(tools,'/tools',True),'bridge')
    assert finish(ident,'frontend',900)
    built=json.loads((out/'frontend-result.json').read_text())
    assert built['status']=='PASS' and built['source_unchanged'] and built['frontend_originals_unchanged']
    assert sha(out/'frontend-source/node_modules/.pnpm/playwright-core@1.63.0/node_modules/playwright-core/browsers.json')=='545d52f8382c391e605562c330e9c1c534a16045898203037a49bb8bd769a946'
    return tools


def run(root,state,sql,start,finish,tools):
    status=json.loads((root/'status-cases.json').read_text());assert status['status']=='PASS'
    db='bbpub_control_normal';runner=state['container_id'];pg=state['postgres_id']
    out=root/'out';result=dict(status='INCOMPLETE',source=state['source'],cases={})
    def save():write(root/'browser-cases.json',result)
    def rows():
        return json.loads(sql(db,"SELECT coalesce(jsonb_agg(jsonb_build_object('id',encode(session_id,'hex'),'last_seen',last_seen_at_us,'idle_expires',idle_expires_at_us,'revoked',revoked_at_us,'xmin',xmin::text) ORDER BY session_id),'[]') FROM control.browser_sessions"))
    def ledger():
        return {table:json.loads(sql(db,f"SELECT coalesce(jsonb_agg(jsonb_build_object('row',to_jsonb(t),'xmin',xmin::text) ORDER BY {key}),'[]') FROM control.{table} t")) for table,key in [('jobs','job_id'),('export_execution_units','job_id'),('artifacts','artifact_id'),('artifact_files','artifact_id,file_id'),('snapshot_leases','lease_id'),('archive_snapshot_pins','lease_id'),('retention_claims','claim_id')]}
    before=ledger();assert before==status['after'];result['before']=before
    def active(value):sql(db,f"BEGIN; SELECT 1 FROM control.datasets FOR UPDATE; UPDATE control.local_accounts SET active={value} WHERE username='alice'; COMMIT;")
    expected_commands=['observe-before','observe-after','restart','hold-account','verify-blocked','release-account','deactivate']
    try:
        for engine in ['chromium','firefox','webkit']:
            for mount in ['root','control']:
                name=engine+'-'+mount;case=out/'browser-cases'/name;case.mkdir(parents=True,mode=0o700)
                observed=result['cases'][name]=dict(status='INCOMPLETE',commands=[]);save()
                active('true')
                started=time.monotonic();time.sleep(60);observed['rate_wait_seconds']=time.monotonic()-started
                write(case/'input.json',dict(completed=status['expected'],accepted=status['accepted_expected']))
                server=holder=None
                try:
                    server=Process(['docker','exec','-i',runner,'python3','-B','/out/build/source/experiments/export-execution/browser_server.py',name,mount],input_pipe=True)
                    observed['ready']=event(server,'ready')
                    def control_backend():return json.loads(sql(db,"SELECT coalesce(jsonb_agg(jsonb_build_object('pid',pid,'backend_start',backend_start)),'[]') FROM pg_stat_activity WHERE usename='bigbrotr_control' AND datname=current_database()"))
                    observed['backend']=control_backend();assert len(observed['backend'])==1;save()
                    def bind(p,target,ro=False):return ['--mount',f'type=bind,src={p},dst={target}'+(',readonly' if ro else '')]
                    ident=start('browser-'+name,IMAGE,['/tools/node/bin/node','/out/frontend-source/experiments/export-execution/job_browser.mjs',engine,mount],
                                bind(out,'/out',True)+bind(case,'/case')+bind(tools,'/tools',True)+['--env','PLAYWRIGHT_BROWSERS_PATH=/ms-playwright','--env','HOME=/tmp'],'container:'+pg)
                    observed['container']=ident;save();index=0;boundary=None;deadline=time.monotonic()+150
                    while True:
                        assert time.monotonic()<deadline,'browser host deadline'
                        entry=case/f'command-{index+1}.json'
                        if entry.exists():
                            assert entry.stat().st_size<1024
                            request=json.loads(entry.read_text());assert request==dict(name=expected_commands[index])
                            selected=request['name'];detail=dict(name=selected)
                            if selected=='observe-before':boundary=rows();detail['rows']=boundary
                            elif selected=='observe-after':detail['rows']=rows();assert detail['rows']==boundary
                            elif selected=='restart':
                                unchanged=rows();send(server,json.dumps(dict(operation='restart'))+'\n')
                                detail['terminal']=event(server,'restart-terminal');detail['ready']=event(server,'restarted')
                                assert detail['ready']['owner']['owner_epoch']!=observed['ready']['owner']['owner_epoch']
                                assert rows()==unchanged;detail['session_rows_unchanged']=True
                                detail['backend']=control_backend();assert len(detail['backend'])==1;observed['current_backend']=detail['backend']
                            elif selected=='hold-account':
                                holder=Process(['docker','exec','-i',pg,'psql','-XAtq','-v','ON_ERROR_STOP=1','-U','postgres','-d',db],input_pipe=True)
                                send(holder,"BEGIN; SELECT json_build_object('event','locked','pid',pg_backend_pid()) FROM control.local_accounts WHERE username='alice' FOR UPDATE;\n")
                                detail['holder']=event(holder,'locked');held_pid=detail['holder']['pid'];held_at=time.monotonic()
                            elif selected=='verify-blocked':
                                until=time.monotonic()+2
                                while True:
                                    waiting=json.loads(sql(db,f"SELECT coalesce(jsonb_agg(jsonb_build_object('pid',pid,'backend_start',backend_start,'query',query,'wait_event_type',wait_event_type)),'[]') FROM pg_stat_activity WHERE {held_pid}=ANY(pg_blocking_pids(pid)) AND datname=current_database() AND usename='bigbrotr_control'"))
                                    if waiting:break
                                    assert time.monotonic()<until;time.sleep(.01)
                                assert len(waiting)==1 and waiting[0]['wait_event_type']=='Lock'
                                assert {k:waiting[0][k] for k in ['pid','backend_start']}==observed['current_backend'][0]
                                assert 'FROM control.local_accounts' in waiting[0]['query'] and 'FOR UPDATE' in waiting[0]['query']
                                detail['waiter']=waiting[0]
                            elif selected=='release-account':
                                send(holder,'ROLLBACK;\n\\q\n');assert holder.wait(5)==0;holder.stop();holder=None
                                detail['held_seconds']=time.monotonic()-held_at;assert detail['held_seconds']<3
                            elif selected=='deactivate':active('false')
                            observed['commands'].append(detail);save();index+=1
                            temporary=case/f'response-{index}.tmp';write(temporary,dict(state='verified'));temporary.rename(case/f'response-{index}.json')
                        c=json.loads(command('docker','inspect',ident))[0]
                        assert c['Config']['Labels'][state['owner_label']]==state['owner']
                        if not c['State']['Running']:break
                        time.sleep(.025)
                    observed['container_ok']=finish(ident,'browser-'+name,5)
                    observed['browser']=json.loads((case/'browser-report.json').read_text());save()
                    assert observed['container_ok'] and observed['browser']['status']=='PASS' and index==len(expected_commands)
                    send(server,json.dumps(dict(operation='stop'))+'\n');assert server.wait(12)==0
                    observed['terminal']=event(server,'stop-terminal');assert not server.stderr and not server.overflow
                    assert ledger()==before;observed['ledgers_unchanged']=True
                    observed['backends']={database:int(sql(database,"SELECT count(*) FROM pg_stat_activity WHERE pid<>pg_backend_pid() AND datname=current_database()")) for database in [db,'bbpub_normal']}
                    assert not any(observed['backends'].values())
                    observed['status']='PASS';save()
                finally:
                    if holder:holder.stop()
                    if server:server.stop()
                active('true')
        result['after']=ledger();assert result['after']==before
        result['status']='PASS';save();return result
    except BaseException as error:
        result['failure']=type(error).__name__;raise
    finally:save()
