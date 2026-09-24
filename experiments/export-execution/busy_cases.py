#!/usr/bin/env python3
"""A real foreign artifact flock must prevent known-refusal parking."""
import fcntl
import hashlib
import json
import os
import subprocess
import time
from signed_publication_cases import Process, command, event, send


def run(root, state, sql, job, before, snapshot):
    db='bbpub_control_normal';pg=state['postgres_id'];runner=state['container_id']
    result=dict(status='INCOMPLETE',source=state['source'],job=job,before=before)
    path=root/'busy-cases.json';assert not path.exists()
    blocker=client=None;blocker_pid=None;descriptor=None
    app='bigbrotr_export_busy_fixture'
    def save():path.write_text(json.dumps(result,indent=2)+'\n')
    def until(observe, predicate, label):
        end=time.monotonic()+8
        while True:
            value=observe()
            if predicate(value):return value
            if time.monotonic()>=end:raise TimeoutError(label)
            time.sleep(.02)
    def target(observation):
        return next(v['row'] for v in observation['jobs'] if v['row']['job_id']=='\\x'+job)
    def mutex(path):
        info=path.lstat()
        return dict(dev=info.st_dev,ino=info.st_ino,mode=info.st_mode,nlink=info.st_nlink,uid=info.st_uid,
                    size=info.st_size,sha256=hashlib.sha256(path.read_bytes()).hexdigest())
    def contender(path):
        # A distinct process and open-file description contend on the actual
        # mutex. No inheritance of the fixture's owning descriptor.
        script="""import fcntl,sys
with open(sys.argv[1],'rb') as f:
    try: fcntl.flock(f,fcntl.LOCK_EX|fcntl.LOCK_NB)
    except BlockingIOError: print('busy')
    else: print('available')
"""
        return command('python3','-c',script,str(path))
    try:
        # Reservation commits first. Block only the later fresh-reader INSERT
        # so its artifact identity is visible before any filesystem acquisition.
        query='BEGIN; LOCK TABLE control.archive_snapshot_pins IN SHARE MODE; SELECT pg_sleep(30); ROLLBACK;'
        with (root/'busy-blocker.stdout').open('xb') as out,(root/'busy-blocker.stderr').open('xb') as err:
            blocker=subprocess.Popen(['docker','exec','--env','PGAPPNAME='+app,pg,'psql','-XAtq','-v','ON_ERROR_STOP=1','-U','postgres','-d',db,'-c',query],stdout=out,stderr=err)
        blocker_pid=until(lambda:sql(db,f"SELECT a.pid FROM pg_stat_activity a JOIN pg_locks l USING(pid) WHERE a.application_name='{app}' AND l.relation='control.archive_snapshot_pins'::regclass AND l.mode='ShareLock' AND l.granted"),bool,'reader admission blocker')
        assert blocker_pid.isdecimal()
        client=Process(['docker','exec','-i',runner,'python3','-B','/out/build/source/experiments/export-execution/busy_client.py'],input_pipe=True)
        result['ready']=event(client,'ready')
        query=f"SELECT encode(artifact_id,'hex') FROM control.export_execution_units WHERE job_id=decode('{job}','hex')"
        artifact=until(lambda:sql(db,query),bool,'committed output reservation')
        assert len(artifact)==32 and all(c in '0123456789abcdef' for c in artifact)
        destination=root/'out/signed_normal/export-destination'
        assert not (destination/artifact).exists() and not (destination/('.artifact-'+artifact+'.partial')).exists()
        lock=destination/('.artifact-'+artifact+'.lock')
        descriptor=os.open(lock,os.O_RDWR|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW|os.O_CLOEXEC,0o600)
        fcntl.flock(descriptor,fcntl.LOCK_EX|fcntl.LOCK_NB)
        result['mutex_before']=mutex(lock);result['namespace_before']=sorted(p.name for p in destination.iterdir())
        info=result['mutex_before']
        assert info['mode']==0o100600 and info['nlink']==1 and info['size']==0 and info['uid']==os.getuid()
        assert contender(lock)=='busy'
        wait=until(lambda:sql(db,f"SELECT jsonb_build_object('pid',a.pid,'backend_start',a.backend_start,'query',a.query,'blocking_pids',pg_blocking_pids(a.pid)) FROM pg_stat_activity a JOIN control.owner_epochs o ON o.control_backend_pid=a.pid AND o.control_backend_start=a.backend_start WHERE o.role='control' AND o.state='active' AND a.usename='bigbrotr_control' AND a.datname=current_database() AND a.wait_event_type='Lock' AND a.query LIKE 'INSERT INTO control.archive_snapshot_pins(%' AND {blocker_pid}=ANY(pg_blocking_pids(a.pid))"),bool,'actual reader INSERT blocked')
        result['blocked_reader']=json.loads(wait);result['reserved']=snapshot();save()
        assert target(result['reserved'])['state']=='queued' and target(result['reserved'])['job_revision']==2
        assert sql(db,f"SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE pid={blocker_pid} AND application_name='{app}'")=='t'
        blocker.wait(timeout=10);blocker_pid=None
        def cleaned(observation):
            new=[v['row'] for v in observation['archive_snapshot_pins'] if v not in before['archive_snapshot_pins']]
            released={v['row']['lease_id'] for v in observation['snapshot_leases'] if v['row']['state']=='released'}
            return len(new)==1 and new[0]['lease_id'] in released
        result['observed']=until(snapshot,cleaned,'known Busy reader cleanup');save()
        send(client,'collect\n');assert client.wait(20)==0
        result['client_terminal']=event(client,'terminal');result['terminal']=snapshot()
        result['backends']={database:int(sql(database,"SELECT count(*) FROM pg_stat_activity WHERE pid<>pg_backend_pid() AND datname=current_database()")) for database in [db,'bbpub_normal']}
        result['mutex_after']=mutex(lock);result['namespace_after']=sorted(p.name for p in destination.iterdir())
        result['contender_while_held']=contender(lock);save()
        trace=(root/'out/private-grant-http/executor-busy.trace').read_text()
        result['artifact_flock_trace']=[line for line in trace.splitlines() if '.artifact-'+artifact+'.lock>' in line and 'flock(' in line]
        assert len(result['artifact_flock_trace'])==1
        line=result['artifact_flock_trace'][0]
        assert 'LOCK_EX|LOCK_NB' in line and ('= -1 EAGAIN ' in line or '= -1 EWOULDBLOCK ' in line)
        terminal=result['client_terminal'];row=target(result['terminal'])
        assert not client.stderr and not client.overflow
        assert terminal['autonomous_exit'] and terminal['exit']==4
        assert 'without matched output custody' in terminal['stderr']
        assert terminal['parent_absent'] and terminal['tracer_absent'] and terminal['lock_available'] and terminal['scratch_empty']
        assert result['observed']==result['terminal']
        assert row['state']=='running' and row['job_revision']==3 and row['checkpoint_revision']==0 and row['reason_code'] is None
        assert row['result_manifest_digest'] is None and row['completed_at_us'] is None
        assert result['mutex_before']==result['mutex_after'] and result['namespace_before']==result['namespace_after']
        assert result['contender_while_held']=='busy'
        assert result['backends']=={db:0,'bbpub_normal':0}
        for table in ['artifacts','artifact_files']:assert before[table]==result['terminal'][table]
        for table in ['jobs','export_execution_units','snapshot_leases','archive_snapshot_pins','retention_claims']:
            for old in before[table]:
                if table=='jobs' and old['row']['job_id']=='\\x'+job:continue
                assert old in result['terminal'][table]
        os.close(descriptor);descriptor=None
        result['contender_after_release']=contender(lock);assert result['contender_after_release']=='available'
        result['status']='PASS'
        return result
    except BaseException as error:
        result['failure']=type(error).__name__;raise
    finally:
        if descriptor is not None:os.close(descriptor)
        if blocker_pid is not None:sql(db,f"SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE pid={blocker_pid} AND application_name='{app}'")
        if blocker is not None and blocker.poll() is None:blocker.wait(timeout=10)
        if client is not None:
            client.stop();result['client_output']=client.stdout.decode();result['client_error']=client.stderr.decode()
        save()
