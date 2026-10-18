"""One selected refusal after a fresh, genuine same-root terminal3 journey.

Each selector needs its own engine root. Privileged corruption is explicit
negative-fixture setup, never preparation/authority for the application. The
normal continuation, ordinary fixtures and production APIs are unchanged.
"""
import functools
import json
from pathlib import Path
import re
import shutil
import sys

import installed_archive_catch_up as normal
from installed_archive_catch_up import SOURCE, lab, read, server_clock, wait_for
from installed_archive_catch_up_faults import observed

STATUS='PASS_INSTALLED_ARCHIVE_CATCH_UP_REFUSAL'
CASES=('current-join','orphan-history','family-counts','missing-input',
       'frontier-cancel','frontier-control-loss')
WAIT_CASES=CASES[-2:]
SCHEMA_SQL="""SELECT jsonb_build_object(
 'triggers',(SELECT jsonb_agg(to_jsonb(t) ORDER BY t.oid) FROM pg_trigger t
   JOIN pg_class c ON c.oid=t.tgrelid JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='control'),
 'constraints',(SELECT jsonb_agg(to_jsonb(c) ORDER BY c.oid) FROM pg_constraint c
   JOIN pg_namespace n ON n.oid=c.connamespace WHERE n.nspname='control'))"""


def mutation_sql(case,consumer):
    assert re.fullmatch(r'\\x[0-9a-f]{32}',consumer)
    target="decode('"+consumer[2:]+"','hex')"
    if case=='current-join':
        change=f'UPDATE control.consumers SET current_revision=current_revision+1 WHERE consumer_id={target}'
    elif case=='orphan-history':
        change=f'DELETE FROM control.consumers WHERE consumer_id={target}'
    elif case=='family-counts':
        change=f'UPDATE control.archive_frontier_snapshots SET dictionary_count=dictionary_count-1,payload_count=payload_count+1 WHERE consumer_id={target} AND frontier_revision=(SELECT current_revision FROM control.consumers WHERE consumer_id={target})'
    else:raise ValueError(case)
    return "BEGIN; SET LOCAL session_replication_role=replica;\n"+change+";\n"+"""SELECT json_build_object('xid',pg_current_xact_id()::text,'role',current_user,
 'replication_role',current_setting('session_replication_role'),'pid',pg_backend_pid());
COMMIT;
SELECT current_setting('session_replication_role');
"""


def mutate(pg,client,output,database,rows,setup,case,before,name):
    schema=json.loads(lab.sql(pg,database,SCHEMA_SQL))
    value=dict(case=case,before=before,schema_before=schema,schema_after=None,sql=None,response=None,quarantine=None,after=None)
    def save():lab.write_json(output/(name+'-mutation.json'),value)
    save()
    if case in CASES[:3]:
        consumer,=before['control']['consumers']
        value['sql']=mutation_sql(case,consumer['consumer_id'])
        (output/(name+'-mutation.sql')).write_text(value['sql']);save()
        answer=lab.docker('exec','-i',pg['postgres'],'psql','-X','-A','-t','-q','--no-password',
            '-v','ON_ERROR_STOP=1','-U','postgres','-d',database,data=value['sql'],check=False,timeout=5)
        value['response']=dict(returncode=answer.returncode,stdout=answer.stdout,stderr=answer.stderr);save()
        assert answer.returncode==0 and not answer.stderr
    elif case=='missing-input':
        first=min(before['control']['archive_attempts'],key=lambda r:r['ordinal'])
        filename=first['attempt_id'][2:]+'.inputs';source=setup['home']/'state'/filename
        destination=output/(name+'-quarantined.inputs')
        assert source.is_file() and not source.is_symlink() and not destination.exists()
        raw=source.read_bytes();meta=source.lstat()
        value['quarantine']=dict(filename=filename,path=str(destination),hex=raw.hex(),bytes=len(raw),sha256=normal.digest(source),
            device=meta.st_dev,inode=meta.st_ino,mode=meta.st_mode,links=meta.st_nlink)
        save()  # Preserve the exact original before the reversible rename.
        source.rename(destination)
        moved=destination.lstat()
        assert destination.read_bytes()==raw and all(getattr(moved,k)==getattr(meta,k)
            for k in ('st_dev','st_ino','st_mode','st_nlink','st_size'))
    value['schema_after']=json.loads(lab.sql(pg,database,SCHEMA_SQL))
    assert value['schema_before']==value['schema_after']
    value['after']=observed(pg,client,output,name+'-prepared',database,rows,setup)
    save();return value


def blocked(pg,database,holder):
    assert re.fullmatch('[a-z0-9_]+',database)
    pid=holder['pid'];assert type(pid) is int and pid>0
    return json.loads(lab.sql(pg,database,f"""SELECT coalesce(json_agg(json_build_object(
      'pid',a.pid,'backend_start',a.backend_start,'database',a.datname,'user',a.usename,
      'state',a.state,'query',a.query,'query_start',a.query_start,'wait_type',a.wait_event_type,
      'wait_event',a.wait_event,'blockers',pg_blocking_pids(a.pid),
      'server_us',(EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint,
      'control_epoch',o.epoch,'control_incarnation',o.process_incarnation,
      'control_lease',o.lease_expires_at_us,
      'holder_start',(SELECT backend_start::text FROM pg_stat_activity WHERE pid={pid}),
      'waiting_locks',(SELECT json_agg(json_build_object('relation',relation::regclass::text,'mode',mode,'granted',granted))
         FROM pg_locks WHERE pid=a.pid AND NOT granted))), '[]')
      FROM pg_stat_activity a JOIN control.owner_epochs o ON o.control_backend_pid=a.pid
        AND o.control_backend_start=a.backend_start AND o.role='control' AND o.state='active'
      WHERE a.datname=current_database() AND a.usename='bigbrotr_control'
        AND a.wait_event_type='Lock' AND {pid}=ANY(pg_blocking_pids(a.pid))"""))


def act_on_wait(pg,client,output,database,prefix,holder,schedule):
    rows=wait_for(lambda:blocked(pg,database,holder),prefix.name+' actual frontier wait',80)
    waiter,=rows;schedule['blocked']=waiter
    assert waiter['query'].startswith('INSERT INTO control.archive_frontier_snapshots(')
    assert waiter['holder_start']==holder['backend_start']
    schedule['action_before_us']=server_clock(pg,database)
    lab.write_json(output/(prefix.name+'-schedule.json'),schedule)
    if schedule['mode']=='frontier-cancel':
        identity=read(Path(str(prefix)+'.pid.json'))
        schedule['signal']=identity
        normal.signal_child(client,identity)
        schedule['action_response']='SIGTERM_SENT_TO_EXACT_PROCESS'
    else:
        start=waiter['backend_start'];assert "'" not in start
        statement=f"SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE pid={waiter['pid']} AND backend_start='{start}'::timestamptz AND datname='{database}' AND usename='bigbrotr_control' AND {holder['pid']}=ANY(pg_blocking_pids(pid))"
        schedule['termination_sql']=statement
        schedule['action_response']=lab.sql(pg,database,statement)
        assert schedule['action_response']=='t'
    schedule['action_after_us']=server_clock(pg,database)
    lab.write_json(output/(prefix.name+'-schedule.json'),schedule)


def execute(pg,client,output,database,rows,setup,case,previous):
    from installed_archive_catch_up_refusal_oracle import process_checked
    sys.path.insert(0,str(SOURCE/'experiments/control-migrations'))
    from concurrency_cases import Session
    name='catch-up-refusal-'+case;prefix=output/name
    before=observed(pg,client,output,name+'-before',database,rows,setup)
    assert {k:v for k,v in before.items() if k not in ('physical','local_records')}=={k:v for k,v in previous.items() if k not in ('physical','local_records')}
    mutation=mutate(pg,client,output,database,rows,setup,case,before,name)
    config=setup['config'];config_text=(output/Path(config).name).read_text()
    (output/(name+'-config.toml')).write_text(config_text)
    window=dict(before_us=server_clock(pg,database));fixture=dict(errors=[],snapshots=[])
    schedule=None;sessions=[];holder=None;holder_closed=case not in WAIT_CASES
    failure=None;after=None;closed=False;backend={};answer=dict(outcome='OUTCOME_UNKNOWN')
    def save_schedule():lab.write_json(output/(name+'-schedule.json'),schedule)
    def preserve(suffix,value):
        try:lab.write_json(output/(name+'-'+suffix+'.json'),value)
        except BaseException as error:fixture['errors'].append(dict(stage=suffix+'-capture',error=repr(error)))
    try:
        if case in WAIT_CASES:
            schedule=dict(mode=case,status='UNKNOWN',holder=None,blocked=None,cleanup_errors=[])
            save_schedule()
            holder=Session(pg,database,'bb_catchup_refusal_holder',sessions,role='bigbrotr_migrator')
            schedule['holder']=dict(identity=holder.backend)
            schedule['holder']['begin']=holder.execute('BEGIN; SET LOCAL idle_in_transaction_session_timeout=120000;')
            schedule['holder']['held']=holder.execute('LOCK TABLE control.archive_frontier_snapshots IN SHARE MODE;')
            save_schedule()
        lab.docker('exec','-d',client,'python3','-B','/lab/installed_archive_catch_up_client.py','catchup',config,'/lab/'+name,timeout=5)
        wait_for(lambda:Path(str(prefix)+'.pid.json').exists() or Path(str(prefix)+'.terminal.json').exists(),name+' identity',5)
        if holder:
            act_on_wait(pg,client,output,database,prefix,holder.backend,schedule)
            # Keep the INSERT blocked until the actual executable has exited.
            # SIGTERM delivery alone must not race a newly unblocked COMMIT.
            wait_for(Path(str(prefix)+'.terminal.json').exists,name+' terminal while held',6)
            schedule['terminal_before_release']=normal.response(prefix)
            process_checked(schedule['terminal_before_release'],case,config,name)
            schedule['terminal_observed_us']=server_clock(pg,database);save_schedule()
            schedule['release_before_us']=server_clock(pg,database)
            schedule['holder']['rollback']=holder.execute('ROLLBACK;')
            schedule['release_after_us']=server_clock(pg,database);save_schedule()
        wait_for(Path(str(prefix)+'.terminal.json').exists,name+' process join',110)
    except BaseException as error:failure=error
    finally:
        # Finish the owned SQL holder even if malformed process output or an
        # observation failed. Wrapper reap alone is never backend exclusion.
        for session in reversed(sessions):
            try:
                if session.pending is None:
                    if schedule and 'rollback' not in schedule['holder']:
                        schedule['holder']['rollback']=session.execute('ROLLBACK;')
                    session.process.stdin.close();session.process.wait(timeout=5)
                    schedule['holder']['exit']=session.process.returncode
                    schedule['holder']['absence']=normal.exact_absence(pg,database,[dict(pid=session.backend['pid'],backend_start=session.backend['backend_start'],database=database)])
                    assert session.process.returncode==0
                    session.process.stdout.close();holder_closed=True
                else:raise RuntimeError('holder result unresolved')
            except BaseException as error:
                fixture['errors'].append(dict(stage='holder-custody',error=repr(error)))
                try:session.close(lambda:None)
                except BaseException as cleanup:fixture['errors'].append(dict(stage='holder-cleanup',error=repr(cleanup)))
        if schedule:
            schedule['status']='OBSERVED_CLOSED_AND_JOINED' if holder_closed else 'UNKNOWN'
        preserve('schedule',schedule)
        try:
            if not Path(str(prefix)+'.terminal.json').exists() and Path(str(prefix)+'.pid.json').exists():
                normal.signal_child(client,read(Path(str(prefix)+'.pid.json')))
                wait_for(Path(str(prefix)+'.terminal.json').exists,name+' cleanup join',5)
        except BaseException as error:fixture['errors'].append(dict(stage='join',error=repr(error)))
        try:
            answer=normal.response(prefix);process_checked(answer,case,config,name);closed=True
        except BaseException as error:fixture['errors'].append(dict(stage='process-custody',error=repr(error)))
        try:window['after_us']=server_clock(pg,database)
        except BaseException as error:fixture['errors'].append(dict(stage='clock',error=repr(error)))
        for label,value in [('response',answer),('clock',window),('fixture',fixture)]:preserve(label,value)
        try:observed(pg,client,output,name+'-provisional',database,rows,setup,native=False)
        except BaseException as error:fixture['errors'].append(dict(stage='provisional',error=repr(error)))
        try:
            normal.observe_backend_absence(pg,database,output,name+'-control')
            current=rows(database);old={(r['resource_id'],r['epoch']) for r in before['control']['owner_epochs']}
            expected=[dict(pid=r['control_backend_pid'],backend_start=r['control_backend_start'],database=database)
                for r in current['owner_epochs'] if r['role']=='control' and (r['resource_id'],r['epoch']) not in old]
            assert len(expected)==1
            backend['exact']=normal.exact_absence(pg,database,expected)
            backend['all']={db:lab.sql(pg,db,'SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid()')
                for db in (database,setup['resource']['catalog_database'])}
            assert all(v=='0' for v in backend['all'].values())
            if closed and holder_closed:after=observed(pg,client,output,name+'-after',database,rows,setup)
        except BaseException as error:fixture['errors'].append(dict(stage='external',error=repr(error)))
        preserve('backends',backend);preserve('fixture',fixture)
        preserve('custody',dict(status='OBSERVED_CLOSED_AND_JOINED' if after else 'UNKNOWN',process_closed=closed,holder_closed=holder_closed,errors=fixture['errors']))
    if failure:raise failure
    assert after is not None and not fixture['errors']
    value=dict(step=case,operation='catchup',database=database,config=config,config_text=config_text,before=mutation['after'],after=after,
        response=answer,window=window,fixture=fixture,backend=backend,mutation=mutation,schedule=schedule)
    lab.write_json(output/(name+'-verification.json'),value);return value


def run(pg,client,output,database,name,rows,*,case,**context):
    assert case in CASES
    return normal.run(pg,client,output,database,name,rows,continuation=functools.partial(continue_refusal,case=case),**context)


def continue_refusal(pg,client,output,database,rows,setup,initialized,initial_after,wanted,initial_result,*,case):
    from installed_archive_catch_up_refusal_oracle import verify,negative_controls
    from installed_family_replay import ENDPOINT
    for folder,filename in [('ingest-bootstrap','installed_archive_catch_up_client.py'),('ingest-succession','installed_family_client.py')]:
        shutil.copy2(SOURCE/'experiments'/folder/filename,output/filename)
    baseline=normal.snapshot(pg,client,output,'catch-up-initial',database,rows,setup)
    assert baseline['control']==initial_after
    value=dict(status='RUNNING',case=case,database=database,config=setup['config'],initial=initial_result,initialized=initialized,
        setup_resource=setup['resource'],initial_setup={k:setup[k] for k in ('control','catalog','resource')},initial_state=baseline,
        initial_science={k:wanted[k] for k in ('tables','batches')},initial_event_hex=(setup['home']/'event.jsonl').read_bytes().hex(),
        initial_transcript=[read(p) for p in sorted(setup['home'].glob('relay-closed-*.json'))],steps=[],refusal=None,client=client)
    def save():lab.write_json(output/'archive-catch-up-refusal.json',value)
    save();before=baseline
    try:
        for step in normal.STEPS[:9]:
            action=None
            if step=='live3':
                action=normal.family_action(pg,client,output,database,rows,setup)
                cfg=output/Path(setup['config']).name
                rules='[{endpoint="'+ENDPOINT+'",cidr="127.0.0.1/32",port=7447}]'
                cfg.write_text(normal.set_value(cfg.read_text(),'network.private','rules',[]).replace('rules = []','rules = '+rules))
                normal.invoke(client,output,'catch-up-live3-config','config','validate','--file',setup['config'])
            phase=normal.execute(pg,client,output,database,rows,setup,step,before,action)
            value['steps'].append(phase);before=phase['after'];save()
        value['refusal']=execute(pg,client,output,database,rows,setup,case,before);save()
        if case in WAIT_CASES:
            result=lab.docker('exec',client,'python3','-B','/lab/installed_archive_read.py',setup['config'],
                '/lab/'+setup['home'].name+'/census-refusal-final','--full-stream-order',check=False,timeout=40)
            value['readback']=dict(returncode=result.returncode,stdout=result.stdout,stderr=result.stderr)
            lab.write_json(output/'catch-up-refusal-final-readback.json',value['readback']);save()
            assert result.returncode==0 and not result.stderr
            after=value['refusal']['after']
            assert rows(database)==after['control'] and normal.catalog_rows(pg,setup['resource']['catalog_database'])==after['catalog']
            assert all(normal.physical(setup['home'])[a]==after['physical'][a] for a in ('data','state'))
        value['oracle']=verify(value);value['negative_controls']=negative_controls(value);value['status']='PASS';save()
        return dict(name='archive-export',status='PASS',case=case,scope='one fresh same-root refusal; no outcome normalization',
            installed_processes=11,oracle=value['oracle'],negative_controls=value['negative_controls'],initial=initial_result)
    except BaseException as error:
        value.update(status='FAIL',error=repr(error));save();raise
