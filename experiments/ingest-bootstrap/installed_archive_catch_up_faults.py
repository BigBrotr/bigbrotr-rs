"""Four fresh catch-up processes on one genuinely initialized physical root.

The proxy selects an actual publication/frontier transaction by query and omits
its observed COMMIT completion. No captured row or copied coordinator is adopted.
Only the isolated runner may execute this module; import performs no I/O.
"""
import json
from pathlib import Path
import shutil
import sys

import installed_archive_catch_up as normal
from installed_archive_catch_up import (SOURCE, lab, read, snapshot, response,
    wait_for, set_value, observe_backend_absence, exact_absence, server_clock)
from installed_archive_catch_up_client import argv

STATUS='PASS_INSTALLED_ARCHIVE_CATCH_UP_FAULTS'
CASES=('publication-loss','frontier-loss','resolution','exact-retry')
TARGETS={'publication-loss':'publication','frontier-loss':'control-frontier'}
PROXY='archive-catch-up-commit-proxy.py'


def local_records(home):
    """Complete small retained journals, including actual inode/boot identities."""
    paths=sorted((home/'state').rglob('*'))
    assert len(paths)<=128
    result={};total=0
    for path in paths:
        assert not path.is_symlink()
        if path.is_file():
            data=path.read_bytes();total+=len(data)
            assert len(data)<=1024*1024 and total<=8*1024*1024
            result[str(path.relative_to(home/'state'))]=data.hex()
    return result


def observed(pg,client,output,label,database,rows,setup,*,native=True):
    value=snapshot(pg,client,output,label,database,rows,setup,native=native)
    value['local_records']=local_records(setup['home'])
    lab.write_json(output/(label+'-state.json'),value)
    return value


def proxy_result(process,command):
    return dict(command=command,exit=process.process.poll(),stdout=process.stdout.decode(errors='replace'),
        stderr=process.stderr.decode(errors='replace'),overflow=process.overflow,
        readers_joined=not any(t.is_alive() for t in process.threads))


def closed(answer,config,prefix,loss):
    """A reaped error is not a successful report or remote-wrapper assumption."""
    assert answer['outcome']=='PROCESS_EXITED' and type(answer['returncode']) is int
    assert answer['returncode']==(4 if loss else 0)
    terminal=answer['terminal']
    assert terminal['command']==argv('catchup',Path(config)) and terminal['config']==config
    assert terminal['prefix']=='/lab/'+prefix.name and terminal['reaped'] is True
    assert terminal['forced'] is False and terminal['error'] is None
    assert type(terminal['pid']) is int and terminal['pid']>0
    assert type(terminal['start_ticks']) is int and terminal['start_ticks']>0
    if loss:
        from installed_archive_catch_up_fault_oracle import ERRORS
        assert not answer['stdout'] and answer['stderr']==ERRORS[prefix.name.removeprefix('catch-up-')]
    else:normal.process_closed(answer,'catchup',config,prefix)


def execute(pg,client,output,database,rows,setup,case,previous):
    # Imports are inert; Process never starts until the explicit runtime call.
    sys.path.insert(0,str(SOURCE/'experiments/control-migrations'))
    from fault_cases import Process
    name='catch-up-'+case;prefix=output/name;target=TARGETS.get(case)
    config='/lab/'+name+'.toml';config_path=output/Path(config).name
    direct=(output/Path(setup['config']).name).read_text()
    config_path.write_text(direct)
    before=observed(pg,client,output,name+'-before',database,rows,setup)
    assert {k:v for k,v in before.items() if k not in ('physical','local_records')}=={
        k:v for k,v in previous.items() if k not in ('physical','local_records')}
    if 'local_records' in previous:assert before['local_records']==previous['local_records']
    window=dict(before_us=server_clock(pg,database));fixture=dict(errors=[],snapshots=[])
    wire=None;proxy=None;proxy_command=None;failure=None;after=None;process_closed=False;proxy_closed=target is None
    answer=dict(returncode=None,stdout='',stderr='',outcome='OUTCOME_UNKNOWN',terminal=None);backend={}
    def save_wire():lab.write_json(output/(name+'-wire.json'),wire)
    try:
        if target:
            proxy_command=['docker','exec','-i',client,'python3','-B','/lab/'+PROXY,
                '--target',target,'--mode','after-complete','--profile','retained-catch-up']
            proxy=Process(proxy_command,input_pipe=True,limit=256*1024)
            wire=dict(status='UNKNOWN',target=target,mode='after-complete',ready=None,process=proxy_result(proxy,proxy_command))
            save_wire()
            def ready():
                for line in bytes(proxy.stdout).split(b'\n')[:-1]:
                    item=json.loads(line)
                    if item.get('event')=='ready':return item
                assert not proxy.overflow and proxy.process.poll() is None
            wire['ready']=wait_for(ready,name+' proxy ready',5);save_wire()
            assert wire['ready']['source_sha256']==normal.digest(output/PROXY)
            port=wire['ready']['port'];assert type(port) is int and 1024<=port<=65535
            section='postgres.catalog' if target=='publication' else 'postgres.control'
            config_path.write_text(set_value(direct,section,'endpoint','127.0.0.1:'+str(port)))
        (output/(name+'-config.toml')).write_text(config_path.read_text())
        lab.docker('exec','-d',client,'python3','-B','/lab/installed_archive_catch_up_client.py',
            'catchup',config,'/lab/'+name,timeout=5)
        wait_for(lambda:Path(str(prefix)+'.pid.json').exists() or Path(str(prefix)+'.terminal.json').exists(),name+' identity',5)
        wait_for(Path(str(prefix)+'.terminal.json').exists,name+' actual process join',110)
    except BaseException as error:failure=error
    finally:
        try:
            if not Path(str(prefix)+'.terminal.json').exists() and Path(str(prefix)+'.pid.json').exists():
                normal.signal_child(client,read(Path(str(prefix)+'.pid.json')))
                wait_for(Path(str(prefix)+'.terminal.json').exists,name+' cleanup join',5)
        except BaseException as error:fixture['errors'].append(dict(stage='join',error=repr(error)))
        try:
            answer=response(prefix);closed(answer,config,prefix,target is not None);process_closed=True
        except BaseException as error:fixture['errors'].append(dict(stage='process-custody',error=repr(error)))
        if proxy:
            try:
                proxy.process.stdin.write(b'stop\n');proxy.process.stdin.flush()
                assert proxy.wait(5)==0
                wire['process']=proxy_result(proxy,proxy_command)
                from installed_archive_catch_up_fault_oracle import wire_checked
                wire_checked(wire,target,normal.digest(output/PROXY),client)
                wire['status']='OBSERVED_CLOSED_AND_JOINED';proxy_closed=True
            except BaseException as error:fixture['errors'].append(dict(stage='proxy-custody',error=repr(error)))
            finally:
                # Killing docker-exec does not assert remote exclusion. Unknown
                # stays unknown and enclosing owned-container cleanup is required.
                try:proxy.stop()
                except BaseException as error:fixture['errors'].append(dict(stage='proxy-wrapper-cleanup',error=repr(error)))
                wire['process']=proxy_result(proxy,proxy_command);save_wire()
        else:save_wire()
        try:window['after_us']=server_clock(pg,database)
        except BaseException as error:fixture['errors'].append(dict(stage='clock',error=repr(error)))
        for label,value in [('response',answer),('clock',window),('fixture',fixture)]:lab.write_json(output/(name+'-'+label+'.json'),value)
        try:observed(pg,client,output,name+'-provisional',database,rows,setup,native=False)
        except BaseException as error:fixture['errors'].append(dict(stage='provisional',error=repr(error)))
        try:
            observe_backend_absence(pg,database,output,name+'-control')
            current=rows(database);old={(r['resource_id'],r['epoch']) for r in before['control']['owner_epochs']}
            expected=[dict(pid=r['control_backend_pid'],backend_start=r['control_backend_start'],database=database)
                for r in current['owner_epochs'] if r['role']=='control' and (r['resource_id'],r['epoch']) not in old]
            assert len(expected)==1
            backend['exact']=exact_absence(pg,database,expected)
            backend['all']={db:lab.sql(pg,db,'SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid()')
                for db in (database,setup['resource']['catalog_database'])}
            assert all(v=='0' for v in backend['all'].values())
            if process_closed and proxy_closed:
                after=observed(pg,client,output,name+'-after',database,rows,setup)
        except BaseException as error:fixture['errors'].append(dict(stage='external',error=repr(error)))
        lab.write_json(output/(name+'-backends.json'),backend);lab.write_json(output/(name+'-fixture.json'),fixture)
        lab.write_json(output/(name+'-custody.json'),dict(status='OBSERVED_CLOSED_AND_JOINED' if after is not None else 'UNKNOWN',
            process_closed=process_closed,proxy_closed=proxy_closed,errors=fixture['errors']))
    if failure:raise failure
    assert after is not None and not fixture['errors']
    value=dict(step=case,operation='catchup',database=database,config=config,config_text=config_path.read_text(),
        before=before,after=after,response=answer,window=window,fixture=fixture,backend=backend,wire=wire)
    lab.write_json(output/(name+'-verification.json'),value)
    return value


def run(pg,client,output,database,name,rows,**context):
    return normal.run(pg,client,output,database,name,rows,continuation=continue_faults,**context)


def continue_faults(pg,client,output,database,rows,setup,initialized,initial_after,wanted,initial_result):
    from installed_archive_catch_up_fault_oracle import verify,negative_controls
    from installed_family_replay import ENDPOINT
    for folder,filename in [('ingest-bootstrap','installed_archive_catch_up_client.py'),('ingest-succession','installed_family_client.py')]:
        shutil.copy2(SOURCE/'experiments'/folder/filename,output/filename)
    shutil.copy2(SOURCE/'experiments/archive-engine/native_commit_proxy.py',output/PROXY)
    baseline=snapshot(pg,client,output,'catch-up-initial',database,rows,setup)
    assert baseline['control']==initial_after
    value=dict(status='RUNNING',database=database,config=setup['config'],initial=initial_result,initialized=initialized,
        setup_resource=setup['resource'],initial_setup={k:setup[k] for k in ('control','catalog','resource')},initial_state=baseline,
        initial_science={k:wanted[k] for k in ('tables','batches')},initial_event_hex=(setup['home']/'event.jsonl').read_bytes().hex(),
        initial_transcript=[read(p) for p in sorted(setup['home'].glob('relay-closed-*.json'))],steps=[],faults=[],
        proxy_sha256=normal.digest(output/PROXY),client=client)
    def save():lab.write_json(output/'archive-catch-up-faults.json',value)
    save();before=baseline
    try:
        for step in normal.STEPS[:9]:
            action=None
            if step=='live3':
                action=normal.family_action(pg,client,output,database,rows,setup)
                cfg=output/Path(setup['config']).name
                rules='[{endpoint="'+ENDPOINT+'",cidr="127.0.0.1/32",port=7447}]'
                cfg.write_text(set_value(cfg.read_text(),'network.private','rules',[]).replace('rules = []','rules = '+rules))
                normal.invoke(client,output,'catch-up-live3-config','config','validate','--file',setup['config'])
            phase=normal.execute(pg,client,output,database,rows,setup,step,before,action)
            value['steps'].append(phase);before=phase['after'];save()
        for case in CASES:
            phase=execute(pg,client,output,database,rows,setup,case,before)
            value['faults'].append(phase);before=phase['after'];save()
        # Independent native C API science at the final complete frontier only;
        # no HTTP, export, reader lease or ordinary-runtime repetition here.
        result=lab.docker('exec',client,'python3','-B','/lab/installed_archive_read.py',setup['config'],
            '/lab/'+setup['home'].name+'/census-fault-final','--full-stream-order',check=False,timeout=40)
        value['readback']=dict(returncode=result.returncode,stdout=result.stdout,stderr=result.stderr)
        lab.write_json(output/'catch-up-fault-final-readback.json',value['readback']);save()
        assert result.returncode==0 and not result.stderr
        assert rows(database)==before['control'] and normal.catalog_rows(pg,setup['resource']['catalog_database'])==before['catalog']
        assert all(normal.physical(setup['home'])[a]==before['physical'][a] for a in ('data','state'))
        value['oracle']=verify(value);value['negative_controls']=negative_controls(value);value['status']='PASS';save()
        return dict(name='archive-export',status='PASS',scope='same physical root publication and first bridge COMMIT reply loss',
            installed_processes=14,scientific_records=22,archive_receipts=7,final_snapshot=8,
            oracle=value['oracle'],negative_controls=value['negative_controls'],initial=initial_result)
    except BaseException as error:
        value.update(status='FAIL',error=repr(error));save();raise
