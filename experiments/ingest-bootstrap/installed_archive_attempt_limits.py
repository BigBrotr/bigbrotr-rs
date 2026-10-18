"""Two genuine attempt-boundary refusals on one new same-physical-root lineage.

The embedded client owns actual subprocess reap. Import is inert; only explicit
runner calls execute anything. No SQL mutation, catalog seal or fake receipt.
"""
import json
from pathlib import Path
import sys

COMMANDS={'holder':'successor-archive-attempt-holder','cap-setup':'successor-archive-attempt-cap'}
LIMITS={'holder':240,'cap-setup':1800,'held-refusal':120,'cap-refusal':4200}


def command(mode,database,config,prefix):
    assert mode in LIMITS and config.parent==prefix.parent==Path('/lab') and config.suffix=='.toml'
    if mode not in COMMANDS:return ['/build/bigbrotr','archive','catch-up','--file',str(config)]
    return ['/build/ingest_bootstrap_probe',COMMANDS[mode],database,'/run/secrets/application',str(config),'262144']+([str(prefix)+'.release'] if mode=='holder' else [])


def client(mode,database,config,prefix):
    import os
    import subprocess
    import time
    def publish(path,value):
        temporary=Path(str(path)+'.tmp')
        with temporary.open('x') as stream:json.dump(value,stream)
        temporary.rename(path)
    started=time.monotonic();forced=False;error=None;cmd=command(mode,database,config,prefix)
    with Path(str(prefix)+'.stdout').open('xb') as out,Path(str(prefix)+'.stderr').open('xb') as err:
        child=subprocess.Popen(cmd,stdout=out,stderr=err,env={})
        identity=dict(pid=child.pid,config=str(config),prefix=str(prefix),command=cmd)
        try:
            identity['start_ticks']=int(Path(f'/proc/{child.pid}/stat').read_text().split()[21])
            publish(Path(str(prefix)+'.pid.json'),identity);child.wait(timeout=LIMITS[mode])
        except BaseException as caught:
            forced=True;error=repr(caught);child.kill();child.wait(timeout=5);raise
        finally:publish(Path(str(prefix)+'.terminal.json'),dict(identity,reaped=child.returncode is not None,
            exit_code=child.returncode,forced=forced,error=error,elapsed_seconds=time.monotonic()-started))


if __name__=='__main__' and len(sys.argv)>1 and sys.argv[1]=='client':
    assert len(sys.argv)==6
    client(sys.argv[2],sys.argv[3],Path(sys.argv[4]),Path(sys.argv[5]));sys.exit(0)

import shutil
import installed_archive_catch_up as normal
from installed_archive_catch_up import SOURCE,lab,read,server_clock,wait_for
STATUS='PASS_INSTALLED_ARCHIVE_ATTEMPT_LIMITS'


# The new finite probe emits full bounded SQL/native snapshots. The installed
# report helper's 128 KiB stdout limit is intentionally unchanged for old modes.
OUTPUT_BYTES=128*1024*1024

def stream(prefix,name):
    path=Path(str(prefix)+'.'+name)
    if not path.exists():return ''
    limit=OUTPUT_BYTES if name=='stdout' else 131072
    with path.open('rb') as opened:raw=opened.read(limit+1)
    assert len(raw)<=limit
    return raw.decode()


def messages(prefix):
    lines=stream(prefix,'stdout').splitlines(keepends=True)
    assert len(lines)<=2048 and all(len(line.encode())<=16*1024*1024 for line in lines)
    return [json.loads(line) for line in lines if line.endswith('\n')]


def response(prefix):
    result=dict(returncode=None,stdout=stream(prefix,'stdout'),stderr=stream(prefix,'stderr'),outcome='OUTCOME_UNKNOWN',terminal=None)
    path=Path(str(prefix)+'.terminal.json')
    if path.exists():
        terminal=read(path);result['terminal']=terminal
        assert type(terminal) is dict and type(terminal['reaped']) is bool and type(terminal['forced']) is bool
        assert terminal['exit_code'] is None or type(terminal['exit_code']) is int
        if terminal['reaped'] and not terminal['forced']:
            result.update(returncode=terminal['exit_code'],outcome='PROCESS_EXITED')
    return result


def observed(pg,client,output,label,database,rows,setup,*,native=True):
    value=normal.snapshot(pg,client,output,label,database,rows,setup,native=native)
    paths=sorted((setup['home']/'state').rglob('*'));assert len(paths)<=1024
    records={};total=0
    for path in paths:
        assert not path.is_symlink()
        if path.is_file():
            raw=path.read_bytes();total+=len(raw);assert len(raw)<=1024*1024 and total<=16*1024*1024
            records[str(path.relative_to(setup['home']/'state'))]=raw.hex()
    value['local_records']=records;lab.write_json(output/(label+'-state.json'),value);return value


def start(client,output,database,setup,mode):
    name='attempt-'+mode;prefix=output/name
    (output/(name+'-config.toml')).write_text((output/Path(setup['config']).name).read_text())
    lab.docker('exec','-d',client,'python3','-B','/lab/installed_archive_attempt_limits.py','client',mode,database,
        setup['config'],'/lab/'+name,timeout=5)
    wait_for(lambda:Path(str(prefix)+'.pid.json').exists() or Path(str(prefix)+'.terminal.json').exists(),name+' start',5)
    return prefix


def backends(pg,output,database,setup,name,before,rows):
    normal.observe_backend_absence(pg,database,output,name+'-control')
    current=rows(database);old={(r['resource_id'],r['epoch']) for r in before['control']['owner_epochs']}
    expected=[dict(pid=r['control_backend_pid'],backend_start=r['control_backend_start'],database=database)
        for r in current['owner_epochs'] if r['role']=='control' and (r['resource_id'],r['epoch']) not in old]
    assert len(expected)==1
    value=dict(exact=normal.exact_absence(pg,database,expected),all={db:lab.sql(pg,db,
        'SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid()')
        for db in (database,setup['resource']['catalog_database'])})
    assert all(n=='0' for n in value['all'].values());lab.write_json(output/(name+'-backends.json'),value);return value


def finish(pg,client,output,database,rows,setup,mode,before,window,*,prefix=None):
    from installed_archive_attempt_limits_oracle import process_checked
    name='attempt-'+mode;prefix=prefix or output/name
    value=dict(mode=mode,before=before,after=None,window=window,config=setup['config'],database=database,
        config_text=(output/(name+'-config.toml')).read_text(),response={'outcome':'OUTCOME_UNKNOWN'},backend=None,errors=[])
    closed=False
    try:
        wait_for(Path(str(prefix)+'.terminal.json').exists,name+' join',LIMITS[mode]+5)
        value['response']=response(prefix);process_checked(value['response'],mode,database,setup['config'])
        closed=True
    except BaseException as error:value['errors'].append(dict(stage='process',error=repr(error)))
    finally:
        # Unknown never permits native reopen. Outer runner retains resources and
        # stops the owned container if the bounded process could not be excluded.
        def capture(stage,call):
            try:return call()
            except BaseException as error:value['errors'].append(dict(stage=stage,error=repr(error)))
        capture('response-write',lambda:lab.write_json(output/(name+'-response.json'),value['response']))
        capture('provisional',lambda:observed(pg,client,output,name+'-provisional',database,rows,setup,native=False))
        def external():
            value['backend']=backends(pg,output,database,setup,name,before,rows)
            if closed:value['after']=observed(pg,client,output,name+'-after',database,rows,setup)
        capture('external',external)
        def clock():window['after_us']=server_clock(pg,database)
        capture('clock',clock)
        capture('clock-write',lambda:lab.write_json(output/(name+'-clock.json'),window))
        capture('custody-write',lambda:lab.write_json(output/(name+'-custody.json'),dict(status='OBSERVED_CLOSED_AND_JOINED' if value['after'] and not value['errors'] else 'UNKNOWN',errors=value['errors'])))
        lab.write_json(output/(name+'-verification.json'),value)
    assert value['after'] is not None and not value['errors']
    return value


def run(pg,client,output,database,name,rows,**context):
    return normal.run(pg,client,output,database,name,rows,continuation=continuation,**context)


def continuation(pg,client,output,database,rows,setup,initialized,initial_after,wanted,initial_result):
    from installed_archive_attempt_limits_oracle import verify,negative_controls
    from installed_family_replay import ENDPOINT
    for folder,filename in [('ingest-bootstrap','installed_archive_attempt_limits.py'),('ingest-bootstrap','installed_archive_catch_up_client.py'),('ingest-succession','installed_family_client.py')]:
        shutil.copy2(SOURCE/'experiments'/folder/filename,output/filename)
    baseline=normal.snapshot(pg,client,output,'catch-up-initial',database,rows,setup);assert baseline['control']==initial_after
    value=dict(status='RUNNING',database=database,config=setup['config'],initial=initial_result,initialized=initialized,
        setup_resource=setup['resource'],initial_setup={k:setup[k] for k in ('control','catalog','resource')},initial_state=baseline,
        initial_science={k:wanted[k] for k in ('tables','batches')},initial_event_hex=(setup['home']/'event.jsonl').read_bytes().hex(),
        initial_transcript=[read(p) for p in sorted(setup['home'].glob('relay-closed-*.json'))],steps=[],attempts=[],client=client)
    def save():lab.write_json(output/'archive-attempt-limits.json',value)
    save();before=baseline;holder=None
    try:
        for step in normal.STEPS[:9]:
            action=None
            if step=='live3':
                action=normal.family_action(pg,client,output,database,rows,setup)
                cfg=output/Path(setup['config']).name;rules='[{endpoint="'+ENDPOINT+'",cidr="127.0.0.1/32",port=7447}]'
                cfg.write_text(normal.set_value(cfg.read_text(),'network.private','rules',[]).replace('rules = []','rules = '+rules))
                normal.invoke(client,output,'catch-up-live3-config','config','validate','--file',setup['config'])
            phase=normal.execute(pg,client,output,database,rows,setup,step,before,action)
            value['steps'].append(phase);before=phase['after'];save()
        before=observed(pg,client,output,'attempt-holder-before',database,rows,setup)
        window=dict(before_us=server_clock(pg,database));holder=output/'attempt-holder'
        start(client,output,database,setup,'holder')
        def ready():
            events=messages(holder)
            return events if events and events[-1].get('event')=='archive-attempt-ready' else None
        events=wait_for(ready,'typed holder released native/global custody',60)
        assert events[-1]==dict(event='archive-attempt-ready',native='closed',control='released-and-joined',observer='joined',
            coordinator='released-and-reacquired',gate='/lab/attempt-holder.release',ordinal=1)
        backend=backends(pg,output,database,setup,'attempt-holder',before,rows)
        held=observed(pg,client,output,'attempt-holder-ready',database,rows,setup)
        window['after_us']=server_clock(pg,database)
        value['holder_ready']=dict(events=events,before=before,after=held,window=window.copy(),backend=backend)
        lab.write_json(output/'attempt-holder-ready.json',value['holder_ready']);save()
        refusal_before=observed(pg,client,output,'attempt-held-refusal-before',database,rows,setup)
        rw=dict(before_us=server_clock(pg,database));refused=start(client,output,database,setup,'held-refusal')
        # Keep the genuine holder alive through the expected installed refusal.
        wait_for(Path(str(refused)+'.terminal.json').exists,'held refusal terminal',125)
        from installed_archive_attempt_limits_oracle import process_checked
        process_checked(response(refused),'held-refusal',database,setup['config'])
        value['held_terminal_us']=server_clock(pg,database);save()
        Path(str(holder)+'.release').write_text('release\n');value['holder_release_us']=server_clock(pg,database);save()
        wait_for(Path(str(holder)+'.terminal.json').exists,'holder actual join',10)
        value['holder_response']=response(holder);process_checked(value['holder_response'],'holder',database,setup['config'])
        lab.write_json(output/'attempt-holder-response.json',value['holder_response']);holder=None;save()
        value['attempts'].append(finish(pg,client,output,database,rows,setup,'held-refusal',refusal_before,rw,prefix=refused));save()
        for mode in ('cap-setup','cap-refusal'):
            before=observed(pg,client,output,'attempt-'+mode+'-before',database,rows,setup)
            window=dict(before_us=server_clock(pg,database));prefix=start(client,output,database,setup,mode)
            value['attempts'].append(finish(pg,client,output,database,rows,setup,mode,before,window,prefix=prefix));save()
        value['oracle']=verify(value);value['negative_controls']=negative_controls(value);value['status']='PASS';save()
        return dict(name='archive-export',status='PASS',installed_processes=12,finite_probes=2,oracle=value['oracle'],negative_controls=value['negative_controls'],initial=initial_result)
    except BaseException as error:value.update(status='FAIL',error=repr(error));save();raise
    finally:
        if holder is not None:
            # Always release the owned holder even when any capture/write fails.
            try:
                Path(str(holder)+'.release').write_text('release\n')
                wait_for(Path(str(holder)+'.terminal.json').exists,'failed holder join',10)
                value['holder_cleanup']=response(holder)
            except BaseException as error:value['holder_cleanup']={'outcome':'OUTCOME_UNKNOWN','error':repr(error)}
            save()
