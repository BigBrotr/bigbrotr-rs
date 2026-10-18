#!/usr/bin/env python3
"""Ten installed processes and two genuine interrupted setups on five fresh copies.

The outer qualified runner owns resource/copy/build exclusion. This harness
never opens an original and never opens copied native state after unknown
process custody. Only the installed APIs create closure, gaps and retirement.
"""
from pathlib import Path
import json
import shutil
import sys

sys.dont_write_bytecode = True
import transition
from installed_family import configure
from installed import messages, wait_for
from installed_empty import exact_absence, signal_child
from installed_prepared_model import PINS
from installed_prepared_replay import CLONES, ORIGIN_FOR_CLONE, SPECS, CASES, STATUS, process_closed

history, schema, lab = transition.history, transition.schema, transition.lab
read, normalized = transition.read, transition.normalized
ORIGINS = tuple(PINS.values())
GRANTS = ('GRANT UPDATE ON control.producer_epochs TO bigbrotr_control;',
          'GRANT INSERT ON control.producer_outcomes TO bigbrotr_control;')
SUITE = transition.Suite(directory='installed-prepared', label='org.bigbrotr.installed-prepared',
    status=STATUS, client_prefix='bigbrotr-installed-prepared-', database_prefix='bb_installed_prepared_',
    clones=CLONES,cases=CASES,
    scope='Genuine prepare COMMIT reply loss and native-close/SQL COMMIT refusal setups; installed gap, OPEN and CLOSING empty retirement/retry; '
          'two fresh continuous Serve4/5 lifetimes and two classified installed refusals',
    not_run=('No activation/adoption of an interrupted candidate; no new job or HTTP/Sync input',
             'No import closure, Archive, ACK/GC, later capacity or RSS qualification'))


def execute(pg, client, output, case, directory, expected, catalog, seed):
    from installed_prepared_replay import verify, negative_controls
    name,database=case['name'],case['database']; prefix=output/name
    before=schema.rows(pg,database)
    native=history.dump(client,output,name+'-before',directory)
    inputs=history.inputs_at(directory/'store')
    lab.write_json(output/(name+'-before.json'),before)
    lab.write_json(output/(name+'-inputs.json'),inputs)
    assert normalized(before)==normalized(expected[0]) and native==expected[1] and inputs==expected[2]
    snapshots,window,fixture=[],{},dict(cleanup_errors=[])
    response=dict(returncode=None,stdout='',stderr='',outcome='OUTCOME_UNKNOWN',terminal=None)
    failure=terminal=None

    def snapshot(stage):
        value=dict(stage=stage,server_us=history.server_clock(pg,database),
            control=schema.rows(pg,database),inputs=history.inputs_at(directory/'store'))
        value['observed_by_us']=history.server_clock(pg,database)
        snapshots.append(value)
        lab.write_json(output/(name+'-'+stage+'.json'),value['control'])
        lab.write_json(output/(name+'-observations.json'),snapshots)
        return value

    try:
        window['before_us']=history.server_clock(pg,database)
        lab.write_json(output/(name+'-clock.json'),window)
        lab.docker('exec','-d',client,'python3','/lab/installed_prepared_client.py',case['operation'],
            case['config'],'/lab/'+name,timeout=5)
        wait_for(lambda: Path(str(prefix)+'.pid.json').exists() or Path(str(prefix)+'.terminal.json').exists(),
            'installed cycle process identity',5)
        if case['operation']=='live':
            published=wait_for(lambda: messages(prefix) or Path(str(prefix)+'.terminal.json').exists(),
                'new successor activation report',15)
            assert isinstance(published,list) and len(published)==1, 'Serve exited before activation'
            initial,=published
            initialized=snapshot('activated')
            epoch=int(initial['producer_epoch'])
            target,=[r for r in initialized['control']['owner_epochs'] if r['role']=='ingest' and r['epoch']==epoch]
            deadline=int(initial['lease_expires_at_us'])
            minimum=max(deadline,int(initial['prepared_at_us'])+6_000_000)
            def renewed():
                now=history.server_clock(pg,database)
                assert not Path(str(prefix)+'.terminal.json').exists(), 'Serve exited before renewal'
                return now if now>minimum else None
            at=wait_for(renewed,'lifetime beyond initial deadline and six seconds',15)
            current=snapshot('renewed')
            fixture['renewal']=dict(initial_deadline_us=deadline,observed_at_us=at,initial_target=target,current=current)
            snapshot('before-stop')
            identity=read(Path(str(prefix)+'.pid.json'))
            signal=dict(kind='SIGTERM',identity=identity,before_us=history.server_clock(pg,database))
            signal_child(client,identity)
            signal['after_us']=history.server_clock(pg,database)
            fixture['signal']=signal
            lab.write_json(output/(name+'-signal.json'),signal)
        wait_for(Path(str(prefix)+'.terminal.json').exists,'installed process joined',45 if case['operation']!='live' else 12)
        terminal=read(Path(str(prefix)+'.terminal.json'))
    except BaseException as error:
        failure=error
        response['capture_error']=repr(error)
    finally:
        path=Path(str(prefix)+'.terminal.json')
        try:
            if terminal is None and path.exists(): terminal=read(path)
            if terminal is None and Path(str(prefix)+'.pid.json').exists():
                signal_child(client,read(Path(str(prefix)+'.pid.json')))
                wait_for(path.exists,'failed installed process join',5)
                terminal=read(path)
        except BaseException as error:
            fixture['cleanup_errors'].append(dict(stage='process-join',error=repr(error)))
        try: window['after_us']=history.server_clock(pg,database)
        except BaseException as error: fixture['cleanup_errors'].append(dict(stage='final-clock',error=repr(error)))
        for stream in ('stdout','stderr'):
            try:
                path=Path(str(prefix)+'.'+stream)
                if path.exists():
                    data=path.read_bytes(); assert len(data)<=65536
                    response[stream]=data.decode()
            except BaseException as error: fixture['cleanup_errors'].append(dict(stage=stream,error=repr(error)))
        response['terminal']=terminal
        try:
            if terminal is not None:
                assert type(terminal) is dict, 'malformed terminal object'
                assert type(terminal['reaped']) is bool and type(terminal['forced']) is bool
                assert terminal['exit_code'] is None or type(terminal['exit_code']) is int
                if terminal['reaped'] is True and terminal['forced'] is False:
                    response.update(returncode=terminal['exit_code'],outcome='PROCESS_EXITED')
        except BaseException as error:
            fixture['cleanup_errors'].append(dict(stage='terminal-shape',error=repr(error)))
        for suffix,value in [('response',response),('clock',window),('fixture',fixture),('observations',snapshots)]:
            lab.write_json(output/(name+'-'+suffix+'.json'),value)
        after=after_inputs=after_native=after_catalog=None
        backend,errors={},[]
        # Always retain provisional SQL/input observations, even if the wrapper
        # reply cannot prove native custody. They are not a final-state claim.
        for stage,action in [('sql',lambda:schema.rows(pg,database)),
                             ('inputs',lambda:history.inputs_at(directory/'store'))]:
            try: lab.write_json(output/(name+'-provisional-'+stage+'.json'),action())
            except BaseException as error: errors.append(dict(stage='provisional-'+stage,error=repr(error)))
        closed=False
        try: process_closed(response,case); closed=True
        except BaseException as error: errors.append(dict(stage='native-custody',error=repr(error)))
        try:
            backend['absence']=history.observe_backend_absence(pg,database,output,name+'-control')
            # Final SQL is observed only after backend exclusion.
            after=schema.rows(pg,database)
            lab.write_json(output/(name+'-after.json'),after)
            after_inputs=history.inputs_at(directory/'store')
            lab.write_json(output/(name+'-after-inputs.json'),after_inputs)
            old={(r['resource_id'],r['epoch']) for r in before['owner_epochs']}
            tuples=[dict(pid=r['control_backend_pid'],backend_start=r['control_backend_start'],database=database,control_epoch=r['epoch'])
                for r in after['owner_epochs'] if r['role']=='control' and (r['resource_id'],r['epoch']) not in old]
            backend['exact']=exact_absence(pg,database,tuples)
            backend['all']=schema.absence(pg,database); assert backend['all']==[]
            lab.write_json(output/(name+'-backends.json'),backend)
        except BaseException as error: errors.append(dict(stage='backend-absence',error=repr(error)))
        if closed and not errors:
            try:
                after_native=history.dump(client,output,name+'-after',directory)
                after_catalog=history.catalog(pg,database)
            except BaseException as error: errors.append(dict(stage='external-native-catalog',error=repr(error)))
        else:
            lab.write_json(output/(name+'-external-unavailable.json'),dict(status='OUTCOME_UNKNOWN',native='NOT_RUN',
                sql_and_inputs='provisional',cleanup='outer owned client exclusion'))
        lab.write_json(output/(name+'-external.json'),dict(control=after,native=after_native,inputs=after_inputs,backend=backend,catalog=after_catalog))
        custody=dict(status='OBSERVED_CLOSED_AND_JOINED' if closed and not errors else 'OUTCOME_UNKNOWN',
            database=database,backend_absent=backend.get('all')==[],errors=errors)
        lab.write_json(output/(name+'-custody.json'),custody)
        case['custody']=custody['status']
    if failure is not None: raise failure
    assert not fixture['cleanup_errors'] and not errors
    contents={p.name:p.read_bytes().hex() for p in (directory/'store/producer-inputs-v1').iterdir()}
    context=dict(case=dict(case),before=before,after=after,native_before=native,native_after=after_native,
        inputs_before=inputs,inputs_after=after_inputs,input_contents=contents,snapshots=snapshots,response=response,
        fixture=fixture,server_window=window,backend=backend,catalog_before=catalog,catalog_after=after_catalog)
    lab.write_json(output/(name+'-verification.json'),context)
    result,controls=verify(context,seed),negative_controls(context,seed)
    case.update(status=result['status'],returncode=response['returncode'],oracle=result,negative_controls=controls,server_window=window)
    return after,after_native,after_inputs


def setup(pg,client,output,case,directory,expected,proxy):
    """The ordinary finite command creates the incomplete durable state."""
    from transition_faults import invoke, proxy_closed
    from installed_prepared_setup import verify, negative_controls
    name,database=case['name'],case['database']
    before=schema.rows(pg,database); inputs=history.inputs_at(directory/'store')
    native=history.dump(client,output,name+'-before',directory)
    lab.write_json(output/(name+'-before.json'),before);lab.write_json(output/(name+'-inputs.json'),inputs)
    assert normalized(before)==normalized(expected[0]) and native==expected[1] and inputs==expected[2]
    mode,commit=('after-complete',11) if case['operation']=='prepare' else ('before-forward',12)
    fault=dict(point=case['operation'],mode=mode,commit_number=commit,proxy_port=None,wire=dict(status='NOT_RUN'))
    response=dict(returncode=None,stdout='',stderr='',outcome='UNKNOWN');window={}
    try:
        response,window=invoke(pg,client,output,case,proxy,fault,commit_number=commit)
    finally:
        path=output/(name+'-response.json')
        if path.exists():
            try:
                value=read(path);assert type(value) is dict
                response=value
            except (ValueError,TypeError,AssertionError) as error:response['response_error']=repr(error)
        case['returncode']=response.get('returncode')
        events,after,after_native,after_inputs=transition.external(pg,client,output,case,directory,response,
            control_failed=True,proxy_closed=proxy_closed(fault))
    assert type(response['returncode']) is int and response['returncode']==0 and response['stderr']==''
    assert not any(k in response for k in ('response_error','orchestration_error','clock_error'))
    result=verify(events,before,native,inputs,case,window,fault=fault)
    controls=negative_controls(events,before,native,inputs,case,window,fault=fault)
    final,=[e for e in events if e['event']=='lineage-history-state' and e['stage']=='final']
    assert normalized(after)==normalized({k:json.loads(v) for k,v in final['state']['control_tables']})
    assert after_native==dict(event='dump-state',rows=final['rows'],sequence=final['state']['sequence']) and after_inputs==inputs
    case.update(status='PASS_SETUP_COMMIT_UNKNOWN',oracle=result,negative_controls=controls,
        response_bytes=len(response['stdout'].encode()),server_window=window,fault=fault)
    return after,after_native,after_inputs


def run_sequence(pg,client,output,clone,directory,expected,report):
    helper=Path(__file__).with_name('installed_prepared_client.py');target=output/helper.name
    if not target.exists():shutil.copy2(helper,target)
    assert history.digest(target)==history.digest(helper)
    proxy_source=history.SOURCE/'experiments/control-migrations/fault_proxy.py'
    proxy=output/'installed-prepared-proxy.py'
    if not proxy.exists():shutil.copy2(proxy_source,proxy)
    assert history.digest(proxy)==history.digest(proxy_source)
    configure(pg,client,output,clone['config'])
    seed=expected
    for name,mode,operation,predecessor in SPECS:
        if mode!=clone['name']:continue
        case=dict(name=name,clone=mode,origin=clone['origin'],operation=operation,predecessor=predecessor,
            database=clone['database'],config=clone['config'],status='RUNNING')
        if operation in ('prepare','empty-setup'):
            case.update(operation='empty' if operation=='empty-setup' else operation,metadata_bytes=262144,proxy_port=None,setup=True)
        report['cases'].append(case);lab.write_json(output/'result.json',report)
        if case.get('setup'):
            expected=setup(pg,client,output,case,directory,expected,proxy)
        else:
            expected=execute(pg,client,output,case,directory,expected,clone['catalog_before'],seed)
        case['catalog_after']=history.catalog(pg,clone['database']);assert case['catalog_after']==clone['catalog_before']
        lab.write_json(output/'result.json',report)
    return case['name']


def run(root,build):
    return transition.run(root,build,suite=SUITE,sequence=run_sequence,origin_specs=ORIGINS,
        origin_for_clone=ORIGIN_FOR_CLONE,extra_grants=GRANTS)


if __name__=='__main__':
    assert len(sys.argv)==3
    run(Path(sys.argv[1]).resolve(strict=True),Path(sys.argv[2]).resolve(strict=True))
