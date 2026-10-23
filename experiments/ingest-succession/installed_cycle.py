#!/usr/bin/env python3
"""Twenty-seven real installed processes on three fresh preserved endpoint copies.

The outer qualified runner owns resource/copy/build exclusion. This harness
never opens an original and never opens copied native state after unknown
process custody. Only the installed APIs create closure, gaps and retirement.
"""
from pathlib import Path
import shutil
import sys

sys.dont_write_bytecode = True
import transition
from installed_family import configure
from installed import messages, wait_for
from installed_empty import exact_absence, signal_child
from installed_cycle_model import PINS
from installed_cycle_replay import CLONES, STEPS, CASES, STATUS, process_closed

history, schema, lab = transition.history, transition.schema, transition.lab
read, normalized = transition.read, transition.normalized
ORIGINS = tuple(PINS[name] for name in CLONES)
GRANTS = ('GRANT UPDATE ON control.producer_epochs TO bigbrotr_control;',
          'GRANT INSERT ON control.producer_outcomes TO bigbrotr_control;')
SUITE = transition.Suite(directory='installed-cycle',label='org.bigbrotr.installed-successor-cycle',
    status=STATUS,client_prefix='bigbrotr-installed-cycle-',database_prefix='bb_installed_cycle_',
    clones=CLONES,cases=CASES,
    scope='Three installed positive recovery/source/whole-intent abandonment/retirement first+retry cycles; '
          'then fresh continuous Serve4/4/5 without Sync or HTTP commands, renewal and joined shutdown',
    not_run=('New acquisition in the final Serve; unresolved staged/reserved input tails',
             'Import closure, Archive, acknowledgement/GC, injected faults and capacity/RSS qualification'))


def execute(pg, client, output, case, directory, expected, catalog, seed, *, checks=None):
    if checks is None:
        from installed_cycle_replay import verify, negative_controls, process_closed
    else:
        # An explicit bounded profile may change semantic expectations while
        # retaining this exact process/backend/native capture and join path.
        verify, negative_controls, process_closed = checks
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
        lab.docker('exec','-d',client,'python3','/lab/installed_cycle_client.py',case['operation'],
            case['config'],'/lab/'+name,timeout=5)
        wait_for(lambda: Path(str(prefix)+'.pid.json').exists() or Path(str(prefix)+'.terminal.json').exists(),
            'installed cycle process identity',5)
        if case['operation'] in ('live','live-initial'):
            published=wait_for(lambda: messages(prefix) or Path(str(prefix)+'.terminal.json').exists(),
                'new successor activation report',15)
            assert isinstance(published,list) and len(published)==1, 'Serve exited before activation'
            initial,=published
            initialized=snapshot('activated')
            epoch=int(initial['producer_epoch'])
            target,=[r for r in initialized['control']['owner_epochs'] if r['role']=='ingest' and r['epoch']==epoch]
            deadline=int(initial['initial_lease_expires_at_us' if case['operation']=='live-initial' else 'lease_expires_at_us'])
            minimum=max(deadline,int(initial['created_at_us' if case['operation']=='live-initial' else 'prepared_at_us'])+6_000_000)
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
        wait_for(Path(str(prefix)+'.terminal.json').exists,'installed process joined',12 if case['operation'] in ('live','live-initial') else 45)
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
    case.update(status='PASS',returncode=response['returncode'],oracle=result,negative_controls=controls,server_window=window)
    return after,after_native,after_inputs


def run_sequence(pg,client,output,clone,directory,expected,report):
    helper=Path(__file__).with_name('installed_cycle_client.py'); target=output/helper.name
    if not target.exists(): shutil.copy2(helper,target)
    assert history.digest(target)==history.digest(helper)
    configure(pg,client,output,clone['config'])
    seed=expected
    predecessor=None
    for step in STEPS:
        case=dict(name=clone['name']+'-'+step,clone=clone['name'],origin=clone['origin'],
            operation=step.removesuffix('-retry'),predecessor=predecessor,database=clone['database'],
            config=clone['config'],status='RUNNING')
        report['cases'].append(case); lab.write_json(output/'result.json',report)
        expected=execute(pg,client,output,case,directory,expected,clone['catalog_before'],seed)
        case['catalog_after']=history.catalog(pg,clone['database'])
        assert case['catalog_after']==clone['catalog_before']
        predecessor=case['name']; lab.write_json(output/'result.json',report)
    return predecessor


def run(root,build):
    return transition.run(root,build,suite=SUITE,sequence=run_sequence,origin_specs=ORIGINS,
        origin_for_clone={n:n for n in CLONES},extra_grants=GRANTS)


if __name__=='__main__':
    assert len(sys.argv)==3
    run(Path(sys.argv[1]).resolve(strict=True),Path(sys.argv[2]).resolve(strict=True))
