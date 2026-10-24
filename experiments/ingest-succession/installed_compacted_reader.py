#!/usr/bin/env python3
"""Bounded synthetic post-GC initial-reader fixture, not product reclamation.

Four excluded clones first execute actual abandonment and retirement. One raw
test-only synced batch then installs the literal floor and deletes seven exact
receipt keys. Every input and removed byte remains retained as fixture evidence.
The existing transition owner alone owns engines, cloning, custody and cleanup.
The explicit history-fault profile copies a qualified PreparedOpen3 endpoint and
observes read delivery through the existing finite rollback proxy.
"""
import argparse
import json
from datetime import datetime, timezone
from functools import partial
import os
from pathlib import Path
import shutil
import sys
import time
import tomllib

sys.dont_write_bytecode = True
import transition
import installed_cycle
from installed_family import configure
import installed_compacted_reader_oracle as oracle

history, schema, lab = transition.history, transition.schema, transition.lab
SUITE = transition.Suite(directory=oracle.DIRECTORY,label=oracle.LABEL,status=oracle.STATUS,
    client_prefix='bigbrotr-compacted-reader-',database_prefix='bb_compacted_reader_',
    clones=oracle.CLONES,cases=oracle.CASES,
    scope='Synthetic post-GC reader fixture after authentic installed abandonment and retirement: '
          'full residual originals, absent-original exact retry, incompatible typed head/floor digest/missing pending refusals',
    not_run=oracle.NOT_RUN)


def native_mutation(pg,client,output,clone,directory,expected,seed,mode):
    name = clone['name']; label = name+'-synthetic-'+mode
    # This all-session observer connects to postgres, never to the target DB.
    transition.observe_origin_absence(pg,clone['database'],output,label)
    oracle.terminal_checked(expected[0],seed)
    oracle.same(schema.rows(pg,clone['database']),expected[0])
    assert history.inputs_at(directory/'store') == seed[2]
    floor = oracle.floor_bytes(seed,name).hex()
    before = history.dump(client,output,label+'-before',directory)
    assert before == expected[1]
    mutation = dict(kind='privileged-synthetic-post-GC-reader-fixture',mode=mode,clone=name,
        floor=floor,before=before,control=expected[0],inputs=expected[2],after=None,response=None)
    lab.write_json(output/(label+'.json'),mutation)
    response,_ = history.capture(client,output,label,'compacted-reader-fixture',
        '/lab/'+name+'/store',mode,floor)
    mutation['response'] = dict(returncode=response.returncode,stdout=response.stdout,stderr=response.stderr)
    lab.write_json(output/(label+'.json'),mutation)
    # No fresh native open is allowed after a failed/unknown mutator process.
    assert response.returncode == 0 and response.stderr == ''
    wanted = oracle.compacted_native(seed,name,missing=mode == 'missing-pending')
    oracle.mutation_checked(mutation,seed,expected,wanted,complete=False)
    mutation['after'] = history.dump(client,output,label+'-after',directory)
    oracle.same(schema.rows(pg,clone['database']),expected[0])
    assert history.inputs_at(directory/'store') == seed[2]
    lab.write_json(output/(label+'.json'),mutation)
    oracle.mutation_checked(mutation,seed,expected,wanted)
    return expected[0],wanted,expected[2]


def incompatible_head(pg,output,clone,expected,seed):
    label = clone['name']+'-head-mutation'
    transition.observe_origin_absence(pg,clone['database'],output,label)
    before = schema.rows(pg,clone['database']); oracle.same(before,expected[0])
    statement = oracle.retained.ack_statement(seed)
    mutation = dict(kind='privileged-isolated-valid-head-corruption',before=before,
        statement=statement,sql_output=None,after=None)
    lab.write_json(output/(label+'.json'),mutation)
    (output/(label+'.sql')).write_text(statement)
    mutation['sql_output'] = lab.sql(pg,clone['database'],statement)
    mutation['after'] = schema.rows(pg,clone['database'])
    lab.write_json(output/(label+'.json'),mutation)
    after = oracle.head_mutation_checked(mutation,expected[0],seed)
    return after,expected[1],expected[2]


def quarantine(output,directory,expected,seed):
    assert expected[2] == seed[2]
    spool = directory/'store/producer-inputs-v1'; target = directory/'quarantined-originals'
    target.mkdir(mode=0o700)
    before = history.census(spool)
    record = dict(kind='reversible-fixture-original-quarantine',before=before,after=None,
        path=str(target.relative_to(output)),files={},quarantine_after=None,status='INCOMPLETE')
    for name in sorted(seed[2]):
        raw = oracle.retained.regular_read(spool/name)
        record['files'][name] = dict(hex=raw.hex(),metadata=before[name])
    lab.write_json(output/'positive-quarantine.json',record)
    for name in sorted(seed[2]): (spool/name).rename(target/name)
    for path in (spool,target,directory):
        fd = os.open(path,os.O_RDONLY|os.O_DIRECTORY)
        try: os.fsync(fd)
        finally: os.close(fd)
    record.update(after=history.census(spool),quarantine_after=history.census(target),status='RETAINED')
    lab.write_json(output/'positive-quarantine.json',record)
    oracle.quarantine_checked(record,seed)
    assert history.inputs_at(directory/'store') == {}
    return expected[0],expected[1],{}


def run_sequence(pg,client,output,clone,directory,expected,report,*,origin):
    oracle.same(expected[0],origin[0]); assert expected[1:] == origin[1:]
    seed = origin; oracle.known(seed)
    assert clone['name'] in oracle.CLONES and clone['origin'] == 'initial'
    assert tuple(g['statement'] for g in clone['grants']) == oracle.GRANTS
    assert all(g['stdout'] == '' for g in clone['grants'])
    oracle.catalog_checked(report['origins']['initial']['catalog'],clone['catalog_before'])
    helper = Path(__file__).with_name('installed_cycle_client.py'); target = output/helper.name
    if not target.exists(): shutil.copy2(helper,target)
    assert history.digest(helper) == history.digest(target)
    configure(pg,client,output,clone['config'],ack=True)
    predecessor = None
    for step in oracle.STEPS[clone['name']]:
        if step in ('residual','refuse'):
            oracle.terminal_checked(expected[0],seed)
            lab.write_json(output/(clone['name']+'-terminal-seed.json'),dict(control=expected[0],native=expected[1],inputs=expected[2]))
            expected = native_mutation(pg,client,output,clone,directory,expected,seed,'install')
            if clone['name'] == 'incompatible-head': expected = incompatible_head(pg,output,clone,expected,seed)
            if clone['name'] == 'missing-pending':
                expected = native_mutation(pg,client,output,clone,directory,expected,seed,'missing-pending')
        elif step == 'absent': expected = quarantine(output,directory,expected,seed)
        case = dict(name=clone['name']+'-'+step,clone=clone['name'],origin='initial',
            operation='abandon-initial' if step == 'abandon' else 'retire-initial',predecessor=predecessor,
            database=clone['database'],config=clone['config'],status='RUNNING')
        report['cases'].append(case); lab.write_json(output/'result.json',report)
        diagnostic = None
        def verify(context,original):
            nonlocal diagnostic
            if step == 'refuse' and clone['name'] == 'floor-digest':
                window = context['server_window']
                bounds = [datetime.fromtimestamp(window[k]/1_000_000,timezone.utc).isoformat() for k in ('before_us','after_us')]
                reply = lab.docker('logs','--timestamps','--since',bounds[0],'--until',bounds[1],pg['postgres'],check=False,timeout=5)
                diagnostic = dict(returncode=reply.returncode,stdout=reply.stdout,stderr=reply.stderr)
                lab.write_json(output/(case['name']+'-diagnostic.json'),diagnostic)
            return oracle.verify(context,original,expected,diagnostic=diagnostic)
        def controls(context,original):
            return oracle.negative_controls(context,original,expected,diagnostic=diagnostic)
        expected = installed_cycle.execute(pg,client,output,case,directory,expected,
            clone['catalog_before'],seed,checks=(verify,controls,oracle.process_closed))
        if step == 'retire': oracle.terminal_checked(expected[0],seed)
        if step == 'absent':
            q = transition.read(output/'positive-quarantine.json')
            assert history.census(output/q['path']) == q['quarantine_after']
            assert history.census(directory/'store/producer-inputs-v1') == q['after']
        case['catalog_after'] = history.catalog(pg,clone['database'])
        assert case['catalog_after'] == clone['catalog_before']
        predecessor = case['name']; lab.write_json(output/'result.json',report)
    return predecessor


def run(root,build,product_source):
    origin = oracle.retained.origin_checked(root.parent/ORIGIN['run']/ORIGIN['subdirectory'],ack_refusals=True)
    oracle.known(origin)
    return transition.run(root,build,suite=SUITE,sequence=partial(run_sequence,origin=origin),
        origin_specs=(ORIGIN,),origin_for_clone={name:'initial' for name in oracle.CLONES},
        extra_grants=oracle.GRANTS,producer_progress=True,producer_ack=True,preparation_hole=True,
        compacted_reader=True,product_source=product_source)



HISTORY_SUITE = transition.Suite(directory=oracle.HistoryProfile.DIRECTORY,label=oracle.HistoryProfile.LABEL,
    status=oracle.HistoryProfile.STATUS,client_prefix='bigbrotr-compacted-history-',database_prefix=oracle.HistoryProfile.DATABASE_PREFIX,
    clones=oracle.HISTORY_CLONES,cases=oracle.HistoryProfile.CASES,
    scope='Synthetic compacted initial ancestor: actual terminal2 and prepared3; full/partial/absent residuals and exact mixed-history refusals',
    not_run=oracle.HISTORY_NOT_RUN)


def history_probe(pg,client,output,case,directory,expected,catalog,seed):
    name=case['name']; before=schema.rows(pg,case['database']); oracle.same(before,expected[0])
    native=history.dump(client,output,name+'-before',directory); inputs=history.inputs_at(directory/'store')
    assert (native,inputs)==expected[1:]
    lab.write_json(output/(name+'-before.json'),before); lab.write_json(output/(name+'-inputs.json'),inputs)
    args=[case['operation'],case['database'],'/run/secrets/application',case['config']]
    if case['operation']!=oracle.HISTORY_SEED_MODE: args.append('262144')
    window=dict(before_us=history.server_clock(pg,case['database']))
    lab.write_json(output/(name+'-clock.json'),window)
    try: history.capture(client,output,name,*args)
    finally:
        path=output/(name+'-response.json')
        if not path.exists(): lab.write_json(path,dict(returncode=None,stdout='',stderr='',outcome='UNKNOWN'))
        response=transition.read(path); window['after_us']=history.server_clock(pg,case['database'])
        lab.write_json(output/(name+'-clock.json'),window)
        closure=oracle.seed_closed if case['operation']==oracle.HISTORY_SEED_MODE else (
            transition.closed_and_joined if case['operation']=='successor-prepare' else history.closed_and_joined)
        _,after,after_native,after_inputs=history.external(pg,client,output,case,directory,response,closure=closure)
    context=dict(case=dict(case),before=before,native_before=native,inputs_before=inputs,after=after,
        native_after=after_native,inputs_after=after_inputs,response=response,server_window=window,
        catalog_before=catalog,catalog_after=history.catalog(pg,case['database']),arguments=args,
        backend_absence=transition.read(output/(name+'-control-backends.json')),backends=transition.read(output/(name+'-backends.json')),
        custody=transition.read(output/(name+'-custody.json')))
    lab.write_json(output/(name+'-verification.json'),context)
    checked=oracle.verify_history_context(context,seed,expected)
    controls=oracle.history_controls(context,seed,expected)
    case.update(status='PASS',returncode=response['returncode'],oracle=checked,negative_controls=controls,server_window=window)
    return after,after_native,after_inputs


def history_floor(pg,client,output,clone,directory,expected,seed,*,mode='install'):
    label=clone['name']+'-synthetic-'+mode
    transition.observe_origin_absence(pg,clone['database'],output,label)
    oracle.same(schema.rows(pg,clone['database']),expected[0])
    assert history.inputs_at(directory/'store')==expected[2]
    # This is the complete independent chain result, never the freshly dumped
    # product state. The privileged helper checks it before issuing one batch.
    wanted=oracle.history_native_compacted(expected,seed,missing=mode=='missing-later-pending')
    expected_path=output/(clone['name']+'-'+mode+'-expected-native.json')
    lab.write_json(expected_path,expected[1])
    before=history.dump(client,output,label+'-before',directory); assert before==expected[1]
    record=dict(kind='privileged-synthetic-post-GC-history-fixture',mode=mode,clone=clone['name'],
        floor=oracle.floor_bytes(seed,'positive').hex(),before=expected[1],control=expected[0],inputs=expected[2],after=None,response=None)
    lab.write_json(output/(label+'.json'),record)
    reply,_=history.capture(client,output,label,'compacted-history-fixture','/lab/'+clone['name']+'/store',mode,
        record['floor'],'/lab/'+expected_path.name)
    record['response']=dict(returncode=reply.returncode,stdout=reply.stdout,stderr=reply.stderr)
    lab.write_json(output/(label+'.json'),record)
    assert reply.returncode==0 and reply.stderr==''
    # Check the helper's complete closing telemetry before another native open.
    provisional=dict(record,after=wanted); oracle.history_mutation_checked(provisional,expected,seed)
    record['after']=history.dump(client,output,label+'-after',directory)
    oracle.same(schema.rows(pg,clone['database']),expected[0]); assert history.inputs_at(directory/'store')==expected[2]
    lab.write_json(output/(label+'.json'),record)
    return oracle.history_mutation_checked(record,expected,seed)


def history_quarantine(output,directory,expected,seed,step):
    spool=directory/'store/producer-inputs-v1'; target=directory/('quarantined-'+step)
    target.mkdir(mode=0o700); before=history.census(spool)
    initial=sorted(n for n in expected[2] if n in seed[2])
    names=initial[:3] if step=='partial' else initial if step=='absent' else (
        [n for n in expected[2] if n not in seed[2]] if step=='missing-required' else initial[:1])
    record=dict(kind='reversible-history-input-quarantine',status='INCOMPLETE',path=str(target.relative_to(output)),
        before=before,after=None,quarantine_after=None,files={},replacement=None)
    for name in names:
        raw=oracle.retained.regular_read(spool/name)
        record['files'][name]=dict(hex=raw.hex(),metadata=before[name])
    path=output/(directory.name+'-'+step+'-quarantine.json'); lab.write_json(path,record)
    for name in names: (spool/name).rename(target/name)
    if step=='corrupt-residual':
        name,=names; raw=bytes.fromhex(record['files'][name]['hex']); changed=raw[:-1]+bytes([raw[-1]^1])
        fd=os.open(spool/name,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600)
        try:
            os.fchmod(fd,0o600)
            with os.fdopen(fd,'wb',closefd=False) as stream: stream.write(changed); stream.flush(); os.fsync(fd)
        finally: os.close(fd)
        record['replacement']=dict(name=name,hex=changed.hex())
    for d in (spool,target,directory):
        fd=os.open(d,os.O_RDONLY|os.O_DIRECTORY)
        try: os.fsync(fd)
        finally: os.close(fd)
    record.update(status='RETAINED',after=history.census(spool),quarantine_after=history.census(target)); lab.write_json(path,record)
    result=oracle.history_quarantine_checked(record,expected,seed,directory.name,step)
    assert history.inputs_at(directory/'store')==result[2]
    return result


def history_head(pg,output,clone,expected,seed):
    label=clone['name']+'-head-mutation'; transition.observe_origin_absence(pg,clone['database'],output,label)
    before=schema.rows(pg,clone['database']); oracle.same(before,expected[0])
    statement=oracle.retained.ack_statement(seed)
    record=dict(kind='privileged-isolated-valid-head-corruption',before=before,statement=statement,sql_output=None,after=None)
    lab.write_json(output/(label+'.json'),record); (output/(label+'.sql')).write_text(statement)
    record['sql_output']=lab.sql(pg,clone['database'],statement); record['after']=schema.rows(pg,clone['database'])
    lab.write_json(output/(label+'.json'),record)
    return oracle.history_head_checked(record,expected,seed)


def run_history_sequence(pg,client,output,clone,directory,expected,report,*,origin):
    seed=origin; oracle.known(seed); oracle.same(expected[0],seed[0]); assert expected[1:]==seed[1:]
    assert clone['name'] in oracle.HISTORY_CLONES and clone['origin']=='initial'
    assert clone['grants']==[dict(statement=s,stdout='') for s in oracle.HISTORY_GRANTS]
    oracle.history_catalog_checked(report['origins']['initial']['catalog'],clone['catalog_before'])
    helper=Path(__file__).with_name('installed_cycle_client.py'); target=output/helper.name
    if not target.exists(): shutil.copy2(helper,target)
    assert history.digest(helper)==history.digest(target)
    configure(pg,client,output,clone['config'],ack=True)
    predecessor=None
    for step in oracle.HISTORY_STEPS[clone['name']]:
        if step in ('full','refuse'):
            oracle.history_lineage(expected,seed,compacted=False)
            lab.write_json(output/(clone['name']+'-uncompacted-seed.json'),dict(control=expected[0],native=expected[1],inputs=expected[2]))
            expected=history_floor(pg,client,output,clone,directory,expected,seed)
            if clone['name']=='history-incompatible-head': expected=history_head(pg,output,clone,expected,seed)
            elif clone['name']=='history-missing-pending':
                expected=history_floor(pg,client,output,clone,directory,expected,seed,mode='missing-later-pending')
            elif clone['name'] in ('history-missing-required','history-corrupt-residual'):
                expected=history_quarantine(output,directory,expected,seed,clone['name'].removeprefix('history-'))
        elif step in ('partial','absent'): expected=history_quarantine(output,directory,expected,seed,step)
        operation={'abandon':'abandon-initial','retire':'retire-initial','dictionary2':oracle.HISTORY_SEED_MODE,
            'source2':'source-activated','retire2':'retire-activated','prepare3':'successor-prepare'}.get(step,
            'successor-lineage-history' if clone['name']=='history-strict' else 'successor-lineage-compacted-history')
        case=dict(name=clone['name']+'-'+step,clone=clone['name'],origin='initial',operation=operation,
            predecessor=predecessor,database=clone['database'],config=clone['config'],status='RUNNING')
        report['cases'].append(case); lab.write_json(output/'result.json',report)
        if step in ('abandon','retire','source2','retire2'):
            expected=installed_cycle.execute(pg,client,output,case,directory,expected,clone['catalog_before'],seed,
                checks=(lambda c,s:oracle.verify_history_installed(c,s,expected),
                    lambda c,s:oracle.history_controls(c,s,expected,installed=True),oracle.history_installed_closed))
            if step=='retire': oracle.terminal_checked(expected[0],seed)
        else: expected=history_probe(pg,client,output,case,directory,expected,clone['catalog_before'],seed)
        if step=='retire2':
            oracle.history_lineage(expected,seed,compacted=False)
            lab.write_json(output/(clone['name']+'-terminal2.json'),dict(control=expected[0],native=expected[1],inputs=expected[2]))
        case['catalog_after']=history.catalog(pg,clone['database']); assert case['catalog_after']==clone['catalog_before']
        predecessor=case['name']; lab.write_json(output/'result.json',report)
    return predecessor


def run_history(root,build,product_source):
    origin=oracle.retained.origin_checked(root.parent/ORIGIN['run']/ORIGIN['subdirectory'],ack_refusals=True)
    oracle.known(origin)
    return transition.run(root,build,suite=HISTORY_SUITE,sequence=partial(run_history_sequence,origin=origin),
        origin_specs=(ORIGIN,),origin_for_clone={name:'initial' for name in oracle.HISTORY_CLONES},
        extra_grants=oracle.HISTORY_GRANTS,producer_progress=True,producer_ack=True,preparation_hole=True,
        compacted_reader=True,compacted_history=True,product_source=product_source)


FAULT_SUITE = transition.Suite(directory=oracle.HistoryFaultProfile.DIRECTORY,label=oracle.HistoryFaultProfile.LABEL,
    status=oracle.HistoryFaultProfile.STATUS,client_prefix='bigbrotr-compacted-fault-',database_prefix=oracle.HistoryFaultProfile.DATABASE_PREFIX,
    clones=oracle.FAULT_CLONES,cases=oracle.HistoryFaultProfile.CASES,
    scope='Exact-source compacted PreparedOpen3 copies; observed rollback baseline, between-transaction head changes, delivery loss, cancellation and real lease expiry',
    not_run=oracle.HistoryFaultProfile.NOT_RUN)


def fault_wire_next(process,wanted,seconds=5,*,decode=oracle.fault_wire_event):
    # Process.event intentionally accepts only its original base schema. Keep
    # the existing bounded transport reader and validate the rollback extension.
    deadline=time.monotonic()+seconds
    with process.condition:
        while True:
            assert not process.overflow
            end=process.stdout.find(b'\n',process.offset)
            if end>=0:
                value=decode(json.loads(bytes(process.stdout[process.offset:end])))
                process.offset=end+1; assert value['event']==wanted
                return value
            assert time.monotonic()<deadline and (process.process.poll() is None or process.threads[0].is_alive())
            process.condition.wait(min(.05,max(0,deadline-time.monotonic())))


def fault_backend_observation(pg,database,probe):
    probe.observe(); backend,=[v for v in probe.parsed if v['event']=='lineage-history-backend' and v['present']]
    assert backend['database']==database and type(backend['pid']) is int and backend['pid']>0
    assert oracle.re.fullmatch(r'[0-9T:.+ Z-]+',backend['backend_start'])
    statement="SELECT json_build_object('observed_at_us',(EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint," \
        "'pid',pid,'backend_start',backend_start::text,'database',datname,'user',usename,'state',state," \
        "'xact_start',xact_start::text,'query',query) FROM pg_catalog.pg_stat_activity WHERE pid="+str(backend['pid'])+ \
        " AND backend_start='"+backend['backend_start']+"'::timestamptz AND datname=current_database();"
    return json.loads(lab.sql(pg,database,statement))


def fault_invoke(pg,client,output,case,expected,initial,baseline):
    from fault_cases import Process
    from initial_empty_locks import Probe
    from activated_retirement_expiry import wait_event
    from retirement_wait import open_gate
    name,mode=oracle.fault_specification(case); label=case['name']; database=case['database']
    prefix='/lab/'+label+'-gate'; target=2 if name in ('rollback-before','rollback-after','cancel','expiry') else 0
    if target:
        assert baseline is not None and baseline['wire_stages']['compacted-returned']['rollbacks']==[target]*4
        assert baseline['wire_stages']['before-compacted']['rollbacks']==[target-1]*4
    fault=dict(mode=mode,prefix=prefix,target=target,before_forward=name=='rollback-before',wire={},gates=[],held=None,errors=[])
    proxy=probe=None; started=time.monotonic(); window=dict(before_us=history.server_clock(pg,database))
    response=dict(returncode=None,stdout='',stderr='',seconds=None,outcome='UNKNOWN',readers_joined=False,overflow=False,command=None)
    args=None
    def save():
        lab.write_json(output/(label+'-fault.json'),fault)
        lab.write_json(output/(label+'-clock.json'),window)
    try:
        command=['docker','exec','-i',client,'python3','-B','/lab/rollback_proxy.py','--upstream-port','5432','--listen-port','0','--rollback-number',str(target)]
        if fault['before_forward']: command.append('--before-forward')
        proxy=Process(command,input_pipe=True)
        ready=fault_wire_next(proxy,'ready')
        fault['wire']=dict(status='RUNNING',sources={n:history.digest(output/n) for n in ('fault_proxy.py','rollback_proxy.py')},
            ready=ready,receipt=None,events=[],stdout='',stderr='',exit_code=None,readers_joined=False,overflow=False,forced_wrapper_stop=False,command=command)
        assert ready['proxy_sha256']==fault['wire']['sources']['fault_proxy.py']
        args=[oracle.FAULT_MODE,database,'/run/secrets/application',case['config'],'262144',str(ready['listen_port']),mode,prefix]
        response['command']=['docker','exec',client,'/build/ingest_bootstrap_probe',*args]
        save(); probe=Probe(response['command'])
        stages=oracle.FAULT_STAGES if mode=='baseline' else (name.removeprefix('head-'),) if name.startswith('head-') else ('before-compacted',) if mode in ('cancel','expiry') else ()
        for stage in stages:
            event=wait_event(probe,'lineage-history-fault-gate',stage=stage,phase='ready')
            proxy.send('status'); status=fault_wire_next(proxy,'status')
            record=dict(ready=event,status=status,backend=fault_backend_observation(pg,database,probe),control=schema.rows(pg,database),
                head=None,go_before_us=None,go_after_us=None,released=None)
            fault['gates'].append(record); save()
            if name.startswith('head-'):
                # Exact privileged mutation between product transactions. It
                # cannot manufacture a native/terminal prerequisite.
                statement=oracle.retained.ack_statement(initial)
                mutation=dict(kind='privileged-isolated-valid-head-corruption',before=record['control'],statement=statement,sql_output=None,after=None)
                record['head']=mutation; save(); (output/(label+'-head.sql')).write_text(statement)
                mutation['sql_output']=lab.sql(pg,database,statement); mutation['after']=schema.rows(pg,database)
                oracle.history_head_checked(mutation,(record['control'],expected[1],expected[2]),initial); save()
            path=output/Path(event['path']).name
            assert event['path']==prefix+('.'+stage+'.go' if mode=='baseline' else '.go')
            record['go_before_us']=history.server_clock(pg,database); open_gate(path)
            record['go_after_us']=history.server_clock(pg,database)
            record['released']=wait_event(probe,'lineage-history-fault-gate',stage=stage,phase='released'); save()
        if target:
            held=fault_wire_next(proxy,'held',25)
            record=dict(wire=held,backend=fault_backend_observation(pg,database,probe),control=schema.rows(pg,database),
                action_before_us=None,action_after_us=None,cancel=None,expiry=[])
            fault['held']=record; save()
            if name=='cancel':
                open_gate(output/(label+'-gate.cancel'))
                record['cancel']=wait_event(probe,'lineage-history-fault-cancel',phase='applied',timeout=1); save()
            elif name=='expiry':
                control=oracle.only(record['control']['owner_epochs'],role='control',state='active')
                deadline=time.monotonic()+3
                while True:
                    now=history.server_clock(pg,database)
                    record['expiry'].append(dict(server_us=now,remaining_us=control['lease_expires_at_us']-now)); save()
                    if now>=control['lease_expires_at_us']+50_000: break
                    assert time.monotonic()<deadline, 'real UTC expiry was not observed'
                    time.sleep(.02)
            record['action_before_us']=history.server_clock(pg,database)
            proxy.send('close' if name.startswith('rollback-') else 'release')
            record['action_after_us']=history.server_clock(pg,database)
            if not name.startswith('rollback-'): fault_wire_next(proxy,'released')
            save()
        code=probe.wait(15); probe.observe()
        response.update(returncode=code,outcome='PROCESS_EXITED',readers_joined=not any(t.is_alive() for t in probe.threads),overflow=probe.overflow)
        fault['wire']['receipt']=fault_wire_next(proxy,'receipt')
        fault['wire']['exit_code']=proxy.wait(5)
    except BaseException as error:
        fault['errors'].append(dict(stage='invoke',error=repr(error)))
    finally:
        # A stopped local wrapper does not certify remote native custody. Keep
        # original partial bytes; the outer owner still excludes its client.
        if probe is not None:
            code=probe.process.poll()
            if code is not None:
                for thread in probe.threads: thread.join(1)
                response.update(returncode=code,outcome='PROCESS_EXITED',readers_joined=not any(t.is_alive() for t in probe.threads),overflow=probe.overflow)
            response.update(stdout=bytes(probe.stdout).decode(errors='replace'),stderr=bytes(probe.stderr).decode(errors='replace'))
            if code is None: fault['errors'].append(dict(stage='probe-stop',error='forced-wrapper-stop'))
            probe.stop()
        if proxy is not None:
            forced=proxy.process.poll() is None
            proxy.stop(); wire=fault['wire']
            wire.update(stdout=bytes(proxy.stdout).decode(errors='replace'),stderr=bytes(proxy.stderr).decode(errors='replace'),
                exit_code=proxy.process.returncode,forced_wrapper_stop=forced,readers_joined=not any(t.is_alive() for t in proxy.threads),overflow=proxy.overflow)
            try:
                wire['events']=[oracle.fault_wire_event(json.loads(v)) for v in wire['stdout'].splitlines()]
                if wire['events'] and wire['events'][-1]['event']=='receipt': wire['receipt']=wire['events'][-1]
                wire['status']='CAPTURED' if wire['exit_code']==0 and not forced and wire['readers_joined'] and not wire['stderr'] and not wire['overflow'] else 'INCOMPLETE'
            except BaseException as error:
                wire['status']='INCOMPLETE'; fault['errors'].append(dict(stage='proxy-metadata',error=repr(error)))
        response['seconds']=time.monotonic()-started; window['after_us']=history.server_clock(pg,database)
        lab.write_json(output/(label+'-response.json'),response); save()
    return response,window,fault,args


def run_fault_sequence(pg,client,output,clone,directory,expected,report,*,endpoint,initial):
    assert clone['origin']=='compacted' and expected==endpoint
    assert clone['grants']==[dict(statement=s,stdout='') for s in oracle.HISTORY_GRANTS]
    assert clone['catalog_before']==report['origins']['compacted']['catalog']
    configure(pg,client,output,clone['config'],ack=True)
    for source,target in ((history.SOURCE/'experiments/control-migrations/fault_proxy.py','fault_proxy.py'),
            (history.SOURCE/'experiments/relay-source/rollback_proxy.py','rollback_proxy.py'),
            (Path(__file__).with_name('installed_cycle_client.py'),'installed_cycle_client.py')):
        path=output/target
        if not path.exists(): shutil.copy2(source,path)
        assert history.digest(source)==history.digest(path)
    name=clone['name']; label=name+'-read'
    baseline=None if name=='baseline' else oracle.only(report['cases'],name='baseline-read')['oracle']
    case=dict(name=label,clone=name,origin='compacted',operation=oracle.FAULT_MODE,predecessor=None,database=clone['database'],config=clone['config'],status='RUNNING')
    report['cases'].append(case); lab.write_json(output/'result.json',report)
    before=schema.rows(pg,case['database']); oracle.same(before,expected[0])
    native=history.dump(client,output,label+'-before',directory); inputs=history.inputs_at(directory/'store')
    assert (native,inputs)==expected[1:]
    lab.write_json(output/(label+'-before.json'),before); lab.write_json(output/(label+'-inputs.json'),inputs)
    response,window,fault,args=fault_invoke(pg,client,output,case,expected,initial,baseline)
    _,after,after_native,after_inputs=history.external(pg,client,output,case,directory,response,
        closure=lambda r,d:oracle.fault_closed(r,d,fault['wire']))
    context=dict(case=dict(case),before=before,native_before=native,inputs_before=inputs,after=after,native_after=after_native,inputs_after=after_inputs,
        response=response,server_window=window,catalog_before=clone['catalog_before'],catalog_after=history.catalog(pg,case['database']),arguments=args,fault=fault,
        backend_absence=transition.read(output/(label+'-control-backends.json')),backends=transition.read(output/(label+'-backends.json')),custody=transition.read(output/(label+'-custody.json')))
    lab.write_json(output/(label+'-verification.json'),context)
    result=oracle.verify_history_fault(context,initial,expected,baseline)
    controls=oracle.history_fault_controls(context,initial,expected,baseline)
    case.update(status='PASS',returncode=response['returncode'],server_window=window,oracle=result,negative_controls=controls,catalog_after=context['catalog_after'])
    lab.write_json(output/'result.json',report)
    return label


def run_history_faults(root,build,product_source):
    endpoint,initial=oracle.fault_origin_checked(root)
    try:
        return transition.run(root,build,suite=FAULT_SUITE,sequence=partial(run_fault_sequence,endpoint=endpoint,initial=initial),
            origin_specs=(oracle.FAULT_ORIGIN,),origin_for_clone={n:'compacted' for n in oracle.FAULT_CLONES},extra_grants=oracle.HISTORY_GRANTS,
            producer_progress=True,producer_ack=True,preparation_hole=True,compacted_reader=True,compacted_history=True,product_source=product_source)
    finally:
        # Recheck the pinned original, old checker/ELF evidence and separately
        # retained initial quarantines after the runtime owner has stopped.
        assert oracle.fault_origin_checked(root)==(endpoint,initial)


JOURNEY_SUITE = transition.Suite(directory=oracle.InitialJourneyProfile.DIRECTORY,label=oracle.InitialJourneyProfile.LABEL,
    status=oracle.InitialJourneyProfile.STATUS,client_prefix='bigbrotr-compacted-initial-',database_prefix=oracle.InitialJourneyProfile.DATABASE_PREFIX,
    clones=oracle.InitialJourneyProfile.CLONES,cases=oracle.InitialJourneyProfile.CASES,
    scope='Qualified synthetic initial floor: prepare/exact retry, confirmed SQL/native gap, continuous activation and first dictionary, installed startup/shutdown and strict refusals',
    not_run=oracle.InitialJourneyProfile.NOT_RUN)


def journey_gate_invoke(pg,client,output,case,directory,*,compacted=True):
    from initial_empty_locks import Probe
    from activated_retirement_expiry import wait_event
    from retirement_wait import open_gate
    assert type(compacted) is bool
    basis='compacted' if compacted else 'original'
    label=case['name']; prefix='/lab/'+label+'-gate'
    args=[case['operation'],case['database'],'/run/secrets/application',case['config'],prefix]
    command=['docker','exec',client,'/build/ingest_bootstrap_probe',*args]
    gate=dict(ready=None,released=None,native=None,control=None,inputs=None,observed_before_us=None,
        observed_after_us=None,go_before_us=None,go_after_us=None,errors=[])
    response=dict(returncode=None,stdout='',stderr='',seconds=None,outcome='UNKNOWN',readers_joined=False,overflow=False,command=command)
    started=time.monotonic(); probe=None
    def save(): lab.write_json(output/(label+'-gate.json'),gate)
    try:
        save(); probe=Probe(command)
        gate['ready']=wait_event(probe,'successor-'+basis+'-gap-gate',phase='ready')
        gate['native']=wait_event(probe,'successor-'+basis+'-gap-native')['native']
        gate['observed_before_us']=history.server_clock(pg,case['database'])
        gate['control']=schema.rows(pg,case['database']); gate['inputs']=history.inputs_at(directory/'store')
        gate['observed_after_us']=history.server_clock(pg,case['database']); save()
        path=output/(label+'-gate.go'); assert gate['ready']['path']==prefix+'.go'
        gate['go_before_us']=history.server_clock(pg,case['database']); open_gate(path)
        gate['go_after_us']=history.server_clock(pg,case['database']); save()
        gate['released']=wait_event(probe,'successor-'+basis+'-gap-gate',phase='released'); save()
        code=probe.wait(15); probe.observe()
        response.update(returncode=code,outcome='PROCESS_EXITED',readers_joined=not any(t.is_alive() for t in probe.threads),overflow=probe.overflow)
    except BaseException as error:
        gate['errors'].append(dict(stage='gap-capture',error=repr(error)))
        raise
    finally:
        if probe is not None:
            probe.stop()
            response.update(stdout=probe.stdout.decode(),stderr=probe.stderr.decode(),readers_joined=not any(t.is_alive() for t in probe.threads),overflow=probe.overflow)
        response['seconds']=time.monotonic()-started; lab.write_json(output/(label+'-response.json'),response); save()
    return args,gate


def journey_probe(pg,client,output,case,directory,expected,catalog,initial,*,preparation=False,delivery=False):
    assert type(preparation) is bool and type(delivery) is bool and (not delivery or preparation)
    label=case['name']; before=schema.rows(pg,case['database']); oracle.same(before,expected[0])
    native=history.dump(client,output,label+'-before',directory); inputs=history.inputs_at(directory/'store')
    assert (native,inputs)==expected[1:]
    lab.write_json(output/(label+'-before.json'),before); lab.write_json(output/(label+'-inputs.json'),inputs)
    args=[case['operation'],case['database'],'/run/secrets/application',case['config']]; gate=None
    window=dict(before_us=history.server_clock(pg,case['database'])); lab.write_json(output/(label+'-clock.json'),window)
    try:
        if case['operation'] in (oracle.JOURNEY_GAP_MODE,oracle.PREPARATION_ORIGINAL_GAP):
            args,gate=journey_gate_invoke(pg,client,output,case,directory,compacted=case['operation']==oracle.JOURNEY_GAP_MODE)
        else: history.capture(client,output,label,*args)
    finally:
        path=output/(label+'-response.json')
        if not path.exists(): lab.write_json(path,dict(returncode=None,stdout='',stderr='',outcome='UNKNOWN'))
        response=transition.read(path); window['after_us']=history.server_clock(pg,case['database'])
        lab.write_json(output/(label+'-clock.json'),window)
        _,after,after_native,after_inputs=history.external(pg,client,output,case,directory,response,closure=oracle.journey_closed)
    context=dict(case=dict(case),before=before,native_before=native,inputs_before=inputs,after=after,native_after=after_native,inputs_after=after_inputs,
        response=response,server_window=window,catalog_before=catalog,catalog_after=history.catalog(pg,case['database']),arguments=args,gate=gate,
        backend_absence=transition.read(output/(label+'-control-backends.json')),backends=transition.read(output/(label+'-backends.json')),
        custody=transition.read(output/(label+'-custody.json')))
    lab.write_json(output/(label+'-verification.json'),context)
    check,control=(oracle.verify_preparation_context,oracle.preparation_controls) if preparation else (oracle.verify_journey_context,oracle.journey_controls)
    options=dict(delivery=delivery) if preparation else {}
    checked=check(context,initial,expected,**options); controls=control(context,initial,expected,**options)
    case.update(status='PASS',returncode=response['returncode'],oracle=checked,negative_controls=controls,server_window=window)
    return after,after_native,after_inputs


def journey_mutation(pg,client,output,clone,directory,expected,initial):
    name=clone['name']; label=name+'-mutation'
    transition.observe_origin_absence(pg,clone['database'],output,label)
    oracle.same(schema.rows(pg,clone['database']),expected[0])
    assert history.dump(client,output,label+'-before',directory)==expected[1] and history.inputs_at(directory/'store')==expected[2]
    record=dict(kind='isolated-compacted-initial-'+name,before=dict(control=expected[0],native=expected[1],inputs=expected[2]),after=None)
    lab.write_json(output/(label+'.json'),record)
    if name=='head':
        statement=oracle.retained.ack_statement(initial)
        record['head']=dict(kind='privileged-isolated-valid-head-corruption',before=expected[0],statement=statement,sql_output=None,after=None)
        (output/(label+'.sql')).write_text(statement); lab.write_json(output/(label+'.json'),record)
        record['head']['sql_output']=lab.sql(pg,clone['database'],statement)
        record['head']['after']=schema.rows(pg,clone['database'])
    elif name=='target-nonzero':
        reply,_=history.capture(client,output,label,'history-corrupt','/lab/'+name+'/store','target-nonzero')
        record['response']=dict(returncode=reply.returncode,stdout=reply.stdout,stderr=reply.stderr)
        lab.write_json(output/(label+'.json'),record)
        assert reply.returncode==0 and reply.stderr==''
        events=[json.loads(v) for v in reply.stdout.splitlines()]; assert len(events)==2 and events[-1]['phase']=='after'
    else:
        assert name=='target-input'
        origin=output.parent.parent/oracle.JOURNEY_ORIGIN['run']/oracle.JOURNEY_ORIGIN['subdirectory']
        q=transition.read(origin/'positive-quarantine.json')
        source_name=oracle.only(initial[0]['producer_epochs'])['producer_slot_id'][2:]+'-0000000000000001-0000000000000001.cbor'
        target_name=source_name.replace('-0000000000000001-','-0000000000000002-',1)
        raw=oracle.retained.regular_read(origin/q['path']/source_name)
        spool=directory/'store/producer-inputs-v1'; before=history.census(spool)
        record['input']=dict(source_name=source_name,target_name=target_name,hex=raw.hex(),before=before,after=None)
        lab.write_json(output/(label+'.json'),record)
        descriptor=os.open(spool/target_name,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600)
        with os.fdopen(descriptor,'wb') as stream: stream.write(raw); stream.flush(); os.fsync(stream.fileno())
        descriptor=os.open(spool,os.O_RDONLY|os.O_DIRECTORY)
        try: os.fsync(descriptor)
        finally: os.close(descriptor)
        record['input']['after']=history.census(spool)
    # No engine open follows an unknown mutation outcome. The fixed helper's
    # successful return closes its native handle before this independent dump.
    record['after']=dict(control=schema.rows(pg,clone['database']),native=history.dump(client,output,label+'-after',directory),inputs=history.inputs_at(directory/'store'))
    lab.write_json(output/(label+'.json'),record)
    return oracle.journey_mutation_checked(record,expected,initial,name)


def run_journey_sequence(pg,client,output,clone,directory,expected,report,*,endpoint,initial):
    assert expected==endpoint and clone['origin']=='compacted-initial'
    oracle.history_catalog_checked(report['origins']['compacted-initial']['catalog'],clone['catalog_before'],grants=oracle.JOURNEY_GRANTS)
    assert clone['grants']==[dict(statement=s,stdout='') for s in oracle.JOURNEY_GRANTS]
    # Fresh copy, all sessions excluded, before any product/fixture mutation.
    transition.observe_origin_absence(pg,clone['database'],output,clone['name']+'-logical-origin')
    actual=history.dump(client,output,clone['name']+'-logical-origin',directory)
    assert actual==endpoint[1] and (len(actual['rows']),actual['sequence'])==(81,121)
    assert history.inputs_at(directory/'store')==endpoint[2]=={}
    helper=Path(__file__).with_name('installed_cycle_client.py'); target=output/helper.name
    if not target.exists(): shutil.copy2(helper,target)
    assert history.digest(helper)==history.digest(target)
    configure(pg,client,output,clone['config'],ack=True)
    predecessor=None
    for step in oracle.JOURNEY_STEPS[clone['name']]:
        if step=='refuse': expected=journey_mutation(pg,client,output,clone,directory,expected,initial)
        operation={'prepare':oracle.JOURNEY_PREPARE_MODE,'prepare-gap':oracle.JOURNEY_GAP_MODE,'current-refuse':'retire-initial',
            'dictionary':oracle.JOURNEY_LIVE_MODE,'strict':'successor-prepared-recover','serve':'live-initial'}.get(step,oracle.JOURNEY_READ_MODE)
        case=dict(name=clone['name']+'-'+step,clone=clone['name'],origin='compacted-initial',operation=operation,
            predecessor=predecessor,database=clone['database'],config=clone['config'],status='RUNNING')
        report['cases'].append(case); lab.write_json(output/'result.json',report)
        if step in ('current-refuse','serve'):
            expected=installed_cycle.execute(pg,client,output,case,directory,expected,clone['catalog_before'],initial,
                checks=(lambda c,s:oracle.verify_journey_context(c,s,expected),lambda c,s:oracle.journey_controls(c,s,expected),oracle.journey_installed_closed))
        else: expected=journey_probe(pg,client,output,case,directory,expected,clone['catalog_before'],initial)
        case['catalog_after']=history.catalog(pg,clone['database']); assert case['catalog_after']==clone['catalog_before']
        predecessor=case['name']; lab.write_json(output/'result.json',report)
    return predecessor


def run_initial_journey(root,build,product_source):
    endpoint,initial=oracle.journey_origin_checked(root)
    try:
        return transition.run(root,build,suite=JOURNEY_SUITE,sequence=partial(run_journey_sequence,endpoint=endpoint,initial=initial),
            origin_specs=(oracle.JOURNEY_ORIGIN,),origin_for_clone={n:'compacted-initial' for n in oracle.InitialJourneyProfile.CLONES},
            extra_grants=oracle.JOURNEY_GRANTS,producer_progress=True,producer_ack=True,preparation_hole=True,
            compacted_reader=True,compacted_initial=True,product_source=product_source)
    finally:
        assert oracle.journey_origin_checked(root)==(endpoint,initial)


PREPARATION_SUITE = transition.Suite(directory=oracle.InitialPreparationProfile.DIRECTORY,label=oracle.InitialPreparationProfile.LABEL,
    status=oracle.InitialPreparationProfile.STATUS,client_prefix='bigbrotr-initial-preparation-',database_prefix=oracle.InitialPreparationProfile.DATABASE_PREFIX,
    clones=oracle.InitialPreparationProfile.CLONES,cases=oracle.InitialPreparationProfile.CASES,
    scope='Installed completion of existing initial preparations: original and compacted native1 gaps/native2 retries; preserved intent and isolated refusals',
    not_run=oracle.InitialPreparationProfile.NOT_RUN)


def preparation_mutation(pg,client,output,clone,directory,expected,initial):
    name=clone['name']
    if name in ('head','target-nonzero','target-input'):
        return journey_mutation(pg,client,output,clone,directory,expected,initial)
    assert name in ('outcome','target-closing')
    label=name+'-mutation'; transition.observe_origin_absence(pg,clone['database'],output,label)
    oracle.same(schema.rows(pg,clone['database']),expected[0])
    assert history.dump(client,output,label+'-before',directory)==expected[1] and history.inputs_at(directory/'store')==expected[2]
    record=dict(kind='isolated-initial-preparation-'+name,before=dict(control=expected[0],native=expected[1],inputs=expected[2]),after=None)
    lab.write_json(output/(label+'.json'),record)
    if name=='outcome':
        record.update(statement=oracle.preparation_outcome_statement(initial),sql_output=None)
        (output/(label+'.sql')).write_text(record['statement']); lab.write_json(output/(label+'.json'),record)
        record['sql_output']=lab.sql(pg,clone['database'],record['statement'])
    else:
        reply,_=history.capture(client,output,label,'history-corrupt','/lab/'+name+'/store','target-closing')
        record['response']=dict(returncode=reply.returncode,stdout=reply.stdout,stderr=reply.stderr)
        lab.write_json(output/(label+'.json'),record)
        assert reply.returncode==0 and reply.stderr==''
        events=[json.loads(v) for v in reply.stdout.splitlines()]; assert len(events)==2 and events[-1]['phase']=='after'
    record['after']=dict(control=schema.rows(pg,clone['database']),native=history.dump(client,output,label+'-after',directory),inputs=history.inputs_at(directory/'store'))
    lab.write_json(output/(label+'.json'),record)
    return oracle.preparation_mutation_checked(record,expected,initial,name)


def preparation_quarantine(output,directory,expected,initial):
    assert expected[2]==initial[2]
    name=oracle.only(initial[0]['producer_epochs'])['producer_slot_id'][2:]+'-0000000000000001-0000000000000003.cbor'
    spool=directory/'store/producer-inputs-v1'; target=directory/'quarantined-required'; target.mkdir(mode=0o700)
    q=dict(kind='reversible-required-initial-original-quarantine',status='INCOMPLETE',path=str(target.relative_to(output)),
        name=name,hex=oracle.retained.regular_read(spool/name).hex(),before=history.census(spool),after=None,quarantine_after=None)
    path=output/(directory.name+'-required-quarantine.json'); lab.write_json(path,q)
    (spool/name).rename(target/name)
    for folder in (spool,target,directory):
        fd=os.open(folder,os.O_RDONLY|os.O_DIRECTORY)
        try: os.fsync(fd)
        finally: os.close(fd)
    q.update(status='RETAINED',after=history.census(spool),quarantine_after=history.census(target)); lab.write_json(path,q)
    result=oracle.preparation_quarantine_checked(q,expected,initial)
    assert history.inputs_at(directory/'store')==result[2]
    return result


def run_preparation_sequence(pg,client,output,clone,directory,expected,report,*,origins,initial,delivery=False):
    assert type(delivery) is bool
    names=oracle.DELIVERY_ORIGIN_NAMES if delivery else oracle.PREPARATION_ORIGIN_NAMES
    steps=oracle.DELIVERY_STEPS if delivery else oracle.PREPARATION_STEPS
    name=clone['name']; origin_name=names[name]
    assert clone['origin']==origin_name and expected==origins[origin_name]
    assert clone['grants']==[dict(statement=s,stdout='') for s in oracle.JOURNEY_GRANTS]
    oracle.history_catalog_checked(report['origins'][origin_name]['catalog'],clone['catalog_before'],grants=oracle.JOURNEY_GRANTS)
    # Before any setup, bind the entire excluded clone to the qualified logical
    # endpoint. Native originals are never opened by the fixture owner.
    transition.observe_origin_absence(pg,clone['database'],output,name+'-logical-origin')
    assert history.dump(client,output,name+'-logical-origin',directory)==expected[1]
    assert history.inputs_at(directory/'store')==expected[2]
    helper=Path(__file__).with_name('installed_cycle_client.py'); target=output/helper.name
    if not target.exists(): shutil.copy2(helper,target)
    assert history.digest(helper)==history.digest(target)
    configure(pg,client,output,clone['config'],ack=True)
    predecessor=None
    if delivery:
        for filename,relative in oracle.DELIVERY_SOURCES.items():
            target=output/filename; source=history.SOURCE/relative
            if not target.exists(): shutil.copy2(source,target)
            assert history.digest(target)==history.digest(source)
    for step in steps[name]:
        if step=='refuse' and name!='activated-zero': expected=preparation_mutation(pg,client,output,clone,directory,expected,initial)
        elif step=='missing-original': expected=preparation_quarantine(output,directory,expected,initial)
        if step in ('prepare','prepare-gap'):
            basis='original' if name.startswith('original-') else 'compacted'
            operation='successor-'+basis+('-normal' if step=='prepare' else '-gap-only')
        else: operation={'abandon':'abandon-initial','retire':'retire-initial'}.get(step,'complete-initial')
        case=dict(name=name+'-'+step,clone=name,origin=origin_name,operation=operation,predecessor=predecessor,
            database=clone['database'],config=clone['config'],status='RUNNING')
        report['cases'].append(case); lab.write_json(output/'result.json',report)
        if step in ('prepare','prepare-gap'):
            expected=journey_probe(pg,client,output,case,directory,expected,clone['catalog_before'],initial,preparation=True,delivery=delivery)
        else:
            baseline=None
            if delivery and step=='loss':
                route='gap' if name.startswith('gap-') else 'prepared'
                baseline=oracle.only(report['cases'],name=route+'-baseline-baseline')['oracle']
            fault=preparation_delivery_hook(pg,client,output,case,directory) if delivery and step in ('baseline','loss') else None
            expected=installed_cycle.execute(pg,client,output,case,directory,expected,clone['catalog_before'],initial,
                checks=(lambda c,s:oracle.verify_preparation_context(c,s,expected,delivery=delivery,baseline=baseline),
                    lambda c,s:oracle.preparation_controls(c,s,expected,delivery=delivery,baseline=baseline),
                    partial(oracle.preparation_closed,delivery=delivery)),fault=fault)
        case['catalog_after']=history.catalog(pg,clone['database']); assert case['catalog_after']==clone['catalog_before']
        predecessor=case['name']; lab.write_json(output/'result.json',report)
    return predecessor


def run_initial_preparation(root,build,product_source):
    oracle.preparation_qualification(transition.read(build/'component-result.json'))
    origins,initial=oracle.preparation_origins_checked(root)
    try:
        return transition.run(root,build,suite=PREPARATION_SUITE,sequence=partial(run_preparation_sequence,origins=origins,initial=initial),
            origin_specs=oracle.PREPARATION_ORIGINS,origin_for_clone=oracle.PREPARATION_ORIGIN_NAMES,
            extra_grants=oracle.JOURNEY_GRANTS,producer_progress=True,producer_ack=True,preparation_hole=True,
            compacted_reader=True,compacted_initial=True,initial_preparation=True,product_source=product_source)
    finally:
        assert oracle.preparation_origins_checked(root)==(origins,initial)


PREPARATION_DELIVERY_SUITE = transition.Suite(directory=oracle.InitialPreparationDeliveryProfile.DIRECTORY,
    label=oracle.InitialPreparationDeliveryProfile.LABEL,status=oracle.InitialPreparationDeliveryProfile.STATUS,
    client_prefix='bigbrotr-initial-delivery-',database_prefix=oracle.InitialPreparationDeliveryProfile.DATABASE_PREFIX,
    clones=oracle.InitialPreparationDeliveryProfile.CLONES,cases=oracle.InitialPreparationDeliveryProfile.CASES,
    scope='Installed compacted initial completion: measured positive wire baselines, predispatch START loss, postdispatch rollback delivery loss and fresh retries',
    not_run=oracle.InitialPreparationDeliveryProfile.NOT_RUN)


def preparation_delivery_hook(pg,client,output,case,directory):
    from fault_cases import Process
    name,step=oracle.preparation_specification(case,delivery=True)
    assert step in ('baseline','loss')
    boundary='observe' if step=='baseline' else name.rsplit('-',1)[1]
    start,rollback=(5,3) if name.startswith('gap-') else (4,2)
    label=case['name']; config=output/Path(case['config']).name
    record=dict(status='INCOMPLETE',boundary=boundary,start_number=start,rollback_number=rollback,wire=None,held=None,
        configuration=dict(path=case['config'],direct=None,proxied=None,restored=None),errors=[])
    proxy=None
    def save(): lab.write_json(output/(label+'-delivery.json'),record)
    def next_event(kind): return fault_wire_next(proxy,kind,decode=oracle.preparation_wire_event)
    def callback(phase):
        nonlocal proxy
        try:
            if phase=='before-launch':
                command=['docker','exec','-i',client,'python3','-B','/lab/initial_gap_proxy.py','--upstream-port','5432',
                    '--listen-port','0','--boundary',boundary,'--start-number',str(start),'--rollback-number',str(rollback)]
                record['wire']=dict(command=command,sources={n:history.digest(output/n) for n in oracle.DELIVERY_SOURCES},ready=None,
                    held=None,receipt=None,events=[],stdout='',stderr='',exit_code=None,readers_joined=False,overflow=False,forced_wrapper_stop=False)
                save(); proxy=Process(command,input_pipe=True); ready=next_event('ready'); record['wire']['ready']=ready
                assert (ready['proxy_sha256'],ready['gap_proxy_sha256'],ready['rollback_proxy_sha256'])==tuple(
                    record['wire']['sources'][n] for n in ('fault_proxy.py','initial_gap_proxy.py','rollback_proxy.py'))
                direct=config.read_text(); proxied=history.set_value(direct,'postgres.control','endpoint','127.0.0.1:'+str(ready['listen_port']))
                record['configuration'].update(direct=direct,proxied=proxied); save()
                (output/(label+'-direct.toml')).write_text(direct); (output/(label+'-proxied.toml')).write_text(proxied)
                config.write_text(proxied)
            elif phase=='launched':
                if step=='loss':
                    record['wire']['held']=next_event('held'); save()
                    capture=dict(before_us=history.server_clock(pg,case['database']),control=schema.rows(pg,case['database']),
                        inputs=history.inputs_at(directory/'store'),backend=None,after_us=None,close_before_us=None,close_after_us=None)
                    record['held']=capture; save()
                    control=max((r for r in capture['control']['owner_epochs'] if r['role']=='control'),key=lambda r:r['epoch'])
                    pid=control['control_backend_pid']; assert type(pid) is int and pid>0
                    statement="SELECT json_build_object('observed_at_us',(EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint," \
                        "'pid',pid,'backend_start',backend_start,'database',datname,'user',usename,'state',state," \
                        "'xact_start',xact_start::text,'query',query) FROM pg_catalog.pg_stat_activity WHERE pid="+str(pid)+ \
                        " AND datname=current_database() AND usename='bigbrotr_control';"
                    capture['backend']=json.loads(lab.sql(pg,case['database'],statement))
                    capture['after_us']=history.server_clock(pg,case['database']); save()
                    capture['close_before_us']=history.server_clock(pg,case['database']); proxy.send('close')
                    capture['close_after_us']=history.server_clock(pg,case['database']); save()
            else:
                assert phase=='child-joined'
                try:
                    assert proxy is not None
                    record['wire']['receipt']=next_event('receipt')
                    assert proxy.wait(5)==0
                finally:
                    try:
                        if proxy is not None:
                            forced=proxy.process.poll() is None; proxy.stop()
                            wire=record['wire']; wire.update(stdout=bytes(proxy.stdout).decode(errors='replace'),stderr=bytes(proxy.stderr).decode(errors='replace'),
                                exit_code=proxy.process.returncode,readers_joined=not any(t.is_alive() for t in proxy.threads),
                                overflow=proxy.overflow,forced_wrapper_stop=forced)
                            wire['events']=[oracle.preparation_wire_event(json.loads(v)) for v in wire['stdout'].splitlines()]
                    finally:
                        settings=record['configuration']
                        if settings['proxied'] is not None:
                            assert config.read_text()==settings['proxied']
                            config.write_text(settings['direct']); settings['restored']=config.read_text()
                            (output/(label+'-restored.toml')).write_text(settings['restored'])
                        save()
                terminal=transition.read(output/(label+'.terminal.json'))
                assert terminal['reaped'] is True and terminal['forced'] is False and terminal['error'] is None
                assert record['wire']['readers_joined'] and not record['wire']['overflow'] and not record['wire']['forced_wrapper_stop']
                assert record['wire']['stderr']=='' and record['configuration']['restored']==record['configuration']['direct']
                assert record['errors']==[]
                record['status']='JOINED_CONFIG_RESTORED'
            save(); return record
        except BaseException as error:
            record['errors'].append(dict(phase=phase,error=repr(error))); save()
            if phase!='child-joined' and proxy is not None and proxy.process.poll() is None:
                try: proxy.send('close')
                except BaseException: pass
            raise
    return callback


def run_initial_preparation_faults(root,build,product_source):
    oracle.preparation_qualification(transition.read(build/'component-result.json'))
    origins,initial=oracle.preparation_delivery_origins(root)
    try:
        return transition.run(root,build,suite=PREPARATION_DELIVERY_SUITE,
            sequence=partial(run_preparation_sequence,origins=origins,initial=initial,delivery=True),
            origin_specs=oracle.DELIVERY_ORIGINS,origin_for_clone=oracle.DELIVERY_ORIGIN_NAMES,
            extra_grants=oracle.JOURNEY_GRANTS,producer_progress=True,producer_ack=True,preparation_hole=True,
            compacted_reader=True,compacted_initial=True,initial_preparation=True,product_source=product_source)
    finally:
        assert oracle.preparation_delivery_origins(root)==(origins,initial)


ACTIVATED_SUITE = transition.Suite(directory=oracle.ActivatedProfile.DIRECTORY,label=oracle.ActivatedProfile.LABEL,
    status=oracle.ActivatedProfile.STATUS,client_prefix='bigbrotr-compacted-activated-',database_prefix=oracle.ActivatedProfile.DATABASE_PREFIX,
    clones=oracle.ActivatedProfile.CLONES,cases=oracle.ActivatedProfile.CASES,
    scope='Installed compacted activated-initial recovery and ACK, authentic first checkpoint and exact reservation/outcome COMMIT loss; original-input regression',
    not_run=oracle.ActivatedProfile.NOT_RUN)


def activated_seed_invoke(pg,client,output,case,directory,baseline):
    from fault_cases import Process, safe_event
    from initial_empty_locks import Probe
    name,step=oracle.activated_specification(case); assert step=='seed'
    normal=name in ('progress-wire','original')
    target=21 if name=='original' else 6 if name=='reserved-tail' else 7
    mode='hold' if normal else 'before-forward' if name=='outcome-before' else 'after-complete'
    if not normal:
        assert baseline['wire']==dict(outcome_commit=7,reservation_commit=6,commit_counts=[8]*4,boundary='hold-before-forward-released')
    proxy=probe=None; label=case['name']; started=time.monotonic(); args=None
    window=dict(before_us=history.server_clock(pg,case['database']))
    wire=dict(command=None,source_sha256=history.digest(output/'fault_proxy.py'),ready=None,held=None,receipt=None,
        events=[],stdout='',stderr='',exit_code=None,readers_joined=False,overflow=False,forced_wrapper_stop=False,errors=[])
    response=dict(returncode=None,stdout='',stderr='',seconds=None,outcome='UNKNOWN',readers_joined=False,overflow=False,command=None)
    def save():
        lab.write_json(output/(label+'-wire.json'),wire); lab.write_json(output/(label+'-clock.json'),window)
    try:
        command=['docker','exec','-i',client,'python3','-B','/lab/fault_proxy.py','--upstream-port','5432','--listen-port','0',
            '--mode',mode,'--commit-number',str(target),'--later-commits','1','--deadline-seconds','30']
        wire['command']=command; proxy=Process(command,input_pipe=True); ready=proxy.event('ready',5); wire['ready']=ready
        assert ready['proxy_sha256']==wire['source_sha256']
        args=[case['operation'],case['database'],'/run/secrets/application',case['config'],str(ready['listen_port'])]
        response['command']=['docker','exec',client,'/build/ingest_bootstrap_probe',*args]; save()
        probe=Probe(response['command'])
        if normal:
            held=proxy.event('held',15); probe.observe()
            backend,=[v for v in probe.parsed if v['event']=='successor-backend' and v['present']]
            assert backend['database']==case['database'] and type(backend['pid']) is int and backend['pid']>0
            assert oracle.re.fullmatch('[0-9T:.+ Z-]+',backend['backend_start'])
            capture=dict(event=held,before_us=history.server_clock(pg,case['database']),after_us=None,release_before_us=None,release_after_us=None,
                control=schema.rows(pg,case['database']),inputs=history.inputs_at(directory/'store'),backend=None)
            wire['held']=capture; save()
            statement="SELECT json_build_object('observed_at_us',(EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint," \
                "'pid',pid,'backend_start',backend_start,'database',datname,'user',usename,'state',state," \
                "'xact_start',xact_start,'query',query) FROM pg_catalog.pg_stat_activity WHERE pid="+str(backend['pid'])+ \
                " AND backend_start='"+backend['backend_start']+"'::timestamptz AND datname=current_database();"
            capture['backend']=json.loads(lab.sql(pg,case['database'],statement)); capture['after_us']=history.server_clock(pg,case['database'])
            capture['release_before_us']=history.server_clock(pg,case['database']); proxy.send('release')
            capture['release_after_us']=history.server_clock(pg,case['database']); proxy.event('released',5); save()
        code=probe.wait(20); probe.observe()
        response.update(returncode=code,outcome='PROCESS_EXITED',readers_joined=not any(t.is_alive() for t in probe.threads),overflow=probe.overflow)
        wire['receipt']=proxy.event('receipt',5); wire['exit_code']=proxy.wait(5)
    except BaseException as error: wire['errors'].append(dict(stage='invoke',error=repr(error)))
    finally:
        if probe is not None:
            code=probe.process.poll()
            if code is not None:
                for thread in probe.threads: thread.join(1)
                response.update(returncode=code,outcome='PROCESS_EXITED',readers_joined=not any(t.is_alive() for t in probe.threads),overflow=probe.overflow)
            else: wire['errors'].append(dict(stage='probe-stop',error='forced-wrapper-stop'))
            response.update(stdout=bytes(probe.stdout).decode(errors='replace'),stderr=bytes(probe.stderr).decode(errors='replace'))
            probe.stop()
        if proxy is not None:
            forced=proxy.process.poll() is None; proxy.stop()
            wire.update(stdout=bytes(proxy.stdout).decode(errors='replace'),stderr=bytes(proxy.stderr).decode(errors='replace'),
                exit_code=proxy.process.returncode,readers_joined=not any(t.is_alive() for t in proxy.threads),overflow=proxy.overflow,forced_wrapper_stop=forced)
            try: wire['events']=[safe_event(json.loads(v)) for v in wire['stdout'].splitlines()]
            except BaseException as error: wire['errors'].append(dict(stage='proxy-metadata',error=repr(error)))
        response['seconds']=time.monotonic()-started; window['after_us']=history.server_clock(pg,case['database'])
        lab.write_json(output/(label+'-response.json'),response); save()
    return response,window,wire,args


def activated_seed(pg,client,output,case,directory,expected,catalog,initial,baseline):
    label=case['name']; before=schema.rows(pg,case['database']); oracle.same(before,expected[0])
    native=history.dump(client,output,label+'-before',directory); inputs=history.inputs_at(directory/'store')
    assert (native,inputs)==expected[1:]
    lab.write_json(output/(label+'-before.json'),before); lab.write_json(output/(label+'-inputs.json'),inputs)
    response,window,wire,args=activated_seed_invoke(pg,client,output,case,directory,baseline)
    # Proxy closure is part of the external-native exclusion gate, not a later
    # semantic assertion after reopening the store.
    def closed(reply,database):
        assert wire['errors']==[] and wire['exit_code']==0 and wire['readers_joined'] and not wire['overflow']
        assert not wire['forced_wrapper_stop'] and wire['stderr']==''
        return oracle.activated_seed_closed(reply,database)
    _,after,after_native,after_inputs=history.external(pg,client,output,case,directory,response,closure=closed)
    context=dict(case=dict(case),before=before,native_before=native,inputs_before=inputs,after=after,native_after=after_native,inputs_after=after_inputs,
        response=response,server_window=window,catalog_before=catalog,catalog_after=history.catalog(pg,case['database']),arguments=args,wire=wire,
        backend_absence=transition.read(output/(label+'-control-backends.json')),backends=transition.read(output/(label+'-backends.json')),
        custody=transition.read(output/(label+'-custody.json')))
    lab.write_json(output/(label+'-verification.json'),context)
    checked=oracle.verify_activated_context(context,initial,expected,baseline=baseline)
    controls=oracle.activated_controls(context,initial,expected,baseline=baseline)
    case.update(status='PASS',returncode=response['returncode'],oracle=checked,negative_controls=controls,server_window=window)
    return after,after_native,after_inputs


def activated_quarantine(output,directory,expected):
    assert len(expected[2])==1; name=next(iter(expected[2])); spool=directory/'store/producer-inputs-v1'; target=directory/'quarantined-required'
    target.mkdir(mode=0o700); before=history.census(spool); data=oracle.retained.regular_read(spool/name)
    q=dict(kind='reversible-required-target-quarantine',path=str(target.relative_to(output)),name=name,hex=data.hex(),before=before,after=None,quarantine_after=None,status='INCOMPLETE')
    lab.write_json(output/'missing-input-quarantine.json',q); (spool/name).rename(target/name)
    for path in (spool,target,directory):
        fd=os.open(path,os.O_RDONLY|os.O_DIRECTORY)
        try: os.fsync(fd)
        finally: os.close(fd)
    q.update(after=history.census(spool),quarantine_after=history.census(target),status='RETAINED')
    lab.write_json(output/'missing-input-quarantine.json',q)
    return oracle.activated_quarantine_checked(q,expected)


def run_activated_sequence(pg,client,output,clone,directory,expected,report,*,origins,initial):
    name=clone['name']; origin_name=oracle.ACTIVATED_ORIGIN_NAMES[name]
    assert clone['origin']==origin_name and expected==origins[origin_name]
    assert clone['grants']==[dict(statement=s,stdout='') for s in oracle.ACTIVATED_GRANTS]
    oracle.history_catalog_checked(report['origins'][origin_name]['catalog'],clone['catalog_before'],grants=oracle.ACTIVATED_GRANTS)
    transition.observe_origin_absence(pg,clone['database'],output,name+'-logical-origin')
    assert history.dump(client,output,name+'-logical-origin',directory)==expected[1]
    assert history.inputs_at(directory/'store')==expected[2]
    for source,target in [(Path(__file__).with_name('installed_cycle_client.py'),output/'installed_cycle_client.py'),
        (history.SOURCE/oracle.ACTIVATED_PROXY,output/'fault_proxy.py')]:
        if not target.exists(): shutil.copy2(source,target)
        assert history.digest(target)==history.digest(source)
    configure(pg,client,output,clone['config'],ack=True)
    if name=='missing-input': expected=activated_quarantine(output,directory,expected)
    predecessor=None
    for step in oracle.ACTIVATED_STEPS[name]:
        operation=oracle.ACTIVATED_SEED_MODES[name] if step=='seed' else 'abandon-initial' if step=='abandon' else 'retire-initial' if name=='original' and step=='retire' else {'source':'source-activated','retire':'retire-activated'}.get(step,'recover-activated')
        case=dict(name=name+'-'+step,clone=name,origin=origin_name,operation=operation,predecessor=predecessor,database=clone['database'],config=clone['config'],status='RUNNING')
        report['cases'].append(case); lab.write_json(output/'result.json',report)
        baseline=oracle.only(report['cases'],name='progress-wire-seed')['oracle'] if step=='seed' and name not in ('progress-wire','original') else None
        if step=='seed': expected=activated_seed(pg,client,output,case,directory,expected,clone['catalog_before'],initial,baseline)
        else:
            expected=installed_cycle.execute(pg,client,output,case,directory,expected,clone['catalog_before'],initial,
                checks=(lambda c,s:oracle.verify_activated_context(c,s,expected),lambda c,s:oracle.activated_controls(c,s,expected),oracle.activated_closed))
        case['catalog_after']=history.catalog(pg,clone['database']); assert case['catalog_after']==clone['catalog_before']
        predecessor=case['name']; lab.write_json(output/'result.json',report)
    return predecessor


def run_activated(root,build,product_source):
    oracle.activated_qualification(transition.read(build/'component-result.json'))
    origins,initial=oracle.activated_origins_checked(root)
    try:
        return transition.run(root,build,suite=ACTIVATED_SUITE,sequence=partial(run_activated_sequence,origins=origins,initial=initial),
            origin_specs=oracle.ACTIVATED_ORIGINS,origin_for_clone=oracle.ACTIVATED_ORIGIN_NAMES,extra_grants=oracle.ACTIVATED_GRANTS,
            producer_progress=True,producer_ack=True,preparation_hole=True,compacted_reader=True,compacted_initial=True,
            compacted_activated=True,product_source=product_source)
    finally: assert oracle.activated_origins_checked(root)==(origins,initial)


TERMINAL_SUITE = transition.Suite(directory=oracle.ActivatedTerminalProfile.DIRECTORY,label=oracle.ActivatedTerminalProfile.LABEL,
    status=oracle.ActivatedTerminalProfile.STATUS,client_prefix='bigbrotr-compacted-terminal-',database_prefix=oracle.ActivatedTerminalProfile.DATABASE_PREFIX,
    clones=oracle.ActivatedTerminalProfile.CLONES,cases=oracle.ActivatedTerminalProfile.CASES,
    scope='Installed compacted activated zero/Event source closure, actual activating-Control acquisition gap and exact12-row retirement with retries; bounded prerequisite refusals only',
    not_run=oracle.ActivatedTerminalProfile.NOT_RUN)


def terminal_attribution(output,directory,initial):
    origin=output.parent.parent/oracle.JOURNEY_ORIGIN['run']/oracle.JOURNEY_ORIGIN['subdirectory']
    q=transition.read(origin/'positive-quarantine.json'); target=directory/'terminal-seed-inputs'; target.mkdir(mode=0o700)
    slot=oracle.only(initial[0]['producer_epochs'])['producer_slot_id'][2:]; contents={}
    for n in oracle.TERMINAL_ATTRIBUTION:
        name=f'{slot}-0000000000000001-{n:016x}.cbor'; data=oracle.retained.regular_read(origin/q['path']/name)
        contents[name]=data.hex()
        fd=os.open(target/name,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600)
        with os.fdopen(fd,'wb') as stream: stream.write(data); stream.flush(); os.fsync(stream.fileno())
    oracle.terminal_attribution(contents,initial)
    for path in (target,directory):
        fd=os.open(path,os.O_RDONLY|os.O_DIRECTORY)
        try: os.fsync(fd)
        finally: os.close(fd)
    record=dict(kind='retained-attribution-originals-outside-spool',path=str(target.relative_to(output)),contents=contents,physical=history.census(target))
    lab.write_json(output/(directory.name+'-attribution.json'),record)
    return contents


def terminal_seed(pg,client,output,case,directory,expected,catalog,initial,attribution):
    label=case['name']; before=schema.rows(pg,case['database']); oracle.same(before,expected[0])
    native=history.dump(client,output,label+'-before',directory); inputs=history.inputs_at(directory/'store'); assert (native,inputs)==expected[1:]
    lab.write_json(output/(label+'-before.json'),before); lab.write_json(output/(label+'-inputs.json'),inputs)
    resolution=lab.docker('exec',client,'python3','-c',
        "import json,time; n=round(time.clock_getres(time.CLOCK_REALTIME)*1000000000); print(json.dumps(dict(clock='CLOCK_REALTIME',resolution_ns=n,resolution_us=(n+999)//1000)))",timeout=5)
    lab.write_json(output/(label+'-receive-clock.json'),dict(returncode=resolution.returncode,stdout=resolution.stdout,stderr=resolution.stderr))
    assert resolution.returncode==0 and resolution.stderr==''; receive_clock=json.loads(resolution.stdout)
    args=[case['operation'],case['database'],'/run/secrets/application',case['config']]
    window=dict(before_us=history.server_clock(pg,case['database'])); lab.write_json(output/(label+'-clock.json'),window)
    try: history.capture(client,output,label,*args)
    finally:
        path=output/(label+'-response.json')
        if not path.exists(): lab.write_json(path,dict(returncode=None,stdout='',stderr='',outcome='UNKNOWN'))
        response=transition.read(path); response['command']=['docker','exec',client,'/build/ingest_bootstrap_probe',*args]
        lab.write_json(path,response)
        window['after_us']=history.server_clock(pg,case['database']); lab.write_json(output/(label+'-clock.json'),window)
        _,after,after_native,after_inputs=history.external(pg,client,output,case,directory,response,closure=oracle.terminal_seed_closed)
    context=dict(case=dict(case),before=before,native_before=native,inputs_before=inputs,after=after,native_after=after_native,inputs_after=after_inputs,
        response=response,server_window=window,catalog_before=catalog,catalog_after=history.catalog(pg,case['database']),arguments=args,
        attribution=attribution,receive_clock=receive_clock,backend_absence=transition.read(output/(label+'-control-backends.json')),
        backends=transition.read(output/(label+'-backends.json')),custody=transition.read(output/(label+'-custody.json')))
    lab.write_json(output/(label+'-verification.json'),context)
    checked=oracle.verify_terminal_context(context,initial,expected); controls=oracle.terminal_controls(context,initial,expected)
    case.update(status='PASS',returncode=response['returncode'],oracle=checked,negative_controls=controls,server_window=window)
    return after,after_native,after_inputs


def terminal_mutation(pg,client,output,clone,directory,expected,initial,step,kind):
    label=clone['name']+'-'+step+'-mutation'
    transition.observe_origin_absence(pg,clone['database'],output,label)
    oracle.same(schema.rows(pg,clone['database']),expected[0])
    assert history.dump(client,output,label+'-before',directory)==expected[1] and history.inputs_at(directory/'store')==expected[2]
    statement=oracle.terminal_mutation_statement(kind,expected,initial)
    record=dict(kind='isolated-terminal-'+kind,before=dict(control=expected[0],native=expected[1],inputs=expected[2]),after=None,statement=statement,sql_output=None)
    lab.write_json(output/(label+'.json'),record); (output/(label+'.sql')).write_text(statement)
    record['sql_output']=lab.sql(pg,clone['database'],statement); lab.write_json(output/(label+'.json'),record)
    record['after']=dict(control=schema.rows(pg,clone['database']),native=history.dump(client,output,label+'-after',directory),inputs=history.inputs_at(directory/'store'))
    lab.write_json(output/(label+'.json'),record)
    return oracle.terminal_mutation_checked(record,expected,initial,kind)


def run_terminal_sequence(pg,client,output,clone,directory,expected,report,*,origins,initial):
    name=clone['name']; origin_name=oracle.TERMINAL_ORIGIN_NAMES[name]
    assert clone['origin']==origin_name and expected==origins[origin_name]
    assert clone['grants']==[dict(statement=s,stdout='') for s in oracle.TERMINAL_GRANTS]
    oracle.history_catalog_checked(report['origins'][origin_name]['catalog'],clone['catalog_before'],grants=oracle.TERMINAL_GRANTS)
    transition.observe_origin_absence(pg,clone['database'],output,name+'-logical-origin')
    assert history.dump(client,output,name+'-logical-origin',directory)==expected[1] and history.inputs_at(directory/'store')==expected[2]
    source=Path(__file__).with_name('installed_cycle_client.py'); target=output/'installed_cycle_client.py'
    if not target.exists(): shutil.copy2(source,target)
    assert history.digest(target)==history.digest(source)
    if 'seed' in oracle.TERMINAL_STEPS[name]:
        configure(pg,client,output,clone['config'],ack=True,endpoint='ws://127.0.0.1:7447/installed-fixture')
    else:
        configure(pg,client,output,clone['config'],ack=True)
    oracle.terminal_network_checked(tomllib.loads((output/Path(clone['config']).name).read_text()),name)
    attribution=terminal_attribution(output,directory,initial) if 'seed' in oracle.TERMINAL_STEPS[name] else None
    if name=='missing-input': expected=activated_quarantine(output,directory,expected)
    predecessor=None
    for step in oracle.TERMINAL_STEPS[name]:
        kind=oracle.terminal_mutation_kind(name,step)
        if kind: expected=terminal_mutation(pg,client,output,clone,directory,expected,initial,step,kind)
        operation=oracle.TERMINAL_SEED_MODE if step=='seed' else step.split('-')[0]+'-activated'
        case=dict(name=name+'-'+step,clone=name,origin=origin_name,operation=operation,predecessor=predecessor,database=clone['database'],config=clone['config'],status='RUNNING')
        report['cases'].append(case); lab.write_json(output/'result.json',report)
        if step=='seed': expected=terminal_seed(pg,client,output,case,directory,expected,clone['catalog_before'],initial,attribution)
        else:
            expected=installed_cycle.execute(pg,client,output,case,directory,expected,clone['catalog_before'],initial,
                checks=(lambda c,s:oracle.verify_terminal_context(c,s,expected),lambda c,s:oracle.terminal_controls(c,s,expected),oracle.terminal_closed))
        case['catalog_after']=history.catalog(pg,clone['database']); assert case['catalog_after']==clone['catalog_before']
        predecessor=case['name']; lab.write_json(output/'result.json',report)
    return predecessor


def run_activated_terminal(root,build,product_source):
    oracle.terminal_qualification(transition.read(build/'component-result.json'))
    origins,initial=oracle.terminal_origins_checked(root)
    try:
        return transition.run(root,build,suite=TERMINAL_SUITE,sequence=partial(run_terminal_sequence,origins=origins,initial=initial),
            origin_specs=oracle.TERMINAL_ORIGINS,origin_for_clone=oracle.TERMINAL_ORIGIN_NAMES,extra_grants=oracle.TERMINAL_GRANTS,
            producer_progress=True,producer_ack=True,preparation_hole=True,compacted_reader=True,compacted_initial=True,
            compacted_activated=True,compacted_terminal=True,product_source=product_source)
    finally: assert oracle.terminal_origins_checked(root)==(origins,initial)


ORIGIN = oracle.ORIGIN
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('root',type=Path); parser.add_argument('build',type=Path)
    parser.add_argument('--product-source',required=True,help='Exact reviewed and qualified integrated build revision')
    profile=parser.add_mutually_exclusive_group()
    profile.add_argument("--history",action="store_true",help="Explicit connected Terminal2/PreparedOpen3 synthetic floor profile")
    profile.add_argument('--history-faults',action='store_true',help='Exact retained PreparedOpen3 delivery fault profile')
    profile.add_argument('--initial-journey',action='store_true',help='Explicit compacted initial prepare/gap/activation and installed startup profile')
    profile.add_argument('--initial-preparation',action='store_true',help='Installed original/compacted existing initial preparation completion and exact retry')
    profile.add_argument('--initial-preparation-faults',action='store_true',help='Installed compacted completion wire baselines and before/after dispatch delivery loss')
    profile.add_argument('--activated',action='store_true',help='Installed compacted activated-initial recovery and ACK with real COMMIT loss seeds')
    profile.add_argument('--activated-terminal',action='store_true',help='Compacted activated source, real Event abandonment and retirement positive/refusal profile')
    args = parser.parse_args()
    (run_activated_terminal if args.activated_terminal else run_activated if args.activated else run_initial_preparation_faults if args.initial_preparation_faults else run_initial_preparation if args.initial_preparation else run_initial_journey if args.initial_journey else run_history_faults if args.history_faults else run_history if args.history else run)(args.root.resolve(strict=True),args.build.resolve(strict=True),args.product_source)
