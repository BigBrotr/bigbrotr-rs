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


def empty_preparer_setup(pg,output,clone,directory,expected):
    """Admit and seal the ordinary unowned job before the gate's dataset lock."""
    from concurrency_cases import Session
    from initial_empty_locks import guards,session_identity,JOB,ACTOR,REQUEST,DIGEST
    database=clone['database']; sessions=[]
    setup=dict(before=schema.rows(pg,database),job_id='\\x'+JOB,principal_id='\\x'+ACTOR,request_key='\\x'+REQUEST,
        request_digest='\\x'+DIGEST,guards_before=guards(pg,database),statement=[],transactions=[],status='INCOMPLETE',cleanup_errors=[])
    record=dict(setup=setup,inputs_before=history.inputs_at(directory/'store'),actor_exit_code=None,actor_closed_by_us=None)
    oracle.same(setup['before'],expected[0]); assert record['inputs_before']==expected[2]=={}
    def save(): lab.write_json(output/'preparer-setup.json',record)
    def owned():
        current,=json.loads(lab.docker('inspect',pg['postgres']).stdout)
        assert current['State']['Running'] and current['Config']['Labels'][lab.LABEL]==pg['owner']
    try:
        setup['server_window']=dict(before_us=history.server_clock(pg,database))
        actor=Session(pg,database,'preparer_setup',sessions,role='bigbrotr_control')
        setup.update(backend=actor.backend,identity=session_identity(actor),created_at_us=history.server_clock(pg,database))
        created=setup['created_at_us']
        setup['statement']=['BEGIN ISOLATION LEVEL READ COMMITTED;',
            "INSERT INTO control.jobs(job_id,dataset_id,principal_id,request_key,request_digest,kind,config_revision,policy_revision,created_at_us) SELECT "
            f"decode('{JOB}','hex'),dataset_id,decode('{ACTOR}','hex'),decode('{REQUEST}','hex'),decode('{DIGEST}','hex'),'import',config_revision,policy_revision,{created} "
            "FROM control.datasets RETURNING to_jsonb(jobs)||jsonb_build_object('xmin',xmin::text);",
            f"UPDATE control.jobs j SET input_complete=true WHERE job_id=decode('{JOB}','hex') RETURNING to_jsonb(j)||jsonb_build_object('xmin',xmin::text);",'COMMIT;']
        save()
        for statement in setup['statement']:
            setup['transactions'].append(actor.execute(statement)); save()
        setup['server_window']['after_us']=history.server_clock(pg,database)
        setup.update(after=schema.rows(pg,database),guards_after=guards(pg,database))
        record['inputs_after']=history.inputs_at(directory/'store')
        actor.close(owned); sessions.remove(actor)
        record['actor_exit_code']=actor.process.returncode
        setup['actor_absence']=installed_cycle.exact_absence(pg,database,[actor.backend])
        record['actor_closed_by_us']=history.server_clock(pg,database)
        setup['status']='CAPTURED'; oracle.empty_preparer_setup_checked(record,expected)
    finally:
        for actor in reversed(sessions):
            try: actor.close(owned)
            except BaseException as error: setup['cleanup_errors'].append(dict(stage='setup-actor',error=repr(error)))
        save()
    return record


def empty_preparer_claim(pg,output,case,directory,gate):
    """Claim the already admitted job under the actual waiting preparer."""
    from concurrency_cases import Session
    from initial_empty_locks import guards,session_identity,claim_statement
    database=case['database']; before=gate['control']
    intent=oracle.only(before['initial_ingest_successions'])
    control=oracle.only(before['owner_epochs'],role='control',resource_id=intent['prepared_by_resource'],epoch=intent['prepared_by_epoch'])
    assert control['process_incarnation']==intent['prepared_by_incarnation'] and control['state']=='active'
    value=dict(status='INCOMPLETE',cleanup_errors=[],before=before,control=control,transactions=[])
    sessions=[]
    def save(): lab.write_json(output/(case['name']+'-import-claim.json'),value)
    def owned():
        current,=json.loads(lab.docker('inspect',pg['postgres']).stdout)
        assert current['State']['Running'] and current['Config']['Labels'][lab.LABEL]==pg['owner']
    def live():
        return json.loads(lab.sql(pg,database,"SELECT json_build_object('pid',pid,'backend_start',backend_start,'database',datname,'user',usename,'state',state,'xact_start',xact_start,'query_start',query_start,'query',query,'backend_xid',backend_xid::text,'blockers',pg_blocking_pids(pid)) FROM pg_stat_activity WHERE pid="+str(control['control_backend_pid'])+" AND backend_start='"+control['control_backend_start']+"'::timestamptz AND datname='"+database+"';"))
    try:
        oracle.same(schema.rows(pg,database),before)
        value.update(inputs_before=history.inputs_at(directory/'store'),guards_before=guards(pg,database),live_before=live(),
            server_window=dict(before_us=history.server_clock(pg,database)))
        oracle.empty_preparer_live_checked(value['live_before'],control,intent,database,value['server_window']['before_us'])
        actor=Session(pg,database,'empty_initial_preparer_claim',sessions,role='bigbrotr_control')
        value.update(backend=actor.backend,identity=session_identity(actor),claimed_at_us=history.server_clock(pg,database))
        value['statements']=['BEGIN ISOLATION LEVEL READ COMMITTED;',claim_statement(control,False),'COMMIT;']; save()
        for statement in value['statements']:
            value['transactions'].append(actor.execute(statement)); save()
        value['server_window']['after_us']=history.server_clock(pg,database)
        value.update(after=schema.rows(pg,database),inputs_after=history.inputs_at(directory/'store'),guards_after=guards(pg,database),live_after=live())
        value['observed_by_us']=history.server_clock(pg,database)
        assert value['guards_before']==value['guards_after'] and value['live_before']==value['live_after']
        assert value['inputs_before']==value['inputs_after']=={} and value['observed_by_us']<control['lease_expires_at_us']
        oracle.empty_preparer_claim_rows(value,before,control)
        actor.close(owned); sessions.remove(actor)
        value['actor_exit_code']=actor.process.returncode
        value['actor_absence']=lab.sql(pg,'postgres',"SELECT count(*) FROM pg_stat_activity WHERE pid="+str(actor.backend['pid'])+" AND backend_start='"+actor.backend['backend_start']+"'::timestamptz AND datname='"+database+"';")
        value['actor_closed_by_us']=history.server_clock(pg,database)
        assert type(value['actor_exit_code']) is int and value['actor_absence']=='0'
        value['status']='CAPTURED'
    finally:
        for actor in reversed(sessions):
            try: actor.close(owned)
            except BaseException as error: value['cleanup_errors'].append(dict(stage='actor',error=repr(error)))
        save()
    return value


def journey_gate_invoke(pg,client,output,case,directory,*,compacted=True,preparer=False):
    from initial_empty_locks import Probe
    from activated_retirement_expiry import wait_event
    from retirement_wait import open_gate
    assert type(compacted) is bool and type(preparer) is bool and (not preparer or compacted)
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
        if preparer:
            gate['preparer_claim']=empty_preparer_claim(pg,output,case,directory,gate); save()
        path=output/(label+'-gate.go'); assert gate['ready']['path']==prefix+'.go'
        gate['go_before_us']=history.server_clock(pg,case['database'])
        if preparer: assert gate['go_before_us']<gate['preparer_claim']['control']['lease_expires_at_us']
        open_gate(path)
        gate['go_after_us']=history.server_clock(pg,case['database']); save()
        gate['released']=wait_event(probe,'successor-'+basis+'-gap-gate',phase='released'); save()
        if preparer:
            gate['released_observed_us']=history.server_clock(pg,case['database'])
            assert gate['released_observed_us']<gate['preparer_claim']['control']['lease_expires_at_us']; save()
        code=probe.wait(15); probe.observe()
        response.update(returncode=code,outcome='PROCESS_EXITED',readers_joined=not any(t.is_alive() for t in probe.threads),overflow=probe.overflow)
        if preparer:
            gate['preparer_control_absence']=history.observe_backend_absence(pg,case['database'],output,label+'-preparer-control')
            control=gate['preparer_claim']['control']
            gate['preparer_absence']=installed_cycle.exact_absence(pg,case['database'],[dict(pid=control['control_backend_pid'],
                backend_start=control['control_backend_start'],database=case['database'],control_epoch=control['epoch'])])
    except BaseException as error:
        gate['errors'].append(dict(stage='gap-capture',error=repr(error)))
        raise
    finally:
        if probe is not None:
            probe.stop()
            response.update(stdout=probe.stdout.decode(),stderr=probe.stderr.decode(),readers_joined=not any(t.is_alive() for t in probe.threads),overflow=probe.overflow)
        response['seconds']=time.monotonic()-started; lab.write_json(output/(label+'-response.json'),response); save()
    return args,gate


def journey_probe(pg,client,output,case,directory,expected,catalog,initial,*,preparation=False,delivery=False,preparer=False):
    assert type(preparation) is bool and type(delivery) is bool and (not delivery or preparation)
    assert type(preparer) is bool and not (preparer and (preparation or delivery))
    label=case['name']; before=schema.rows(pg,case['database']); oracle.same(before,expected[0])
    native=history.dump(client,output,label+'-before',directory); inputs=history.inputs_at(directory/'store')
    assert (native,inputs)==expected[1:]
    lab.write_json(output/(label+'-before.json'),before); lab.write_json(output/(label+'-inputs.json'),inputs)
    args=[case['operation'],case['database'],'/run/secrets/application',case['config']]; gate=None
    window=dict(before_us=history.server_clock(pg,case['database'])); lab.write_json(output/(label+'-clock.json'),window)
    try:
        if case['operation'] in (oracle.JOURNEY_GAP_MODE,oracle.PREPARATION_ORIGINAL_GAP):
            args,gate=journey_gate_invoke(pg,client,output,case,directory,compacted=case['operation']==oracle.JOURNEY_GAP_MODE,preparer=preparer)
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
    check,control=(oracle.verify_empty_preparer_context,oracle.empty_preparer_controls) if preparer else (oracle.verify_preparation_context,oracle.preparation_controls) if preparation else (oracle.verify_journey_context,oracle.journey_controls)
    options=dict(delivery=delivery) if preparation and not preparer else {}
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


def terminal_import_capture(pg,client,output,case,args,directory):
    from activated_retirement_expiry import wait_event
    from concurrency_cases import Session
    from initial_empty_locks import Probe,guards,session_identity
    from initial_empty_preparer import statements
    from retirement_wait import open_gate
    label=case['name']; database=case['database']; prefix=output/(label+'-gate')
    assert not prefix.with_suffix('.go').exists() and not prefix.with_suffix('.go').is_symlink()
    value=dict(status='INCOMPLETE',cleanup_errors=[],gates=[],transactions=[])
    response=dict(command=['docker','exec',client,'/build/ingest_bootstrap_probe',*args],returncode=None,stdout='',stderr='',outcome='UNKNOWN_PROCESS_OUTCOME')
    sessions=[]; probe=None; started=time.monotonic()
    def save(): lab.write_json(output/(label+'-import-claim.json'),value)
    def owned():
        current,=json.loads(lab.docker('inspect',pg['postgres']).stdout)
        assert current['State']['Running'] and current['Config']['Labels'][lab.LABEL]==pg['owner']
    def live(control):
        return json.loads(lab.sql(pg,database,"SELECT json_build_object('pid',pid,'backend_start',backend_start,'database',datname,'user',usename,'state',state,'xact_start',xact_start) FROM pg_stat_activity WHERE pid="+str(control['control_backend_pid'])+" AND backend_start='"+control['control_backend_start']+"'::timestamptz AND datname='"+database+"';"))
    try:
        probe=Probe(response['command'])
        waiting=wait_event(probe,'live-terminal-import-gate',phase='waiting')
        value['gates'].append(dict(event=waiting,observed_us=history.server_clock(pg,database)))
        state=oracle.only(probe.parsed,event='successor-state',stage='activated-zero')
        pg_before={table:json.loads(rows) for table,rows in state['state']['control_tables']}
        value['before']=schema.rows(pg,database); oracle.same(value['before'],pg_before)
        control=oracle.only(pg_before['owner_epochs'],role='control',state='active'); value['control']=control
        backend=oracle.only(probe.parsed,event='successor-backend',stage='terminal-import-gate')
        assert backend==dict(event='successor-backend',stage='terminal-import-gate',database=database,
            pid=control['control_backend_pid'],backend_start=control['control_backend_start'],control_epoch=control['epoch'],present=True)
        assert waiting==dict(event='live-terminal-import-gate',mode=oracle.TERMINAL_IMPORT_MODE,phase='waiting',
            go='/lab/'+prefix.with_suffix('.go').name,gate_timeout_ms=10000,poll_ms=5,
            control_resource=control['resource_id'][2:],control_epoch=control['epoch'],control_incarnation=control['process_incarnation'][2:],
            activated_at_us=oracle.only(pg_before['initial_ingest_activations'])['activated_at_us'])
        value.update(inputs_before=history.inputs_at(directory/'store'),guards_before=guards(pg,database),live_before=live(control),
            server_window=dict(before_us=history.server_clock(pg,database)))
        assert value['inputs_before']=={} and value['live_before']==dict(pid=control['control_backend_pid'],
            backend_start=control['control_backend_start'],database=database,user='bigbrotr_control',state='idle',xact_start=None)
        actor=Session(pg,database,'terminal_import_claim',sessions,role='bigbrotr_control')
        value.update(backend=actor.backend,identity=session_identity(actor),created_at_us=history.server_clock(pg,database))
        value['statements']=statements(control,value['created_at_us']); save()
        for statement in value['statements']:
            value['transactions'].append(actor.execute(statement)); save()
        value['server_window']['after_us']=history.server_clock(pg,database)
        value.update(after=schema.rows(pg,database),inputs_after=history.inputs_at(directory/'store'),guards_after=guards(pg,database),live_after=live(control))
        value['observed_by_us']=history.server_clock(pg,database)
        assert value['guards_before']==value['guards_after'] and value['live_before']==value['live_after']
        assert value['inputs_before']==value['inputs_after'] and value['observed_by_us']<control['lease_expires_at_us']
        oracle.terminal_import_rows(value,pg_before,control)
        # Close the ordinary auxiliary actor before releasing the native owner.
        actor.close(owned); sessions.remove(actor)
        value['actor_exit_code']=actor.process.returncode
        value['actor_absence']=lab.sql(pg,'postgres',"SELECT count(*) FROM pg_stat_activity WHERE pid="+str(actor.backend['pid'])+" AND backend_start='"+actor.backend['backend_start']+"'::timestamptz AND datname='"+database+"';")
        assert type(value['actor_exit_code']) is int and value['actor_absence']=='0'
        value['go_before_us']=history.server_clock(pg,database)
        assert value['go_before_us']<control['lease_expires_at_us']; save()
        open_gate(prefix.with_suffix('.go'))
        value['gates'].append(dict(event=wait_event(probe,'live-terminal-import-gate',phase='released'),observed_us=history.server_clock(pg,database)))
        code=probe.wait(12); probe.observe()
        assert code==0 and not probe.stderr
        value['status']='CAPTURED'
    except BaseException as error:
        value['failure']=repr(error)
    finally:
        for actor in reversed(sessions):
            try: actor.close(owned)
            except BaseException as error: value['cleanup_errors'].append(dict(stage='actor',error=repr(error)))
        if probe is not None:
            code=probe.process.poll()
            if code is not None:
                for thread in probe.threads: thread.join(1)
                response.update(returncode=code,outcome='PROCESS_EXITED',readers_joined=not any(t.is_alive() for t in probe.threads),overflow=probe.overflow)
            response.update(stdout=bytes(probe.stdout).decode(errors='replace'),stderr=bytes(probe.stderr).decode(errors='replace'))
            try: probe.stop()
            except BaseException as error: value['cleanup_errors'].append(dict(stage='probe',error=repr(error)))
        response['seconds']=time.monotonic()-started
        save(); lab.write_json(output/(label+'-response.json'),response)
    return value


def terminal_guard_mutation(pg,client,output,clone,directory,expected,kind):
    from initial_empty_locks import guards
    label=clone['name']+'-retire'+('-refuse' if kind=='native-open' else '')+'-mutation'
    transition.observe_origin_absence(pg,clone['database'],output,label)
    oracle.same(schema.rows(pg,clone['database']),expected[0])
    assert history.dump(client,output,label+'-before',directory)==expected[1] and history.inputs_at(directory/'store')==expected[2]
    record=dict(kind='privileged-isolated-terminal-'+kind,before=dict(control=expected[0],native=expected[1],inputs=expected[2]),after=None,
        server_window=dict(before_us=history.server_clock(pg,clone['database'])),guards_before=guards(pg,clone['database']))
    record['statement']=oracle.terminal_guard_mutation_statement(kind,expected,record['server_window']['before_us']); record['sql_output']=None
    lab.write_json(output/(label+'.json'),record); (output/(label+'.sql')).write_text(record['statement'])
    record['sql_output']=lab.sql(pg,clone['database'],record['statement'])
    record['server_window']['after_us']=history.server_clock(pg,clone['database']); record['guards_after']=guards(pg,clone['database'])
    record['after']=dict(control=schema.rows(pg,clone['database']),native=history.dump(client,output,label+'-after',directory),inputs=history.inputs_at(directory/'store'))
    lab.write_json(output/(label+'.json'),record)
    checked=oracle.terminal_guard_mutation_checked(record,expected,kind)
    lab.write_json(output/(label+'-controls.json'),oracle.terminal_guard_mutation_controls(record,expected,kind))
    return checked


def terminal_seed(pg,client,output,case,directory,expected,catalog,initial,attribution,*,faults=False,post_proof=False,guards=False):
    label=case['name']; before=schema.rows(pg,case['database']); oracle.same(before,expected[0])
    native=history.dump(client,output,label+'-before',directory); inputs=history.inputs_at(directory/'store'); assert (native,inputs)==expected[1:]
    lab.write_json(output/(label+'-before.json'),before); lab.write_json(output/(label+'-inputs.json'),inputs)
    resolution=lab.docker('exec',client,'python3','-c',
        "import json,time; n=round(time.clock_getres(time.CLOCK_REALTIME)*1000000000); print(json.dumps(dict(clock='CLOCK_REALTIME',resolution_ns=n,resolution_us=(n+999)//1000)))",timeout=5)
    lab.write_json(output/(label+'-receive-clock.json'),dict(returncode=resolution.returncode,stdout=resolution.stdout,stderr=resolution.stderr))
    assert resolution.returncode==0 and resolution.stderr==''; receive_clock=json.loads(resolution.stdout)
    args=[case['operation'],case['database'],'/run/secrets/application',case['config']]
    if guards: args.append('/lab/'+label+'-gate')
    import_claim=None
    window=dict(before_us=history.server_clock(pg,case['database'])); lab.write_json(output/(label+'-clock.json'),window)
    try:
        if guards: import_claim=terminal_import_capture(pg,client,output,case,args,directory)
        else: history.capture(client,output,label,*args)
    finally:
        path=output/(label+'-response.json')
        if not path.exists(): lab.write_json(path,dict(returncode=None,stdout='',stderr='',outcome='UNKNOWN'))
        response=transition.read(path); response['command']=['docker','exec',client,'/build/ingest_bootstrap_probe',*args]
        lab.write_json(path,response)
        window['after_us']=history.server_clock(pg,case['database']); lab.write_json(output/(label+'-clock.json'),window)
        _,after,after_native,after_inputs=history.external(pg,client,output,case,directory,response,closure=partial(oracle.terminal_seed_closed,zero=faults and case['clone']=='zero-job',import_claim=import_claim))
    context=dict(case=dict(case),before=before,native_before=native,inputs_before=inputs,after=after,native_after=after_native,inputs_after=after_inputs,
        response=response,server_window=window,catalog_before=catalog,catalog_after=history.catalog(pg,case['database']),arguments=args,
        attribution=attribution,receive_clock=receive_clock,backend_absence=transition.read(output/(label+'-control-backends.json')),
        backends=transition.read(output/(label+'-backends.json')),custody=transition.read(output/(label+'-custody.json')))
    if guards: context['import_claim']=import_claim
    lab.write_json(output/(label+'-verification.json'),context)
    checked=oracle.verify_terminal_context(context,initial,expected,faults=faults,post_proof=post_proof,guards=guards); controls=oracle.terminal_controls(context,initial,expected,faults=faults,post_proof=post_proof,guards=guards)
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



def terminal_delivery_hook(pg,client,output,case,directory,*,post_proof=False,expected=None,initial=None,empty_initial=False,empty_proof=False):
    from fault_cases import Process,safe_event
    assert type(empty_proof) is bool and not (empty_proof and (empty_initial or post_proof))
    proof=post_proof or empty_proof
    assert type(empty_initial) is bool and not (empty_initial and post_proof)
    if empty_proof:
        name,step=oracle.empty_shape_specification(case);assert step=='proof'
        operation='retire-empty';target,later=2,2;normal=name=='proof-baseline';mode='hold-after-complete'
    elif empty_initial:
        name,step=oracle.empty_delivery_specification(case); assert step in ('baseline','loss')
        operation='retire-empty'; target,later=3,1; normal=step=='baseline'
        mode='hold' if normal else 'before-forward' if name=='before' else 'after-complete'
    else:
        name,step=oracle.terminal_specification(case,faults=not post_proof,post_proof=post_proof)
        operation=name.split('-')[0] if post_proof else step.split('-')[0]
        target=(oracle.TERMINAL_PROOF_SELECTORS if post_proof else oracle.TERMINAL_COMMIT_SELECTORS)[operation]; later=4 if post_proof else 3
        normal=name.endswith('-baseline') if post_proof else step.endswith('-baseline')
        assert post_proof or normal or step.endswith('-loss')
        mode='hold-after-complete' if post_proof else 'hold' if normal else 'before-forward' if name.endswith('-before') else 'after-complete'
    label=case['name']; config=output/Path(case['config']).name; proxy=None
    record=dict(status='INCOMPLETE',operation=operation,mode=mode,target_commit=target,wire=None,held=None,
        configuration=dict(path=case['config'],direct=None,proxied=None,restored=None),errors=[])
    if proof: record['mutation']=None
    def save(): lab.write_json(output/(label+'-delivery.json'),record)
    def callback(phase):
        nonlocal proxy
        try:
            if phase=='before-launch':
                command=['docker','exec','-i',client,'python3','-B','/lab/fault_proxy.py','--upstream-port','5432',
                    '--listen-port','0','--mode',mode,'--commit-number',str(target),'--later-commits',str(later),'--deadline-seconds','30']
                record['wire']=dict(command=command,source_sha256=history.digest(output/'fault_proxy.py'),ready=None,receipt=None,
                    events=[],stdout='',stderr='',exit_code=None,readers_joined=False,overflow=False,forced_wrapper_stop=False)
                save(); proxy=Process(command,input_pipe=True); ready=proxy.event('ready',5); record['wire']['ready']=ready
                assert ready['proxy_sha256']==record['wire']['source_sha256']
                direct=config.read_text(); proxied=history.set_value(direct,'postgres.control','endpoint','127.0.0.1:'+str(ready['listen_port']))
                record['configuration'].update(direct=direct,proxied=proxied); save()
                (output/(label+'-direct.toml')).write_text(direct); (output/(label+'-proxied.toml')).write_text(proxied); config.write_text(proxied)
            elif phase=='launched':
                if normal or proof:
                    held=proxy.event('held',10)
                    capture=dict(event=held,before_us=history.server_clock(pg,case['database']),after_us=None,release_before_us=None,
                        release_after_us=None,control=schema.rows(pg,case['database']),inputs=history.inputs_at(directory/'store'),backend=None)
                    record['held']=capture; save()
                    current=max((r for r in capture['control']['owner_epochs'] if r['role']=='control'),key=lambda r:r['epoch'])
                    pid=current['control_backend_pid']; assert type(pid) is int and pid>0
                    statement="SELECT json_build_object('observed_at_us',(EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint," \
                        "'pid',pid,'backend_start',backend_start,'database',datname,'user',usename,'state',state," \
                        "'xact_start',xact_start,'query',query) FROM pg_catalog.pg_stat_activity WHERE pid="+str(pid)+ \
                        " AND datname=current_database() AND usename='bigbrotr_control';"
                    capture['backend']=json.loads(lab.sql(pg,case['database'],statement))
                    capture['after_us']=history.server_clock(pg,case['database']); save()
                    if empty_initial:
                        assert expected is not None
                        window=dict(before_us=transition.read(output/(label+'-clock.json'))['before_us'],after_us=capture['after_us'])
                        checked=oracle.terminal_held_state(expected,capture['control'],window,operation)
                        assert capture['inputs']==expected[2] and capture['after_us']<checked.control['lease_expires_at_us']
                        b=capture['backend']; assert b['state']=='idle in transaction' and b['xact_start'] is not None
                        assert ' '.join(b['query'].split())=='SELECT control.require_empty_initial_retirement($1::bytea,$2::bytea)'
                        assert b['pid']==checked.control['control_backend_pid'] and b['backend_start']==checked.control['control_backend_start']
                        assert b['database']==case['database'] and b['user']=='bigbrotr_control'
                        assert [held[k] for k in ('commits_seen','commits_forwarded','server_commit_complete','client_commit_complete')]==[3,2,2,2]
                    if proof:
                        assert expected is not None and initial is not None
                        window=dict(before_us=transition.read(output/(label+'-clock.json'))['before_us'],after_us=capture['after_us'])
                        checked=oracle.terminal_held_state(expected,capture['control'],window,operation)
                        assert capture['inputs']==expected[2] and capture['after_us']<checked.control['lease_expires_at_us']
                        backend=capture['backend']; assert backend['state']=='idle' and backend['xact_start'] is None and backend['query']=='COMMIT'
                        assert backend['pid']==checked.control['control_backend_pid'] and backend['backend_start']==checked.control['control_backend_start']
                        assert backend['database']==case['database'] and backend['user']=='bigbrotr_control'
                        assert [held[k] for k in ('commits_seen','commits_forwarded','server_commit_complete','client_commit_complete')]==[target,target,target,target-1]
                        if empty_proof and not normal:
                            record['mutation']=empty_shape_corruption(pg,output,case['database'],label+'-mutation',capture['control'],initial,name)
                            assert record['mutation']['after_us']<checked.control['lease_expires_at_us'];save()
                        elif not normal:
                            kind='head-drift' if name.endswith('-head') else 'digest-drift'
                            mutation=dict(kind=kind,before=capture['control'],after=None,statement=oracle.terminal_drift_statement(kind,capture['control'],initial),
                                sql_output=None,before_us=history.server_clock(pg,case['database']),after_us=None)
                            record['mutation']=mutation; save(); (output/(label+'-mutation.sql')).write_text(mutation['statement'])
                            mutation['sql_output']=lab.sql(pg,case['database'],mutation['statement']); save()
                            mutation['after']=schema.rows(pg,case['database']); mutation['after_us']=history.server_clock(pg,case['database']); save()
                            oracle.terminal_drift_checked(mutation,capture['control'],initial,kind,dict(before_us=window['before_us'],after_us=mutation['after_us']))
                    capture['release_before_us']=history.server_clock(pg,case['database']); proxy.send('release')
                    capture['release_after_us']=history.server_clock(pg,case['database']); proxy.event('released',5); save()
            else:
                assert phase=='child-joined'
                try:
                    assert proxy is not None
                    record['wire']['receipt']=proxy.event('receipt',5); assert proxy.wait(5)==0
                finally:
                    try:
                        if proxy is not None:
                            forced=proxy.process.poll() is None; proxy.stop(); wire=record['wire']
                            wire.update(stdout=bytes(proxy.stdout).decode(errors='replace'),stderr=bytes(proxy.stderr).decode(errors='replace'),
                                exit_code=proxy.process.returncode,readers_joined=not any(t.is_alive() for t in proxy.threads),overflow=proxy.overflow,forced_wrapper_stop=forced)
                            wire['events']=[safe_event(json.loads(v)) for v in wire['stdout'].splitlines()]
                    finally:
                        settings=record['configuration']
                        if settings['proxied'] is not None:
                            assert config.read_text()==settings['proxied']; config.write_text(settings['direct']); settings['restored']=config.read_text()
                            (output/(label+'-restored.toml')).write_text(settings['restored'])
                        save()
                terminal=transition.read(output/(label+'.terminal.json'))
                assert terminal['reaped'] is True and terminal['forced'] is False and terminal['error'] is None
                assert record['wire']['readers_joined'] and not record['wire']['overflow'] and not record['wire']['forced_wrapper_stop']
                assert record['wire']['stderr']=='' and record['configuration']['restored']==record['configuration']['direct'] and record['errors']==[]
                record['status']='JOINED_CONFIG_RESTORED'
            save(); return record
        except BaseException as error:
            record['errors'].append(dict(phase=phase,error=repr(error))); save()
            if phase!='child-joined' and proxy is not None and proxy.process.poll() is None:
                try: proxy.send('close')
                except BaseException: pass
            raise
    return callback


def terminal_wait(pg,client,output,case,directory,expected,catalog,initial):
    import retirement_wait
    label=case['name']; before=schema.rows(pg,case['database']); oracle.same(before,expected[0])
    native=history.dump(client,output,label+'-before',directory); inputs=history.inputs_at(directory/'store'); assert (native,inputs)==expected[1:]
    lab.write_json(output/(label+'-before.json'),before); lab.write_json(output/(label+'-inputs.json'),inputs)
    name=case['clone']; mode='gate' if name in ('head-drift','digest-drift','event-mask') else name
    def drift(held):
        # This observer mutates only the selected SQL rows after the actual
        # proof gate. The probe retains the native owner throughout the wait.
        record=dict(kind=name,before=held,after=None,statement=oracle.terminal_drift_statement(name,held,initial),sql_output=None,
            before_us=history.server_clock(pg,case['database']),after_us=None)
        lab.write_json(output/(label+'-mutation.json'),record); (output/(label+'-mutation.sql')).write_text(record['statement'])
        record['sql_output']=lab.sql(pg,case['database'],record['statement']); lab.write_json(output/(label+'-mutation.json'),record)
        record['after']=schema.rows(pg,case['database']); record['after_us']=history.server_clock(pg,case['database'])
        lab.write_json(output/(label+'-mutation.json'),record)
        return record
    response,window,schedule=retirement_wait.invoke(pg,client,output,case,mode,compacted=True,drift=drift if mode=='gate' else None)
    diagnostic=retirement_wait.diagnostic(pg,output,label,schedule,window,mode,compacted=True)
    def closure(value,database):
        assert retirement_wait.closed_and_joined(value,schedule,compacted=True), 'wait auxiliaries remain unexcluded'
        return oracle.terminal_wait_closed(value,database)
    _,after,after_native,after_inputs=history.external(pg,client,output,case,directory,response,closure=closure)
    args=[case['operation'],case['database'],'/run/secrets/application',case['config'],'/lab/'+label+'-gate']
    context=dict(case=dict(case),before=before,native_before=native,inputs_before=inputs,after=after,native_after=after_native,inputs_after=after_inputs,
        response=response,server_window=window,catalog_before=catalog,catalog_after=history.catalog(pg,case['database']),arguments=args,
        schedule=schedule,diagnostic=diagnostic,backend_absence=transition.read(output/(label+'-control-backends.json')),
        backends=transition.read(output/(label+'-backends.json')),custody=transition.read(output/(label+'-custody.json')))
    lab.write_json(output/(label+'-verification.json'),context)
    checked=oracle.verify_terminal_context(context,initial,expected,faults=True); controls=oracle.terminal_controls(context,initial,expected,faults=True)
    case.update(status='PASS',returncode=response['returncode'],oracle=checked,negative_controls=controls,server_window=window)
    return after,after_native,after_inputs


def run_terminal_sequence(pg,client,output,clone,directory,expected,report,*,origins,initial,faults=False,post_proof=False,guards=False):
    profile=oracle.ActivatedTerminalGuardProfile if guards else oracle.ActivatedSourceProofProfile if post_proof else oracle.ActivatedTerminalFaultProfile if faults else oracle.ActivatedTerminalProfile
    name=clone['name']; origin_name=oracle.TERMINAL_GUARD_ORIGIN_NAMES[name] if guards else 'compacted-initial' if faults or post_proof else oracle.TERMINAL_ORIGIN_NAMES[name]
    assert clone['origin']==origin_name and expected==origins[origin_name]
    assert clone['grants']==[dict(statement=s,stdout='') for s in oracle.TERMINAL_GRANTS]
    oracle.history_catalog_checked(report['origins'][origin_name]['catalog'],clone['catalog_before'],grants=oracle.TERMINAL_GRANTS)
    transition.observe_origin_absence(pg,clone['database'],output,name+'-logical-origin')
    assert history.dump(client,output,name+'-logical-origin',directory)==expected[1] and history.inputs_at(directory/'store')==expected[2]
    for filename in ('installed_cycle_client.py','fault_proxy.py') if faults or post_proof else ('installed_cycle_client.py',):
        source=history.SOURCE/oracle.ACTIVATED_PROXY if filename=='fault_proxy.py' else Path(__file__).with_name(filename)
        target=output/filename
        if not target.exists(): shutil.copy2(source,target)
        assert history.digest(target)==history.digest(source)
    event='seed' in profile.STEPS[name] and not (faults and name=='zero-job')
    if event: configure(pg,client,output,clone['config'],ack=True,endpoint='ws://127.0.0.1:7447/installed-fixture')
    else: configure(pg,client,output,clone['config'],ack=True)
    oracle.terminal_network_checked(tomllib.loads((output/Path(clone['config']).name).read_text()),name,faults=faults,post_proof=post_proof,guards=guards)
    attribution=terminal_attribution(output,directory,initial) if event else None
    if not (faults or post_proof or guards) and name=='missing-input': expected=activated_quarantine(output,directory,expected)
    predecessor=None
    for step in profile.STEPS[name]:
        if guards and ((name=='draining' and step=='retire') or name=='native-open'):
            expected=terminal_guard_mutation(pg,client,output,clone,directory,expected,name)
        kind=oracle.terminal_mutation_kind(name,step) if not (faults or post_proof or guards) else None
        if kind: expected=terminal_mutation(pg,client,output,clone,directory,expected,initial,step,kind)
        operation=(oracle.TERMINAL_IMPORT_MODE if guards else oracle.TERMINAL_ZERO_MODE if faults and name=='zero-job' else oracle.TERMINAL_SEED_MODE) if step=='seed' else (oracle.TERMINAL_WAIT_MODES[name] if faults and step=='wait' else step.split('-')[0]+'-activated')
        if post_proof and step=='proof': operation=name.split('-')[0]+'-activated'
        case=dict(name=name+'-'+step,clone=name,origin=origin_name,operation=operation,predecessor=predecessor,database=clone['database'],config=clone['config'],status='RUNNING')
        report['cases'].append(case); lab.write_json(output/'result.json',report)
        baseline=None
        if faults and step.endswith('-loss'):
            baseline=oracle.only(report['cases'],name='baseline-'+step.split('-')[0]+'-baseline')['oracle']
            assert baseline['status']=='PASS'
        if post_proof and step=='proof' and not name.endswith('-baseline'):
            baseline=oracle.only(report['cases'],name=name.split('-')[0]+'-baseline-proof')['oracle']; assert baseline['status']=='PASS'
        if step=='seed': expected=terminal_seed(pg,client,output,case,directory,expected,clone['catalog_before'],initial,attribution,faults=faults,post_proof=post_proof,guards=guards)
        elif faults and step=='wait': expected=terminal_wait(pg,client,output,case,directory,expected,clone['catalog_before'],initial)
        else:
            hook=terminal_delivery_hook(pg,client,output,case,directory,post_proof=post_proof,expected=expected,initial=initial) if (faults and step.endswith(('-baseline','-loss'))) or (post_proof and step=='proof') else None
            def verify_context(c,s):
                if guards and name=='import' and step=='retire-refuse':
                    from activated_retirement_import import diagnostic
                    backend={k:c['backend']['exact'][0][k] for k in ('pid','backend_start','database')}
                    c['guard_diagnostic']=diagnostic(pg,backend,c['server_window'])
                    lab.write_json(output/(case['name']+'-diagnostic.json'),c['guard_diagnostic'])
                    lab.write_json(output/(case['name']+'-verification.json'),c)
                return oracle.verify_terminal_context(c,s,expected,faults=faults,post_proof=post_proof,guards=guards,baseline=baseline)
            expected=installed_cycle.execute(pg,client,output,case,directory,expected,clone['catalog_before'],initial,
                checks=(verify_context,
                    lambda c,s:oracle.terminal_controls(c,s,expected,faults=faults,post_proof=post_proof,guards=guards,baseline=baseline),partial(oracle.terminal_closed,faults=faults,post_proof=post_proof,guards=guards)),fault=hook)
        case['catalog_after']=history.catalog(pg,clone['database']); assert case['catalog_after']==clone['catalog_before']
        predecessor=case['name']; lab.write_json(output/'result.json',report)
    return predecessor


def run_activated_terminal(root,build,product_source,*,faults=False,post_proof=False,guards=False):
    assert all(type(v) is bool for v in (faults,post_proof,guards)) and sum((faults,post_proof,guards))<=1
    oracle.terminal_qualification(transition.read(build/'component-result.json'))
    def origins_checked():
        if guards: return oracle.terminal_guard_origins_checked(root)
        if not (faults or post_proof): return oracle.terminal_origins_checked(root)
        seed,initial=oracle.journey_origin_checked(root); return {'compacted-initial':seed},initial
    origins,initial=origins_checked()
    profile=oracle.ActivatedTerminalGuardProfile if guards else oracle.ActivatedSourceProofProfile if post_proof else oracle.ActivatedTerminalFaultProfile
    suite=transition.Suite(directory=profile.DIRECTORY,label=profile.LABEL,status=profile.STATUS,client_prefix=profile.CLIENT_PREFIX,
        database_prefix=profile.DATABASE_PREFIX,clones=profile.CLONES,cases=profile.CASES,not_run=profile.NOT_RUN,
        scope='Compacted activated actual unresolved import, draining retirement and isolated native OPEN guard' if guards else 'Compacted Source/abandon post-proof ancestor H/D under released server-completed refresh holds' if post_proof else 'Compacted activated terminal final COMMIT wire baselines/loss/retries, genuine cancelled-job waits, retirement post-proof H/D/Event-mask and zero-request claimed job') if faults or post_proof or guards else TERMINAL_SUITE
    pins=oracle.TERMINAL_GUARD_ORIGINS if guards else (oracle.JOURNEY_ORIGIN,) if faults or post_proof else oracle.TERMINAL_ORIGINS
    names=oracle.TERMINAL_GUARD_ORIGIN_NAMES if guards else {name:'compacted-initial' for name in profile.CLONES} if faults or post_proof else oracle.TERMINAL_ORIGIN_NAMES
    try:
        return transition.run(root,build,suite=suite,sequence=partial(run_terminal_sequence,origins=origins,initial=initial,faults=faults,post_proof=post_proof,guards=guards),
            origin_specs=pins,origin_for_clone=names,extra_grants=oracle.TERMINAL_GRANTS,
            producer_progress=True,producer_ack=True,preparation_hole=True,compacted_reader=True,compacted_initial=True,
            compacted_activated=True,compacted_terminal=True,product_source=product_source)
    finally: assert origins_checked()==(origins,initial)


def empty_restore(pg,client,output,clone,directory,expected,initial):
    label=clone['name']+'-restore'; transition.observe_origin_absence(pg,clone['database'],output,label)
    before=dict(control=schema.rows(pg,clone['database']),native=history.dump(client,output,label+'-before',directory),inputs=history.inputs_at(directory/'store'))
    assert before==dict(control=expected[0],native=expected[1],inputs=expected[2])
    pin=oracle.EMPTY_PREPARED_ORIGINS[1]; filename=oracle.only(initial[0]['producer_epochs'])['producer_slot_id'][2:]+'-0000000000000001-0000000000000003.cbor'
    source=pin['run']+'/'+pin['subdirectory']+'/original-prepared/quarantined-required/'+filename
    data=oracle.retained.regular_read(output.parent.parent/source); assert initial[2][filename]==dict(bytes=len(data),sha256=oracle.sha(data))
    spool=directory/'store/producer-inputs-v1'
    record=dict(kind='restore-retained-original-on-isolated-clone',source=source,name=filename,hex=data.hex(),before=before,after=None,
        physical_before=history.census(spool),physical_after=None)
    lab.write_json(output/(label+'.json'),record)
    fd=os.open(spool/filename,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600)
    with os.fdopen(fd,'wb') as stream: stream.write(data); stream.flush(); os.fsync(stream.fileno())
    fd=os.open(spool,os.O_RDONLY|os.O_DIRECTORY)
    try: os.fsync(fd)
    finally: os.close(fd)
    record['physical_after']=history.census(spool)
    record['after']=dict(control=schema.rows(pg,clone['database']),native=history.dump(client,output,label+'-after',directory),inputs=history.inputs_at(directory/'store'))
    lab.write_json(output/(label+'.json'),record)
    return oracle.empty_restore_checked(record,expected,initial)


def empty_digest_mutation(pg,client,output,clone,directory,expected,initial):
    label=clone['name']+'-mutation'; transition.observe_origin_absence(pg,clone['database'],output,label)
    before=dict(control=schema.rows(pg,clone['database']),native=history.dump(client,output,label+'-before',directory),inputs=history.inputs_at(directory/'store'))
    assert before==dict(control=expected[0],native=expected[1],inputs=expected[2])
    mutation=dict(kind='digest-drift',before=before['control'],after=None,
        statement=oracle.terminal_drift_statement('digest-drift',before['control'],initial),sql_output=None,
        before_us=history.server_clock(pg,clone['database']),after_us=None)
    record=dict(before=before,after=None,mutation=mutation)
    lab.write_json(output/(label+'.json'),record); (output/(label+'.sql')).write_text(mutation['statement'])
    mutation['sql_output']=lab.sql(pg,clone['database'],mutation['statement'])
    mutation['after']=schema.rows(pg,clone['database']); mutation['after_us']=history.server_clock(pg,clone['database'])
    record['after']=dict(control=mutation['after'],native=history.dump(client,output,label+'-after',directory),inputs=history.inputs_at(directory/'store'))
    lab.write_json(output/(label+'.json'),record)
    return oracle.empty_digest_mutation_checked(record,expected,initial)


def empty_lock_hook(pg,client,output,case,directory,expected):
    """One finite installed schedule; auxiliary custody closes before native inspection."""
    from concurrency_cases import Session
    from fault_cases import Process,safe_event
    from initial_empty_locks import session_identity,claim_statement,block_observation
    name,step=oracle.empty_lock_specification(case);assert step=='attempt'
    first=name=='retirement-first';target,later=(3,1) if first else (2,2);mode='hold' if first else 'hold-after-complete'
    label=case['name'];config=output/Path(case['config']).name;proxy=actor=None;sessions=[]
    schedule=dict(status='INCOMPLETE',actor=None,blocks=[],confirmed_blocks=[],pre_age=[],expiry=[],signal=None,
        end_before_us=None,end_after_us=None,cleanup_errors=[])
    record=dict(status='INCOMPLETE',mode=name,wire=None,held=None,
        configuration=dict(path=case['config'],direct=None,proxied=None,restored=None),schedule=schedule,errors=[])
    def save():lab.write_json(output/(label+'-delivery.json'),record)
    def owned():
        observed,=json.loads(lab.docker('inspect',pg['postgres']).stdout)
        assert observed['State']['Running'] and observed['Config']['Labels'][lab.LABEL]==pg['owner']
    def callback(phase):
        nonlocal proxy,actor
        try:
            if phase=='before-launch':
                actor=Session(pg,case['database'],label.replace('-','_')+'_actor',sessions,role='bigbrotr_control')
                schedule['actor']=dict(backend=actor.backend,identity=session_identity(actor),begin=None,transaction_identity=None,
                    statement=None,result=None,end_statement=None,end=None,closed=None)
                schedule['actor']['begin']=actor.execute('BEGIN ISOLATION LEVEL READ COMMITTED;')
                schedule['actor']['transaction_identity']=session_identity(actor);save()
                command=['docker','exec','-i',client,'python3','-B','/lab/fault_proxy.py','--upstream-port','5432','--listen-port','0',
                    '--mode',mode,'--commit-number',str(target),'--later-commits',str(later),'--deadline-seconds','30']
                record['wire']=dict(command=command,source_sha256=history.digest(output/'fault_proxy.py'),ready=None,receipt=None,events=[],stdout='',stderr='',exit_code=None,readers_joined=False,overflow=False,forced_wrapper_stop=False)
                save();proxy=Process(command,input_pipe=True);ready=proxy.event('ready',5);record['wire']['ready']=ready
                assert ready['proxy_sha256']==record['wire']['source_sha256']
                direct=config.read_text();oracle.empty_lock_config(tomllib.loads(direct),case)
                proxied=history.set_value(direct,'postgres.control','endpoint','127.0.0.1:'+str(ready['listen_port']))
                record['configuration'].update(direct=direct,proxied=proxied);save()
                (output/(label+'-direct.toml')).write_text(direct);(output/(label+'-proxied.toml')).write_text(proxied);config.write_text(proxied)
            elif phase=='launched':
                event=proxy.event('held',10)
                held=dict(event=event,before_us=history.server_clock(pg,case['database']),after_us=None,release_before_us=None,release_after_us=None,
                    control=schema.rows(pg,case['database']),inputs=history.inputs_at(directory/'store'),backend=None)
                record['held']=held;save()
                current=max((r for r in held['control']['owner_epochs'] if r['role']=='control'),key=lambda r:r['epoch'])
                control_backend=dict(pid=current['control_backend_pid'],backend_start=current['control_backend_start'],database=case['database'])
                statement="SELECT json_build_object('observed_at_us',(EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint,'pid',pid,'backend_start',backend_start,'database',datname,'user',usename,'state',state,'xact_start',xact_start,'query',query) FROM pg_stat_activity WHERE pid="+str(current['control_backend_pid'])+" AND datname=current_database() AND usename='bigbrotr_control';"
                held['backend']=json.loads(lab.sql(pg,case['database'],statement));held['after_us']=history.server_clock(pg,case['database']);save()
                window=dict(before_us=transition.read(output/(label+'-clock.json'))['before_us'],after_us=held['after_us'])
                m=oracle.model.Model(*expected,window);oracle.preparation_control(m,held['control'],success=False,renewed=True,lease_us=8_000_000 if name=='expiry' else 6_000_000)
                oracle.same(m.pg,held['control']);assert held['inputs']==expected[2]
                b=held['backend'];assert b['pid']==control_backend['pid'] and b['backend_start']==control_backend['backend_start']
                assert b['database']==case['database'] and b['user']=='bigbrotr_control'
                assert b['state']==('idle in transaction' if first else 'idle') and (b['xact_start'] is not None if first else b['xact_start'] is None)
                assert ' '.join(b['query'].split())==('SELECT control.require_empty_initial_retirement($1::bytea,$2::bytea)' if first else 'COMMIT')
                assert [event[k] for k in ('commits_seen','commits_forwarded','server_commit_complete','client_commit_complete')]==([3,2,2,2] if first else [2,2,2,1])
                a=schedule['actor']
                a['statement']=claim_statement(current,name in ('cancel','expiry'));save()
                if first:
                    actor.start(a['statement']);schedule['blocks']=block_observation(pg,case['database'],actor.backend,control_backend,timeout=.7);save()
                else:
                    a['result']=actor.execute(a['statement'],rows=1);save()
                if name=='expiry':
                    until=time.monotonic()+5
                    while True:
                        now=history.server_clock(pg,case['database']);remaining=current['lease_expires_at_us']-now
                        schedule['pre_age'].append(dict(server_us=now,remaining_us=remaining));save()
                        assert remaining>=3_500_000 and time.monotonic()<until,'refresh hold missed valid expiry dispatch window'
                        if remaining<=4_000_000:break
                        time.sleep(min(.1,(remaining-4_000_000)/1_000_000))
                held['release_before_us']=history.server_clock(pg,case['database'])
                assert held['release_before_us']<current['lease_expires_at_us']
                proxy.send('release');held['release_after_us']=history.server_clock(pg,case['database']);proxy.event('released',2);save()
                if first:a['result']=actor.expect(actor.finish(.7),rows=1)
                else:
                    schedule['blocks']=block_observation(pg,case['database'],control_backend,actor.backend,timeout=.7);save()
                if name=='expiry':
                    until=time.monotonic()+5
                    while True:
                        now=history.server_clock(pg,case['database']);schedule['expiry'].append(dict(server_us=now,remaining_us=current['lease_expires_at_us']-now));save()
                        if now>=current['lease_expires_at_us']+50_000:break
                        assert time.monotonic()<until,'UTC expiry not reached within final phase'
                        time.sleep(.02)
                    schedule['confirmed_blocks']=block_observation(pg,case['database'],control_backend,actor.backend,timeout=.5);save()
                if name=='cancel':
                    from installed import wait_for
                    identity=transition.read(output/(label+'.pid.json'))
                    signal=dict(kind='SIGTERM',identity=identity,before_us=history.server_clock(pg,case['database']),after_us=None,terminal=None,terminal_observed_us=None)
                    schedule['signal']=signal;save();installed_cycle.signal_child(client,identity)
                    signal['after_us']=history.server_clock(pg,case['database']);save()
                    wait_for((output/(label+'.terminal.json')).exists,'cancelled empty installed child joined while holder remains',5)
                    signal['terminal']=transition.read(output/(label+'.terminal.json'));signal['terminal_observed_us']=history.server_clock(pg,case['database'])
                    # The server may still be waiting after client/driver join.
                    # Roll back the holder next; final custody observes backend
                    # exclusion after actor/proxy closure, before native reopen.
                    save()
                schedule['end_before_us']=history.server_clock(pg,case['database'])
                a['end_statement']='COMMIT;' if name in ('claim-commit','retirement-first') else 'ROLLBACK;'
                a['end']=actor.execute(a['end_statement']);schedule['end_after_us']=history.server_clock(pg,case['database'])
                schedule['status']='CAPTURED';save()
            else:
                assert phase=='child-joined'
                try:
                    for session in reversed(sessions):
                        try:
                            session.close(owned)
                            absent=installed_cycle.exact_absence(pg,case['database'],[session.backend])
                            schedule['actor']['closed']=dict(backend=session.backend,exit_code=session.process.returncode,absence=absent,pending=session.pending)
                        except BaseException as error:schedule['cleanup_errors'].append(dict(stage='actor-close',error=repr(error)))
                    assert proxy is not None
                    record['wire']['receipt']=proxy.event('receipt',5);assert proxy.wait(5)==0
                finally:
                    try:
                        if proxy is not None:
                            forced=proxy.process.poll() is None;proxy.stop();wire=record['wire']
                            wire.update(stdout=bytes(proxy.stdout).decode(errors='replace'),stderr=bytes(proxy.stderr).decode(errors='replace'),exit_code=proxy.process.returncode,
                                readers_joined=not any(t.is_alive() for t in proxy.threads),overflow=proxy.overflow,forced_wrapper_stop=forced)
                            wire['events']=[safe_event(json.loads(v)) for v in wire['stdout'].splitlines()]
                    finally:
                        cfg=record['configuration']
                        if cfg['proxied'] is not None:
                            assert config.read_text()==cfg['proxied'];config.write_text(cfg['direct']);cfg['restored']=config.read_text();(output/(label+'-restored.toml')).write_text(cfg['restored'])
                        save()
                t=transition.read(output/(label+'.terminal.json'));assert t['reaped'] is True and t['forced'] is False and t['error'] is None
                assert schedule['status']=='CAPTURED' and schedule['cleanup_errors']==[] and record['errors']==[]
                assert record['wire']['readers_joined'] and not record['wire']['overflow'] and not record['wire']['forced_wrapper_stop']
                assert record['wire']['stderr']=='' and cfg['direct']==cfg['restored']
                record['status']='JOINED_CONFIG_RESTORED';save()
            return record
        except BaseException as error:
            record['errors'].append(dict(phase=phase,error=repr(error)));save()
            if phase!='child-joined' and proxy is not None and proxy.process.poll() is None:
                try:proxy.send('close')
                except BaseException:pass
            raise
    return callback


def empty_shape_corruption(pg,output,database,label,expected,initial,name):
    import subprocess
    statement=oracle.empty_shape_actor_sql('before')+oracle.empty_shape_corruption_sql(name,expected,initial)+oracle.empty_shape_actor_sql('after')
    record=dict(kind='privileged-isolated-empty-'+name,database=database,before=expected,after=None,statement=statement,
        response=None,before_us=history.server_clock(pg,database),after_us=None)
    def save():lab.write_json(output/(label+'.json'),record)
    save();(output/(label+'.sql')).write_text(statement)
    try:
        response=lab.docker('exec','-i',pg['postgres'],'psql','-X','-A','-t','-q','--no-password','-v','ON_ERROR_STOP=1',
            '-U','postgres','-d',database,data=statement,check=False,timeout=10)
        record['response']=dict(returncode=response.returncode,stdout=response.stdout,stderr=response.stderr);save()
        assert response.returncode==0 and response.stderr==''
        record['after']=schema.rows(pg,database);record['after_us']=history.server_clock(pg,database);save()
        oracle.empty_shape_corruption_checked(record,expected,initial,name,
            dict(before_us=record['before_us'],after_us=record['after_us']),database)
        return record
    except subprocess.TimeoutExpired as error:
        def text(value):return value.decode(errors='replace') if isinstance(value,bytes) else value or ''
        record['response']=dict(returncode=None,stdout=text(error.stdout),stderr=text(error.stderr),outcome='UNKNOWN_TIMEOUT')
        raise
    finally:save()


def empty_shape_snapshot(pg,client,output,clone,directory,expected,*,prepared=None):
    restore=prepared is not None;label='proof-baseline-'+('restore-native' if restore else 'preserve-native')
    window=dict(before_us=history.server_clock(pg,clone['database']),after_us=None)
    transition.observe_origin_absence(pg,clone['database'],output,label)
    before=dict(control=schema.rows(pg,clone['database']),native=history.dump(client,output,label+'-before',directory),inputs=history.inputs_at(directory/'store'))
    oracle.same(before['control'],expected[0]);assert (before['native'],before['inputs'])==expected[1:]
    transition.observe_origin_absence(pg,clone['database'],output,label+'-before-copy')
    store=directory/'store';snapshot=directory/'prepared-native-snapshot';terminal=directory/'terminal-native-preserved'
    record=dict(kind='isolated-older-native-paired-with-genuine-terminal-sql' if restore else 'excluded-independent-prepared-native-copy',
        before=before,after=None,source_files=history.census(store),retained_files=None,
        prepared_path='proof-baseline/prepared-native-snapshot',terminal_path='proof-baseline/terminal-native-preserved',server_window=window)
    if restore:record.update(prepared_files=history.census(snapshot),restored_files=None)
    def save():lab.write_json(output/(label+'.json'),record)
    save()
    if restore:
        assert record['prepared_files']==prepared['retained_files'] and not terminal.exists() and not terminal.is_symlink()
        store.rename(terminal);record['retained_files']=history.census(terminal);save()
        shutil.copytree(snapshot,store);record['restored_files']=history.census(store)
        oracle.empty_shape_copy_checked(record['prepared_files'],record['restored_files'])
    else:
        assert not snapshot.exists() and not snapshot.is_symlink()
        shutil.copytree(store,snapshot);record['retained_files']=history.census(snapshot)
        oracle.empty_shape_copy_checked(record['source_files'],record['retained_files'])
    save();transition.observe_origin_absence(pg,clone['database'],output,label+'-after-copy')
    record['after']=dict(control=schema.rows(pg,clone['database']),native=history.dump(client,output,label+'-after',directory),inputs=history.inputs_at(store))
    assert history.census(snapshot)==(prepared['retained_files'] if restore else record['retained_files'])
    if restore:assert history.census(terminal)==record['retained_files']
    window['after_us']=history.server_clock(pg,clone['database'])
    save();controls=oracle.empty_shape_fixture_controls(record,expected,prepared=prepared)
    lab.write_json(output/(label+'-controls.json'),controls);clone.setdefault('shape_controls',{})[label]=controls
    return oracle.empty_shape_snapshot_checked(record,expected,restore=restore,prepared=prepared),record


def empty_shape_missing(pg,client,output,clone,directory,expected,*,malformed=False):
    assert type(malformed) is bool
    name='malformed-floor' if malformed else 'missing-handoff';assert clone['name']==name
    mutation='empty-compacted-malformed-floor' if malformed else name
    label=name+'-mutation';window=dict(before_us=history.server_clock(pg,clone['database']),after_us=None)
    transition.observe_origin_absence(pg,clone['database'],output,label)
    before=dict(control=schema.rows(pg,clone['database']),native=history.dump(client,output,label+'-before',directory),inputs=history.inputs_at(directory/'store'))
    oracle.same(before['control'],expected[0]);assert (before['native'],before['inputs'])==expected[1:]
    record=dict(kind='isolated-empty-'+name,before=before,after=None,response=None,
        arguments=['history-corrupt','/lab/'+name+'/store',mutation],server_window=window)
    lab.write_json(output/(label+'.json'),record)
    response,_=history.capture(client,output,label,*record['arguments'])
    record['response']=dict(returncode=response.returncode,stdout=response.stdout,stderr=response.stderr);lab.write_json(output/(label+'.json'),record)
    assert response.returncode==0 and response.stderr==''
    transition.observe_origin_absence(pg,clone['database'],output,label+'-after-mutation')
    record['after']=dict(control=schema.rows(pg,clone['database']),native=history.dump(client,output,label+'-after',directory),inputs=history.inputs_at(directory/'store'))
    window['after_us']=history.server_clock(pg,clone['database'])
    lab.write_json(output/(label+'.json'),record)
    controls=oracle.empty_shape_fixture_controls(record,expected)
    lab.write_json(output/(label+'-controls.json'),controls);clone.setdefault('shape_controls',{})[label]=controls
    return oracle.empty_shape_missing_checked(record,expected,malformed=malformed)


def run_empty_sequence(pg,client,output,clone,directory,expected,report,*,origins,initial,delivery=False,locks=False,shapes=False,malformed=False):
    from initial_empty import guarded_fault
    from initial_gap_policy import GUARDS
    name=clone['name']; origin='empty-compacted' if delivery or locks or shapes or malformed else oracle.EMPTY_ORIGIN_NAMES[name]
    assert clone['origin']==origin and expected==origins[origin]
    assert clone['grants']==[dict(statement=s,stdout='') for s in oracle.EMPTY_GRANTS]
    oracle.history_catalog_checked(report['origins'][origin]['catalog'],clone['catalog_before'],grants=oracle.EMPTY_GRANTS)
    transition.observe_origin_absence(pg,clone['database'],output,name+'-logical-origin')
    assert history.dump(client,output,name+'-logical-origin',directory)==expected[1] and history.inputs_at(directory/'store')==expected[2]
    helper=Path(__file__).with_name('installed_cycle_client.py'); target=output/helper.name
    if not target.exists(): shutil.copy2(helper,target)
    assert history.digest(helper)==history.digest(target)
    if delivery or locks or shapes:
        source=history.SOURCE/oracle.ACTIVATED_PROXY; target=output/'fault_proxy.py'
        if not target.exists(): shutil.copy2(source,target)
        assert history.digest(target)==history.digest(source)
    configure(pg,client,output,clone['config'],ack=True)
    if locks:
        from initial_empty_locks import setup_import
        path=output/Path(clone['config']).name
        if name=='expiry':
            text=path.read_text()
            for key,value in (('lease_ttl','8s'),('renewal_timeout','5s'),('renewal_safety_margin','1s')):text=history.set_value(text,'runtime',key,value)
            path.write_text(text)
        answer=lab.cli(dict(pg,client=client),'config','validate','--file',clone['config'])
        lab.write_json(output/(name+'-lock-config-validation.json'),dict(returncode=answer.returncode,stdout=answer.stdout,stderr=answer.stderr,configuration=path.read_text()))
        assert answer.returncode==0 and answer.stderr==''
        oracle.empty_lock_config(tomllib.loads(path.read_text()),dict(clone=name,config=clone['config'],database=clone['database']))
        if name in ('claim-commit','claim-rollback','retirement-first'):
            def owned():
                observed,=json.loads(lab.docker('inspect',pg['postgres']).stdout)
                assert observed['State']['Running'] and observed['Config']['Labels'][lab.LABEL]==pg['owner']
            setup=setup_import(pg,clone['database'],output,name,lambda database:schema.rows(pg,database),owned)
            setup['actor_absence']=installed_cycle.exact_absence(pg,clone['database'],[setup['backend']])
            lab.write_json(output/(name+'-setup.json'),setup)
            clone['setup_controls']=oracle.empty_lock_setup_controls(setup,expected)
            expected=oracle.empty_lock_setup_checked(setup,expected)
            assert history.catalog(pg,clone['database'])==clone['catalog_before']
    if name=='original': expected=empty_restore(pg,client,output,clone,directory,expected,initial)
    if name in ('head','target-input'): expected=journey_mutation(pg,client,output,clone,directory,expected,initial)
    if name=='digest': expected=empty_digest_mutation(pg,client,output,clone,directory,expected,initial)
    prepared_snapshot=None
    if malformed:expected=empty_shape_missing(pg,client,output,clone,directory,expected,malformed=True)
    if shapes:
        if name=='proof-baseline':expected,prepared_snapshot=empty_shape_snapshot(pg,client,output,clone,directory,expected)
        elif name=='missing-handoff':expected=empty_shape_missing(pg,client,output,clone,directory,expected)
        elif name=='partial-sql':
            label=name+'-mutation';transition.observe_origin_absence(pg,clone['database'],output,label)
            mutation=empty_shape_corruption(pg,output,clone['database'],label,expected[0],initial,name)
            controls=oracle.empty_shape_fixture_controls(mutation,expected,initial=initial,database=clone['database'])
            lab.write_json(output/(label+'-controls.json'),controls);clone.setdefault('shape_controls',{})[label]=controls
            expected=mutation['after'],expected[1],expected[2]
    predecessor=None; fault=None
    if name=='native-first':
        original_guards=json.loads(lab.sql(pg,clone['database'],'SELECT '+GUARDS))
        guarded_fault(pg,clone['database'],output,name)
        installed=transition.read(output/(name+'-fault-installed.json'))
        fault=dict(installed=installed,removed=None,hit=None,guards_original=original_guards,
            catalog_original=clone['catalog_before'],catalog_installed=history.catalog(pg,clone['database']),catalog_restored=None)
        oracle.empty_fault_guard_delta(fault)
        oracle.same(schema.rows(pg,clone['database']),expected[0])
        lab.write_json(output/(name+'-fault.json'),fault)
    for step in (oracle.EMPTY_MALFORMED_STEPS if malformed else oracle.EMPTY_SHAPE_STEPS if shapes else oracle.EMPTY_LOCK_STEPS if locks else oracle.EMPTY_DELIVERY_STEPS if delivery else oracle.EMPTY_STEPS)[name]:
        if shapes and step=='native-open':expected,_=empty_shape_snapshot(pg,client,output,clone,directory,expected,prepared=prepared_snapshot)
        if step=='complete':
            restored=guarded_fault(pg,clone['database'],output,name,remove=True)
            assert restored==fault['guards_original']
            fault['removed']=transition.read(output/(name+'-fault-removed.json'))
            fault['catalog_restored']=history.catalog(pg,clone['database']); assert fault['catalog_restored']==clone['catalog_before']
            lab.write_json(output/(name+'-fault.json'),fault)
        case=dict(name=name+'-'+step,clone=name,origin=origin,operation='retire-empty',predecessor=predecessor,
            database=clone['database'],config=clone['config'],status='RUNNING')
        report['cases'].append(case); lab.write_json(output/'result.json',report)
        catalog=fault['catalog_installed'] if step=='failure' else clone['catalog_before']
        baseline=None
        if delivery and step=='loss':
            baseline=oracle.only(report['cases'],name='baseline-baseline')['oracle']; assert baseline['status']=='PASS'
        if shapes and name in ('proof-head','proof-digest'):
            baseline=oracle.only(report['cases'],name='proof-baseline-proof')['oracle'];assert baseline['status']=='PASS'
        hook=terminal_delivery_hook(pg,client,output,case,directory,empty_proof=True,expected=expected,initial=initial) if shapes and step=='proof' else empty_lock_hook(pg,client,output,case,directory,expected) if locks and step=='attempt' else terminal_delivery_hook(pg,client,output,case,directory,empty_initial=True,expected=expected) if delivery and step in ('baseline','loss') else None
        def verify(c,s):
            if locks:
                c['empty_configuration']=(output/Path(case['config']).name).read_text()
                if name=='claim-commit':
                    from initial_empty_locks import claim_guard
                    current=max((r for r in c['after']['owner_epochs'] if r['role']=='control'),key=lambda r:r['epoch'])
                    backend=dict(pid=current['control_backend_pid'],backend_start=current['control_backend_start'],database=case['database'])
                    c['empty_claim_guard']=claim_guard(pg,backend,c['server_window']);lab.write_json(output/(case['name']+'-claim-guard.json'),c['empty_claim_guard'])
                lab.write_json(output/(case['name']+'-verification.json'),c)
            if fault is not None:
                if step=='failure':
                    fault['hit']=lab.sql(pg,clone['database'],'SELECT last_value::text||is_called::text FROM empty_retirement_fixture.source_hit')
                    lab.write_json(output/(name+'-fault.json'),fault)
                c['empty_fault']=json.loads(json.dumps(fault)); lab.write_json(output/(case['name']+'-verification.json'),c)
            return oracle.verify_empty_context(c,s,expected,delivery=delivery,baseline=baseline,locks=locks,shapes=shapes,malformed=malformed)
        expected=installed_cycle.execute(pg,client,output,case,directory,expected,catalog,initial,
            checks=(verify,lambda c,s:oracle.empty_controls(c,s,expected,delivery=delivery,baseline=baseline,locks=locks,shapes=shapes,malformed=malformed),partial(oracle.empty_closed,delivery=delivery,locks=locks,shapes=shapes,malformed=malformed)),fault=hook)
        case['catalog_after']=history.catalog(pg,clone['database']); assert case['catalog_after']==catalog
        predecessor=case['name']; lab.write_json(output/'result.json',report)
    assert history.catalog(pg,clone['database'])==clone['catalog_before']
    if shapes and name=='proof-baseline':
        restored=transition.read(output/'proof-baseline-restore-native.json')
        assert history.census(directory/'prepared-native-snapshot')==prepared_snapshot['retained_files']
        assert history.census(directory/'terminal-native-preserved')==restored['retained_files']
    return predecessor


def run_empty_preparer_sequence(pg,client,output,clone,directory,expected,report,*,endpoint,initial):
    assert expected==endpoint and clone['origin']=='compacted-initial' and clone['name']=='preparer'
    oracle.history_catalog_checked(report['origins']['compacted-initial']['catalog'],clone['catalog_before'],grants=oracle.EMPTY_GRANTS)
    assert clone['grants']==[dict(statement=s,stdout='') for s in oracle.EMPTY_GRANTS]
    transition.observe_origin_absence(pg,clone['database'],output,'preparer-logical-origin')
    native=history.dump(client,output,'preparer-logical-origin',directory)
    assert native==endpoint[1] and (len(native['rows']),native['sequence'])==(81,121)
    assert history.inputs_at(directory/'store')==endpoint[2]=={}
    helper=Path(__file__).with_name('installed_cycle_client.py'); target=output/helper.name
    if not target.exists(): shutil.copy2(helper,target)
    assert history.digest(helper)==history.digest(target)
    configure(pg,client,output,clone['config'],ack=True)
    setup=empty_preparer_setup(pg,output,clone,directory,expected)
    clone['setup_controls']=oracle.empty_preparer_setup_controls(setup,expected)
    expected=oracle.empty_preparer_setup_checked(setup,expected)
    predecessor=None
    for step in oracle.EMPTY_PREPARER_STEPS['preparer']:
        operation={'prepare-gap':oracle.JOURNEY_GAP_MODE,'complete':'complete-initial','refuse':'retire-empty'}[step]
        case=dict(name='preparer-'+step,clone='preparer',origin='compacted-initial',operation=operation,
            predecessor=predecessor,database=clone['database'],config=clone['config'],status='RUNNING')
        report['cases'].append(case); lab.write_json(output/'result.json',report)
        if step=='prepare-gap':
            expected=journey_probe(pg,client,output,case,directory,expected,clone['catalog_before'],initial,preparer=True)
        else:
            def verify(c,s):
                if step=='refuse':
                    from initial_empty_locks import claim_guard
                    current=max((r for r in c['after']['owner_epochs'] if r['role']=='control'),key=lambda r:r['epoch'])
                    backend=dict(pid=current['control_backend_pid'],backend_start=current['control_backend_start'],database=case['database'])
                    c['empty_claim_guard']=claim_guard(pg,backend,c['server_window'])
                    lab.write_json(output/(case['name']+'-claim-guard.json'),c['empty_claim_guard'])
                    lab.write_json(output/(case['name']+'-verification.json'),c)
                return oracle.verify_empty_preparer_context(c,s,expected)
            closed=partial(oracle.empty_closed if step=='refuse' else oracle.preparation_closed,preparer=True)
            expected=installed_cycle.execute(pg,client,output,case,directory,expected,clone['catalog_before'],initial,
                checks=(verify,lambda c,s:oracle.empty_preparer_controls(c,s,expected),closed))
        case['catalog_after']=history.catalog(pg,clone['database']); assert case['catalog_after']==clone['catalog_before']
        predecessor=case['name']; lab.write_json(output/'result.json',report)
    return predecessor


def run_empty_preparer(root,build,product_source):
    assert product_source==oracle.EMPTY_PREPARER_PRODUCT
    endpoint,initial=oracle.journey_origin_checked(root); profile=oracle.EmptyInitialPreparerProfile
    suite=transition.Suite(directory=profile.DIRECTORY,label=profile.LABEL,status=profile.STATUS,client_prefix=profile.CLIENT_PREFIX,
        database_prefix=profile.DATABASE_PREFIX,clones=profile.CLONES,cases=profile.CASES,not_run=profile.NOT_RUN,
        scope='Actual compacted preparation gap, ordinary original-preparer import claim, fresh installed completion and named empty-retirement veto')
    try:
        return transition.run(root,build,suite=suite,sequence=partial(run_empty_preparer_sequence,endpoint=endpoint,initial=initial),
            origin_specs=(oracle.JOURNEY_ORIGIN,),origin_for_clone={'preparer':'compacted-initial'},extra_grants=oracle.EMPTY_GRANTS,
            producer_progress=True,producer_ack=True,preparation_hole=True,compacted_reader=True,compacted_initial=True,
            empty_initial=True,product_source=product_source)
    finally: assert oracle.journey_origin_checked(root)==(endpoint,initial)


def run_empty_initial(root,build,product_source,*,delivery=False,locks=False,shapes=False,malformed=False):
    assert type(malformed) is bool and not (malformed and (delivery or locks or shapes))
    if malformed:assert product_source==oracle.EMPTY_MALFORMED_PRODUCT
    assert type(shapes) is bool and not (shapes and (locks or delivery))
    assert type(locks) is bool and not (locks and delivery)
    origins,initial=oracle.empty_origins_checked(root,delivery=delivery or locks or shapes or malformed); profile=oracle.EmptyInitialMalformedProfile if malformed else oracle.EmptyInitialShapeProfile if shapes else oracle.EmptyInitialLockProfile if locks else oracle.EmptyInitialDeliveryProfile if delivery else oracle.EmptyInitialProfile
    suite=transition.Suite(directory=profile.DIRECTORY,label=profile.LABEL,status=profile.STATUS,client_prefix=profile.CLIENT_PREFIX,
        database_prefix=profile.DATABASE_PREFIX,clones=profile.CLONES,cases=profile.CASES,not_run=profile.NOT_RUN,
        scope='One exact malformed compacted floor: fixture write, pre-Control installed refusal and complete preservation' if malformed else 'Compacted empty post-proof H/D and explicit partial/mixed-snapshot refusal shapes' if shapes else 'Compacted empty current-Control claim ordering, actual installed cancellation and DB-UTC expiry' if locks else 'Compacted empty initial final COMMIT measured baseline and before/after loss with fresh completion/retry' if delivery else 'Compacted and original empty initial target retirement, fresh retry, actual native-first SQL rollback continuation and selected refusals')
    try:
        return transition.run(root,build,suite=suite,sequence=partial(run_empty_sequence,origins=origins,initial=initial,delivery=delivery,locks=locks,shapes=shapes,malformed=malformed),
            origin_specs=oracle.EMPTY_PREPARED_ORIGINS[:1] if delivery or locks or shapes or malformed else oracle.EMPTY_ORIGINS,origin_for_clone={n:'empty-compacted' for n in profile.CLONES} if delivery or locks or shapes or malformed else oracle.EMPTY_ORIGIN_NAMES,extra_grants=oracle.EMPTY_GRANTS,
            producer_progress=True,producer_ack=True,preparation_hole=True,compacted_reader=True,compacted_initial=True,
            empty_initial=True,product_source=product_source)
    finally: assert oracle.empty_origins_checked(root,delivery=delivery or locks or shapes or malformed)==(origins,initial)


ORIGIN = oracle.ORIGIN
def later_prepare_invoke(pg,client,output,case,directory,expected,initial,baseline):
    from fault_cases import Process,safe_event
    from initial_empty_locks import Probe
    from activated_retirement_expiry import wait_event
    _,step=oracle.later_specification(case);normal=step=='prepare';label=case['name'];database=case['database']
    target=oracle.LATER_PREPARE_CANDIDATE if normal else baseline['prepare_commit']['ordinal']
    if not normal:assert baseline['status']=='PASS' and baseline['case']=='normal-prepare'
    mode='hold-after-complete' if normal else 'after-complete'
    fault=dict(mode=mode,target=target,held=None,prefix=None,wire=None,errors=[])
    probe=proxy=None;started=time.monotonic();window=dict(before_us=history.server_clock(pg,database))
    response=dict(returncode=None,stdout='',stderr='',seconds=None,outcome='UNKNOWN',readers_joined=False,overflow=False,command=None)
    args=None
    def save():
        lab.write_json(output/(label+'-wire.json'),fault)
        lab.write_json(output/(label+'-clock.json'),window)
    try:
        command=['docker','exec','-i',client,'python3','-B','/lab/fault_proxy.py','--upstream-port','5432','--listen-port','0',
            '--mode',mode,'--commit-number',str(target),'--later-commits','1' if normal else '0','--deadline-seconds','30']
        fault['wire']=dict(command=command,source_sha256=history.digest(output/'fault_proxy.py'),ready=None,receipt=None,
            events=[],stdout='',stderr='',exit_code=None,readers_joined=False,overflow=False,forced_wrapper_stop=False)
        save();proxy=Process(command,input_pipe=True);ready=proxy.event('ready',5);fault['wire']['ready']=ready
        assert ready['proxy_sha256']==fault['wire']['source_sha256'];case['proxy_port']=ready['listen_port']
        args=[case['operation'],database,'/run/secrets/application',case['config'],'262144',str(ready['listen_port'])]
        response['command']=['docker','exec',client,'/build/ingest_bootstrap_probe',*args]
        save();probe=Probe(response['command'])
        if normal:
            held=proxy.event('held',15)
            wait_event(probe,'lineage-history-state',stage='planned',timeout=5)
            capture=dict(event=held,control=None,inputs=None,backend=None,before_us=history.server_clock(pg,database),
                after_us=None,release_before_us=None,release_after_us=None)
            fault['held']=capture;save()
            capture['backend']=fault_backend_observation(pg,database,probe)
            capture['control']=schema.rows(pg,database);capture['inputs']=history.inputs_at(directory/'store')
            capture['after_us']=history.server_clock(pg,database);probe.observe();fault['prefix']=list(probe.parsed);save()
            # Establish the actual committed intent before forwarding its reply.
            # A source-predicted ordinal alone never authorizes the later loss.
            oracle.later_held_checked(capture,fault['prefix'],expected,initial,case,
                dict(before_us=window['before_us'],after_us=capture['after_us']))
            capture['release_before_us']=history.server_clock(pg,database)
            control=oracle.only(capture['control']['owner_epochs'],role='control',state='active')
            assert capture['release_before_us']<control['lease_expires_at_us']
            proxy.send('release');capture['release_after_us']=history.server_clock(pg,database)
            proxy.event('released',5);assert capture['release_after_us']<control['lease_expires_at_us'];save()
        code=probe.wait(15);probe.observe()
        response.update(returncode=code,outcome='PROCESS_EXITED',readers_joined=not any(t.is_alive() for t in probe.threads),overflow=probe.overflow)
        if not normal:proxy.event('fault',5)
        fault['wire']['receipt']=proxy.event('receipt',5);assert proxy.wait(5)==0
    except BaseException as error:
        fault['errors'].append(dict(stage='invoke',error=repr(error)))
    finally:
        if probe is not None:
            code=probe.process.poll()
            if code is not None:
                for thread in probe.threads:thread.join(1)
                response.update(returncode=code,outcome='PROCESS_EXITED',readers_joined=not any(t.is_alive() for t in probe.threads),overflow=probe.overflow)
            response.update(stdout=bytes(probe.stdout).decode(errors='replace'),stderr=bytes(probe.stderr).decode(errors='replace'))
            if code is None:fault['errors'].append(dict(stage='probe-stop',error='forced-wrapper-stop'))
            probe.stop()
        if proxy is not None:
            forced=proxy.process.poll() is None;proxy.stop();wire=fault['wire']
            wire.update(stdout=bytes(proxy.stdout).decode(errors='replace'),stderr=bytes(proxy.stderr).decode(errors='replace'),exit_code=proxy.process.returncode,
                forced_wrapper_stop=forced,readers_joined=not any(t.is_alive() for t in proxy.threads),overflow=proxy.overflow)
            try:wire['events']=[safe_event(json.loads(v)) for v in wire['stdout'].splitlines()]
            except BaseException as error:fault['errors'].append(dict(stage='proxy-metadata',error=repr(error)))
        response['seconds']=time.monotonic()-started;window['after_us']=history.server_clock(pg,database)
        lab.write_json(output/(label+'-response.json'),response);save()
    return response,window,fault,args


def later_prepare(pg,client,output,case,directory,expected,catalog,initial,baseline):
    label=case['name'];before=schema.rows(pg,case['database']);native=history.dump(client,output,label+'-before',directory)
    inputs=history.inputs_at(directory/'store');oracle.same(before,expected[0]);assert (native,inputs)==expected[1:]
    lab.write_json(output/(label+'-before.json'),before);lab.write_json(output/(label+'-inputs.json'),inputs)
    response,window,fault,args=later_prepare_invoke(pg,client,output,case,directory,expected,initial,baseline)
    wire=fault['wire'] or {}
    closed=not fault['errors'] and wire.get('exit_code')==0 and wire.get('readers_joined') is True and wire.get('forced_wrapper_stop') is False and wire.get('overflow') is False and wire.get('stderr')==''
    events,after,after_native,after_inputs=transition.external(pg,client,output,case,directory,response,
        control_failed=case['clone']=='gap',proxy_closed=closed)
    context=dict(case=dict(case),before=before,after=after,native_before=native,native_after=after_native,inputs_before=inputs,inputs_after=after_inputs,
        response=response,server_window=window,catalog_before=catalog,catalog_after=history.catalog(pg,case['database']),
        backend_absence=transition.read(output/(label+'-control-backends.json')),backends=transition.read(output/(label+'-backends.json')),
        custody=transition.read(output/(label+'-custody.json')),arguments=args,wire=fault)
    lab.write_json(output/(label+'-verification.json'),context)
    result=oracle.verify_later_probe(context,initial,expected,baseline)
    controls=oracle.later_controls(context,initial,expected,baseline)
    case.update(status='PASS',returncode=response['returncode'],oracle=result,negative_controls=controls,server_window=window)
    return after,after_native,after_inputs


def later_sql_fault(pg,database,output,record=None):
    from installed_empty_oracle import FAULT_INSTALL,FAULT_REMOVE
    from initial_empty_locks import guards
    remove=record is not None
    if not remove:
        record=dict(installed=None,removed=None,hit=None,guards_original=guards(pg,database),catalog_original=history.catalog(pg,database),
            catalog_installed=None,catalog_restored=None)
    statement=FAULT_REMOVE if remove else FAULT_INSTALL.replace('NEW.owner_epoch=2','NEW.owner_epoch=3')
    hit=lab.sql(pg,database,'SELECT last_value::text||is_called::text FROM empty_retirement_fixture.source_hit') if remove else None
    if remove:assert hit==record['hit']=='1true'
    lab.sql(pg,database,statement)
    if not remove:assert lab.sql(pg,database,'SELECT is_called FROM empty_retirement_fixture.source_hit')=='f'
    value=dict(statement=statement,hit=hit,guards=guards(pg,database))
    record['removed' if remove else 'installed']=value
    record['catalog_restored' if remove else 'catalog_installed']=history.catalog(pg,database)
    lab.write_json(output/'closing-late-sql.json',record);oracle.empty_fault_guard_delta(record,epoch=3)
    return record


def run_later_sequence(pg,client,output,clone,directory,expected,report,*,seeds,initial):
    name=clone['name'];assert expected==seeds[clone['origin']]
    assert clone['grants']==[dict(statement=s,stdout='') for s in oracle.HISTORY_GRANTS]
    assert clone['catalog_before']==report['origins'][clone['origin']]['catalog']
    configure(pg,client,output,clone['config'],ack=True)
    for source in (Path(__file__).with_name('installed_cycle_client.py'),history.SOURCE/'experiments/control-migrations/fault_proxy.py'):
        path=output/source.name
        if not path.exists():shutil.copy2(source,path)
        assert history.digest(source)==history.digest(path)
    previous=None;fault=None
    for step in oracle.LATER_STEPS[name]:
        if step=='failure':fault=later_sql_fault(pg,clone['database'],output)
        case=dict(name=name+'-'+step,clone=name,origin=clone['origin'],operation='successor-compacted-prepare' if step.startswith('prepare') else 'retire-empty-successor',
            predecessor=previous,database=clone['database'],config=clone['config'],status='RUNNING')
        report['cases'].append(case);lab.write_json(output/'result.json',report)
        if step.startswith('prepare'):
            baseline=oracle.only(report['cases'],name='normal-prepare')['oracle'] if name=='gap' else None
            expected=later_prepare(pg,client,output,case,directory,expected,clone['catalog_before'],initial,baseline)
        else:
            def verify(context,_seed):
                if name=='closing':
                    if step=='failure':
                        fault['hit']=lab.sql(pg,clone['database'],'SELECT last_value::text||is_called::text FROM empty_retirement_fixture.source_hit')
                        assert fault['hit']=='1true';lab.write_json(output/'closing-late-sql.json',fault)
                    context['later_fault']=json.loads(json.dumps(fault));lab.write_json(output/(case['name']+'-verification.json'),context)
                return oracle.verify_later_installed(context,initial,expected)
            expected=installed_cycle.execute(pg,client,output,case,directory,expected,history.catalog(pg,clone['database']),initial,
                checks=(verify,lambda c,s:oracle.later_controls(c,initial,expected),oracle.later_closed))
            if step=='failure':fault=later_sql_fault(pg,clone['database'],output,fault)
        case['catalog_after']=history.catalog(pg,clone['database']);assert case['catalog_after']==clone['catalog_before']
        previous=case['name'];lab.write_json(output/'result.json',report)
    return previous


def run_later_empty(root,build,product_source):
    assert product_source==oracle.LATER_PRODUCT and oracle.LATER_PRODUCT is not None
    seeds,initial=oracle.later_origins_checked(root)
    profile=oracle.LaterEmptyProfile
    suite=transition.Suite(directory=profile.DIRECTORY,label=profile.LABEL,status=profile.STATUS,
        client_prefix='bigbrotr-compacted-later-',database_prefix=profile.DATABASE_PREFIX,clones=profile.CLONES,cases=profile.CASES,
        scope='Actual compacted later preparation, measured preparation COMMIT loss, installed OPEN/gap/CLOSING empty recovery and exact terminal retry',not_run=profile.NOT_RUN)
    return transition.run(root,build,suite=suite,sequence=partial(run_later_sequence,seeds=seeds,initial=initial),origin_specs=oracle.LATER_ORIGINS,
        origin_for_clone=oracle.LATER_ORIGIN_NAMES,extra_grants=profile.GRANTS,producer_progress=True,producer_ack=True,preparation_hole=True,
        compacted_reader=True,compacted_history=True,later_empty=True,product_source=product_source)



def later_live_inputs(output,directory,expected,initial):
    name=directory.name; spool=directory/'store/producer-inputs-v1'; quarantine=directory/'quarantined-initial'
    attribution=directory/'terminal-seed-inputs'; quarantine.mkdir(mode=0o700); attribution.mkdir(mode=0o700)
    before=history.census(spool);original=sorted(initial[2])
    keep=original if name=='full' else original[:1] if name=='partial' else []
    removed=sorted(set(original)-set(keep))
    filename='36a0253e6d94c18099f6c4dae9b7e040-0000000000000001-0000000000000005.cbor'
    raw=oracle.retained.regular_read(spool/filename)
    fd=os.open(attribution/filename,os.O_CREAT|os.O_EXCL|os.O_WRONLY|os.O_NOFOLLOW,0o600)
    try:
        os.fchmod(fd,0o600)
        with os.fdopen(fd,'wb',closefd=False) as stream:stream.write(raw);stream.flush();os.fsync(fd)
    finally:os.close(fd)
    files={n:dict(hex=oracle.retained.regular_read(spool/n).hex(),metadata=before[n]) for n in removed}
    for n in removed:(spool/n).rename(quarantine/n)
    for p in (spool,quarantine,attribution,directory):
        fd=os.open(p,os.O_RDONLY|os.O_DIRECTORY)
        try:os.fsync(fd)
        finally:os.close(fd)
    record=dict(kind='reversible-later-live-residual-selection',clone=name,before=before,after=history.census(spool),
        quarantine=history.census(quarantine),files=files,attribution=dict(name=filename,hex=raw.hex(),metadata=history.census(attribution)[filename]))
    lab.write_json(output/(name+'-residual-selection.json'),record)
    wanted=oracle.later_live_inputs_checked(record,expected,initial,name)
    assert history.inputs_at(directory/'store')==wanted[2]
    return wanted


def later_live_probe(pg,client,output,case,directory,expected,catalog,initial):
    name=case['name'];database=case['database']
    before=schema.rows(pg,database);oracle.same(before,expected[0]);lab.write_json(output/(name+'-before.json'),before)
    native=history.dump(client,output,name+'-before',directory);inputs=history.inputs_at(directory/'store')
    assert (native,inputs)==expected[1:];lab.write_json(output/(name+'-inputs.json'),inputs)
    window=dict(before_us=history.server_clock(pg,database));lab.write_json(output/(name+'-clock.json'),window)
    args=['successor-compacted-live',database,'/run/secrets/application',case['config'],'262144']
    response=dict(returncode=None,stdout='',stderr='',outcome='UNKNOWN');errors=[]
    try:history.capture(client,output,name,*args)
    except BaseException as error:errors.append(dict(stage='capture',error=repr(error)))
    finally:
        path=output/(name+'-response.json')
        if path.exists():
            try:
                response=transition.read(path);assert isinstance(response,dict)
            except BaseException as error:errors.append(dict(stage='response',error=repr(error)))
        else:lab.write_json(path,response)
        try:window['after_us']=history.server_clock(pg,database)
        except BaseException as error:errors.append(dict(stage='clock',error=repr(error)))
        lab.write_json(output/(name+'-capture-errors.json'),errors);lab.write_json(output/(name+'-clock.json'),window)
        events,after,after_native,after_inputs=transition.external(pg,client,output,case,directory,response,completion_event='lineage-live-complete')
    assert not errors
    context=dict(case=dict(case),before=before,after=after,native_before=native,native_after=after_native,inputs_before=inputs,inputs_after=after_inputs,
        response=response,server_window=window,catalog_before=catalog,catalog_after=history.catalog(pg,database),
        backend_absence=transition.read(output/(name+'-control-backends.json')),backends=transition.read(output/(name+'-backends.json')),
        custody=transition.read(output/(name+'-custody.json')),arguments=args)
    lab.write_json(output/(name+'-verification.json'),context)
    result=oracle.verify_later_live(context,initial,expected);controls=oracle.later_live_controls(context,initial,expected)
    case.update(status='PASS',returncode=response['returncode'],oracle=result,negative_controls=controls,server_window=window)
    return after,after_native,after_inputs


def run_later_live_sequence(pg,client,output,clone,directory,expected,report,*,seed,initial):
    name=clone['name'];assert expected==seed and clone['origin']=='later-terminal'
    assert clone['grants']==[dict(statement=s,stdout='') for s in oracle.HISTORY_GRANTS]
    assert clone['catalog_before']==report['origins']['later-terminal']['catalog']
    configure(pg,client,output,clone['config'],ack=True)
    source=Path(__file__).with_name('installed_cycle_client.py');path=output/source.name
    if not path.exists():shutil.copy2(source,path)
    assert history.digest(path)==history.digest(source)
    transition.observe_origin_absence(pg,clone['database'],output,name+'-residual')
    oracle.same(schema.rows(pg,clone['database']),expected[0])
    assert history.dump(client,output,name+'-residual-before',directory)==expected[1]
    expected=later_live_inputs(output,directory,expected,initial)
    oracle.same(schema.rows(pg,clone['database']),expected[0])
    case=dict(name=name+'-live',clone=name,origin='later-terminal',operation='live' if name=='installed' else 'successor-compacted-live',
        predecessor=None,database=clone['database'],config=clone['config'],status='RUNNING')
    report['cases'].append(case);lab.write_json(output/'result.json',report)
    if name=='installed':
        from installed_cycle_replay import process_closed
        expected=installed_cycle.execute(pg,client,output,case,directory,expected,clone['catalog_before'],initial,
            checks=(lambda c,s:oracle.verify_later_live(c,initial,expected),lambda c,s:oracle.later_live_controls(c,initial,expected),process_closed))
    else:expected=later_live_probe(pg,client,output,case,directory,expected,clone['catalog_before'],initial)
    case['catalog_after']=history.catalog(pg,clone['database']);assert case['catalog_after']==clone['catalog_before']
    lab.write_json(output/'result.json',report);return case['name']


def run_later_live(root,build,product_source):
    assert product_source==oracle.LATER_LIVE_PRODUCT
    seed,initial=oracle.qualified_endpoint(root,None,initial_journey=False,later_terminal=True)
    profile=oracle.LaterLiveProfile
    suite=transition.Suite(directory=profile.DIRECTORY,label=profile.LABEL,status=profile.STATUS,
        client_prefix=profile.CLIENT_PREFIX,database_prefix=profile.DATABASE_PREFIX,clones=profile.CLONES,cases=profile.CASES,
        scope='Three finite compacted continuous live histories with optional residuals, plus independent installed quiet activation/renewal/join',not_run=profile.NOT_RUN)
    return transition.run(root,build,suite=suite,sequence=partial(run_later_live_sequence,seed=seed,initial=initial),
        origin_specs=(oracle.LATER_TERMINAL_ORIGIN,),origin_for_clone={n:'later-terminal' for n in profile.CLONES},extra_grants=profile.GRANTS,
        producer_progress=True,producer_ack=True,preparation_hole=True,compacted_reader=True,compacted_history=True,later_live=True,product_source=product_source)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('root',type=Path); parser.add_argument('build',type=Path)
    parser.add_argument('--product-source',required=True,help='Exact reviewed and qualified integrated build revision')
    profile=parser.add_mutually_exclusive_group()
    profile.add_argument('--later-live',action='store_true',help='Finite compacted continuous live plus separate installed quiet activation and renewal')
    profile.add_argument('--later-empty',action='store_true',help='Actual compacted later preparation and installed gap/OPEN/CLOSING retirement')
    profile.add_argument("--history",action="store_true",help="Explicit connected Terminal2/PreparedOpen3 synthetic floor profile")
    profile.add_argument('--history-faults',action='store_true',help='Exact retained PreparedOpen3 delivery fault profile')
    profile.add_argument('--initial-journey',action='store_true',help='Explicit compacted initial prepare/gap/activation and installed startup profile')
    profile.add_argument('--initial-preparation',action='store_true',help='Installed original/compacted existing initial preparation completion and exact retry')
    profile.add_argument('--initial-preparation-faults',action='store_true',help='Installed compacted completion wire baselines and before/after dispatch delivery loss')
    profile.add_argument('--activated',action='store_true',help='Installed compacted activated-initial recovery and ACK with real COMMIT loss seeds')
    profile.add_argument('--activated-terminal',action='store_true',help='Compacted activated source, real Event abandonment and retirement positive/refusal profile')
    profile.add_argument('--activated-terminal-faults',action='store_true',help='Separate compacted terminal delivery, real job-lock waits and final-proof drift profile')
    profile.add_argument('--activated-source-proof',action='store_true',help='Separate Source/abandon ancestor H/D after actual proof and confirmed server refresh')
    profile.add_argument('--activated-terminal-guards',action='store_true',help='Separate unresolved import, positive draining retirement and isolated native OPEN guard')
    profile.add_argument("--empty-initial",action="store_true",help="Prepared empty initial target retirement and native-first continuation")
    profile.add_argument('--empty-initial-faults',action='store_true',help='Compacted empty final COMMIT baseline, delivery loss and fresh retries')
    profile.add_argument('--empty-initial-malformed-floor',action='store_true',help='One exact malformed compacted floor refusal before Control claim')
    profile.add_argument('--empty-initial-proof-shapes',action='store_true',help='Compacted empty final proof checks and isolated partial/mixed snapshot refusals')
    profile.add_argument('--empty-initial-locks',action='store_true',help='Current-Control import ordering, real cancellation and database UTC expiry')
    profile.add_argument('--empty-initial-preparer',action='store_true',help='Actual original-preparer import claim through fresh installed completion and empty-retirement refusal')
    args = parser.parse_args()
    (run_later_live if args.later_live else run_later_empty if args.later_empty else run_empty_preparer if args.empty_initial_preparer else partial(run_empty_initial,malformed=True) if args.empty_initial_malformed_floor else partial(run_empty_initial,shapes=True) if args.empty_initial_proof_shapes else partial(run_empty_initial,locks=True) if args.empty_initial_locks else partial(run_empty_initial,delivery=True) if args.empty_initial_faults else run_empty_initial if args.empty_initial else partial(run_activated_terminal,guards=True) if args.activated_terminal_guards else partial(run_activated_terminal,post_proof=True) if args.activated_source_proof else partial(run_activated_terminal,faults=True) if args.activated_terminal_faults else run_activated_terminal if args.activated_terminal else run_activated if args.activated else run_initial_preparation_faults if args.initial_preparation_faults else run_initial_preparation if args.initial_preparation else run_initial_journey if args.initial_journey else run_history_faults if args.history_faults else run_history if args.history else run)(args.root.resolve(strict=True),args.build.resolve(strict=True),args.product_source)
