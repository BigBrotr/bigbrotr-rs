#!/usr/bin/env python3
"""Read and bind the synthetic installed reader's complete retained evidence.

No engine, subprocess or network is opened. Captured process/engine closure is
checked; the run owner's separately joined driver remains part of qualification.
"""
import argparse
import json
from pathlib import Path
import re
import stat
import sys
import tomllib

sys.dont_write_bytecode = True
import installed_compacted_reader_oracle as oracle
from installed_family_capture import inventory_checked, physical, spool_checked

loads,sha,same,only,number = oracle.loads,oracle.sha,oracle.same,oracle.only,oracle.number


def replay(root, product_source, *, history_profile=False,fault_profile=False,initial_journey=False,initial_preparation=False,initial_preparation_faults=False,activated=False,activated_terminal=False):
    assert all(type(v) is bool for v in (history_profile,fault_profile,initial_journey,initial_preparation,initial_preparation_faults,activated,activated_terminal))
    assert sum((history_profile,fault_profile,initial_journey,initial_preparation,initial_preparation_faults,activated,activated_terminal))<=1
    profile=oracle.ActivatedTerminalProfile if activated_terminal else oracle.ActivatedProfile if activated else oracle.InitialPreparationDeliveryProfile if initial_preparation_faults else oracle.InitialPreparationProfile if initial_preparation else oracle.InitialJourneyProfile if initial_journey else oracle.HistoryFaultProfile if fault_profile else oracle.HistoryProfile if history_profile else oracle
    if initial_preparation_faults: initial_preparation=True
    root = Path(root).resolve(strict=True); folder = root/profile.DIRECTORY; bound = {}
    def raw(path):
        path = Path(path); assert stat.S_ISREG(path.lstat().st_mode)
        value = path.read_bytes(); bound[str(path)] = sha(value); return value
    def read(path): return loads(raw(path))
    def absent(label,database):
        value = read(folder/(label+'-origin-backends.json'))
        assert set(value) == {'database','scope','limit_seconds','status','attempts','seconds'}
        assert value['database'] == database and value['scope'] == 'all-database-sessions'
        assert value['status'] == 'OBSERVED_ABSENT' and value['limit_seconds'] == 5 and value['attempts']
        elapsed = 0
        for index,item in enumerate(value['attempts']):
            assert set(item) == {'seconds','sessions'}
            elapsed = number(item['seconds'],elapsed,5)
            assert type(item['sessions']) is list and (item['sessions'] == []) == (index == len(value['attempts'])-1)
        number(value['seconds'],elapsed,6)
    report = read(folder/'result.json')
    assert report['status'] == profile.STATUS and report['label'] == profile.LABEL and report['not_run'] == list(profile.NOT_RUN)
    for key,n in [('source',40),('tree',40),('owner',32),('client',64)]: assert re.fullmatch('[0-9a-f]{'+str(n)+'}',report[key])
    assert all(report[k] == [] for k in ('cleanup_errors','missing_owned','unrelated_changed','unexpected_containers'))
    assert [c['name'] for c in report['clones']] == list(profile.CLONES)
    assert [c['name'] for c in report['cases']] == list(profile.CASES)
    assert report['databases'] == [c['database'] for c in report['clones']]
    assert len(set(report['databases'])) == len(profile.CLONES) and not set(report['databases']) & set(report['database_names_before'])
    assert set(report['database_names_after']) == set(report['database_names_before']) | set(report['databases'])
    started,finished = [oracle.retained.instant(report[k]) for k in ('started_at','finished_at')]
    assert started <= finished
    if activated_terminal:
        seeds,initial=oracle.terminal_origins_checked(root,raw); pins=oracle.TERMINAL_ORIGINS
    elif activated:
        seeds,initial=oracle.activated_origins_checked(root,raw); pins=oracle.ACTIVATED_ORIGINS
    elif initial_preparation:
        seeds,initial=(oracle.preparation_delivery_origins if initial_preparation_faults else oracle.preparation_origins_checked)(root,raw)
        pins=oracle.DELIVERY_ORIGINS if initial_preparation_faults else oracle.PREPARATION_ORIGINS
    else:
        pin=oracle.JOURNEY_ORIGIN if initial_journey else oracle.FAULT_ORIGIN if fault_profile else oracle.ORIGIN
        basis=root.parent/pin['run']/pin['subdirectory']
        if initial_journey: seed,initial=oracle.journey_origin_checked(root,raw)
        elif fault_profile: seed,initial=oracle.fault_origin_checked(root,raw)
        else:
            seed=oracle.retained.origin_checked(basis,raw,ack_refusals=True); oracle.known(seed)
        seeds={pin['name']:seed}; pins=(pin,)
    assert set(report['origins'])=={p['name'] for p in pins}
    for pin in pins:
        origin_name=pin['name']; seed=seeds[origin_name]
        basis=root.parent/pin['run']/pin['subdirectory']; store=basis/pin['store']
        declaration = report['origins'][origin_name]
        assert set(declaration) == set(pin) | {'pins','files','inputs','native','previous_client','rows','catalog','preserved'}
        assert declaration['pins'] == pin and all(declaration[k] == v for k,v in pin.items() if k != 'store')
        assert declaration['store'] == str(store)
        same(declaration['rows'],seed[0]); same(read(folder/(origin_name+'-original.json')),seed[0])
        assert declaration['native'] == seed[1] and declaration['inputs'] == seed[2]
        assert declaration['previous_client'] == read(basis/'result.json')['client']
        assert sha(declaration['catalog'].encode()) == pin['catalog_sha256']
        assert physical(store,raw) == declaration['files']; spool_checked(store/'producer-inputs-v1',seed[2],raw,complete=True)
        assert declaration['preserved'] == read(folder/(origin_name+'-preserved.json')) == dict(
            rows_unchanged=True,catalog_unchanged=True,native_unchanged=True,inputs_unchanged=True,physical_unchanged=True)
        absent(origin_name,pin['database'])
    terminal = read(folder/'terminal-containers.json')
    inventory_checked(read(folder/'preflight-containers.json'),terminal,report['client'],report['postgres'])
    assert report['postgres'] == '7177550705c49b14237e20790af411af58f62f22d110cd0dfa1afddaf78861ec'
    assert report['postgres_owner'] == '5e69b0f060b1607c25230d1dbd3e5608'
    for filename,ident,code in [('client-terminal.json',report['client'],143),('postgres-terminal.json',report['postgres'],0)]:
        value = read(folder/filename); assert value == terminal[ident]
        state = value['State']; assert state['Status'] == 'exited' and state['Pid'] == 0
        assert all(state[k] is False for k in ('Running','Paused','Restarting','OOMKilled','Dead')) and state['Error'] == ''
        assert type(state['ExitCode']) is int and state['ExitCode'] == code
    client = terminal[report['client']]
    mount = only(client['Mounts'],Destination='/lab'); assert mount['Source'] == str(folder) and mount['RW'] is True
    mount = only(client['Mounts'],Destination='/build'); assert mount['RW'] is False
    build = Path(mount['Source']); assert build.is_absolute()
    built,component,qualification = [read(p) for p in (build/'build-result.json',build/'component-result.json',build.parent/'qualification.json')]
    oracle.qualification_checked(built,component,qualification,product_source,
        initial_journey=initial_journey or initial_preparation or activated or activated_terminal,initial_preparation=initial_preparation,
        compacted_activated=activated or activated_terminal,compacted_terminal=activated_terminal)
    if initial_preparation: oracle.preparation_qualification(component)
    assert report['build'] == built and report['component'] == component
    data = raw(build.parent/'build/source-manifest.json'); assert sha(data) == built['source_manifest_sha256']
    manifest = loads(data); assert manifest['revision'] == product_source and manifest['tree'] == built['tree']
    for name,digest in component['executables_sha256'].items(): assert sha(raw(build/name)) == digest
    provenance = read(folder/'progress-source.json')
    assert set(provenance) == {'source','revision','tree','build','files_sha256'}
    assert provenance['source'] == str(root/'source') and provenance['build'] == str(build)
    assert provenance['revision'] == report['source'] and provenance['tree'] == report['tree']
    for name,digest in provenance['files_sha256'].items():
        assert not Path(name).is_absolute() and '..' not in Path(name).parts
        assert sha(raw(root/'source'/name)) == digest
    actual,compiled = provenance['files_sha256'],manifest['files_sha256']
    assert all(name.startswith(('docs/','experiments/')) or actual.get(name) == compiled.get(name) for name in set(actual)|set(compiled))
    assert raw(folder/'installed_cycle_client.py') == raw(root/'source/experiments/ingest-succession/installed_cycle_client.py')
    results = []; previous_time = started
    for clone in report['clones']:
        name = clone['name']; directory = folder/name
        if activated_terminal:
            origin_name=oracle.TERMINAL_ORIGIN_NAMES[name]; seed=seeds[origin_name]; declaration=report['origins'][origin_name]
        elif activated:
            origin_name=oracle.ACTIVATED_ORIGIN_NAMES[name]; seed=seeds[origin_name]; declaration=report['origins'][origin_name]
        elif initial_preparation:
            origin_name=(oracle.DELIVERY_ORIGIN_NAMES if initial_preparation_faults else oracle.PREPARATION_ORIGIN_NAMES)[name]
            seed=seeds[origin_name]; declaration=report['origins'][origin_name]
        assert clone['origin'] == origin_name and clone['directory'] == name and clone['status'] == 'PASS'
        assert clone['database'] == (profile.DATABASE_PREFIX if history_profile or fault_profile or initial_journey or initial_preparation or activated or activated_terminal else 'bb_compacted_reader_')+report['source'][:7]+'_'+name.replace('-','_')
        assert clone['config'] == '/lab/'+clone['database']+'.toml' and clone['final_backends'] == []
        assert clone['final_case'] == name+'-'+profile.STEPS[name][-1]
        same(read(folder/(name+'-copy-before.json')),seed[0]); same(read(folder/(name+'-migrated.json')),seed[0])
        assert [a['action'] for a in clone['migrations']] == ['plan','apply','apply']
        for index,a in enumerate(clone['migrations']):
            assert set(a) == {'action','exit_code','stdout','stderr'} and type(a['exit_code']) is int and a['exit_code'] == 0 and a['stderr'] == ''
            assert loads(a['stdout']) == dict(format='bigbrotr-control-migration/1',scope='postgres-control',target_version='40',
                source_version='40',current_version='40',status='planned' if index == 0 else 'current',applied_count='0',steps=[],
                dataset_initialization='not-performed',release_upgrade_qualification='not-performed')
        assert clone['grants'] == [dict(statement=s,stdout='') for s in profile.GRANTS]
        assert clone['catalog_before'] == clone['catalog_after'] == clone['final_catalog']
        if activated_terminal: oracle.history_catalog_checked(declaration['catalog'],clone['catalog_before'],grants=oracle.TERMINAL_GRANTS)
        elif activated: oracle.history_catalog_checked(declaration['catalog'],clone['catalog_before'],grants=oracle.ACTIVATED_GRANTS)
        elif initial_journey or initial_preparation: oracle.history_catalog_checked(declaration['catalog'],clone['catalog_before'],grants=oracle.JOURNEY_GRANTS)
        elif fault_profile: assert declaration['catalog']==clone['catalog_before']
        else: (oracle.history_catalog_checked if history_profile else oracle.catalog_checked)(declaration['catalog'],clone['catalog_before'])
        config = tomllib.loads(raw(folder/Path(clone['config']).name).decode()); ds = only(seed[0]['datasets'])
        assert config['storage']['ingest_root'] == '/lab/'+name+'/store'
        assert config['topology']['dataset_id'] == ds['dataset_id'][2:] and config['topology']['instance_id'] == ds['instance_id'][2:]
        assert config['topology']['local_roles'] == ['control','ingest','sync','archive']
        assert config['administration']['enabled'] is False and config['sync']['enabled'] is False and config['ingest']['producer_outstanding_requests'] == 2
        assert {k:config['postgres']['control'][k] for k in ('database','endpoint','user','password_ref','tls')} == dict(
            database=clone['database'],endpoint='127.0.0.1:5432',user='bigbrotr_control',password_ref='file:/run/secrets/application',
            tls=dict(local_boundary='loopback',mode='local-plaintext'))
        assert {k:config['runtime'][k] for k in ('lease_ttl','heartbeat_interval','renewal_timeout','authorization_ttl','graceful_shutdown_timeout')} == dict(
            lease_ttl='6s',heartbeat_interval='1s',renewal_timeout='1s',authorization_ttl='5s',graceful_shutdown_timeout='2s')
        for filename in (name+'-config-validation.json',clone['database']+'-installed-config-validation.json'):
            v = read(folder/filename); assert type(v['returncode']) is int and v['returncode'] == 0 and v['stderr'] == ''; loads(v['stdout'])
        if activated_terminal:
            checked,previous_time=journey_clone_replay(folder,clone,report,seed,initial,raw,read,absent,previous_time,finished,terminal_profile=True)
            results.extend(checked); continue
        if activated:
            baseline=only(results,name='progress-wire-seed')['oracle'] if name in ('outcome-after','outcome-before','reserved-tail') else None
            checked,previous_time=journey_clone_replay(folder,clone,report,seed,initial,raw,read,absent,previous_time,finished,activated=True,baseline=baseline)
            results.extend(checked); continue
        if initial_journey or initial_preparation:
            baseline=None
            if initial_preparation_faults and not name.endswith('-baseline'):
                route='gap' if name.startswith('gap-') else 'prepared'
                baseline=only(results,name=route+'-baseline-baseline')['oracle']
            checked,previous_time=journey_clone_replay(folder,clone,report,seed,initial,raw,read,absent,previous_time,finished,
                preparation=initial_preparation,delivery=initial_preparation_faults,baseline=baseline)
            results.extend(checked)
            continue
        if fault_profile:
            checked,previous_time=fault_clone_replay(folder,clone,report,seed,initial,raw,read,previous_time,finished,
                None if name=='baseline' else results[0]['oracle'])
            results.append(checked)
            continue
        if history_profile:
            checked,previous_time=history_clone_replay(folder,clone,report,seed,raw,read,absent,previous_time,finished)
            results.extend(checked)
            continue
        originals = directory/('quarantined-originals' if name == 'positive' else 'store/producer-inputs-v1')
        spool_checked(originals,seed[2],raw,complete=True)
        expected = seed
        for step in profile.STEPS[name]:
            if step in ('residual','refuse'):
                oracle.terminal_checked(expected[0],seed)
                assert read(folder/(name+'-terminal-seed.json')) == dict(control=expected[0],native=expected[1],inputs=expected[2])
                for mode in (('install','missing-pending') if name == 'missing-pending' else ('install',)):
                    label = name+'-synthetic-'+mode; absent(label,clone['database'])
                    mutation = read(folder/(label+'.json')); wanted = oracle.compacted_native(seed,name,missing=mode == 'missing-pending')
                    oracle.mutation_checked(mutation,seed,expected,wanted)
                    response = read(folder/(label+'-response.json'))
                    assert {k:response[k] for k in ('returncode','stdout','stderr')} == mutation['response']; number(response['seconds'],0,120)
                    assert read(folder/(label+'-before-native.json')) == expected[1] and read(folder/(label+'-after-native.json')) == wanted
                    expected = expected[0],wanted,expected[2]
                if name == 'incompatible-head':
                    label = name+'-head-mutation'; absent(label,clone['database']); mutation = read(folder/(label+'.json'))
                    assert raw(folder/(label+'.sql')).decode() == mutation['statement']
                    expected = oracle.head_mutation_checked(mutation,expected[0],seed),expected[1],expected[2]
            if step == 'absent':
                q = read(folder/'positive-quarantine.json'); oracle.quarantine_checked(q,seed)
                assert physical(originals,raw) == q['quarantine_after']
                assert physical(directory/'store/producer-inputs-v1',raw) == q['after']
                expected = expected[0],expected[1],{}
            label = name+'-'+step; case = only(report['cases'],name=label); oracle.specification(case)
            assert case['status'] == 'PASS' and case['custody'] == 'OBSERVED_CLOSED_AND_JOINED'
            assert case['catalog_after'] == clone['catalog_before']
            item = lambda suffix:read(folder/(label+'-'+suffix+'.json'))
            context = item('verification')
            fields = ('name','clone','origin','operation','predecessor','database','config','custody')
            assert context['case'] == {k:case[k] for k in fields} | dict(status='RUNNING')
            for suffix,key in [('before','before'),('after','after'),('before-native','native_before'),('after-native','native_after'),
                               ('inputs','inputs_before'),('after-inputs','inputs_after'),('response','response'),('clock','server_window'),
                               ('fixture','fixture'),('observations','snapshots'),('backends','backend')]: assert item(suffix) == context[key]
            for stage in ('before','after'):
                reply = item(stage+'-dump-response'); assert type(reply['returncode']) is int and reply['returncode'] == 0 and reply['stderr'] == ''
                assert loads(reply['stdout']) == context['native_'+stage]; number(reply['seconds'],0,120)
            identity,ended = read(folder/(label+'.pid.json')),read(folder/(label+'.terminal.json'))
            assert set(identity) == {'pid','start_ticks','command','config','prefix'}
            assert all(ended[k] == v for k,v in identity.items()) and ended == context['response']['terminal']
            assert case['returncode'] == context['response']['returncode']
            for stream in ('stdout','stderr'): assert raw(folder/(label+'.'+stream)).decode() == context['response'][stream]
            window = context['server_window']; assert window == case['server_window']
            number(window['before_us'],previous_time,finished,integer=True); previous_time = number(window['after_us'],window['before_us'],finished,integer=True)
            assert item('custody') == dict(status='OBSERVED_CLOSED_AND_JOINED',database=case['database'],backend_absent=True,errors=[])
            assert item('control-backends') == context['backend']['absence']
            assert item('external') == dict(control=context['after'],native=context['native_after'],inputs=context['inputs_after'],
                backend=context['backend'],catalog=clone['catalog_before'])
            assert context['catalog_before'] == context['catalog_after'] == clone['catalog_before']
            assert context['input_contents'] == {n:raw(originals/n).hex() for n in expected[2]}
            diagnostic = item('diagnostic') if name == 'floor-digest' and step == 'refuse' else None
            result = oracle.verify(context,seed,expected,diagnostic=diagnostic)
            controls = oracle.negative_controls(context,seed,expected,diagnostic=diagnostic)
            assert case['oracle'] == result and case['negative_controls'] == controls
            results.append(dict(name=label,oracle=result,negative_controls=controls))
            expected = context['after'],context['native_after'],context['inputs_after']
        same(expected[0],read(folder/(name+'-final-sql.json')))
        spool_checked(directory/'store/producer-inputs-v1',expected[2],raw,complete=True)
    from installed_progress_context_oracle import bind_ack_modules
    bind_ack_modules(provenance,raw)
    return dict(status=profile.STATUS+'_CAPTURE_REPLAY',source=report['source'],product_source=product_source,
        cases=results,bound_files=bound,not_run=list(profile.NOT_RUN),
        preservation='Original files reread; original SQL/catalog and engine/process closure are captured-run evidence',
        scope=('Compacted activated terminal positive/refusal captures; final delivery/cancellation gates NOT_RUN' if activated_terminal else 'Installed compacted activated-initial recovery and ACK with authentic COMMIT loss seeds' if activated else 'Installed compacted initial completion delivery faults and fresh retries only' if initial_preparation_faults else 'Installed existing initial preparation completion and exact retries only' if initial_preparation else 'Synthetic compacted initial preparation/gap/activation and installed startup only' if initial_journey else 'Synthetic compacted successor history delivery faults' if fault_profile else 'Synthetic connected compacted initial ancestor only' if history_profile else 'Synthetic post-GC initial reader only')+'; separate run-owner driver exit verification required')



def history_clone_replay(folder,clone,report,seed,raw,read,absent,previous_time,finished):
    name=clone['name']; directory=folder/name; expected=seed; results=[]
    quarantines=[]
    def input_bytes(filename,identity):
        candidates=[directory/'store/producer-inputs-v1'/filename]
        candidates += [directory/('quarantined-'+s)/filename for s in ('partial','absent','missing-required','corrupt-residual')]
        matches=[]
        for path in candidates:
            if path.exists():
                value=raw(path)
                if dict(bytes=len(value),sha256=sha(value))==identity: matches.append(value)
        assert matches and all(v==matches[0] for v in matches)
        return matches[0]
    for step in oracle.HISTORY_STEPS[name]:
        if step in ('full','refuse'):
            oracle.history_lineage(expected,seed,compacted=False)
            assert read(folder/(name+'-uncompacted-seed.json'))==dict(control=expected[0],native=expected[1],inputs=expected[2])
            for mode in (('install','missing-later-pending') if name=='history-missing-pending' else ('install',)):
                label=name+'-synthetic-'+mode; absent(label,clone['database'])
                record=read(folder/(label+'.json'))
                assert read(folder/(name+'-'+mode+'-expected-native.json'))==expected[1]
                assert read(folder/(label+'-before-native.json'))==expected[1]
                response=read(folder/(label+'-response.json'))
                assert {k:response[k] for k in ('returncode','stdout','stderr')}==record['response']; number(response['seconds'],0,120)
                expected=oracle.history_mutation_checked(record,expected,seed)
                assert read(folder/(label+'-after-native.json'))==expected[1]
                for phase in ('before','after'):
                    dump=read(folder/(label+'-'+phase+'-dump-response.json'))
                    assert type(dump['returncode']) is int and dump['returncode']==0 and dump['stderr']==''
                    assert loads(dump['stdout'])==record[phase]; number(dump['seconds'],0,120)
            if name=='history-incompatible-head':
                label=name+'-head-mutation'; absent(label,clone['database']); record=read(folder/(label+'.json'))
                assert raw(folder/(label+'.sql')).decode()==record['statement']
                expected=oracle.history_head_checked(record,expected,seed)
            if name in ('history-missing-required','history-corrupt-residual'):
                mutation=name.removeprefix('history-'); q=read(folder/(name+'-'+mutation+'-quarantine.json'))
                expected=oracle.history_quarantine_checked(q,expected,seed,name,mutation); quarantines.append(q)
        elif step in ('partial','absent'):
            q=read(folder/(name+'-'+step+'-quarantine.json'))
            expected=oracle.history_quarantine_checked(q,expected,seed,name,step); quarantines.append(q)
        label=name+'-'+step; case=only(report['cases'],name=label); oracle.history_specification(case)
        assert case['status']=='PASS' and case['custody']=='OBSERVED_CLOSED_AND_JOINED' and case['catalog_after']==clone['catalog_before']
        item=lambda suffix:read(folder/(label+'-'+suffix+'.json'))
        context=item('verification'); assert context['case']=={k:case[k] for k in ('name','clone','origin','operation','predecessor','database','config','custody')}|dict(status='RUNNING')
        for suffix,key in [('before','before'),('after','after'),('before-native','native_before'),('after-native','native_after'),
            ('inputs','inputs_before'),('after-inputs','inputs_after'),('response','response'),('clock','server_window')]:
            assert item(suffix)==context[key]
        same(context['before'],expected[0]); assert (context['native_before'],context['inputs_before'])==expected[1:]
        for phase in ('before','after'):
            dump=item(phase+'-dump-response'); assert type(dump['returncode']) is int and dump['returncode']==0 and dump['stderr']==''
            assert loads(dump['stdout'])==context['native_'+phase]; number(dump['seconds'],0,120)
        assert context['catalog_before']==context['catalog_after']==clone['catalog_before']
        assert case['returncode']==context['response']['returncode']==0
        window=context['server_window']; assert window==case['server_window']
        number(window['before_us'],previous_time,finished,integer=True); previous_time=number(window['after_us'],window['before_us'],finished,integer=True)
        installed=step in ('abandon','retire','source2','retire2')
        if installed:
            for suffix,key in [('fixture','fixture'),('observations','snapshots'),('backends','backend')]: assert item(suffix)==context[key]
            identity=read(folder/(label+'.pid.json')); ended=read(folder/(label+'.terminal.json'))
            assert set(identity)=={'pid','start_ticks','command','config','prefix'} and all(ended[k]==v for k,v in identity.items())
            assert ended==context['response']['terminal']
            for stream in ('stdout','stderr'): assert raw(folder/(label+'.'+stream)).decode()==context['response'][stream]
            assert item('control-backends')==context['backend']['absence']
            assert item('custody')==dict(status='OBSERVED_CLOSED_AND_JOINED',database=case['database'],backend_absent=True,errors=[])
            assert item('external')==dict(control=context['after'],native=context['native_after'],inputs=context['inputs_after'],
                backend=context['backend'],catalog=clone['catalog_before'])
            assert context['input_contents']=={n:input_bytes(n,v).hex() for n,v in context['inputs_after'].items()}
            checked=oracle.verify_history_installed(context,seed,expected)
        else:
            for suffix,key in [('control-backends','backend_absence'),('backends','backends'),('custody','custody')]: assert item(suffix)==context[key]
            checked=oracle.verify_history_context(context,seed,expected)
        controls=oracle.history_controls(context,seed,expected,installed=installed)
        assert case['oracle']==checked and case['negative_controls']==controls
        results.append(dict(name=label,oracle=checked,negative_controls=controls))
        expected=context['after'],context['native_after'],context['inputs_after']
        if step=='retire2':
            oracle.history_lineage(expected,seed,compacted=False)
            assert read(folder/(name+'-terminal2.json'))==dict(control=expected[0],native=expected[1],inputs=expected[2])
    same(read(folder/(name+'-final-sql.json')),expected[0])
    spool_checked(directory/'store/producer-inputs-v1',expected[2],raw,complete=True)
    if quarantines:
        assert physical(directory/'store/producer-inputs-v1',raw)==quarantines[-1]['after']
    for q in quarantines:
        assert physical(folder/q['path'],raw)==q['quarantine_after']
        for filename,saved in q['files'].items(): assert raw(folder/q['path']/filename).hex()==saved['hex']
    # Bind all retained clone engine files without opening the engine. The raw
    # contents are custody evidence, never a replacement for the native oracle.
    physical(directory/'store',raw)
    return results,previous_time


def fault_clone_replay(folder,clone,report,seed,initial,raw,read,previous_time,finished,baseline):
    name=clone['name']; label=name+'-read'; directory=folder/name
    case=only(report['cases'],name=label); oracle.fault_specification(case)
    assert case['status']=='PASS' and case['custody']=='OBSERVED_CLOSED_AND_JOINED' and case['catalog_after']==clone['catalog_before']
    item=lambda suffix:read(folder/(label+'-'+suffix+'.json'))
    context=item('verification')
    assert context['case']=={k:case[k] for k in ('name','clone','origin','operation','predecessor','database','config','custody')}|dict(status='RUNNING')
    for suffix,key in [('before','before'),('after','after'),('before-native','native_before'),('after-native','native_after'),
        ('inputs','inputs_before'),('after-inputs','inputs_after'),('response','response'),('clock','server_window'),
        ('fault','fault'),('control-backends','backend_absence'),('backends','backends'),('custody','custody')]:
        assert item(suffix)==context[key]
    for phase in ('before','after'):
        dump=item(phase+'-dump-response'); assert type(dump['returncode']) is int and dump['returncode']==0 and dump['stderr']==''
        assert loads(dump['stdout'])==context['native_'+phase]; number(dump['seconds'],0,120)
    assert context['catalog_before']==context['catalog_after']==clone['catalog_before']
    assert case['returncode']==context['response']['returncode']==0
    window=context['server_window']; assert window==case['server_window']
    number(window['before_us'],previous_time,finished,integer=True); previous_time=number(window['after_us'],window['before_us'],finished,integer=True)
    wire=context['fault']['wire']; assert wire['command'][3]=='bigbrotr-compacted-fault-'+report['source'][:7]
    for relative,filename in [('experiments/control-migrations/fault_proxy.py','fault_proxy.py'),
        ('experiments/relay-source/rollback_proxy.py','rollback_proxy.py')]:
        data=raw(folder/filename); assert data==raw(folder.parent/'source'/relative) and sha(data)==wire['sources'][filename]
    signals=[]
    for gate in context['fault']['gates']:
        signals.append(Path(gate['ready']['path']).name)
        if gate['head'] is not None: assert raw(folder/(label+'-head.sql')).decode()==gate['head']['statement']
    if name=='cancel': signals.append(label+'-gate.cancel')
    assert sorted(p.name for p in folder.glob(label+'-gate*'))==sorted(signals)
    for signal in signals:
        path=folder/signal; meta=path.lstat()
        assert stat.S_ISREG(meta.st_mode) and meta.st_mode&0o777==0o600 and meta.st_nlink==1 and raw(path)==b''
    checked=oracle.verify_history_fault(context,initial,seed,baseline)
    controls=oracle.history_fault_controls(context,initial,seed,baseline)
    assert case['oracle']==checked and case['negative_controls']==controls
    same(read(folder/(name+'-final-sql.json')),context['after'])
    spool_checked(directory/'store/producer-inputs-v1',seed[2],raw,complete=True)
    physical(directory/'store',raw)
    return dict(name=label,oracle=checked,negative_controls=controls),previous_time


def journey_clone_replay(folder,clone,report,seed,initial,raw,read,absent,previous_time,finished,*,preparation=False,delivery=False,activated=False,terminal_profile=False,baseline=None):
    assert type(preparation) is bool and type(delivery) is bool and (not delivery or preparation)
    name=clone['name']; directory=folder/name; expected=seed; results=[]
    assert type(activated) is bool and not (activated and (preparation or delivery))
    assert type(terminal_profile) is bool and not (terminal_profile and (activated or preparation or delivery))
    steps=oracle.TERMINAL_STEPS if terminal_profile else oracle.ACTIVATED_STEPS if activated else oracle.DELIVERY_STEPS if delivery else oracle.PREPARATION_STEPS if preparation else oracle.JOURNEY_STEPS
    check=oracle.verify_terminal_context if terminal_profile else oracle.verify_activated_context if activated else oracle.verify_preparation_context if preparation else oracle.verify_journey_context
    controls_check=oracle.terminal_controls if terminal_profile else oracle.activated_controls if activated else oracle.preparation_controls if preparation else oracle.journey_controls
    specification=oracle.terminal_specification if terminal_profile else oracle.activated_specification if activated else oracle.preparation_specification if preparation else oracle.journey_specification
    label=name+'-logical-origin'; absent(label,clone['database'])
    assert read(folder/(label+'-native.json'))==seed[1]
    response=read(folder/(label+'-dump-response.json'))
    assert type(response['returncode']) is int and response['returncode']==0 and response['stderr']=='' and loads(response['stdout'])==seed[1]
    if (activated or terminal_profile) and name=='missing-input':
        q=read(folder/'missing-input-quarantine.json'); expected=oracle.activated_quarantine_checked(q,expected)
        assert physical(folder/q['path'],raw)==q['quarantine_after'] and raw(folder/q['path']/q['name']).hex()==q['hex']
        assert physical(directory/'store/producer-inputs-v1',raw)==q['after']
    attribution=None
    if terminal_profile and 'seed' in steps[name]:
        a=read(folder/(name+'-attribution.json'))
        assert set(a)=={'kind','path','contents','physical'} and a['kind']=='retained-attribution-originals-outside-spool'
        assert a['path']==name+'/terminal-seed-inputs'; attribution=a['contents']; oracle.terminal_attribution(attribution,initial)
        assert physical(folder/a['path'],raw)==a['physical'] and set(a['physical'])=={'.',*attribution}
        for filename,value in attribution.items(): assert raw(folder/a['path']/filename).hex()==value
    for step in steps[name]:
        if terminal_profile:
            kind=oracle.terminal_mutation_kind(name,step)
            if kind:
                label=name+'-'+step+'-mutation'; absent(label,clone['database']); mutation=read(folder/(label+'.json'))
                assert read(folder/(label+'-before-native.json'))==expected[1]
                expected=oracle.terminal_mutation_checked(mutation,expected,initial,kind)
                assert read(folder/(label+'-after-native.json'))==expected[1]
                assert raw(folder/(label+'.sql')).decode()==mutation['statement']
                for phase in ('before','after'):
                    dump=read(folder/(label+'-'+phase+'-dump-response.json'))
                    assert type(dump['returncode']) is int and dump['returncode']==0 and dump['stderr']==''
                    assert loads(dump['stdout'])==mutation[phase]['native']; number(dump['seconds'],0,120)
        if preparation and step=='missing-original':
            q=read(folder/(name+'-required-quarantine.json'))
            expected=oracle.preparation_quarantine_checked(q,expected,initial)
            assert physical(folder/q['path'],raw)==q['quarantine_after']
            assert raw(folder/q['path']/q['name']).hex()==q['hex']
            assert physical(directory/'store/producer-inputs-v1',raw)==q['after']
        if step=='refuse' and not activated and not terminal_profile and not (preparation and name=='activated-zero'):
            label=name+'-mutation'; absent(label,clone['database']); mutation=read(folder/(label+'.json'))
            assert read(folder/(label+'-before-native.json'))==expected[1]
            expected=(oracle.preparation_mutation_checked if preparation else oracle.journey_mutation_checked)(mutation,expected,initial,name)
            assert read(folder/(label+'-after-native.json'))==expected[1]
            for phase in ('before','after'):
                dump=read(folder/(label+'-'+phase+'-dump-response.json'))
                assert type(dump['returncode']) is int and dump['returncode']==0 and dump['stderr']==''
                assert loads(dump['stdout'])==mutation[phase]['native']; number(dump['seconds'],0,120)
            if name=='head': assert raw(folder/(label+'.sql')).decode()==mutation['head']['statement']
            elif preparation and name=='outcome': assert raw(folder/(label+'.sql')).decode()==mutation['statement']
            elif name in ('target-nonzero','target-closing'):
                response=read(folder/(label+'-response.json'))
                assert {k:response[k] for k in ('returncode','stdout','stderr')}==mutation['response']; number(response['seconds'],0,120)
            else:
                path=directory/'store/producer-inputs-v1'/mutation['input']['target_name']
                assert raw(path).hex()==mutation['input']['hex']
                assert physical(path.parent,raw)==mutation['input']['after']
        label=name+'-'+step; case=only(report['cases'],name=label); specification(case,**(dict(delivery=delivery) if preparation else {}))
        assert case['status']=='PASS' and case['custody']=='OBSERVED_CLOSED_AND_JOINED' and case['catalog_after']==clone['catalog_before']
        item=lambda suffix:read(folder/(label+'-'+suffix+'.json'))
        context=item('verification')
        fields=('name','clone','origin','operation','predecessor','database','config','custody')
        assert context['case']=={k:case[k] for k in fields}|dict(status='RUNNING')
        for suffix,key in [('before','before'),('after','after'),('before-native','native_before'),('after-native','native_after'),
            ('inputs','inputs_before'),('after-inputs','inputs_after'),('response','response'),('clock','server_window')]:
            assert item(suffix)==context[key]
        for phase in ('before','after'):
            dump=item(phase+'-dump-response'); assert type(dump['returncode']) is int and dump['returncode']==0 and dump['stderr']==''
            assert loads(dump['stdout'])==context['native_'+phase]; number(dump['seconds'],0,120)
        assert context['catalog_before']==context['catalog_after']==clone['catalog_before']
        assert case['returncode']==context['response']['returncode']
        window=context['server_window']; assert window==case['server_window']
        number(window['before_us'],previous_time,finished,integer=True); previous_time=number(window['after_us'],window['before_us'],finished,integer=True)
        installed=step!='seed' if activated or terminal_profile else step not in ('prepare','prepare-gap') if preparation else step in ('current-refuse','serve')
        if installed:
            assert item('fixture')==context['fixture'] and item('observations')==context['snapshots'] and item('backends')==context['backend']
            assert item('control-backends')==context['backend']['absence']
            identity=read(folder/(label+'.pid.json')); terminal=read(folder/(label+'.terminal.json'))
            assert identity=={k:terminal[k] for k in ('pid','start_ticks','command','config','prefix')}
            assert terminal==context['response']['terminal']
            for stream in ('stdout','stderr'): assert raw(folder/(label+'.'+stream)).decode()==context['response'][stream]
            assert item('custody')==dict(status='OBSERVED_CLOSED_AND_JOINED',database=case['database'],backend_absent=True,errors=[])
            assert item('external')==dict(control=context['after'],native=context['native_after'],inputs=context['inputs_after'],backend=context['backend'],catalog=clone['catalog_before'])
            for snapshot in context['snapshots']: assert item(snapshot['stage'])==snapshot['control']
            if step=='serve': assert item('signal')==context['fixture']['signal']
            if preparation or activated or terminal_profile:
                contents={}
                for filename,identity in context['inputs_after'].items():
                    candidates=[directory/'store/producer-inputs-v1'/filename,directory/'quarantined-required'/filename]
                    values=[raw(p) for p in candidates if p.exists()]
                    assert len(values)==1 and dict(bytes=len(values[0]),sha256=sha(values[0]))==identity
                    contents[filename]=values[0].hex()
                assert context['input_contents']==contents
            else: assert context['input_contents']=={}
            if delivery and step in ('baseline','loss'):
                fault=context['fixture']['delivery_fault']; assert item('delivery')==fault
                for suffix,key in [('direct','direct'),('proxied','proxied'),('restored','restored')]:
                    assert raw(folder/(label+'-'+suffix+'.toml')).decode()==fault['configuration'][key]
                assert raw(folder/Path(case['config']).name).decode()==fault['configuration']['direct']
                for filename,relative in oracle.DELIVERY_SOURCES.items():
                    data=raw(folder/filename); assert data==raw(folder.parent/'source'/relative)
                    assert sha(data)==fault['wire']['sources'][filename]
                assert fault['wire']['command'][3]=='bigbrotr-initial-delivery-'+report['source'][:7]
        else:
            assert item('custody')==context['custody'] and item('control-backends')==context['backend_absence'] and item('backends')==context['backends']
            if step=='prepare-gap':
                assert item('gate')==context['gate']; path=folder/(label+'-gate.go'); meta=path.lstat()
                assert stat.S_ISREG(meta.st_mode) and stat.S_IMODE(meta.st_mode)==0o600 and meta.st_nlink==1 and raw(path)==b''
                assert context['response']['command']==['docker','exec',('bigbrotr-initial-delivery-' if delivery else 'bigbrotr-initial-preparation-' if preparation else 'bigbrotr-compacted-initial-')+report['source'][:7],'/build/ingest_bootstrap_probe',*context['arguments']]
            if terminal_profile:
                assert context['response']['command']==['docker','exec','bigbrotr-compacted-terminal-'+report['source'][:7],'/build/ingest_bootstrap_probe',*context['arguments']]
                assert context['attribution']==attribution
                resolution=item('receive-clock')
                assert type(resolution['returncode']) is int and resolution['returncode']==0 and resolution['stderr']==''
                assert loads(resolution['stdout'])==context['receive_clock']
            if activated:
                assert item('wire')==context['wire']
                assert context['wire']['command'][3]=='bigbrotr-compacted-activated-'+report['source'][:7]
                assert raw(folder/'fault_proxy.py')==raw(folder.parent/'source'/oracle.ACTIVATED_PROXY)
                assert sha(raw(folder/'fault_proxy.py'))==context['wire']['source_sha256']
        options=dict(baseline=baseline if step=='seed' else None) if activated else dict(delivery=delivery,baseline=baseline) if preparation else {}
        result=check(context,initial,expected,**options); controls=controls_check(context,initial,expected,**options)
        assert case['oracle']==result and case['negative_controls']==controls
        results.append(dict(name=label,oracle=result,negative_controls=controls))
        expected=context['after'],context['native_after'],context['inputs_after']
    same(read(folder/(name+'-final-sql.json')),expected[0])
    spool_checked(directory/'store/producer-inputs-v1',expected[2],raw,complete=True)
    physical(directory/'store',raw)
    return results,previous_time


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__); parser.add_argument('root',type=Path)
    parser.add_argument('--product-source',required=True); parser.add_argument('--output',type=Path,required=True)
    profile=parser.add_mutually_exclusive_group()
    profile.add_argument('--history',action='store_true')
    profile.add_argument('--history-faults',action='store_true')
    profile.add_argument('--initial-journey',action='store_true')
    profile.add_argument('--initial-preparation',action='store_true')
    profile.add_argument('--initial-preparation-faults',action='store_true')
    profile.add_argument('--activated',action='store_true')
    profile.add_argument('--activated-terminal',action='store_true')
    args = parser.parse_args(); result = replay(args.root,args.product_source,history_profile=args.history,fault_profile=args.history_faults,initial_journey=args.initial_journey,initial_preparation=args.initial_preparation,initial_preparation_faults=args.initial_preparation_faults,activated=args.activated,activated_terminal=args.activated_terminal)
    with args.output.open('x') as stream: json.dump(result,stream,indent=2); stream.write('\n')
