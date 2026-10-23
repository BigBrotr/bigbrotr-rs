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


def replay(root, product_source, *, history_profile=False):
    assert type(history_profile) is bool
    profile=oracle.HistoryProfile if history_profile else oracle
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
    pin = oracle.ORIGIN; basis = root.parent/pin['run']/pin['subdirectory']; store = basis/pin['store']
    seed = oracle.retained.origin_checked(basis,raw,ack_refusals=True); oracle.known(seed)
    assert set(report['origins']) == {'initial'}; declaration = report['origins']['initial']
    assert set(declaration) == set(pin) | {'pins','files','inputs','native','previous_client','rows','catalog','preserved'}
    assert declaration['pins'] == pin and all(declaration[k] == v for k,v in pin.items() if k != 'store')
    assert declaration['store'] == str(store)
    same(declaration['rows'],seed[0]); same(read(folder/'initial-original.json'),seed[0])
    assert declaration['native'] == seed[1] and declaration['inputs'] == seed[2]
    assert declaration['previous_client'] == read(basis/'result.json')['client']
    assert sha(declaration['catalog'].encode()) == pin['catalog_sha256']
    assert physical(store,raw) == declaration['files']; spool_checked(store/'producer-inputs-v1',seed[2],raw,complete=True)
    assert declaration['preserved'] == read(folder/'initial-preserved.json') == dict(
        rows_unchanged=True,catalog_unchanged=True,native_unchanged=True,inputs_unchanged=True,physical_unchanged=True)
    absent('initial',pin['database'])
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
    oracle.qualification_checked(built,component,qualification,product_source)
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
        assert clone['origin'] == 'initial' and clone['directory'] == name and clone['status'] == 'PASS'
        assert clone['database'] == (profile.DATABASE_PREFIX if history_profile else 'bb_compacted_reader_')+report['source'][:7]+'_'+name.replace('-','_')
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
        (oracle.history_catalog_checked if history_profile else oracle.catalog_checked)(declaration['catalog'],clone['catalog_before'])
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
        scope=('Synthetic connected compacted initial ancestor only' if history_profile else 'Synthetic post-GC initial reader only')+'; separate run-owner driver exit verification required')



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


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__); parser.add_argument('root',type=Path)
    parser.add_argument('--product-source',required=True); parser.add_argument('--output',type=Path,required=True)
    parser.add_argument('--history',action='store_true')
    args = parser.parse_args(); result = replay(args.root,args.product_source,history_profile=args.history)
    with args.output.open('x') as stream: json.dump(result,stream,indent=2); stream.write('\n')
