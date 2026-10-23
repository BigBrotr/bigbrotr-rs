#!/usr/bin/env python3
"""Bind default recovery or opt-in ACK refusals to raw files; never open an engine."""
import argparse
import json
from pathlib import Path
import re
import stat
import sys
import tomllib

sys.dont_write_bytecode = True
import installed_progress_recovery_oracle as oracle
from installed_family_capture import inventory_checked, physical, spool_checked

sha, loads, only, same, number = oracle.sha, oracle.loads, oracle.only, oracle.same, oracle.number


def replay(root, *, ack_refusals=False):
    profile = oracle.settings(ack_refusals)
    root = Path(root).resolve(strict=True); folder = root / profile['directory']; bound = {}
    def raw(path):
        path = Path(path); assert stat.S_ISREG(path.lstat().st_mode)
        value = path.read_bytes(); bound[str(path)] = sha(value); return value
    def read(path): return loads(raw(path))
    report = read(folder / 'result.json')
    assert report['status'] == profile['status'] and report['label'] == profile['label']
    for key, count in [('source',40),('tree',40),('owner',32),('client',64)]:
        assert re.fullmatch('[0-9a-f]{'+str(count)+'}', report[key])
    assert all(report[k] == [] for k in ('cleanup_errors','missing_owned','unrelated_changed','unexpected_containers'))
    assert [c['name'] for c in report['clones']] == list(profile['clones'])
    assert [c['name'] for c in report['cases']] == list(profile['cases'])
    assert report['databases'] == [c['database'] for c in report['clones']]
    assert len(set(report['databases'])) == len(profile['clones'])
    assert not set(report['databases']) & set(report['database_names_before'])
    assert set(report['database_names_after']) == set(report['database_names_before']) | set(report['databases'])
    started, finished = [oracle.instant(report[k]) for k in ('started_at','finished_at')]
    assert started <= finished

    pin = profile['origin']; origin_name = pin['name']
    assert set(report['origins']) == {origin_name}
    declaration = report['origins'][origin_name]
    assert set(declaration) == set(pin) | {'pins','files','inputs','native','previous_client','rows','catalog','preserved'}
    assert declaration['pins'] == pin and all(declaration[k] == v for k,v in pin.items() if k != 'store')
    basis = root.parent / pin['run'] / pin['subdirectory']; store = basis / pin['store']
    assert declaration['store'] == str(store)
    seed = oracle.origin_checked(basis, raw,ack_refusals=ack_refusals)
    data = raw(basis / 'result.json'); assert sha(data) == pin['result_sha256']; previous = loads(data)
    assert all(previous[k] == pin[k] for k in ('source','owner','label','status'))
    assert previous['client'] == declaration['previous_client']
    if ack_refusals:
        assert oracle.initial_ack_origin_report(basis,pin,previous,raw) == pin['client_id']
        absence = read(folder/(origin_name+'-origin-backends.json'))
        assert set(absence) == {'database','scope','limit_seconds','status','attempts','seconds'}
        assert absence['database'] == pin['database'] and absence['scope'] == 'all-database-sessions'
        assert type(absence['limit_seconds']) is int and absence['limit_seconds'] == 5
        assert absence['status'] == 'OBSERVED_ABSENT' and absence['attempts']
        elapsed = 0
        for index,attempt in enumerate(absence['attempts']):
            assert set(attempt) == {'seconds','sessions'}
            elapsed = number(attempt['seconds'],elapsed,5)
            sessions = attempt['sessions']; assert type(sessions) is list
            assert (sessions == []) == (index == len(absence['attempts'])-1)
            for session in sessions:
                assert set(session) == {'pid','backend_start','backend_type','usename','state'}
                number(session['pid'],1,integer=True)
                assert all(session[k] is None or type(session[k]) is str
                    for k in ('backend_start','backend_type','usename','state'))
            assert [s['pid'] for s in sessions] == sorted({s['pid'] for s in sessions})
        number(absence['seconds'],elapsed,6)
    else:
        assert previous['client'] == pin['client_id']
        assert all(previous[k] == [] for k in ('cleanup_errors','missing_owned','unrelated_changed','unexpected_containers'))
        original = only(previous['clones'], name='activated'); final = only(previous['cases'], name=pin['final_case'])
        assert original['database'] == pin['database'] and original['directory']+'/store' == pin['store']
        assert original['status'] == final['status'] == 'PASS' and original['final_case'] == pin['final_case']
        assert original['final_backends'] == [] and final['custody'] == 'OBSERVED_CLOSED_AND_JOINED'
        assert type(final['returncode']) is int and final['returncode'] == 0
        assert sha(original['final_catalog'].encode()) == pin['catalog_sha256']
    same(seed[0], declaration['rows']); same(seed[0], read(folder / (origin_name+'-original.json')))
    assert seed[1] == declaration['native'] and seed[2] == declaration['inputs']
    assert sha(declaration['catalog'].encode()) == pin['catalog_sha256']
    assert physical(store, raw) == declaration['files']
    spool_checked(store/'producer-inputs-v1', seed[2], raw, complete=True)
    preserved = dict(rows_unchanged=True,catalog_unchanged=True,native_unchanged=True,
        inputs_unchanged=True,physical_unchanged=True)
    assert declaration['preserved'] == read(folder/(origin_name+'-preserved.json')) == preserved
    assert all(v is True for v in declaration['preserved'].values())

    terminal = read(folder/'terminal-containers.json')
    inventory_checked(read(folder/'preflight-containers.json'),terminal,report['client'],report['postgres'])
    assert report['postgres'] == '7177550705c49b14237e20790af411af58f62f22d110cd0dfa1afddaf78861ec'
    assert report['postgres_owner'] == '5e69b0f060b1607c25230d1dbd3e5608'
    for filename, ident, exitcode in [('client-terminal.json',report['client'],143),
                                      ('postgres-terminal.json',report['postgres'],0)]:
        value = read(folder/filename); assert value == terminal[ident] and value['Id'] == ident
        state = value['State']; assert state['Status'] == 'exited' and state['Pid'] == 0
        assert all(state[k] is False for k in ('Running','Paused','Restarting','OOMKilled','Dead')) and not state['Error']
        assert type(state['ExitCode']) is int and state['ExitCode'] == exitcode
    client = terminal[report['client']]
    mount = only(client['Mounts'],Destination='/lab'); assert mount['RW'] is True and mount['Source'] == str(folder)
    mount = only(client['Mounts'],Destination='/build'); assert mount['RW'] is False
    build = Path(mount['Source']); assert build.is_absolute()
    built, component = read(build/'build-result.json'), read(build/'component-result.json')
    qualification = read(build.parent/'qualification.json')
    frozen = oracle.progress.require_frozen_profile(ack=ack_refusals,preparation_hole=ack_refusals)
    assert qualification['status'] == ('PASS_PRODUCER_ACK_COMPONENTS' if ack_refusals else 'PASS_PRODUCER_PROGRESS_COMPONENTS')
    if ack_refusals:
        oracle.progress.preparation_hole_qualification(built,component,qualification)
    assert report['build'] == built and report['component'] == component and component['status'] == 'PASS'
    assert component['source'] == built['source'] == qualification['source'] == frozen['product_source']
    for field in ('tree','source_manifest_sha256','lockfile_sha256'):
        assert component[field] == built[field] == qualification[field]
    data = raw(build.parent/'build/source-manifest.json'); assert sha(data) == built['source_manifest_sha256']
    manifest = loads(data); assert manifest['revision'] == built['source'] and manifest['tree'] == built['tree']
    for name, digest in component['executables_sha256'].items():
        assert Path(name).name == name and re.fullmatch('[a-z_]+',name) and sha(raw(build/name)) == digest
    assert component['executables_sha256']['bigbrotr'] == built['binary_sha256']
    assert {'bigbrotr','ingest_bootstrap_probe'} <= set(component['executables_sha256'])
    if ack_refusals:
        assert component['executables_sha256'] == oracle.progress.ack_executables(preparation_hole=True)
        provenance = read(folder/'progress-source.json')
        assert set(provenance) == {'source','revision','tree','build','files_sha256'}
        assert Path(provenance['source']) == root/'source' and Path(provenance['build']) == build
        assert provenance['revision'] == report['source'] and provenance['tree'] == report['tree']
        for name,digest in provenance['files_sha256'].items():
            assert not Path(name).is_absolute() and '..' not in Path(name).parts
            assert sha(raw(root/'source'/name)) == digest
        actual,compiled = provenance['files_sha256'],manifest['files_sha256']
        assert all(name.startswith(('docs/','experiments/')) or actual.get(name) == compiled.get(name)
            for name in set(actual)|set(compiled))
    for name, digest in manifest['files_sha256'].items():
        if name.startswith(('apps/','crates/','migrations/')) or name in ('Cargo.toml','Cargo.lock','rust-toolchain.toml'):
            assert sha(raw(root/'source'/name)) == digest
    assert raw(folder/'installed_cycle_client.py') == raw(root/'source/experiments/ingest-succession/installed_cycle_client.py')
    # Bind fixture-only owners too, so replay records the exact interpretation.
    for name in ('installed_cycle.py','installed_progress_recovery.py','installed_progress_recovery_oracle.py',
                 'installed_progress_recovery_replay.py','installed_cycle_model.py','installed_progress_replay.py'):
        raw(root/'source/experiments/ingest-succession'/name)

    results = []; previous_time = started
    for clone in report['clones']:
        name = clone['name']; directory = folder/name
        assert clone['origin'] == origin_name and clone['directory'] == name and clone['status'] == 'PASS'
        assert clone['database'] == profile['prefix'] + report['source'][:7] + '_' + name.replace('-','_')
        assert clone['config'] == '/lab/' + clone['database'] + '.toml' and clone['final_backends'] == []
        final_case = 'positive-retry' if name == 'positive' else name+'-refuse'
        assert clone['final_case'] == final_case
        same(seed[0],read(folder/(name+'-copy-before.json')))
        same(seed[0],read(folder/(name+'-migrated.json')))
        assert [a['action'] for a in clone['migrations']] == ['plan','apply','apply']
        for index, action in enumerate(clone['migrations']):
            assert set(action) == {'action','exit_code','stdout','stderr'} and type(action['exit_code']) is int
            assert action['exit_code'] == 0 and action['stderr'] == ''
            version = '40' if ack_refusals else '39'
            assert loads(action['stdout']) == dict(format='bigbrotr-control-migration/1',scope='postgres-control',
                target_version=version,source_version=version,current_version=version,status='planned' if index == 0 else 'current',
                applied_count='0',steps=[],dataset_initialization='not-performed',release_upgrade_qualification='not-performed')
        statements = [] if ack_refusals else [raw(root/'source/experiments/ingest-succession/schema_grants.sql').decode(),
            'GRANT SELECT ON control.initial_ingest_activations TO bigbrotr_control;',
            'GRANT UPDATE ON control.producer_epochs TO bigbrotr_control;',
            'GRANT INSERT ON control.producer_outcomes TO bigbrotr_control;', oracle.progress.PROGRESS_GRANTS]
        assert clone['grants'] == [dict(statement=s,stdout='') for s in statements]
        assert clone['catalog_before'] == clone['catalog_after'] == clone['final_catalog'] == declaration['catalog']
        config = tomllib.loads(raw(folder/Path(clone['config']).name).decode()); ds = only(seed[0]['datasets'])
        assert {k:config['postgres']['control'][k] for k in ('database','endpoint','user','password_ref')} == dict(
            database=clone['database'],endpoint='127.0.0.1:5432',user='bigbrotr_control',password_ref='file:/run/secrets/application')
        assert config['postgres']['control']['tls'] == dict(local_boundary='loopback',mode='local-plaintext')
        assert config['storage']['ingest_root'] == '/lab/' + name + '/store'
        assert config['topology']['dataset_id'] == ds['dataset_id'][2:] and config['topology']['instance_id'] == ds['instance_id'][2:]
        assert config['topology']['local_roles'] == ['control','ingest','sync','archive']
        assert config['administration']['enabled'] is False and config['sync']['enabled'] is False
        assert config['ingest']['producer_outstanding_requests'] == (2 if ack_refusals else 65)
        assert {k:config['runtime'][k] for k in ('lease_ttl','heartbeat_interval','renewal_timeout','authorization_ttl','graceful_shutdown_timeout')} == dict(
            lease_ttl='6s',heartbeat_interval='1s',renewal_timeout='1s',authorization_ttl='5s',graceful_shutdown_timeout='2s')
        assert config['runtime']['processes']['application'] == dict(memory_max_bytes='1536MiB',baseline_bytes='128MiB',
            payload_bytes='1GiB',native_bytes='256MiB',completion_reserve_bytes='32MiB')
        for filename in (name+'-config-validation.json',clone['database']+'-installed-config-validation.json'):
            value = read(folder/filename); assert type(value['returncode']) is int and value['returncode'] == 0
            assert value['stderr'] == ''; loads(value['stdout'])
        mutation = None; expected = seed
        if name != 'positive':
            mutation = read(folder/(name+'-mutation.json'))
            if ack_refusals:
                expected = oracle.ack_corruption_checked(mutation,seed,name)
                if name == 'ack-ahead':
                    assert raw(folder/(name+'-corruption.sql')).decode() == mutation['statement']
                else:
                    assert not (folder/(name+'-corruption.sql')).exists()
                    q = mutation['quarantine']; path = folder/q['path']
                    assert raw(path).hex() == q['hex'] and physical(path,raw)['.'] == q['metadata']
                assert physical(directory/'store/producer-inputs-v1',raw) == mutation['spool_after']
                assert read(folder/(name+'-spool-final.json')) == mutation['spool_after']
            else:
                assert raw(folder/(name+'-corruption.sql')).decode() == mutation['statement']
                expected = oracle.corruption_checked(mutation,seed,name),seed[1],seed[2]
            absence = read(folder/(name+'-operator-backends.json'))
            assert absence['database'] == clone['database'] and absence['user'] == 'bigbrotr_control'
            assert absence['status'] == 'OBSERVED_ABSENT' and absence['attempts'][-1]['backends'] == []
        steps = ('recover','retry') if name == 'positive' else ('refuse',)
        for step in steps:
            label = name+'-'+step; case = only(report['cases'],name=label); oracle.specification(case,ack_refusals=ack_refusals)
            assert case['status'] == 'PASS' and case['custody'] == 'OBSERVED_CLOSED_AND_JOINED'
            assert type(case['returncode']) is int and case['returncode'] == (0 if name == 'positive' else 4)
            assert case['catalog_after'] == clone['catalog_before']
            item = lambda suffix:read(folder/(label+'-'+suffix+'.json'))
            context = item('verification')
            fields = ('name','clone','origin','operation','predecessor','database','config','custody')
            assert context['case'] == {k:case[k] for k in fields} | dict(status='RUNNING')
            before,after,native,after_native,inputs,after_inputs = [item(s) for s in
                ('before','after','before-native','after-native','inputs','after-inputs')]
            same(expected[0],before); assert native == expected[1] and inputs == expected[2]
            same(before,context['before']); same(after,context['after'])
            assert native == context['native_before'] and after_native == context['native_after']
            assert inputs == context['inputs_before'] and after_inputs == context['inputs_after']
            for stage,value in [('before',native),('after',after_native)]:
                answer = item(stage+'-dump-response'); assert set(answer) == {'returncode','stdout','stderr','seconds'}
                assert type(answer['returncode']) is int and answer['returncode'] == 0 and answer['stderr'] == ''
                number(answer['seconds'],0,120); assert loads(answer['stdout']) == value
            response = item('response'); assert response == context['response']; oracle.process_closed(response,case,ack_refusals=ack_refusals)
            identity, ended = read(folder/(label+'.pid.json')),read(folder/(label+'.terminal.json'))
            assert set(identity) == {'pid','start_ticks','command','config','prefix'}
            assert all(ended[k] == v for k,v in identity.items()) and ended == response['terminal']
            for stream in ('stdout','stderr'):
                data = raw(folder/(label+'.'+stream)); assert len(data) <= 65536 and data.decode() == response[stream]
            window = item('clock'); assert window == context['server_window'] == case['server_window']
            number(window['before_us'],previous_time,finished,integer=True)
            previous_time = number(window['after_us'],window['before_us'],finished,integer=True)
            assert item('fixture') == context['fixture'] and item('observations') == context['snapshots'] == []
            backend = item('backends'); assert backend == context['backend'] and item('control-backends') == backend['absence']
            oracle.backend_checked(backend,case['database'],before,after)
            assert item('external') == dict(control=after,native=after_native,inputs=after_inputs,backend=backend,catalog=clone['catalog_before'])
            assert context['catalog_before'] == context['catalog_after'] == clone['catalog_before']
            assert item('custody') == dict(status='OBSERVED_CLOSED_AND_JOINED',database=case['database'],backend_absent=True,errors=[])
            assert not (folder/(label+'-external-unavailable.json')).exists()
            assert set(item('provisional-sql')) == set(before) and item('provisional-inputs') == inputs
            spool_checked(directory/'store/producer-inputs-v1',after_inputs,raw,complete=True)
            assert context['input_contents'] == {n:raw(directory/'store/producer-inputs-v1'/n).hex() for n in after_inputs}
            diagnostic = item('diagnostic') if name != 'positive' and not ack_refusals else None
            result = oracle.verify(context,seed,mutation=mutation,diagnostic=diagnostic,ack_refusals=ack_refusals)
            controls = oracle.negative_controls(context,seed,mutation=mutation,diagnostic=diagnostic,ack_refusals=ack_refusals)
            assert case['oracle'] == result and case['negative_controls'] == controls
            results.append(dict(name=label,oracle=result,negative_controls=controls))
            expected = after,after_native,after_inputs
        same(expected[0],read(folder/(name+'-final-sql.json')))
    if ack_refusals:
        from installed_progress_context_oracle import bind_ack_modules
        bind_ack_modules(provenance,raw)
    return dict(status=profile['status']+'_CAPTURE_REPLAY',source=report['source'],cases=results,bound_files=bound,
        original_preservation='Physical/input files reread; SQL/catalog preservation is captured-run evidence',
        scope='Initial A>C and exact original5 refusals; no native ACK ambiguity/full-window proof/GC qualification' if ack_refusals else
            'Successor3 typed recovery/retry and two privileged SQL corruptions; ACK0, no native-fact injection/GC/COMMIT-loss qualification')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('root',type=Path); parser.add_argument('--output',type=Path)
    parser.add_argument('--ack-refusals',action='store_true')
    args = parser.parse_args(); result = replay(args.root,ack_refusals=args.ack_refusals); text = json.dumps(result,indent=2)+'\n'
    if args.output:
        with args.output.open('x') as stream: stream.write(text)
    else: print(text,end='')
