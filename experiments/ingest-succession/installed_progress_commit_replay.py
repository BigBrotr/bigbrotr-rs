#!/usr/bin/env python3
"""Offline raw binding of the two COMMIT losses and four recovery processes."""
import argparse
from pathlib import Path
import re
import stat
import sys
import tomllib
import json

sys.dont_write_bytecode = True
import installed_progress_commit_oracle as oracle
import installed_family_capture as capture

sha, loads, only, same, number = capture.sha, capture.loads, capture.only, capture.same, capture.number


def replay(root):
    root = Path(root).resolve(strict=True); folder = root/oracle.DIRECTORY; bound = {}
    def raw(path):
        path = Path(path); assert stat.S_ISREG(path.lstat().st_mode)
        value = path.read_bytes(); bound[str(path)] = sha(value); return value
    def read(path): return loads(raw(path))
    report = read(folder/'result.json')
    assert report['status'] == oracle.STATUS and report['label'] == oracle.LABEL
    for key,count in [('source',40),('tree',40),('owner',32),('client',64)]:
        assert re.fullmatch('[0-9a-f]{'+str(count)+'}',report[key])
    provenance = read(root/'progress-source.json')
    assert set(provenance) == {'source','revision','tree','build','files_sha256'}
    assert provenance['source'] == str(root/'source') and provenance['revision'] == report['source']
    assert provenance['tree'] == report['tree'] and provenance['files_sha256']
    for name,digest in provenance['files_sha256'].items():
        assert not Path(name).is_absolute() and '..' not in Path(name).parts
        assert sha(raw(root/'source'/name)) == digest
    assert all(report[k] == [] for k in ('cleanup_errors','missing_owned','unrelated_changed','unexpected_containers'))
    assert [c['name'] for c in report['clones']] == list(oracle.MODES)
    assert [c['name'] for c in report['cases']] == list(oracle.CASES)
    assert report['databases'] == [c['database'] for c in report['clones']]
    assert len(set(report['databases'])) == 2
    assert not set(report['databases']) & set(report['database_names_before'])
    assert set(report['database_names_after']) == set(report['database_names_before']) | set(report['databases'])
    started,finished = [capture.instant(report[k]) for k in ('started_at','finished_at')]
    assert started <= finished and set(report['origins']) == {'activated'}
    origin = capture.origin_checked(root,folder,'activated',report['origins']['activated'],raw,read)
    terminal = read(folder/'terminal-containers.json')
    capture.inventory_checked(read(folder/'preflight-containers.json'),terminal,report['client'],report['postgres'])
    assert report['postgres'] == '7177550705c49b14237e20790af411af58f62f22d110cd0dfa1afddaf78861ec'
    assert report['postgres_owner'] == '5e69b0f060b1607c25230d1dbd3e5608'
    for filename,ident,code in [('client-terminal.json',report['client'],143),('postgres-terminal.json',report['postgres'],0)]:
        value = read(folder/filename); assert value == terminal[ident] and value['Id'] == ident
        state = value['State']; assert state['Status'] == 'exited' and state['Pid'] == 0
        assert all(state[k] is False for k in ('Running','Paused','Restarting','OOMKilled','Dead')) and not state['Error']
        assert type(state['ExitCode']) is int and state['ExitCode'] == code
    client = terminal[report['client']]
    assert client['Name'] == '/'+oracle.CLIENT_PREFIX+report['source'][:7]
    mount = only(client['Mounts'],Destination='/lab'); assert mount['RW'] is True and mount['Source'] == str(folder)
    mount = only(client['Mounts'],Destination='/build'); assert mount['RW'] is False
    build = Path(mount['Source']); assert build.is_absolute()
    assert provenance['build'] == str(build)
    built,component,qualification = read(build/'build-result.json'),read(build/'component-result.json'),read(build.parent/'qualification.json')
    assert qualification['status'] == 'PASS_PRODUCER_PROGRESS_COMPONENTS'
    assert report['build'] == built and report['component'] == component and component['status'] == 'PASS'
    oracle.qualification_checked(built,component,qualification)
    assert component['source'] == built['source'] == qualification['source'] == oracle.progress.require_frozen_profile()['product_source']
    for field in ('tree','source_manifest_sha256','lockfile_sha256'):
        assert component[field] == built[field] == qualification[field]
    data = raw(build.parent/'build/source-manifest.json'); assert sha(data) == built['source_manifest_sha256']
    manifest = loads(data); assert manifest['revision'] == built['source'] and manifest['tree'] == built['tree']
    actual,compiled = provenance['files_sha256'],manifest['files_sha256']
    assert all(name.startswith(('docs/','experiments/')) or actual.get(name)==compiled.get(name)
        for name in set(actual)|set(compiled)), 'unqualified product/source delta'
    assert actual['migrations/postgres/0039_producer_progress.sql'] == oracle.progress.FROZEN_PROFILE['migration_sha256']
    for name,digest in component['executables_sha256'].items():
        assert Path(name).name == name and re.fullmatch('[a-z_]+',name) and sha(raw(build/name)) == digest
    assert component['executables_sha256']['bigbrotr'] == built['binary_sha256']
    assert {'bigbrotr','ingest_bootstrap_probe'} <= set(component['executables_sha256'])
    for name,digest in manifest['files_sha256'].items():
        if name.startswith(('apps/','crates/','migrations/')) or name in ('Cargo.toml','Cargo.lock','rust-toolchain.toml'):
            assert sha(raw(root/'source'/name)) == digest
    for name,relative in [('installed_family_client.py','ingest-succession'),
            ('installed_cycle_client.py','ingest-succession'),('installed_sync_client.py','ingest-bootstrap')]:
        assert raw(folder/name) == raw(root/'source/experiments'/relative/name)
    assert sha(raw(folder/'progress-commit-proxy.py')) == oracle.PROXY_SHA256
    assert raw(folder/'progress-commit-proxy.py') == raw(root/'source/experiments/archive-engine/native_commit_proxy.py')
    for name in ('installed_family','installed_family_replay','installed_family_capture','installed_family_http',
            'installed_family_science','installed_progress_replay','installed_cycle','installed_cycle_model',
            'installed_cycle_replay','installed_progress_commit_loss','installed_progress_commit_oracle',
            'installed_progress_commit_replay','transition'):
        raw(root/'source/experiments/ingest-succession'/(name+'.py'))

    results = []; previous_time = started
    for clone in report['clones']:
        mode = clone['name']; directory = folder/mode
        migrated = capture.clone_checked(folder,clone,origin,report,raw,read,progress='split',commit_loss=True)
        expected = migrated,origin[1],origin[2]
        direct_raw = raw(folder/(mode+'-direct-config.toml'))
        assert direct_raw == raw(folder/Path(clone['config']).name)
        direct = tomllib.loads(direct_raw.decode())
        seed = None
        for step in ('loss','recover','retry'):
            label = mode+'-'+step; case = only(report['cases'],name=label)
            assert case['status'] == 'PASS' and case['custody'] == 'OBSERVED_CLOSED_AND_JOINED'
            assert type(case['returncode']) is int and case['returncode'] == (4 if step=='loss' else 0)
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
            response = item('response'); assert response == context['response']
            identity,ended = read(folder/(label+'.pid.json')),read(folder/(label+'.terminal.json'))
            assert set(identity) == {'pid','start_ticks','command','config','prefix'}
            assert all(ended[k] == v for k,v in identity.items()) and ended == response['terminal']
            for stream in ('stdout','stderr'):
                data = raw(folder/(label+'.'+stream)); assert len(data) <= 65536 and data.decode() == response[stream]
            window = item('clock'); assert window == context['server_window'] == case['server_window']
            number(window['before_us'],previous_time,finished,integer=True)
            previous_time = number(window['after_us'],window['before_us'],finished,integer=True)
            assert item('fixture') == context['fixture'] and item('observations') == context['snapshots']
            backend = item('backends'); assert backend == context['backend'] and item('control-backends') == backend['absence']
            capture.backend_checked(backend,case['database'],before,after)
            assert item('external') == dict(control=after,native=after_native,inputs=after_inputs,backend=backend,catalog=clone['catalog_before'])
            assert context['catalog_before'] == context['catalog_after'] == clone['catalog_before']
            assert item('custody') == dict(status='OBSERVED_CLOSED_AND_JOINED',database=case['database'],backend_absent=True,errors=[])
            assert not (folder/(label+'-external-unavailable.json')).exists()
            assert set(item('provisional-sql')) == set(before)
            capture.spool_checked(directory/'store/producer-inputs-v1',after_inputs,raw,complete=True)
            assert context['input_contents'] == {n:raw(directory/'store/producer-inputs-v1'/n).hex() for n in after_inputs}
            if step == 'loss':
                fault = item('fault'); assert fault == context['fault']
                assert fault['command'][3] == client['Name'].lstrip('/')
                changed = tomllib.loads(raw(folder/(label+'-config.toml')).decode())
                assert changed['postgres']['control']['endpoint'] == '127.0.0.1:'+str(fault['ready']['port'])
                changed['postgres']['control']['endpoint'] = '127.0.0.1:5432'
                assert changed == direct
                validation = item('config-validation')
                assert type(validation['returncode']) is int and validation['returncode'] == 0 and validation['stderr'] == ''
                loads(validation['stdout'])
                for cut in context['snapshots']:
                    same(cut['control'],item(cut['stage']))
                    capture.spool_checked(directory/'store/producer-inputs-v1',cut['inputs'],raw,complete=False)
                capture.journey_checked(directory,context,identity,window,raw,read,progress=True,commit_loss=True)
                transport = capture.progress_transport_checked(directory,context['journey'],context['snapshots'],
                    window,raw,read,commit_loss=True)
                assert case['progress_transport'] == transport
                gate = fault['gate']['captured']
                assert gate == dict(phase='mutation',request_id=transport['mutation_request'],
                    observation_sha256=sha(raw(directory/'progress-mutation-observation.json')))
                result,controls = oracle.verify(context),oracle.controls(context)
                seed = after,after_native,after_inputs
            else:
                assert context['snapshots'] == [] and item('provisional-inputs') == inputs
                result,controls = oracle.verify_recovery(context,seed),oracle.controls(context,seed)
            assert case['oracle'] == result and case['negative_controls'] == controls
            results.append(dict(name=label,oracle=result,negative_controls=controls))
            expected = after,after_native,after_inputs
        same(expected[0],read(folder/(mode+'-final-sql.json')))
    return dict(status=oracle.STATUS+'_CAPTURE_REPLAY',source=report['source'],cases=results,bound_files=bound,
        original_preservation='Physical/input files reread; SQL/catalog preservation is captured-run evidence',
        scope='Proof-only COMMIT reply loss, original error, failed-Control fencing, recovery/retry; ACK0, all details retained')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('root',type=Path); parser.add_argument('--output',type=Path)
    args = parser.parse_args(); result = replay(args.root); text = json.dumps(result,indent=2)+'\n'
    if args.output:
        with args.output.open('x') as stream: stream.write(text)
    else: print(text,end='')
