#!/usr/bin/env python3
"""Offline raw binding of the four NEW-owner COMMIT-loss processes."""
import hashlib
import importlib.util
import json
import math
from pathlib import Path
import re
import stat
import sys

sys.dont_write_bytecode=True
HERE=Path(__file__).resolve().parent
for directory in (HERE,HERE.parent/'acquisition-scope',HERE.parent/'network-transport',HERE.parent/'ingest-succession'):
    sys.path.insert(0,str(directory))
sys.path.insert(0,str(HERE.parent/'control-migrations'))
import lab  # Import the inert helper, never the network runner named lab.py.
spec=importlib.util.spec_from_file_location('bootstrap_oracle',HERE/'oracle.py')
bootstrap=importlib.util.module_from_spec(spec);sys.modules['bootstrap_oracle']=bootstrap;spec.loader.exec_module(bootstrap)
from installed_archive_catch_up_fault_oracle import verify,negative_controls
from installed_archive_catch_up_capture import Files,absence


def replay(root):
    root=Path(root).resolve();directory=root/'bindings';f=Files()
    report=f.json(directory/'result.json');journey=f.json(directory/'archive-catch-up-faults.json')
    assert report['status']=='PASS_INSTALLED_ARCHIVE_CATCH_UP_FAULTS' and journey['status']=='PASS'
    assert re.fullmatch('[0-9a-f]{40}',report['source']) and not report['cleanup_errors'] and not report['missing_owned'] and not report['unrelated_changed']
    assert len(report['terminal'])==2
    for item in report['terminal']:
        state=item['State'];assert state['Running'] is False and state['Pid']==0 and state['OOMKilled'] is False
        assert type(state['ExitCode']) is int and state['ExitCode']==(143 if item['Id']==report['client']['id'] else 0)
    assert len(report['cases'])==1 and report['cases'][0]['status']=='PASS'
    assert report['cases'][0]['oracle']==journey['oracle'] and report['cases'][0]['negative_controls']==journey['negative_controls']
    original=f.json(directory/'catch-up-originals-before.json')
    assert set(original)=={'bb_scope_54d1a18_basic','bb_source_template'}
    assert original==f.json(directory/'catch-up-originals-after.json')
    resource,=report['archive_resources'];assert resource==journey['setup_resource']==journey['initial_setup']['resource']
    home=Path(resource['home']);assert home==directory/'archive-export'
    assert journey['config']==resource['config'] and journey['database']==resource['control_database']
    assert journey['initial_setup']['control']==f.json(directory/'archive-export-setup-control.json')
    assert journey['initial_setup']['catalog']==f.json(directory/'archive-export-setup-catalog.json')
    for suffix in ('lineage-upgrade','lineage-migrations','lineage-grants','reader-privileges','setup-roles','setup-files'):
        f.json(directory/('archive-export-'+suffix+'.json'))
    migrations=f.json(directory/'archive-export-lineage-migrations.json')
    assert [m['action'] for m in migrations]==['plan','apply','apply']
    assert all(m['returncode']==0 and not m['stderr'] for m in migrations)
    assert f.bytes(home/'event.jsonl').hex()==journey['initial_event_hex']
    assert [f.json(p) for p in sorted(home.glob('relay-closed-*.json'))]==journey['initial_transcript']
    assert len(list(home.glob('relay-open-*.json')))==len(journey['initial_transcript'])
    for p in sorted(home.glob('relay-open-*.json')):f.json(p)
    initial_stdout=f.text(directory/'archive-export.stdout');initial_reports=[json.loads(s) for s in initial_stdout.splitlines()]
    assert initial_reports[0]==journey['initialized'] and not f.text(directory/'archive-export.stderr')
    identity=f.json(directory/'archive-export.pid.json');terminal=f.json(directory/'archive-export.terminal.json')
    assert all(terminal[k]==v for k,v in identity.items()) and terminal['reaped'] is True and terminal['forced'] is False and terminal['exit_code']==0
    assert terminal==journey['initial']['terminal'] and terminal['config']==journey['config'] and terminal['prefix']=='/lab/archive-export'
    assert f.json(directory/'archive-export-after.json')==journey['initial_state']['control']
    assert f.json(directory/'archive-export-final-catalog.json')==journey['initial_state']['catalog']
    assert f.json(directory/'archive-export-final-roles.json')==journey['initial_state']['system_roles']
    assert f.json(directory/'archive-export-native.json')['rows']==journey['initial_state']['native']['rows']
    for name in ('client-result.json','client-exit.json','status-observations.json','signal-observation.json'):
        path=home/name
        if path.exists():f.json(path)
    def state(label,value):
        assert f.json(directory/(label+'-state.json'))==value
        assert f.json(directory/(label+'-native.json'))==value['native']
        raw=f.json(directory/(label+'-dump-response.json'))
        assert raw['returncode']==0 and not raw['stderr'] and raw.get('outcome','PROCESS_EXITED')=='PROCESS_EXITED'
        assert type(raw['seconds']) in (int,float) and math.isfinite(raw['seconds']) and 0<=raw['seconds']<=90
        assert json.loads(raw['stdout'])==value['native']
    state('catch-up-initial',journey['initial_state'])
    assert f.bytes(directory/'archive-catch-up-commit-proxy.py')==f.bytes(root/'source/experiments/archive-engine/native_commit_proxy.py')
    assert hashlib.sha256(f.bytes(directory/'archive-catch-up-commit-proxy.py')).hexdigest()==journey['proxy_sha256']
    assert journey['client']==report['client']['name']
    client_terminal,=[item for item in report['terminal'] if item['Id']==report['client']['id']]
    assert client_terminal['Name']=='/'+journey['client']
    for phase in journey['steps']+journey['faults']:
        name='catch-up-'+phase['step'];prefix=directory/name
        def item(suffix):return f.json(directory/(name+'-'+suffix+'.json'))
        state(name+'-before',phase['before']);state(name+'-after',phase['after'])
        assert item('response')==phase['response'] and item('clock')==phase['window'] and item('fixture')==phase['fixture']
        assert f.text(Path(str(prefix)+'.stdout'))==phase['response']['stdout']
        assert f.text(Path(str(prefix)+'.stderr'))==phase['response']['stderr']
        terminal=f.json(Path(str(prefix)+'.terminal.json'));assert terminal==phase['response']['terminal']
        identity=f.json(Path(str(prefix)+'.pid.json'));assert all(terminal[k]==v for k,v in identity.items())
        if phase in journey['faults']:
            assert item('custody')==dict(status='OBSERVED_CLOSED_AND_JOINED',process_closed=True,proxy_closed=True,errors=[])
            assert item('wire')==phase['wire']
            assert f.text(directory/Path(phase['config']).name)==phase['config_text']
        else:
            assert item('custody')==dict(status='OBSERVED_CLOSED_AND_JOINED',errors=[])
        assert f.text(directory/(name+'-config.toml'))==phase['config_text']
        assert item('backends')==phase['backend'];absence(item('control-backends'),journey['database'],phase['backend']['exact'][0])
        verification={k:v for k,v in phase.items() if k not in ('readback','artifact_files','artifact_contents')}
        assert item('verification')==verification
        provisional=item('provisional-state')
        assert {k:v for k,v in provisional.items() if k!='physical'}=={k:v for k,v in phase['after'].items() if k not in ('physical','native')}
        snapshots=phase['fixture']['snapshots']
        if snapshots:assert item('observations')==snapshots
        if 'readback' in phase:assert item('readback')==phase['readback']
        if 'artifact_files' in phase:assert item('artifacts')==dict(files=phase['artifact_files'],contents=phase['artifact_contents'])
        if phase['step']=='live3':
            network=directory/'catch-up-network';data=phase['fixture']['journey']
            assert f.json(network/'client-result.json')==data['result']
            assert f.json(network/'client-exit.json')==data['terminal']==phase['fixture']['client_terminal']
            assert not f.text(network/'client.stdout') and not f.text(network/'client.stderr')
            assert [f.json(p) for p in sorted(network.glob('relay-open-*.json'))]==data['opened']
            assert [f.json(p) for p in sorted(network.glob('relay-closed-*.json'))]==data['closed']
            for name in ('fixture','events','prepared','submitted','commands-finished'):f.json(network/(name+'.json'))
            if (network/'signal-observation.json').exists():assert f.json(network/'signal-observation.json')==phase['fixture']['signal']
    assert f.json(directory/'catch-up-fault-final-readback.json')==journey['readback']
    assert f.text(directory/Path(journey['config']).name)==journey['steps'][-1]['config_text']
    final=journey['faults'][-1]['after']
    for name,raw in final['contents'].items():assert f.bytes(home/'store/producer-inputs-v1'/name).hex()==raw
    assert {p.name for p in (home/'store/producer-inputs-v1').iterdir()}==set(final['contents'])
    for name,raw in final['retained'].items():assert f.bytes(home/name).hex()==raw
    # No Rocks open: ordinary OS reads bind retained physical file identities.
    for area in ('store','data','state','scratch'):
        path=home/area;meta=path.lstat();expected=final['physical'][area]
        assert dict(device=meta.st_dev,inode=meta.st_ino,mode=meta.st_mode)=={k:v for k,v in expected.items() if k!='files'}
        assert {str(p.relative_to(path)) for p in path.rglob('*')}==set(expected['files'])
        for name,item in expected['files'].items():
            pathfile=path/name;meta=pathfile.lstat();assert not pathfile.is_symlink()
            assert dict(mode=stat.S_IMODE(meta.st_mode),inode=meta.st_ino,device=meta.st_dev,links=meta.st_nlink)=={k:v for k,v in item.items() if k!='sha256'}
            if item['sha256'] is not None:assert hashlib.sha256(f.bytes(pathfile)).hexdigest()==item['sha256']
            else:assert stat.S_ISDIR(meta.st_mode)
    for name,raw in final['local_records'].items():assert f.bytes(home/'state'/name).hex()==raw
    # Hash every top-level emitted capture, including commands/setup evidence;
    # semantic bindings above remain distinct from hash-only preservation.
    for path in sorted(directory.iterdir()):
        if path.is_file():f.bytes(path)
    checked=verify(journey);controls=negative_controls(journey)
    assert checked==journey['oracle'] and controls==journey['negative_controls']
    return dict(status='PASS',source=report['source'],oracle=checked,negative_controls=controls,bound_files=f.bound)


if __name__=='__main__':
    assert len(sys.argv)==2
    print(json.dumps(replay(Path(sys.argv[1])),sort_keys=True))
