#!/usr/bin/env python3
"""Bind raw refusal/prefix captures offline; no engine/native store imports."""
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
import lab  # Inert helper only; never the network engine runner named lab.py.
spec=importlib.util.spec_from_file_location('bootstrap_oracle',HERE/'oracle.py')
bootstrap=importlib.util.module_from_spec(spec);sys.modules['bootstrap_oracle']=bootstrap;spec.loader.exec_module(bootstrap)
from installed_archive_catch_up_capture import Files,absence
from installed_archive_catch_up_refusal_oracle import verify,negative_controls,WAIT_CASES


def bind_process(f,directory,database,name,phase):
    """Raw envelope common to prefix and refusal; science is checked separately."""
    prefix=directory/name
    def item(suffix):return f.json(directory/(name+'-'+suffix+'.json'))
    assert item('response')==phase['response'] and item('clock')==phase['window'] and item('fixture')==phase['fixture']
    assert f.text(Path(str(prefix)+'.stdout'))==phase['response']['stdout'] and f.text(Path(str(prefix)+'.stderr'))==phase['response']['stderr']
    terminal=f.json(Path(str(prefix)+'.terminal.json'));assert terminal==phase['response']['terminal']
    identity=f.json(Path(str(prefix)+'.pid.json'));assert all(terminal[k]==v for k,v in identity.items())
    assert set(identity)=={'pid','start_ticks','command','config','prefix'}
    assert f.text(directory/(name+'-config.toml'))==phase['config_text']
    assert item('backends')==phase['backend'];absence(item('control-backends'),database,phase['backend']['exact'][0])
    assert item('verification')==phase
    provisional=item('provisional-state')
    assert {k:v for k,v in provisional.items() if k!='physical'}=={k:v for k,v in phase['after'].items() if k not in ('physical','native')}


def replay(root):
    root=Path(root).resolve();directory=root/'bindings';f=Files()
    report=f.json(directory/'result.json');journey=f.json(directory/'archive-catch-up-refusal.json')
    assert report['status']=='PASS_INSTALLED_ARCHIVE_CATCH_UP_REFUSAL' and journey['status']=='PASS'
    assert report['wave']=='installed-archive-catch-up-refusal-'+journey['case']
    assert re.fullmatch('[0-9a-f]{40}',report['source']) and not report['cleanup_errors'] and not report['missing_owned'] and not report['unrelated_changed']
    assert len(report['terminal'])==2
    for item in report['terminal']:
        state=item['State'];assert state['Running'] is False and state['Pid']==0 and state['OOMKilled'] is False
        assert type(state['ExitCode']) is int and state['ExitCode']==(143 if item['Id']==report['client']['id'] else 0)
    result,=report['cases'];assert result['status']=='PASS' and result['case']==journey['case']
    assert result['oracle']==journey['oracle'] and result['negative_controls']==journey['negative_controls']
    original=f.json(directory/'catch-up-originals-before.json')
    assert set(original)=={'bb_scope_54d1a18_basic','bb_source_template'} and original==f.json(directory/'catch-up-originals-after.json')
    resource,=report['archive_resources'];assert resource==journey['setup_resource']==journey['initial_setup']['resource']
    home=Path(resource['home']);assert home==directory/'archive-export'
    assert journey['config']==resource['config'] and journey['database']==resource['control_database']
    assert journey['client']==report['client']['id']
    assert journey['initial_setup']['control']==f.json(directory/'archive-export-setup-control.json')
    assert journey['initial_setup']['catalog']==f.json(directory/'archive-export-setup-catalog.json')
    for suffix in ('lineage-upgrade','lineage-migrations','lineage-grants','reader-privileges','setup-roles','setup-files'):
        f.json(directory/('archive-export-'+suffix+'.json'))
    migrations=f.json(directory/'archive-export-lineage-migrations.json')
    assert [m['action'] for m in migrations]==['plan','apply','apply'] and all(m['returncode']==0 and not m['stderr'] for m in migrations)
    assert f.bytes(home/'event.jsonl').hex()==journey['initial_event_hex']
    assert [f.json(p) for p in sorted(home.glob('relay-closed-*.json'))]==journey['initial_transcript']
    assert len(list(home.glob('relay-open-*.json')))==len(journey['initial_transcript'])
    for path in sorted(home.glob('relay-open-*.json')):f.json(path)
    initial_reports=[json.loads(s) for s in f.text(directory/'archive-export.stdout').splitlines()]
    assert initial_reports[0]==journey['initialized'] and not f.text(directory/'archive-export.stderr')
    identity=f.json(directory/'archive-export.pid.json');terminal=f.json(directory/'archive-export.terminal.json')
    assert all(terminal[k]==v for k,v in identity.items()) and terminal['reaped'] is True and terminal['forced'] is False and terminal['exit_code']==0
    assert terminal==journey['initial']['terminal'] and terminal['config']==journey['config'] and terminal['prefix']=='/lab/archive-export'
    assert f.json(directory/'archive-export-after.json')==journey['initial_state']['control']
    assert f.json(directory/'archive-export-final-catalog.json')==journey['initial_state']['catalog']
    assert f.json(directory/'archive-export-final-roles.json')==journey['initial_state']['system_roles']
    assert f.json(directory/'archive-export-native.json')['rows']==journey['initial_state']['native']['rows']
    for name in ('client-result.json','client-exit.json','status-observations.json','signal-observation.json'):
        if (home/name).exists():f.json(home/name)
    for filename in ('installed_archive_catch_up_client.py','installed_family_client.py'):
        folder='ingest-succession' if filename=='installed_family_client.py' else 'ingest-bootstrap'
        assert f.bytes(directory/filename)==f.bytes(root/'source/experiments'/folder/filename)

    def state(label,value):
        assert f.json(directory/(label+'-state.json'))==value
        assert f.json(directory/(label+'-native.json'))==value['native']
        raw=f.json(directory/(label+'-dump-response.json'))
        assert raw['returncode']==0 and not raw['stderr'] and raw.get('outcome','PROCESS_EXITED')=='PROCESS_EXITED'
        assert type(raw['seconds']) in (int,float) and math.isfinite(raw['seconds']) and 0<=raw['seconds']<=90
        assert json.loads(raw['stdout'])==value['native']

    state('catch-up-initial',journey['initial_state'])
    for phase in journey['steps']:
        name='catch-up-'+phase['step'];state(name+'-before',phase['before']);state(name+'-after',phase['after']);bind_process(f,directory,journey['database'],name,phase)
        assert f.json(directory/(name+'-custody.json'))==dict(status='OBSERVED_CLOSED_AND_JOINED',errors=[])
        if phase['fixture']['snapshots']:assert f.json(directory/(name+'-observations.json'))==phase['fixture']['snapshots']
        if phase['step']=='live3':
            network=directory/'catch-up-network';data=phase['fixture']['journey']
            assert f.json(network/'client-result.json')==data['result']
            assert f.json(network/'client-exit.json')==data['terminal']==phase['fixture']['client_terminal']
            assert not f.text(network/'client.stdout') and not f.text(network/'client.stderr')
            assert [f.json(p) for p in sorted(network.glob('relay-open-*.json'))]==data['opened']
            assert [f.json(p) for p in sorted(network.glob('relay-closed-*.json'))]==data['closed']
            for filename in ('fixture','events','prepared','submitted','commands-finished'):f.json(network/(filename+'.json'))
            if (network/'signal-observation.json').exists():assert f.json(network/'signal-observation.json')==phase['fixture']['signal']
    phase=journey['refusal'];name='catch-up-refusal-'+journey['case'];mutation=phase['mutation']
    state(name+'-before',mutation['before']);state(name+'-prepared',phase['before']);state(name+'-after',phase['after']);bind_process(f,directory,journey['database'],name,phase)
    assert f.json(directory/(name+'-mutation.json'))==mutation
    assert f.json(directory/(name+'-schedule.json'))==phase['schedule']
    assert f.json(directory/(name+'-custody.json'))==dict(status='OBSERVED_CLOSED_AND_JOINED',process_closed=True,holder_closed=True,errors=[])
    if mutation['sql'] is not None:assert f.text(directory/(name+'-mutation.sql'))==mutation['sql']
    if mutation['quarantine']:
        q=mutation['quarantine'];path=Path(q['path']);assert path==directory/(name+'-quarantined.inputs')
        assert f.bytes(path).hex()==q['hex'];meta=path.lstat()
        assert {k:q[k] for k in ('device','inode','mode','links','bytes')}==dict(device=meta.st_dev,inode=meta.st_ino,mode=meta.st_mode,links=meta.st_nlink,bytes=meta.st_size)
    if journey['case'] in WAIT_CASES:assert f.json(directory/'catch-up-refusal-final-readback.json')==journey['readback']
    assert f.text(directory/Path(journey['config']).name)==phase['config_text']
    final=phase['after']
    assert {p.name for p in (home/'store/producer-inputs-v1').iterdir()}==set(final['contents'])
    for name,raw in final['contents'].items():assert f.bytes(home/'store/producer-inputs-v1'/name).hex()==raw
    for name,raw in final['retained'].items():assert f.bytes(home/name).hex()==raw
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
    for path in sorted(directory.iterdir()):
        if path.is_file():f.bytes(path)
    checked=verify(journey);controls=negative_controls(journey)
    assert checked==journey['oracle'] and controls==journey['negative_controls']
    return dict(status='PASS',source=report['source'],oracle=checked,negative_controls=controls,bound_files=f.bound)


if __name__=='__main__':
    assert len(sys.argv)==2
    print(json.dumps(replay(Path(sys.argv[1])),sort_keys=True))
