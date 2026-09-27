#!/usr/bin/env python3
"""Owned exact-Git Linux build and fresh Control lab; preserve all evidence/data."""
import datetime
import hashlib
import json
import os
from pathlib import Path
import re
import secrets
import shutil
import subprocess
import sys
import tarfile
import time

sys.dont_write_bytecode = True
revision, tree = sys.argv[1:3]
enrollment = sys.argv[3:] == ["--enrollment"]
cursor_runtime = sys.argv[3:] == ["--runtime"]
assert not sys.argv[3:] or enrollment or cursor_runtime
family = "cursor-runtime" if cursor_runtime else "cursor-enrollment" if enrollment else "cursor-registration"
offline = enrollment or cursor_runtime
assert all(re.fullmatch('[0-9a-f]{40}', value) for value in [revision, tree])
base = Path('/home/tux/bigbrotr-implementation-20260917/runs')
root = base / (family + '-' + revision[:7])
source = root / 'source'; out = root / 'out'; build = out / 'build'
cache = base / ('cursor-enrollment-9b02bd9' if cursor_runtime else 'cursor-rotation-e684915' if enrollment else 'cursor-keyring-1f251db')
cache_owner = '203570ac651e0191d5b613262c9b2091' if cursor_runtime else '89b64550b3049b02bc61122a16c3efe5' if enrollment else 'ca3561d0e4feb8654200a2e26591abdd'
cache_source = '9b02bd9f692ec07f57954b140fcd6efd20220ebe' if cursor_runtime else 'e68491570fec7f6df63f4a161dc6ba7a7d9bb491' if enrollment else '1f251dbfb59c870dd413d6da338dfdeb2911a9c9'
instrument = base / 'strace-exit-9b30c35'
image = 'sha256:7e946a0c0aa4a5d357c3ae517fe9830f675cfc98fd26a9cba79b2bc957928b9d'
label = 'org.bigbrotr.'+family+'-build'


def cmd(*args): return subprocess.check_output(args, text=True, timeout=60).strip()
def sha(path):
    with path.open('rb') as stream: return hashlib.file_digest(stream, 'sha256').hexdigest()
def write(path, value):
    staging = path.with_suffix('.partial')
    staging.write_text(json.dumps(value, indent=2) + '\n')
    staging.replace(path)
def projection(c):
    return dict(id=c['Id'], name=c['Name'], image=c['Image'],
                state={k:c['State'][k] for k in ['Status','Running','Paused','Restarting','OOMKilled','Dead','Pid','ExitCode','Error','StartedAt','FinishedAt']},
                mounts=sorted(c['Mounts'], key=lambda m:m['Destination']),
                networks=c['NetworkSettings']['Networks'], restart=c['HostConfig']['RestartPolicy'])
def census():
    ids = cmd('docker','ps','-aq').split()
    containers = json.loads(cmd('docker','inspect',*ids)) if ids else []
    return containers, dict(at=datetime.datetime.now(datetime.timezone.utc).isoformat(),
        containers=[projection(c) for c in containers], free_bytes=shutil.disk_usage(base).free,
        directories=sorted(p.name for p in base.iterdir()), processes=cmd('ps','-eo','pid,ppid,stat,comm'),
        ports=cmd('ss','-lntup'), volumes=cmd('docker','volume','ls'), networks=cmd('docker','network','ls'),
        filesystem=cmd('findmnt','-T',str(base),'-n','-o','SOURCE,FSTYPE,OPTIONS'))


assert cmd('git','-C',str(source),'rev-parse','HEAD') == revision
assert cmd('git','-C',str(source),'rev-parse','HEAD^{tree}') == tree
assert not cmd('git','-C',str(source),'status','--porcelain','--untracked-files=all')
assert cmd('uname','-m') == 'x86_64' and not out.exists()
prior, preflight = census(); write(root/'preflight.json', preflight)
assert preflight['free_bytes'] > 12*1024**3
assert not any('bigbrotr' in c['Name'].lower() and c['State']['Running'] for c in prior)
for origin, owner in [(cache,cache_owner), (instrument,'84ac8cbf5292679b8ef7043fed7a2799')]:
    state = json.loads((origin/'resources.json').read_text())
    assert state['owner'] == owner and state['state'] == 'stopped-retained'
    for entry in state['containers']:
        c = next(c for c in prior if c['Id'] == entry['id'])
        assert owner in c['Config']['Labels'].values() and c['State']['Pid'] == 0 and not c['State']['Running']
    for c in prior:
        if c['State']['Running']:
            assert not any(Path(m['Source']).is_relative_to(origin) for m in c['Mounts'] if m.get('Source','').startswith('/'))
assert sha(instrument/'out/strace-exit-fixed') == '509ea4b8a7eb303990155f013e1e288df1054d63e5c81b11bc3c31b5905b7458'
assert sha(cache/'out/native-tests/bigbrotr') == ('a943777fd6f064a5682e80fe95f7b8ebe92952c7f2c1fb50f28feb866e640716' if cursor_runtime else '2b65d3537ab7b3f147840b07358e20f0e27750c391d1683445672bfde347dadd' if enrollment else '7ebfe502a87cadc101461d5fd5bd823e2e3ee268571bc50db7050fa148cb35c0')
if offline: assert sha(source/'Cargo.lock') == sha(cache/'out/build/source/Cargo.lock')
write(root/'image.json', json.loads(cmd('docker','image','inspect',image))[0])
owner = secrets.token_hex(16)
state = dict(source=revision, tree=tree, owner=owner, owner_label=label, root=str(root),
             state='preparing', containers=[], started_at=preflight['at'])
write(root/'resources.json',state)
container = None
pg = None
runtime = root/'postgres'
try:
    build.mkdir(parents=True, mode=0o700)
    export = build/'source'; export.mkdir()
    archive = build/'source.tar'
    cmd('git','-C',str(source),'archive','--format=tar','--output',str(archive),revision)
    with tarfile.open(archive) as bundle:
        assert all(not Path(e.name).is_absolute() and '..' not in Path(e.name).parts and (e.isfile() or e.isdir()) for e in bundle.getmembers())
        bundle.extractall(export, filter='data')
    write(build/'source-manifest.json',dict(format='bigbrotr-source/1', revision=revision, tree=tree,
        archive_sha256=sha(archive), files_sha256={str(p.relative_to(export)):sha(p) for p in sorted(export.rglob('*')) if p.is_file()}))
    for name in ['target','cargo-home']:
        original = cache/'out/build'/name
        assert original.is_dir() and not original.is_symlink()
        if name == 'cargo-home':
            assert not any((original / n).exists() for n in ['credentials','credentials.toml','config','config.toml'])
        subprocess.run(['cp','-a','--reflink=auto',str(original),str(build/name)], check=True, timeout=120)
    write(root/'cache-origin.json', dict(root=str(cache), source=cache_source,
        manifest_sha256=sha(cache/'out/build/source-manifest.json'),
        scope='copied compiler cache; fresh offline build with unchanged lock' if offline else 'copied compiler cache only; changed root lock fetched with --locked and fresh build'))
    assert shutil.disk_usage(base).free > 8*1024**3
    container = cmd('docker','create','--init','--name','bigbrotr-'+family+'-'+revision[:7],
        '--label',label+'='+owner,'--cpus','2','--memory','6g','--memory-swap','6g','--pids-limit','512',
        '--network','none' if offline else 'bridge','--user',f'{os.getuid()}:{os.getgid()}', '--read-only','--cap-drop','ALL',
        '--security-opt','no-new-privileges','--tmpfs','/tmp:rw,exec,nosuid,nodev,size=512m',
        '--mount',f'type=bind,src={out},dst=/out', '--mount',f'type=bind,src={export},dst=/out/build/source,readonly',
        image,'python3','-B','/out/build/source/experiments/cursor-registration/native.py', *(['--runtime'] if cursor_runtime else ['--enrollment'] if enrollment else []))
    state.update(state='running', containers=[dict(name='native-build',id=container)]); write(root/'resources.json',state)
    cmd('docker','start',container)
    deadline = time.monotonic()+4000
    while True:
        c=json.loads(cmd('docker','inspect',container))[0]
        assert c['Config']['Labels'][label] == owner
        if not c['State']['Running']: break
        if time.monotonic()>deadline: raise TimeoutError('native build deadline')
        time.sleep(.5)
    assert c['State']['ExitCode'] == 0 and c['State']['Pid'] == 0 and not c['State']['OOMKilled']
    assert json.loads((out/'native-tests/result.json').read_text())['status'] == 'PASS'
    if cursor_runtime:
        shutil.copy2(cache/'out/native-tests/bigbrotr', out/'native-tests/previous-bigbrotr')
        assert sha(out/'native-tests/previous-bigbrotr') == sha(cache/'out/native-tests/bigbrotr')
        write(root/'negative-control.json', dict(source=cache_source, binary_sha256=sha(out/'native-tests/previous-bigbrotr'), source_manifest_sha256=sha(cache/'out/build/source-manifest.json')))
    sys.path.insert(0, str(source/'experiments/control-migrations'))
    import lab as control_lab
    # Existing qualified engine orchestration owns its separately labeled resources.
    run_name = 'cursor_' + ''.join(chr(ord('a')+int(c,16)) for c in revision[:7])
    control_lab.setup(runtime, out/'native-tests', source, run_name)
    pg = json.loads((runtime/'resources.json').read_text())
    state['postgres_resource_ids']={name:json.loads(cmd('docker','inspect',name))[0]['Id'] for name in [pg['client'],pg['postgres']]}
    state['postgres_root'] = str(runtime); write(root/'resources.json',state)
    with (root/'cases.stdout').open('xb') as stdout, (root/'cases.stderr').open('xb') as stderr:
        result = subprocess.run(['python3','-B',str(source/'experiments/cursor-registration'/('runtime.py' if cursor_runtime else 'enrollment.py' if enrollment else 'run.py')),'--root',str(runtime)],
                                stdout=stdout,stderr=stderr,timeout=600)
    state['cases_exit'] = result.returncode
    assert result.returncode == 0
    assert json.loads((runtime/('cursor-runtime.json' if cursor_runtime else 'cursor-enrollment.json' if enrollment else 'cursor-registration.json')).read_text())['status'] == 'PASS'
    state['qualification'] = 'CHECKS_PASSED'
except BaseException as error:
    state.update(qualification='INCOMPLETE', failure=type(error).__name__)
    raise
finally:
    cleanup_errors = []
    def stop_owned(name, expected_label, expected_owner, expected_id=None, required=False):
        try:
            matches = cmd('docker','ps','-aq','--filter','name=^/'+name+'$').split()
            if not matches:
                if expected_id is not None or required:raise RuntimeError('known owned container missing')
                return None
            assert len(matches)==1
            c=json.loads(cmd('docker','inspect',matches[0]))[0]
            assert c['Name']=='/'+name and c['Config']['Labels'].get(expected_label)==expected_owner
            assert expected_id is None or c['Id']==expected_id
            if c['State']['Running']: cmd('docker','stop','--time','10',c['Id'])
            c=json.loads(cmd('docker','inspect',c['Id']))[0]
            assert not c['State']['Running'] and c['State']['Pid']==0
            logs=subprocess.run(['docker','logs',c['Id']],capture_output=True,timeout=60,check=True)
            (root/(name+'.stdout')).write_bytes(logs.stdout); (root/(name+'.stderr')).write_bytes(logs.stderr)
            return projection(c)
        except BaseException as error:
            cleanup_errors.append(dict(name=name,error=type(error).__name__))
            return None
    # A timeout of the host fixture is not proof its docker-exec child stopped.
    # Helper identity derives from our setup, not the possibly interrupted
    # practical report. A missing/partial report cannot prevent other stops.
    if pg is None and (runtime/'resources.json').exists():
        try:
            pg = json.loads((runtime/'resources.json').read_text())
        except (OSError, ValueError) as error:
            cleanup_errors.append(dict(name='postgres-resource-record',error=type(error).__name__))
    if pg is not None:
        terminal=stop_owned(pg['client']+'-cursor-fault','org.bigbrotr.cursor-registration',pg['owner'],required=(enrollment or cursor_runtime) and state.get('qualification')=='CHECKS_PASSED')
        if terminal: state.setdefault('fault_terminals',[]).append(terminal)
        for name in [pg['client'],pg['postgres']]:
            terminal=stop_owned(name,'org.bigbrotr.control-migration-lab',pg['owner'],state.get('postgres_resource_ids',{}).get(name),required=pg['state']=='running')
            if terminal: state.setdefault('postgres_terminals',[]).append(terminal)
        if not cleanup_errors:
            try:
                network=json.loads(cmd('docker','network','inspect',pg['network']))[0]
                assert network['Labels']['org.bigbrotr.control-migration-lab']==pg['owner'] and not network['Containers']
                state['retired_network']=network; write(root/'resources.json',state)
                cmd('docker','network','rm',network['Id'])
                assert subprocess.run(['docker','network','inspect',network['Id']],capture_output=True,timeout=60).returncode!=0
                state['network_removed']=True
            except BaseException as error:
                cleanup_errors.append(dict(name='owned-network',error=type(error).__name__))
        pg['state']='cleanup-unresolved' if cleanup_errors else 'stopped-retained'
        try:
            write(runtime/'resources.json',pg)
        except OSError as error:
            cleanup_errors.append(dict(name='postgres-resource-record',error=type(error).__name__))
    if container:
        state['terminal']=stop_owned('bigbrotr-'+family+'-'+revision[:7],label,owner,container)
    try:
        now, final=census(); before={c['id']:c for c in preflight['containers']}
        assert set(before) <= {c['Id'] for c in now}
        assert all(projection(c)==before[c['Id']] for c in now if c['Id'] in before)
        final['preexisting_preserved']=len(before); write(root/'final-census.json',final)
    except BaseException as error:
        cleanup_errors.append(dict(name='final-census',error=type(error).__name__))
    state.update(state='cleanup-unresolved' if cleanup_errors else 'stopped-retained',
                 cleanup_errors=cleanup_errors,finished_at=datetime.datetime.now(datetime.timezone.utc).isoformat())
    state['qualification']='PASS' if not cleanup_errors and state.get('qualification')=='CHECKS_PASSED' else 'INCOMPLETE'
    write(root/'resources.json',state)
    if cleanup_errors: raise RuntimeError('owned resources require explicit cleanup resolution')
