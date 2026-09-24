#!/usr/bin/env python3
"""One owned networkless Linux container, exact Git inputs and copied caches.

Run only trusted owner-selected commits from a separately transferred checkout.
No database, network, volume or unrelated workload is created or modified.
"""
import datetime
import hashlib
import json
import os
import re
import secrets
import shutil
import subprocess
import sys
import tarfile
import time
from pathlib import Path

sys.dont_write_bytecode = True
revision, tree = sys.argv[1:3]
reuse = sys.argv[3] if len(sys.argv) == 4 else None
assert len(sys.argv) in (3,4) and (reuse is None or re.fullmatch('[0-9a-f]{40}',reuse))
assert all(re.fullmatch('[0-9a-f]{40}', v) for v in [revision, tree])
base = Path('/home/tux/bigbrotr-implementation-20260917/runs')
root = base/('cursor-keyring-'+revision[:7])
source, out = root/'source', root/'out'
cache = base/'export-runtime-8dbd3c4'
instrument = base/'strace-exit-9b30c35'
image = 'sha256:7e946a0c0aa4a5d357c3ae517fe9830f675cfc98fd26a9cba79b2bc957928b9d'
label = 'org.bigbrotr.cursor-keyring'


def cmd(*args):
    return subprocess.check_output(args, text=True, timeout=60).strip()


def sha(path):
    with path.open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def write(path, value):
    path.write_text(json.dumps(value, indent=2)+'\n')


def projection(c):
    return dict(id=c['Id'], name=c['Name'], image=c['Image'],
                state={k:c['State'][k] for k in ['Status','Running','Paused','Restarting','OOMKilled',
                                               'Dead','Pid','ExitCode','Error','StartedAt','FinishedAt']},
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
assert cmd('uname','-m') == 'x86_64'
prior, preflight = census()
write(root/'preflight.json', preflight)
assert preflight['free_bytes'] > 12*1024**3
assert not any('bigbrotr' in c['Name'].lower() and c['State']['Running'] for c in prior)
assert not out.exists()
origins = [(instrument,'84ac8cbf5292679b8ef7043fed7a2799')]
if not reuse:
    origins.append((cache,'1ae77d53919b4876eeff7df396179ccd'))
else:
    previous = base/('cursor-keyring-'+reuse[:7])
    old = json.loads((previous/'resources.json').read_text())
    assert old['source'] == reuse and old['state'] == 'stopped-retained'
    origins.append((previous,old['owner']))
for origin, owner in origins:
    state = json.loads((origin/'resources.json').read_text())
    assert state['owner'] == owner and state['state'] == 'stopped-retained'
    for entry in state['containers']:
        c = next(c for c in prior if c['Id'] == entry['id'])
        assert owner in c['Config']['Labels'].values()
        assert not c['State']['Running'] and c['State']['Pid'] == 0
    for c in prior:
        if c['State']['Running']:
            assert not any(Path(m['Source']).is_relative_to(origin) for m in c['Mounts'] if m.get('Source','').startswith('/'))
assert sha(instrument/'out/strace-exit-fixed') == '509ea4b8a7eb303990155f013e1e288df1054d63e5c81b11bc3c31b5905b7458'
if not reuse:
    assert sha(source/'Cargo.lock') == sha(cache/'out/build/source/Cargo.lock')
write(root/'image.json', json.loads(cmd('docker','image','inspect',image))[0])
owner = secrets.token_hex(16)
state = dict(source=revision, tree=tree, owner=owner, owner_label=label, root=str(root),
             state='preparing', containers=[], image=image, started_at=preflight['at'])
write(root/'resources.json', state)
container = None
try:
    build = out/'build'
    build.mkdir(parents=True, mode=0o700)
    export = build/'source'
    export.mkdir()
    archive = build/'source.tar'
    cmd('git','-C',str(source),'archive','--format=tar','--output',str(archive),revision)
    with tarfile.open(archive) as tar:
        assert all(not Path(e.name).is_absolute() and '..' not in Path(e.name).parts and (e.isfile() or e.isdir()) for e in tar.getmembers())
        tar.extractall(export, filter='data')
    manifest = dict(format='bigbrotr-source/1', revision=revision, tree=tree, archive_sha256=sha(archive),
                    files_sha256={str(p.relative_to(export)):sha(p) for p in sorted(export.rglob('*')) if p.is_file()})
    write(build/'source-manifest.json', manifest)
    if not reuse:
        for name in ['target','cargo-home']:
            original = cache/'out/build'/name
            assert original.is_dir() and not original.is_symlink()
            subprocess.run(['cp','-a','--reflink=auto',str(original),str(build/name)],check=True,timeout=120)
        write(root/'cache-origin.json',dict(root=str(cache), owner='1ae77d53919b4876eeff7df396179ccd',
            source_manifest_sha256=sha(cache/'out/build/source-manifest.json'), scope='copied compiler caches; fresh current build'))
    else:
        write(root/'artifact-origin.json',dict(root=str(previous),source=reuse,owner=old['owner']))
    assert shutil.disk_usage(base).free > 8*1024**3
    args = ['docker','create','--init','--name','bigbrotr-cursor-keyring-'+revision[:7],
            '--label',label+'='+owner,'--cpus','2','--memory','6g','--memory-swap','6g','--pids-limit','512',
            '--network','none','--user',f'{os.getuid()}:{os.getgid()}', '--read-only', '--cap-drop','ALL',
            '--cap-add','SYS_PTRACE','--security-opt','seccomp=unconfined','--security-opt','no-new-privileges',
            '--tmpfs','/tmp:rw,exec,nosuid,nodev,size=512m',
            '--mount',f'type=bind,src={out},dst=/out',
            '--mount',f'type=bind,src={export},dst=/out/build/source,readonly',
            '--mount',f'type=bind,src={instrument}/out,dst=/instrument,readonly']
    if reuse:
        for name in ['out/build/source','out/build/source-manifest.json','out/native-tests']:
            args += ['--mount',f'type=bind,src={previous}/{name},dst=/reuse/{name},readonly']
    args += [image,'python3','-B','/out/build/source/experiments/cursor-keyring/native.py']
    if reuse:
        args += ['--reuse']
    container = cmd(*args)
    state.update(state='running', containers=[dict(name='rust-keyring',id=container)])
    write(root/'resources.json',state)
    cmd('docker','start',container)
    deadline = time.monotonic()+2000
    while time.monotonic()<deadline:
        c=json.loads(cmd('docker','inspect',container))[0]
        assert c['Config']['Labels'][label] == owner
        if not c['State']['Running']:
            break
        time.sleep(.5)
    else:
        raise TimeoutError('owned native lab deadline')
    assert c['State']['ExitCode'] == 0 and c['State']['Pid'] == 0 and not c['State']['OOMKilled']
    assert json.loads((out/'native-tests/complete.json').read_text())['status'] == 'PASS'
    state['qualification'] = 'PASS'
except BaseException as error:
    state.update(qualification='INCOMPLETE', failure=type(error).__name__)
    raise
finally:
    if container:
        c=json.loads(cmd('docker','inspect',container))[0]
        assert c['Config']['Labels'][label] == owner
        if c['State']['Running']:
            cmd('docker','kill',container)
        c=json.loads(cmd('docker','inspect',container))[0]
        assert c['State']['Pid'] == 0 and not c['State']['Running']
        logs = subprocess.run(['docker','logs',container], check=True, stdout=subprocess.PIPE,
                              stderr=subprocess.STDOUT, timeout=60)
        (root/'container.log').write_bytes(logs.stdout)
        state['terminal']=projection(c)
    now, final = census()
    before={c['id']:c for c in preflight['containers']}
    assert all(projection(c)==before[c['Id']] for c in now if c['Id'] in before)
    assert set(before) <= {c['Id'] for c in now}
    final['preexisting_preserved']=len(before)
    write(root/'final-census.json',final)
    state.update(state='stopped-retained',finished_at=final['at'])
    write(root/'resources.json',state)
