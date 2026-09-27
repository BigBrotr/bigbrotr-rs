#!/usr/bin/env python3
"""Fresh PostgreSQL qualification after the recorded 9b02 network setup failure.

The executable AND practical fixture retain their original Git identity. This
wrapper only provisions a new lab; it does not rebuild or reattribute Rust tests.
"""
import datetime
import hashlib
import json
from pathlib import Path
import secrets
import shutil
import subprocess
import sys

sys.dont_write_bytecode = True
source = Path(__file__).resolve().parents[2]
base = Path('/home/tux/bigbrotr-implementation-20260917/runs')
original = base / 'cursor-enrollment-9b02bd9'
product = '9b02bd9f692ec07f57954b140fcd6efd20220ebe'


def cmd(*args):
    return subprocess.check_output(args, text=True, timeout=60).strip()


def sha(path):
    with path.open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


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
    ids = cmd('docker', 'ps', '-aq').split()
    containers = json.loads(cmd('docker', 'inspect', *ids)) if ids else []
    return containers, dict(at=datetime.datetime.now(datetime.timezone.utc).isoformat(),
        containers=[projection(c) for c in containers], free_bytes=shutil.disk_usage(base).free,
        processes=cmd('ps','-eo','pid,ppid,stat,comm'), ports=cmd('ss','-lntup'),
        networks=cmd('docker','network','ls'), volumes=cmd('docker','volume','ls'),
        directories=sorted(p.name for p in base.iterdir()),
        filesystem=cmd('findmnt','-T',str(base),'-n','-o','SOURCE,FSTYPE,OPTIONS'))


revision = cmd('git','-C',str(source),'rev-parse','HEAD')
root = base / ('cursor-enrollment-retry-' + revision[:7])
assert source == root/'source' and not (root/'resources.json').exists()
assert not cmd('git','-C',str(source),'status','--porcelain','--untracked-files=all')
assert cmd('git','-C',str(original/'source'),'rev-parse','HEAD') == product
assert not cmd('git','-C',str(original/'source'),'status','--porcelain','--untracked-files=all')
old = json.loads((original/'resources.json').read_text())
assert old['source']==product and old['owner']=='203570ac651e0191d5b613262c9b2091'
assert old['state']=='stopped-retained' and old['qualification']=='INCOMPLETE' and not old['cleanup_errors']
assert not (original/'postgres/cursor-enrollment.json').exists()
manifest = json.loads((original/'out/build/source-manifest.json').read_text())
assert manifest['revision']==product
for name,digest in manifest['files_sha256'].items():
    assert sha(original/'source'/name)==digest==sha(original/'out/build/source'/name)
build = original/'out/native-tests'
assert sha(build/'bigbrotr')=='a943777fd6f064a5682e80fe95f7b8ebe92952c7f2c1fb50f28feb866e640716'
assert json.loads((build/'result.json').read_text())['status']=='PASS'
assert sha(original/'out/build/source-manifest.json')=='5cb5fa359392a5756e9283ee388a96ecc4199c4e05bcfa0818c851998a4063f9'
prior, preflight = census()
assert preflight['free_bytes']>4*1024**3
assert not any(c['State']['Running'] and 'bigbrotr' in c['Name'] for c in prior)
for entry in old['containers']:
    c=next(c for c in prior if c['Id']==entry['id'])
    assert c['Config']['Labels'][old['owner_label']]==old['owner'] and c['State']['Pid']==0
write(root/'preflight.json',preflight)
state=dict(source=revision, tree=cmd('git','-C',str(source),'rev-parse','HEAD^{tree}'),
    product_source=product, owner=secrets.token_hex(16), root=str(root), state='running',
    containers=[], qualification='INCOMPLETE', artifact_root=str(original), started_at=preflight['at'])
write(root/'resources.json',state)
write(root/'artifact-origin.json',dict(root=str(original), source=product, owner=old['owner'],
    source_manifest_sha256=sha(original/'out/build/source-manifest.json'),
    binary_sha256=sha(build/'bigbrotr'), fixture_sha256=sha(original/'source/experiments/cursor-registration/enrollment.py'),
    scope='Original executable and original practical fixture, no compilation, test or Clippy rerun.'))
sys.path.insert(0,str(original/'source/experiments/control-migrations'))
import lab
runtime=root/'postgres'
pg=None
try:
    run_name='cursor_retry_'+''.join(chr(ord('a')+int(c,16)) for c in revision[:7])
    lab.setup(runtime,build,original/'source',run_name)
    pg=json.loads((runtime/'resources.json').read_text())
    with (root/'cases.stdout').open('xb') as out, (root/'cases.stderr').open('xb') as err:
        result=subprocess.run(['python3','-B',str(original/'source/experiments/cursor-registration/enrollment.py'),'--root',str(runtime)],stdout=out,stderr=err,timeout=600)
    state['cases_exit']=result.returncode
    assert result.returncode==0
    assert json.loads((runtime/'cursor-enrollment.json').read_text())['status']=='PASS'
    state['qualification']='CHECKS_PASSED'
except BaseException as error:
    state['failure']=type(error).__name__
    raise
finally:
    errors=[]
    if pg is None and runtime.exists():
        try:pg=json.loads((runtime/'resources.json').read_text())
        except (OSError,ValueError) as error:
            errors.append(dict(resource='postgres-resource-record',error=type(error).__name__))
    if pg is not None:
        for name,label in [(pg['client']+'-cursor-fault','org.bigbrotr.cursor-registration'),(pg['client'],lab.LABEL),(pg['postgres'],lab.LABEL)]:
            try:
                ids=cmd('docker','ps','-aq','--filter','name=^/'+name+'$').split()
                if not ids:continue
                assert len(ids)==1
                c=json.loads(cmd('docker','inspect',ids[0]))[0]
                assert c['Name']=='/'+name and c['Config']['Labels'][label]==pg['owner']
                if c['State']['Running']:cmd('docker','stop','--time','10',c['Id'])
                c=json.loads(cmd('docker','inspect',c['Id']))[0]
                assert c['State']['Pid']==0 and not c['State']['Running']
                logs=subprocess.run(['docker','logs',c['Id']],capture_output=True,timeout=60,check=True)
                (root/(name+'.stdout')).write_bytes(logs.stdout);(root/(name+'.stderr')).write_bytes(logs.stderr)
                state.setdefault('terminals',[]).append(projection(c))
            except BaseException as error:errors.append(dict(resource=name,error=type(error).__name__))
        pg['state']='cleanup-unresolved' if errors else 'stopped-retained'
        try:write(runtime/'resources.json',pg)
        except OSError as error:errors.append(dict(resource='postgres-resource-record',error=type(error).__name__))
    elif state['qualification']=='CHECKS_PASSED':
        errors.append(dict(resource='postgres-resource-record',error='missing after successful execution'))
    try:
        now,final=census();before={c['id']:c for c in preflight['containers']}
        assert set(before)<={c['Id'] for c in now} and all(projection(c)==before[c['Id']] for c in now if c['Id'] in before)
        final['preexisting_preserved']=len(before)
        write(root/'final-census.json',final)
    except BaseException as error:errors.append(dict(resource='final-census',error=type(error).__name__))
    state.update(state='cleanup-unresolved' if errors else 'stopped-retained',cleanup_errors=errors,finished_at=datetime.datetime.now(datetime.timezone.utc).isoformat())
    state['qualification']='PASS' if not errors and state['qualification']=='CHECKS_PASSED' else 'INCOMPLETE'
    write(root/'resources.json',state)
    if errors:raise RuntimeError('owned resources require explicit cleanup resolution')
