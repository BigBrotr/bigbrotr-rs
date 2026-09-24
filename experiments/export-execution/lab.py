#!/usr/bin/env python3
"""Isolated trusted Git schema qualification with a retained native seed producer.

Original4e ELF creates the real archive and0813 acceptance. Default mode qualifies
SQL constraints only; --rust-adapter separately compiles and exercises the current
Rust consumer against that retained native worker and independently checked seed.
"""
import base64, datetime, hashlib, json, os, re, secrets, shutil, subprocess, sys, tarfile, time, traceback, urllib.request
from pathlib import Path

revision, tree = sys.argv[1:3]
responsive = sys.argv[3:4] == ["--runtime-responsive"]
busy = sys.argv[3:4] == ["--runtime-busy"]
pause_commits = sys.argv[3:4] == ["--runtime-pause-commits"]
job_status = sys.argv[3:4] == ["--runtime-job-status"]
revocation = sys.argv[3:4] == ["--runtime-revocation"]
parking = busy or pause_commits or sys.argv[3:4] == ["--runtime-parking"]
executor = parking or revocation or job_status or sys.argv[3:4] == ["--runtime-executor"]
runtime = responsive or executor
reuse_runtime = sys.argv[4] if runtime and len(sys.argv)==5 else None
assert reuse_runtime is None or re.fullmatch("[0-9a-f]{40}", reuse_runtime)
rollback = sys.argv[3:] == ["--rust-rollback"]
adapter = sys.argv[3:] in (["--rust-adapter"], ["--rust-rollback"])
assert sys.argv[3:] in ([], ["--rust-adapter"], ["--rust-rollback"], ["--runtime-responsive"], ["--runtime-executor"], ["--runtime-parking"], ["--runtime-busy"], ["--runtime-pause-commits"], ["--runtime-revocation"], ["--runtime-job-status"]) or (runtime and len(sys.argv)==5)
compile_current = adapter or (runtime and not reuse_runtime)
assert all(re.fullmatch('[0-9a-f]{40}', v) for v in [revision, tree])
base = Path('/home/tux/bigbrotr-implementation-20260917')
root = base / 'runs' / (('export-runtime-' if executor else 'export-responsive-' if responsive else 'export-rollback-' if rollback else 'export-adapter-' if adapter else 'export-execution-') + revision[:7])
source, out = root / 'source', root / 'out'
cache_root = base / 'runs/export-checker-fixed-4e6b5c0'
cache = cache_root / 'out/build'
rust_image = 'sha256:7e946a0c0aa4a5d357c3ae517fe9830f675cfc98fd26a9cba79b2bc957928b9d'
pg_image = 'postgres@sha256:f1c3376c26f2609ab9f29f71f824103fe2fcd8ee0346485cb6122a4f93df6f94'
label = 'org.bigbrotr.export-execution'

def cmd(*args):
    return subprocess.check_output(args, text=True, timeout=120).strip()

def sha(path):
    with path.open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()

def write(path, value):
    path.write_text(json.dumps(value, indent=2) + '\n')

def inspect(ident):
    return json.loads(cmd('docker', 'inspect', ident))[0]

def stable(c):
    return dict(id=c['Id'], name=c['Name'], image=c['Image'],
                lifecycle={k: c['State'][k] for k in ['Status', 'Running', 'Paused', 'Restarting', 'OOMKilled', 'Dead', 'Pid', 'ExitCode', 'Error', 'StartedAt', 'FinishedAt']},
                mounts=sorted(c['Mounts'], key=lambda m: m['Destination']),
                networks=c['NetworkSettings']['Networks'], restart=c['HostConfig']['RestartPolicy'])

assert cmd('git', '-C', str(source), 'rev-parse', 'HEAD') == revision
assert cmd('git', '-C', str(source), 'rev-parse', 'HEAD^{tree}') == tree
assert not cmd('git', '-C', str(source), 'status', '--porcelain', '--untracked-files=all')
prior = json.loads(cmd('docker', 'inspect', *cmd('docker', 'ps', '-aq').split()))
for c in prior:
    c['Config'].pop('Env', None)
assert not any('bigbrotr' in c['Name'] and c['State']['Running'] for c in prior)
preflight = dict(containers=prior, directories=sorted(p.name for p in (base / 'runs').iterdir()),
                 tcp_wmem=Path('/proc/sys/net/ipv4/tcp_wmem').read_text(), ports=cmd('ss', '-lntup'), processes=cmd('ps', '-eo', 'pid,ppid,stat,comm'),
                 volumes=cmd('docker', 'volume', 'ls'), networks=cmd('docker', 'network', 'ls'),
                 free_bytes=shutil.disk_usage(base).free, architecture=cmd('uname', '-m'))
assert preflight['architecture'] == 'x86_64' and preflight['free_bytes'] > (12 if compile_current else 4)*1024**3
# Copy the exact historical native seed producer, not compiler caches.
# It establishes the original0813 state; only new SQL is under qualification.
write(root / 'preflight.json', preflight)
old = json.loads((cache_root / 'resources.json').read_text())
assert old['owner'] == '3f840fe30a3ffef7f9944c72ea6e38f9'
for entry in old['containers']:
    old_container = next(c for c in prior if c['Id'] == entry['id'])
    assert old_container['State']['Status'] == 'exited' and old_container['State']['Pid'] == 0
    assert old_container['Config']['Labels'][old['owner_label']] == old['owner']
assert sha(source / 'Cargo.lock') == sha(cache / 'source/Cargo.lock')
owner = secrets.token_hex(16)
state = dict(source=revision, tree=tree, owner=owner, root=str(root), state='preparing', containers=[],
             rust_image=rust_image, owner_label=label, postgres_image=pg_image,
             started_at=datetime.datetime.now(datetime.timezone.utc).isoformat(),
             runtime_profile='job-status' if job_status else 'revocation' if revocation else 'busy' if busy else 'pause-commits' if pause_commits else 'parking' if parking else 'executor' if executor else 'responsive' if responsive else None,
             adapter_profile='rollback' if rollback else 'completion' if adapter else None)
def save(): write(root / 'resources.json', state)
save()
audit = dict(status='INCOMPLETE', source=revision, owner=owner, cases={})

def owned(ident):
    c = inspect(ident)
    assert c['Config']['Labels'][label] == owner
    return c

def mount(host, target, readonly=False):
    return ['--mount', f'type=bind,src={host},dst={target}' + (',readonly' if readonly else '')]

def start(name, image, arguments, mounts, network, memory='6g', pids='512', readonly=True, user=True):
    args = ['docker', 'create', '--init', '--name', 'bigbrotr-export-execution-' + revision[:7] + '-' + name,
            '--label', label + '=' + owner, '--cpus', '2', '--memory', memory, '--memory-swap', memory,
            '--pids-limit', pids, '--network', network, '--shm-size', '256m']
    if user:
        args += ['--user', f'{os.getuid()}:{os.getgid()}']
    if readonly:
        args += ['--read-only', '--cap-drop', 'ALL', '--security-opt', 'no-new-privileges',
                 '--tmpfs', '/tmp:rw,exec,nosuid,nodev,size=512m']
    ident = cmd(*args, *mounts, image, *arguments)
    state['containers'].append(dict(name=name, id=ident))
    save()
    cmd('docker', 'start', ident)
    return ident

def finish(ident, name, seconds):
    deadline = time.monotonic() + seconds
    while owned(ident)['State']['Running'] and time.monotonic() < deadline:
        time.sleep(.5)
    c = owned(ident)
    if c['State']['Running']:
        cmd('docker', 'kill', ident)
        c = owned(ident)
    assert not c['State']['Running'] and c['State']['Pid'] == 0
    (root / (name + '.container.log')).write_text(cmd('docker', 'logs', ident) + '\n')
    audit.setdefault('container_terminals', {})[name] = stable(c)
    return c['State']['ExitCode'] == 0 and not c['State']['OOMKilled']

try:
    build = out / 'build'
    build.mkdir(parents=True, mode=0o700)
    export = build / 'source'
    export.mkdir()
    archive = build / 'source.tar'
    cmd('git', '-C', str(source), 'archive', '--format=tar', '--output', str(archive), revision)
    with tarfile.open(archive) as tar:
        assert all(not Path(e.name).is_absolute() and '..' not in Path(e.name).parts and (e.isfile() or e.isdir()) for e in tar.getmembers())
        tar.extractall(export, filter='data')
    manifest = dict(format='bigbrotr-source/1', revision=revision, tree=tree, archive_sha256=sha(archive),
                    files_sha256={str(p.relative_to(export)): sha(p) for p in sorted(export.rglob('*')) if p.is_file()})
    write(build / 'source-manifest.json', manifest)
    artifact_source='4e6b5c011c067800f52e67365b2b848b7943d4bc'
    assert old['source']==artifact_source and old['state']=='stopped-retained'
    original_manifest=json.loads((cache/'source-manifest.json').read_text())
    assert original_manifest['revision']==artifact_source
    assert all(sha(cache/'source'/name)==digest for name,digest in original_manifest['files_sha256'].items())
    old_ready=json.loads((cache_root/'out/native-tests/ready.json').read_text())
    assert old_ready['source']==artifact_source and old_ready['source_manifest_sha256']==sha(cache/'source-manifest.json')
    for directory in ['native-tests','archive-worker']:
        original=cache_root/'out'/directory
        assert not any(p.is_symlink() for p in original.rglob('*'))
        shutil.copytree(original,out/directory)
    artifacts={key:('archive-worker/bigbrotr-archive-worker' if key=='worker' else 'native-tests/'+name) for key,name in [('control','bigbrotr'),('worker','bigbrotr-archive-worker'),('event_reader','archive-event-probe'),('publisher','archive-publication-probe'),('reader','archive-read-probe')]}
    assert all(sha(out/name)==old_ready[key] for key,name in artifacts.items())
    before=json.loads((cache_root/'native-assets.json').read_text())
    assert all(sha(out/'archive-worker'/name)==meta['sha256'] for name,meta in before.items())
    write(root/'artifact-source-manifest.json',original_manifest)
    write(root/'artifact-reuse.json',dict(source=artifact_source,tree=old['tree'],owner=old['owner'],root=str(cache_root),
         source_manifest_sha256=sha(cache/'source-manifest.json'),
         artifacts={key:old_ready[key] for key in artifacts},native_assets=before,
         validation='Original4e producer/reader only seeds and independently verifies the original0813 archive/job fixture. This reuse record covers original seed artifacts only. Current compilation/adapter evidence, when selected, is recorded separately under adapter-checks and adapter-cases.'))
    state['artifact_origin']=artifact_source;save()
    if reuse_runtime:
        runtime_root=base/'runs'/(('export-runtime-' if executor else 'export-responsive-')+reuse_runtime[:7])
        runtime_state=json.loads((runtime_root/'resources.json').read_text())
        assert runtime_state['source']==reuse_runtime and runtime_state['state']=='stopped-retained'
        for entry in runtime_state['containers']:
            item=next(c for c in prior if c['Id']==entry['id'])
            assert item['Config']['Labels'][runtime_state['owner_label']]==runtime_state['owner']
            assert not item['State']['Running'] and item['State']['Pid']==0
        runtime_manifest=json.loads((runtime_root/'out/build/source-manifest.json').read_text())
        assert runtime_manifest['revision']==reuse_runtime
        assert all(sha(runtime_root/'out/build/source'/name)==digest for name,digest in runtime_manifest['files_sha256'].items())
        def product_inputs(files):
            return {name:digest for name,digest in files.items() if not name.startswith(('docs/','experiments/'))}
        assert product_inputs(manifest['files_sha256'])==product_inputs(runtime_manifest['files_sha256'])
        checks=json.loads((runtime_root/'out/adapter-checks/result.json').read_text())
        assert checks['status']=='PASS'
        built=json.loads((runtime_root/'out/adapter-checks/artifacts.json').read_text())
        assert built['source']==reuse_runtime and built['source_manifest_sha256']==sha(runtime_root/'out/build/source-manifest.json')
        assert built['build_info']['source']['revision']==reuse_runtime
        assert built['build_info']['archive_worker']['sha256']==built['artifacts']['bigbrotr-archive-worker']
        current=out/'runtime-artifacts';current.mkdir()
        ready=dict(old_ready)
        for key,name,target in [('control','bigbrotr','native-tests'),('worker','bigbrotr-archive-worker','archive-worker')]:
            original=runtime_root/'out/runtime-artifacts'/name
            assert not original.is_symlink() and sha(original)==built['artifacts'][name]
            shutil.copy2(original,current/name)
            installed=out/target/name
            assert not installed.is_symlink() and sha(installed)==old_ready[key]
            installed.unlink();shutil.copy2(original,installed)
            ready[key]=sha(installed)
        write(out/'seed-ready.json',old_ready)
        ready.update(runtime_source=reuse_runtime,runtime_source_manifest_sha256=built['source_manifest_sha256'])
        write(out/'native-tests/ready.json',ready)
        write(root/'runtime-source-manifest.json',runtime_manifest)
        write(root/'runtime-reuse.json',dict(root=str(runtime_root),owner=runtime_state['owner'],
            build=built,validation='Exact product/build input equality; fixture-only changes. No new compilation or Clippy run.'))
        state['runtime_artifact_origin']=reuse_runtime;save()
    if compile_current:
        # Original seed cache is stopped and inventoried above; copy only caches,
        # then compile current trusted Git inputs. Original remains read-only.
        for name in ['target','cargo-home']:
            assert (cache/name).is_dir() and not (cache/name).is_symlink()
            subprocess.run(['cp','-a','--reflink=auto',str(cache/name),str(build/name)],check=True,timeout=120)
        write(root/'cache-origin.json',dict(root=str(cache_root),source=artifact_source,
            source_manifest_sha256=sha(cache/'source-manifest.json'),scope='Copied compiler caches only; fresh current Rust compilation required.'))

    assert shutil.disk_usage(base).free>(8*1024**3 if compile_current else 512*1024**2)
    if responsive or busy:
        instrument_root=base/'runs/strace-exit-9b30c35'
        instrument_state=json.loads((instrument_root/'resources.json').read_text())
        assert instrument_state['owner']=='84ac8cbf5292679b8ef7043fed7a2799'
        assert instrument_state['state']=='stopped-retained'
        for entry in instrument_state['containers']:
            item=next(c for c in prior if c['Id']==entry['id'])
            assert item['Config']['Labels']['org.bigbrotr.strace-exit']==instrument_state['owner']
            assert not item['State']['Running'] and item['State']['Pid']==0
        instrument=out/'checker-instrument';instrument.mkdir()
        expected={'strace-exit-fixed':'509ea4b8a7eb303990155f013e1e288df1054d63e5c81b11bc3c31b5905b7458',
                  'result.json':'cf0317fc8934eb70f3cf2d74ac6ba0680be09366a4ade586aabd95991c6c3abc'}
        for name,digest in expected.items():
            assert sha(instrument_root/'out'/name)==digest
            shutil.copy2(instrument_root/'out'/name,instrument/name)
            assert sha(instrument/name)==digest
        write(root/'checker-instrument.json',dict(source=instrument_state['source'],owner=instrument_state['owner'],root=str(instrument_root),files=expected))
    native_assets = out/'archive-worker'
    write(root / 'rust-image.json', json.loads(cmd('docker', 'image', 'inspect', rust_image))[0])
    state['network_id'] = cmd('docker', 'network', 'create', '--internal', '--label', label + '=' + owner, 'bigbrotr-export-execution-' + revision[:7])
    save()
    secret = root / 'secrets'
    secret.mkdir(mode=0o755)
    for role in ['postgres', 'BootstrapMixed', 'CatalogManager', 'bigbrotr_control', 'bigbrotr_receipt_reader', 'bigbrotr_receipt_denied']:
        path = secret / role
        path.write_text(secrets.token_hex(32))
        path.chmod(0o444)
    data = root / 'pgdata'
    data.mkdir(mode=0o777)
    data.chmod(0o777)
    pg = start('postgres', pg_image, ['bash', '-c', 'while :; do docker-entrypoint.sh postgres -c max_connections=24 -c shared_buffers=128MB; sleep 0.1; done'],
               mount(data, '/var/lib/postgresql/data') + mount(secret, '/secrets', True) +
               ['--env', 'POSTGRES_PASSWORD_FILE=/secrets/postgres', '--env', 'POSTGRES_INITDB_ARGS=--auth-host=scram-sha-256 --auth-local=trust'],
               state['network_id'], memory='1g', pids='128', readonly=False, user=False)
    state['postgres_id'] = pg
    save()
    for _ in range(100):
        if subprocess.run(['docker', 'exec', pg, 'pg_isready', '-h', '127.0.0.1', '-U', 'postgres'], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL).returncode == 0: break
        time.sleep(.1)
    else: raise RuntimeError('PG readiness timeout')
    for role in ['BootstrapMixed', 'CatalogManager', 'bigbrotr_control', 'bigbrotr_receipt_reader', 'bigbrotr_receipt_denied']:
        options = 'CREATEROLE NOINHERIT' if role == 'CatalogManager' else 'NOCREATEROLE'
        password = (secret / role).read_text()
        assert re.fullmatch('[0-9a-f]{64}', password)
        sql = f'CREATE ROLE "{role}" LOGIN NOSUPERUSER NOCREATEDB {options} NOREPLICATION NOBYPASSRLS PASSWORD ' + "'" + password + "';"
        subprocess.run(['docker', 'exec', '-i', pg, 'psql', '-X', '-v', 'ON_ERROR_STOP=1', '-U', 'postgres'], input=sql, text=True, capture_output=True, check=True, timeout=10)
    del password, sql
    assert cmd('docker','exec',pg,'psql','-XAt','-U','postgres','-c',"SELECT rolcanlogin AND rolcreaterole AND NOT(rolsuper OR rolcreatedb OR rolinherit OR rolreplication OR rolbypassrls) FROM pg_roles WHERE rolname='CatalogManager'") == 't'
    state['server'] = cmd('docker', 'exec', pg, 'psql', '-XAt', '-U', 'postgres', '-c', "SELECT version()||' fsync='||current_setting('fsync')||' full_page_writes='||current_setting('full_page_writes')")
    save()
    wrapper = """import subprocess,time
from pathlib import Path
deadline=time.monotonic()+2400
while not Path('/out/qualification-complete').exists():
    if time.monotonic()>=deadline: raise TimeoutError('qualification completion marker')
    time.sleep(.2)
"""
    runner = start('rust-visibility', rust_image, ['python3', '-B', '-c', wrapper],
        mount(out, '/out') + mount(export, '/out/build/source', True) + mount(native_assets, '/assets', True) + mount(secret, '/secrets', True), 'container:' + pg)
    state['container_id'] = runner
    state['state'] = 'ready-reused-artifacts'
    save()
    deadline=time.monotonic()+2100
    while not (out/'native-tests/ready.json').exists():
        assert owned(runner)['State']['Running']
        if time.monotonic()>=deadline: raise TimeoutError('build readiness')
        time.sleep(.5)
    if compile_current:
        state['state']='building-current-rust-adapter';save()
        with (root/'adapter-build.log').open('xb') as log:
            compiled=subprocess.run(['docker','exec',runner,'python3','-B','/out/build/source/experiments/export-execution/native.py',*(['--runtime'] if runtime else [])],stdout=log,stderr=subprocess.STDOUT,timeout=1800)
        assert compiled.returncode==0
    state['state']='qualifying-runtime' if runtime else 'qualifying-rust-adapter' if adapter else 'qualifying-schema';save()
    with (root/'schema-cases.log').open('xb') as log:
        cases=subprocess.run(['python3','-B',str(source/('experiments/export-execution/runtime_cases.py' if executor else 'experiments/archive-engine/export_job_http_cases.py' if responsive else 'experiments/export-execution/adapter_cases.py' if adapter else 'experiments/export-execution/cases.py')),str(root),*(['responsive'] if responsive else ['job-status'] if job_status else ['revocation'] if revocation else ['busy'] if busy else ['pause-commits'] if pause_commits else ['parking'] if parking else [])],stdout=log,stderr=subprocess.STDOUT,timeout=240)
    result=json.loads((root/('runtime-cases.json' if executor else 'export-job-http-cases.json' if responsive else 'adapter-cases.json' if adapter else 'schema-cases.json')).read_text())
    audit['result']=result['status']
    audit['independent_sql']={}
    for database in ['bbpub_normal','bbpub_control_normal']:
        sql="SELECT json_build_object('backends',(SELECT count(*) FROM pg_stat_activity WHERE pid<>pg_backend_pid() AND datname=current_database()))"
        observed=json.loads(cmd('docker','exec',pg,'psql','-XAt','-U','postgres','-d',database,'-c',sql))
        audit['independent_sql'][database]=observed
        assert observed==dict(backends=0)
    assert cases.returncode==0 and result['status']=='PASS'
    (out/'qualification-complete').write_text('PASS\n')
    audit['container_ok']=finish(runner,'rust-visibility',30)
    assert audit['container_ok']

except BaseException as error:
    audit['failure'] = dict(kind=type(error).__name__, frames=[dict(file=Path(f.filename).name,line=f.lineno,function=f.name) for f in traceback.extract_tb(error.__traceback__)])
    print('native laboratory failure: ' + type(error).__name__, flush=True)
finally:
    cleanup_errors = []
    for ident in cmd('docker', 'ps', '-aq', '--filter', 'label=' + label + '=' + owner).split():
        if ident and not any(c['id'].startswith(ident) for c in state['containers']):
            c = owned(ident)
            state['containers'].append(dict(name=c['Name'], id=c['Id']))
    save()
    for entry in state['containers']:
        try:
            c = owned(entry['id'])
            if c['State']['Running']:
                cmd('docker', 'stop', '--time', '10', entry['id'])
            c = owned(entry['id'])
            assert not c['State']['Running'] and c['State']['Pid'] == 0 and not c['State']['OOMKilled']
            audit.setdefault('container_terminals', {})[entry['name']]=stable(c)
            (root/(entry['name']+'.container.log')).write_text(cmd('docker','logs',entry['id'])+'\n')
        except BaseException as error:
            cleanup_errors.append(dict(id=entry['id'], kind=type(error).__name__))
    if state.get('network_id'):
        network = json.loads(cmd('docker', 'network', 'inspect', state['network_id']))[0]
        assert network['Labels'][label] == owner and not network['Containers']
        cmd('docker', 'network', 'rm', state['network_id'])
        state['network_state'] = 'removed after owner/no-endpoint audit'
    after = json.loads(cmd('docker', 'inspect', *[c['Id'] for c in prior]))
    audit['preexisting_preserved'] = all(stable(a) == stable(b) for a,b in zip(prior,after))
    audit['preexisting_count'] = len(prior)
    write(root/'preexisting-terminal.json',[stable(c) for c in after])
    audit['unrelated_running'] = sum(c['State']['Running'] for c in after)
    audit['source_unchanged'] = all(sha(export/name)==digest for name,digest in manifest['files_sha256'].items()) if 'manifest' in globals() else False
    audit['source_checkout_clean'] = not cmd('git', '-C', str(source), 'status', '--porcelain', '--untracked-files=all')
    audit['cleanup_errors'] = cleanup_errors
    audit['original_source_unchanged'] = all(sha(cache/'source'/name)==digest for name,digest in original_manifest['files_sha256'].items())
    audit['host_tcp_wmem_preserved'] = Path('/proc/sys/net/ipv4/tcp_wmem').read_text() == preflight['tcp_wmem']
    assert audit['host_tcp_wmem_preserved']
    audit['free_bytes'] = shutil.disk_usage(base).free
    audit['status'] = 'PASS_PENDING_CAPTURE' if not audit.get('failure') and not cleanup_errors and audit.get('result') == 'PASS' and audit.get('container_ok') and audit['preexisting_preserved'] and audit['source_unchanged'] and audit['source_checkout_clean'] and audit['original_source_unchanged'] else 'INCOMPLETE'
    write(root / 'terminal-audit.json', audit)
    state['state'] = 'stopped-retained' if not cleanup_errors else 'cleanup-incomplete'
    state['terminal_audit'] = 'terminal-audit.json'
    save()
    print(json.dumps(dict(status=audit['status'], owner=owner, root=str(root))), flush=True)
