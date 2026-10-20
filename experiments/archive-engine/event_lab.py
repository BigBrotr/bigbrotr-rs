#!/usr/bin/env python3
"""Run the existing native-row cases with qualified, retained build artifacts.

Reuses the setup and resource custody of the retained event-query-absence882654e
launcher. No compilation, source mutation, cache copying or other event campaign.
The case runner owns semantic results; this outer owner supplies terminal custody.
"""
import argparse
import datetime
import hashlib
import json
import os
from pathlib import Path
import re
import secrets
import shutil
import subprocess
import time
import traceback


def command(*args):
    return subprocess.check_output(args, text=True, timeout=120).strip()


def sha(path):
    with path.open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def write(path, value):
    path.write_text(json.dumps(value, indent=2) + '\n')


def pinned(path, expected):
    assert path.is_file() and not path.is_symlink()
    assert path.stat().st_size == expected['bytes'] and sha(path) == expected['sha256']


def inspect(identity):
    return json.loads(command('docker', 'inspect', identity))[0]


def stable(container):
    return dict(id=container['Id'], name=container['Name'], image=container['Image'],
                lifecycle={k: container['State'][k] for k in (
                    'Status', 'Running', 'Paused', 'Restarting', 'OOMKilled',
                    'Dead', 'Pid', 'ExitCode', 'Error', 'StartedAt', 'FinishedAt')},
                mounts=sorted(container['Mounts'], key=lambda m: m['Destination']),
                networks=container['NetworkSettings']['Networks'],
                restart=container['HostConfig']['RestartPolicy'])


def run(root, build, application_build):
    os.umask(0o077)
    source = root / 'source'
    assert root.resolve() == root and root.parent == Path('/home/tux/bigbrotr-implementation-20260917/runs')
    assert root.stat().st_uid == os.getuid() and root.stat().st_mode & 0o077 == 0
    assert Path(__file__).resolve() == source / 'experiments/archive-engine/event_lab.py'
    revision = command('git', '-C', str(source), 'rev-parse', 'HEAD')
    tree = command('git', '-C', str(source), 'rev-parse', 'HEAD^{tree}')
    assert all(re.fullmatch('[0-9a-f]{40}', value) for value in (revision, tree))
    assert not command('git', '-C', str(source), 'status', '--porcelain', '--untracked-files=all')
    assert not (root / 'resources.json').exists() and not (root / 'out').exists()
    qualification = json.loads((build / 'out/qualification.json').read_text())
    assert qualification['status'] == 'PASS_EXPORT_NATIVE_ROW_BUILD_ONLY'
    assert qualification['source'] == revision and qualification['tree'] == tree
    manifest_path = build / 'out/build/source-manifest.json'
    assert sha(manifest_path) == qualification['source_manifest_sha256']
    manifest = json.loads(manifest_path.read_text())
    assert all(sha(source / name) == digest for name, digest in manifest['files_sha256'].items())
    artifacts = build / 'out/artifacts'
    for name, expected in qualification['artifacts'].items():
        assert not Path(name).is_absolute() and '..' not in Path(name).parts
        pinned(artifacts / name, expected)
    app_qualification = json.loads((application_build / 'out/qualification.json').read_text())
    assert app_qualification['status'] == 'PASS_EXPORT_ADMISSION_BUILD_ONLY'
    app_source = 'f1e726f78d7f925cf84cec19117fee5d607773c5'
    worker_source = '6c42a41534a0d7ec3e425fa34b1b3c88bdceab79'
    assert app_qualification['source'] == app_source
    production = ['apps/bigbrotr/src', 'apps/bigbrotr/build.rs', 'apps/bigbrotr/Cargo.toml', 'crates', 'Cargo.toml',
                  'Cargo.lock', 'rust-toolchain.toml', 'contracts', 'tools/build.py',
                  'tools/archive_bundle.py']
    assert not command('git', '-C', str(source), 'diff', app_source, revision, '--', *production)
    assert not command('git', '-C', str(source), 'diff', worker_source, revision, '--',
                       'apps/bigbrotr-archive-worker/src', 'apps/bigbrotr-archive-worker/build.rs',
                       'apps/bigbrotr-archive-worker/Cargo.toml',
                       'crates', 'Cargo.toml', 'Cargo.lock', 'rust-toolchain.toml',
                       'contracts/release/archive-engine.json', 'tools/archive_bundle.py')
    application = application_build / 'out/artifacts/bigbrotr'
    app_identity = dict(bytes=33559112,
                        sha256='868cc5264b931245a7066ded3465b412659f8558fc07b616e84d8f04d7eada99')
    pinned(application, app_identity)
    worker_identity = dict(bytes=14421792,
                           sha256='0b9107ab422f2ae72af164d863f3e90e24e213231dcaeade9e0b5fe10320b4c1')
    pinned(artifacts / 'archive-worker/bigbrotr-archive-worker', worker_identity)
    assert app_qualification['build_info']['archive_worker'] == worker_identity
    available = shutil.disk_usage(root).free
    assert available > 4 * 1024**3
    label = 'org.bigbrotr.native-row-lab'
    owner = secrets.token_hex(16)
    rust_image = 'sha256:7e946a0c0aa4a5d357c3ae517fe9830f675cfc98fd26a9cba79b2bc957928b9d'
    pg_image = 'postgres@sha256:f1c3376c26f2609ab9f29f71f824103fe2fcd8ee0346485cb6122a4f93df6f94'
    prior_ids = command('docker', 'ps', '-aq').split()
    prior = json.loads(command('docker', 'inspect', *prior_ids)) if prior_ids else []
    for item in prior:
        assert not any(Path(m['Source']).is_relative_to(root)
                       for m in item['Mounts'] if m.get('Source', '').startswith('/'))
    write(root / 'preflight.json', dict(free_bytes=available, required_free_bytes=4 * 1024**3,
                                      containers=[stable(c) for c in prior]))
    state = dict(source=revision, tree=tree, owner=owner, owner_label=label, root=str(root),
                 state='preparing', containers=[], rust_image=rust_image, postgres_image=pg_image,
                 build=str(build), build_qualification_sha256=sha(build / 'out/qualification.json'),
                 application_source=app_source, application_build=str(application_build),
                 application_qualification_sha256=sha(application_build / 'out/qualification.json'),
                 started_at=datetime.datetime.now(datetime.timezone.utc).isoformat())
    audit = dict(status='INCOMPLETE', source=revision, owner=owner)

    def save():
        write(root / 'resources.json', state)

    def owned(identity):
        item = inspect(identity)
        assert item['Config']['Labels'][label] == owner
        return item

    def mount(host, target, readonly=False):
        return ['--mount', f'type=bind,src={host},dst={target}' + (',readonly' if readonly else '')]

    def start(name, image, arguments, mounts, network, postgres=False):
        args = ['docker', 'create', '--init', '--name', 'bigbrotr-native-row-' + owner[:10] + '-' + name,
                '--label', label + '=' + owner, '--cpus', '2',
                '--memory', '1g' if postgres else '6g', '--memory-swap', '1g' if postgres else '6g',
                '--pids-limit', '128' if postgres else '512', '--network', network, '--shm-size', '256m']
        if not postgres:
            args += ['--user', f'{os.getuid()}:{os.getgid()}', '--read-only', '--cap-drop', 'ALL',
                     '--security-opt', 'no-new-privileges', '--tmpfs', '/tmp:rw,exec,nosuid,nodev,size=512m']
        identity = command(*args, *mounts, image, *arguments)
        state['containers'].append(dict(name=name, id=identity))
        save()
        command('docker', 'start', identity)
        return identity

    save()
    out = root / 'out'
    try:
        out.mkdir()
        shutil.copytree(artifacts / 'archive-worker', out / 'archive-worker')
        shutil.copytree(artifacts / 'native-tests', out / 'native-tests')
        for name, expected in qualification['artifacts'].items():
            pinned(out / name, expected)
        ready_path = out / 'native-tests/ready.json'
        ready = json.loads(ready_path.read_text())
        assert ready['source'] == revision and ready['worker'] == worker_identity['sha256']
        shutil.copy2(ready_path, out / 'build-ready.json')
        shutil.copy2(application, out / 'native-tests/bigbrotr')
        pinned(out / 'native-tests/bigbrotr', app_identity)
        ready.update(control=app_identity['sha256'], control_source=app_source)
        write(ready_path, ready)
        write(out / 'source-manifest.json', manifest)
        state['network_id'] = command('docker', 'network', 'create', '--internal',
                                      '--label', label + '=' + owner, 'bigbrotr-native-row-' + owner[:10])
        save()
        secret = root / 'secrets'
        secret.mkdir(mode=0o755)
        secret.chmod(0o755)
        roles = ['postgres', 'BootstrapMixed', 'CatalogManager', 'bigbrotr_control',
                 'bigbrotr_receipt_reader', 'bigbrotr_receipt_denied']
        for role in roles:
            path = secret / role
            path.write_text(secrets.token_hex(32))
            path.chmod(0o444)
        data = root / 'pgdata'
        data.mkdir()
        data.chmod(0o777)
        pg = start('postgres', pg_image, ['postgres', '-c', 'max_connections=24',
                                         '-c', 'shared_buffers=128MB'],
                   mount(data, '/var/lib/postgresql/data') + mount(secret, '/secrets', True) +
                   ['--env', 'POSTGRES_PASSWORD_FILE=/secrets/postgres',
                    '--env', 'POSTGRES_INITDB_ARGS=--auth-host=scram-sha-256 --auth-local=trust'],
                   state['network_id'], postgres=True)
        state['postgres_id'] = pg
        save()
        for _ in range(100):
            if subprocess.run(['docker', 'exec', pg, 'pg_isready', '-h', '127.0.0.1', '-U', 'postgres'],
                              stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=2).returncode == 0:
                break
            time.sleep(.1)
        else:
            raise RuntimeError('PG readiness timeout')
        for role in roles[1:]:
            options = 'CREATEROLE NOINHERIT' if role == 'CatalogManager' else 'NOCREATEROLE'
            password = (secret / role).read_text()
            assert re.fullmatch('[0-9a-f]{64}', password)
            sql = f'CREATE ROLE "{role}" LOGIN NOSUPERUSER NOCREATEDB {options} NOREPLICATION NOBYPASSRLS PASSWORD \'{password}\';'
            subprocess.run(['docker', 'exec', '-i', pg, 'psql', '-X', '-v', 'ON_ERROR_STOP=1',
                            '-U', 'postgres'], input=sql, text=True, capture_output=True,
                           check=True, timeout=10)
        del password, sql
        state['server'] = command('docker', 'exec', pg, 'psql', '-XAt', '-U', 'postgres', '-c',
                                  "SELECT version()||' fsync='||current_setting('fsync')||' full_page_writes='||current_setting('full_page_writes')")
        save()
        wrapper = """import time
from pathlib import Path
deadline=time.monotonic()+600
while not Path('/out/qualification-complete').exists():
    if time.monotonic()>=deadline: raise TimeoutError('qualification completion marker')
    time.sleep(.2)
"""
        runner = start('runner', rust_image, ['python3', '-B', '-c', wrapper],
                       mount(source, '/source', True) + mount(out, '/out') +
                       mount(out / 'archive-worker', '/out/archive-worker', True) +
                       mount(out / 'native-tests', '/out/native-tests', True) +
                       mount(secret, '/secrets', True), 'container:' + pg)
        state.update(container_id=runner, state='qualifying')
        save()
        with (root / 'native-row-cases.log').open('xb') as log:
            cases = subprocess.run(['python3', '-B', str(source / 'experiments/archive-engine/event_cases.py'),
                                    str(root), '--native-row-boundary'],
                                   stdout=log, stderr=subprocess.STDOUT, timeout=480)
        result = json.loads((root / 'native-row-cases.json').read_text())
        audit['result'] = result['status']
        assert cases.returncode == 0 and result['status'] == 'PASS'
        audit['independent_sql'] = {}
        for database in ['bbpub_' + kind + mode for mode in ['row_exact', 'row_over']
                         for kind in ['', 'control_']]:
            observed = int(command('docker', 'exec', pg, 'psql', '-XAt', '-U', 'postgres',
                                   '-d', database, '-c',
                                   'SELECT count(*) FROM pg_stat_activity WHERE pid<>pg_backend_pid() AND datname=current_database()'))
            audit['independent_sql'][database] = dict(backends=observed)
            assert observed == 0
        (out / 'qualification-complete').write_text('PASS\n')
        subprocess.run(['docker', 'wait', runner], stdout=subprocess.DEVNULL, check=True, timeout=30)
        terminal = owned(runner)['State']
        assert terminal['Status'] == 'exited' and terminal['Pid'] == 0
        assert terminal['ExitCode'] == 0 and not terminal['OOMKilled']
        audit['runner_natural_exit'] = True
    except BaseException as error:
        audit['failure'] = dict(kind=type(error).__name__,
                               frames=[dict(file=Path(f.filename).name, line=f.lineno, function=f.name)
                                       for f in traceback.extract_tb(error.__traceback__)])
    finally:
        cleanup_errors = []
        try:
            discovered = command('docker', 'ps', '-aq', '--filter', 'label=' + label + '=' + owner).split()
            for identity in discovered:
                if not any(c['id'].startswith(identity) for c in state['containers']):
                    item = owned(identity)
                    state['containers'].append(dict(name=item['Name'].lstrip('/'), id=item['Id']))
        except BaseException as error:
            cleanup_errors.append(dict(operation='discover-owned', kind=type(error).__name__))
        for entry in reversed(state['containers']):
            try:
                item = owned(entry['id'])
                forced_stop = item['State']['Running']
                if forced_stop:
                    command('docker', 'stop', '--time', '10', entry['id'])
                item = owned(entry['id'])
                audit.setdefault('containers', {})[entry['name']] = dict(observation=stable(item),
                                                                       stop_requested=forced_stop)
                (root / (entry['name'] + '.container.log')).write_text(command('docker', 'logs', entry['id']) + '\n')
                terminal = item['State']
                assert terminal['Status'] == 'exited' and not terminal['Running'] and terminal['Pid'] == 0
                assert not terminal['OOMKilled'] and terminal['ExitCode'] == 0
            except BaseException as error:
                cleanup_errors.append(dict(id=entry['id'], kind=type(error).__name__))
        if state.get('network_id'):
            try:
                network = json.loads(command('docker', 'network', 'inspect', state['network_id']))[0]
                assert network['Labels'][label] == owner and not network['Containers']
                command('docker', 'network', 'rm', state['network_id'])
                state['network_state'] = 'removed after owner/no-endpoint audit'
            except BaseException as error:
                cleanup_errors.append(dict(network=state['network_id'], kind=type(error).__name__))
        audit.update(preexisting_preserved=False, source_unchanged=False, source_checkout_clean=False)
        for name, observe in (
            ('preexisting_preserved', lambda: [
                stable(c) for c in (json.loads(command('docker', 'inspect', *prior_ids)) if prior_ids else [])
            ] == [stable(c) for c in prior]),
            ('source_unchanged', lambda: all(
                sha(source / name) == digest for name, digest in manifest['files_sha256'].items())),
            ('source_checkout_clean', lambda: not command(
                'git', '-C', str(source), 'status', '--porcelain', '--untracked-files=all')),
        ):
            try:
                audit[name] = observe()
            except BaseException as error:
                cleanup_errors.append(dict(operation=name, kind=type(error).__name__))
        audit['cleanup_errors'] = cleanup_errors
        audit['status'] = ('PASS_PENDING_ROOT_REVIEW' if not audit.get('failure') and not cleanup_errors
                           and audit.get('result') == 'PASS' and audit.get('runner_natural_exit')
                           and audit['preexisting_preserved'] and audit['source_unchanged']
                           and audit['source_checkout_clean'] else 'INCOMPLETE')
        audit['scope'] = 'Native row boundary under declared read profile; no installed Export-job/pin, empirical256MiB cgroup or whole-service memory qualification.'
        write(root / 'terminal-audit.json', audit)
        state.update(state='stopped-retained' if not cleanup_errors else 'cleanup-incomplete',
                     terminal_audit='terminal-audit.json',
                     finished_at=datetime.datetime.now(datetime.timezone.utc).isoformat())
        save()
    print(json.dumps(dict(status=audit['status'], root=str(root))), flush=True)
    return 0 if audit['status'] == 'PASS_PENDING_ROOT_REVIEW' else 1


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--root', required=True, type=Path)
    parser.add_argument('--build', required=True, type=Path)
    parser.add_argument('--application-build', required=True, type=Path)
    args = parser.parse_args()
    raise SystemExit(run(args.root.resolve(), args.build.resolve(), args.application_build.resolve()))
