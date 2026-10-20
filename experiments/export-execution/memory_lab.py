#!/usr/bin/env python3
"""Root-owned fresh Export laboratory; preserve every failed or unknown outcome."""
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
import sys
import time

sys.dont_write_bytecode = True
SOURCE = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(SOURCE / 'experiments/control-migrations'))
from lab import docker, sql, PG_IMAGE
from memory_setup import set_value
from memory_setup_io import census
from memory_trace import audit_trace

MIB = 1024**2
LABEL = 'org.bigbrotr.export-memory-lab'
CLIENT = 'sha256:720f24ff85acd081de8ee8405bb2498b24cae499075bbc29f49d19756706d1a7'
PSQL = '/usr/lib/postgresql/15/bin/psql'
PSQL_PIN = dict(path=PSQL, bytes=794144,
               sha256='b6917af4a4c78ffd554b12e8fd9a6f4ac6590719373b45b15a10f7dec99af009')
APPLICATION_SOURCE = '6c42a41534a0d7ec3e425fa34b1b3c88bdceab79'
ARTIFACTS = {
    'bigbrotr': '60f70d1b9dffb133807fb9d43e6b142f3a8784c98e91bffab4de244279573572',
    'ingest_bootstrap_probe': 'e2e134045cb45d6c1d4f6e2c36716fc9476e7abc854dcfd5e935ce65f5b5f849',
    'archive-worker/bigbrotr-archive-worker': '0b9107ab422f2ae72af164d863f3e90e24e213231dcaeade9e0b5fe10320b4c1',
}
PROBE_SHA = '046b3d2f88ec7c1a4750ad4307e5d691947fb23560159363f5462f988ce1e927'
TRACER_SHA = '509ea4b8a7eb303990155f013e1e288df1054d63e5c81b11bc3c31b5905b7458'


def require(value, message):
    if not value:
        raise ValueError(message)


def sha(path):
    with Path(path).open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def pin(path, visible=None):
    return dict(path=str(visible or path), bytes=path.stat().st_size, sha256=sha(path))


def write(path, value):
    with path.open('x') as stream:
        json.dump(value, stream, indent=2)
        stream.write('\n')


def git(*args):
    return subprocess.check_output(['git', '-C', str(SOURCE), *args], text=True).strip()


def inspect(ident):
    return json.loads(docker('inspect', ident).stdout)[0]


class Laboratory:
    def __init__(self, root, build, probe, instrument, *, application_build=None, export_memory_mib=1024,
                 signed_input='archive-sample', manager_build=None):
        require(root.is_absolute() and root.resolve() == root and not root.exists(), 'fresh canonical root')
        require(not git('status', '--porcelain', '--untracked-files=all'), 'clean committed source')
        require(shutil.disk_usage(root.parent).free >= 69324505088, 'completion reserve plus headroom')
        require(export_memory_mib in (256, 1024), 'selected finite export cap')
        self.application_source = APPLICATION_SOURCE
        self.artifacts = ARTIFACTS
        self.application_qualification = None
        self.manager_build = None
        self.manager_qualification = None
        self.reference_build = build if application_build is not None else None
        self.ingest_build = build
        self.ingest_visible = ('/reference-build' if self.reference_build is not None else '/build') + '/ingest_bootstrap_probe'
        self.export_memory_mib = export_memory_mib
        require(signed_input in ('archive-sample', 'content-heavy', 'tag-heavy'), 'selected frozen input')
        self.signed_input = signed_input
        for path, expected in ARTIFACTS.items():
            require((build / path).is_file() and not (build / path).is_symlink()
                    and sha(build / path) == expected, 'qualified artifact: ' + path)
        if application_build is not None:
            require(application_build.is_absolute() and application_build.resolve() == application_build,
                    'canonical application build root')
            resources_path = application_build / 'resources.json'
            qualification_path = application_build / 'out/qualification.json'
            manifest_path = application_build / 'out/build/source-manifest.json'
            for path in (resources_path, qualification_path, manifest_path):
                require(path.is_file() and path.resolve() == path, 'regular application build receipt')
            resources = json.loads(resources_path.read_text())
            qualification = json.loads(qualification_path.read_text())
            manifest = json.loads(manifest_path.read_text())
            source, tree = qualification['source'], qualification['tree']
            require(re.fullmatch('[0-9a-f]{40}', source) and re.fullmatch('[0-9a-f]{40}', tree)
                    and git('rev-parse', source + '^{tree}') == tree, 'actual application source tree')
            require(resources['root'] == str(application_build) and resources['source'] == source
                    and resources['tree'] == tree and resources['state'] == 'stopped-retained'
                    and resources['qualification'] == qualification['status'] == 'PASS_EXPORT_ADMISSION_BUILD_ONLY',
                    'qualified application build only; no runtime qualification inferred')
            require(sha(manifest_path) == qualification['source_manifest_sha256']
                    and manifest['revision'] == source and manifest['tree'] == tree,
                    'exact application source manifest')
            info = qualification['build_info']
            require(info['source'] == dict(state='builder-supplied', revision=source, tree=tree,
                        dirty=False, manifest_sha256=qualification['source_manifest_sha256'])
                    and info['profile_assertion'] == 'release' and info['target'] == 'x86_64-unknown-linux-gnu'
                    and qualification['worker_source'] == APPLICATION_SOURCE,
                    'qualified embedded application build identity')
            require(info['archive_worker'] == qualification['artifacts']['archive-worker/bigbrotr-archive-worker']
                    == dict(bytes=(build / 'archive-worker/bigbrotr-archive-worker').stat().st_size,
                            sha256=ARTIFACTS['archive-worker/bigbrotr-archive-worker']),
                    'unchanged qualified adjacent worker pin')
            build = application_build / 'out/artifacts'
            require(build.is_dir() and build.resolve() == build, 'canonical application artifacts')
            paths = list(build.rglob('*'))
            require(all(not path.is_symlink() and (path.is_file() or path.is_dir()) for path in paths),
                    'regular application artifact tree')
            actual = {str(path.relative_to(build)): dict(bytes=path.stat().st_size, sha256=sha(path))
                      for path in paths if path.is_file()}
            require(actual == qualification['artifacts'], 'exact qualified parent and complete worker bundle')
            require(qualification['lockfile_sha256'] == info['inputs_sha256']['Cargo.lock'] == sha(SOURCE / 'Cargo.lock')
                    and all(sha(SOURCE / name) == expected for name, expected in info['inputs_sha256'].items()),
                    'current embedded build inputs')
            self.application_source = source
            self.artifacts = {name: item['sha256'] for name, item in actual.items()}
            self.application_qualification = dict(root=str(application_build), source=source, tree=tree,
                resources=pin(resources_path), qualification=pin(qualification_path),
                source_manifest=pin(manifest_path), status=qualification['status'], build_info=info)
        # As in event_lab.py, compare installed build inputs, not unrelated
        # research examples. The retained acceptance probe has its own exact pin.
        require(not git('diff', '--name-only', self.application_source, 'HEAD', '--',
            'apps/bigbrotr/src', 'apps/bigbrotr/build.rs', 'apps/bigbrotr/Cargo.toml',
            'apps/bigbrotr-archive-worker/src', 'apps/bigbrotr-archive-worker/build.rs',
            'apps/bigbrotr-archive-worker/Cargo.toml',
            'crates', 'Cargo.toml', 'Cargo.lock', 'rust-toolchain.toml',
            'tools/build.py', 'tools/archive_bundle.py', 'contracts', 'migrations'), 'unchanged qualified product inputs')
        if manager_build is not None:
            require(self.application_qualification is not None
                    and self.application_source == 'f1e726f78d7f925cf84cec19117fee5d607773c5',
                    'manager measurement uses the qualified f1e installed application')
            require(manager_build.is_absolute() and manager_build.resolve() == manager_build
                    and not root.is_relative_to(manager_build), 'separate canonical manager build root')
            resources_path = manager_build / 'resources.json'
            qualification_path = manager_build / 'out/qualification.json'
            manifest_path = manager_build / 'out/build/source-manifest.json'
            result_path = manager_build / 'out/manager-memory-checks/result.json'
            for path in (resources_path, qualification_path, manifest_path, result_path):
                require(path.is_file() and path.resolve() == path, 'regular manager build receipt')
            resources = json.loads(resources_path.read_text())
            qualification = json.loads(qualification_path.read_text())
            manifest = json.loads(manifest_path.read_text())
            result = json.loads(result_path.read_text())
            source, tree = qualification['source'], qualification['tree']
            require(re.fullmatch('[0-9a-f]{40}', source) and re.fullmatch('[0-9a-f]{40}', tree)
                    and git('rev-parse', source + '^{tree}') == tree
                    and resources['root'] == str(manager_build) and resources['source'] == source
                    and resources['tree'] == tree and resources['state'] == 'stopped-retained'
                    and resources['qualification'] == qualification['status'] == 'PASS_EXPORT_MANAGER_MEMORY_BUILD_ONLY',
                    'stopped exact-source manager build only')
            require(sha(manifest_path) == qualification['source_manifest_sha256']
                    and manifest['revision'] == source and manifest['tree'] == tree
                    and result == qualification['manager_memory'] and result['status'] == 'PASS'
                    and result['source'] == source and result['tree'] == tree
                    and result['source_manifest']['sha256'] == qualification['source_manifest_sha256'],
                    'matching manager source and compiler/test receipts')
            expected_checks = {'rustc-path', 'rustc-version', 'cargo-metadata', 'cargo-lib',
                               'rustc-diagnostic', 'rustc-counter-tests', 'allocator-counter-tests'}
            require(len(result['checks']) == len(expected_checks)
                    and {check['name'] for check in result['checks']} == expected_checks
                    and all(check['status'] == 'PASS' and check['returncode'] == 0 for check in result['checks'])
                    and type(result['counter_tests']) is int and result['counter_tests'] > 0
                    and result['compiler']['bytes'] > 0
                    and re.fullmatch('[0-9a-f]{64}', result['compiler']['sha256'])
                    and result['compiler']['version'].startswith('rustc 1.98.1 ')
                    and 'host: x86_64-unknown-linux-gnu' in result['compiler']['version'],
                    'nonempty all-pass deterministic tests and pinned compiler')
            require(not git('diff', '--name-only', source, 'HEAD', '--',
                'apps', 'crates', 'Cargo.toml', 'Cargo.lock', 'rust-toolchain.toml',
                'tools/build.py', 'tools/archive_bundle.py', 'contracts', 'migrations',
                'experiments/export-execution/memory_measure.rs', 'experiments/export-execution/memory.rs',
                'experiments/export-execution/memory', 'experiments/export-execution/memory_run.py',
                'experiments/export-execution/memory_oracle.py', 'experiments/export-execution/manager_memory_build.py'),
                'unchanged qualified measurement and shared product sources')
            self.manager_build = manager_build / 'out/artifacts'
            require(self.manager_build.is_dir() and self.manager_build.resolve() == self.manager_build,
                    'canonical manager artifact root')
            for name, label in [('export-manager-memory-probe', 'executable'),
                                ('export-manager-memory-tests', 'test_binary')]:
                path = self.manager_build / name
                require(path.is_file() and path.resolve() == path, 'regular qualified manager executable')
                actual = {key: value for key, value in pin(path).items() if key != 'path'}
                require(actual == qualification['artifacts'][name]
                        == {key: result[label][key] for key in ('bytes', 'sha256')}, 'manager executable bytes')
            bundle = self.manager_build / 'archive-worker'
            actual = {'archive-worker/' + str(path.relative_to(bundle)): dict(bytes=path.stat().st_size, sha256=sha(path))
                      for path in bundle.rglob('*') if path.is_file() and path.resolve() == path}
            expected = {name: value for name, value in qualification['artifacts'].items() if name.startswith('archive-worker/')}
            require(actual == expected and set(actual) == {name for name in self.artifacts if name.startswith('archive-worker/')}
                    and all(value['sha256'] == self.artifacts[name] for name, value in actual.items()),
                    'unchanged qualified manager native bundle; unrelated rlibs remain receipt-pinned')
            self.manager_qualification = dict(root=str(manager_build), source=source, tree=tree,
                resources=pin(resources_path), qualification=pin(qualification_path),
                source_manifest=pin(manifest_path), result=pin(result_path),
                executable=pin(self.manager_build / 'export-manager-memory-probe', '/manager-build/export-manager-memory-probe'))
        require(sha(probe / 'export-execution-probe') == PROBE_SHA, 'qualified new export probe')
        require(sha(instrument / 'strace-exit-fixed') == TRACER_SHA, 'qualified process tracer')
        require(inspect(CLIENT)['Id'] == CLIENT, 'exact client image')
        self.root, self.out = root, root / 'out'
        self.build, self.probe, self.instrument = build, probe, instrument
        root.mkdir(mode=0o700)
        self.out.mkdir(mode=0o700)
        self.state = dict(format='bigbrotr-export-memory-lab/1', owner=secrets.token_hex(16),
            root=str(root), source=git('rev-parse', 'HEAD'), tree=git('rev-parse', 'HEAD^{tree}'),
            source_root=str(SOURCE), image=CLIENT, postgres_image=PG_IMAGE,
            state='preparing', containers=[], started_at=datetime.datetime.now(datetime.timezone.utc).isoformat())
        if self.application_qualification is not None:
            self.state.update(application_source=self.application_source,
                application_build=self.application_qualification,
                ingest_probe_source=APPLICATION_SOURCE, reference_build=str(self.reference_build))
        if self.manager_qualification is not None:
            self.state['manager_build'] = self.manager_qualification
        if export_memory_mib != 1024:
            self.state['export_memory_mib'] = export_memory_mib
        if signed_input != 'archive-sample':
            self.state['signed_input'] = signed_input
        self.save()
        self.pg = None
        if self.application_qualification is not None:
            # Retain the supplied receipts as this lab's frozen inputs. The
            # original qualified build and its artifacts remain read-only.
            for key in ('resources', 'qualification', 'source_manifest'):
                original = self.application_qualification[key]
                raw = Path(original['path']).read_bytes()
                require(len(raw) == original['bytes'] and hashlib.sha256(raw).hexdigest() == original['sha256'],
                        'unchanged supplied application receipt')
                path = self.out / ('application-build-' + key + '.json')
                with path.open('xb') as stream:
                    stream.write(raw)
                path.chmod(0o400)
                self.application_qualification[key] = pin(path, '/lab/' + path.name)
            self.save()
        if self.manager_qualification is not None:
            for key in ('resources', 'qualification', 'source_manifest', 'result'):
                original = self.manager_qualification[key]
                raw = Path(original['path']).read_bytes()
                require(len(raw) == original['bytes'] and hashlib.sha256(raw).hexdigest() == original['sha256'],
                        'unchanged supplied manager receipt')
                path = self.out / ('manager-build-' + key + '.json')
                with path.open('xb') as stream:
                    stream.write(raw)
                path.chmod(0o400)
                self.manager_qualification[key] = pin(path, '/lab/' + path.name)
            self.save()

    def save(self):
        temporary = self.root / 'resources.tmp'
        temporary.write_text(json.dumps(self.state, indent=2) + '\n')
        temporary.replace(self.root / 'resources.json')

    def owned(self, ident):
        value = inspect(ident)
        require(value['Config']['Labels'].get(LABEL) == self.state['owner'], 'actual container owner')
        return value

    def create(self, name, arguments):
        name = 'bigbrotr-export-memory-' + self.state['owner'][:10] + '-' + name
        ident = docker('create', '--name', name, '--label', LABEL + '=' + self.state['owner'],
            '--user', f'{os.getuid()}:{os.getgid()}', '--cpus', '2', '--pids-limit', '128',
            '--read-only', '--cap-drop', 'ALL', '--security-opt', 'no-new-privileges',
            '--tmpfs', '/tmp:rw,exec,nosuid,nodev,size=128m', *arguments).stdout.strip()
        self.state['containers'].append(dict(id=ident, name=name))
        self.save()
        return ident

    def postgres(self):
        protected = self.out / 'secrets'
        protected.mkdir(mode=0o700)
        for name in ('bootstrap', 'migrator', 'application', 'manager', 'reader', 'owner'):
            path = protected / name
            with path.open('x') as stream:
                stream.write(secrets.token_hex(32))
            path.chmod(0o400)
        data = self.root / 'pg-data'
        data.mkdir(mode=0o700)
        self.pg = self.create('postgres', [
            '--memory', '1536m', '--memory-swap', '1536m', '--network', 'none',
            '--tmpfs', '/var/run/postgresql:rw,nosuid,nodev,size=16m,mode=0777',
            '--mount', f'type=bind,src={data},dst=/var/lib/postgresql/data',
            '--mount', f'type=bind,src={protected},dst=/run/secrets,readonly',
            '--env', 'POSTGRES_PASSWORD_FILE=/run/secrets/bootstrap', '--env', 'POSTGRES_DB=postgres',
            '--env', 'POSTGRES_INITDB_ARGS=--auth-local=trust --auth-host=scram-sha-256',
            PG_IMAGE, 'postgres', '-c', 'max_connections=32', '-c', 'shared_buffers=128MB',
            '-c', 'work_mem=4MB', '-c', 'log_statement=none', '-c', 'log_min_error_statement=panic',
            '-c', 'log_parameter_max_length_on_error=0'])
        docker('start', self.pg)
        deadline = time.monotonic() + 60
        while docker('exec', self.pg, 'pg_isready', '-h', '127.0.0.1', '-U', 'postgres', check=False).returncode:
            require(time.monotonic() < deadline, 'PostgreSQL startup deadline')
            time.sleep(.1)
        server = json.loads(self.sql('postgres', "SELECT json_build_object('version',current_setting('server_version_num'),"
            "'fsync',current_setting('fsync'),'full_page_writes',current_setting('full_page_writes'));"))
        require(server == dict(version='160015', fsync='on', full_page_writes='on'), 'qualified PostgreSQL settings')
        write(self.out / 'server.json', server)
        for role, secret, attributes in [
            ('bb_migrator', 'migrator', 'NOSUPERUSER NOCREATEDB NOCREATEROLE'),
            ('bb_application', 'application', 'NOSUPERUSER NOCREATEDB NOCREATEROLE'),
            ('bb_manager', 'manager', 'NOSUPERUSER NOCREATEDB CREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS'),
            ('bb_reader', 'reader', 'NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS'),
        ]:
            self.sql('postgres', f"CREATE ROLE {role} LOGIN {attributes} PASSWORD '{(protected / secret).read_text()}';")
        self.sql('postgres', 'CREATE DATABASE bb_export_control OWNER bb_migrator;')
        self.sql('postgres', 'CREATE DATABASE bb_export_catalog;')
        self.sql('bb_export_catalog', 'REVOKE ALL ON DATABASE bb_export_catalog FROM PUBLIC; REVOKE ALL ON SCHEMA public FROM PUBLIC;')
        self.state.update(state='engines-ready', postgres=self.pg)
        self.save()

    def sql(self, database, statement):
        self.owned(self.pg)
        return sql(dict(postgres=self.pg), database, statement)

    def run(self, name, command, *, trace=False, minimum_workers=1, memory_mib=3072, before_release=None):
        """A fresh cgroup and exec-only start gate preserve initial PID identity."""
        require(re.fullmatch('[a-z-]+', name), 'finite phase name')
        gate = '/lab/start-' + name
        require(not (self.out / ('start-' + name)).exists(), 'fresh phase invocation')
        if trace:
            command = ['/instrument/strace-exit-fixed', '-f', '-ttt', '-e', 'trace=process', '-s', '256',
                '-o', '/lab/' + name + '.trace', *command]
        bootstrap = ("import os,sys,time; from pathlib import Path; "
            "p=Path(sys.argv[1]); end=time.monotonic()+30\n"
            "while not p.exists():\n"
            " if time.monotonic()>end: raise TimeoutError('root start gate')\n"
            " time.sleep(.01)\n"
            "os.execvp(sys.argv[2],sys.argv[2:])")
        ident = self.create(name, [
            '--memory', str(memory_mib) + 'm', '--memory-swap', str(memory_mib) + 'm',
            '--network', 'container:' + self.pg, '--cgroupns', 'private',
            '--mount', f'type=bind,src={self.out},dst=/lab',
            '--mount', f'type=bind,src={self.build},dst=/build,readonly',
            *(['--mount', f'type=bind,src={self.reference_build},dst=/reference-build,readonly']
              if self.reference_build is not None else []),
            *(['--mount', f'type=bind,src={self.manager_build},dst=/manager-build,readonly']
              if self.manager_build is not None else []),
            '--mount', f'type=bind,src={self.probe},dst=/probe,readonly',
            '--mount', f'type=bind,src={self.instrument},dst=/instrument,readonly',
            '--mount', f'type=bind,src={SOURCE},dst=/source,readonly',
            CLIENT, 'python3', '-c', bootstrap, gate, *command])
        docker('start', ident)
        original = self.owned(ident)
        pid = original['State']['Pid']
        require(pid > 0 and original['State']['Running'], 'actual gated initial process')
        process = Path('/proc') / str(pid)
        group_line, = (process / 'cgroup').read_text().splitlines()
        require(group_line.startswith('0::/'), 'unified actual cgroup')
        group = Path('/sys/fs/cgroup') / group_line[3:].lstrip('/')
        require(group.resolve() == group and group.is_dir(), 'canonical owned cgroup')
        start = dict(container=original, pid=pid,
            start_ticks=(process / 'stat').read_text().rsplit(')', 1)[1].split()[19],
            cgroup=str(group), inode=group.stat().st_ino,
            processes=(group / 'cgroup.procs').read_text().split())
        require(start['processes'] == [str(pid)], 'sole initial container process')
        write(self.out / (name + '-start.json'), start)
        if before_release is not None:
            before_release(start)
        (self.out / ('start-' + name)).write_text('authenticated owned start\n')
        forced = False
        try:
            docker('wait', ident, timeout=180)
        except subprocess.TimeoutExpired:
            forced = True
            self.owned(ident)
            docker('kill', ident)
            raise
        finally:
            final = self.owned(ident)
            if final['State']['Running']:
                forced = True
                docker('kill', ident)
                final = self.owned(ident)
            log = docker('logs', ident, check=False)
            (self.out / (name + '.stdout')).write_text(log.stdout)
            (self.out / (name + '.stderr')).write_text(log.stderr)
            current_group = dict(path=str(group), exists=group.exists(), processes=[])
            if group.exists():
                require(group.stat().st_ino == start['inode'], 'original cgroup identity')
                current_group['processes'] = (group / 'cgroup.procs').read_text().split()
            write(self.out / (name + '-terminal.json'), dict(container=final, forced=forced, cgroup=current_group))
        require(not forced and final['State']['Status'] == 'exited' and final['State']['ExitCode'] == 0
            and final['State']['Pid'] == 0 and not final['State']['OOMKilled']
            and not current_group['processes'], 'natural terminal container and empty cgroup')
        if trace:
            write(self.out / (name + '-trace-audit.json'), audit_trace(
                (self.out / (name + '.trace')).read_text(), minimum_workers=minimum_workers))
        return start, final, current_group

    def prepare(self):
        """Generate real fresh identities; provision no product authority in SQL."""
        script = """import hashlib,json,subprocess
from pathlib import Path
p=Path('/usr/lib/postgresql/15/bin/psql')
assert p.resolve()==p and p.is_file() and p.stat().st_nlink==1
v=dict(path=str(p),bytes=p.stat().st_size,sha256=hashlib.sha256(p.read_bytes()).hexdigest())
Path('/lab/psql.json').write_text(json.dumps(v)+'\\n')
"""
        if self.application_qualification is not None:
            script += """with Path('/lab/application-build-info.json').open('x') as output:
 subprocess.run(['/build/bigbrotr','build-info'],check=True,stdout=output,env={})
"""
        script += """
subprocess.run(['/build/bigbrotr','config','init','--recipe','archive-only','--output','/lab/template.toml'],check=True,env={})
"""
        self.run('configure', ['python3', '-c', script])
        require(json.loads((self.out / 'psql.json').read_text()) == PSQL_PIN, 'actual canonical client executable')
        if self.application_qualification is not None:
            require(json.loads((self.out / 'application-build-info.json').read_text())
                    == self.application_qualification['build_info'], 'actual installed parent embedded build info')
        text = (self.out / 'template.toml').read_text()
        values = [
            ('config', 'authority_mode', 'managed'),
            ('topology', 'local_roles', ['control', 'ingest', 'sync', 'archive']),
            ('administration', 'enabled', True), ('administration.listen', 'port', 18081),
            ('sync', 'enabled', True), ('sync', 'provenance', 'local_first_witness'),
            ('ingest', 'producer_outstanding_requests', 32),
            ('archive.publisher', 'enabled', True), ('archive.reader', 'enabled', False),
            ('archive', 'catalog_id', secrets.token_hex(16)), ('archive', 'worker_memory_bytes', '512MiB'),
            ('runtime', 'lease_ttl', '6s'), ('runtime', 'heartbeat_interval', '1s'),
            ('runtime', 'renewal_timeout', '1s'), ('runtime', 'renewal_safety_margin', '1s'),
            ('runtime', 'graceful_shutdown_timeout', '2s'), ('runtime', 'authorization_ttl', '5s'),
            ('runtime.processes.application', 'memory_max_bytes', '1088MiB'),
            ('runtime.processes.application', 'baseline_bytes', '64MiB'),
            ('runtime.processes.application', 'payload_bytes', '768MiB'),
            ('runtime.processes.application', 'native_bytes', '192MiB'),
            ('runtime.processes.application', 'completion_reserve_bytes', '64MiB'),
            ('runtime.processes.native', 'memory_max_bytes', '1984MiB'),
            ('read.execution', 'max_memory_bytes', '1GiB'),
            ('export.memory', 'max_bytes', '1GiB' if self.export_memory_mib == 1024 else '256MiB'),
            ('export.destination', 'id', secrets.token_hex(16)),
            ('export.destination', 'root', '/lab/fresh-root/destination'),
            ('network.dns', 'resolver', '127.0.0.1:9'), ('network.tls', 'extra_roots', []),
            ('postgres.catalog', 'read_user', 'bb_reader'),
            ('postgres.catalog', 'read_password_ref', 'file:/lab/secrets/reader'),
        ]
        for key, directory in [('ingest_root', 'store'), ('archive_root', 'data'),
                               ('archive_state_root', 'state'), ('scratch_root', 'scratch')]:
            values.append(('storage', key, '/lab/fresh-root/' + directory))
        for name, role, secret in [('control', 'bb_application', 'application'), ('catalog', 'bb_manager', 'manager')]:
            values += [('postgres.' + name, 'endpoint', '127.0.0.1:5432'),
                ('postgres.' + name, 'database', 'bb_export_' + name),
                ('postgres.' + name, 'user', role),
                ('postgres.' + name, 'password_ref', 'file:/lab/secrets/' + secret),
                ('postgres.' + name + '.tls', 'mode', 'local-plaintext'),
                ('postgres.' + name + '.tls', 'local_boundary', 'loopback')]
        for section, key, value in values:
            text = set_value(text, section, key, value)
        rule = dict(endpoint='ws://127.0.0.1:7447/installed-fixture', cidr='127.0.0.1/32', port=7447)
        text = set_value(text, 'network.private', 'rules', [rule])
        text = text.replace(json.dumps([rule]),
            '[{endpoint="ws://127.0.0.1:7447/installed-fixture",cidr="127.0.0.1/32",port=7447}]')
        recipe = self.out / 'recipe.toml'
        with recipe.open('x') as stream:
            stream.write(text)
        recipe.chmod(0o400)
        tracked = subprocess.check_output(['git', '-C', str(SOURCE), 'ls-files', '-z']).decode().split('\0')[:-1]
        manifest = dict(revision=self.state['source'], tree=self.state['tree'],
            files_sha256={name: sha(SOURCE / name) for name in tracked})
        write(self.out / 'source-manifest.json', manifest)
        keys = ['activate', 'owner', 'endpoint', 'enrollment', 'scope', 'acquisition',
                'accept-export', 'history.read', 'export', 'private-export']
        fixture = 'tests/fixtures/protocol/' + ('archive-event-response.json'
            if self.signed_input == 'archive-sample' else 'archive-event-' + self.signed_input + '.json')
        self.spec = dict(format='bigbrotr-export-fresh-setup/1', name='signed-memory',
            root='/lab/fresh-root', application_source=self.application_source,
            application=pin(self.build / 'bigbrotr', '/build/bigbrotr'),
            ingest_probe=pin(self.ingest_build / 'ingest_bootstrap_probe', self.ingest_visible),
            probe=pin(self.probe / 'export-execution-probe', '/probe/export-execution-probe'),
            psql=PSQL_PIN, config=pin(recipe, '/lab/recipe.toml'),
            recipe=pin(SOURCE / fixture, '/source/' + fixture),
            worker=dict(root='/build/archive-worker',
                bytes=(self.build / 'archive-worker/bigbrotr-archive-worker').stat().st_size,
                sha256=self.artifacts['archive-worker/bigbrotr-archive-worker']),
            source_manifest=pin(self.out / 'source-manifest.json', '/lab/source-manifest.json'),
            operator=dict(user='postgres', password_ref='file:/lab/secrets/bootstrap'),
            migrator=dict(user='bb_migrator', password_ref='file:/lab/secrets/migrator'),
            username='fixture-owner', password_ref='file:/lab/secrets/owner',
            keys={key: secrets.token_hex(16) for key in keys},
            limits=dict(memory_max_bytes=1536*MIB, baseline_bytes=64*MIB,
                data_bytes=768*MIB, native_bytes=192*MIB, control_bytes=32*MIB,
                engine_memory_bytes=64*MIB, native_reserve_bytes=96*MIB,
                row_bytes=MIB, scan_bytes=256*MIB, temp_bytes=64*MIB, export_bytes=MIB,
                disk_reserve_bytes=65536*MIB, query_ms=4000))
        if self.signed_input != 'archive-sample':
            self.spec['event_kind'] = 'frozen-signed-event'

    def publish(self):
        def inputs(start):
            inventory = dict(format='bigbrotr-export-environment/1', source=self.state['source'],
                tree=self.state['tree'], postgres=self.owned(self.pg), phase=start,
                application_source=self.application_source, build_root=str(self.build),
                artifacts=self.artifacts, probe_sha256=PROBE_SHA, tracer_sha256=TRACER_SHA,
                scope='Fresh isolated PostgreSQL and publisher container; no accepted export or measurement')
            if self.application_qualification is not None:
                inventory.update(application_build=self.application_qualification,
                    ingest_probe_source=APPLICATION_SOURCE,
                    ingest_probe=pin(self.ingest_build / 'ingest_bootstrap_probe', self.ingest_visible),
                    reference_build=str(self.reference_build))
            if self.export_memory_mib != 1024:
                inventory['export_memory_mib'] = self.export_memory_mib
            if self.manager_qualification is not None:
                inventory['manager_build'] = self.manager_qualification
            write(self.out / 'input-inventory.json', inventory)
            self.spec['inventory'] = pin(self.out / 'input-inventory.json', '/lab/input-inventory.json')
            write(self.out / 'setup-input.json', self.spec)
        return self.run('publish', ['python3', '-B', '/source/experiments/export-execution/memory_setup.py',
            'publish', '/lab/setup-input.json'], trace=True, before_release=inputs)

    def exclusion(self, name):
        """Fresh private credentials went only to this root's exact containers."""
        current = docker('ps', '-q').stdout.split()
        holders = []
        for ident in current:
            value = inspect(ident)
            for mount in value['Mounts']:
                source = Path(mount['Source'])
                if source == self.root or source in self.root.parents or self.root in source.parents:
                    holders.append(dict(id=value['Id'], mount=mount))
        require(holders and {row['id'] for row in holders} == {self.pg}, 'only owned PostgreSQL can still hold laboratory credentials')
        self.owned(self.pg)
        backends = json.loads(self.sql('postgres', "SELECT coalesce(json_agg(json_build_object("
            "'pid',pid,'start',backend_start,'database',datname,'user',usename)),'[]') "
            "FROM pg_stat_activity WHERE datname IN ('bb_export_control','bb_export_catalog');"))
        write(self.out / (name + '-backends.json'), backends)
        require(backends == [], 'actual Control/catalog backend absence')
        write(self.out / (name + '-holders.json'), dict(holders=holders,
            scope='Fresh private laboratory credentials supplied only to root executor and authenticated owned containers; not a claim about hostile host-root access.'))

    def custody(self, start, terminal, group):
        self.exclusion('publish')
        order = lambda mount: mount['Destination']
        require(sorted(terminal['Mounts'], key=order) == sorted(start['container']['Mounts'], key=order),
                'unchanged original publisher mounts')
        expected = {'/lab': (self.out, True), '/build': (self.build, False), '/probe': (self.probe, False),
                    '/source': (SOURCE, False), '/instrument': (self.instrument, False)}
        if self.reference_build is not None:
            expected['/reference-build'] = (self.reference_build, False)
        if self.manager_build is not None:
            expected['/manager-build'] = (self.manager_build, False)
        for target, (source, writable) in expected.items():
            mount, = [row for row in terminal['Mounts'] if row['Destination'] == target]
            require(Path(mount['Source']) == source and mount['RW'] == writable, 'exact original physical mount')
        fresh = self.out / 'fresh-root'
        checkpoint = json.loads((fresh / 'publication.json').read_text())
        require(checkpoint['status'] == 'PUBLISHED_AWAITING_ROOT_CUSTODY', 'known publication boundary')
        require({key: census(fresh / key) for key in ('store', 'data', 'state', 'destination')} == checkpoint['files'],
            'original preobserver roots unchanged after whole container termination')
        require(census(fresh / 'scratch') == json.loads((fresh / 'publisher-scratch.json').read_text()),
            'original publication scratch unchanged')
        write(self.out / 'publish-cgroup.json', group)
        write(self.out / 'publish-mounts.json', dict(mounts=terminal['Mounts'],
            physical_roots_unchanged=True, scratch_unchanged=True,
            holder_evidence=pin(self.out / 'publish-holders.json', '/lab/publish-holders.json')))
        evidence = dict(trace='publish.trace', trace_audit='publish-trace-audit.json',
            container='publish-terminal.json', cgroup='publish-cgroup.json',
            backends='publish-backends.json', mounts='publish-mounts.json')
        receipt = dict(format='bigbrotr-export-publication-custody/1', status='VERIFIED_FOR_NATIVE_REOPEN',
            root='/lab/fresh-root', publication=pin(fresh / 'publication.json', '/lab/fresh-root/publication.json'),
            source_manifest=self.spec['source_manifest'], inventory=self.spec['inventory'],
            container_id=terminal['Id'], cgroup_path=str(start['cgroup']),
            tracer=pin(self.instrument / 'strace-exit-fixed', '/instrument/strace-exit-fixed'),
            identity='trace-birth-exec-lineage',
            evidence={key: pin(self.out / value, '/lab/' + value) for key, value in evidence.items()},
            checks={key: True for key in ('natural_exit', 'empty_cgroup', 'backends_absent',
                'physical_roots_unchanged', 'complete_worker_lifetimes', 'no_other_credential_holders')})
        path = self.out / 'publication-custody.json'
        write(path, receipt)
        return sha(path)

    def acceptance(self, *, prepare_only=False):
        self.postgres()
        self.prepare()
        receipt = self.custody(*self.publish())
        self.run('continue', ['python3', '-B', '/source/experiments/export-execution/memory_setup.py',
            'continue', '/lab/fresh-root', '/lab/publication-custody.json', receipt],
            trace=True, minimum_workers=0)
        self.exclusion('continue')
        prepared = json.loads((self.out / 'fresh-root/result.json').read_text())
        require(prepared['status'] == 'PREPARED_FOR_ROOT_ACCEPTANCE', 'known real grant preparation')
        if prepare_only:
            self.state.update(outcome='PREPARED_FOR_ROOT_ACCEPTANCE',
                preparation_result=pin(self.out / 'fresh-root/result.json'), acceptance='NOT_RUN',
                measurement='NOT_RUN', qualification_256mib=False, five_exports_acceptance=False)
            self.save()
            return
        self.run('accept', ['python3', '-B', '/source/experiments/export-execution/memory_setup_accept.py',
            '/lab/fresh-root', '/sys/fs/cgroup'], trace=True, minimum_workers=0)
        self.exclusion('accept')
        result = json.loads((self.out / 'fresh-root/accept-run/result.json').read_text())
        require(result['status'] == 'ACCEPTED_UNRESERVED_INPUTS_PREPARED', 'known independently checked accepted job')
        self.state.update(outcome='ACCEPTED_INPUTS_PREPARED',
            acceptance_result=pin(self.out / 'fresh-root/accept-run/result.json'),
            measurement='NOT_RUN', qualification_256mib=False, five_exports_acceptance=False)
        self.save()

    def manager_measure(self):
        """Measure one accepted job; preserve the original accepted descriptor."""
        require(self.manager_qualification is not None and self.state['outcome'] == 'ACCEPTED_INPUTS_PREPARED',
                'fresh manager diagnostic after genuine single acceptance')
        fresh = self.out / 'fresh-root'
        accepted = json.loads((fresh / 'accept-run/result.json').read_text())
        original = pin(fresh / 'run.json')
        require(accepted['run_sha256'] == original['sha256'], 'same accepted original descriptor')
        descriptor = json.loads((fresh / 'run.json').read_text())
        require(descriptor['probe'] == '/probe/export-execution-probe' and descriptor['probe_sha256'] == PROBE_SHA
                and descriptor['cgroup'] == '/sys/fs/cgroup'
                and not (fresh / 'manager-measure-run').exists() and not (fresh / 'measure-ledger.jsonl').exists(),
                'first measurement and original qualified acceptance probe')
        descriptor.update(probe=self.manager_qualification['executable']['path'],
                          probe_sha256=self.manager_qualification['executable']['sha256'])
        write(fresh / 'manager-run.json', descriptor)
        self.state.update(manager_descriptor=pin(fresh / 'manager-run.json'), original_accepted_run=original)
        self.save()
        # A short file command keeps strace's exec arguments complete. SQL is
        # read-only through IO; the original captures and science remain intact.
        script = """import json,os,sys,tomllib
from pathlib import Path
sys.dont_write_bytecode=True
sys.path.insert(0,'/source/experiments/export-execution')
from memory_setup_io import IO,write,require,census
from memory_setup import immutable
from memory_oracle import verify
os.umask(0o077)
phase=sys.argv[1]
require(phase in ('before','after'),'explicit capture phase')
fresh=Path('/lab/fresh-root')
output=Path('/lab')/('manager-'+phase)
output.mkdir(mode=0o700)
spec=json.loads((fresh/'input.json').read_text())
config=tomllib.loads((fresh/'destination.toml').read_text())
io=IO(output,spec,config)
try:
 io.backends('before',absent=True)
 state=io.capture('state')
 files=immutable(fresh)
 write(output/'files.json',files)
 write(output/'scratch.json',census(fresh/'scratch'))
 if phase=='before':
  require(state==json.loads((fresh/'accept-run/after.json').read_text()),'same entire accepted SQL state')
  require(files==json.loads((fresh/'prepared.json').read_text())['files'],'same original physical roots')
 else:
  before=json.loads(Path('/lab/manager-before/state.json').read_text())
  require(state['catalog']==before['catalog'],'complete catalog unchanged')
  original=json.loads(Path('/lab/manager-before/files.json').read_text())
  require(all(files[name]==original[name] for name in ('store','data','state')),'scientific roots unchanged')
  write(output/'science.json',verify(fresh/'manager-measure-run'))
 io.backends('after',absent=True)
except BaseException:
 io.failure_custody()
 raise
"""
        path = self.out / 'manager-capture.py'
        with path.open('x') as stream:
            stream.write(script)
        path.chmod(0o400)
        def freshness(start):
            group = Path(start['cgroup'])
            limits = json.loads((fresh / 'case.json').read_text())['limits']
            maximum = int((group / 'memory.max').read_text())
            swap = (group / 'memory.swap.max').read_text().strip()
            require(maximum == limits['memory_max_bytes'] == 1536*MIB and swap == '0',
                    'fresh manager observer/probe/worker cgroup with no swap')
            write(self.out / 'manager-measure-freshness.json', dict(start=start, memory_max=maximum,
                memory_swap_max=swap, case=pin(fresh / 'case.json'), run=pin(fresh / 'manager-run.json'),
                original_accepted_run=original, capture_script=pin(path),
                observer_source=pin(SOURCE / 'experiments/export-execution/memory_run.py'),
                science_source=pin(SOURCE / 'experiments/export-execution/memory_oracle.py'),
                manager=self.manager_qualification,
                scope='Fresh sole gated observer; observer+diagnostic+worker, no tracer or peak reset/subtraction'))
        self.run('manager-before', ['python3', '-B', '/lab/manager-capture.py', 'before'], trace=True, minimum_workers=0)
        self.exclusion('manager-before')
        self.state.update(state='manager-measuring', outcome='MANAGER_MEASUREMENT_RUNNING')
        self.save()
        self.run('manager-measure', ['python3', '-B', '/source/experiments/export-execution/memory_run.py',
            '/lab/fresh-root/manager-run.json', '/lab/fresh-root/manager-measure-run'],
            memory_mib=1536, before_release=freshness)
        self.exclusion('manager-measure')
        require(pin(fresh / 'run.json') == original, 'original accepted run descriptor preserved')
        self.run('manager-after', ['python3', '-B', '/lab/manager-capture.py', 'after'], trace=True, minimum_workers=0)
        self.exclusion('manager-after')
        self.state.update(outcome='CAPTURED_MANAGER_SCIENCE_SQL_AUDIT_PENDING',
            manager_capture=pin(fresh / 'manager-measure-run/result.json'),
            manager_science=pin(self.out / 'manager-after/science.json'),
            manager_before=pin(self.out / 'manager-before/state.json'),
            manager_after=pin(self.out / 'manager-after/state.json'),
            qualification_256mib=False, five_exports_acceptance=False,
            review_required='Root and critical review must audit nine-table delta, physical output and outcome; '
                            'this1536MiB diagnostic is separate from installed256MiB qualification.')
        self.save()

    def installed_five(self):
        """Use the prepared genuine256MiB policy; never accept a setup-only job."""
        require(self.export_memory_mib == 256 and self.application_qualification is not None
                and self.state['outcome'] == 'PREPARED_FOR_ROOT_ACCEPTANCE', 'prepared installed profile')
        descriptor = dict(format='bigbrotr-installed-export-memory-run/1',
            prepared_root='/lab/fresh-root', cgroup='/sys/fs/cgroup',
            keys=[secrets.token_hex(16) for _ in range(5)])
        write(self.out / 'installed-run.json', descriptor)
        self.state['installed_descriptor'] = pin(self.out / 'installed-run.json')
        self.save()
        # No strace process inside this empirical memory boundary. The existing
        # observer samples real processes; positive process/group joins, actual
        # container termination and backend exclusion establish outer custody.
        self.run('installed-five', ['python3', '-B', '/source/experiments/export-execution/memory_run.py',
            '/lab/installed-run.json', '/lab/installed-five', '--installed'], memory_mib=256)
        self.exclusion('installed-five')
        capture_root = self.out / 'installed-five'
        initial = json.loads((capture_root / 'input.json').read_text())
        captured = json.loads((capture_root / 'result.json').read_text())
        require(captured['status'] == 'CAPTURED_INSTALLED_FIVE' and captured['error'] is None
                and captured['sampler_joined'] and not captured['sampler_errors']
                and len(captured['commands']) == len(captured['statuses']) == 5
                and captured['worker_sha256_after'] == initial['source']['worker']['sha256']
                and captured['descriptor_sha256_after'] == initial['descriptor_sha256']
                    == sha(self.out / 'installed-run.json'), 'complete installed capture with unchanged inputs')
        before, after = initial['cgroup_before'], captured['cgroup_after']
        require(before['limit'] == after['limit'] == '268435456'
                and before['peak'] <= after['peak'] <= 268435456
                and after['pids'] == [initial['observer']['pid']]
                and all(after['events'].get(key, 0) == before['events'].get(key, 0)
                        for key in ('oom', 'oom_kill', 'oom_group_kill')), 'actual cgroup limit/terminal outcome')
        samples = []
        with (capture_root / 'samples.jsonl').open() as stream:
            for line in stream:
                require(len(line) <= 65536 and len(samples) < 14000, 'bounded raw installed samples')
                samples.append(json.loads(line))
        require(samples and all(row['cgroup']['limit'] == '268435456' for row in samples)
                and [row['elapsed_ns'] for row in samples] == sorted(row['elapsed_ns'] for row in samples),
                'observed constant physical cap and sample chronology')
        workers = [p for row in samples for p in row['processes']
                   if p['executable'] == '/build/archive-worker/bigbrotr-archive-worker']
        parents = [p for row in samples for p in row['processes'] if p['pid'] == captured['server_pid']]
        require(workers and parents and all(p['ppid'] == captured['server_pid'] for p in workers)
                and len({(p['pid'], p['start_ticks']) for p in workers}) >= 5, 'actual repeated native workers')
        memory = dict(cgroup_lifetime_peak=after['peak'], cgroup_before_peak=before['peak'],
            cgroup_event_changes={key: value-before['events'].get(key, 0) for key, value in after['events'].items()},
            parent_rss_sample_max=max(p['memory'].get('VmRSS', 0) for p in parents),
            worker_rss_sample_max=max(p['memory'].get('VmRSS', 0) for p in workers),
            observed_worker_identities=len({(p['pid'], p['start_ticks']) for p in workers}),
            samples=len(samples), operation_cap_bytes=268435456,
            configured_application_envelope=initial['configured_application_envelope'],
            configured_native_envelope=initial['configured_native_envelope'],
            http_acceptance_native_reservation_bytes=initial['http_acceptance_native_reservation_bytes'],
            http_acceptance_reservation_bytes=initial['http_acceptance_reservation_bytes'],
            shared_control_reservation_bytes=initial['shared_control_reservation_bytes'],
            export_reservation_bytes=initial['plaintext_export_reservation_bytes'],
            limits='Warm-cache empirical observer+Serve+CLI+native cgroup, PostgreSQL outside. '
                   'Whole-service configured grants exceed256MiB. No baseline subtraction, live ledger, '
                   'complete worker trace, internal-phase or maximum-event claim.')
        write(capture_root / 'memory-summary.json', memory)
        # Offline oracle runs only after the measured container has naturally
        # terminated and this root has authenticated backend/credential exclusion.
        script = """import json,sys
from pathlib import Path
sys.path.insert(0,'/source/experiments/export-execution')
import memory_setup
from test_memory_oracle import installed
p=Path('/lab/installed-five')
inputs=json.loads((p/'audit-input.json').read_text())
payload=dict(before_capture=json.loads((p/'before.json').read_text()),
 after_capture=json.loads((p/'after.json').read_text()),**inputs)
with (p/'oracle-input.json').open('x') as stream:
 json.dump(payload,stream,indent=2);stream.write('\\n')
result=installed(payload)
with (p/'science-result.json').open('x') as stream:
 json.dump(result['baseline'],stream,indent=2);stream.write('\\n')
with (p/'oracle-regression.json').open('x') as stream:
 json.dump(result,stream,indent=2);stream.write('\\n')
"""
        self.run('installed-oracle', ['python3', '-B', '-c', script])
        science = json.loads((capture_root / 'science-result.json').read_text())
        regression = json.loads((capture_root / 'oracle-regression.json').read_text())
        require(science['status'] == 'PASS_INSTALLED_FIVE_SQL_AND_SIGNED_FILES'
                and regression['status'] == 'PASS_OFFLINE_INSTALLED_ORACLE_REGRESSION'
                and regression['baseline'] == science
                and all(row['status'] == 'REJECTED' for row in regression['controls']),
                'positive independent science and rejected negative controls')
        self.state.update(outcome='INSTALLED_FIVE_CAPTURE_AND_OFFLINE_SCIENCE_COMPLETE',
            installed_capture=pin(capture_root / 'result.json'),
            memory_observation=pin(capture_root / 'memory-summary.json'),
            science_result=pin(capture_root / 'science-result.json'),
            oracle_regression=pin(capture_root / 'oracle-regression.json'),
            qualification_256mib=False, five_exports_acceptance=False,
            review_required='Capture and independent offline result retained; root must review scope '
                            'before qualification. No whole-service256MiB deployment sizing claim.')
        self.save()

    def close(self):
        """Stop only this invocation's authenticated containers; retain all data."""
        errors = []
        for entry in reversed(self.state['containers']):
            try:
                value = self.owned(entry['id'])
                if value['State']['Running']:
                    docker('stop', '--time', '10', entry['id'])
                    value = self.owned(entry['id'])
                require(not value['State']['Running'] and value['State']['Pid'] == 0, 'terminal owned container')
                entry['terminal'] = value['State']
            except BaseException as error:
                errors.append(dict(id=entry['id'], error=type(error).__name__))
        self.state.update(state='stopped-retained' if not errors else 'cleanup-required',
            cleanup_errors=errors, finished_at=datetime.datetime.now(datetime.timezone.utc).isoformat())
        self.save()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('root', 'build', 'probe', 'instrument'):
        parser.add_argument('--' + name, required=True, type=Path)
    parser.add_argument('--application-build', type=Path,
        help='qualified export-admission build root; --build retains the unchanged ingest dump probe')
    parser.add_argument('--export-memory-mib', type=int, choices=(256, 1024), default=1024)
    parser.add_argument('--signed-input', choices=('archive-sample', 'content-heavy', 'tag-heavy'),
        default='archive-sample', help='frozen supported Sync boundary; export row profile remains1MiB')
    modes = parser.add_mutually_exclusive_group()
    modes.add_argument('--prepare-only', action='store_true', help='stop before root Export acceptance')
    modes.add_argument('--installed-five', action='store_true',
        help='prepare genuine256MiB policy and observe five ordinary Serve exports')
    modes.add_argument('--manager-build', type=Path,
        help='qualified manager diagnostic build root; accept and measure one fresh job')
    args = parser.parse_args()
    require(all(p.is_absolute() and p.resolve() == p for p in
                (args.root, args.build, args.probe, args.instrument,
                 *([args.application_build] if args.application_build is not None else []))),
            'canonical absolute laboratory inputs')
    require(args.manager_build is None or (args.manager_build.is_absolute()
            and args.manager_build.resolve() == args.manager_build and args.application_build is not None),
            'canonical manager build with qualified installed application')
    os.umask(0o077)
    require(not args.installed_five or (args.application_build is not None
            and args.export_memory_mib == 256 and not args.prepare_only), 'explicit installed five profile')
    require(args.prepare_only or args.installed_five or args.signed_input == 'archive-sample',
            'pinned single-job acceptance probe supports only archive-sample')
    lab = Laboratory(args.root, args.build, args.probe, args.instrument,
        application_build=args.application_build, export_memory_mib=args.export_memory_mib,
        signed_input=args.signed_input, manager_build=args.manager_build)
    try:
        lab.acceptance(prepare_only=args.prepare_only or args.installed_five)
        if args.installed_five:
            lab.installed_five()
        if args.manager_build is not None:
            lab.manager_measure()
    except BaseException as error:
        lab.state.update(outcome='UNKNOWN_PRESERVED', error=type(error).__name__,
            next='Authenticate retained outcome; never replay this root or repair authority automatically')
        raise
    finally:
        lab.close()
        if lab.state['cleanup_errors'] and sys.exc_info()[0] is None:
            raise RuntimeError('owned cleanup incomplete; preserve accepted outcome and inspect resources.json')


if __name__ == '__main__':
    main()
