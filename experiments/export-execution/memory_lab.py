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
    def __init__(self, root, build, probe, instrument):
        require(root.is_absolute() and root.resolve() == root and not root.exists(), 'fresh canonical root')
        require(not git('status', '--porcelain', '--untracked-files=all'), 'clean committed source')
        require(shutil.disk_usage(root.parent).free >= 69324505088, 'completion reserve plus headroom')
        require(not git('diff', '--name-only', APPLICATION_SOURCE, 'HEAD', '--',
            'apps', 'crates', 'Cargo.toml', 'Cargo.lock', 'rust-toolchain.toml',
            'tools/build.py', 'tools/archive_bundle.py', 'contracts', 'migrations'), 'unchanged qualified product inputs')
        for path, expected in ARTIFACTS.items():
            require((build / path).is_file() and not (build / path).is_symlink()
                    and sha(build / path) == expected, 'qualified artifact: ' + path)
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
        self.save()
        self.pg = None

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
subprocess.run(['/build/bigbrotr','config','init','--recipe','archive-only','--output','/lab/template.toml'],check=True,env={})
"""
        self.run('configure', ['python3', '-c', script])
        require(json.loads((self.out / 'psql.json').read_text()) == PSQL_PIN, 'actual canonical client executable')
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
            ('export.memory', 'max_bytes', '1GiB'),
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
        self.spec = dict(format='bigbrotr-export-fresh-setup/1', name='signed-memory',
            root='/lab/fresh-root', application_source=APPLICATION_SOURCE,
            application=pin(self.build / 'bigbrotr', '/build/bigbrotr'),
            ingest_probe=pin(self.build / 'ingest_bootstrap_probe', '/build/ingest_bootstrap_probe'),
            probe=pin(self.probe / 'export-execution-probe', '/probe/export-execution-probe'),
            psql=PSQL_PIN, config=pin(recipe, '/lab/recipe.toml'),
            recipe=pin(SOURCE / 'tests/fixtures/protocol/archive-event-response.json',
                '/source/tests/fixtures/protocol/archive-event-response.json'),
            worker=dict(root='/build/archive-worker',
                bytes=(self.build / 'archive-worker/bigbrotr-archive-worker').stat().st_size,
                sha256=ARTIFACTS['archive-worker/bigbrotr-archive-worker']),
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

    def publish(self):
        def inputs(start):
            inventory = dict(format='bigbrotr-export-environment/1', source=self.state['source'],
                tree=self.state['tree'], postgres=self.owned(self.pg), phase=start,
                application_source=APPLICATION_SOURCE, build_root=str(self.build),
                artifacts=ARTIFACTS, probe_sha256=PROBE_SHA, tracer_sha256=TRACER_SHA,
                scope='Fresh isolated PostgreSQL and publisher container; no accepted export or measurement')
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

    def acceptance(self):
        self.postgres()
        self.prepare()
        receipt = self.custody(*self.publish())
        self.run('continue', ['python3', '-B', '/source/experiments/export-execution/memory_setup.py',
            'continue', '/lab/fresh-root', '/lab/publication-custody.json', receipt],
            trace=True, minimum_workers=0)
        self.exclusion('continue')
        prepared = json.loads((self.out / 'fresh-root/result.json').read_text())
        require(prepared['status'] == 'PREPARED_FOR_ROOT_ACCEPTANCE', 'known real grant preparation')
        self.run('accept', ['python3', '-B', '/source/experiments/export-execution/memory_setup_accept.py',
            '/lab/fresh-root', '/sys/fs/cgroup'], trace=True, minimum_workers=0)
        self.exclusion('accept')
        result = json.loads((self.out / 'fresh-root/accept-run/result.json').read_text())
        require(result['status'] == 'ACCEPTED_UNRESERVED_INPUTS_PREPARED', 'known independently checked accepted job')
        self.state.update(outcome='ACCEPTED_INPUTS_PREPARED',
            acceptance_result=pin(self.out / 'fresh-root/accept-run/result.json'),
            measurement='NOT_RUN', qualification_256mib=False, five_exports_acceptance=False)
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
    args = parser.parse_args()
    require(all(p.is_absolute() and p.resolve() == p for p in
                (args.root, args.build, args.probe, args.instrument)), 'canonical absolute laboratory inputs')
    os.umask(0o077)
    lab = Laboratory(args.root, args.build, args.probe, args.instrument)
    try:
        lab.acceptance()
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
