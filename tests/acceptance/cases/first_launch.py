"""Fresh installed signed acquisition, ordinary history and exact JSONL export.

This case reuses the independent clients and export oracle. It does not qualify
the native RocksDB census, typed producer progress or independent C API census.
All raw evidence is protected: it includes generated credentials and sessions.
"""
from contextlib import contextmanager
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import platform
import re
import secrets
import shutil
import signal
import socket
import subprocess
import sys
import time


LABEL = 'org.bigbrotr.m1-first-launch'
IMAGE_LABEL = 'org.bigbrotr.acceptance.bundle-sha256'
PORTS = (15432, 18081, 7447)
STAGES = ('prerequisites', 'candidate', 'runtime_image', 'fresh_bootstrap',
          'signed_acquisition', 'ordinary_history_export', 'cleanup')
LIMITS = [
    'Fresh signed acquisition, Archive frontier and ordinary Control history/export only.',
    'NOT_RUN: independent native/C API census, typed progress qualification, Ingest successor/resend, Index readiness and complete M1.',
    'Configured container limits are not empirical capacity qualification; local candidate identity is not release authentication.',
]
RESTART_STAGES = STAGES[:-1] + ('restart_baseline', 'ingest_restart_resend',
    'archive_catch_up', 'catch_up_exact_retry', 'successor_history', 'cleanup')
RESTART_LIMITS = [
    'Fresh base export, same-store epoch1→2 Ingest recovery/resend, independent scientific C API census, Archive catch-up/retry and ordinary history of both events.',
    'NOT_RUN: native RocksDB census, provenance-off repeat, second-event export, Index readiness, dependency-outage qualification and complete M1.',
    LIMITS[2],
]
OFF_CASE = 'p01-first-launch-restart-provenance-off'
RESTART_CASES = ('p01-first-launch-restart', OFF_CASE, 'p01-first-launch-index-topology',
                 'p01-first-launch-index-live-topology', 'p01-first-launch-closed-reclamation')
OFF_LIMITS = [
    RESTART_LIMITS[0] + ' Fresh accepted sync.provenance=off; no scientific witness rows.',
    'NOT_RUN: native RocksDB census, second-event export, Index readiness, dependency-outage qualification and complete M1.',
    LIMITS[2],
]


class MissingPrerequisite(Exception):
    pass


def require(condition, reason):
    if not condition:
        raise ValueError(reason)


def digest(path):
    with Path(path).open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def save(path, value):
    path.write_text(json.dumps(value, indent=2) + '\n')


def load(path):
    return json.loads(path.read_text())


def failure_trace(error, source):
    """Record source locations only; omit frames outside the selected tree."""
    source = source.resolve()
    frames = []
    frame = error.__traceback__
    while frame is not None:
        try:
            relative = Path(frame.tb_frame.f_code.co_filename).resolve().relative_to(source)
        except ValueError:
            pass
        else:
            frames.append(dict(file=relative.as_posix(), line=frame.tb_lineno,
                               function=frame.tb_frame.f_code.co_name))
        frame = frame.tb_next
    return frames


def check_container(info, entry, owner, project, state):
    """A name is only a creation intent; labels, ID and mounts establish custody."""
    require(info['Name'] == '/' + entry['name'], 'container name changed')
    require(re.fullmatch('[0-9a-f]{64}', info['Id']), 'invalid container ID')
    require(entry.get('id') in (None, info['Id']), 'container identity changed')
    labels = info['Config']['Labels'] or {}
    require(labels.get(LABEL) == owner and labels.get('com.docker.compose.project') == project
            and labels.get('com.docker.compose.service') == entry['service'], 'container ownership mismatch')
    expected = {'/var/lib/postgresql/data': state / 'postgres', '/run/operator': state / 'secrets/operator',
                '/run/secrets': state / 'secrets/runtime',
                '/opt/bigbrotr/provision.sql': state.parent / 'bundle/packaging/compose/provision.sql'} if entry['service'] == 'database' else {
                    '/var/lib/bigbrotr': state / 'data', '/etc/bigbrotr': state / 'config',
                    '/run/secrets': state / 'secrets/runtime'}
    if entry['service'] == 'initialize':
        expected['/run/operator'] = state / 'secrets/operator'
    if entry['service'] == 'index-initialize':
        expected['/run/index-initialize'] = state / 'secrets/index-initialize'
    mounts = {item['Destination']: item for item in info['Mounts']}
    require({item['Destination'] for item in info['Mounts'] if item['Type'] == 'bind'} == set(expected),
            'unexpected container bind mount')
    require(all(mounts.get(target, {}).get('Source') == str(source)
                for target, source in expected.items()), 'container mount mismatch')
    if entry['service'] == 'index-initialize':
        require(not mounts['/run/index-initialize']['RW'], 'initializer secret mount must be read-only')
    if entry['service'] == 'app':
        require('/run/operator' not in mounts, 'ordinary app received operator credentials')
    return info


def check_network(info, name, *, owner=None):
    require(info['Name'] == name and info['Driver'] == 'bridge'
            and info['Scope'] == 'local', 'expected local bridge network')
    require(re.fullmatch('[0-9a-f]{64}', info['Id']), 'invalid network ID')
    if owner is not None:
        require((info.get('Labels') or {}).get(LABEL) == owner, 'network ownership mismatch')
    return info


def local_docker_endpoint(environment, context):
    # Docker's explicit context takes precedence over DOCKER_HOST. Checking the
    # latter first could authorize a remote context through an unused unix URL.
    if environment.get('DOCKER_CONTEXT'):
        require(context['Name'] == environment['DOCKER_CONTEXT'], 'selected Docker context differs')
        endpoint = context['Endpoints']['docker']['Host']
    else:
        endpoint = environment.get('DOCKER_HOST') or context['Endpoints']['docker']['Host']
    require(endpoint.startswith('unix:///'), 'first-launch requires a local Docker socket')
    return endpoint


def expected_tables(source, inputs=None):
    manifest = load(source / 'migrations/manifest.json')
    require(manifest['application_order'][-1]['version'] == 41, 'first-launch schema profile changed')
    tables = {'schema_versions'}
    for entry in manifest['application_order']:
        tables.update(re.findall(r'CREATE TABLE control\.(\w+)',
                                 (source / 'migrations/postgres' / entry['name']).read_text()))
    require(len(tables) == 82, 'first-launch Control table profile changed')
    if inputs is not None:
        names = ['migrations/manifest.json'] + ['migrations/postgres/' + entry['name']
                                                for entry in manifest['application_order']]
        inputs['migration_inputs_sha256'] = {name: digest(source / name) for name in names}
    return tables


def stop_process(process):
    # The process group remains owned even if its leader already exited.
    for sig, timeout in ((signal.SIGTERM, 1), (signal.SIGKILL, 2)):
        try:
            os.killpg(process.pid, sig)
        except ProcessLookupError:
            pass
        try:
            process.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            continue
    require(process.poll() is not None, 'owned process did not join')


@contextmanager
def interrupted():
    def stop(signum, frame):
        raise InterruptedError('first-launch interrupted')
    previous = {sig: signal.signal(sig, stop) for sig in (signal.SIGINT, signal.SIGTERM)}
    try:
        yield
    finally:
        for sig, handler in previous.items():
            signal.signal(sig, handler)


class Runtime:
    def __init__(self, args, inputs):
        self.args, self.inputs = args, inputs
        self.restart_case = getattr(args, 'case', None) in RESTART_CASES
        self.provenance = getattr(args, 'case', None) != OFF_CASE
        self.initial_cut = 12 if self.provenance else 8
        self.root, self.source = args.evidence, args.source_root.resolve()
        self.owner = secrets.token_hex(16)
        self.project = 'bigbrotr-acceptance-' + self.owner
        self.state = self.root / 'instance'
        self.commands = self.root / 'commands'
        self.commands.mkdir(mode=0o700)
        self.environment = {key: os.environ[key] for key in
            ('PATH', 'HOME', 'DOCKER_HOST', 'DOCKER_CONTEXT', 'DOCKER_CONFIG', 'XDG_RUNTIME_DIR')
            if key in os.environ}
        self.environment.update(LANG='C', LC_ALL='C')
        self.resources = dict(format='bigbrotr-first-launch-resources/1', owner=self.owner,
                              project=self.project, state=str(self.state), containers=[],
                              image=None, network=None, cleanup_errors=[])
        self.processes = []
        self.number = 0
        self.pg = None
        self.client = None
        self.original_lab_command = None
        self.record_resources()

    def record_resources(self):
        save(self.root / 'resources.json', self.resources)

    def command(self, command, *, data=None, timeout=30, check=True, label=None):
        self.number += 1
        prefix = self.commands / (f'{self.number:04}-' + (label or Path(command[0]).name))
        receipt = dict(argv=[str(arg) for arg in command], timeout_seconds=timeout,
                       started_at_us=time.time_ns() // 1000, status='starting')
        if data is not None:
            receipt['stdin_sha256'] = hashlib.sha256(data.encode()).hexdigest()
        save(prefix.with_suffix('.json'), receipt)
        process = subprocess.Popen(command, env=self.environment, cwd=self.root,
            stdin=subprocess.PIPE if data is not None else subprocess.DEVNULL,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, start_new_session=True)
        self.processes.append(process)
        receipt.update(pid=process.pid, status='running')
        save(prefix.with_suffix('.json'), receipt)
        try:
            stdout, stderr = process.communicate(input=data, timeout=timeout)
        finally:
            stop_process(process)
            try:
                stdout, stderr = process.communicate(timeout=2)
            except subprocess.TimeoutExpired:
                self.resources['cleanup_errors'].append(dict(resource='process-pipes', id=process.pid,
                    error='pipe remained open after owned group termination'))
                for stream in (process.stdin, process.stdout, process.stderr):
                    if stream is not None:
                        stream.close()
                raise
            prefix.with_suffix('.stdout').write_text(stdout)
            prefix.with_suffix('.stderr').write_text(stderr)
            receipt.update(exit_code=process.returncode, finished_at_us=time.time_ns() // 1000,
                           status='joined')
            save(prefix.with_suffix('.json'), receipt)
            self.processes.remove(process)
        result = subprocess.CompletedProcess(command, process.returncode, stdout, stderr)
        require(not check or result.returncode == 0, 'command failed: ' + prefix.name)
        return result

    def docker(self, *command, **options):
        return self.command(['docker', *command], **options)

    def preflight(self):
        if platform.system() != 'Linux' or platform.machine() != 'x86_64' or sys.version_info < (3, 11):
            raise MissingPrerequisite('requires native Linux amd64 and Python 3.11 or newer')
        if os.geteuid() == 0 or not shutil.which('docker'):
            raise MissingPrerequisite('requires a non-root user with Docker access')
        if self.args.candidate_manifest is None or not self.args.candidate_manifest.is_file():
            raise MissingPrerequisite('missing candidate manifest')
        if self.restart_case and (not re.fullmatch('/[a-zA-Z0-9/_.-]+', str(self.root))
                or any(part in ('.', '..') for part in str(self.root).split('/'))):
            raise MissingPrerequisite('C API census requires an absolute SQL-safe evidence path (letters, digits, slash, dot, underscore, hyphen; no traversal)')
        if shutil.disk_usage(self.root).free <= 65 * 1024**3:
            raise MissingPrerequisite('requires more than 65 GiB free completion space')
        for port in PORTS:
            try:
                with socket.socket() as listener:
                    listener.bind(('127.0.0.1', port))
            except OSError as error:
                raise MissingPrerequisite(f'loopback port {port} unavailable') from error
        for command in (('info', '--format', '{{json .ServerVersion}}'), ('compose', 'version', '--short')):
            if self.docker(*command, check=False).returncode:
                raise MissingPrerequisite('Docker daemon and Compose v2 are required')
        selected_context = [self.environment['DOCKER_CONTEXT']] if self.environment.get('DOCKER_CONTEXT') else []
        context, = json.loads(self.docker('context', 'inspect', *selected_context).stdout)
        self.inputs['docker_endpoint'] = local_docker_endpoint(self.environment, context)
        require(self.args.compose_network is None or re.fullmatch('[a-zA-Z0-9][a-zA-Z0-9_.-]{0,127}',
                self.args.compose_network), 'invalid borrowed network name')
        if self.args.compose_network:
            result = self.docker('network', 'inspect', self.args.compose_network, check=False)
            if result.returncode:
                raise MissingPrerequisite('borrowed network is not available')
            self.borrowed_network, = json.loads(result.stdout)
            check_network(self.borrowed_network, self.args.compose_network)
        self.inputs['available_disk_bytes'] = shutil.disk_usage(self.root).free

    def candidate(self):
        from candidate import extract_candidate
        for relative, actual in (('tests/acceptance/run.py', Path(__file__).parents[1] / 'run.py'),
                                 ('tests/acceptance/cases/first_launch.py', Path(__file__))):
            require(digest(actual) == digest(self.source / relative), 'acceptance source mismatch')
        selected = extract_candidate(self.args.candidate_manifest, self.root / 'bundle', source_root=self.source)
        self.bundle = Path(selected['bundle_root'])
        self.manifest = selected['manifest']
        self.inputs.update(candidate_manifest_sha256=selected['manifest_sha256'],
            candidate_bundle_sha256=selected['bundle_sha256'], product_source=self.manifest['artifact']['source'],
            assembly_source={key: self.manifest['assembly'][key] for key in ('revision', 'tree')})
        self.tables = expected_tables(self.source, self.inputs)
        self.inputs['expected_control_tables'] = sorted(self.tables)
        self.load_helpers()
        self.inputs['acceptance_files_sha256'] = {str(path.relative_to(self.source)): digest(path)
            for path in sorted({Path(module.__file__).resolve() for module in list(sys.modules.values())
                if getattr(module, '__file__', None) and Path(module.__file__).resolve().is_relative_to(self.source)
                and Path(module.__file__).suffix == '.py'})}
        for name in ('installed_archive_client.py', 'installed_archive_export_client.py', 'installed_archive_profile.py'):
            relative = 'experiments/ingest-bootstrap/' + name
            self.inputs['acceptance_files_sha256'][relative] = digest(self.source / relative)
        if self.restart_case:
            for name in ('installed_live_client.py', 'installed_archive_catch_up_client.py', 'installed_archive_read.py'):
                relative = 'experiments/ingest-bootstrap/' + name
                self.inputs['acceptance_files_sha256'][relative] = digest(self.source / relative)
            selector = self.source / 'tests/fixtures/acquisition/selector-events.json'
            require(digest(selector) == self.restart_checks.SELECTOR_SHA256, 'successor signed fixture changed')
            self.inputs['selector_fixture_sha256'] = digest(selector)
        event = self.source / 'tests/fixtures/ingest/import-event.jsonl'
        require(digest(event) == self.signed_sha256, 'signed fixture changed')
        self.inputs['signed_fixture_sha256'] = digest(event)

    def source_unchanged(self):
        for name, expected in self.inputs['acceptance_files_sha256'].items():
            require(digest(self.source / name) == expected, 'acceptance source changed during execution')
        require(digest(self.home / 'event.jsonl') == self.inputs['signed_fixture_sha256'], 'signed fixture changed during execution')
        if self.restart_case:
            require(digest(self.source / 'tests/fixtures/acquisition/selector-events.json') == self.inputs['selector_fixture_sha256'],
                'successor fixture changed during execution')
            if hasattr(self, 'successor_home'):
                require(digest(self.successor_home / 'selector-events.json') == self.inputs['selector_fixture_sha256']
                    and digest(self.successor_home / 'event.jsonl') == self.inputs['signed_fixture_sha256'], 'successor copied fixture changed')

    def load_helpers(self):
        # Existing independent owners, including their legacy import alias.
        sys.dont_write_bytecode = True
        for folder in ('experiments/control-migrations', 'experiments/ingest-bootstrap',
                       'experiments/ingest-succession'):
            sys.path.insert(0, str(self.source / folder))
        spec = importlib.util.spec_from_file_location('bootstrap_oracle', self.source / 'experiments/ingest-bootstrap/oracle.py')
        module = importlib.util.module_from_spec(spec)
        sys.modules['bootstrap_oracle'] = module
        spec.loader.exec_module(module)
        import lab
        from schema import rows
        from installed import set_value
        from installed_archive import catalog_rows, files
        from installed_archive_frontier import current_owner_view
        from installed_archive_export_oracle import validate
        from installed_sync import validate_source
        from installed_sync_oracle import SIGNED_SHA256
        self.rows, self.catalog_rows, self.files = rows, catalog_rows, files
        self.set_value, self.current_owner_view = set_value, current_owner_view
        self.export_validate, self.validate_source = validate, validate_source
        self.signed_sha256 = SIGNED_SHA256
        if self.restart_case:
            from cases import first_launch_restart
            from cases import first_launch_index_baseline
            # Load every independent owner before recording source identity.
            import installed_archive_read
            import installed_archive_catch_up_oracle
            import installed_progress_context_oracle
            self.restart_checks = first_launch_restart
            self.history_baseline = first_launch_index_baseline
            self.restart_profile = first_launch_restart.profile(self.provenance)
        # Route the reused SQL observers through the same bounded process/log
        # owner. Their SQL and independent row/oracle semantics are unchanged.
        self.lab, self.original_lab_command = lab, lab.command
        lab.command = self.command

    def image_and_state(self):
        dockerfile = (self.bundle / 'packaging/image/Dockerfile').read_text()
        base, = re.findall(r'^FROM (\S+@sha256:[0-9a-f]{64})$', dockerfile, re.M)
        compose_text = (self.bundle / 'packaging/compose/compose.yaml').read_text()
        postgres, = re.findall(r'^    image: (postgres@sha256:[0-9a-f]{64})$', compose_text, re.M)
        self.inputs['runtime_images'] = {}
        for name, reference in (('base', base), ('postgres', postgres)):
            result = self.docker('image', 'inspect', reference, check=False)
            if result.returncode:
                raise MissingPrerequisite('required pinned ' + name + ' image is not present locally')
            info, = json.loads(result.stdout)
            require(info['Architecture'] == 'amd64' and info['Os'] == 'linux', 'wrong runtime image platform')
            self.inputs['runtime_images'][name] = dict(reference=reference, id=info['Id'])
        tag = self.project + ':candidate'
        require(self.docker('image', 'inspect', tag, check=False).returncode != 0, 'image tag already exists')
        self.resources['image'] = dict(tag=tag, id=None, status='creating')
        self.record_resources()
        # A retained BuildKit COPY layer kept old directory modes after a source
        # mode correction. Recreate only this small wrapper; product binaries and
        # pinned base layers are reused, and verify-worker checks the installation.
        self.docker('build', '--no-cache', '--network=none', '--pull=false', '--label', LABEL + '=' + self.owner,
            '--label', IMAGE_LABEL + '=' + self.inputs['candidate_bundle_sha256'], '--iidfile', str(self.root / 'image.id'),
            '--tag', tag, '--file', str(self.bundle / 'packaging/image/Dockerfile'), str(self.bundle),
            timeout=180, label='runtime-image')
        image, = json.loads(self.docker('image', 'inspect', tag).stdout)
        require(image['Id'] == (self.root / 'image.id').read_text().strip(), 'runtime image ID mismatch')
        require(image['Config']['Labels'][LABEL] == self.owner
                and image['Config']['Labels'][IMAGE_LABEL] == self.inputs['candidate_bundle_sha256'], 'runtime image label mismatch')
        self.resources['image'].update(id=image['Id'], status='retained')
        self.prepare_state()
        borrowed = self.args.compose_network
        name = borrowed or self.project
        self.resources['network'] = dict(name=name, id=None, borrowed=bool(borrowed), status='inspecting' if borrowed else 'creating')
        self.record_resources()
        if not borrowed:
            require(self.docker('network', 'inspect', name, check=False).returncode != 0, 'network already exists')
            self.docker('network', 'create', '--driver', 'bridge', '--label', LABEL + '=' + self.owner, name)
        network, = json.loads(self.docker('network', 'inspect', name).stdout)
        check_network(network, name, owner=None if borrowed else self.owner)
        if borrowed:
            require(network['Id'] == self.borrowed_network['Id'], 'borrowed network changed')
        self.resources['network'].update(id=network['Id'], status='borrowed' if borrowed else 'retained',
            original_endpoints=sorted((network.get('Containers') or {}).keys()))
        save(self.root / 'network-before.json', network)
        self.record_resources()
        override = self.root / 'compose-network.json'
        save(override, dict(networks={'default': dict(external=True, name=name)}))
        self.environment.update(BIGBROTR_PROJECT=self.project, BIGBROTR_OWNER=self.owner,
            BIGBROTR_STATE=str(self.state), BIGBROTR_IMAGE=image['Id'], BIGBROTR_UID=str(os.geteuid()),
            BIGBROTR_GID=str(os.getegid()))
        self.compose = ['docker', 'compose', '--env-file', '/dev/null', '-f',
            str(self.bundle / 'packaging/compose/compose.yaml'), '-f', str(override)]
        self.command([*self.compose, '--profile', 'operator', 'config', '--quiet'])

    def prepare_state(self):
        self.state.mkdir(mode=0o700)
        for name in ('config', 'data', 'postgres', 'secrets', 'secrets/runtime', 'secrets/operator',
                     'data/archive', 'data/archive-state', 'data/index', 'data/scratch', 'data/export'):
            (self.state / name).mkdir(mode=0o700)
        for name in ('runtime/control', 'runtime/catalog', 'runtime/archive-read',
                     'operator/bootstrap', 'operator/migrator', 'operator/owner-password'):
            (self.state / 'secrets' / name).write_text(secrets.token_hex(32))
        self.ids = {key: secrets.token_hex(16) for key in ('instance', 'dataset', 'catalog', 'activation', 'owner')}
        save(self.state / 'operation-ids.json', self.ids)
        text = (self.bundle / 'config/recipes/acceptance.toml').read_text()
        for key, digit in (('instance', '1'), ('dataset', '2'), ('catalog', '3')):
            require(text.count(digit * 32) == 1, 'unexpected recipe identity placeholder')
            text = text.replace(digit * 32, self.ids[key])
        if self.restart_case:
            text = self.restart_checks.deployment_recipe(text, provenance=self.provenance)
        self.config = self.state / 'config/instance.toml'
        self.config.write_text(text)
        self.inputs['initial_recipe_sha256'] = digest(self.config)
        self.home = self.root / 'archive-export'
        self.home.mkdir(mode=0o700)
        shutil.copyfile(self.source / 'tests/fixtures/ingest/import-event.jsonl', self.home / 'event.jsonl')

    def inspect(self, entry):
        result = self.docker('inspect', entry.get('id') or entry['name'], check=False)
        require(result.returncode == 0, 'recorded container unavailable')
        info, = json.loads(result.stdout)
        check_container(info, entry, self.owner, self.project, self.state)
        if entry['service'] != 'database':
            require(info['Image'] == self.resources['image']['id'], 'application image changed')
        else:
            require(info['Image'] == self.inputs['runtime_images']['postgres']['id'], 'database image changed')
        entry.update(id=info['Id'], status=info['State']['Status'])
        self.record_resources()
        return info

    def launch(self, label, service, *arguments):
        entry = dict(name=self.project + '-' + label, service=service, id=None, status='creating')
        require(self.docker('inspect', entry['name'], check=False).returncode != 0, 'container name already exists')
        self.resources['containers'].append(entry)
        self.record_resources()
        if service == 'database':
            # Compose's conventional service name is recorded before creation.
            entry['name'] = self.project + '-database-1'
            self.record_resources()
            require(self.docker('inspect', entry['name'], check=False).returncode != 0, 'database container already exists')
            self.command([*self.compose, 'create', '--no-build', '--pull', 'never', 'database'])
            self.inspect(entry)
            self.docker('start', entry['id'])
        else:
            result = self.command([*self.compose, 'run', '-d', '--no-deps', '--pull', 'never',
                '--name', entry['name'], service, *arguments])
            require(re.fullmatch('[0-9a-f]{64}', result.stdout.strip()), 'missing launched container ID')
            entry['id'] = result.stdout.strip()
        self.inspect(entry)
        return entry

    def logs(self, entry, output):
        result = self.docker('logs', entry['id'])
        (output / 'serve.stdout').write_text(result.stdout)
        (output / 'serve.stderr').write_text(result.stderr)
        return [json.loads(line) for line in result.stdout.splitlines()]

    def finite(self, label, *arguments, service='app', timeout=180):
        entry = self.launch(label, service, *arguments)
        ended = self.docker('wait', entry['id'], timeout=timeout)
        output = self.root / 'bootstrap'
        result = self.docker('logs', entry['id'])
        (output / (label + '.stdout')).write_text(result.stdout)
        (output / (label + '.stderr')).write_text(result.stderr)
        info = self.inspect(entry)
        require(ended.stdout.strip() == '0' and info['State']['ExitCode'] == 0
                and info['State']['Pid'] == 0 and not info['State']['OOMKilled'] and not result.stderr,
                'finite command failed: ' + label)
        return json.loads(result.stdout)

    def wait(self, test, label, seconds, entry=None, client=None):
        """Accept an observation within its bound while supplied owners live."""
        deadline = time.monotonic() + seconds
        while True:
            require(time.monotonic() < deadline, 'timeout: ' + label)
            result = test()
            require(time.monotonic() < deadline, 'timeout: ' + label)
            if entry:
                require(self.inspect(entry)['State']['Running'], 'owner exited before ' + label)
            if client:
                require(client.poll() is None, 'client exited before ' + label)
            # Ownership inspection can itself block. Its time belongs to this
            # observation, including the successful final predicate.
            require(time.monotonic() < deadline, 'timeout: ' + label)
            if result:
                return result
            time.sleep(.05)

    def sql(self, database, statement):
        self.inspect(self.pg)
        return self.lab.sql({'postgres': self.pg['id']}, database, statement)

    def capture(self, output, prefix):
        self.inspect(self.pg)
        pg = {'postgres': self.pg['id']}
        control = self.rows(pg, 'bigbrotr_control')
        require(set(control) == self.tables, 'Control inventory differs from migrations')
        catalog = self.catalog_rows(pg, 'bigbrotr_catalog')
        save(output / (prefix + '-control.json'), control)
        save(output / (prefix + '-catalog.json'), catalog)
        return control, catalog

    def bootstrap(self):
        (self.root / 'bootstrap').mkdir(mode=0o700)
        require(self.finite('build-info', 'build-info') == self.manifest['artifact']['build_info'], 'installed build identity mismatch')
        worker = self.finite('verify-worker', 'archive', 'verify-worker', '--root', '/opt/bigbrotr/archive-worker')
        require(worker['result'] == 'verified', 'installed worker not verified')
        config = ['--file', '/etc/bigbrotr/instance.toml']
        self.finite('config-validate', 'config', 'validate', *config)
        self.pg = self.launch('database', 'database')
        # The image's temporary initialization server accepts Unix sockets only.
        # TCP readiness identifies the final server after initialization/restart.
        self.wait(lambda: self.docker('exec', self.pg['id'], 'pg_isready', '-h', '127.0.0.1', '-U', 'postgres',
                                     '-d', 'postgres', check=False).returncode == 0, 'PostgreSQL startup', 60)
        self.inputs['postgres_server_version'] = self.sql('postgres', 'SHOW server_version_num')
        require(self.inputs['postgres_server_version'] == '160015', 'unexpected PostgreSQL version')
        def provision(stage):
            self.inspect(self.pg)
            self.docker('exec', self.pg['id'], 'psql', '-X', '-U', 'postgres', '-d', 'postgres',
                '-v', 'ON_ERROR_STOP=1', '-v', 'stage=' + stage, '-f', '/opt/bigbrotr/provision.sql',
                timeout=60, label='provision-' + stage)
        provision('bootstrap')
        for action in ('plan', 'apply'):
            self.finite('migration-' + action, 'migrate', action, *config, '--user', 'bigbrotr_migrator',
                        '--password-ref', 'file:/run/operator/migrator', '--target', '41', service='initialize')
        provision('control')
        self.finite('config-initialize', 'config', 'initialize', *config)
        self.finite('config-activate', 'config', 'activate-initial', *config,
                    '--request-id', self.ids['activation'], '--expected-revision', '1')
        issued = self.finite('owner-token', 'access', 'owner-token', 'issue', *config,
            '--username', 'alice', '--request-id', self.ids['owner'], service='initialize')
        require(re.fullmatch('[0-9a-f]{64}', issued['token']), 'invalid owner token')
        (self.state / 'secrets/operator/owner-token').write_text(issued['token'])
        self.enrolled = self.finite('owner-enroll', 'access', 'enroll-owner', *config,
            '--challenge-id', issued['challenge_id'], '--token-ref', 'file:/run/operator/owner-token',
            '--password-ref', 'file:/run/operator/owner-password', service='initialize')
        self.finite('owner-status', 'access', 'owner-token', 'status', *config)
        self.finite('archive-initialize', 'archive', 'initialize', *config,
            '--initializer-user', 'postgres', '--initializer-password-ref', 'file:/run/operator/bootstrap',
            '--worker-root', '/opt/bigbrotr/archive-worker', service='initialize')
        self.finite('archive-state', 'archive', 'state-initialize', *config)
        self.finite('archive-coordinator', 'archive', 'coordinator-initialize', *config)
        provision('reader')
        save(self.home / 'fixture-profile.json', dict(format='bigbrotr-installed-archive-fixture/1',
            product_binary=str(self.bundle / 'bigbrotr'),
            password_ref='file:' + str(self.state / 'secrets/operator/owner-password'),
            expected_principal_id=self.enrolled['principal_id'],
            expected_actions=['audit.read', 'budgets.change', 'data.read', 'policy.change',
                              'principals.manage', 'roles.control', 'status.read']))

    def stop_container(self, entry, output=None):
        info = self.inspect(entry)
        if info['State']['Running']:
            self.docker('stop', '--time', '30', entry['id'], timeout=40)
        info = self.inspect(entry)
        require(not info['State']['Running'] and info['State']['Pid'] == 0
                and not info['State']['OOMKilled'], 'container did not stop cleanly')
        if output:
            self.logs(entry, output)
            save(output / 'terminal-container.json', dict(id=info['Id'], state=info['State']))
        return info

    def initial(self):
        self.source_unchanged()
        output = self.root / 'initial'
        output.mkdir(mode=0o700)
        before, _ = self.capture(output, 'before')
        require(not before['producer_requests'] and not before['jobs'], 'initial state is not fresh')
        require(before['local_accounts'][0]['principal_id'] == '\\x' + self.enrolled['principal_id'], 'enrolled owner differs')
        if self.restart_case:
            self.restart_checks.accepted_profile(before, self.provenance)
        self.inputs['sync_provenance'] = 'local_first_witness' if self.provenance else 'off'
        setup = dict(control=before, resource=dict(catalog_id=self.ids['catalog']))
        entry = self.initial_launch()
        first = self.wait(lambda: self.logs(entry, output), 'initial lifetime', 20, entry=entry)[0]
        require(first['state'] == 'initialized' and first['ready'] is False
                and first['acquisition'] == 'finite-initial', 'untruthful initial readiness')
        self.initialized = first
        save(output / 'initialized.json', first)
        save(self.home / 'visibility.json', first['private_scope'])
        with (self.home / 'client.stdout').open('x') as out, (self.home / 'client.stderr').open('x') as err:
            self.client = subprocess.Popen([sys.executable, '-B', str(self.source /
                'experiments/ingest-bootstrap/installed_archive_client.py'), str(self.home)],
                env=self.environment, cwd=self.root, stdout=out, stderr=err, start_new_session=True)
        self.processes.append(self.client)
        save(output / 'client-process.json', dict(pid=self.client.pid, process_group=self.client.pid))
        self.initial_source_barrier(entry, output)
        self.wait(lambda: (self.home / 'prepared.json').exists(), 'source commands', 15, entry, self.client)
        prepared, _ = self.capture(output, 'prepared')
        self.validate_source(self.current_owner_view(prepared, setup), first, load(self.home / 'prepared.json'))
        (self.home / 'prepared.continue').write_text('independent source relationships checked\n')
        self.wait(lambda: (self.home / 'submitted.json').exists(), 'submitted acquisition', 15, entry, self.client)
        self.initial_archive_observation(entry, output)
        self.initial_publication(entry, output)
        (self.home / 'submitted.continue').write_text('original frontier observed\n')
        self.wait(lambda: (self.home / 'commands-finished.json').exists(), 'client logout', 15, entry, self.client)
        self.wait(self.initial_source_quiet, 'quiet controlled receive', 5, entry, self.client)
        (self.home / 'stop-requested').write_text('owned orderly stop\n')
        terminal = self.stop_container(entry, output)
        final = self.logs(entry, output)[-1]
        require(terminal['State']['ExitCode'] == 0 and final['native'] == 'closed'
                and final['owner_release'] == 'committed', 'initial owner did not close and release')
        (self.home / 'commands-finished.continue').write_text('original owner joined\n')
        require(self.client.wait(timeout=6) == 0 and not (self.home / 'client.stderr').read_bytes(), 'signed client failed')
        stop_process(self.client)
        self.processes.remove(self.client)
        require(load(self.home / 'client-result.json')['status'] == 'PASS', 'missing signed client result')
        after, _ = self.capture(output, 'after')
        selected = self.current_owner_view(after, setup)
        owner, = [row for row in selected['owner_epochs'] if row['role'] == 'control']
        require(owner['state'] == 'released', 'initial Control ownership retained')
        self.backend_absence(output)

    def initial_source_quiet(self):
        return any(load(path)['number'] >= 3
                   and (not getattr(self, 'retained_boundary', False) or load(path).get('retained_quiet') is True)
                   and not path.with_name(path.name.replace('open', 'closed')).exists()
                   for path in sorted(self.home.glob('relay-open-*.json'), reverse=True))

    def initial_archive_observation(self, entry, output):
        def published():
            require(not (self.home / 'relay-error.json').exists(), 'controlled relay failed')
            return self.archive_published()
        self.wait(published, 'signed Archive frontier', 60, entry, self.client)
        control, catalog = self.capture(output, 'published')
        require(max(row['snapshot_id'] for row in catalog['public.ducklake_snapshot']) == 5,
                'unexpected Archive snapshot')
        require(len(control['producer_requests']) == len(control['producer_outcomes']) == 4, 'unexpected producer request count')

    def initial_source_barrier(self, entry, output):
        # Closed variants may hold the existing source client before later
        # admissions. The ordinary signed journey has no additional barrier.
        pass

    def initial_launch(self):
        return self.launch('initial', 'app', 'serve', '--file', '/etc/bigbrotr/instance.toml',
            '--roles', 'control,ingest,sync,archive', '--initialize-ingest')

    def archive_published(self):
        return self.sql('bigbrotr_control', 'SELECT count(*) FROM control.consumer_frontiers WHERE last_complete_sequence=' + str(self.initial_cut)) == '1'

    def initial_publication(self, entry, output):
        # The optional Index case observes its own live publication here, before
        # releasing the existing signed client's completion gate.
        pass

    def backend_absence(self, output):
        observed = {database: self.sql(database,
            'SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid()')
            for database in ('bigbrotr_control', 'bigbrotr_catalog')}
        save(output / 'backend-absence.json', observed)
        require(set(observed.values()) == {'0'}, 'application database backend remains')

    def ordinary_client(self, output, entry):
        result = self.command([sys.executable, '-B', str(self.source /
            'experiments/ingest-bootstrap/installed_archive_export_client.py'), str(self.home)],
            timeout=40, label='ordinary-client')
        require(not result.stderr, 'ordinary client diagnostic output')

    def ordinary(self):
        self.source_unchanged()
        output = self.root / 'ordinary'
        output.mkdir(mode=0o700)
        original = self.config.read_text()
        (output / 'initial-config.toml').write_text(original)
        destination = secrets.token_hex(16)
        changed = self.set_value(original, 'archive.reader', 'enabled', True)
        changed = self.set_value(changed, 'export.destination', 'id', destination)
        changed = self.set_value(changed, 'export.destination', 'root', '/var/lib/bigbrotr/export')
        self.config.write_text(changed)
        self.finite('reader-validate', 'config', 'validate', '--file', '/etc/bigbrotr/instance.toml')
        initialized = self.finite('destination-initialize', 'export', 'destination-initialize',
                                  '--file', '/etc/bigbrotr/instance.toml')
        marker = self.state / 'data/export/destination.json'
        require(initialized['binding_sha256'] == digest(marker), 'destination binding mismatch')
        (self.home / 'destination').mkdir(mode=0o700)
        shutil.copyfile(marker, self.home / 'destination/destination.json')
        save(self.home / 'export-input.json', dict(scope=self.initialized['private_scope'],
            catalog_id=self.ids['catalog'], destination_id=destination))
        before, catalog_before = self.capture(output, 'before')
        files_before = self.files(self.state / 'data/archive')
        save(output / 'before-archive-files.json', files_before)
        entry = self.launch('ordinary', 'app', 'serve', '--file', '/etc/bigbrotr/instance.toml', '--roles', 'control')
        first = self.wait(lambda: self.logs(entry, output), 'ordinary Control', 20, entry=entry)[0]
        require(first['state'] == 'idle', 'ordinary Control did not start')
        save(output / 'initialized.json', first)
        self.ordinary_client(output, entry)
        terminal = self.stop_container(entry, output)
        require(terminal['State']['ExitCode'] == 0, 'ordinary Control failed to stop')
        after, catalog_after = self.capture(output, 'after')
        files_after = self.files(self.state / 'data/archive')
        save(output / 'after-archive-files.json', files_after)
        require(catalog_after == catalog_before and files_after == files_before, 'ordinary read/export mutated Archive')
        self.export_result = self.export_validate(before, after, self.home, self.initialized, self.ids['catalog'],
            expected_principal=self.enrolled['principal_id'], expected_table_count=82, expected_tables=self.tables,
            destination_root=self.state / 'data/export', initial_cut=self.initial_cut)
        save(output / 'export-verdict.json', self.export_result)
        self.backend_absence(output)
        self.source_unchanged()
        self.inputs['export'] = {key: self.export_result[key] for key in
            ('jsonl_bytes', 'jsonl_sha256', 'manifest_sha256', 'export_pin_retained', 'all_fresh_readers_released')}

    def census(self, output, name, *, profile='default'):
        """A joined, read-only C API child using only the verified candidate worker."""
        self.inspect(self.pg)
        pg = {'postgres': self.pg['id']}
        catalog_before = self.catalog_rows(pg, 'bigbrotr_catalog')
        save(output / (name + '-census-before-catalog.json'), catalog_before)
        config = self.set_value(self.config.read_text(), 'storage', 'archive_root', str(self.state / 'data/archive'))
        # This stopped-product diagnostic uses the owned catalog directly;
        # --catalog-port validates the declaration, it does not override it.
        config = self.set_value(config, 'postgres.catalog', 'endpoint', '127.0.0.1:15432')
        config = self.set_value(config, 'postgres.catalog', 'read_password_ref',
            'file:' + str(self.state / 'secrets/runtime/archive-read'))
        path = output / (name + '-reader.toml')
        path.write_text(config)
        command = [sys.executable, '-B', str(self.source / 'experiments/ingest-bootstrap/installed_archive_read.py'),
            str(path), str(output / (name + '-temporary')), '--worker-root', str(self.bundle / 'archive-worker'),
            '--catalog-port', '15432', '--full-stream-order', '--override-data-path']
        if profile != 'default':
            command += ['--census-profile', profile]
        result = self.command(command, timeout=30, label='census-' + name)
        require(not result.stderr, 'independent C API diagnostic output')
        value = json.loads(result.stdout)
        save(output / (name + '-science.json'), value)
        self.backend_absence(output)
        self.inspect(self.pg)
        catalog_after = self.catalog_rows(pg, 'bigbrotr_catalog')
        save(output / (name + '-census-after-catalog.json'), catalog_after)
        require(catalog_after == catalog_before, 'independent C API census changed the catalog')
        return value

    def restart_baseline(self):
        self.source_unchanged()
        output = self.root / 'restart'
        output.mkdir(mode=0o700)
        self.restart_output = output
        self.original_read_config = self.config.read_text()
        self.restart_before, _ = self.capture(output, 'before')
        self.original_export_files = self.files(self.state / 'data/export')
        save(output / 'before-export-files.json', self.original_export_files)
        self.baseline_census = self.census(output, 'initial')
        initial_before = load(self.root / 'initial/before-control.json')
        initial_after = load(self.root / 'initial/after-control.json')
        view = self.current_owner_view(initial_after, dict(control=initial_before, resource=dict(catalog_id=self.ids['catalog'])))
        wires = [load(path) for path in sorted(self.home.glob('relay-closed-*.json'))]
        owner, = [r for r in view['owner_epochs'] if r['role'] == 'control']
        self.old_event = load(self.home / 'event.jsonl')
        self.first_science = self.restart_checks.first_science(view, self.initialized, self.old_event, wires,
            self.baseline_census, self.state / 'data/ingest', owner['released_at_us'], provenance=self.provenance)
        self.restart_checks.publication(initial_after, self.baseline_census, self.first_science,
            revision=4, snapshot=5, topology=0, cuts=[(1, self.initial_cut)])
        self.original_inputs = self.files(self.state / 'data/ingest/producer-inputs-v1')
        save(output / 'original-input-files.json', self.original_inputs)

    def wait_login_capacity(self, output):
        """Observe the real password window before starting a finite owner."""
        account, peer = (self.restart_checks.login_buckets()[kind][0] for kind in ('account', 'peer'))
        dataset = self.ids['dataset']
        require(re.fullmatch('[0-9a-f]{32}', dataset), 'invalid fixture dataset')
        statement = f"""SELECT json_build_object(
            'now_us',(EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint,
            'buckets',(SELECT coalesce(json_agg(r),'[]') FROM (
                SELECT encode(dataset_id,'hex') AS dataset_id,kind,bucket,attempts,window_started_at_us
                FROM control.login_buckets WHERE dataset_id=decode('{dataset}','hex')
                AND ((kind='account' AND bucket={account}) OR (kind='peer' AND bucket={peer}))
                ORDER BY kind) r))"""
        observations, deadline = [], time.monotonic() + 65
        while True:
            require(time.monotonic() < deadline, 'fixture login window did not become eligible')
            observed = json.loads(self.sql('bigbrotr_control', statement))
            require(time.monotonic() < deadline, 'fixture login observation exceeded its deadline')
            delay = self.restart_checks.login_delay(observed, dataset)
            observations.append(dict(observed, wait_us=delay))
            save(output / 'login-admission.json', observations)
            if delay == 0:
                return
            require(len(observations) < 8 and delay / 1_000_000 < deadline - time.monotonic(),
                'fixture login window exceeded its bound')
            # Re-read PostgreSQL's clock at expiry; sleeping alone is not proof
            # of eligibility. No bucket rewrite, login retry or limit change.
            time.sleep(delay / 1_000_000)

    def restart_ingest(self):
        output = self.restart_output
        self.config.write_text((self.root / 'ordinary/initial-config.toml').read_text())
        args = ['ingest', 'recover', '--file', '/etc/bigbrotr/instance.toml']
        abandoned = self.finite('abandon-initial', *args, '--abandon-acquisition')
        require(abandoned['acquisition_abandonment'] == dict(jobs='1', reason='initial_producer_abandoned', coverage='unknown'),
            'initial acquisition abandonment differs')
        retired = self.finite('retire-initial', *args, '--retire-initial')
        self.retired_initial, _ = self.capture(output, 'retired-initial')
        self.restart_checks.retired(self.retired_initial, retired, 1, self.first_science, cut=self.initial_cut)
        self.backend_absence(output)
        self.wait_login_capacity(output)
        home = self.root / 'successor-client'
        home.mkdir(mode=0o700)
        self.successor_home = home
        for name in ('event.jsonl', 'fixture-profile.json', 'visibility.json'):
            shutil.copyfile(self.home / name, home / name)
        shutil.copyfile(self.source / 'tests/fixtures/acquisition/selector-events.json', home / 'selector-events.json')
        self.new_event, = [row['event'] for row in load(home / 'selector-events.json')['events'] if row['name'] == 'included']
        entry = self.successor_launch()
        started = self.wait(lambda: self.logs(entry, output), 'successor activation', 20, entry=entry)[0]
        require(started['state'] == 'activated' and started['producer_epoch'] == '2'
            and started['topology_generation'] == '1' and started['config_revision'] == started['policy_revision'] == '1'
            and started['outstanding_request_limit'] == '32' and started['ready'] is False
            and started['acquisition'] == 'finite-initial-successor', 'unexpected successor profile')
        self.successor_started = started
        save(output / 'successor-started.json', started)
        activated, _ = self.capture(output, 'successor-started')
        self.successor_activated(started, activated)
        with (home / 'client.stdout').open('x') as out, (home / 'client.stderr').open('x') as err:
            client = subprocess.Popen([sys.executable, '-B', str(self.source / 'experiments/ingest-bootstrap/installed_live_client.py'),
                'journey', str(home), '--profile', str(home), '--expected-config-revision', '1', '--expected-policy-revision', '1',
                *self.successor_client_options()],
                env=self.environment, cwd=self.root, stdout=out, stderr=err, start_new_session=True)
        self.processes.append(client)
        save(output / 'client-process.json', dict(pid=client.pid, process_group=client.pid))
        self.wait(lambda: (home / 'prepared.json').exists(), 'successor source registration', 15, entry, client)
        prepared, _ = self.capture(output, 'successor-prepared')
        self.restart_checks.prepared(prepared, load(home / 'prepared.json'), self.initialized['private_scope'])
        (home / 'prepared.continue').write_text('source relationships independently checked\n')
        self.wait(lambda: (home / 'submitted.json').exists(), 'successor acquisition', 15, entry, client)
        def classified():
            require(not (home / 'relay-error.json').exists(), 'successor relay failed')
            return self.sql('bigbrotr_control', 'SELECT count(*) FROM control.acquisition_progress WHERE producer_epoch=2 AND classified_events=3') == '1'
        self.successor_submission(entry, client, classified)
        self.successor_published(entry, client)
        self.wait(lambda: (home / 'commands-finished.json').exists(), 'successor logout', 15, entry, client)
        def quiet():
            return any(load(path)['number'] >= 2 and not path.with_name(path.name.replace('open', 'closed')).exists()
                for path in sorted(home.glob('relay-open-*.json'), reverse=True))
        self.wait(quiet, 'successor quiet receive', 5, entry, client)
        terminal = self.stop_container(entry, output)
        final = self.logs(entry, output)[-1]
        require(terminal['State']['ExitCode'] == 0 and final == dict(format='bigbrotr-initial-successor/1', state='stopped',
            native='closed', owner_release='committed', source_closure='not-performed', producer_epoch='2', ready=False),
            'successor did not close and release')
        (home / 'commands-finished.continue').write_text('successor owner joined\n')
        require(client.wait(timeout=6) == 0 and not (home / 'client.stderr').read_bytes(), 'successor client failed')
        stop_process(client)
        self.processes.remove(client)
        require(load(home / 'client-result.json')['status'] == 'PASS', 'missing successor client verdict')
        self.successor_after, _ = self.capture(output, 'successor-after')
        self.successor_completed = time.time_ns() // 1000
        self.successor_wires = [load(path) for path in sorted(home.glob('relay-closed-*.json'))]
        self.restart_checks.successor_wire(self.successor_wires, self.old_event, self.new_event)
        self.backend_absence(output)
        abandoned = self.finite('abandon-successor', *args, '--abandon-activated-acquisition')
        require(abandoned['acquisition_abandonment'] == dict(jobs='1', reason='activated_initial_producer_abandoned', coverage='unknown'),
            'successor acquisition abandonment differs')
        self.successor_abandoned()
        self.successor_retirement = self.finite('retire-successor', *args, '--retire-activated-initial')
        self.retired_successor, _ = self.capture(output, 'retired-successor')
        self.backend_absence(output)

    def successor_launch(self):
        return self.launch('successor', 'app', 'serve', '--file', '/etc/bigbrotr/instance.toml',
            '--activate-initial-successor', '--roles', 'control,ingest,sync')

    def successor_activated(self, started, state):
        """Optional bridge observation before the successor client starts."""

    def successor_client_options(self):
        return []

    def successor_submission(self, entry, client, classified):
        self.wait(classified, 'three classified successor events', 30, entry, client)
        (self.successor_home / 'submitted.continue').write_text('finite three-event head observed\n')

    def successor_published(self, entry, client):
        """Optional opt-in observation after the finite submission is released."""

    def successor_abandoned(self):
        """Optional case observation at the actual closed, unretired boundary."""

    def restart_archive(self):
        output = self.restart_output
        report = self.finite('archive-catch-up', 'archive', 'catch-up', '--file', '/etc/bigbrotr/instance.toml')
        self.caught_up, self.caught_up_catalog = self.capture(output, 'caught-up')
        for table in ('producer_requests', 'producer_outcomes', 'producer_checkpoints',
                      'producer_checkpoint_dependencies', 'acquisition_progress'):
            require(self.caught_up[table] == self.successor_after[table], 'terminal replay changed producer evidence: ' + table)
        self.caught_up_files = self.files(self.state / 'data/archive')
        save(output / 'caught-up-archive-files.json', self.caught_up_files)
        self.final_census = self.census(output, 'caught-up')
        self.final_science = self.restart_checks.successor_science(self.retired_initial, self.successor_after,
            self.successor_started, self.first_science, self.old_event, self.new_event, self.successor_wires,
            self.final_census, self.state / 'data/ingest', self.successor_completed, provenance=self.provenance)
        self.restart_checks.retired(self.retired_successor, self.successor_retirement, 2, self.final_science,
            cut=self.restart_profile['successor_cut'])
        self.restart_checks.publication(self.caught_up, self.final_census, self.final_science,
            revision=6, snapshot=self.restart_profile['snapshot'], topology=1,
            cuts=[(1, self.initial_cut), (2, self.restart_profile['successor_cut'])])
        self.restart_checks.catch_up_report(report, self.caught_up, retry=False, provenance=self.provenance)
        self.restart_checks.bridge(self.caught_up, self.baseline_census['tables']['archive_commits'], provenance=self.provenance)
        self.restart_checks.preserve_export(self.restart_before, self.caught_up)
        require(self.files(self.state / 'data/export') == self.original_export_files, 'original export files changed')
        originals = self.files(self.state / 'data/ingest/producer-inputs-v1')
        require(len(originals) == 4 + self.restart_profile['successor_requests']
            and all(originals.get(name) == value for name, value in self.original_inputs.items()),
            'original input custody changed')
        self.all_input_files = originals
        save(output / 'independent-verdict.json', dict(scientific_rows=sum(self.restart_profile['counts']),
            family_counts=self.restart_profile['counts'], producer_requests=[4, self.restart_profile['successor_requests']],
            archive_commits=4 + self.restart_profile['successor_requests'], snapshot_id=self.restart_profile['snapshot'], frontier_revision=6,
            original_science_unchanged=True, original_export_unchanged=True, native_census='NOT_RUN'))

    def restart_retry(self):
        output = self.restart_output
        report = self.finite('archive-catch-up-retry', 'archive', 'catch-up', '--file', '/etc/bigbrotr/instance.toml')
        after, catalog = self.capture(output, 'retry')
        self.restart_checks.catch_up_report(report, after, retry=True, provenance=self.provenance)
        self.restart_checks.retry_unchanged(self.caught_up, after)
        require(catalog == self.caught_up_catalog and self.files(self.state / 'data/archive') == self.caught_up_files,
            'exact catch-up retry changed Archive')
        require(self.census(output, 'retry') == self.final_census, 'exact catch-up retry changed scientific rows')
        require(self.files(self.state / 'data/ingest/producer-inputs-v1') == self.all_input_files, 'catch-up retry changed original inputs')

    def restart_history(self):
        self.source_unchanged()
        output = self.root / 'successor-history'
        output.mkdir(mode=0o700)
        self.config.write_text(self.original_read_config)
        shutil.copyfile(self.home / 'fixture-profile.json', output / 'fixture-profile.json')
        save(output / 'events.json', [self.old_event, self.new_event])
        save(output / 'read-input.json', dict(scope=self.initialized['private_scope'], catalog_id=self.ids['catalog'],
            frontier_revision=6, snapshot_id=self.restart_profile['snapshot'], config_revision='1', policy_revision='1'))
        before, catalog = self.capture(output, 'before')
        self.wait_login_capacity(output)
        entry = self.launch('successor-history', 'app', 'serve', '--file', '/etc/bigbrotr/instance.toml', '--roles', 'control')
        first = self.wait(lambda: self.logs(entry, output), 'successor ordinary Control', 20, entry=entry)[0]
        require(first['state'] == 'idle', 'ordinary successor reader did not start')
        startup = self.history_baseline.successor_client(self,output,entry)
        require(load(output / 'history-client-result.json')['status'] == 'PASS', 'successor ordinary history failed')
        terminal = self.stop_container(entry, output)
        require(terminal['State']['ExitCode'] == 0, 'successor ordinary Control did not stop')
        after, final_catalog = self.capture(output, 'after')
        self.restart_checks.history_unchanged(before, after, provenance=self.provenance,startup=startup)
        self.restart_checks.preserve_export(self.restart_before, after)
        require(final_catalog == catalog == self.caught_up_catalog and self.files(self.state / 'data/archive') == self.caught_up_files,
            'ordinary history changed Archive')
        require(self.files(self.state / 'data/export') == self.original_export_files, 'ordinary history changed original export')
        require(self.files(self.state / 'data/ingest/producer-inputs-v1') == self.all_input_files, 'ordinary history changed original inputs')
        self.backend_absence(output)
        self.source_unchanged()

    def cleanup(self):
        errors = self.resources['cleanup_errors']
        for process in list(self.processes):
            try:
                stop_process(process)
                self.processes.remove(process)
            except Exception as error:
                errors.append(dict(resource='process', id=process.pid, error=type(error).__name__))
        # Stop applications first, then PostgreSQL. All data and stopped resources
        # are retained; borrowed bridges and unrelated endpoints are untouched.
        entries = sorted(self.resources['containers'], key=lambda entry: entry['service'] == 'database')
        for entry in entries:
            try:
                # A failed inspect after an ambiguous create is not evidence of
                # absence: daemon failure and an unfinished create must fail custody.
                self.inspect(entry)
                info = self.stop_container(entry)
                entry.update(status='stopped-retained', exit_code=info['State']['ExitCode'], pid=info['State']['Pid'])
                self.docker('logs', entry['id'], label='terminal-' + entry['service'])
            except Exception as error:
                errors.append(dict(resource='container', name=entry['name'], error=type(error).__name__))
        if self.resources['image'] and self.resources['image']['id'] is None:
            try:
                entry = self.resources['image']
                info, = json.loads(self.docker('image', 'inspect', entry['tag']).stdout)
                require(info['Config']['Labels'][LABEL] == self.owner
                    and info['Config']['Labels'][IMAGE_LABEL] == self.inputs['candidate_bundle_sha256'],
                    'ambiguous image build ownership')
                entry.update(id=info['Id'], status='retained-after-ambiguous-build')
            except Exception as error:
                errors.append(dict(resource='image-build', error=type(error).__name__))
        if self.resources['network']:
            try:
                entry = self.resources['network']
                info, = json.loads(self.docker('network', 'inspect', entry['id'] or entry['name']).stdout)
                check_network(info, entry['name'], owner=None if entry['borrowed'] else self.owner)
                require(entry['id'] in (None, info['Id']), 'network identity changed')
                entry['id'] = info['Id']
                save(self.root / 'network-after.json', info)
            except Exception as error:
                errors.append(dict(resource='network', error=type(error).__name__))
        if self.original_lab_command is not None:
            self.lab.command = self.original_lab_command
        self.record_resources()
        require(not errors, 'owned resource cleanup incomplete')


def first_launch(args, checks, inputs):
    """Append to the established acceptance report; never emit a second verdict."""
    reclamation = getattr(args,'case',None)=='p01-first-launch-closed-reclamation'
    restart = getattr(args, 'case', None) in RESTART_CASES and not reclamation
    retained_boundary = reclamation or getattr(args, 'case', None) == 'p01-first-launch-retained-boundary'
    index_outage = getattr(args, 'case', None) == 'p01-first-launch-index-outage'
    archive_outage = retained_boundary or getattr(args, 'case', None) == 'p01-first-launch-archive-outage'
    shared_budget = getattr(args, 'case', None) == 'p01-first-launch-shared-budget'
    live_topology = getattr(args, 'case', None) == 'p01-first-launch-index-live-topology'
    index_topology = live_topology or getattr(args, 'case', None) == 'p01-first-launch-index-topology'
    index_commit = getattr(args, 'case', None) in ('p01-first-launch-index-commit-before','p01-first-launch-index-commit-after')
    index_restart = index_commit or getattr(args, 'case', None) == 'p01-first-launch-index-restart'
    pin_release = getattr(args, 'case', None) in ('p01-first-launch-index-pin-release-before', 'p01-first-launch-index-pin-release-after')
    index_case = index_outage or shared_budget or archive_outage or index_topology or index_commit or pin_release or getattr(args, 'case', None) in ('p01-first-launch-index', 'p01-first-launch-index-short-pin', 'p01-first-launch-index-restart')
    if index_case:
        from cases.first_launch_index import IndexRuntime, STAGES as INDEX_STAGES, LIMITS as INDEX_LIMITS
        if args.case == 'p01-first-launch-index-short-pin':
            from cases.first_launch_index_pin import ShortPinRuntime as IndexRuntime, LIMITS as INDEX_LIMITS
        if index_restart:
            from cases.first_launch_index_restart import (RestartIndexRuntime as IndexRuntime,
                STAGES as INDEX_STAGES, LIMITS as INDEX_LIMITS)
        if pin_release:
            from cases.first_launch_index_pin_release import (PinReleaseRuntime as IndexRuntime,
                STAGES as INDEX_STAGES, LIMITS as INDEX_LIMITS)
        if index_commit:
            from cases.first_launch_index_commit import (IndexCommitRuntime as IndexRuntime,
                STAGES as INDEX_STAGES, LIMITS as INDEX_LIMITS)
        if index_topology:
            from cases.first_launch_index_topology import (TopologyIndexRuntime as IndexRuntime,
                STAGES as INDEX_STAGES, LIMITS as INDEX_LIMITS)
        if live_topology:
            from cases.first_launch_index_live import (LiveTopologyIndexRuntime as IndexRuntime,
                STAGES as INDEX_STAGES, LIMITS as INDEX_LIMITS)
        if archive_outage:
            from cases.first_launch_archive_outage import (ArchiveOutageRuntime as IndexRuntime,
                STAGES as INDEX_STAGES, LIMITS as INDEX_LIMITS)
        if index_outage:
            from cases.first_launch_index_outage import IndexOutageRuntime as IndexRuntime, LIMITS as INDEX_LIMITS
        if retained_boundary:
            from cases.first_launch_retained import (RetainedRuntime as IndexRuntime,
                STAGES as INDEX_STAGES, LIMITS as INDEX_LIMITS)
        if reclamation:
            from cases.first_launch_retained import (ReclamationRuntime as IndexRuntime,
                RECLAMATION_STAGES as INDEX_STAGES, RECLAMATION_LIMITS as INDEX_LIMITS)
        if shared_budget:
            from cases.first_launch_shared_budget import (SharedBudgetRuntime as IndexRuntime,
                STAGES as INDEX_STAGES, LIMITS as INDEX_LIMITS)
    tutorial = getattr(args, 'case', None) == 'p01-first-launch-tutorial'
    runtime_type = IndexRuntime if index_case else Runtime
    if tutorial:
        from cases.first_launch_tutorial import TutorialRuntime, LIMITS as TUTORIAL_LIMITS
        runtime_type = TutorialRuntime
    args.case_limits = OFF_LIMITS if getattr(args, 'case', None) == OFF_CASE else RESTART_LIMITS if restart else LIMITS
    if index_case:
        args.case_limits = INDEX_LIMITS
    if tutorial:
        args.case_limits = TUTORIAL_LIMITS
    args.case_cleanup = 'Owned process groups joined and verified containers stopped; protected data, logs, images and networks retained. Borrowed networks never removed.'
    stage_checks = {name: dict(name='first_launch_' + name, result='NOT_RUN') for name in (INDEX_STAGES if index_case else RESTART_STAGES if restart else STAGES)}
    checks.extend(stage_checks.values())
    previous_mask = os.umask(0o077)
    runtime = None
    current = 'prerequisites'
    interrupt_scope = interrupted()
    interrupt_scope.__enter__()
    try:
        args.evidence.chmod(0o700)
        runtime = runtime_type(args, inputs)
        operations = [('prerequisites', runtime.preflight), ('candidate', runtime.candidate),
                ('runtime_image', runtime.image_and_state), ('fresh_bootstrap', runtime.bootstrap),
                ('signed_acquisition', runtime.initial)]
        if index_restart:
            operations += [('index_restart_baseline', runtime.index_restart_baseline)]
            if index_commit:
                operations += [('index_publication_commit_loss', runtime.index_commit_fault)]
            operations += [('index_catch_up', runtime.index_catch_up), ('index_exact_retry', runtime.index_retry)]
        if index_case:
            operations += [('index_publication', runtime.verify_index)]
        if not archive_outage:
            operations += [('ordinary_history_export', runtime.ordinary)]
        if pin_release:
            operations += [('pin_release_uncertainty', runtime.pin_release_uncertainty)]
        if reclamation:
            operations += [('closed_reclamation',runtime.closed_reclamation),('compacted_successor',runtime.compacted_successor),
                ('compacted_consumers',runtime.compacted_consumers),('compacted_history_export',runtime.compacted_history_export)]
        if restart:
            operations += [('restart_baseline', runtime.restart_baseline), ('ingest_restart_resend', runtime.restart_ingest),
                ('archive_catch_up', runtime.restart_archive), ('catch_up_exact_retry', runtime.restart_retry)]
            if index_topology:
                operations += [('index_epoch2_catch_up', runtime.index_epoch2_catch_up),
                               ('index_epoch2_exact_retry', runtime.index_epoch2_retry)]
            operations += [('successor_history', runtime.restart_history)]
            if live_topology:
                operations += [('historical_index_topology', runtime.historical_index_topology)]
        for current, operation in operations:
            operation()
            stage_checks[current]['result'] = 'PASS'
    except MissingPrerequisite as error:
        stage_checks[current]['detail'] = str(error)
    except (Exception, KeyboardInterrupt) as error:
        stage_checks[current].update(result='FAIL', detail=type(error).__name__)
        save(args.evidence / 'failure.json', dict(stage=current, error_type=type(error).__name__, reason=str(error),
                                                traceback=failure_trace(error, args.source_root)))
    finally:
        try:
            if runtime is not None:
                runtime.cleanup()
            stage_checks['cleanup']['result'] = 'PASS'
        except (Exception, KeyboardInterrupt) as error:
            stage_checks['cleanup'].update(result='FAIL', detail=type(error).__name__)
        interrupt_scope.__exit__(None, None, None)
        os.umask(previous_mask)
