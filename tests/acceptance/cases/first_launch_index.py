"""One fresh installed Index lifetime alongside the existing Archive journey.

No engine is started on import. Credentials and raw captures stay in the same
protected, retained first-launch resource owner; no restart or outage claim.
"""
import hashlib
import http.client
import importlib.util
import json
import os
from pathlib import Path
import re
import secrets
import shutil
import socket
import stat
import subprocess
import sys
import time

from cases.first_launch import (Runtime, MissingPrerequisite, LABEL, digest, load,
                               require, save, stop_process)
from cases import first_launch_index_oracle as oracle

STAGES = ('prerequisites', 'candidate', 'runtime_image', 'fresh_bootstrap',
          'signed_acquisition', 'index_publication', 'ordinary_history_export', 'cleanup')
LIMITS = [
    'Fresh installed signed source publishes all ten Index targets and PG proof while Archive advances; ordinary Archive history/export remains exact.',
    'Authenticated initial role/source/publication observations must match the independent signed cut and current PG identity without touching session idle time.',
    'After positive readiness, one independently inserted conflicting event row must make Index unavailable while authenticated Control and the original Archive read/export journey remain usable; the fault is retained.',
    'Ordinary Control must report both private capabilities Ready at rev4/cut12/snapshot5, then refuse full readiness while one exact retained input is withheld; a narrow read cannot revive it, and exact restoration permits full-probe recovery.',
    'Original ordinary journey40s/export12s gates remain; five added readiness phases each have an absolute30s bound (overall190s). Status-only barriers must not refresh session idle time.',
    'NOT_RUN: direct RocksDB/native census, Index restart, remote uncertainty resolution, column-TTL negative, Index query, independent service outages/capacity and complete M1.',
    'The signed fixture has empty tags: event_tag_index has an explicit zero outcome; nonempty tag projection retains its separate fixture qualification.',
    'Configured process/disk limits are not empirical capacity qualification; local candidate identity is not release authentication.',
]
HTTP_PORT, TCP_PORT = 18123, 19000
DATABASE = 'serving_fixture'
ROLES = ('provisioner', 'initializer', 'writer', 'reader')
SYSTEM_TABLES = ('databases', 'disks', 'storage_policies', 'settings', 'tables', 'columns', 'projections')


def input_identity(path, *, directory=False, empty=False):
    require(type(empty) is bool and not (directory and empty), 'input identity profile')
    require(path.is_absolute() and path.resolve(strict=True) == path, 'input custody path alias')
    flags = os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK
    descriptor = os.open(path, flags | (os.O_DIRECTORY if directory else 0))
    try:
        metadata = os.fstat(descriptor)
        require((stat.S_ISDIR(metadata.st_mode) if directory else stat.S_ISREG(metadata.st_mode))
                and metadata.st_uid == os.geteuid()
                and stat.S_IMODE(metadata.st_mode) == (0o700 if directory else 0o600), 'input custody metadata')
        value = dict(device=metadata.st_dev, inode=metadata.st_ino, uid=metadata.st_uid,
                     gid=metadata.st_gid, mode=stat.S_IMODE(metadata.st_mode))
        if not directory:
            require(metadata.st_nlink == 1 and (metadata.st_size == 0 if empty else 0 < metadata.st_size <= 65536),
                    'retained input size/link bound')
            with os.fdopen(os.dup(descriptor), 'rb') as opened:
                encoded = opened.read(65537)
            require(len(encoded) == metadata.st_size, 'retained input changed while reading')
            value.update(links=metadata.st_nlink, bytes=metadata.st_size,
                         mtime_ns=metadata.st_mtime_ns, sha256=hashlib.sha256(encoded).hexdigest())
        return value
    finally:
        os.close(descriptor)


def move_input(journal, *, restore=False):
    """Rename only this recorded private input; never overwrite a replacement."""
    root = Path(journal['directory'])
    require(input_identity(root, directory=True) == journal['directory_identity'], 'input directory changed')
    source, target = journal['original'], journal['withheld']
    if restore:
        source, target = target, source
    require(re.fullmatch(r'[0-9a-f]{32}\.inputs', journal['original'])
            and journal['withheld'] == journal['original'] + '.ordinary-readiness-withheld', 'input rename names')
    require(input_identity(root/source) == journal['before'], 'retained input changed')
    require(not os.path.lexists(root/target), 'input rename destination already exists')
    descriptor = os.open(root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        metadata = os.fstat(descriptor)
        require((metadata.st_dev,metadata.st_ino) ==
                (journal['directory_identity']['device'],journal['directory_identity']['inode']), 'input directory replaced')
        os.rename(source, target, src_dir_fd=descriptor, dst_dir_fd=descriptor)
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    require(not os.path.lexists(root/source) and input_identity(root/target) == journal['before'], 'input rename incomplete')
    journal['status'] = 'restored' if restore else 'withheld'
    journal['events'].append(journal['status'])
    if restore:
        journal['after'] = input_identity(root/target)


def content_conflict_sql(schema):
    """Closed fixture mutation; no schema supplied by the implementation."""
    columns, = [columns for name, columns, _ in schema.FAMILIES if name == 'events']
    names = [name for name, _ in schema.COMMON + columns]
    assert names.count('content') == 1 and "'" not in oracle.CONFLICTING_CONTENT
    fields = ','.join('`' + name + '`' for name in names)
    values = ','.join("'" + oracle.CONFLICTING_CONTENT + "'" if name == 'content'
                      else '`' + name + '`' for name in names)
    return f'INSERT INTO {DATABASE}.events ({fields}) SELECT {values} FROM {DATABASE}.events LIMIT 1'


def recipe(text, set_value):
    for section, key, value in [
        ('topology', 'local_roles', ['control', 'ingest', 'sync', 'archive', 'index']),
        ('indexer', 'enabled', True), ('indexer', 'batch_max_rows', oracle.BATCH_MAX_ROWS),
        ('indexer', 'insert_workers', 1), ('indexer', 'query_memory', '256MiB'),
        ('clickhouse', 'placement', 'local'), ('clickhouse', 'endpoint', f'http://127.0.0.1:{HTTP_PORT}'),
        ('clickhouse', 'database', DATABASE), ('clickhouse', 'write_user', 'writer'),
        ('clickhouse', 'write_password_ref', 'file:/run/secrets/index-writer'),
        ('clickhouse', 'read_user', 'reader'), ('clickhouse', 'read_password_ref', 'file:/run/secrets/index-reader'),
        ('clickhouse', 'initialize_user', 'initializer'),
        ('clickhouse', 'initialize_password_ref', 'file:/run/index-initialize/password'),
        ('clickhouse.tls', 'mode', 'local-plaintext'), ('clickhouse.tls', 'local_boundary', 'loopback'),
        ('runtime.processes.clickhouse', 'enabled', True),
        ('runtime.processes.clickhouse', 'memory_max_bytes', '4GiB'),
        ('runtime.processes.clickhouse', 'baseline_bytes', '1GiB'),
        ('runtime.processes.clickhouse', 'payload_bytes', '512MiB'),
        ('runtime.processes.clickhouse', 'native_bytes', '1GiB'),
        ('runtime.processes.clickhouse', 'completion_reserve_bytes', '512MiB'),
    ]:
        text = set_value(text, section, key, value)
    return text


def check_clickhouse(info, entry, runtime):
    require(info['Name'] == '/' + entry['name'] and re.fullmatch('[0-9a-f]{64}', info['Id'])
            and entry.get('id') in (None, info['Id']), 'ClickHouse container identity')
    labels = info['Config']['Labels'] or {}
    require(labels.get(LABEL) == runtime.owner
            and labels.get('com.docker.compose.project') == runtime.project
            and labels.get('com.docker.compose.service') == 'clickhouse', 'ClickHouse ownership')
    require(info['Image'] == runtime.inputs['runtime_images']['clickhouse']['id'], 'ClickHouse image changed')
    expected = {'/var/lib/clickhouse': runtime.state/'clickhouse/data',
                '/var/log/clickhouse-server': runtime.state/'clickhouse/logs',
                '/run/ch-private': runtime.state/'clickhouse/private'}
    mounts = {m['Destination']: m for m in info['Mounts']}
    require({m['Destination'] for m in info['Mounts'] if m['Type'] == 'bind'} == set(expected)
            and all(mounts[k]['Source'] == str(v) for k, v in expected.items())
            and len(mounts) == len(info['Mounts'])
            and all(m['Type'] == 'bind' or (m['Type'] == 'tmpfs' and m['Destination'] == '/tmp') for m in info['Mounts'])
            and not mounts['/run/ch-private']['RW'], 'ClickHouse bind mounts')
    host = info['HostConfig']
    require(host['NetworkMode'] == 'host' and not host['PortBindings']
            and host['Memory'] == host['MemorySwap'] == 4 * 1024**3
            and host['NanoCpus'] == 2_000_000_000 and host['PidsLimit'] == 256
            and host['ReadonlyRootfs'] and info['Config']['User'] == f'{os.geteuid()}:{os.getegid()}',
            'ClickHouse resource profile')
    require(not host['Privileged'] and not host['CapAdd'] and host['CapDrop'] == ['ALL']
            and host['SecurityOpt'] == ['no-new-privileges']
            and host['Tmpfs'] == {'/tmp':'rw,nosuid,nodev,noexec,size=64m'}
            and host['RestartPolicy']['Name'] == 'no', 'ClickHouse isolation profile')
    return info


class IndexRuntime(Runtime):
    def preflight(self):
        super().preflight()
        if not re.fullmatch('/[a-zA-Z0-9/_.-]+', str(self.root)):
            raise MissingPrerequisite('Index C API comparison needs the documented SQL-safe evidence path')
        if shutil.disk_usage(self.root).free <= 67 * 1024**3:
            raise MissingPrerequisite('Index case needs more than 67 GiB free: existing completion reserve plus 2 GiB ClickHouse allowance')
        for port in (HTTP_PORT, TCP_PORT):
            try:
                with socket.socket() as listener:
                    listener.bind(('127.0.0.1', port))
            except OSError as error:
                raise MissingPrerequisite(f'ClickHouse loopback port {port} unavailable') from error

    def candidate(self):
        for path in (Path(__file__), Path(oracle.__file__)):
            require(digest(path) == digest(self.source/'tests/acceptance/cases'/path.name), 'Index acceptance source mismatch')
        super().candidate()

    def load_helpers(self):
        super().load_helpers()
        from cases import first_launch_restart
        from cases import first_launch_index_baseline
        self.baseline = first_launch_index_baseline
        for module in (first_launch_index_baseline,sys.modules[first_launch_index_baseline.ShortPinRuntime.__module__]):
            path = Path(module.__file__)
            require(digest(path) == digest(self.source/'tests/acceptance/cases'/path.name), 'ordinary baseline helper source mismatch')
        from schema import ROWS_SQL
        import installed_archive_read
        import installed_progress_context_oracle
        self.science_checks = first_launch_restart
        # Keep the maintained complete inventory, with one snapshot while the
        # independent Index actor may publish between observer table reads.
        marker = 'SELECT pg_temp.capture();'
        require(ROWS_SQL.count(marker) == 1, 'Control observer statement changed')
        snapshot_sql = ROWS_SQL.replace(marker,
            'BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY; SELECT pg_temp.capture(); COMMIT;')
        self.rows = lambda pg,database: json.loads(self.lab.sql(pg,database,snapshot_sql))
        path = self.source/'experiments/clickhouse-frontier/initial_schema.py'
        spec = importlib.util.spec_from_file_location('installed_index_schema', path)
        self.schema = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = self.schema
        spec.loader.exec_module(self.schema)
        self.schema.inventory()  # Existing independent pinned schema vector.
        original = self.current_owner_view
        def source_view(state, setup):
            return original(oracle.without_index_owner(state, self.state/'data/index'), setup)
        self.current_owner_view = source_view

    def prepare_state(self):
        super().prepare_state()
        for name in ('clickhouse', 'clickhouse/data', 'clickhouse/logs', 'clickhouse/private', 'secrets/index-initialize'):
            (self.state/name).mkdir(mode=0o700)
        private = self.state/'clickhouse/private'
        passwords = {role: secrets.token_hex(32) for role in ROLES}
        for role, password in passwords.items():
            (private/(role+'.xml')).write_text(f'<config><host>127.0.0.1</host><port>{TCP_PORT}</port>'
                f'<user>{role}</user><password>{password}</password></config>\n')
        (self.state/'secrets/runtime/index-writer').write_text(passwords['writer'])
        (self.state/'secrets/runtime/index-reader').write_text(passwords['reader'])
        (self.state/'secrets/index-initialize/password').write_text(passwords['initializer'])
        (private/'provision.sql').write_text('\n'.join(
            f"CREATE USER {role} IDENTIFIED WITH sha256_password BY '{passwords[role]}' HOST LOCAL"
            + (' SETTINGS max_concurrent_queries_for_user=1 MIN 1 MAX 1' if role == 'writer' else '') + ';'
            for role in ROLES if role != 'provisioner'))
        encoded = hashlib.sha256(passwords['provisioner'].encode()).hexdigest()
        (private/'users.xml').write_text('<clickhouse><profiles><default><max_memory_usage>268435456</max_memory_usage>'
            '<max_threads>2</max_threads><log_queries>0</log_queries></default></profiles><users><provisioner>'
            f'<password_sha256_hex>{encoded}</password_sha256_hex><networks><ip>127.0.0.1</ip><ip>::1</ip></networks>'
            '<profile>default</profile><quota>default</quota><access_management>1</access_management>'
            '</provisioner></users><quotas><default><interval><duration>3600</duration><queries>0</queries>'
            '<errors>0</errors><result_rows>0</result_rows><read_rows>0</read_rows><execution_time>0</execution_time>'
            '</interval></default></quotas></clickhouse>\n')
        (private/'server.xml').write_text(f'''<clickhouse>
<logger><level>warning</level><log>/var/log/clickhouse-server/server.log</log><errorlog>/var/log/clickhouse-server/error.log</errorlog><size>10M</size><count>2</count><console>true</console></logger>
<path>/var/lib/clickhouse/</path><tmp_path>/var/lib/clickhouse/tmp/</tmp_path><user_files_path>/var/lib/clickhouse/user_files/</user_files_path><format_schema_path>/var/lib/clickhouse/format_schemas/</format_schema_path>
<users_config>/run/ch-private/users.xml</users_config><access_control_path>/var/lib/clickhouse/access/</access_control_path><default_profile>default</default_profile><default_database>default</default_database><timezone>UTC</timezone>
<listen_host>127.0.0.1</listen_host><tcp_port>{TCP_PORT}</tcp_port><http_port>{HTTP_PORT}</http_port><max_connections>16</max_connections><max_concurrent_queries>16</max_concurrent_queries><max_server_memory_usage>3221225472</max_server_memory_usage>
<max_thread_pool_size>128</max_thread_pool_size><background_schedule_pool_size>16</background_schedule_pool_size><background_pool_size>16</background_pool_size><background_move_pool_size>2</background_move_pool_size><background_fetches_pool_size>2</background_fetches_pool_size><background_common_pool_size>2</background_common_pool_size><background_buffer_flush_schedule_pool_size>2</background_buffer_flush_schedule_pool_size><background_message_broker_schedule_pool_size>2</background_message_broker_schedule_pool_size><background_distributed_schedule_pool_size>2</background_distributed_schedule_pool_size>
<mark_cache_size>67108864</mark_cache_size><uncompressed_cache_size>16777216</uncompressed_cache_size>
</clickhouse>\n''')
        self.config.write_text(recipe(self.config.read_text(), self.set_value))
        self.inputs['initial_recipe_sha256'] = digest(self.config)

    def image_and_state(self):
        result = self.docker('image', 'inspect', self.schema.IMAGE, check=False)
        if result.returncode:
            raise MissingPrerequisite('pinned ClickHouse 26.8.35.10 image must be present locally')
        info, = json.loads(result.stdout)
        require(info['Architecture'] == 'amd64' and info['Os'] == 'linux'
                and 'clickhouse/clickhouse-server@'+self.schema.IMAGE.split('@')[1] in info['RepoDigests'], 'ClickHouse image platform/digest')
        super().image_and_state()
        self.inputs['runtime_images']['clickhouse'] = dict(reference=self.schema.IMAGE, id=info['Id'])
        self.resources['clickhouse_profile'] = dict(image_id=info['Id'], http_port=HTTP_PORT, tcp_port=TCP_PORT,
            memory_bytes=4*1024**3, cpus=2, pids=256, observed_disk_limit_bytes=2*1024**3,
            completion_free_bytes=65*1024**3, private=str(self.state/'clickhouse/private'))
        self.record_resources()
        override = self.root/'compose-index.json'
        save(override, {'services': {'index-initialize': {'extends': {
            'file':str(self.bundle/'packaging/compose/compose.yaml'), 'service':'app'}, 'volumes': [
            {'type':'bind', 'source':str(self.state/'secrets/index-initialize'), 'target':'/run/index-initialize',
             'read_only':True, 'bind':{'create_host_path':False}}]}}})
        self.compose += ['-f', str(override)]
        self.command([*self.compose, '--profile', 'operator', 'config', '--quiet'])

    def inspect(self, entry):
        if entry['service'] != 'clickhouse':
            return super().inspect(entry)
        result = self.docker('inspect', entry.get('id') or entry['name'])
        info, = json.loads(result.stdout)
        check_clickhouse(info, entry, self)
        entry.update(id=info['Id'], status=info['State']['Status'])
        self.record_resources()
        return info

    def ch(self, sql, role='reader', *, rows=False, error=None):
        self.inspect(self.ch_entry)
        used = sum(p.stat().st_size for folder in ('data', 'logs')
                   for p in (self.state/'clickhouse'/folder).rglob('*') if p.is_file())
        free = shutil.disk_usage(self.root).free
        require(used < 2 * 1024**3 and free > 65 * 1024**3, 'ClickHouse disk/completion allowance')
        self.resources['clickhouse_disk'] = dict(bytes=used, free_bytes=free, limit_bytes=2 * 1024**3)
        self.record_resources()
        require(role in ROLES, 'unknown ClickHouse observer role')
        result = self.docker('exec', '-i', self.ch_entry['id'], 'clickhouse-client',
            '--config-file', '/run/ch-private/'+role+'.xml', '--output_format_json_quote_64bit_integers=0',
            '--max_result_rows=175', '--max_result_bytes=1048576', '--result_overflow_mode=throw',
            '--max_execution_time=10', '--receive_timeout=15',
            '--multiquery', data=sql + (' FORMAT JSONEachRow' if rows else ''), check=False,
            label='clickhouse-'+role)
        if error:
            require(result.returncode != 0 and error in result.stderr, 'expected ClickHouse refusal')
            return
        require(result.returncode == 0, 'ClickHouse observer failed')
        return [json.loads(line) for line in result.stdout.splitlines()] if rows else result.stdout.strip()

    def bootstrap(self):
        super().bootstrap()
        entry = dict(name=self.project+'-clickhouse', service='clickhouse', id=None, status='creating')
        require(self.docker('inspect', entry['name'], check=False).returncode != 0, 'ClickHouse name already exists')
        self.resources['containers'].append(entry); self.record_resources()
        result = self.docker('run', '-d', '--name', entry['name'], '--network', 'host', '--pull', 'never',
            '--memory', '4g', '--memory-swap', '4g', '--cpus', '2', '--pids-limit', '256',
            '--user', f'{os.geteuid()}:{os.getegid()}', '--cap-drop', 'ALL', '--security-opt', 'no-new-privileges',
            '--read-only', '--tmpfs', '/tmp:rw,nosuid,nodev,noexec,size=64m', '--ulimit', 'nofile=65536:65536',
            '--log-opt', 'max-size=10m', '--log-opt', 'max-file=2',
            '--label', LABEL+'='+self.owner, '--label', 'com.docker.compose.project='+self.project,
            '--label', 'com.docker.compose.service=clickhouse',
            '--mount', f'type=bind,source={self.state}/clickhouse/data,target=/var/lib/clickhouse',
            '--mount', f'type=bind,source={self.state}/clickhouse/logs,target=/var/log/clickhouse-server',
            '--mount', f'type=bind,source={self.state}/clickhouse/private,target=/run/ch-private,readonly',
            '--entrypoint', '/usr/bin/clickhouse-server', self.inputs['runtime_images']['clickhouse']['id'],
            '--config-file=/run/ch-private/server.xml')
        require(re.fullmatch('[0-9a-f]{64}', result.stdout.strip()), 'ClickHouse launch ID absent')
        entry['id'] = result.stdout.strip(); self.ch_entry = entry; self.inspect(entry)
        self.wait(lambda: self.docker('exec', entry['id'], 'clickhouse-client', '--config-file',
            '/run/ch-private/provisioner.xml', '--query', 'SELECT 1', check=False).returncode == 0,
            'ClickHouse startup', 45, entry=entry)
        require(self.ch('SELECT version()', 'provisioner') == self.schema.VERSION, 'ClickHouse version')
        self.ch((self.state/'clickhouse/private/provision.sql').read_text(), 'provisioner')
        self.ch(f'CREATE DATABASE {DATABASE} ENGINE=Atomic', 'provisioner')
        # The initializer verifies empty tables after creating them. It needs
        # SELECT for that actual guard, but neither INSERT nor general DDL.
        for role, rights in [('writer', 'SELECT, INSERT'), ('reader', 'SELECT'), ('initializer', 'SELECT, CREATE TABLE')]:
            self.ch(f'GRANT {rights} ON {DATABASE}.* TO {role}', 'provisioner')
            for table in SYSTEM_TABLES:
                self.ch(f'GRANT SELECT ON system.{table} TO {role}', 'provisioner')
        require(self.ch(f"SELECT count() FROM system.tables WHERE database='{DATABASE}'") == '0', 'fresh Index database not empty')
        grants = {role:self.ch('SHOW GRANTS FOR '+role, 'provisioner') for role in ROLES[1:]}
        save(self.root/'bootstrap/clickhouse-grants.json', grants)
        self.ch(f'CREATE TABLE {DATABASE}.forbidden (x UInt8) ENGINE=Memory', 'writer', error='ACCESS_DENIED')
        for value in (0, 2):
            self.ch(f'SELECT 1 SETTINGS max_concurrent_queries_for_user={value}', 'writer', error='SETTING_CONSTRAINT_VIOLATION')

    def initial_launch(self):
        before, _ = self.capture(self.root/'initial', 'index-policy')
        oracle.accepted_profile(before)
        return self.launch('initial-index', 'index-initialize', 'serve', '--file', '/etc/bigbrotr/instance.toml',
            '--roles', 'control,ingest,sync,archive,index', '--initialize-ingest', '--initialize-index')

    def archive_published(self):
        return self.sql('bigbrotr_control', f"SELECT count(*) FROM control.consumer_frontiers f JOIN control.consumers c USING(consumer_id) WHERE c.consumer_role='archive' AND c.consumer_id=decode('{self.ids['catalog']}','hex') AND f.frontier_revision=c.current_revision AND f.last_complete_sequence=12") == '1'

    def initial_publication(self, entry, output):
        self.wait(lambda: self.sql('bigbrotr_control', "SELECT count(*) FROM control.consumer_frontiers f JOIN control.consumers c USING(consumer_id) WHERE c.consumer_role='index' AND f.frontier_revision=c.current_revision AND f.last_complete_sequence=12") == '1',
            'live Index complete source frontier', 45, entry, self.client)
        self.index_live, _ = self.capture(output, 'index-live')
        oracle.validate_metadata(self.index_live, self.state/'data/index', complete=True)
        self.index_rows = {name:self.ch(oracle.select_rows(self.schema, DATABASE, name), rows=True)
                           for name, _, _ in self.schema.FAMILIES}
        save(output/'clickhouse-live.json', self.index_rows)
        tables = self.ch(f"SELECT name,toString(uuid) AS uuid,engine_full FROM system.tables WHERE database='{DATABASE}' ORDER BY name", rows=True)
        columns = self.ch(f"SELECT table,name,type,position,default_kind,default_expression FROM system.columns WHERE database='{DATABASE}' ORDER BY table,position", rows=True)
        oracle.schema_metadata(self.schema,self.state/'data/index',tables,columns)
        save(output/'clickhouse-schema.json',dict(tables=tables,columns=columns))
        require(self.inspect(entry)['State']['Running'] and self.archive_published(), 'combined owner/Archive stopped during Index publication')
        self.ch(f'ALTER TABLE {DATABASE}.events ADD COLUMN forbidden UInt8', 'writer', error='ACCESS_DENIED')
        self.live_roles(entry, output)

    def live_roles(self, entry, output):
        """Use the existing private session; never print its cookie or CSRF."""
        saved = load(self.home/'session.private')
        require(saved['format'] == 'bigbrotr-cli-session/1'
                and saved['origin'] == 'http://127.0.0.1:18081', 'role client session binding')
        before, _ = self.capture(output, 'roles-before')
        clock_before = int(self.sql('bigbrotr_control',
            'SELECT (EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint'))
        observations = []
        last = 0
        def observed(invalidated=False):
            nonlocal last
            if time.monotonic() - last < .25:
                return False
            last = time.monotonic()
            connection = http.client.HTTPConnection('127.0.0.1', 18081, timeout=3)
            try:
                connection.request('GET', '/management/v1/roles', headers={
                    'Origin': saved['origin'], 'Cookie': 'BigBrotrLocalSession='+saved['session'],
                    'X-CSRF-Token': saved['csrf']})
                response = connection.getresponse()
                raw = response.read(131073)
                require(len(raw) <= 131072 and response.status == 200, 'authenticated role observation failed')
                value = json.loads(raw)
            finally:
                connection.close()
            require(len(observations) < 256, 'role observation capture bound')
            observations.append(value)
            save(output/'role-observations.json', observations)
            require(value['format'] == 'bigbrotr-role-status/1' and value['authority'] == 'observation-only',
                    'role observation authority/format')
            caps = value['capabilities']
            return (len(caps) == 4 and all(c['state'] == 'ready' for c in caps[:3])
                    and caps[-1]['state'] == ('unavailable' if invalidated else 'ready'))
        # The existing signed-client65s gate still bounds this whole operation.
        # Slow or absent physical verification is a failure, not a timeout uplift.
        self.wait(observed, 'fresh physical Index and Archive role observations', 45, entry, self.client)
        clock_after = int(self.sql('bigbrotr_control',
            'SELECT (EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint'))
        save(output/'role-clock.json', dict(before_us=str(clock_before),after_us=str(clock_after)))
        after, _ = self.capture(output, 'roles-after')
        require(before['browser_sessions'] == after['browser_sessions'], 'status observation refreshed or mutated session')
        oracle.live_roles(observations[-1], after, clock_before, clock_after)
        save(output/'role-verdict.json', dict(status='PASS', observations=len(observations),
            source_through=12, archive_revision=4, index_revision=4,
            session_idle='unchanged', negative_readiness='NOT_RUN', ordinary_readiness='NOT_RUN'))

        # Retain the successful baseline before the fault, even if any following
        # step fails. Exact duplicates are harmless to DISTINCT readback, so add
        # one conflicting typed value at the same source/discriminator instead.
        # The extra row rejects before cursor EOF; the remote slot must remain
        # uncertain even after its local executor joins.
        mutation = dict(kind='append_conflicting_event_content', original_capture='clickhouse-live.json',
            positive_observation=len(observations)-1, verification='not_observed')
        save(output/'role-invalidation.json', mutation)
        self.ch(content_conflict_sql(self.schema), 'provisioner')
        corrupted = {name:self.ch(oracle.select_rows(self.schema, DATABASE, name), rows=True)
                     for name, _, _ in self.schema.FAMILIES}
        save(output/'clickhouse-corrupted.json', corrupted)
        oracle.content_conflict(self.index_rows, corrupted)
        clock_invalidated = int(self.sql('bigbrotr_control',
            'SELECT (EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint'))
        save(output/'role-invalidation.json', dict(mutation, verification='independently_observed',
            captured_at_us=str(clock_invalidated), original_events=1, conflicting_events=1))
        # Index's coalesced full verification is due every15s. This wait neither
        # resets nor extends the signed client's existing65s submitted barrier.
        self.wait(lambda: observed(True), 'Index unavailable after conflicting physical row',
                  30, entry, self.client)
        clock_failed = int(self.sql('bigbrotr_control',
            'SELECT (EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint'))
        save(output/'role-invalidated-clock.json', dict(before_us=str(clock_invalidated),after_us=str(clock_failed)))
        failed, _ = self.capture(output, 'roles-invalidated')
        oracle.unchanged_publication(after, failed)
        oracle.live_roles(observations[-1], failed, clock_invalidated, clock_failed, invalidated=True)
        require(self.inspect(entry)['State']['Running'] and self.archive_published(),
                'combined owner/Archive stopped after physical invalidation')
        save(output/'role-invalidation-verdict.json', dict(status='PASS', observations=len(observations),
            index='unavailable', reason='remote_outcome_unknown', control='ready', authenticated_http=200,
            source_through=12, archive_revision=4, index_revision=4, original_events=1, conflicting_events=1,
            publication='unchanged', session_idle='unchanged', physical_fault='retained', ordinary_readiness='NOT_RUN'))

    def verify_index(self):
        output = self.root/'index'; output.mkdir(mode=0o700)
        after, _ = self.capture(output, 'after')
        oracle.validate_metadata(after, self.state/'data/index', complete=True)
        before = load(self.root/'initial/before-control.json')
        view = self.current_owner_view(after, dict(control=before, resource=dict(catalog_id=self.ids['catalog'])))
        owner, = [r for r in view['owner_epochs'] if r['role'] == 'control']
        census = self.census(output, 'published')
        wires = [load(path) for path in sorted(self.home.glob('relay-closed-*.json'))]
        science = self.science_checks.first_science(view, self.initialized, load(self.home/'event.jsonl'),
            wires, census, self.state/'data/ingest', owner['released_at_us'])
        self.index_science = science
        result = oracle.publication(self.index_live, after, self.index_rows, science, self.schema, self.state/'data/index')
        # publication() still checks the original typed bytes against the signed
        # source. Its positive census precedes the separately retained fault.
        result.update(physical_rows_scope='before_intentional_content_conflict',
            readiness='positive_then_physical_invalidation', physical_fault='retained')
        final = [json.loads(line) for line in (self.root/'initial/serve.stdout').read_text().splitlines()][-1]
        require(final['index']['published_revision'] == '4'
                and final['index']['complete_through'] == [dict(shard_id=0, owner_epoch='1', sequence='12')]
                and final['index']['readiness'] == 'not-qualified', 'Index terminal diagnostic differs')
        save(output/'verdict.json', result)
        self.inputs['index'] = result

    def baseline_client(self, output, entry):
        self.baseline.ordinary_client(self,output,entry)

    def startup_quiescence(self, output, entry, historical, deadline):
        return self.baseline.startup_quiescence(self,output,entry,historical,deadline)

    def ordinary_profile(self):
        from installed_archive_export_client import (READINESS_MODE, READINESS_PHASES, PHASE_SECONDS, CLIENT_SECONDS)
        return READINESS_MODE, READINESS_PHASES, PHASE_SECONDS, CLIENT_SECONDS

    def ordinary_special_phase(self, phase, signal, deadline, output, entry, client):
        return False

    def ordinary_verdict(self, completed, timing, journal):
        oracle.ordinary_complete(completed, timing, journal)
        return dict(status='PASS',phases=completed, source='unknown',control='ready',
            archive_revision=4,cut=12,snapshot=5,restoration='exact',
            original_export_gate='pending existing independent signed export oracle')

    def ordinary_client(self, output, entry):
        READINESS_MODE, READINESS_PHASES, PHASE_SECONDS, CLIENT_SECONDS = self.ordinary_profile()
        selected = load(self.home/'export-input.json')
        save(self.home/'export-input.json', dict(selected, readiness_mode=READINESS_MODE))
        command = [sys.executable, '-B', str(self.source/'experiments/ingest-bootstrap/installed_archive_export_client.py'), str(self.home)]
        observations, completed, journal = [], [], None
        overall = time.monotonic() + CLIENT_SECONDS
        with (output/'ordinary-client.stdout').open('w') as stdout, (output/'ordinary-client.stderr').open('w') as stderr:
            client = subprocess.Popen(command, env=self.environment, cwd=self.root, stdin=subprocess.DEVNULL,
                stdout=stdout, stderr=stderr, start_new_session=True)
            self.processes.append(client)
            save(output/'ordinary-client-process.json', dict(argv=command,pid=client.pid,status='running',timeout_seconds=CLIENT_SECONDS))
            try:
                for phase in READINESS_PHASES:
                    marker = self.home/('ordinary-'+phase+'.json')
                    self.wait(lambda: marker.exists(), 'ordinary '+phase+' barrier',
                              max(.001,overall-time.monotonic()), entry, client)
                    signal = load(marker)
                    require(signal['phase'] == phase and type(signal['started_ns']) is int
                            and signal['deadline_ns'] - signal['started_ns'] == PHASE_SECONDS*1_000_000_000,
                            'ordinary phase deadline declaration')
                    deadline = min(overall, signal['deadline_ns']/1_000_000_000)
                    require(signal['started_ns']/1_000_000_000 <= time.monotonic() < deadline, 'ordinary phase deadline')
                    if self.ordinary_special_phase(phase, signal, deadline, output, entry, client):
                        require(time.monotonic() < deadline, 'ordinary special phase exceeded deadline')
                        completed.append(phase)
                        (self.home/('ordinary-'+phase+'.continue')).write_text('independent observation checked\n')
                        continue
                    if phase == 'restored':
                        self.inspect(entry)
                        move_input(journal, restore=True)
                        save(output/'ordinary-input-journal.json', journal)
                    before, _ = self.capture(output, 'ordinary-'+phase+'-before')
                    clock_before = int(self.sql('bigbrotr_control', 'SELECT (EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint'))
                    if phase == 'missing':
                        self.inspect(entry)  # Exact recorded app, labels and mounted data root.
                        directory = self.state/'data/archive-state'
                        identity = input_identity(directory, directory=True)
                        initialization = bytes.fromhex(digest(self.state/'data/archive/.bigbrotr-initialization/intent.json'))
                        row, expected = oracle.ordinary_input(before, self.index_science, initialization)
                        binding = directory/'coordinator.json'
                        require(input_identity(binding)['sha256'] == oracle.raw(row['coordinator_binding_sha256'],32).hex(), 'coordinator binding differs')
                        oracle.exact(load(binding), dict(format='bigbrotr-local-coordinator/1', initialization_sha256=list(initialization),
                            state_root='/var/lib/bigbrotr/archive-state', state_device=identity['device'], state_inode=identity['inode']))
                        name = oracle.raw(row['attempt_id'],16).hex()+'.inputs'
                        original = input_identity(directory/name)
                        require(original['sha256'] == hashlib.sha256(expected).hexdigest()
                                and original['bytes'] == len(expected), 'independent retained input bytes differ')
                        journal = dict(owner=self.owner,container_id=entry['id'],directory=str(directory),
                            directory_identity=identity,original=name,withheld=name+'.ordinary-readiness-withheld',
                            before=original,after=None,status='prepared',events=['prepared'])
                        save(output/'ordinary-input-journal.json', journal)
                        move_input(journal)
                        save(output/'ordinary-input-journal.json', journal)
                    if phase == 'narrow':
                        # This response was captured before the parent's SQL;
                        # do not poll until a subsequent probe hides a false Ready.
                        require(len(observations) < 256, 'ordinary observation bound')
                        observations.append(dict(phase='narrow-immediate',value=signal['payload']))
                        save(output/'ordinary-role-observations.json',observations)
                        oracle.ordinary_roles(signal['payload'], before, failure_clock,
                                              clock_before, phase='narrow')
                    last_poll = 0
                    def observed():
                        nonlocal last_poll
                        if time.monotonic() - last_poll < .75:
                            return False
                        last_poll = time.monotonic()
                        saved = load(self.home/'export-session.private')
                        require(saved['format'] == 'bigbrotr-cli-session/1' and saved['origin'] == 'http://127.0.0.1:18081', 'ordinary session binding')
                        connection = http.client.HTTPConnection('127.0.0.1',18081,timeout=min(3,max(.001,deadline-time.monotonic())))
                        try:
                            connection.request('GET','/management/v1/roles',headers=dict(Origin=saved['origin'],
                                Cookie='BigBrotrLocalSession='+saved['session'], **{'X-CSRF-Token':saved['csrf']}))
                            response = connection.getresponse(); raw = response.read(131073)
                            if response.status != 200 or len(raw) > 131072:
                                save(output/('ordinary-'+phase+'-http-failure.json'),dict(status=response.status,
                                    retained_body_hex=raw.hex(),body_limit_bytes=131072))
                            require(response.status == 200 and len(raw) <= 131072, 'ordinary authenticated role observation failed')
                            value = json.loads(raw)
                        finally:
                            connection.close()
                        require(len(observations) < 256 and time.monotonic() < deadline, 'ordinary observation bound')
                        observations.append(dict(phase=phase,value=value))
                        save(output/'ordinary-role-observations.json', observations)
                        # SQL brackets are finalized below; identity/source/cut
                        # checks also apply to every intermediate response.
                        oracle.ordinary_roles(value,before,clock_before,(1 << 63)-1)
                        caps = value['capabilities']
                        if phase == 'setup':
                            return all(c['reason'] == 'not_observed' for c in caps)
                        if phase in ('missing','narrow'):
                            return all(c['state'] == 'unavailable' for c in caps) if phase == 'missing' else True
                        return all(c['state'] == 'ready' for c in caps) if phase == 'positive' else caps[0]['state'] == 'ready'
                    self.wait(observed, 'ordinary '+phase+' roles', max(.001,deadline-time.monotonic()), entry, client)
                    clock_after = int(self.sql('bigbrotr_control', 'SELECT (EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint'))
                    after, _ = self.capture(output, 'ordinary-'+phase+'-after')
                    oracle.ordinary_unchanged(before, after)
                    oracle.ordinary_roles(observations[-1]['value'],after,clock_before,clock_after,phase)
                    require(time.monotonic() < deadline, 'ordinary phase capture exceeded deadline')
                    save(output/('ordinary-'+phase+'-verdict.json'), dict(status='PASS',phase=phase,
                        clock_before_us=str(clock_before),clock_after_us=str(clock_after),session_idle='unchanged'))
                    if phase == 'missing':
                        failure_clock = clock_before
                    completed.append(phase)
                    (self.home/('ordinary-'+phase+'.continue')).write_text('independent observation checked\n')
                client.wait(timeout=max(.001,overall-time.monotonic()))
                require(client.returncode == 0 and time.monotonic() < overall, 'ordinary client failed or timed out')
                require(not (output/'ordinary-client.stderr').read_bytes(), 'ordinary client diagnostic output')
                save(output/'ordinary-readiness-verdict.json',
                    self.ordinary_verdict(completed,load(self.home/'ordinary-timing.json'),journal))
            except BaseException as error:
                save(output/'ordinary-readiness-failure.json', dict(error_type=type(error).__name__,reason=str(error),completed=completed))
                raise
            finally:
                # Restore even after client/status/capture failure, but never
                # overwrite a replacement or discard the original failure.
                pending_error = sys.exc_info()[0] is not None
                try:
                    if journal and os.path.lexists(Path(journal['directory'])/journal['withheld']):
                        self.inspect(entry)
                        move_input(journal, restore=True)
                        save(output/'ordinary-input-journal.json',journal)
                    elif journal:
                        require(input_identity(Path(journal['directory'])/journal['original']) == journal['before'],
                                'original retained input not restored')
                except Exception as error:
                    self.resources['cleanup_errors'].append(dict(resource='ordinary-retained-input',error=type(error).__name__))
                    save(output/'ordinary-input-restore-failure.json',dict(error_type=type(error).__name__,reason=str(error)))
                    if not pending_error:
                        raise
                finally:
                    stop_process(client)
                    self.processes.remove(client)
                    save(output/'ordinary-client-process.json',dict(argv=command,pid=client.pid,status='joined',
                        exit_code=client.returncode,timeout_seconds=CLIENT_SECONDS))

    def cleanup(self):
        # Preserve the base ownership/reaping checks; stop every application
        # before ClickHouse, then PostgreSQL. Ambiguous creations remain errors.
        self.resources['containers'].sort(key=lambda e:e['service'] in ('clickhouse','database'))
        super().cleanup()
