"""One fresh installed Index lifetime alongside the existing Archive journey.

No engine is started on import. Credentials and raw captures stay in the same
protected, retained first-launch resource owner; no restart/readiness claim.
"""
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import secrets
import shutil
import socket
import sys

from cases.first_launch import (Runtime, MissingPrerequisite, LABEL, digest, load,
                               require, save)
from cases import first_launch_index_oracle as oracle

STAGES = ('prerequisites', 'candidate', 'runtime_image', 'fresh_bootstrap',
          'signed_acquisition', 'index_publication', 'ordinary_history_export', 'cleanup')
LIMITS = [
    'Fresh installed signed source publishes all ten Index targets and PG proof while Archive advances; ordinary Archive history/export remains exact.',
    'NOT_RUN: direct RocksDB/native census, Index restart, remote uncertainty resolution, column-TTL negative, Index query/readiness, outage/capacity and complete M1.',
    'The signed fixture has empty tags: event_tag_index has an explicit zero outcome; nonempty tag projection retains its separate fixture qualification.',
    'Configured process/disk limits are not empirical capacity qualification; local candidate identity is not release authentication.',
]
HTTP_PORT, TCP_PORT = 18123, 19000
DATABASE = 'serving_fixture'
ROLES = ('provisioner', 'initializer', 'writer', 'reader')
SYSTEM_TABLES = ('databases', 'disks', 'storage_policies', 'settings', 'tables', 'columns', 'projections')


def recipe(text, set_value):
    for section, key, value in [
        ('topology', 'local_roles', ['control', 'ingest', 'sync', 'archive', 'index']),
        ('indexer', 'enabled', True), ('indexer', 'batch_max_rows', 100),
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
        result = oracle.publication(self.index_live, after, self.index_rows, science, self.schema, self.state/'data/index')
        final = [json.loads(line) for line in (self.root/'initial/serve.stdout').read_text().splitlines()][-1]
        require(final['index']['published_revision'] == '4'
                and final['index']['complete_through'] == [dict(shard_id=0, owner_epoch='1', sequence='12')]
                and final['index']['readiness'] == 'not-qualified', 'Index terminal diagnostic differs')
        save(output/'verdict.json', result)
        self.inputs['index'] = result

    def cleanup(self):
        # Preserve the base ownership/reaping checks; stop every application
        # before ClickHouse, then PostgreSQL. Ambiguous creations remain errors.
        self.resources['containers'].sort(key=lambda e:e['service'] in ('clickhouse','database'))
        super().cleanup()
