"""Known catalog-only refusal while the same Control/Sync/Index lifetime lives."""
import hashlib
import json
import os
from pathlib import Path
import socket
import sys
import time
import tomllib

from cases.first_launch import digest, load, require, save
from cases.first_launch_index import DATABASE, input_identity
from cases.first_launch_index_restart import RestartIndexRuntime
from cases.first_launch_index_pin import ShortPinRuntime, process_identity
from cases import first_launch_index_oracle as index
from cases import first_launch_archive_outage_oracle as oracle

STAGES = ('prerequisites', 'candidate', 'runtime_image', 'fresh_bootstrap',
          'signed_acquisition', 'index_publication', 'cleanup')
LIMITS = [
    'Known catalog-only connection refusal after the independently ready source3/Archive2/Index2 prefix. The unchanged signed peer then reaches source12/Index4 while Archive retains revision2/cut3 and issued attempt3.',
    'The existing15s prefix gate and65s signed-client gate remain. Physical Index readiness uses its existing45s/256-sample envelope at most once per250ms, including the intentional15s sweep cooldown. The catalog-only proxy uses the existing retained-catch-up120s/64MiB/131072-frame/64-connection profile and is positively joined before the next admission.',
    'Same Control process/lease, six classified EVENTs, exact original inputs/typed progress, ten-family Index rows and retained three-dictionary Archive prefix are checked independently.',
    'NOT_RUN: automatic Archive recovery, generic connection/auth/TLS failures, uncertain remote outcomes, native RocksDB census, shared-budget/disk-pressure capacity and complete M1.',
    'Diagnostic C API census uses the separately declared direct catalog reader after product shutdown; the running product never bypasses its unavailable catalog route. Signed history/export remains covered by separate existing journeys.',
]


def custody_lock(path, *, held, expected=None):
    """Observe the recorded flock without creating/replacing its private file."""
    import fcntl
    identity = input_identity(path, empty=True)
    if expected is not None:
        index.exact(identity, expected)
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    try:
        opened = os.fstat(descriptor)
        require((opened.st_dev, opened.st_ino) == (identity['device'], identity['inode']),
                'custody lock replaced while opening')
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            require(held, 'custody lock still held after product join')
        else:
            fcntl.flock(descriptor, fcntl.LOCK_UN)
            require(not held, 'parked custody lock was released early')
    finally:
        os.close(descriptor)
    return identity


class ArchiveOutageRuntime(RestartIndexRuntime):
    inject_content_conflict = False

    def candidate(self):
        for path in (Path(__file__), Path(oracle.__file__)):
            require(digest(path) == digest(self.source/'tests/acceptance/cases'/path.name),
                    'Archive outage acceptance source mismatch')
        super().candidate()

    def load_helpers(self):
        super().load_helpers()
        sys.path.insert(0, str(self.source/'experiments/archive-engine'))
        from fault_cases import Process
        from signed_publication_cases import event, send, proxy_observation
        self.proxy_process, self.proxy_event = Process, event
        self.proxy_send, self.proxy_observation = send, proxy_observation
        # Imported now so the base candidate records all reused helper bytes.
        import installed_archive_oracle
        import installed_progress_context_oracle

    def bootstrap(self):
        path = self.source/'experiments/archive-engine/native_commit_proxy.py'
        self.inputs['acceptance_files_sha256'][str(path.relative_to(self.source))] = digest(path)
        output = self.root/'catalog-route'; output.mkdir(mode=0o700)
        self.route_output = output
        args = [sys.executable, '-B', str(path), '--mode', 'pass', '--profile', 'retained-catch-up',
                '--upstream-port', '15432', '--catalog-manager', 'bigbrotr_catalog',
                '--catalog-reader', 'bigbrotr_archive_read', '--catalog-database', 'bigbrotr_catalog']
        self.route = self.proxy_process(args, input_pipe=True)
        self.route_identity = process_identity(self.route.process.pid)
        require(self.route_identity is not None, 'catalog route process identity')
        self.resources['catalog_route'] = dict(status='starting', process=self.route_identity,
                                               argv=args, source_sha256=digest(path))
        self.record_resources()
        ready = self.proxy_event(self.route, 'ready', 10); save(output/'ready.json', ready)
        require(ready['source_sha256'] == digest(path) and ready['mode'] == 'pass'
                and ready['profile'] == 'retained-catch-up' and ready['upstream_port'] == 15432
                and type(ready['port']) is int and 1 <= ready['port'] <= 65535, 'catalog route identity')
        self.route_port = ready['port']
        prior = tomllib.loads(self.config.read_text())
        self.config.write_text(self.set_value(self.config.read_text(), 'postgres.catalog', 'endpoint',
                                              f'127.0.0.1:{self.route_port}'))
        current = tomllib.loads(self.config.read_text())
        expected = dict(prior['postgres']['catalog'], endpoint=f'127.0.0.1:{self.route_port}')
        index.exact(current['postgres']['catalog'], expected)
        index.exact(current['postgres']['control'], prior['postgres']['control'])
        require(current['postgres']['control']['endpoint'] == '127.0.0.1:15432', 'independent direct Control route')
        self.inputs['initial_recipe_sha256'] = digest(self.config)
        self.resources['catalog_route'].update(status='running', port=self.route_port)
        self.record_resources()
        super().bootstrap()

    def stop_route(self):
        if not hasattr(self, 'route') or self.resources['catalog_route']['status'] == 'joined':
            return
        require(process_identity(self.route.process.pid) == self.route_identity, 'catalog route PID/start changed')
        self.proxy_send(self.route, 'stop\n')
        require(self.route.wait(5) == 0, 'catalog route did not join successfully')
        observed = self.proxy_observation(self.route)
        save(self.route_output/'terminal.json', observed)
        require(not observed['stderr'] and not observed['overflow'], 'catalog route output incomplete')
        events = [json.loads(line) for line in observed['stdout'].splitlines()]
        terminal, = [row for row in events if row['event'] == 'terminal']
        require(terminal['unexpected'] == terminal['faults'] == 0, 'catalog route had an earlier unknown fault')
        require(process_identity(self.route_identity['pid']) is None, 'catalog route process remains')
        # A successful connection here invalidates the intended refusal. This is
        # an observation, not a replacement listener or a product retry.
        with socket.socket() as connection:
            connection.settimeout(1)
            result = connection.connect_ex(('127.0.0.1', self.route_port))
        import errno
        require(result == errno.ECONNREFUSED, 'closed catalog endpoint is not a known refusal')
        self.resources['catalog_route'].update(status='joined', exit_code=0, refusal_errno=result)
        self.record_resources()

    def establish_prefix_fault(self, entry, output):
        self.parent = process_identity(self.inspect(entry)['State']['Pid'])
        require(self.parent is not None and ShortPinRuntime.worker(self, self.parent, allow_absent=True) is None,
                'prefix native worker has not joined')
        require(self.sql('bigbrotr_catalog',
            'SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid()') == '0',
            'prefix catalog transport remains active')
        self.prefix, self.prefix_catalog = self.capture(output, 'catalog-prefix')
        self.prefix_files = self.files(self.state/'data/archive')
        self.prefix_inputs = self.files(self.state/'data/ingest/producer-inputs-v1')
        save(output/'catalog-prefix-files.json', self.prefix_files)
        save(output/'catalog-prefix-inputs.json', self.prefix_inputs)
        before_us = int(self.sql('bigbrotr_control', 'SELECT (EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint'))
        self.prefix_timing = dict(before_ns=time.clock_gettime_ns(time.CLOCK_BOOTTIME))
        self.prefix_roles = ShortPinRuntime.roles(self, self.prefix_deadline, session_path=self.home/'session.private')
        self.prefix_timing['after_ns'] = time.clock_gettime_ns(time.CLOCK_BOOTTIME)
        after_us = int(self.sql('bigbrotr_control', 'SELECT (EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint'))
        index.live_roles(self.prefix_roles, self.prefix, before_us, after_us, revision=2, sequence=3)
        save(output/'catalog-prefix-age.json', dict(roles=self.prefix_roles, timing=self.prefix_timing))
        self.stop_route()
        return 'positive complete prefix checked; catalog-only route joined with known refusal'

    def initial_archive_observation(self, entry, output):
        def published():
            require(not (self.home/'relay-error.json').exists(), 'controlled relay failed')
            return self.sql('bigbrotr_control', "SELECT count(*) FROM control.consumer_frontiers f JOIN control.consumers c USING(consumer_id) WHERE c.consumer_role='index' AND f.frontier_revision=c.current_revision AND f.last_complete_sequence=12") == '1'
        self.wait(published, 'independent Index source12 while catalog is unavailable', 45, entry, self.client)
        self.wait(lambda: self.sql('bigbrotr_control',
            'SELECT count(*) FROM control.acquisition_progress WHERE producer_epoch=1 AND classified_events=6') == '1',
            'six durably classified source events', 15, entry, self.client)

    def initial_publication(self, entry, output):
        # Use the existing IndexRuntime.live_roles physical-readiness envelope.
        # Publication does not reset its intentional 15s sweep cooldown. The
        # signed client's original 65s barrier still bounds the whole operation.
        deadline = time.monotonic() + 45
        before_us = int(self.sql('bigbrotr_control', 'SELECT (EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint'))
        observations = []
        last = 0
        def parked():
            nonlocal last
            if time.monotonic() - last < .25:
                return False
            last = time.monotonic()
            timing = dict(before_ns=time.clock_gettime_ns(time.CLOCK_BOOTTIME))
            value = ShortPinRuntime.roles(self, deadline, session_path=self.home/'session.private')
            timing['after_ns'] = time.clock_gettime_ns(time.CLOCK_BOOTTIME)
            require(len(observations) < 256, 'Archive outage role observation bound')
            observations.append(value); save(output/'catalog-outage-roles.json', observations)
            self.parked_timing = timing
            caps = {row['capability']: row for row in value['capabilities']}
            return (caps.get('archive_publication', {}).get('reason') == 'dependency_unavailable'
                    and caps.get('index_publication', {}).get('state') == 'ready')
        self.wait(parked, 'truthful Archive unavailability and current Index',
                  deadline-time.monotonic(), entry, self.client)
        self.index_live, catalog = self.capture(output, 'catalog-outage')
        after_us = int(self.sql('bigbrotr_control', 'SELECT (EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint'))
        oracle.parked_roles(observations[-1], self.index_live, before_us, after_us)
        oracle.retained_age(self.prefix_roles, observations[-1], self.prefix_timing, self.parked_timing)
        save(output/'catalog-outage-age.json', dict(prefix=self.prefix_timing, parked=self.parked_timing))
        oracle.prefix_preservation(self.prefix, self.index_live)
        third = oracle.archived_prefix(self.prefix, self.index_live)
        index.exact(catalog, self.prefix_catalog)
        index.exact(self.files(self.state/'data/archive'), self.prefix_files)
        before_owner = index.only(self.prefix['owner_epochs'], role='control', state='active')
        current = index.only(self.index_live['owner_epochs'], role='control', state='active')
        require(all(current[k] == before_owner[k] for k in ('resource_id', 'epoch', 'process_incarnation', 'role_instance_id'))
                and current['state'] == 'active' and current['lease_expires_at_us'] > before_owner['lease_expires_at_us'],
                'same Control owner did not renew during isolated Archive outage')
        require(process_identity(self.parent['pid']) == self.parent
                and ShortPinRuntime.worker(self, self.parent, allow_absent=True) is None,
                'Control identity changed or Archive native worker remains')
        directory = self.state/'data/archive-state'
        self.attempt_identity = input_identity(directory/index.raw(third['attempt_id'],16).hex(), directory=True)
        self.attempt_lock = directory/index.raw(third['attempt_id'],16).hex()/'custody.lock'
        self.lock_identity = custody_lock(self.attempt_lock, held=True)
        binding = directory/'coordinator.json'
        input_identity(binding)
        require(digest(binding) == index.raw(third['coordinator_binding_sha256'],32).hex()
                and binding.read_bytes() == (self.state/'data/archive/.bigbrotr-initialization/coordinator.json').read_bytes(),
                'issued attempt coordinator binding differs from both retained ends')
        intent = self.attempt_lock.with_name('intent.json')
        input_identity(intent)
        # The catalog binding has a terminal slash; configuration keeps the
        # user's path unchanged (configuration reference / catalog identity).
        catalog_root = tomllib.loads(self.config.read_text())['storage']['archive_root']
        if not catalog_root.endswith('/'):
            catalog_root += '/'
        expected = dict(format='bigbrotr-local-archive-custody/1',
            boot_id=Path('/proc/sys/kernel/random/boot_id').read_text().strip(),
            lock_device=self.lock_identity['device'], lock_inode=self.lock_identity['inode'],
            catalog_root=catalog_root,
            attempt_id=list(index.raw(third['attempt_id'],16)), publication_ordinal=3,
            topology_generation='0')
        expected.update({key:index.raw(third[key],size).hex() for key,size in
            [('dataset_id',16), ('catalog_id',16), ('coordinator_incarnation',16),
             ('worker_incarnation',16), ('batch_digest',32)]})
        require(load(intent) == expected, 'retained Archive attempt intent differs')
        self.parked_files = self.files(directory)
        save(output/'catalog-outage-state-files.json', self.parked_files)
        self.index_rows = {name:self.ch(index.select_rows(self.schema, DATABASE, name), rows=True)
                           for name in index.TARGETS}
        save(output/'clickhouse-outage.json', self.index_rows)
        save(output/'catalog-outage-live-verdict.json', dict(status='PASS', source_cut=12,
            archive_revision=2, archive_cut=3, index_revision=4, issued_attempt=3,
            parent=self.parent, control='same_owner_renewed', native_worker='absent',
            automatic_retry='not_observed', capacity='NOT_RUN'))

    def verify_index(self):
        self.source_unchanged()
        output = self.root/'index'; output.mkdir(mode=0o700)
        after, catalog = self.capture(output, 'after')
        terminal_clock = int(self.sql('bigbrotr_control', 'SELECT (EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint'))
        oracle.terminal_preservation(self.index_live, after, terminal_clock)
        save(output/'terminal-clock.json', dict(observed_at_us=terminal_clock))
        third = oracle.archived_prefix(self.prefix, after)
        index.exact(catalog, self.prefix_catalog)
        index.exact(self.files(self.state/'data/archive'), self.prefix_files)
        index.exact(self.files(self.state/'data/archive-state'), self.parked_files)
        index.exact(input_identity(self.attempt_lock.parent, directory=True), self.attempt_identity)
        custody_lock(self.attempt_lock, held=False, expected=self.lock_identity)
        before = load(self.root/'initial/before-control.json')
        view = self.current_owner_view(after, dict(control=before, resource=dict(catalog_id=self.ids['catalog'])))
        from installed_sync import validate_queued
        client = load(self.home/'client-result.json')
        self.validate_source(view, self.initialized, client['observations'])
        validate_queued(view, client['observations'], expected_principal=self.enrolled['principal_id'])
        owner = index.only(view['owner_epochs'], role='control')
        wires = [load(path) for path in sorted(self.home.glob('relay-closed-*.json'))]
        wanted = oracle.science(view, self.initialized, load(self.home/'event.jsonl'), wires,
            self.index_rows, self.state/'data/ingest', owner['released_at_us'])
        result = index.publication(self.index_live, after, self.index_rows, wanted, self.schema, self.state/'data/index')
        initialization = bytes.fromhex(digest(self.state/'data/archive/.bigbrotr-initialization/intent.json'))
        body = oracle.retained_input(third, wanted['batches'][2], initialization)
        path = self.state/'data/archive-state'/(index.raw(third['attempt_id'],16).hex()+'.inputs')
        identity = input_identity(path)
        require(path.read_bytes() == body and identity['sha256'] == hashlib.sha256(body).hexdigest(),
                'issued attempt original input differs')
        current_inputs = self.files(self.state/'data/ingest/producer-inputs-v1')
        require(len(current_inputs) == 4 and all(current_inputs.get(k) == v for k,v in self.prefix_inputs.items()),
                'already acknowledged prefix inputs changed')
        census = self.census(output, 'retained-prefix')
        oracle.archive_science(self.prefix, census, wanted)
        tables = self.ch(f"SELECT name,toString(uuid) AS uuid,engine_full FROM system.tables WHERE database='{DATABASE}' ORDER BY name", rows=True)
        columns = self.ch(f"SELECT table,name,type,position,default_kind,default_expression FROM system.columns WHERE database='{DATABASE}' ORDER BY table,position", rows=True)
        index.schema_metadata(self.schema, self.state/'data/index', tables, columns)
        save(output/'clickhouse-schema.json', dict(tables=tables, columns=columns))
        index.exact(self.custody(), self.custody_before)
        result.update(format='bigbrotr-installed-archive-outage/1',
            independent_source_and_c_api=False, independent_source_and_clickhouse=True,
            archive_prefix_c_api=True, archive_cut=3, retained_attempt=3,
            source_inputs='four_exact_originals', typed_progress='six_classifications',
            native_census='NOT_RUN', archive_recovery='NOT_RUN', capacity='NOT_RUN')
        save(output/'verdict.json', result); self.inputs['archive_outage'] = result
        self.source_unchanged()

    def cleanup(self):
        try:
            try:
                self.stop_route()
            except Exception as error:
                self.resources['cleanup_errors'].append(dict(resource='catalog-route', error=type(error).__name__))
            if hasattr(self, 'route'):
                try:
                    self.route.stop()
                except Exception as error:
                    self.resources['cleanup_errors'].append(dict(resource='catalog-route-forced-join', error=type(error).__name__))
                try:
                    save(self.route_output/'cleanup.json', self.proxy_observation(self.route))
                except Exception as error:
                    self.resources['cleanup_errors'].append(dict(resource='catalog-route-capture', error=type(error).__name__))
        finally:
            super().cleanup()
