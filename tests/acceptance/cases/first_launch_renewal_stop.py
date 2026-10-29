"""Stop an initialized Ingest owner during one identified PostgreSQL renewal.

Reuse the installed candidate/Compose owner and the existing bounded psql holder.
The row lock supplies the schedule; no peer, sleep or HTTP completion implies
that a particular product SQL operation has started.
"""
import json
import re
import socket
import sys
import time
import tomllib

from cases.first_launch import Runtime, require, save
from cases.first_launch_index_pin import process_identity

STAGES = ('prerequisites', 'candidate', 'runtime_image', 'fresh_bootstrap',
          'renewal_stop', 'cleanup')
LIMITS = [
    'Actual initialized Control/Ingest renewal only; no acquisition, Archive/Index worker, mutating HTTP or general shutdown qualification.',
    'The same positively identified Control backend/query remains blocked through SIGTERM. Known completion must drain; an original deadline must refuse success.',
    'PostgreSQL administrator observations and lock orchestration are independent of the actual configured product login. No administrator execution substitutes for product privileges.',
    'No rollback or absence of Control renewal effects is inferred on failure; retained rows and original terminal output record the observed outcome.',
]
RENEWAL_QUERY = ('SELECT substring(dataset_id FROM 1 FOR 17), '
                 'substring(instance_id FROM 1 FOR 17) FROM control.datasets LIMIT 2 FOR UPDATE')
EFFECT_TABLES = ('producer_epochs', 'producer_requests', 'jobs', 'consumer_frontiers',
                 'index_owner_sessions')


def select_waiter(rows, backend, holder_pid):
    if not rows:
        return None
    require(len(rows) == 1, 'multiple product renewal waiters')
    row, = rows
    require(all(row[key] == backend[key] for key in ('pid', 'backend_start', 'usename'))
            and row['wait_event_type'] == 'Lock'
            and row['blocking_pids'] == [holder_pid]
            and row['query'] == RENEWAL_QUERY, 'wrong product renewal phase or backend')
    return row


def verify_terminal(outcome, code, records, stderr, owners, effects_before, effects_after):
    fields = {'ingest_owner', *EFFECT_TABLES}
    require(set(effects_before) == fields and set(effects_after) == fields
            and all(isinstance(effects_before[k], list) and isinstance(effects_after[k], list) for k in fields)
            and len(effects_before['ingest_owner']) == len(effects_before['producer_epochs']) == 1,
            'complete initialized Ingest effect observations required')
    require(effects_before == effects_after, 'stop dispatched later Ingest renewal or durable work')
    require(len(owners) == 1, 'original Control owner identity missing')
    if outcome == 'known':
        require(code == 0 and not stderr and len(records) == 2
                and records[0]['state'] == 'initialized'
                and records[1]['state'] == 'stopped'
                and records[1]['native'] == 'closed'
                and records[1]['owner_release'] == 'committed'
                and owners[0]['state'] == 'released', 'known renewal did not drain and release')
    else:
        require(outcome == 'deadline' and code == 4 and len(records) == 1
                and records[0]['state'] == 'initialized' and owners[0]['state'] == 'active'
                and ('deadline' in stderr or 'expired' in stderr),
                'held renewal did not preserve its original deadline and unknown outcome')


class RenewalStopRuntime(Runtime):
    holder = None

    def load_helpers(self):
        super().load_helpers()
        sys.path.insert(0, str(self.source / 'experiments/archive-engine'))
        from signed_publication_cases import Process, event, send
        self.holder_process, self.holder_event, self.holder_send = Process, event, send

    def prepare_state(self):
        super().prepare_state()
        # Declare the existing default timing explicitly; neither outcome widens it.
        text = self.config.read_text()
        for key, value in (('authorization_ttl', '5s'), ('renewal_timeout', '5s')):
            text = self.set_value(text, 'runtime', key, value)
        self.config.write_text(text)
        from cases.first_launch import digest
        self.inputs['initial_recipe_sha256'] = digest(self.config)

    def effects(self):
        require(set(EFFECT_TABLES) <= self.tables, 'renewal observation schema missing')
        fields = ["'ingest_owner', (SELECT coalesce(jsonb_agg(to_jsonb(t) || "
                  "jsonb_build_object('xmin',xmin::text) ORDER BY resource_id,epoch),'[]') "
                  "FROM control.owner_epochs t WHERE role='ingest')"]
        fields += [f"'{name}', (SELECT coalesce(jsonb_agg(to_jsonb(t) ORDER BY to_jsonb(t)::text),'[]') "
                   f"FROM control.{name} t)" for name in EFFECT_TABLES]
        return json.loads(self.sql('bigbrotr_control', 'SELECT jsonb_build_object(' + ','.join(fields) + ')'))

    def renewal_stop(self):
        self.source_unchanged()
        output = self.root / 'renewal-stop'
        output.mkdir(mode=0o700)
        outcome = self.args.renewal_stop_outcome
        require(outcome in ('known', 'deadline'), 'explicit renewal stop outcome required')
        config = tomllib.loads(self.config.read_text())
        user = config['postgres']['control']['user']
        require(re.fullmatch('[a-z_][a-z0-9_]{0,62}', user), 'closed product role grammar')
        entry = self.launch('initial-renewal-stop', 'app', 'serve', '--file',
                            '/etc/bigbrotr/instance.toml', '--roles', 'control,ingest', '--initialize-ingest')
        first = self.wait(lambda: self.logs(entry, output), 'initialized Ingest', 20, entry=entry)[0]
        require(first['state'] == 'initialized' and first['acquisition'] == 'not-started',
                'unexpected initialized owner profile')
        require(any(row['kind'] == 'private-session-http' and row['port'] == 18081
                    for row in first['listeners']), 'actual private listener missing')
        # Establish the positive listener before locking SQL; an already closed
        # or unrelated endpoint cannot satisfy the later stop observation.
        with socket.create_connection(('127.0.0.1', 18081), timeout=.2):
            pass
        backend_query = ("SELECT coalesce(jsonb_agg(jsonb_build_object('pid',a.pid,"
            "'backend_start',a.backend_start,'usename',a.usename,'resource_id',encode(o.resource_id,'hex'),"
            "'epoch',o.epoch::text)),'[]') FROM pg_stat_activity a JOIN control.owner_epochs o "
            "ON o.control_backend_pid=a.pid AND o.control_backend_start=a.backend_start "
            f"WHERE a.datname=current_database() AND a.usename='{user}' AND o.role='control' AND o.state='active'")
        backends = json.loads(self.sql('bigbrotr_control', backend_query))
        require(len(backends) == 1, 'one actual product Control backend required')
        backend, = backends
        require(re.fullmatch('[0-9a-f]{32}', backend['resource_id'])
                and re.fullmatch('[1-9][0-9]{0,19}', backend['epoch']), 'closed owner identity encoding')
        evidence = dict(status='INCOMPLETE', outcome=outcome, backend=backend)
        save(output / 'schedule.json', evidence)
        holder = self.holder_process(['docker', 'exec', '-i', self.pg['id'], 'psql', '-XAtq',
            '-v', 'ON_ERROR_STOP=1', '-U', 'postgres', '-d', 'bigbrotr_control'], input_pipe=True)
        self.holder = holder
        process = dict(process_identity(holder.process.pid), status='running')
        self.holder_record = process
        save(output / 'holder-process.json', process)
        try:
            self.holder_send(holder, "BEGIN; SELECT json_build_object('event','locked','pid',pg_backend_pid()) "
                             "FROM control.datasets FOR UPDATE;\n")
            locked = self.holder_event(holder, 'locked', timeout=3)
            holder_pid = locked['pid']
            require(type(holder_pid) is int and holder_pid > 0 and holder_pid != backend['pid'],
                    'independent PostgreSQL lock holder required')

            def blocked():
                rows = json.loads(self.sql('bigbrotr_control', "SELECT coalesce(jsonb_agg(jsonb_build_object("
                    "'pid',pid,'backend_start',backend_start,'usename',usename,'query',query,"
                    "'wait_event_type',wait_event_type,'blocking_pids',pg_blocking_pids(pid))),'[]') "
                    f"FROM pg_stat_activity WHERE datname=current_database() AND {holder_pid}=ANY(pg_blocking_pids(pid))"))
                return select_waiter(rows, backend, holder_pid)

            evidence.update(holder=locked, waiter=self.wait(blocked, 'actual renewal query', 4, entry=entry))
            before = self.effects()
            require(not before['producer_requests'] and not before['jobs'], 'unexpected dispatched source work')
            save(output / 'effects-before.json', before)
            save(output / 'schedule.json', evidence)
            self.docker('kill', '--signal', 'TERM', entry['id'])
            signalled = time.monotonic()

            def admission_closed():
                try:
                    with socket.create_connection(('127.0.0.1', 18081), timeout=.1) as peer:
                        peer.settimeout(.1)
                        peer.sendall(b'GET /management/v1/me HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n')
                        return peer.recv(1) == b''
                except (ConnectionRefusedError, ConnectionResetError):
                    return True
                except socket.timeout:
                    return False

            if outcome == 'known':
                self.wait(admission_closed, 'private admission closed while renewal blocked', .6, entry=entry)
                require(blocked() == evidence['waiter'], 'signal abandoned the same PostgreSQL renewal')
                evidence['admission_closed_before_unlock'] = True
                evidence['parent_alive_before_unlock'] = self.inspect(entry)['State']['Running']
                require(evidence['parent_alive_before_unlock'], 'owner exited before known SQL completion')
                self.holder_send(holder, 'ROLLBACK;\n\\q\n')
                require(holder.wait(3) == 0 and not holder.stderr and not holder.overflow, 'lock holder did not join')
            # The negative schedule keeps the lock through the actual terminal
            # refusal: no success can be manufactured by unlocking after expiry.
            self.docker('wait', entry['id'], timeout=10)
            evidence['signal_to_terminal_seconds'] = time.monotonic() - signalled
            if outcome == 'deadline':
                require(holder.process.poll() is None, 'negative lock disappeared before refusal')
                self.holder_send(holder, 'ROLLBACK;\n\\q\n')
                require(holder.wait(3) == 0 and not holder.stderr and not holder.overflow, 'negative holder did not join')
            holder.stop()
            process.update(status='joined', exit_code=holder.process.returncode)
            save(output / 'holder-process.json', process)
            self.holder = None
            info = self.inspect(entry)
            require(not info['State']['Running'] and info['State']['Pid'] == 0 and not info['State']['OOMKilled'],
                    'initial renewal owner not terminated')
            records = self.logs(entry, output)
            after = self.effects()
            owners = json.loads(self.sql('bigbrotr_control', "SELECT coalesce(jsonb_agg(jsonb_build_object("
                "'state',state)),'[]') FROM control.owner_epochs "
                f"WHERE resource_id=decode('{backend['resource_id']}','hex') AND epoch={backend['epoch']}"))
            save(output / 'effects-after.json', after)
            save(output / 'owner-after.json', owners)
            save(output / 'terminal-container.json', dict(id=info['Id'], state=info['State']))
            self.backend_absence(output)
            verify_terminal(outcome, info['State']['ExitCode'], records, (output/'serve.stderr').read_text(),
                            owners, before, after)
            evidence['status'] = 'PASS'
        finally:
            save(output / 'schedule.json', evidence)

    def cleanup(self):
        holder = self.holder
        try:
            if holder:
                holder.stop()
        finally:
            try:
                super().cleanup()
            finally:
                if holder:
                    # A local docker-exec exit alone is not backend exclusion;
                    # the inherited cleanup also stops the owned PG container.
                    holder.stop()
                    require(holder.process.poll() is not None
                            and not any(t.is_alive() for t in holder.threads), 'holder process/pipes not joined')
                    self.holder_record.update(status='joined', exit_code=holder.process.returncode,
                        backend_exclusion='Owned PostgreSQL shutdown is recorded separately by inherited cleanup')
                    save(self.root/'renewal-stop/holder-process.json', self.holder_record)
