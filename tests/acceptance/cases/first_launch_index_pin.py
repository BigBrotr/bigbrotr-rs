"""Closed ordinary full-probe pin expiry; no engines run on import.

The existing signed bootstrap/history/export journey supplies the positive
baseline. Only the next complete reader is gated; neither SQL authority nor a
native process is fabricated. Unknown cleanup remains a separate unexecuted gate.
"""
import http.client
import json
import os
from pathlib import Path
import subprocess
import sys
import time

from cases.first_launch import digest, load, require, save, stop_process
from cases.first_launch_index import IndexRuntime, LIMITS as INDEX_LIMITS
from cases import first_launch_index_oracle as oracle

MODE = 'ordinary-short-pin/1'
PHASES = ('setup', 'positive', 'pin-expiry')
LIMITS = [item for item in INDEX_LIMITS if not item.startswith(('Ordinary Control must', 'Original ordinary journey'))] + [
    'The same ordinary Control process must first report both capabilities Ready; its next complete native reader is blocked under an original <=3s pin while the same Control backend/epoch renews. The original5s authorization and30s probe bounds remain.',
    'The original40s journey/export12s gates remain; setup, positive and pin-expiry each have30s (overall130s). No history/export request occurs after the positive barrier.',
    'NOT_RUN in this variant: withheld-input/restoration, pin-release COMMIT uncertainty before/after, genuine TerminationUnknown and general dependency outages.',
]
AUTHORITY = ('dataset_id','resource_id','epoch','role_instance_id','process_incarnation',
             'control_backend_pid','control_backend_start','acquired_at_us')
PIN_FIXED = ('lease_id','dataset_id','consumer_id','frontier_revision','topology_generation',
             'holder_kind','holder_job_id','holder_role_instance_id','holder_role','holder_incarnation',
             'acquired_at_us','expires_at_us','archive_pin_protocol')
READER_LEASES = ("FROM control.snapshot_leases l JOIN control.archive_snapshot_pins p USING(lease_id) "
                "WHERE l.state='active' AND l.holder_kind='role_instance' "
                "AND l.archive_pin_protocol=1 AND p.purpose='reader'")


def profile(text, set_value, port):
    require(type(port) is int and 1 <= port <= 65535, 'proxy port')
    for section, key, value in (
        ('postgres.catalog','endpoint',f'127.0.0.1:{port}'),
        ('runtime','lease_ttl','3s'), ('runtime','heartbeat_interval','500ms'),
        ('runtime','renewal_timeout','500ms'), ('runtime','renewal_safety_margin','500ms'),
        ('runtime','authorization_ttl','5s'),
    ):
        text = set_value(text,section,key,value)
    return text


def number(value):
    require(type(value) is int, 'exact integer observation required')
    return value


def reader_identity(reader, acquired):
    require(number(reader['pid']) > 0 and isinstance(reader['backend_start'],str)
            and reader['backend_start'] and number(reader['backend_start_us']) >= number(acquired)
            and reader['usename'] == 'bigbrotr_archive_read'
            and reader['datname'] == 'bigbrotr_catalog'
            and reader['application_name'] == 'bigbrotr-archive-cut', 'actual newly admitted native reader')


def select_waiter(readers, blocker, acquired):
    """One actual relation waiter, independently discovered after the setup gate."""
    require(len(readers) <= 1, 'ambiguous native reader waiter')
    if not readers:
        return None
    reader, = readers
    reader_identity(reader, acquired)
    require(reader['wait_event_type'] == 'Lock' and reader['waiting_on_relation'] is True
            and number(blocker['pid']) in reader['blocking_pids'], 'actual reader blocked by owned relation lock')
    return reader


def verify_schedule(evidence):
    """Independent finite timeline checks; no expected value copied from native rows."""
    samples = evidence['samples']
    require(2 <= len(samples) <= 95 and number(evidence['discovery_observations']) >= 1
            and evidence['discovery_observations']+len(samples) <= 96, 'pin observation count')
    first, final = samples[0], samples[-1]
    gate = evidence['gate_sample']
    for identity in (evidence['parent'],evidence['worker']):
        for key in ('pid','ppid','start_ticks'):
            require(number(identity[key]) > 0, 'positive process identity')
    require(evidence['worker']['ppid'] == evidence['parent']['pid']
            and evidence['worker']['pid'] != evidence['parent']['pid'], 'actual native child of the retained parent')
    for identity in (evidence['gate_reader'],evidence['reader'],evidence['blocker']):
        require(number(identity['pid']) > 0 and isinstance(identity['backend_start'],str), 'backend identity')
    original = gate['lease']
    expiry, acquired = (number(original[k]) for k in ('expires_at_us','acquired_at_us'))
    reader_identity(evidence['gate_reader'], acquired)
    oracle.exact(gate['reader'], evidence['gate_reader'])
    oracle.exact(gate['worker'], evidence['worker'])
    require(gate['blocker'] is None and gate['now_us'] < expiry, 'live precheck gate before blocker')
    oracle.exact(select_waiter([first['reader']],evidence['blocker'],acquired), evidence['reader'])
    require(number(evidence['arm_clock_us']) <= acquired, 'no earlier hidden probe')
    require(0 < expiry-acquired <= 3_000_000 and first['pin']['requested_ttl_us'] > 5_000_000,
            'original pin must be the shorter owner-capped deadline')
    require(original['state'] == 'active' and original['lease_revision'] == 1
            and first['now_us'] < expiry and original['archive_pin_protocol'] == 1,
            'original live reader pin')
    require(first['pin']['purpose'] == 'reader' and first['pin']['lease_id'] == original['lease_id']
            and first['pin']['issuer_role'] == 'control'
            and [first['pin'][k] for k in ('dataset_id','issuer_resource','issuer_epoch','issuer_incarnation')]
                == [first['owner'][k] for k in ('dataset_id','resource_id','epoch','process_incarnation')],
            'reader issuer identity')
    require([original[k] for k in ('dataset_id','holder_kind','holder_job_id','holder_role_instance_id','holder_role','holder_incarnation')]
            == [first['owner']['dataset_id'],'role_instance',None,first['owner']['role_instance_id'],'control',first['owner']['process_incarnation']],
            'reader holder identity')
    require(first['pin']['consumer_id'] == original['consumer_id']
            and first['pin']['frontier_revision'] == original['frontier_revision'] == 4
            and original['topology_generation'] == 0, 'selected reader allocation')
    # begin_action precedes the exact <=500ms admission SQL; include the
    # independently checked100ms PG/local clock tolerance. Request-start
    # cancellation may precede stored PG expiry, but not this lower bound.
    earliest_expiry = expiry-600_000
    expected_authority = [gate['owner'][k] for k in AUTHORITY]
    oracle.exact(expected_authority,[evidence['baseline_owner'][k] for k in AUTHORITY])
    blocked = []
    offsets = []
    observations = [gate, *samples]
    for sample in observations:
        now = number(sample['now_us'])
        for row,keys in ((sample['owner'],('epoch','control_backend_pid','acquired_at_us','lease_expires_at_us')),
                (sample['lease'],('frontier_revision','topology_generation','lease_revision','acquired_at_us','updated_at_us','expires_at_us','archive_pin_protocol')),
                (sample['pin'],('issuer_epoch','frontier_revision','requested_ttl_us'))):
            for key in keys:
                number(row[key])
        if sample['lease']['released_at_us'] is not None:
            number(sample['lease']['released_at_us'])
        before, after = number(sample['started_ns']), number(sample['finished_ns'])
        require(0 < after-before < 2_000_000_000, 'bounded SQL sampling')
        offsets.append((now-after//1000, now-before//1000))
        require([sample['owner'][k] for k in AUTHORITY] == expected_authority
                and sample['owner']['state'] == 'active'
                and now < sample['owner']['lease_expires_at_us'] <= now+3_100_000,
                'same live Control authority')
        oracle.exact(sample['control_backend'],dict(pid=first['owner']['control_backend_pid'],
            backend_start=first['owner']['control_backend_start']))
        require([sample['lease'][k] for k in PIN_FIXED] == [original[k] for k in PIN_FIXED]
                and sample['pin'] == first['pin'], 'original pin must not renew or change')
        oracle.exact(sample['parent'],evidence['parent'])
        if sample is not gate:
            oracle.exact(sample['blocker'], evidence['blocker'])
        reader = sample['reader']
        if reader is not None:
            identity = evidence['gate_reader'] if sample is gate else evidence['reader']
            reader_identity(reader, acquired)
            require(reader['backend_start_us'] <= now, 'reader backend started after observation')
            require(all(reader[k] == identity[k] for k in ('pid','backend_start','backend_start_us')),
                    'actual selected native reader identity changed')
            if (sample is not gate and reader['wait_event_type'] == 'Lock'
                    and reader['waiting_on_relation'] is True and evidence['blocker']['pid'] in reader['blocking_pids']
                    and sample['worker'] is not None and now < expiry):
                blocked.append(sample)
        if sample['worker'] is None:
            require(now >= earliest_expiry, 'native disappeared before possible original pin expiry')
        if sample['worker'] is not None:
            oracle.exact(sample['worker'],evidence['worker'])
        lease = sample['lease']
        if lease['state'] == 'active':
            require(lease['lease_revision'] == 1 and lease['released_at_us'] is None
                    and lease['release_reason'] is None, 'active pin history')
        else:
            require(lease['state'] == 'released' and lease['lease_revision'] == 2
                    and lease['release_reason'] == 'reader_joined'
                    and lease['updated_at_us'] == lease['released_at_us']
                    and sample['worker'] is None
                    and lease['released_at_us'] >= earliest_expiry, 'released only after native join at original pin window')
        claims = sample['claims']
        for claim in claims:
            for key in ('topology_generation','shard_id','owner_epoch','first_sequence','last_sequence'):
                number(claim[key])
        require(len(claims) == 1 and claims[0]['lease_id'] == original['lease_id']
                and [claims[0][k] for k in ('dataset_id','topology_generation','shard_id','owner_epoch','first_sequence','last_sequence','purpose')]
                == [original['dataset_id'],0,0,1,1,12,'reader'], 'complete reader retention claim')
        require(claims[0]['state'] == lease['state'], 'claim and pin release together')
        if lease['state'] == 'released':
            require(claims[0]['release_reason'] == 'reader_joined'
                    and type(claims[0]['released_at_us']) is int, 'joined claim release')
    require(all(a['now_us'] < b['now_us'] and a['finished_ns'] < b['started_ns']
                for a,b in zip(observations,observations[1:])), 'monotonic sample order')
    # PG wall time and local monotonic brackets must admit one offset within
    #100ms. A wall-clock step cannot masquerade as either deadline expiring.
    require(max(a for a,_ in offsets)-min(b for _,b in offsets) <= 100_000, 'clock mapping changed')
    require(blocked and blocked[-1]['now_us'] >= earliest_expiry-400_000,
            'actual blocked worker must survive close to earliest pin expiry')
    require(blocked and final['now_us'] >= expiry and final['worker'] is None,
            'blocked native positively absent after original expiry')
    require(final['lease']['state'] == 'released'
            and final['lease']['released_at_us'] < expiry+1_000_000
            and final['now_us'] < expiry+1_250_000, 'joined cancellation must precede longer action deadline')
    # A successful owner refresh samples its3s lease inside a <=500ms operation
    # whose policy ticket begins before it. Its5s action deadline is therefore
    # no earlier than original expiry+1.5s; later policy polls only advance it.
    # The100ms clock tolerance above still leaves a discriminating margin.
    require(final['owner']['lease_expires_at_us'] > gate['owner']['lease_expires_at_us']
            and final['owner']['lease_expires_at_us'] > expiry+3_000_000,
            'same authority renewed past the unchanged original pin')
    require(final['finished_ns']-evidence['phase_started_ns'] < 30_000_000_000,
            'original probe/phase deadline')
    require(evidence['blocker']['pid'] > 0 and evidence['blocker']['backend_start']
            and evidence['blocker']['pid'] != evidence['reader']['pid'], 'independent blocker custody')
    post = evidence['post_expiry_roles']
    require(len(post) >= 2 and all(expiry <= int(value['observed_at_us']) <= number(evidence['post_clock_us'])
            and len(value['capabilities']) == 2 and all(cap['state'] == 'unavailable' and cap['reason'] == 'dependency_unavailable'
                    for cap in value['capabilities']) for value in post), 'no Ready after expired full probe')
    require(evidence['gate_terminal'] == [] and evidence['reader_terminal'] == []
            and evidence['blocker_terminal'] == [], 'catalog backends must actually end')
    return dict(status='PASS',original_pin_lifetime_us=expiry-acquired, blocked_observations=len(blocked),
        admission_skew_ceiling_us=500_000,clock_tolerance_us=100_000,
        same_control_authority_renewed=True,native_joined=True,reader_pin='released',
        action_timeout='not_reached',probe_timeout='not_reached',unknown_cleanup='NOT_RUN')


def process_identity(pid):
    """Observe only this owned process, with start-time reuse protection."""
    require(type(pid) is int and pid > 0, 'process PID')
    path = Path('/proc')/str(pid)
    try:
        raw = (path/'stat').read_bytes()
    except FileNotFoundError:
        return None
    require(len(raw) <= 4096 and b') ' in raw, 'bounded process stat')
    fields = raw.rsplit(b') ',1)[1].split()
    require(len(fields) >= 20 and fields[0] in (b'R', b'S', b'D', b'Z', b'T', b't', b'I'), 'known observed process state')
    return dict(pid=pid,ppid=int(fields[1]),start_ticks=int(fields[19]))


class ShortPinRuntime(IndexRuntime):
    def candidate(self):
        require(digest(Path(__file__)) == digest(self.source/'tests/acceptance/cases'/Path(__file__).name),
                'short-pin source mismatch')
        super().candidate()
        relative = 'experiments/archive-engine/native_commit_proxy.py'
        self.inputs['acceptance_files_sha256'][relative] = digest(self.source/relative)

    def load_helpers(self):
        super().load_helpers()
        sys.path.insert(0,str(self.source/'experiments/archive-engine'))
        from fault_cases import Process
        from signed_publication_cases import event, send, proxy_observation
        self.proxy_process, self.proxy_event, self.proxy_send, self.proxy_observation = Process,event,send,proxy_observation

    def ordinary_profile(self):
        return MODE, PHASES, 30, 130

    def ordinary(self):
        output = self.root/'short-pin'; output.mkdir(mode=0o700)
        original = self.config.read_text()
        proxy_path = self.source/'experiments/archive-engine/native_commit_proxy.py'
        proxy_args = [sys.executable,'-B',str(proxy_path),'--mode','hold-reader-query',
            '--profile','ordinary-reader',
            '--upstream-port','15432','--catalog-manager','bigbrotr_catalog',
            '--catalog-reader','bigbrotr_archive_read','--catalog-database','bigbrotr_catalog',
            '--defer-arm','--lifetime-seconds','180']
        proxy = self.proxy_process(proxy_args,input_pipe=True)
        proxy_identity = process_identity(proxy.process.pid)
        save(output/'proxy-process.json',dict(argv=proxy_args,identity=proxy_identity,
            source_sha256=digest(proxy_path),lifetime_seconds=180,termination_scope='owned child PID only'))
        self.pin_proxy = proxy
        failure = None
        try:
            ready = self.proxy_event(proxy,'ready',10)
            require(proxy_identity is not None and process_identity(proxy.process.pid) == proxy_identity, 'same proxy PID/start')
            require(ready['upstream_host'] == '127.0.0.1' and ready['upstream_port'] == 15432
                    and ready['armed'] is False and ready['source_sha256'] == digest(proxy_path)
                    and ready['profile'] == 'ordinary-reader' and ready['frame_limit'] == 20000
                    and ready['connection_limit'] == 16 and ready['connection_limit_scope'] == 'active'
                    and ready['active_connections'] == ready['peak_connections'] == 0, 'owned deferred proxy')
            save(output/'proxy-ready.json',ready)
            self.config.write_text(profile(original,self.set_value,ready['port']))
            super().ordinary()
        except BaseException as error:
            failure = error
            save(output/'original-failure.json',dict(error_type=type(error).__name__,reason=str(error)))
            raise
        finally:
            try:
                self.proxy_send(proxy,'stop\n')
                require(proxy.wait(5) == 0, 'proxy stop/join')
            except Exception as error:
                save(output/'proxy-stop-failure.json',dict(error_type=type(error).__name__,reason=str(error)))
                self.resources['cleanup_errors'].append(dict(resource='short-pin-proxy',error='proxy stop/join failed'))
                if failure is None:
                    raise
            finally:
                proxy.stop()
                save(output/'proxy-terminal.json',self.proxy_observation(proxy))
                # Retain the exact tested deployment profile and all failure
                # evidence; the enclosing owner stops application/PG resources.

    def bounded_sql(self, database, statement, deadline):
        remaining = deadline-time.monotonic()
        require(remaining > 0, 'short-pin SQL deadline')
        result = self.docker('exec','-i',self.pg['id'],'psql','-XAtq','--no-password',
            '-v','ON_ERROR_STOP=1','-U','postgres','-d',database,
            data="SET statement_timeout='1s';\n"+statement, timeout=min(2,remaining),label='short-pin-observe')
        return json.loads(result.stdout)

    def worker(self, parent, expected=None, *, allow_absent=False):
        require(process_identity(parent['pid']) == parent, 'application PID/start changed')
        children = set()
        tasks = list((Path('/proc')/str(parent['pid'])/'task').iterdir())
        require(len(tasks) <= 256, 'application thread bound')
        for task in tasks:
            try:
                raw = (task/'children').read_bytes()
            except FileNotFoundError:
                continue  # An exited thread cannot own the retained live child.
            require(len(raw) <= 4096, 'native child inventory bound')
            children.update(int(value) for value in raw.split())
        require(len(children) <= 256, 'native child bound')
        if expected is not None:
            # The pinned trusted worker has no exec/fork path. After the initial
            # actual ELF hash, retain PID/start custody until positive reap.
            actual = process_identity(expected['pid'])
            if actual is None:
                require(not children, 'child inventory not reaped or replacement child exists')
                return None
            require(actual == {k:expected[k] for k in ('pid','ppid','start_ticks')}
                    and children == {actual['pid']}, 'native process identity changed or concurrent child exists')
            return expected
        found = []
        for pid in children:
            exe = Path('/proc')/str(pid)/'exe'
            if os.readlink(exe).endswith('/bigbrotr-archive-worker'):
                identity = process_identity(pid)
                require(identity is not None and identity['ppid'] == parent['pid'], 'native parent identity')
                identity['sha256'] = digest(exe)
                require(identity['sha256'] == digest(self.bundle/'archive-worker/bigbrotr-archive-worker'), 'actual verified worker ELF')
                found.append(identity)
        if allow_absent and not children:
            return None
        require(len(found) == len(children) == 1, 'one actual native reader child')
        return found[0]

    def sample(self, lease_id, reader_pid, parent, worker, deadline, blocker=None, acquired=None):
        require(lease_id.startswith('\\x') and len(lease_id) == 34
                and all(c in '0123456789abcdef' for c in lease_id[2:]), 'observed lease identity')
        started = time.monotonic_ns()
        blocker_sql = 'NULL'
        waiting_relation = 'false'
        if blocker is not None:
            pid, relation = number(blocker['pid']),number(blocker['relation'])
            blocker_sql = f"(SELECT json_build_object('pid',a.pid,'backend_start',a.backend_start,'relation',{relation}) FROM pg_stat_activity a WHERE a.pid={pid} AND EXISTS (SELECT 1 FROM pg_locks l WHERE l.pid=a.pid AND l.locktype='relation' AND l.relation={relation} AND l.database=(SELECT oid FROM pg_database WHERE datname='bigbrotr_catalog') AND l.mode='AccessExclusiveLock' AND l.granted))"
            waiting_relation = f"EXISTS (SELECT 1 FROM pg_locks l WHERE l.pid=a.pid AND l.locktype='relation' AND l.relation={relation} AND l.database=(SELECT oid FROM pg_database WHERE datname='bigbrotr_catalog') AND NOT l.granted)"
        if reader_pid is None:
            require(blocker is not None and type(acquired) is int, 'waiter discovery needs original admission and blocker')
            predicate = ("a.datname='bigbrotr_catalog' AND a.usename='bigbrotr_archive_read' "
                         "AND a.application_name='bigbrotr-archive-cut' AND a.wait_event_type='Lock' "
                         f"AND {pid}=ANY(pg_blocking_pids(a.pid))")
        else:
            require(type(reader_pid) is int and reader_pid > 0, 'reader backend PID')
            predicate = f'a.pid={reader_pid}'
        query = f"""SELECT json_build_object(
          'blocker',{blocker_sql},
          'now_us',(EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint,
          'owner',(SELECT row_to_json(o) FROM control.owner_epochs o WHERE role='control' AND state='active'),
          'control_backend',(SELECT json_build_object('pid',a.pid,'backend_start',a.backend_start)
              FROM pg_stat_activity a JOIN control.owner_epochs o ON a.pid=o.control_backend_pid
              AND a.backend_start=o.control_backend_start WHERE o.role='control' AND o.state='active'),
          'lease',(SELECT row_to_json(l) FROM control.snapshot_leases l WHERE lease_id=decode('{lease_id[2:]}','hex')),
          'pin',(SELECT row_to_json(p) FROM control.archive_snapshot_pins p WHERE lease_id=decode('{lease_id[2:]}','hex')),
          'claims',(SELECT coalesce(json_agg(c ORDER BY claim_id),'[]') FROM control.retention_claims c WHERE lease_id=decode('{lease_id[2:]}','hex')),
          'readers',(SELECT coalesce(json_agg(r ORDER BY pid),'[]') FROM (
              SELECT a.pid,a.backend_start,(EXTRACT(EPOCH FROM a.backend_start)*1000000)::bigint AS backend_start_us,
              a.usename,a.datname,a.application_name,a.wait_event_type,pg_blocking_pids(a.pid) AS blocking_pids,
              {waiting_relation} AS waiting_on_relation FROM pg_stat_activity a WHERE {predicate} ORDER BY a.pid LIMIT 2) r));"""
        value = self.bounded_sql('bigbrotr_control',query,deadline)
        readers = value.pop('readers')
        if reader_pid is None:
            oracle.exact(value['blocker'],blocker)
            value['reader'] = select_waiter(readers,blocker,acquired)
        else:
            require(len(readers) <= 1, 'one recorded reader backend')
            value['reader'] = readers[0] if readers else None
        value.update(parent=process_identity(parent['pid']),worker=self.worker(parent,worker))
        value.update(started_ns=started,finished_ns=time.monotonic_ns())
        return value

    def roles(self, deadline, *, session_path=None):
        saved = load(self.home/'export-session.private' if session_path is None else session_path)
        require(saved['format'] == 'bigbrotr-cli-session/1' and saved['origin'] == 'http://127.0.0.1:18081', 'session binding')
        connection = http.client.HTTPConnection('127.0.0.1',18081,timeout=min(2,max(.001,deadline-time.monotonic())))
        try:
            connection.request('GET','/management/v1/roles',headers={'Origin':saved['origin'],
                'Cookie':'BigBrotrLocalSession='+saved['session'],'X-CSRF-Token':saved['csrf']})
            response = connection.getresponse(); raw = response.read(131073)
            require(response.status == 200 and len(raw) <= 131072, 'short-pin role observation')
            return json.loads(raw)
        finally:
            connection.close()

    def ordinary_special_phase(self, phase, signal, deadline, output, entry, client):
        if phase != 'pin-expiry':
            return False
        target = self.root/'short-pin'
        before,catalog = self.capture(target,'before')
        preserved = {name:self.files(self.state/'data'/name) for name in ('archive','archive-state','export','scratch')}
        save(target/'before-files.json',preserved)
        client_observations = digest(self.home/'export-observations.json')
        baseline_before = self.bounded_sql('bigbrotr_control','SELECT (EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint',deadline)
        baseline = self.roles(deadline)
        clock = self.bounded_sql('bigbrotr_control','SELECT (EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint',deadline)
        oracle.ordinary_roles(baseline,before,baseline_before,clock,'positive')
        save(target/'baseline-roles.json',baseline)
        info = self.inspect(entry); self.inspect(self.pg)
        parent = process_identity(info['State']['Pid'])
        require(parent is not None, 'actual serving PID')
        blocker = None
        baseline_owner, = [row for row in before['owner_epochs'] if row['role'] == 'control' and row['state'] == 'active']
        timeline = dict(phase_started_ns=signal['started_ns'],parent=parent,baseline_owner=baseline_owner,samples=[],post_expiry_roles=[])
        save(target/'timeline.json',timeline)
        try:
            require(self.worker(parent,allow_absent=True) is None, 'no native reader before arming')
            # The original completed export deliberately retains a role-owned
            # archive pin. Only reader-purpose pins indicate an earlier probe;
            # all export rows/claims must still remain unchanged afterward.
            arm = self.bounded_sql('bigbrotr_control',
                "SELECT json_build_object('now_us',(EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint,"
                "'active_readers',(SELECT count(*) " + READER_LEASES + "))",deadline)
            save(target/'before-arm.json',arm)
            require(arm['active_readers'] == 0, 'no earlier active reader pin')
            timeline['arm_clock_us'] = arm['now_us']
            save(target/'timeline.json',timeline)
            self.proxy_send(self.pin_proxy,'arm\n')
            save(target/'proxy-armed.json',self.proxy_event(self.pin_proxy,'armed',min(2,deadline-time.monotonic())))
            selected_reader = self.proxy_event(self.pin_proxy,'reader-selected',min(17,deadline-time.monotonic()))
            save(target/'proxy-reader-selected.json',selected_reader)
            held = self.proxy_event(self.pin_proxy,'held',min(2,deadline-time.monotonic()))
            require(held['stage'] == 'reader-query' and type(held['backend_pid']) is int
                    and selected_reader['admitted_after_arm'] is True
                    and selected_reader['backend_pid'] == held['backend_pid']
                    and selected_reader['connection_number'] == held['target_connection']
                    and selected_reader['framed_connections'] == held['framed_connections'] == 1,
                    'actual newly admitted native reader gate')
            save(target/'proxy-held.json',held)
            worker = self.worker(parent)
            pins = self.bounded_sql('bigbrotr_control',"SELECT coalesce(json_agg(l),'[]') " + READER_LEASES,deadline)
            old = {row['lease_id'] for row in before['snapshot_leases']}
            selected = [row for row in pins if row['lease_id'] not in old]
            require(len(selected) == 1, 'one new full-probe pin')
            lease_id = selected[0]['lease_id']
            gate = self.sample(lease_id,held['backend_pid'],parent,worker,deadline)
            require(gate['reader'] is not None, 'actual precheck gate backend')
            reader_identity(gate['reader'],gate['lease']['acquired_at_us'])
            save(target/'gate-sample.json',gate)
            timeline.update(worker=worker,gate_reader=gate['reader'],gate_sample=gate)
            save(target/'timeline.json',timeline)
            # Start an independently owned session. A server idle timeout bounds
            # its lock even if the local docker-exec wrapper becomes unknown.
            args = ['docker','exec','-i',self.pg['id'],'psql','-XAtq','--no-password','-v','ON_ERROR_STOP=1',
                    '-U','postgres','-d','bigbrotr_catalog']
            with (target/'blocker.stdout').open('w') as stdout,(target/'blocker.stderr').open('w') as stderr:
                blocker = subprocess.Popen(args,env=self.environment,cwd=self.root,stdin=subprocess.PIPE,
                    stdout=stdout,stderr=stderr,text=True,start_new_session=True)
                self.processes.append(blocker)
                blocker.stdin.write("BEGIN; SET LOCAL lock_timeout='500ms'; SET LOCAL statement_timeout='1s'; SET LOCAL idle_in_transaction_session_timeout='6s'; LOCK TABLE public.ducklake_data_file IN ACCESS EXCLUSIVE MODE; SELECT json_build_object('pid',pg_backend_pid(),'backend_start',backend_start,'relation','public.ducklake_data_file'::regclass::oid::bigint) FROM pg_stat_activity WHERE pid=pg_backend_pid();\n")
                blocker.stdin.flush()
                until = min(deadline,time.monotonic()+1.5)
                while not (target/'blocker.stdout').stat().st_size:
                    require(blocker.poll() is None and time.monotonic() < until, 'blocker admission')
                    time.sleep(.01)
                require((target/'blocker.stdout').stat().st_size <= 4096, 'blocker output bound')
                timeline['blocker'] = json.loads((target/'blocker.stdout').read_text())
                save(target/'timeline.json',timeline)
                self.proxy_send(self.pin_proxy,'release\n')
                self.proxy_event(self.pin_proxy,'released',min(1,deadline-time.monotonic()))
                expiry = gate['lease']['expires_at_us']
                # Native attachment closes the gated PostgreSQL precheck before
                # opening DuckLake metadata connections. Discover the real SQL
                # waiter independently, retaining the same actual child custody.
                discovery = []
                while True:
                    require(len(discovery) < 95 and time.monotonic() < deadline, 'waiter discovery deadline')
                    sample = self.sample(lease_id,None,parent,worker,deadline,timeline['blocker'],
                        acquired=gate['lease']['acquired_at_us'])
                    discovery.append(sample); save(target/'waiter-discovery.json',discovery)
                    oracle.exact(sample['worker'],worker)
                    require(sample['now_us'] < expiry and sample['lease']['state'] == 'active',
                            'actual waiter must be observed under the original live pin')
                    if sample['reader'] is not None:
                        timeline['discovery_observations'] = len(discovery)
                        timeline['reader'] = sample['reader']
                        timeline['samples'].append(sample)
                        save(target/'timeline.json',timeline)
                        break
                    time.sleep(.01)
                while True:
                    require(len(discovery)+len(timeline['samples']) < 96 and time.monotonic() < deadline, 'pin observation deadline')
                    sample = self.sample(lease_id,timeline['reader']['pid'],parent,worker,deadline,timeline['blocker'])
                    timeline['samples'].append(sample); save(target/'timeline.json',timeline)
                    if (sample['now_us'] >= expiry and sample['worker'] is None and sample['lease']['state'] == 'released'
                            and sample['owner']['lease_expires_at_us'] > expiry+3_000_000):
                        break
                    require(sample['now_us'] < expiry+1_250_000, 'native did not join by discriminating pin deadline')
                    time.sleep(.05)
                # HTTP is observed only after the gate has caused positive join;
                # no user read/export can substitute its own readiness proof.
                role_deadline = min(deadline,time.monotonic()+2)
                role_attempts = 0
                while len(timeline['post_expiry_roles']) < 2:
                    require(role_attempts < 12 and time.monotonic() < role_deadline, 'post-expiry status deadline')
                    role_attempts += 1
                    value = self.roles(role_deadline)
                    save(target/('post-expiry-role-'+str(role_attempts)+'.json'),value)
                    if int(value['observed_at_us']) >= expiry:
                        timeline['post_expiry_roles'].append(value)
                        save(target/'timeline.json',timeline)
                        require(all(cap['state'] != 'ready' for cap in value['capabilities']), 'Ready after original pin expiry')
                    time.sleep(.05)
                blocker.stdin.write('ROLLBACK;\n\\q\n'); blocker.stdin.flush()
                blocker.wait(timeout=min(2,max(.001,deadline-time.monotonic())))
                require(blocker.returncode == 0 and not (target/'blocker.stderr').read_bytes(), 'owned blocker rollback')
            # The backend may have remained lock-waiting after local child reap;
            # require its actual disappearance only after releasing our lock.
            for key,identity in (('gate_terminal',timeline['gate_reader']),('reader_terminal',timeline['reader']),
                                 ('blocker_terminal',timeline['blocker'])):
                while True:
                    rows = self.bounded_sql('bigbrotr_catalog',f"SELECT coalesce(json_agg(json_build_object('pid',pid,'backend_start',backend_start)),'[]') FROM pg_stat_activity WHERE pid={number(identity['pid'])}",deadline)
                    timeline[key] = rows; save(target/'timeline.json',timeline)
                    if not rows:
                        break
                    require(len(rows) == 1 and rows[0] == {k:identity[k] for k in ('pid','backend_start')}, 'catalog PID reused during cleanup')
                    require(time.monotonic() < deadline, 'catalog backend remained after cleanup')
                    time.sleep(.05)
            after,after_catalog = self.capture(target,'after')
            require(digest(self.home/'export-observations.json') == client_observations, 'client performed an operation inside pin barrier')
            oracle.ordinary_unchanged(before,after)
            for table in ('snapshot_leases','archive_snapshot_pins','retention_claims'):
                oracle.exact([row for row in after[table] if row['lease_id'] != lease_id],before[table])
            old_owners = [row for row in before['owner_epochs'] if row['resource_id'] != baseline_owner['resource_id']]
            oracle.exact([row for row in after['owner_epochs'] if row['resource_id'] != baseline_owner['resource_id']],old_owners)
            require(catalog == after_catalog, 'short-pin probe changed scientific catalog')
            require(preserved == {name:self.files(self.state/'data'/name) for name in preserved}, 'short-pin probe changed retained/scientific/export files')
            require(self.inspect(entry)['State']['Pid'] == parent['pid']
                    and process_identity(parent['pid']) == parent, 'same ordinary container remained live')
            timeline['post_clock_us'] = self.bounded_sql('bigbrotr_control',
                'SELECT (EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint',deadline)
            save(target/'timeline.json',timeline)
            for value in timeline['post_expiry_roles']:
                oracle.ordinary_roles(value,after,expiry,timeline['post_clock_us'],'missing')
            self.pin_verdict = verify_schedule(timeline)
            save(target/'verdict.json',self.pin_verdict)
            return True
        finally:
            if blocker is not None:
                if blocker.poll() is None:
                    try:
                        blocker.stdin.write('ROLLBACK;\n\\q\n'); blocker.stdin.flush()
                        blocker.wait(timeout=2)
                    except (OSError,subprocess.TimeoutExpired):
                        self.resources['cleanup_errors'].append(dict(resource='short-pin-blocker',error='remote outcome unresolved; owned PostgreSQL stop required'))
                stop_process(blocker)
                self.processes.remove(blocker)
                save(target/'blocker-process.json',dict(pid=blocker.pid,exit_code=blocker.returncode,
                    remote_exclusion='verified' if timeline.get('blocker_terminal') == [] else 'unknown'))

    def ordinary_verdict(self, completed, timing, journal):
        require(completed == list(PHASES) and journal is None and timing['mode'] == MODE, 'closed short-pin phases')
        require([timing[k] for k in ('original_limit_seconds','export_limit_seconds','overall_limit_seconds')]
                == [40,12,130] and type(timing['original_active_ns']) is int
                and 0 < timing['original_active_ns'] <= 40_000_000_000, 'original journey bounds')
        require([row['phase'] for row in timing['phases']] == completed
                and all(row['limit_seconds'] == 30 and 0 < row['finished_ns']-row['started_ns'] < 30_000_000_000
                        for row in timing['phases']), 'phase timing bounds')
        require(self.pin_verdict['status'] == 'PASS', 'short-pin verdict missing')
        return dict(self.pin_verdict,phases=completed,source='unknown',control='ready',
            original_export_gate='pending existing independent signed export oracle')
