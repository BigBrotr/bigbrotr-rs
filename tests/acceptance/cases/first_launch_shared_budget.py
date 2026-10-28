"""One real capacity wait behind an owned Index operation, then signed publication.

Shares the maintained source gate, engine custody, scientific oracles and ordinary
history/export journey. The operator trace is evidence of scheduling only.
"""
import json
from pathlib import Path
import time
import tomllib

from cases.first_launch import Runtime, digest, load, require, save
from cases.first_launch_index import IndexRuntime, STAGES
from cases.first_launch_index_restart import RestartIndexRuntime
from cases import first_launch_index_oracle as index
from cases import first_launch_shared_budget_oracle as oracle
from cases import first_launch_archive_outage_oracle as terminal

# The finite profile adds one 16 MiB staged wire response to the earlier compiled
# requirements: full Index + four Archive inputs + one login =892,600,502 B;
# retained physical Index + two inputs + Sync =934,974,128 B.
# Shift only this test envelope by the same 16 MiB (848→864), preserving the
# 13,369,162 B admission margin and 29,004,464 B contention gap before measurement.
# This fixed envelope admits complete isolated work but requires actual overlap
# evidence below. It is not an RSS estimate or a scheduling guarantee.
APPLICATION_PAYLOAD_BYTES = 864 * 1024**2


def application_payload():
    require(type(APPLICATION_PAYLOAD_BYTES) is int and APPLICATION_PAYLOAD_BYTES > 0,
            'missing fixed shared-budget recipe')
    return f'{APPLICATION_PAYLOAD_BYTES}B'

LIMITS = [
    'One actual pre-effect Sync or Archive application-budget refusal must retain an identified Index operation; both original deferred/admitted records and the same Control/wait identity are required.',
    'The existing signed prefix15s, client65s, Index query5s and operation30s bounds remain. Only the positively owned ClickHouse is paused; one full82-table capture precedes immediate unpause. Capture overhead can fail the case.',
    'Admission must follow positive local join/result consumption of the denied Index operation. This is not remote exclusion, readiness, an atomic failing-budget snapshot or proof that Index was the only charged owner.',
    'After resumption all four original source batches, six classified events, forty Index outcomes/ten families and independent Archive C API science must match; same-owner orderly shutdown precedes original353-byte history/export with40s/12s bounds.',
    'No fixed denied role or intermediate source cut is assumed. Accepted1 and already-claimed queued2 jobs are separately checked; permanent insufficiency, unknown outcomes and missing/incomplete diagnostics cannot pass.',
    'NOT_RUN: general capacity, RSS/disk pressure, remote uncertainty resolution, lost COMMIT, native TerminationUnknown, automatic recovery, topology continuation and complete M1.',
]

BACKEND = """SELECT COALESCE(json_agg(json_build_object(
    'resource_id',encode(o.resource_id,'hex'),'epoch',o.epoch::text,
    'incarnation',encode(o.process_incarnation,'hex'),
    'role_instance_id',encode(o.role_instance_id,'hex'),
    'pid',a.pid,'started',a.backend_start,
    'observed_at_us',(EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint)), '[]'::json)
    FROM control.owner_epochs o JOIN pg_stat_activity a
      ON a.pid=o.control_backend_pid AND a.backend_start=o.control_backend_start
    WHERE o.role='control' AND o.state='active' AND a.datname=current_database()
      AND a.backend_type='client backend'"""


def records(text):
    """Complete bounded lifecycle stdout, never a filtered/reconstructed pair."""
    require(len(text.encode()) <= 132*4096, 'capacity lifecycle byte bound')
    lines = text.splitlines()
    require(0 < len(lines) <= 132 and all(len(line.encode())+1 <= 4096 for line in lines), 'capacity lifecycle line bound')
    values = [json.loads(line) for line in lines]
    require(values[0].get('state') == 'initialized', 'capacity lifecycle prefix missing')
    result = [v for v in values if v.get('format') == oracle.FORMAT]
    require(len(result) <= 128 and all(v.get('transition') != 'incomplete' for v in result), 'capacity evidence incomplete')
    seen = {}
    for value in result:
        oracle.observation(value)
        key = (value['wait_id'], value['transition'])
        require(key not in seen, 'duplicate capacity transition')
        if value['transition'] == 'admitted':
            require((value['wait_id'], 'deferred') in seen, 'admission lacks original denial')
        seen[key] = value
    return result


class SharedBudgetRuntime(RestartIndexRuntime):
    inject_content_conflict = False
    ordinary_client = IndexRuntime.baseline_client

    def candidate(self):
        for module in (oracle, terminal):
            path = Path(module.__file__)
            require(digest(path) == digest(self.source/'tests/acceptance/cases'/path.name), 'shared-budget oracle source mismatch')
        require(digest(Path(__file__)) == digest(self.source/'tests/acceptance/cases'/Path(__file__).name), 'shared-budget driver source mismatch')
        super().candidate()

    def load_helpers(self):
        super().load_helpers()
        # Bind the existing complete Archive publication encoder before the
        # candidate captures imported helper identities.
        import installed_archive_catch_up_oracle

    def prepare_state(self):
        super().prepare_state()
        self.config.write_text(self.set_value(self.config.read_text(), 'runtime.processes.application',
                                             'payload_bytes', application_payload()))
        self.inputs['initial_recipe_sha256'] = digest(self.config)
        self.inputs['shared_budget_recipe'] = dict(application_payload=application_payload(),
            source='fixed compiled finite-profile selection',capacity='requires actual overlap')

    def initial_launch(self):
        require(tomllib.loads(self.config.read_text())['runtime']['processes']['application']['payload_bytes']
                == application_payload(), 'shared-budget deployment recipe changed')
        return super().initial_launch()

    def capacity_records(self, entry, output):
        response = self.docker('logs', '--tail', '132', entry['id'], label='capacity-observation')
        require(not response.stderr, 'capacity owner emitted an error')
        current = records(response.stdout)
        previous = getattr(self, 'capacity_seen', [])
        require(current[:len(previous)] == previous, 'capacity lifecycle lost or replaced original records')
        self.capacity_seen = current
        save(output/'capacity-observations.json', current)
        return current

    def establish_prefix_fault(self, entry, output):
        # Baseline is collected before the pause, inside the original prefix
        # deadline. A previously completed wait cannot satisfy this case.
        self.capacity_prefix = self.capacity_records(entry, output)
        save(output/'capacity-prefix.json', self.capacity_prefix)
        return super().establish_prefix_fault(entry, output)

    def initial_archive_observation(self, entry, output):
        submitted = load(self.home/'submitted.json')
        receipt, = [row['body'] for row in submitted if row['path'] == 'jobs']
        def denied():
            current = self.capacity_records(entry, output)
            for value in current[len(self.capacity_prefix):]:
                if value['transition'] == 'deferred' and value['retained_index_at_denial'] is not None:
                    oracle.denial(value, self.prefix)
                    require(value['limits']['data_bytes'] == APPLICATION_PAYLOAD_BYTES, 'capacity record differs from declared payload')
                    require(not any(v['wait_id'] == value['wait_id'] and v['transition'] == 'admitted' for v in current),
                            'held denial already admitted before paused capture')
                    return value
            return None
        self.capacity_denial = self.wait(denied, 'actual Index-held capacity refusal', 65, entry, self.client)
        save(output/'capacity-denial.json', self.capacity_denial)
        started = time.monotonic_ns()
        self.capacity_pending, _ = self.capture(output, 'capacity-pending')
        # Only one full capture while paused. Preserve its raw state even if a
        # subsequent oracle or the existing query deadline refuses the case.
        self.capacity_shape = oracle.pending(self.prefix, self.capacity_pending, self.capacity_denial, receipt['job_id'])
        view = self.current_owner_view(self.capacity_pending, dict(
            control=load(output/'before-control.json'), resource=dict(catalog_id=self.ids['catalog'])))
        if self.capacity_shape['job_state'] == 'queued':
            from installed_sync import validate_queued
            validate_queued(view, submitted, expected_principal=self.enrolled['principal_id'])
        else:
            from initial_jobs import validate_job
            validate_job(view, receipt, expected_principal=self.enrolled['principal_id'])
        self.capacity_inputs = self.files(self.state/'data/ingest/producer-inputs-v1')
        require(len(self.capacity_inputs) == self.capacity_shape['source_batches'], 'pending original input inventory')
        save(output/'capacity-pending-inputs.json', self.capacity_inputs)
        backend, = json.loads(self.sql('bigbrotr_control', BACKEND))
        owner = index.only(self.capacity_pending['owner_epochs'], role='control', state='active')
        index.exact([backend[k] for k in ('resource_id','epoch','incarnation','role_instance_id','pid','started')],
            [owner['resource_id'][2:],str(owner['epoch']),owner['process_incarnation'][2:],
             owner['role_instance_id'][2:],owner['control_backend_pid'],owner['control_backend_start']])
        require(owner['acquired_at_us'] <= backend['observed_at_us'] < owner['lease_expires_at_us'], 'capacity Control lease expired')
        save(output/'capacity-live-backend.json', backend)
        index.exact(self.custody(), self.custody_before)
        require(self.inspect(entry)['State']['Running'], 'capacity owner exited')
        info = self.inspect(self.ch_entry)
        require(info['State']['Running'] and info['State']['Paused'], 'held engine unexpectedly resumed')
        save(output/'capacity-pending-verdict.json', dict(status='PASS', **self.capacity_shape,
            capture_started_ns=started,capture_finished_ns=time.monotonic_ns(),control_tables=82))
        self.unpause_clickhouse()
        def resumed():
            current = self.capacity_records(entry, output)
            found = [v for v in current if v['wait_id'] == self.capacity_denial['wait_id'] and v['transition'] == 'admitted']
            if not found:
                return None
            value, = found
            oracle.admitted(self.capacity_denial, value)
            return value
        self.capacity_admission = self.wait(resumed, 'same capacity wait admission after actual Index join', 65, entry, self.client)
        save(output/'capacity-admission.json', self.capacity_admission)
        Runtime.initial_archive_observation(self, entry, output)

    def initial_publication(self, entry, output):
        IndexRuntime.initial_publication(self, entry, output)
        self.capacity_final_live = load(output/'roles-after-control.json')
        oracle.preserved(self.capacity_pending, self.capacity_final_live)
        current = self.files(self.state/'data/ingest/producer-inputs-v1')
        require(all(current.get(k) == v for k,v in self.capacity_inputs.items()), 'accepted original inputs changed')
        index.exact(self.custody(), self.custody_before)

    def verify_index(self):
        IndexRuntime.verify_index(self)
        after = load(self.root/'index/after-control.json')
        observed = int(self.sql('bigbrotr_control','SELECT (EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint'))
        terminal.terminal_preservation(self.capacity_final_live, after, observed, archive_renewal=True)
        self.science_checks.publication(after, load(self.root/'index/published-science.json'), self.index_science,
            revision=4, snapshot=5, topology=0, cuts=[(1,12)])
        current = records((self.root/'initial/serve.stdout').read_text())
        index.exact(current[:len(self.capacity_seen)], self.capacity_seen)
        index.exact(index.only(current, wait_id=self.capacity_denial['wait_id'], transition='deferred'), self.capacity_denial)
        index.exact(index.only(current, wait_id=self.capacity_denial['wait_id'], transition='admitted'), self.capacity_admission)
        index.exact(self.custody(), self.custody_before)
        verdict = dict(format='bigbrotr-installed-shared-budget/1',status='PASS',
            wait_id=self.capacity_denial['wait_id'],**self.capacity_shape,
            final_source_through=12,final_archive_revision=4,final_index_revision=4,
            control_tables=82,local_index_join='positively_consumed',remote_exclusion='NOT_RUN',
            generic_capacity='NOT_RUN',readiness='positive_initial_publication',
            application_payload=application_payload())
        save(self.root/'index/shared-budget-verdict.json', verdict)
        self.inputs['shared_budget'] = verdict
