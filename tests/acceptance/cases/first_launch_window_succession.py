"""Installed finite traversal across populated islands and an empty window."""
import shutil
import time

from cases.first_launch import Runtime, load, save, require, digest
from cases.first_launch_linear_quantum import LinearQuantumRuntime
from cases.first_launch_quantum_science import initial_science
from cases import first_launch_quantum_oracle as quantum
from cases import first_launch_restart as source
from cases import first_launch_retained_native as native


# Deliberately independent of the relay's source recipe.
SELECTOR = dict(version=1, filters=[dict(kinds=[1], until=259199)],
                history_since=0, ephemeral='retain')
ENTRIES = (('request', 0, 86400, 86400), ('request', 0, 2, 86400),
           ('request', 0, 1, 86400), ('request', 86400, 172800, 172800),
           ('request', 172800, 259200, 259200), ('request', 172800, 172802, 259200),
           ('request', 172800, 172802, 259200), ('request', 172800, 172801, 259200))
PAGES = ((0,), (0,), (), (), (1,), (2,), (2,), ())
EVENT_IDS = ('8a0096a3a84f1460c11a432355666cb9fd377cca00a5a1e08ef0071c9c0b9849',
             'fac8ef0b0353caa848edc035806ccdb65f1ff1d17e78535126a86796bcc09e2e',
             'a6d21c3e069f3f776259ef542ca4022054924a3f6d89ac5a9254101e2044f003')
SUMMARY = dict(queries_spent=8, attempted_prefix=259200, gap_count=2,
               pending_work='settled', pending_interval=None)
LIMITS = [
    'Eight real requests traverse three fixed windows. The middle window and both older remainders are actually queried empty; a new canonical ID at the same second must permit another overlap before the repeated-ID gap.',
    'The independent transcript, qualified gaps, contiguous attempted prefix, all six producer inputs/receipts and full native rows are checked after the original owner joins. The relay stays available to reject a ninth request.',
    'This same-owner finite profile does not qualify full500-event pages, partitions, sparse growth, further slices, adoption, physical capacity or Archive publication/reads. Gap/COMMIT fault schedules require separate connected evidence.',
]


def check_wires(wires, events):
    assert tuple(event['id'] for event in events) == EVENT_IDS
    assert [event['created_at'] for event in events] == [1, 172801, 172801]
    assert len(wires) == 8
    nonces, previous = set(), None
    import re
    for number, (wire, entry, page) in enumerate(zip(wires, ENTRIES, PAGES), 1):
        _, lower, upper, _ = entry
        values = [events[index] for index in page]
        request = wire['request']
        ident = request[1]
        assert wire['number'] == number
        assert request == ['REQ', ident, dict(kinds=[1], since=lower, until=upper - 1, limit=3)]
        assert re.fullmatch('[0-9a-f]{48}', ident) and int(ident[:32], 16) and ident[:32] not in nonces
        nonces.add(ident[:32])
        assert wire['frames'] == ['signed'] * len(values) + ['EOSE']
        assert wire['event_ids'] == [event['id'] for event in values]
        assert wire['event_json_hex'] == [quantum.wire_json(event).hex() for event in values]
        assert wire['completion'] == 'request-and-websocket-close'
        assert wire['connected_at_us'] <= wire['request_at_us'] <= wire['sent_at_us'] <= wire['closed_at_us']
        assert wire['closed_at_us'] - wire['request_at_us'] < 3_000_000
        assert previous is None or previous <= wire['connected_at_us']
        assert type(wire['clock_resolution_us']) is int and 0 < wire['clock_resolution_us'] <= 1_000_000
        previous = wire['closed_at_us']


def check_history(state, wires, events, receipts):
    refs = lambda number: quantum.receipt_refs(receipts[number - 1])
    declared = {1: [(events[0], 2, 4, refs(4))], 2: [(events[0], 5, 0, refs(4))],
                3: [], 4: [], 5: [(events[1], 2, 5, refs(5))],
                6: [(events[2], 2, 6, refs(6))], 7: [(events[2], 5, 0, refs(6))], 8: []}
    assert quantum.journey(state, wires, 1, declared, selector=SELECTOR, entries=ENTRIES) == SUMMARY


def check_progress(state, report, wires, events, science, completed):
    import installed_progress_replay as typed
    from installed_progress_context_oracle import current_snapshot
    ds = source.raw(state['datasets'][0]['dataset_id'])
    domains = {row['dictionary_kind']: source.raw(row['allocation_domain']) for row in state['allocation_domains']}
    ref = lambda kind, ordinal=1: [ds, kind, domains[kind], ordinal]
    owner = source.only(state['owner_epochs'], role='control')
    common = dict(dataset_id='\\x' + ds.hex(), producer_slot_id='\\x' + report['producer_slot_id'], producer_epoch=1)
    job, detail, _ = source.attribution(state, 'b1')
    actual = source.only(state['acquisition_progress'], job_id=job['job_id'])
    wire, event = wires[6], events[2]
    progress = dict(common, job_id=job['job_id'], scope_id=detail['scope_id'], progress_revision=5,
        request_id='\\x' + wire['request'][1].encode().hex(), event_ordinal=1, wire_sequence=1,
        input_sha256='\\x' + source.sha(quantum.wire_json(event)), classification=5, mutation_request=None,
        classified_events=5, successful_events=5, rejected_events=0, observed_at_us=actual['observed_at_us'],
        clock_resolution_us=wire['clock_resolution_us'], clock_anomaly_flags=4 if wire['clock_resolution_us'] > 1 else 0,
        xmin=actual['xmin'])
    source.clock(progress['observed_at_us'], wire['sent_at_us'], wire['closed_at_us'])
    progress['intent_digest'] = '\\x' + typed.certification_digest(event, ref(4, 3), ref(2), ref(2),
        [ref(1), ref(3), ref(5)], progress, wire).hex()
    values = {(cf, key.hex()): value.hex() for cf, key, value in science['native']}
    dependencies = []
    for number in range(1, 23):
        first = 1 if number <= 2 else 2 if number == 3 else 3 if number <= 7 else 4 + (number - 8) // 5
        key = b'\x01\x20' + ds + bytes(4) + source.be(1) + source.be(number)
        value = bytes.fromhex(values['facts', key.hex()])
        dependencies.append(dict(common, source_shard=0, source_epoch=1, source_sequence=number,
            payload_digest='\\x' + value[2:34].hex(),
            xmin=source.only(state['producer_outcomes'], request_sequence=first)['xmin']))
    head, = state['producer_checkpoints']
    source.clock(head['committed_at_us'], wire['sent_at_us'], completed)
    assert head['committed_at_us'] < owner['lease_expires_at_us']
    expected = dict(common, progress_format=1, manifest_sha256='\\x' + source.sha(science['manifest']),
        checkpoint_revision=8, complete_through_request=6, progress_jobs=1, dependency_count=22,
        issuer_resource=owner['resource_id'], issuer_epoch=owner['epoch'], issuer_incarnation=owner['process_incarnation'],
        committed_at_us=head['committed_at_us'], xmin=head['xmin'], checkpoint_digest='\\x' + '00' * 32)
    expected['checkpoint_digest'] = '\\x' + typed.head_digest(expected, [progress], dependencies,
        lambda row: typed.native_reference(row, values)).hex()
    current_snapshot(head, state['acquisition_progress'], sorted(state['producer_checkpoint_dependencies'],
        key=lambda row: (row['source_shard'], row['source_epoch'], row['source_sequence'])),
        expected, [progress], dependencies, values, prefix_profile=True)


class WindowSuccessionRuntime(LinearQuantumRuntime):
    def candidate(self):
        super().candidate()
        self.inputs['linear_quantum_scenario'] = 'window-succession'
        for name in ('experiments/ingest-bootstrap/linear_window_transport.py',
                     'experiments/ingest-bootstrap/linear_capped_transport.py',
                     'tests/acceptance/cases/first_launch_quantum_science.py',
                     'tests/acceptance/cases/first_launch_window_succession.py'):
            self.inputs['acceptance_files_sha256'][name] = digest(self.source / name)
        self.inputs['window_corpus_sha256'] = digest(self.source / 'tests/fixtures/acquisition/linear-window-events.json')
        require(self.inputs['window_corpus_sha256'] ==
            'b6ae647fbaa641c8bb2c4ae7dbcf12987a8b3bd01cd14eecbf15476c344111cc', 'window corpus differs')

    def load_helpers(self):
        super().load_helpers()
        from installed_sync import validate_source
        from cases.first_launch_tutorial import owned_listeners, loopback_listener
        import linear_window_transport
        self.owned_listeners, self.loopback_listener = owned_listeners, loopback_listener
        self.validate_source = lambda state, report, commands: validate_source(
            state, report, commands, selector_document=SELECTOR)

    def image_and_state(self):
        Runtime.image_and_state(self)
        from linear_window_transport import PROFILE, corpus
        shutil.copyfile(self.source / 'tests/fixtures/acquisition/linear-window-events.json',
                        self.home / 'linear-window-events.json')
        save(self.home / 'linear-window-profile.json', PROFILE)
        self.events = corpus(self.home)

    def source_unchanged(self):
        super().source_unchanged()
        for path in (self.source / 'tests/fixtures/acquisition/linear-window-events.json',
                     self.home / 'linear-window-events.json'):
            require(digest(path) == self.inputs['window_corpus_sha256'], 'window source changed')

    def initial_launch(self):
        return self.launch('initial', 'app', 'serve', '--file', '/etc/bigbrotr/instance.toml',
                           '--roles', 'control,ingest,sync', '--initialize-ingest')

    def initial_archive_observation(self, entry, output):
        def completed():
            require(not (self.home / 'relay-error.json').exists(), 'window relay failed')
            if not (self.home / 'linear-window-peer-complete.json').exists():
                return False
            return self.sql('bigbrotr_control', "SELECT count(*)=8 AND bool_and(entry_kind='request' "
                            "AND terminal_kind IS NOT NULL) FROM control.acquisition_quanta") == 't'
        self.wait(completed, 'eight settled window requests', 30, entry, self.client)
        self.wires(8)

    def initial_publication(self, entry, output):
        completion = load(self.home / 'linear-window-peer-complete.json')
        require(completion['requests'] == 8 and completion['listener_closed'] is False
                and completion['listening'] is True, 'window relay disappeared before owner stop')
        require(self.client.poll() is None, 'window client exited before stop')
        listeners = self.owned_listeners(self.client.pid)
        self.loopback_listener(listeners, 7447)
        save(output / 'window-live-listener.json', dict(completion=completion, process=listeners))

    def initial_source_quiet(self):
        require(not (self.home / 'relay-error.json').exists(), 'window relay failed before stop')
        require(not (self.home / 'relay-open-009.json').exists(), 'settled windows opened a ninth socket')
        return self.sql('bigbrotr_control', "SELECT count(*)=8 AND bool_and(disposition IS NOT NULL) "
                        "FROM control.acquisition_quanta") == 't'

    def verify_linear_quantum(self):
        self.source_unchanged()
        output = self.root / 'linear-quantum'
        output.mkdir(mode=0o700)
        state, _ = self.capture(output, 'after')
        self.backend_absence(output)
        view = self.source_view(state)
        wires = self.wires(8)
        check_wires(wires, self.events)
        joined = load(self.home / 'linear-window-peer-joined.json')
        require(joined['requests'] == 8 and joined['listener_closed'] is True and joined['errors'] == 0,
                'window peer did not join')
        from installed_sync import validate_source, validate_queued
        commands = load(self.home / 'submitted.json')
        require(len(commands) == 4 and commands[-1]['input']['deadline_us'] == '120000000',
                'window source declaration differs')
        validate_source(view, self.initialized, commands[:3], selector_document=SELECTOR)
        validate_queued(view, commands, expected_principal=self.enrolled['principal_id'])
        dump = native.census(self, output)
        inputs = source.originals(self.state / 'data/ingest', self.initialized['producer_slot_id'], 1, 6)
        completed = time.time_ns() // 1000
        job, detail, attribution = source.attribution(view, 'b1')
        science = initial_science(view, self.initialized, self.events, wires, dump, inputs, completed,
            mutations=((0, 4, 0, 8), (1, 5, 4, 13), (2, 6, 5, 18)),
            first_accepted_at_us=job['created_at_us'], native_sequence=103)
        source.requests(view, 1, science['manifest'], science['inputs'], science['receipts'],
            source.only(view['owner_epochs'], role='control'),
            {4: source.request_binding(self.events[0], wires[0], 1),
             5: source.request_binding(self.events[1], wires[4], 1),
             6: source.request_binding(self.events[2], wires[5], 1)}, attribution)
        check_history(view, wires, self.events, science['receipts'])
        check_progress(view, self.initialized, wires, self.events, science, completed)
        observed, = load(self.home / 'linear-status-observations.json')
        from installed_sync_client import quantum_status
        status = quantum_status(observed, detail['scope_id'][2:],
            str(job['created_at_us']), known_plan=True, selector=SELECTOR,
            expected_entries=dict(enumerate(ENTRIES, 1)))
        require(status == dict(format='bigbrotr-linear-quantum-status/2', quantum_number='8',
            scope_id=detail['scope_id'][2:], lower='172800', upper='172801', horizon='259200',
            result='linear_observed', terminal='eose', gap=None, continuation_upper='259200',
            retained_events=0, classified_prefix=0, coverage='pending', entry_kind='request',
            disposition='window_closed', window_end='259200', window_width='86400', query_budget=128,
            queries_spent=8, attempted_prefix='259200', gap_count=2, pending_work='settled', pending_interval=None),
            'window actor final summary differs')
        save(output / 'verdict.json', dict(status='PASS', scenario='window-succession', requests=8,
            history_entries=8, pending_plans=0, mutation_requests=6, native_rows=len(science['native']),
            native_sequence=103, attempted_prefix=259200, saturated_gaps=[[1, 2], [172801, 172802]],
            new_same_second_id_continued=True, empty_middle_window_queried=True, coverage='pending'))
