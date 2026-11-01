"""Independent controls for partial window obligations; no engines or builds."""
import copy
import importlib.util
import io
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import textwrap
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch
from contextlib import redirect_stderr, redirect_stdout

from test_linear_window_succession import fixture, ACCEPTED, COMPLETED, ENTRIES, SELECTOR
from test_first_launch import ROOT
from test_first_launch_restart import current42_head
from cases import first_launch as base
from cases import first_launch_linear_quantum as linear
from cases import first_launch_window_succession as window
from cases import first_launch_quantum_oracle as quantum
from cases import first_launch_restart as source
from installed_sync_oracle import expected
import installed_progress_replay as typed


# SQL0043 pending plans reserve their full bound and retain no settlement fields.
# This list is deliberately independent of the verifier's own pending-field list.
PENDING_FIELDS = ('terminal_kind', 'terminal_frame', 'request_id', 'terminal_sequence',
    'terminal_received_at_us', 'completed_at_us', 'checkpoint_sha256', 'teardown_complete',
    'retained_events', 'classified_prefix', 'effect_records', 'dependencies', 'checkpoint_after',
    'result_class', 'gap_reason', 'continuation_upper', 'disposition', 'gap_lower', 'gap_upper')
FAULTS = ('gap-write', 'terminal-before', 'terminal-after')
SUMMARIES = {
    'gap-write': dict(queries_spent=2, attempted_prefix=0, gap_count=0,
        pending_work='request_pending', pending_interval=['0', '2']),
    'terminal-before': dict(queries_spent=3, attempted_prefix=0, gap_count=1,
        pending_work='request_pending', pending_interval=['0', '1']),
    'terminal-after': dict(queries_spent=3, attempted_prefix=86400, gap_count=1,
        pending_work='query_ready', pending_interval=['86400', '172800']),
}


def partial_fixture(root, fault):
    state, report, events, wires, _, _, _ = fixture(root)
    count = 2 if fault == 'gap-write' else 3
    wires = wires[:count]
    state['acquisition_quanta'] = state['acquisition_quanta'][:count]
    if fault != 'terminal-after':
        row = state['acquisition_quanta'][-1]
        row.update(dict.fromkeys(PENDING_FIELDS))
        assert row['maximum_dependencies'] == 192
        row['charged_bytes'] = 1024 + len(quantum.raw(row['request_predicate'])) + 2097152 + 3072 + 256 * 192
    # Re-encode the literal first body, rather than accepting a trimmed dump's
    # observed counters or native metadata. Setup1..3 and body4 are the only inputs.
    wire = wires[0]
    rows, inputs, receipts = expected(state, report, events[0], 300_001_000_000,
        wire['sent_at_us'] + 10, wire['sent_at_us'] + 50, 1, acknowledge=True)
    dump = dict(event='dump-state', sequence=67,
        rows=[dict(cf=cf, key=key.hex(), value=value.hex()) for cf, key, value in rows])
    job, detail = state['jobs'][0], state['acquisition_jobs'][0]
    ds = source.raw(job['dataset_id'])
    domains = {row['dictionary_kind']: source.raw(row['allocation_domain']) for row in state['allocation_domains']}
    ref = lambda kind, ordinal=1: [ds, kind, domains[kind], ordinal]
    state['acquisition_scopes'] = [dict(scope_id=detail['scope_id'], endpoint_id='\\x' + '71' * 16, access_scope_id='\\x' + '72' * 16)]
    state['relay_enrollments'] = [dict(endpoint_id='\\x' + '71' * 16, access_scope_id='\\x' + '72' * 16, route_id='\\x' + '73' * 16)]
    state['producer_outcomes'] = [dict(request_sequence=n, xmin=str(100 + n)) for n in range(1, 5)]
    common = dict(dataset_id=job['dataset_id'], producer_slot_id='\\x' + report['producer_slot_id'], producer_epoch=1)
    wire = wires[1]
    progress = dict(common, job_id=job['job_id'], scope_id=detail['scope_id'], progress_revision=2,
        request_id='\\x' + wire['request'][1].encode().hex(), event_ordinal=1, wire_sequence=1,
        input_sha256='\\x' + source.sha(quantum.wire_json(events[0])), classification=5, mutation_request=None,
        classified_events=2, successful_events=2, rejected_events=0, observed_at_us=wire['sent_at_us'] + 20,
        clock_resolution_us=1, clock_anomaly_flags=0, xmin='105')
    # Scope dictionary ordinal3 is independent of event ordinal1 in this prefix.
    progress['intent_digest'] = '\\x' + typed.certification_digest(events[0], ref(4, 3), ref(2), ref(2),
        [ref(1), ref(3), ref(5)], progress, wire).hex()
    values = {(cf, key.hex()): value.hex() for cf, key, value in rows}
    dependencies = []
    for number in range(1, 13):
        first = 1 if number <= 2 else 2 if number == 3 else 3 if number <= 7 else 4
        key = b'\x01\x20' + ds + bytes(4) + source.be(1) + source.be(number)
        dependencies.append(dict(common, source_shard=0, source_epoch=1, source_sequence=number,
            payload_digest='\\x' + bytes.fromhex(values['facts', key.hex()])[2:34].hex(), xmin=str(100 + first)))
    owner = source.only(state['owner_epochs'], role='control')
    manifest = next(value for cf, key, value in rows if cf == 'default' and key == b'\x01\x01')
    head = dict(common, progress_format=1, manifest_sha256='\\x' + source.sha(manifest),
        checkpoint_revision=5, complete_through_request=4, progress_jobs=1, dependency_count=12,
        issuer_resource=owner['resource_id'], issuer_epoch=owner['epoch'], issuer_incarnation=owner['process_incarnation'],
        committed_at_us=wire['closed_at_us'] + 10, xmin='105', checkpoint_digest='\\x' + '00' * 32)
    head['checkpoint_digest'] = '\\x' + typed.head_digest(head, [progress], dependencies,
        lambda row: typed.native_reference(row, values)).hex()
    state.update(producer_checkpoints=[current42_head(head, initial=True)], acquisition_progress=[progress],
        producer_checkpoint_dependencies=dependencies)
    return state, report, events, wires, dump, inputs, receipts


def diagnostic(fault, **changes):
    value = dict(format='bigbrotr-initial-sync-diagnostic/1', scope='caller-progress-not-commit-outcome',
        timing='monotonic-offset-microseconds-saturating', stage='quantum-terminal', reason='operation-error',
        frame_ordinal=None, request_sequence=None, last_delivered_admission_sequence=4,
        entered_us=10, returned_us=20, dispatch_deadline_us=30, captured_us=21)
    value.update(changes)
    error = 'Backend' if fault == 'gap-write' else 'CommitUnknown'
    return 'error: initial Sync unavailable: Quantum(' + error + '); sync_dispatch=' + json.dumps(value) + '\n'


class WindowFaultControls(unittest.TestCase):
    def test_actual_partial_science_and_fault_prefix_head(self):
        for fault in FAULTS:
            with self.subTest(fault=fault), tempfile.TemporaryDirectory() as temporary:
                state, report, events, wires, dump, inputs, receipts = partial_fixture(Path(temporary), fault)
                science = window.initial_science(state, report, events, wires, dump, inputs, COMPLETED,
                    mutations=((0, 4, 0, 8),), first_accepted_at_us=ACCEPTED, native_sequence=67)
                self.assertEqual((len(inputs), len(receipts)), (4, 4))
                window.check_progress(state, report, wires, events, science, COMPLETED, fault_prefix=True)
                refs = quantum.receipt_refs(receipts[3])
                self.assertEqual(quantum.journey(state, wires, 1,
                    {1: [(events[0], 2, 4, refs)], 2: [(events[0], 5, 0, refs)]},
                    selector=SELECTOR, entries=ENTRIES[:len(wires)]), SUMMARIES[fault])
                window.check_wires(wires, events, count=len(wires))
                altered = copy.deepcopy(state)
                ds = source.raw(state['jobs'][0]['dataset_id'])
                domains = {row['dictionary_kind']: source.raw(row['allocation_domain']) for row in state['allocation_domains']}
                ref = lambda kind, ordinal=1: [ds, kind, domains[kind], ordinal]
                wrong_scope = altered['acquisition_progress'][0]
                wrong_scope['intent_digest'] = '\\x' + typed.certification_digest(events[0], ref(4, 1), ref(2), ref(2),
                    [ref(1), ref(3), ref(5)], wrong_scope, wires[1]).hex()
                with self.assertRaises(AssertionError):
                    window.check_progress(altered, report, wires, events, science, COMPLETED, fault_prefix=True)
                for field, value in (('checkpoint_revision', 4), ('complete_through_request', 3), ('dependency_count', 11)):
                    altered = copy.deepcopy(state)
                    altered['producer_checkpoints'][0][field] = value
                    with self.subTest(field=field), self.assertRaises(AssertionError):
                        window.check_progress(altered, report, wires, events, science, COMPLETED, fault_prefix=True)

    def test_pending_plan_clears_every_settlement_field_and_keeps_reservation(self):
        with tempfile.TemporaryDirectory() as temporary:
            state, _, _, _, _, _, _ = partial_fixture(Path(temporary), 'gap-write')
            row = state['acquisition_quanta'][-1]
            self.assertTrue(all(row[field] is None for field in PENDING_FIELDS))
            quantum.shape(state)
            for field, value in (('charged_bytes', 1024), ('dependencies', '\\x'),
                    ('checkpoint_sha256', '\\x' + '00' * 32), ('disposition', 'saturated_second'),
                    ('terminal_frame', '\\x'), ('checkpoint_after', 5)):
                altered = copy.deepcopy(state)
                altered['acquisition_quanta'][-1][field] = value
                with self.subTest(field=field), self.assertRaises(AssertionError):
                    quantum.shape(altered)

    def test_installed_fault_wrapper_requires_the_exact_partial_summary(self):
        for fault in FAULTS:
            with self.subTest(fault=fault), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                home = root / 'home'
                home.mkdir()
                state, report, events, wires, dump, inputs, _ = partial_fixture(home, fault)
                runtime = window.WindowSuccessionRuntime(SimpleNamespace(evidence=root, source_root=ROOT), {})
                runtime.home, runtime.events, runtime.initialized = home, events, report
                runtime.quantum_fault, runtime.enrolled = fault, dict(principal_id='fixture')
                runtime.source_unchanged = Mock()
                runtime.capture = Mock(return_value=(state, None))
                runtime.backend_absence, runtime.source_view = Mock(), Mock(return_value=state)
                runtime.wires, runtime.historical_quantum_restart = Mock(return_value=wires), Mock()
                runtime.gap_plans_before = copy.deepcopy(state['acquisition_quanta'])
                (home / 'submitted.json').write_text(json.dumps([{}, {}, {}, dict(input=dict(deadline_us='120000000'))]))
                ordinal = 2 if fault == 'gap-write' else 3
                (root / 'quantum-proxy-terminal.json').write_text(json.dumps(dict(commit_ordinal=ordinal,
                    matched_commits=ordinal, skipped_commits=ordinal - 1, selected_commits=1,
                    faults=0 if fault == 'gap-write' else 1, gap_write_rejections=1 if fault == 'gap-write' else 0,
                    selected_server_complete=0 if fault == 'terminal-before' else 1,
                    selected_client_complete=1 if fault == 'gap-write' else 0)))
                (root / 'initial').mkdir()
                (root / 'initial/serve.stderr').write_text(diagnostic(fault))
                if fault == 'gap-write':
                    runtime.quantum_proxy_binding = dict(commit_ordinal=2)
                    rejected = dict(event='quantum-gap-write-rejected', classification='fixture-gap-write-refusal',
                        sqlstate='23514', constraint='fixture_gap_write_refusal', backend_pid=321, commit_ordinal=2)
                    (root / 'quantum-proxy.json').write_text(json.dumps(dict(stdout=json.dumps(rejected)+'\n')))
                    (root / 'initial/held-plan-reply.json').write_text(json.dumps(dict(backend_pid=321)))
                with patch('installed_sync.validate_source'), patch('installed_sync.validate_queued'), \
                        patch.object(window.native, 'census', return_value=dump) as census, \
                        patch.object(source, 'originals', return_value=inputs), patch.object(source, 'requests'):
                    runtime.verify_window_fault()
                verdict = json.loads((root / 'linear-quantum/verdict.json').read_text())
                self.assertEqual(verdict['summary'], SUMMARIES[fault])
                self.assertEqual((verdict['mutation_requests'], verdict['native_sequence']), (4, 67))
                census.assert_called_once_with(runtime, root / 'linear-quantum', expected_writer_exit=4)
                runtime.historical_quantum_restart.assert_called_once_with(state, root / 'linear-quantum')

    def test_gap_refusal_requires_one_correlated_literal_constraint_error(self):
        rejected = dict(event='quantum-gap-write-rejected', classification='fixture-gap-write-refusal',
            sqlstate='23514', constraint='fixture_gap_write_refusal', backend_pid=321)
        terminal, held = dict(gap_write_rejections=1), dict(backend_pid=321)
        self.assertEqual(window.gap_write_rejection(terminal, [rejected], held), rejected)
        for count in (0, 2, True):
            with self.subTest(count=count), self.assertRaises(ValueError):
                window.gap_write_rejection(dict(gap_write_rejections=count), [rejected], held)
        for events in ([], [rejected, rejected], [dict(rejected, event='other')]):
            with self.subTest(events=events), self.assertRaises(ValueError):
                window.gap_write_rejection(terminal, events, held)
        for fields in (dict(sqlstate='08006'), dict(constraint='unrelated_constraint'),
                dict(classification='unknown'), dict(backend_pid=322), dict(backend_pid=True), dict(backend_pid=0)):
            with self.subTest(fields=fields), self.assertRaises(ValueError):
                window.gap_write_rejection(terminal, [dict(rejected, **fields)], held)

    def test_diagnostic_requires_exact_error_stage_delivery_and_non_timeout_clock(self):
        for fault in FAULTS:
            text = diagnostic(fault)
            self.assertEqual(window.window_fault_diagnostic(text, fault)['returned_us'], 20)
            for changes in (dict(stage='relay-collection'), dict(reason='deadline'),
                    dict(last_delivered_admission_sequence=3), dict(frame_ordinal=1), dict(request_sequence=4),
                    dict(entered_us=-1), dict(entered_us=21), dict(returned_us=30), dict(returned_us=31),
                    dict(returned_us=True), dict(captured_us=19), dict(dispatch_deadline_us=True),
                    dict(scope='commit-outcome'), dict(timing='wall-clock')):
                with self.subTest(fault=fault, changes=changes), self.assertRaises(AssertionError):
                    window.window_fault_diagnostic(diagnostic(fault, **changes), fault)
            wrong_error = 'CommitUnknown' if fault == 'gap-write' else 'Backend'
            for changed in (text.replace('Backend' if fault == 'gap-write' else 'CommitUnknown', wrong_error),
                    'error: generic failure\n', text.replace('Quantum(', 'Timeout('), text.rstrip(), text + 'extra\n'):
                with self.subTest(fault=fault, text=changed), self.assertRaises(AssertionError):
                    window.window_fault_diagnostic(changed, fault)

    def test_fault_route_declares_second_plan_or_third_terminal_commit(self):
        for fault, ordinal, mode, target in (('gap-write', 2, 'hold-reply', 'plan'),
                ('terminal-before', 3, 'before-forward', 'terminal'), ('terminal-after', 3, 'after-complete', 'terminal')):
            with self.subTest(fault=fault), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                runtime = window.WindowSuccessionRuntime(SimpleNamespace(evidence=root, source_root=ROOT,
                    linear_quantum_fault=fault), dict(acceptance_files_sha256={}))
                runtime.home = root / 'home'
                runtime.home.mkdir()
                runtime.config = root / 'instance.toml'
                runtime.config.write_text('[postgres.control]\nendpoint = "127.0.0.1:15432"\n')
                runtime.set_value = Mock(return_value='[postgres.control]\nendpoint = "127.0.0.1:34567"\n')
                runtime.proxy_process = Mock(return_value=SimpleNamespace(process=SimpleNamespace(pid=123)))
                runtime.proxy_event = lambda *args: dict(runtime.quantum_proxy_binding, event='ready', port=34567)
                with patch.object(base.Runtime, 'bootstrap'):
                    runtime.bootstrap()
                command = runtime.proxy_process.call_args.args[0]
                self.assertEqual(command[3:], ['--mode', mode, '--target', 'control-quantum-' + target,
                    '--upstream-port', '15432', '--commit-ordinal', str(ordinal), '--profile', 'retained-catch-up'])
                binding = runtime.inputs['quantum_proxy']
                self.assertEqual((binding['commit_ordinal'], binding['wire_limit'], binding['lifetime_seconds']),
                    (ordinal, 67108864, 120))
                self.assertEqual(linear.LinearQuantumRuntime.quantum_proxy_ordinal(runtime), 1)

    def test_fault_join_requires_live_owned_listener_and_exact_closed_transcript(self):
        for fault in FAULTS:
            with self.subTest(fault=fault), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                runtime = window.WindowSuccessionRuntime(SimpleNamespace(evidence=root, source_root=ROOT), {})
                runtime.quantum_fault = fault
                runtime.client = SimpleNamespace(pid=123, poll=Mock(return_value=None))
                runtime.owned_listeners = Mock(return_value=[dict(pid=123, port=7447)])
                runtime.loopback_listener, runtime.wires = Mock(), Mock()
                runtime.fault_owner_joined(root)
                runtime.owned_listeners.assert_called_once_with(123)
                runtime.loopback_listener.assert_called_once_with(runtime.owned_listeners.return_value, 7447)
                runtime.wires.assert_called_once_with(2 if fault == 'gap-write' else 3)
                runtime.client.poll.return_value = 0
                with self.assertRaisesRegex(ValueError, 'peer exited'):
                    runtime.fault_owner_joined(root)
                runtime.client.poll.return_value = None
                runtime.loopback_listener.side_effect = ValueError('missing live listener')
                with self.assertRaisesRegex(ValueError, 'missing live listener'):
                    runtime.fault_owner_joined(root)

    def test_cli_allows_only_declared_window_fault_combinations(self):
        spec = importlib.util.spec_from_file_location('window_fault_cli_controls', ROOT / 'tests/acceptance/run.py')
        runner = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(runner)
        vectors = [(scenario, fault, scenario == 'interruptions' or scenario == 'window-succession' and fault in FAULTS)
            for scenario in ('interruptions', 'capped-continuation', 'window-succession')
            for fault in ('plan-before', 'plan-after', 'terminal-before', 'terminal-after', 'gap-write', 'stale-authority', 'empty-terminal-authority')]
        for scenario, fault, allowed in vectors:
            with self.subTest(scenario=scenario, fault=fault), tempfile.TemporaryDirectory() as temporary:
                argv = ['run.py', 'p03-linear-quantum', '--source-root', str(ROOT), '--evidence', str(Path(temporary) / 'out'),
                    '--linear-quantum-scenario', scenario, '--linear-quantum-fault', fault]
                def accepted(args, checks, inputs):
                    checks.append(dict(name='portable-dispatch-only', result='PASS'))
                with patch.object(sys, 'argv', argv), patch.object(base, 'first_launch', side_effect=accepted) as execute, \
                        redirect_stderr(io.StringIO()), redirect_stdout(io.StringIO()):
                    if allowed:
                        self.assertEqual(runner.main(), 0)
                        execute.assert_called_once()
                    else:
                        with self.assertRaises(SystemExit) as refusal:
                            runner.main()
                        self.assertEqual(refusal.exception.code, 2)
                        execute.assert_not_called()

    def test_status_poll_is_disabled_only_for_explicit_window_faults(self):
        from installed_archive_client import submitted_status_poll
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            status = Mock()
            for windows in (False, True):
                self.assertIs(submitted_status_poll(root, windows, status), status)
            fault_file = root / 'linear-quantum-fault.json'
            for fault in FAULTS:
                fault_file.write_text(json.dumps(fault))
                with self.subTest(fault=fault):
                    self.assertIsNone(submitted_status_poll(root, True, status))
                    self.assertIs(submitted_status_poll(root, False, status), status)
            for value in ('plan-before', 'stale-authority', None, {}, ['terminal-before']):
                fault_file.write_text(json.dumps(value))
                with self.subTest(value=value), self.assertRaises(AssertionError):
                    submitted_status_poll(root, True, status)
                self.assertIs(submitted_status_poll(root, False, status), status)
            fault_file.write_text('{')
            with self.assertRaises(json.JSONDecodeError):
                submitted_status_poll(root, True, status)
            status.assert_not_called()

    def test_cold_candidate_binds_progress_family_and_transitive_live_replay(self):
        script = textwrap.dedent('''
            import hashlib, json, sys
            from pathlib import Path
            from types import SimpleNamespace
            from unittest.mock import patch
            source, root = map(Path, sys.argv[1:3])
            sys.path.insert(0, str(source/'tests/acceptance'))
            from cases import first_launch as base
            from cases import first_launch_window_succession as window
            from cases import first_launch_capped_quantum as capped
            from cases import first_launch_linear_quantum as linear
            import candidate
            names = ('installed_progress_replay', 'installed_progress_context_oracle', 'installed_family_science', 'live_replay')
            assert all(name not in sys.modules for name in names), 'control requires cold module state'
            selected = dict(bundle_root=str(root/'bundle'), manifest_sha256='manifest', bundle_sha256='bundle',
                manifest=dict(artifact=dict(source={}), assembly=dict(revision='revision', tree='tree')))
            runtime_type = dict(window=window.WindowSuccessionRuntime, capped=capped.CappedQuantumRuntime, linear=linear.LinearQuantumRuntime)[sys.argv[3]]
            runtime = runtime_type(SimpleNamespace(evidence=root, source_root=source,
                candidate_manifest=root/'candidate.json', native_observer_build=root/'observer'), {})
            with patch.object(candidate, 'extract_candidate', return_value=selected), \\
                    patch.object(base, 'expected_tables', return_value=set()), \\
                    patch.object(window.native, 'artifact', return_value=({}, {})):
                runtime.candidate()
            closure = runtime.inputs['acceptance_files_sha256']
            for name in names:
                path = Path(sys.modules[name].__file__).resolve()
                assert closure[str(path.relative_to(source))] == hashlib.sha256(path.read_bytes()).hexdigest(), name
            print(json.dumps(sorted(str(Path(sys.modules[name].__file__).resolve().relative_to(source)) for name in names)))
        ''')
        for profile in ('window', 'capped', 'linear'):
            with self.subTest(profile=profile), tempfile.TemporaryDirectory() as temporary:
                result = subprocess.run([sys.executable, '-B', '-c', script, str(ROOT), temporary, profile],
                    capture_output=True, text=True, timeout=15)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(json.loads(result.stdout), [
                'experiments/ingest-bootstrap/installed_progress_context_oracle.py',
                'experiments/ingest-succession/installed_family_science.py',
                'experiments/ingest-succession/installed_progress_replay.py',
                'experiments/ingest-succession/live_replay.py'])


if __name__ == '__main__':
    unittest.main()
