"""Outage custody, old observation age and verdict controls; no engines."""
import copy
import fcntl
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import tomllib
import unittest
from unittest.mock import patch, Mock

from cases import first_launch as base
from cases import first_launch_archive_outage as case
from cases import first_launch_archive_outage_oracle as check
from cases import first_launch_index_oracle as index
from test_first_launch import arguments, ROOT
from test_first_launch_index import metadata_fixture


def observation(age):
    return dict(capabilities=[dict(capability='archive_publication', time=dict(age_us=str(age)))])


def preservation_fixture():
    """Literal row-transition fixture; scientific/typed-row validation is separate."""
    before = {name: [] for name in base.expected_tables(ROOT)}
    raw = lambda n: '\\x' + f'{n:032x}'
    dataset = raw(1)
    before['datasets'] = [dict(dataset_id=dataset)]
    for n, role in enumerate(('control', 'ingest', 'archive', 'index'), 2):
        before['owner_epochs'].append(dict(dataset_id=dataset, role=role, resource_id=raw(n),
            epoch=1, role_instance_id=raw(n+10), process_incarnation=raw(20), state='active',
            acquired_at_us=100, lease_expires_at_us=10000, released_at_us=None,
            exclusion_method=None, exclusion_digest=None, control_backend_pid=123, xmin='10'))
        before['role_instances'].append(dict(role=role, role_instance_id=raw(n+10),
            created_at_us=100, retired_at_us=None, xmin='10'))
    historical = dict(before['owner_epochs'][0], epoch=0, role_instance_id=raw(90),
                      state='released', released_at_us=90, xmin='9')
    before['owner_epochs'].append(historical)
    before['role_instances'].append(dict(role='control', role_instance_id=raw(90),
        created_at_us=10, retired_at_us=90, xmin='9'))
    before['producer_epochs'] = [dict(producer_epoch=1, reserved_through_request=2, state='open', xmin='12')]
    before['producer_checkpoints'] = [dict(dataset_id=dataset, producer_slot_id=raw(30),
        producer_epoch=1, progress_format=1, manifest_sha256='\\x'+'31'*32,
        issuer_resource=raw(2), issuer_epoch=1, issuer_incarnation=raw(20),
        complete_through_request=2, checkpoint_revision=2, xmin='12')]
    before['consumers'] = [dict(consumer_id=raw(n), consumer_role=role,
        current_revision=2, current_topology_generation=0, xmin='12')
        for n, role in ((4, 'archive'), (5, 'index'))]
    before['browser_sessions'] = [dict(dataset_id=dataset, session_id=raw(40),
        principal_id=raw(41), issued_at_us=1000, revoked_at_us=None, xmin='13')]
    # Two accepted commands already exist at enrollment. Only scope and job are new.
    for table in ('management_commands', 'management_audit', 'producer_requests',
                  'producer_outcomes', 'archive_attempts', 'projection_plans', 'index_frontier_plans',
                  'consumer_frontier_revisions', 'consumer_frontiers'):
        before[table] = [dict(original=n, xmin=str(20+n)) for n in (1, 2)]
    for table in ('projection_plan_chunks', 'projection_chunks', 'projection_chunk_bounds'):
        before[table] = [dict(original=n, xmin=str(30+n)) for n in range(20)]
    before['producer_checkpoint_dependencies'] = [dict(sequence=n, xmin='12') for n in (1, 2, 3)]
    live = copy.deepcopy(before)
    for table in ('acquisition_selectors', 'acquisition_scopes', 'acquisition_scope_commands',
                  'jobs', 'acquisition_jobs', 'job_admissions', 'acquisition_progress', 'archive_attempts'):
        live[table].append(dict(added=1, xmin='50'))
    for table in ('management_commands', 'management_audit', 'producer_requests', 'producer_outcomes',
                  'projection_plans', 'index_frontier_plans', 'consumer_frontier_revisions', 'consumer_frontiers'):
        live[table].extend(dict(added=n, xmin='50') for n in (3, 4))
    for table in ('projection_plan_chunks', 'projection_chunks', 'projection_chunk_bounds'):
        live[table].extend(dict(added=n, xmin='50') for n in range(20, 40))
    live['producer_checkpoint_dependencies'].extend(dict(sequence=n, xmin='50') for n in range(4, 13))
    for owner in live['owner_epochs']:
        if owner['state'] == 'active' and owner['role'] in ('control', 'ingest', 'index'):
            owner.update(lease_expires_at_us=20000, xmin='51')
    live['producer_epochs'][0].update(reserved_through_request=4, xmin='50')
    live['producer_checkpoints'][0].update(complete_through_request=4, checkpoint_revision=9, xmin='52')
    live['consumers'][1].update(current_revision=4, xmin='50')
    terminal = copy.deepcopy(live)
    owner = terminal['owner_epochs'][0]
    exclusion = (b'bigbrotr/control-exclusion/1\0' + bytes.fromhex(dataset[2:])
        + bytes.fromhex(raw(2)[2:]) + (1).to_bytes(8, 'big') + bytes.fromhex(raw(20)[2:])*2)
    owner.update(state='released', released_at_us=6000, exclusion_method='storage_fenced',
        exclusion_digest='\\x'+hashlib.sha256(exclusion).hexdigest(), xmin='70')
    # release_epoch and role retirement read separate SQL clocks in the same transaction.
    terminal['role_instances'][0].update(retired_at_us=6001, xmin='70')
    terminal['browser_sessions'][0].update(revoked_at_us=5000, xmin='60')
    terminal['management_audit'].append(dict(dataset_id=dataset, actor_id=raw(41),
        action='browser.session.revoke', outcome='accepted', config_revision=1, policy_revision=1,
        subject_id=raw(40), reason=None, requester_database_role=None, requester_os_uid=None,
        audit_id=raw(60), created_at_us=5001, xmin='60'))
    return before, live, terminal


class ArchiveOutageControls(unittest.TestCase):
    def test_census_declares_direct_reader_route_without_changing_product_config(self):
        sys.path.insert(0, str(ROOT/'experiments/ingest-bootstrap'))
        from installed import set_value
        from installed_archive_read import catalog_settings
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            runtime = object.__new__(case.ArchiveOutageRuntime)
            runtime.state, runtime.source, runtime.bundle = root/'state', ROOT, root/'bundle'
            runtime.config = root/'instance.toml'
            original = ('[storage]\narchive_root = "/var/lib/bigbrotr/archive"\n'
                '[postgres.catalog]\nendpoint = "127.0.0.1:33333"\n'
                'database = "bigbrotr_catalog"\nread_user = "bigbrotr_archive_read"\n'
                'read_password_ref = "file:/run/reader"\n')
            runtime.config.write_text(original)
            runtime.set_value = set_value
            runtime.pg = {'id': 'owned-postgres'}
            runtime.inspect, runtime.backend_absence = Mock(), Mock()
            runtime.catalog_rows = Mock(return_value={'unchanged': []})
            def command(argv, **options):
                declared = tomllib.loads(Path(argv[3]).read_text())
                port = int(argv[argv.index('--catalog-port')+1])
                self.assertEqual(port, 15432)
                self.assertEqual(catalog_settings(declared, port)['endpoint'], '127.0.0.1:15432')
                with self.assertRaises(AssertionError):
                    catalog_settings(tomllib.loads(original), port)
                self.assertEqual(runtime.config.read_text(), original)
                return subprocess.CompletedProcess(argv, 0, json.dumps({'tables': {}}), '')
            runtime.command = Mock(side_effect=command)
            self.assertEqual(runtime.census(root, 'prefix'), {'tables': {}})
            runtime.command.assert_called_once()
            self.assertEqual(runtime.catalog_rows.call_count, 2)

    def test_prefix_preserves_old_rows_and_rejects_unrelated_or_missing_additions(self):
        before, live, _ = preservation_fixture()
        check.prefix_preservation(before, live)
        mutations = (
            lambda v: v['datasets'][0].update(dataset_id='foreign'),
            lambda v: v['producer_requests'][0].update(xmin='99'),
            lambda v: v['producer_outcomes'][0].update(original=99),
            lambda v: v['projection_chunks'][0].update(xmin='99'),
            lambda v: v['management_audit'].append(dict(unrelated=True)),
            lambda v: v['jobs'].append(dict(unrelated=True)),
            lambda v: v['projection_plan_chunks'].pop(),
            lambda v: v['producer_checkpoint_dependencies'].pop(),
            lambda v: v['owner_epochs'][0].update(control_backend_pid=456),
            lambda v: v['owner_epochs'][2].update(lease_expires_at_us=20000),
            lambda v: v['owner_epochs'][-1].update(xmin='99'),
            lambda v: v['role_instances'][0].update(retired_at_us=1000),
            lambda v: v['producer_epochs'][0].update(state='closing'),
            lambda v: v['producer_epochs'][0].update(reserved_through_request=5),
            lambda v: v['consumers'][0].update(current_revision=3),
            lambda v: v['consumers'][1].update(current_topology_generation=1),
            lambda v: v['producer_checkpoints'][0].update(issuer_epoch=2),
            lambda v: v['producer_checkpoints'][0].update(checkpoint_revision=10),
            lambda v: v['browser_sessions'][0].update(xmin='99'),
        )
        for number, mutate in enumerate(mutations):
            changed = copy.deepcopy(live); mutate(changed)
            with self.subTest(mutation=number), self.assertRaises(AssertionError):
                check.prefix_preservation(before, changed)

    def test_terminal_binds_logout_release_transaction_and_separate_clock_reads(self):
        _, live, terminal = preservation_fixture()
        check.terminal_preservation(live, terminal, 7000)
        mutations = (
            lambda v: v['management_audit'][-1].update(actor_id='foreign'),
            lambda v: v['management_audit'][-1].update(subject_id='foreign'),
            lambda v: v['management_audit'][-1].update(policy_revision=2),
            lambda v: v['management_audit'][-1].update(created_at_us=4999),
            lambda v: v['management_audit'][-1].update(created_at_us=6001),
            lambda v: v['management_audit'][-1].update(xmin='61'),
            lambda v: v['browser_sessions'][0].update(revoked_at_us=999),
            lambda v: v['browser_sessions'][0].update(principal_id='foreign'),
            lambda v: v['owner_epochs'][0].update(state='active'),
            lambda v: v['owner_epochs'][0].update(exclusion_digest='\\x'+'00'*32),
            lambda v: v['owner_epochs'][2].update(lease_expires_at_us=20000),
            lambda v: v['owner_epochs'][-1].update(xmin='99'),
            lambda v: v['role_instances'][0].update(retired_at_us=5999),
            lambda v: v['role_instances'][0].update(retired_at_us=7001),
            lambda v: v['role_instances'][0].update(xmin='71'),
            lambda v: v['producer_outcomes'][0].update(xmin='99'),
        )
        for number, mutate in enumerate(mutations):
            changed = copy.deepcopy(terminal); mutate(changed)
            with self.subTest(mutation=number), self.assertRaises(AssertionError):
                check.terminal_preservation(live, changed, 7000)

    def test_old_observation_must_age_across_actual_request_brackets(self):
        first = dict(before_ns=1_000_000_000, after_ns=1_000_100_000)
        last = dict(before_ns=3_000_000_000, after_ns=3_000_100_000)
        check.retained_age(observation(1000), observation(2_001_000), first, last)
        for age in (0, 1000, 1_999_000, 2_003_000):
            with self.subTest(age=age), self.assertRaises(AssertionError):
                check.retained_age(observation(1000), observation(age), first, last)
        for bracket in (dict(before_ns=True, after_ns=1_000_100_000),
                        dict(before_ns=1_000_100_001, after_ns=1_000_100_000)):
            with self.assertRaises(AssertionError):
                check.retained_age(observation(1000), observation(2_001_000), bracket, last)

    def test_actual_custody_lock_is_held_until_join_and_cannot_be_replaced(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary).resolve()/'custody.lock'; path.touch(mode=0o600)
            with path.open('rb') as owner:
                fcntl.flock(owner, fcntl.LOCK_EX | fcntl.LOCK_NB)
                identity = case.custody_lock(path, held=True)
                with self.assertRaises(ValueError): case.custody_lock(path, held=False)
            self.assertEqual(case.custody_lock(path, held=False, expected=identity), identity)
            with self.assertRaises(ValueError): case.custody_lock(path, held=True)
            path.rename(path.with_name('original.lock')); path.touch(mode=0o600)
            with self.assertRaises(AssertionError): case.custody_lock(path, held=False, expected=identity)

    def test_proxy_join_and_capture_errors_do_not_skip_application_cleanup(self):
        with tempfile.TemporaryDirectory() as temporary:
            runtime = case.ArchiveOutageRuntime(arguments(Path(temporary)), {})
            runtime.route = Mock(); runtime.route.stop.side_effect = OSError('join failed')
            runtime.route_output = Path(temporary); runtime.proxy_observation = Mock(side_effect=ValueError('capture failed'))
            with patch.object(runtime, 'stop_route', side_effect=TimeoutError), \
                    patch.object(case.RestartIndexRuntime, 'cleanup') as cleanup:
                runtime.cleanup()
            cleanup.assert_called_once()
            self.assertEqual([item['resource'] for item in runtime.resources['cleanup_errors']],
                             ['catalog-route', 'catalog-route-forced-join', 'catalog-route-capture'])

    def test_initial_input_encoding_keeps_independent_third_batch_range(self):
        with tempfile.TemporaryDirectory() as temporary:
            _, _, science = metadata_fixture(Path(temporary))
            batch = science['batches'][2]
            row = dict(attempt_id='\\x'+'11'*16, dataset_id='\\x'+'22'*16,
                catalog_id='\\x'+'33'*16, coordinator_incarnation='\\x'+'44'*16,
                worker_incarnation='\\x'+'55'*16, ordinal=3, topology_generation=0,
                batch_digest='\\x'+batch['archive_digest'])
            raw = check.retained_input(row, batch, bytes(32))
            self.assertEqual(len(raw), 266)
            # One input: shard0, epoch1, key version/domain0121 and first4/last7.
            self.assertEqual(raw[172:202],
                bytes(4)+(1).to_bytes(8,'big')+(4).to_bytes(8,'big')+b'\x01\x21'+(7).to_bytes(8,'big'))
            for field, value in (('ordinal', 2), ('topology_generation', 1), ('batch_digest', '\\x'+'00'*32)):
                with self.subTest(field=field), self.assertRaises(AssertionError):
                    check.retained_input(dict(row, **{field:value}), batch, bytes(32))

    def test_failure_keeps_later_verification_not_run_and_never_starts_export(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary); args = arguments(root); args.case = 'p01-first-launch-archive-outage'; checks=[]
            with patch.object(case, 'ArchiveOutageRuntime') as runtime:
                runtime.return_value.initial.side_effect = AssertionError('original outage')
                base.first_launch(args, checks, {})
                runtime.return_value.verify_index.assert_not_called()
                runtime.return_value.ordinary.assert_not_called()
                runtime.return_value.cleanup.assert_called_once()
            result = {item['name']:item['result'] for item in checks}
            self.assertEqual(result['first_launch_signed_acquisition'], 'FAIL')
            self.assertEqual(result['first_launch_index_publication'], 'NOT_RUN')
            self.assertEqual(base.load(root/'failure.json')['reason'], 'original outage')

    def test_missing_candidate_does_not_launch_resources_or_claim_outage(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            result = subprocess.run([sys.executable, '-B', str(ROOT/'tests/acceptance/run.py'),
                'p01-first-launch-archive-outage', '--source-root', str(ROOT),
                '--candidate-manifest', str(root/'missing'), '--evidence', str(root/'evidence')],
                capture_output=True, text=True, timeout=5)
            self.assertEqual(result.returncode, 2, result.stderr)
            report = base.load(root/'evidence/result.json')
            self.assertEqual(report['counts'], dict(collected=7, executed=1, failed=0, skipped=0, not_run=6))
            self.assertFalse(list((root/'evidence/commands').iterdir()))


if __name__ == '__main__':
    unittest.main()
