"""Portable wrong-interpretation controls for the installed restart scenario."""
import copy
import importlib.util
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import tomllib
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from cases import first_launch as case
from cases import first_launch_restart as check

ROOT = Path(__file__).resolve().parents[2]
sys.dont_write_bytecode = True
for folder in ('control-migrations', 'ingest-bootstrap', 'ingest-succession'):
    sys.path.insert(0, str(ROOT / 'experiments' / folder))
if 'bootstrap_oracle' not in sys.modules:
    spec = importlib.util.spec_from_file_location('bootstrap_oracle', ROOT / 'experiments/ingest-bootstrap/oracle.py')
    module = importlib.util.module_from_spec(spec)
    sys.modules['bootstrap_oracle'] = module
    spec.loader.exec_module(module)
from installed_archive_oracle import expected
from installed_archive_read import sql_path
from installed_archive_catch_up_client import redacted, http
import installed_archive_catch_up_client as history_client
import installed_archive_profile as history_profile
import installed_archive_export_client as export_client
from installed_archive_catch_up_model import archive_science
from initial_succession_oracle import successor_manifest
from installed_family_science import NetworkScience, decode
from installed_progress_replay import head_digest, native_reference, certification_digest
from transition_replay import native_rows


def fixture(provenance=True):
    ds, slot = '22' * 16, '33' * 16
    domains = {k: bytes([100 + k]) * 16 for k in range(1, 10)}
    state = dict(datasets=[dict(dataset_id='\\x' + ds, policy_revision=1)],
        owner_epochs=[dict(role='ingest', resource_id='\\x' + 'aa' * 16,
            process_incarnation='\\x' + 'bb' * 16, acquired_at_us=1000)],
        allocation_domains=[dict(dictionary_kind=k, allocation_domain='\\x' + value.hex()) for k, value in domains.items()])
    public = dict(dataset_id=ds, dictionary_kind=4, allocation_domain=domains[4].hex(), ordinal='1')
    report = dict(owner_resource_id='aa' * 16, producer_slot_id=slot, recovery_generation='44' * 16,
        topology_generation='0', public_scope=public, private_scope=dict(public, ordinal='3'), dictionary_count=3)
    old = json.loads((ROOT / 'tests/fixtures/ingest/import-event.jsonl').read_text())
    first = expected(state, report, old, 2000, 3000, 4000, 1, native_options=dict(acknowledge=True, provenance=provenance))
    previous = next(value for cf, key, value in first['native'] if cf == 'default' and key == b'\x01\x01')
    fresh = [bytes([120 + k]) * 16 for k in range(1, 10)]
    manifest = successor_manifest(previous, bytes.fromhex('cc' * 16), fresh)
    model = SimpleNamespace(ds=state['datasets'][0], slot='\\x' + slot, legacy=dict(previous_manifest='\\x' + previous.hex()),
        inputs={}, native=dict(event='dump-state', sequence=0, rows=[dict(cf=cf, key=key.hex(), value=value.hex()) for cf, key, value in first['native']]))
    plan = dict(epoch=2, topology=1, manifest=manifest, domains=['\\x' + d.hex() for d in fresh])
    artifact = (ROOT / 'tests/fixtures/acquisition/selector-events.json').read_bytes()
    assert check.sha(artifact) == check.SELECTOR_SHA256
    new, = [row['event'] for row in json.loads(artifact)['events'] if row['name'] == 'included']
    encoded = NetworkScience(model, plan, acknowledge=True, new_event=new, endpoint=check.ENDPOINT, initial_provenance=provenance)
    encoded.network(provenance, 5000, 6000, 7000, 1, (6001, 7001) if provenance else None)
    science = archive_science(dict(rows=native_rows(encoded.values)), live_epoch=2, provenance=provenance)
    encoded.first, encoded.initial_state, encoded.initial_report = first, state, report
    return old, new, manifest, encoded, science


def publication_fixture(science, provenance=True):
    dataset, catalog = '\\x' + '22' * 16, '\\x' + '55' * 16
    attempts, receipts = [], []
    for n, batch in enumerate(science['batches'], 1):
        identity = f'{n:032x}'
        attempts.append(dict(attempt_id='\\x' + identity, dataset_id=dataset, catalog_id=catalog,
            ordinal=n, expected_predecessor=n - 1, schema_version=1, topology_generation=batch['topology'],
            batch_digest='\\x' + batch['archive_digest']))
        receipts.append(dict(dataset_id=dataset[2:].upper(), schema_version=1, topology_generation=batch['topology'],
            batch_digest=batch['archive_digest'].upper(), record_count=batch['last'] - batch['first'] + 1,
            input_ranges=[dict(shard_id=0, owner_epoch=batch['epoch'], first_sequence=batch['first'], last_sequence=batch['last'])],
            attempt_id=identity.upper(), publication_ordinal=n))
    from installed_archive_catch_up_oracle import cut_digest
    cuts, snapshot_id = ([(1, 12), (2, 10)], 8) if provenance else ([(1, 8), (2, 5)], 7)
    snapshot = dict(frontier_revision=6, topology_generation=1, snapshot_id=snapshot_id,
        request_digest='\\x' + cut_digest(dataset, 1, receipts, cuts, snapshot_id),
        **dict(zip(check.COUNTS, [13, 2, 2, 2, 3] if provenance else [7, 2, 2, 2, 0])))
    state = dict(datasets=[dict(dataset_id=dataset)], consumers=[dict(consumer_id=catalog, consumer_role='archive', current_revision=6, current_topology_generation=1)],
        archive_attempts=attempts, archive_frontier_snapshots=[snapshot],
        consumer_frontiers=[dict(consumer_id=catalog, frontier_revision=6, topology_generation=1, shard_id=0, owner_epoch=epoch, last_complete_sequence=cut)
            for epoch, cut in cuts],
        archive_frontier_batches=[dict(frontier_revision=6, attempt_id=row['attempt_id']) for row in attempts])
    census = dict(format='bigbrotr-independent-installed-archive-read/1', tables={**copy.deepcopy(science['tables']), 'archive_commits': receipts})
    return state, census


def progress_fixture(new, manifest, science, provenance=True):
    common = dict(dataset_id=science.m.ds['dataset_id'], producer_slot_id=science.m.slot, producer_epoch=2)
    owner = dict(resource_id='\\x' + 'a1' * 16, epoch=9, process_incarnation='\\x' + 'a2' * 16, acquired_at_us=4500, lease_expires_at_us=20000)
    job = dict(dataset_id=common['dataset_id'], job_id='\\x' + 'a3' * 16)
    detail = dict(scope_id='\\x' + 'a4' * 16)
    wire = dict(request=['REQ', 'ab' * 24, dict(kinds=[1], limit=3)], sent_at_us=5900, closed_at_us=8000, clock_resolution_us=1)
    head = dict(common, progress_format=1, manifest_sha256='\\x' + check.sha(manifest), checkpoint_revision=4,
        complete_through_request=3 if provenance else 2, progress_jobs=1, dependency_count=16 if provenance else 11, issuer_resource=owner['resource_id'],
        issuer_epoch=owner['epoch'], issuer_incarnation=owner['process_incarnation'], committed_at_us=8500, xmin='104', checkpoint_digest='\\x' + '00' * 32)
    progress = dict(common, job_id=job['job_id'], scope_id=detail['scope_id'], progress_revision=3,
        request_id='\\x' + wire['request'][1].encode().hex(), event_ordinal=3, wire_sequence=3,
        input_sha256=check.request_binding(new, wire, 3)['progress_input_sha256'], classification=5, mutation_request=None,
        classified_events=3, successful_events=3, rejected_events=0, observed_at_us=7500,
        clock_resolution_us=1, clock_anomaly_flags=0, xmin='104')
    progress['intent_digest'] = '\\x' + certification_digest(new, science.scope, science.ref(2), science.ref(2),
        [science.ref(1), science.ref(3), science.ref(5)] if provenance else None, progress, wire).hex()
    dependencies = []
    coordinates = ([(1, n) for n in (3, 4, 8, 9, 10, 11)] + [(2, n) for n in range(1, 11)] if provenance else
        [(1, n) for n in (3, 4, 5, 6, 7, 8)] + [(2, n) for n in range(1, 6)])
    for epoch, sequence in coordinates:
        ref = science.refs[epoch, sequence]
        first = next((n for n, receipt in science.receipts.items() if ref in decode(receipt.split(b'\0', 1)[1])[5]), 3)
        dependencies.append(dict(common, source_shard=0, source_epoch=epoch, source_sequence=sequence,
            payload_digest='\\x' + ref[1].hex(), xmin=str(100 + first)))
    head['checkpoint_digest'] = '\\x' + head_digest(head, [progress], dependencies, lambda row: native_reference(row, science.values)).hex()
    state = dict(acquisition_progress=[progress], producer_checkpoints=[head], producer_checkpoint_dependencies=dependencies,
        producer_outcomes=[dict(producer_epoch=2, request_sequence=n, xmin=str(100 + n)) for n in range(1, 4 if provenance else 3)])
    return state, job, detail, owner, wire


class RestartControls(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.old, cls.new, cls.manifest, cls.encoded, cls.science = fixture()

    def test_sql_safe_research_paths_and_traversal_refusal(self):
        self.assertEqual(sql_path('/home/tux/bigbrotr-rs/.research/first-launch/data/archive'),
            '/home/tux/bigbrotr-rs/.research/first-launch/data/archive')
        for path in ('/x/../archive', '/x/./archive', "/x/quote'", '/x/space here', 'relative'):
            with self.subTest(path=path), self.assertRaises(AssertionError):
                sql_path(path)

    def test_history_redaction_refuses_supplied_escaped_password(self):
        password = 'Independent "owner" \\ password café'
        redacted(dict(status='PASS'), (password,))
        with self.assertRaisesRegex(AssertionError, 'sensitive bytes'):
            redacted(dict(diagnostic=password), (password,))

    def test_history_mode_never_falls_back_to_legacy_identity(self):
        with tempfile.TemporaryDirectory() as temporary, self.assertRaisesRegex(AssertionError, 'explicit fixture profile'):
            http(Path(temporary), False, history_only=True)

    def test_history_setup_precedes_single_attempt_and_does_not_retry_429(self):
        with tempfile.TemporaryDirectory() as temporary:
            home=Path(temporary)
            expected=dict(scope={},catalog_id='catalog',frontier_revision=6,snapshot_id=8,
                config_revision='1',policy_revision='1',startup_quiescence=True)
            (home/'read-input.json').write_text(json.dumps(expected))
            (home/'events.json').write_text(json.dumps([{'id':'one'},{'id':'two'}]))
            (home/'read-session.private').write_text(json.dumps(dict(session='ab'*32,csrf='cd'*32)))
            profile=dict(product_binary='/unused',password_ref='file:/unused',expected_principal_id='principal',expected_actions=['data.read'])
            me=dict(principal_id='principal',stored_actions=['data.read'],config_revision='1',policy_revision='1')
            answers=[]
            for status,value in ((200,me),(429,{'error':{'code':'capacity_or_rate_limited'}})):
                answer=Mock(status=status);answer.read.return_value=json.dumps(value).encode();answers.append(answer)
            connection=Mock();connection.getresponse.side_effect=answers
            def phase(path,name,*,enclosing_deadline_ns):
                self.assertEqual((path,name,enclosing_deadline_ns),(home,'setup',30_000_000_010))
                self.assertEqual(connection.request.call_count,1)
                self.assertEqual(connection.request.call_args.args[:2],('GET','/management/v1/me'))
                return {'phase':'setup','finished_ns':20}
            with patch.object(history_profile,'load',return_value=profile),patch.object(history_profile,'password_file',return_value='private'),\
                 patch.object(history_client.time,'monotonic_ns',return_value=10),patch.object(history_client.http_client,'HTTPConnection',return_value=connection),\
                 patch.object(history_client.subprocess,'run',return_value=SimpleNamespace(returncode=0,stderr=b'',stdout=b'{}')),\
                 patch.object(export_client,'phase',side_effect=phase) as barrier:
                with self.assertRaises(AssertionError): http(home,False,history_only=True)
            barrier.assert_called_once()
            self.assertEqual(connection.request.call_count,2)
            self.assertEqual(connection.request.call_args.args[:2],('POST','/v1/resources/event_history'))
            observations=json.loads((home/'read-observations.json').read_text())
            self.assertEqual(observations[-1]['status'],429)
            self.assertFalse((home/'history-client-result.json').exists())

    def test_successor_incarnation_and_domains_must_be_fresh(self):
        prior = dict(owner_epochs=[dict(process_incarnation='\\x' + 'aa' * 16)])
        intent = dict(target_incarnation='\\x' + 'bb' * 16, target_role_instance_id='\\x' + 'cc' * 16)
        domains = ['\\x' + f'{n:032x}' for n in range(1, 10)]
        check.fresh_successor(prior, intent, domains)
        for mutation in ('incarnation', 'domain'):
            altered, selected = dict(intent), list(domains)
            if mutation == 'incarnation':
                altered['target_incarnation'] = prior['owner_epochs'][0]['process_incarnation']
            else:
                selected[0] = prior['owner_epochs'][0]['process_incarnation']
            with self.subTest(mutation=mutation), self.assertRaises(AssertionError):
                check.fresh_successor(prior, altered, selected)

    def test_full_science_and_publication_positive(self):
        state, census = publication_fixture(self.science)
        check.publication(state, census, self.science, revision=6, snapshot=8, topology=1, cuts=[(1, 12), (2, 10)])
        self.assertEqual([len(rows) for rows in self.science['tables'].values()], [13, 2, 2, 2, 3])
        endpoints = [r['descriptor_value']['url'] for r in self.science['tables']['dictionaries'] if r['descriptor_schema'] == 'endpoint']
        self.assertIn(check.ENDPOINT, endpoints)
        self.assertNotIn('ws://127.0.0.1:7447/installed-successor-fixture', endpoints)

    def test_science_refuses_changed_original_body_eligibility_receive_visibility_and_extra_fact(self):
        changes = [lambda c: c['tables']['events'][0].update(content='changed'),
            lambda c: c['tables']['event_eligibility_facts'][0].update(first_eligible_at=4001),
            lambda c: c['tables']['event_receive_facts'][0].update(received_at=3001),
            lambda c: c['tables']['event_receive_facts'][0]['observer_id'].update(ordinal=2),
            lambda c: c['tables']['events'][0]['visibility_scope_id'].update(ordinal=1),
            lambda c: c['tables']['provenance_facts'].append(copy.deepcopy(c['tables']['provenance_facts'][-1])),
            lambda c: c['tables']['events'][0]['source_record_id'].update(owner_epoch=True)]
        for change in changes:
            state, census = publication_fixture(self.science)
            change(census)
            with self.subTest(change=change), self.assertRaises(AssertionError):
                check.publication(state, census, self.science, revision=6, snapshot=8, topology=1, cuts=[(1, 12), (2, 10)])

    def test_rejects_legacy_epoch_and_wrong_frontier(self):
        with self.assertRaises(AssertionError):
            archive_science(dict(rows=native_rows(self.encoded.values)))
        state, census = publication_fixture(self.science)
        state['consumer_frontiers'][1]['owner_epoch'] = 3
        with self.assertRaises(AssertionError):
            check.publication(state, census, self.science, revision=6, snapshot=8, topology=1, cuts=[(1, 12), (2, 10)])

    def test_current_progress_and_dependency_transactions(self):
        state, job, detail, owner, wire = progress_fixture(self.new, self.manifest, self.encoded)
        check.successor_progress(state, self.manifest, self.encoded, job, detail, owner, wire, self.new, 9000)
        state['producer_checkpoint_dependencies'][0]['xmin'] = '99'
        with self.assertRaises(AssertionError):
            check.successor_progress(state, self.manifest, self.encoded, job, detail, owner, wire, self.new, 9000)

    def test_coordinated_progress_rehash_cannot_change_semantics(self):
        for key, value in (('classified_events', 4), ('event_ordinal', 2), ('wire_sequence', 2), ('classification', 2)):
            state, job, detail, owner, wire = progress_fixture(self.new, self.manifest, self.encoded)
            state['acquisition_progress'][0][key] = value
            if key == 'classified_events':
                state['acquisition_progress'][0].update(progress_revision=4, successful_events=4)
                state['producer_checkpoints'][0]['checkpoint_revision'] = 5
            if key == 'classification':
                state['acquisition_progress'][0]['mutation_request'] = 3
            # Validly encoded and rehashed wrong semantics must still fail.
            state['producer_checkpoints'][0]['checkpoint_digest'] = '\\x' + head_digest(state['producer_checkpoints'][0],
                state['acquisition_progress'], state['producer_checkpoint_dependencies'],
                lambda row: native_reference(row, self.encoded.values)).hex()
            with self.subTest(key=key), self.assertRaises(AssertionError):
                check.successor_progress(state, self.manifest, self.encoded, job, detail, owner, wire, self.new, 9000)

    def test_coordinated_unused_older_transaction_is_not_final_proof(self):
        state, job, detail, owner, wire = progress_fixture(self.new, self.manifest, self.encoded)
        state['acquisition_progress'][0]['xmin'] = state['producer_checkpoints'][0]['xmin'] = '99'
        with self.assertRaises(AssertionError):
            check.successor_progress(state, self.manifest, self.encoded, job, detail, owner, wire, self.new, 9000)

    def test_integer_equality_does_not_accept_boolean_or_float(self):
        for value in (True, 1.0):
            with self.subTest(value=value), self.assertRaises(AssertionError):
                check.exact(dict(progress_event_ordinal=value), dict(progress_event_ordinal=1), 'position')

    def test_request_positions_are_exact_integers_in_full_records(self):
        state, job, detail, owner, wire = progress_fixture(self.new, self.manifest, self.encoded)
        common = dict(dataset_id=self.encoded.m.ds['dataset_id'], producer_slot_id=self.encoded.m.slot,
            producer_epoch=2, manifest_sha256='\\x' + check.sha(self.manifest))
        source = dict(relay_endpoint_id='\\x' + 'b1' * 16, relay_access_scope_id='\\x' + 'b2' * 16,
            relay_enrollment_generation=1, relay_route_id='\\x' + 'b3' * 16,
            acquisition_scope_id=detail['scope_id'], acquisition_job_id=job['job_id'])
        bindings = {2: check.request_binding(self.new, wire, 1), 3: check.request_binding(self.old, wire, 2)}
        state.update(producer_epochs=[dict(producer_epoch=2, dataset_id=common['dataset_id'], producer_slot_id=common['producer_slot_id'],
            outstanding_request_limit=32, reserved_through_request=3)], producer_requests=[], producer_outcomes=[])
        for n, original in self.encoded.inputs.items():
            receipt = self.encoded.receipts[n]
            state['producer_requests'].append(dict(common, request_sequence=n, request_digest='\\x' + check.sha(original),
                input_bytes=len(original), input_profile=1, issuer_role='control', issuer_resource=owner['resource_id'],
                issuer_epoch=owner['epoch'], issuer_incarnation=owner['process_incarnation'], reserved_at_us=5000 + n,
                **(source if n > 1 else dict.fromkeys(source)),
                **(bindings[n] if n > 1 else dict.fromkeys(check.PROGRESS_FIELDS))))
            state['producer_outcomes'].append(dict(common, request_sequence=n, request_digest='\\x' + check.sha(original),
                outcome_kind=1, outcome_bytes='\\x' + receipt.hex(), outcome_sha256='\\x' + check.sha(receipt), policy_epoch=1,
                recorder_role='control', recorder_resource=owner['resource_id'], recorder_epoch=owner['epoch'],
                recorder_incarnation=owner['process_incarnation'], recorded_at_us=7000 + n))
        args = (2, self.manifest, list(self.encoded.inputs.values()), list(self.encoded.receipts.values()), owner, bindings, source)
        check.requests(state, *args)
        for key in ('progress_event_ordinal', 'progress_wire_sequence'):
            for value in (True, 1.0):
                altered = copy.deepcopy(state)
                altered['producer_requests'][1][key] = value
                with self.subTest(key=key, value=value), self.assertRaises(AssertionError):
                    check.requests(altered, *args)

    def test_exact_retry_refuses_ledger_or_producer_mutation(self):
        before = dict(owner_epochs=[], role_instances=[], producer_requests=[dict(request_sequence=1)], archive_attempts=[dict(ordinal=1)])
        check.retry_unchanged(before, copy.deepcopy(before))
        for table in ('producer_requests', 'archive_attempts'):
            after = copy.deepcopy(before)
            after[table].clear()
            with self.subTest(table=table), self.assertRaises(AssertionError):
                check.retry_unchanged(before, after)

    def test_restart_registration_missing_artifact_is_not_run(self):
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary) / 'evidence'
            result = subprocess.run([sys.executable, '-B', str(ROOT / 'tests/acceptance/run.py'), 'p01-first-launch-restart',
                '--source-root', str(ROOT), '--candidate-manifest', str(Path(temporary) / 'absent.json'), '--evidence', str(output)],
                capture_output=True, text=True, timeout=5)
            report = json.loads((output / 'result.json').read_text())
            self.assertEqual(result.returncode, 2)
            self.assertEqual(report['result'], 'NOT_RUN')
            self.assertEqual(report['counts']['collected'], 12)
            self.assertIn('provenance-off', report['limits'][1])
            self.assertFalse(list((output / 'commands').iterdir()))

    def test_restart_failure_retains_not_run_stages_and_cleanup(self):
        with tempfile.TemporaryDirectory() as temporary:
            args = SimpleNamespace(case='p01-first-launch-restart', evidence=Path(temporary), source_root=ROOT)
            checks = []
            with patch.object(case, 'Runtime') as runtime:
                runtime.return_value.restart_ingest.side_effect = AssertionError('wrong replay')
                case.first_launch(args, checks, {})
                runtime.return_value.restart_archive.assert_not_called()
                runtime.return_value.cleanup.assert_called_once()
            rows = {row['name']: row['result'] for row in checks}
            self.assertEqual(rows['first_launch_ingest_restart_resend'], 'FAIL')
            self.assertEqual(rows['first_launch_archive_catch_up'], 'NOT_RUN')
            self.assertEqual(rows['first_launch_cleanup'], 'PASS')


class ProvenanceOffControls(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.old, cls.new, cls.manifest, cls.encoded, cls.science = fixture(False)

    def test_fresh_recipe_and_stored_acceptance_select_off_independently(self):
        text = (ROOT / 'config/recipes/acceptance.toml').read_text()
        selected = tomllib.loads(check.deployment_recipe(text, provenance=False))
        self.assertEqual(selected['sync']['provenance'], 'off')
        self.assertEqual(len(selected['network']['private']['rules']), 2)
        document = dict(format='bigbrotr-product-policy/2', settings={'sync.provenance': 'off'})
        state = dict(datasets=[dict(config_revision=1, policy_revision=1)],
            config_revisions=[dict(revision=1, document=document, document_digest='independent-fixture')],
            policy_revisions=[dict(revision=1, config_revision=1, document=copy.deepcopy(document), document_digest='independent-fixture')])
        check.accepted_profile(state, False)
        # Even coordinated stored documents cannot replace the selected recipe.
        for table in ('config_revisions', 'policy_revisions'):
            state[table][0]['document']['settings']['sync.provenance'] = 'local_first_witness'
        with self.assertRaisesRegex(AssertionError, 'accepted provenance'):
            check.accepted_profile(state, False)

    def test_off_literal_facts_inputs_cuts_and_original_science(self):
        first = self.encoded.first
        self.assertEqual(len(first['native']), 35)
        self.assertEqual([len(rows) for rows in first['tables'].values()], [5, 1, 1, 1, 0])
        self.assertEqual([(b['first'], b['last']) for b in first['batches']], [(1, 2), (3, 3), (4, 4), (5, 8)])
        self.assertEqual([len(rows) for rows in self.science['tables'].values()], [7, 2, 2, 2, 0])
        self.assertEqual([(b['epoch'], b['first'], b['last']) for b in self.science['batches']],
            [(1, 1, 2), (1, 3, 3), (1, 4, 4), (1, 5, 8), (2, 1, 1), (2, 2, 5)])
        self.assertEqual(len(self.encoded.inputs), 2)
        for original in (first['inputs'][3], self.encoded.inputs[2]):
            self.assertIsNone(decode(original.split(b'\0', 1)[1])[3][5][3])
        schemas = [r['descriptor_schema'] for r in self.science['tables']['dictionaries']]
        self.assertEqual(schemas.count('observer'), 2)
        self.assertFalse({'endpoint', 'access-scope', 'event-verification'} & set(schemas))
        for table, rows in first['tables'].items():
            self.assertEqual([r for r in self.science['tables'][table] if r['source_record_id']['owner_epoch'] == 1], rows)
        self.assertEqual(first['tables']['events'][0]['source_record_id']['sequence'], 6)
        self.assertEqual(first['tables']['event_receive_facts'][0]['source_record_id']['sequence'], 8)

    def test_off_census_refuses_witness_rows_or_rewritten_first_science(self):
        changes = [lambda c: c['tables']['provenance_facts'].append(dict(hidden='witness')),
            lambda c: c['tables']['events'][0].update(content='changed'),
            lambda c: c['tables']['event_eligibility_facts'][0].update(first_eligible_at=4001),
            lambda c: c['tables']['event_receive_facts'][0].update(received_at=3001),
            lambda c: c['tables']['events'][0]['visibility_scope_id'].update(ordinal=1)]
        state, census = publication_fixture(self.science, False)
        check.publication(state, census, self.science, revision=6, snapshot=7, topology=1, cuts=[(1, 8), (2, 5)])
        for change in changes:
            changed = copy.deepcopy(census)
            change(changed)
            with self.subTest(change=change), self.assertRaises(AssertionError):
                check.publication(state, changed, self.science, revision=6, snapshot=7, topology=1, cuts=[(1, 8), (2, 5)])
        with self.assertRaises(AssertionError):
            archive_science(dict(rows=native_rows(self.encoded.values)), live_epoch=2)

    def test_proof_only_predecessor_dependencies_are_between_outcome_and_final_head(self):
        state, job, detail, owner, wire = progress_fixture(self.new, self.manifest, self.encoded, False)
        verify = lambda value: check.successor_progress(value, self.manifest, self.encoded,
            job, detail, owner, wire, self.new, 9000, provenance=False)
        verify(state)
        self.assertEqual(state['producer_checkpoints'][0]['dependency_count'], 11)
        for transaction in ('99', '102', '104', '105'):
            changed = copy.deepcopy(state)
            for row in changed['producer_checkpoint_dependencies']:
                if row['source_epoch'] == 1 and row['source_sequence'] != 3:
                    row['xmin'] = transaction
            with self.subTest(transaction=transaction), self.assertRaises(AssertionError):
                verify(changed)
        changed = copy.deepcopy(state)
        changed['producer_checkpoint_dependencies'][2]['xmin'] = '102'
        with self.assertRaises(AssertionError):
            verify(changed)
        # Rehashing a missing old dependency cannot manufacture a smaller proof.
        changed = copy.deepcopy(state)
        changed['producer_checkpoint_dependencies'].pop(2)
        head = changed['producer_checkpoints'][0]
        head['dependency_count'] = 10
        head['checkpoint_digest'] = '\\x' + head_digest(head, changed['acquisition_progress'],
            changed['producer_checkpoint_dependencies'], lambda r: native_reference(r, self.encoded.values)).hex()
        with self.assertRaises((AssertionError, ValueError)):
            verify(changed)

    def test_off_initial_progress_uses_eight_dependencies_and_no_witness_intent(self):
        from installed_progress_context_oracle import verify_archive_progress
        first, state = self.encoded.first, copy.deepcopy(self.encoded.initial_state)
        common = dict(dataset_id=state['datasets'][0]['dataset_id'], producer_slot_id=self.encoded.m.slot, producer_epoch=1)
        state.update(schema_versions=[dict(migration=39), dict(migration=40)],
            producer_epochs=[dict(common, reserved_through_request=4, outstanding_request_limit=32)],
            acquisition_jobs=[dict(job_id='\\x' + 'd1' * 16, scope_id='\\x' + 'd2' * 16)],
            jobs=[dict(job_id='\\x' + 'd1' * 16, request_key='\\x' + 'b1' * 16, state='queued', checkpoint_revision=0)])
        wires = [dict(number=n, request=['REQ', digit * 48, {'kinds': [1], 'limit': 3}],
            connected_at_us=1000 + n * 2000, request_at_us=1100 + n * 2000,
            sent_at_us=1200 + n * 2000, closed_at_us=1500 + n * 2000, clock_resolution_us=1,
            frames=['signed', 'forged-signature', 'duplicate', 'EOSE'], completion='request-and-websocket-close')
            for n, digit in ((1, 'a'), (2, 'b'))]
        control = dict(resource_id='\\x' + 'a1' * 16, epoch=1, process_incarnation='\\x' + 'a2' * 16,
            acquired_at_us=1000, lease_expires_at_us=10000)
        progress = dict(common, job_id='\\x' + 'd1' * 16, scope_id='\\x' + 'd2' * 16,
            progress_revision=6, request_id='\\x' + wires[1]['request'][1].encode().hex(), event_ordinal=3, wire_sequence=3,
            input_sha256=check.request_binding(self.old, wires[1], 3)['progress_input_sha256'], classification=5,
            mutation_request=None, classified_events=6, successful_events=4, rejected_events=2,
            observed_at_us=5400, clock_resolution_us=1, clock_anomaly_flags=0, xmin='109')
        dataset = check.raw(common['dataset_id'])
        domains = {r['dictionary_kind']: check.raw(r['allocation_domain']) for r in state['allocation_domains']}
        ref = lambda kind, ordinal=1: [dataset, kind, domains[kind], ordinal]
        progress['intent_digest'] = '\\x' + certification_digest(self.old, ref(4, 3), ref(2), ref(2), None, progress, wires[1]).hex()
        values = {(cf, key.hex()): value.hex() for cf, key, value in first['native']}
        manifest = bytes.fromhex(values['default', '0101'])
        deps = [dict(common, source_shard=0, source_epoch=1, source_sequence=n,
            payload_digest='\\x' + bytes.fromhex(values['facts', (b'\x01\x20' + dataset + bytes(4) + check.be(1) + check.be(n)).hex()])[2:34].hex(),
            xmin=str(101 if n <= 2 else 102 if n == 3 else 103 if n == 4 else 104)) for n in range(1, 9)]
        head = dict(common, progress_format=1, manifest_sha256='\\x' + check.sha(manifest), checkpoint_revision=9,
            complete_through_request=4, progress_jobs=1, dependency_count=8, issuer_resource=control['resource_id'],
            issuer_epoch=1, issuer_incarnation=control['process_incarnation'], committed_at_us=5600, xmin='109', checkpoint_digest='\\x' + '00' * 32)
        head['checkpoint_digest'] = '\\x' + head_digest(head, [progress], deps, lambda r: native_reference(r, values)).hex()
        state.update(producer_checkpoints=[head], acquisition_progress=[progress], producer_checkpoint_dependencies=deps,
            producer_outcomes=[dict(request_sequence=n, producer_epoch=1, xmin=str(100 + n)) for n in range(1, 5)])
        verify = lambda: verify_archive_progress(state, wires, 6000, self.old, first['native'], first['inputs'], first['receipts'], control, provenance=False)
        verify()
        progress['intent_digest'] = '\\x' + certification_digest(self.old, ref(4, 3), ref(2), ref(2),
            [ref(1), ref(3), ref(5)], progress, wires[1]).hex()
        head['checkpoint_digest'] = '\\x' + head_digest(head, [progress], deps, lambda r: native_reference(r, values)).hex()
        with self.assertRaises(AssertionError):
            verify()

    def test_off_registration_keeps_twelve_stages_and_missing_input_not_run(self):
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary) / 'evidence'
            result = subprocess.run([sys.executable, '-B', str(ROOT / 'tests/acceptance/run.py'), case.OFF_CASE,
                '--source-root', str(ROOT), '--candidate-manifest', str(Path(temporary) / 'absent.json'), '--evidence', str(output)],
                capture_output=True, text=True, timeout=5)
            report = json.loads((output / 'result.json').read_text())
            self.assertEqual((result.returncode, report['result'], report['counts']['collected']), (2, 'NOT_RUN', 12))
            self.assertIn('sync.provenance=off', report['limits'][0])
            self.assertNotIn('provenance-off repeat', report['limits'][1])
            self.assertFalse(list((output / 'commands').iterdir()))

    def test_history_pins_bind_the_selected_snapshot_through_the_frontier(self):
        for provenance, snapshot in ((True, 8), (False, 7)):
            before = {table: [] for table in ('owner_epochs', 'role_instances', 'browser_sessions', 'local_accounts',
                'management_audit', 'snapshot_leases', 'archive_snapshot_pins', 'retention_claims')}
            before['archive_frontier_snapshots'] = [dict(dataset_id='dataset', consumer_id='catalog', frontier_revision=6, snapshot_id=snapshot)]
            after = copy.deepcopy(before)
            after['owner_epochs'] = [dict(role='control', state='released', released_at_us=9000)]
            after['archive_snapshot_pins'] = [dict(lease_id=str(n), dataset_id='dataset', consumer_id='catalog',
                frontier_revision=6, purpose='reader') for n in (1, 2)]
            after['snapshot_leases'] = [dict(lease_id=str(n), state='released') for n in (1, 2)]
            check.history_unchanged(before, after, provenance=provenance)
            for field, value in (('consumer_id', 'other'), ('dataset_id', 'other'), ('frontier_revision', 5)):
                changed = copy.deepcopy(after)
                changed['archive_snapshot_pins'][0][field] = value
                with self.subTest(provenance=provenance, field=field), self.assertRaises((AssertionError, ValueError)):
                    check.history_unchanged(before, changed, provenance=provenance)
            changed = copy.deepcopy(after)
            changed['snapshot_leases'][0]['state'] = 'active'
            with self.assertRaises(AssertionError):
                check.history_unchanged(before, changed, provenance=provenance)
            for state in (before, after):
                state['archive_frontier_snapshots'][0]['snapshot_id'] = 7 if provenance else 8
            with self.assertRaises(AssertionError):
                check.history_unchanged(before, after, provenance=provenance)


class LoginWindowControls(unittest.TestCase):
    def test_fresh_deployment_adds_only_the_second_exact_fixture_destination(self):
        text = (ROOT / 'config/recipes/acceptance.toml').read_text()
        original = tomllib.loads(text)
        prepared = check.deployment_recipe(text)
        actual = tomllib.loads(prepared)
        expected = copy.deepcopy(original)
        expected['network']['private']['rules'].append(dict(
            endpoint='ws://127.0.0.1:7447/installed-live-fixture', cidr='127.0.0.1/32', port=7447))
        self.assertEqual(actual, expected)
        with self.assertRaisesRegex(AssertionError, 'destination rule changed'):
            check.deployment_recipe(prepared)

    def observation(self, elapsed, account=5, peer=6):
        return dict(now_us=1_000_000 + elapsed, buckets=[
            dict(dataset_id='22' * 16, kind=kind, bucket=bucket, attempts=attempts,
                window_started_at_us=1_000_000)
            for kind, bucket, attempts in [('account', 97, account), ('peer', 13, peer)]])

    def test_password_attempts_expire_at_the_original_sixty_second_boundary(self):
        self.assertEqual(check.login_buckets(), {'account': (97, 5), 'peer': (13, 20)})
        self.assertEqual(check.login_delay(self.observation(59_999_999), '22' * 16), 1)
        self.assertEqual(check.login_delay(self.observation(60_000_000), '22' * 16), 0)
        self.assertEqual(check.login_delay(self.observation(1, account=4), '22' * 16), 0)
        self.assertEqual(check.login_delay(self.observation(1, account=4, peer=20), '22' * 16), 59_999_999)
        self.assertEqual(check.login_delay(dict(now_us=1, buckets=[]), '22' * 16), 0)
        observed = self.observation(60_000_000, peer=20)
        observed['buckets'][1]['window_started_at_us'] += 10_000_000
        self.assertEqual(check.login_delay(observed, '22' * 16), 10_000_000)

    def test_malformed_or_unrelated_gate_state_is_not_permission(self):
        for key, value in [('dataset_id', '33' * 16), ('bucket', 98), ('bucket', True),
                ('attempts', 6), ('attempts', True), ('kind', 'unknown'),
                ('window_started_at_us', 2_000_000)]:
            observed = self.observation(1)
            observed['buckets'][0][key] = value
            with self.subTest(key=key, value=value), self.assertRaises(AssertionError):
                check.login_delay(observed, '22' * 16)
        observed = self.observation(1)
        observed['buckets'][1] = observed['buckets'][0]
        with self.assertRaises(AssertionError):
            check.login_delay(observed, '22' * 16)

    def test_window_is_reobserved_after_wait_without_charging_or_resetting_it(self):
        with tempfile.TemporaryDirectory() as temporary:
            instance = object.__new__(case.Runtime)
            instance.ids = {'dataset': '22' * 16}
            instance.restart_checks = check
            observations = [self.observation(48_000_000), self.observation(60_000_000)]
            statements = []
            def read(database, statement):
                self.assertEqual(database, 'bigbrotr_control')
                statements.append(statement)
                return json.dumps(observations.pop(0))
            instance.sql = read
            with patch.object(case.time, 'monotonic', return_value=100), patch.object(case.time, 'sleep') as wait:
                instance.wait_login_capacity(Path(temporary))
            wait.assert_called_once_with(12)
            self.assertEqual(len(statements), 2)
            self.assertTrue(all(s.startswith('SELECT ') for s in statements))
            receipt = json.loads((Path(temporary) / 'login-admission.json').read_text())
            self.assertEqual([r['wait_us'] for r in receipt], [12_000_000, 0])

    def test_late_database_observation_cannot_certify_admission(self):
        with tempfile.TemporaryDirectory() as temporary:
            instance = object.__new__(case.Runtime)
            instance.ids = {'dataset': '22' * 16}
            instance.restart_checks = check
            instance.sql = lambda *_: json.dumps(self.observation(60_000_000))
            with patch.object(case.time, 'monotonic', side_effect=[100, 100, 166]), \
                    self.assertRaisesRegex(ValueError, 'observation exceeded'):
                instance.wait_login_capacity(Path(temporary))
            self.assertFalse((Path(temporary) / 'login-admission.json').exists())


if __name__ == '__main__':
    unittest.main()
