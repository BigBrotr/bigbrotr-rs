"""Closed SQL-copy verifier controls; no PostgreSQL or product execution."""
import copy
import json
import os
from pathlib import Path
import re
import tempfile
import unittest
from unittest.mock import Mock, patch

import index_publication_runtime as runtime
from concurrency_cases import Incomplete, Mismatch, Session

ROOT = Path(__file__).resolve().parents[2]
MIGRATION = 'ab' * 32


def seed(phase='terminal'):
    manifest = json.loads((ROOT / 'migrations/manifest.json').read_text())
    tables = {'schema_versions'}
    for entry in manifest['application_order']:
        tables.update(re.findall(r'CREATE TABLE control\.(\w+)',
            (ROOT / 'migrations/postgres' / entry['name']).read_text()))
    value = {table: [] for table in tables}
    value.update(datasets=[dict(dataset_id='d', topology_generation=1)],
        projection_generations=[dict(dataset_id='d', owner_resource_id='i', generation_id='g',
            owner_epoch=1, provenance='local_first_witness')],
        consumers=[dict(consumer_id='i', consumer_role='index', current_revision=4, current_topology_generation=0)],
        initial_ingest_successions=[dict(owner_resource_id='s', shard_id=0)],
        index_owner_sessions=[dict(resource_id='i', generation_id='g', owner_epoch=1)],
        projection_plans=[dict(frontier_revision=i, sealed=True) for i in range(1, 5)],
        index_frontier_plans=[{} for _ in range(4)],
        projection_chunks=[dict(state='committed') for _ in range(40)],
        source_streams=[dict(owner_epoch=i, state='closed', last_complete_sequence=cut,
            owner_resource_id='s', shard_id=0) for i, cut in ((1, 12), (2, 10))],
        producer_epochs=[dict(producer_epoch=2, state='retired' if phase == 'terminal' else 'closing')],
        owner_epochs=[dict(role='ingest', epoch=2, state='released' if phase == 'terminal' else 'draining'),
            dict(role='control', epoch=3, state='released')],
        schema_versions=[dict(migration=41, checksum='\\x' + MIGRATION)])
    return value, tables


def summary(phase):
    names = runtime.HANDOFF_TERMINAL if phase == 'terminal' else runtime.HANDOFF_NONTERMINAL
    cases = []
    for name in sorted(names):
        positive = name in runtime.HANDOFF_POSITIVE
        cases.append(dict(name=name, classification='SQL fixture classification',
            state=None if positive else ('P0002' if name == 'missing_typed_receipt' else '23514'),
            message=None if positive else 'actual expected guard refusal'))
    return dict(format='bigbrotr-index-handoff-sql-cases/1', kind='SQL_ONLY_NO_NATIVE_PROOF',
        terminal=phase == 'terminal', case_count=len(cases), cases=cases, final_rows_sha256='cd' * 32)


class HandoffSQLControls(unittest.TestCase):
    def test_session_failure_retains_original_bounded_response_without_success_marker(self):
        for timeout in (0, 1):
            read, write = os.pipe()
            os.close(write)
            with os.fdopen(read, 'rb') as stream:
                session = Session.__new__(Session)
                session.pending = 'bb_result_2'
                session.buffer = b'ERROR: 23514\n'
                session.last_response_lines = []
                session.total_bytes = 0
                session.process = Mock(stdout=stream)
                with self.assertRaises(Incomplete):
                    session.finish(timeout=timeout)
                self.assertEqual(session.last_response_lines, ['ERROR: 23514'])
                self.assertEqual(session.pending, 'bb_result_2')
                self.assertEqual(session.buffer, b'')

    def test_seed_requires_real_phase_and_complete_old_prefix(self):
        for phase in ('terminal', 'nonterminal'):
            original, tables = seed(phase)
            runtime.validate_handoff_seed(original, phase, tables, MIGRATION)
            for change in (
                lambda s: s['source_streams'][1].update(state='open'),
                lambda s: s['source_streams'][1].update(last_complete_sequence=9),
                lambda s: s['source_streams'][0].update(owner_resource_id='other'),
                lambda s: s['consumers'][0].update(current_revision=3),
                lambda s: s['consumers'][0].update(current_topology_generation=1),
                lambda s: s['projection_plans'][3].update(sealed=False),
                lambda s: s['projection_chunks'][0].update(state='prepared'),
                lambda s: s['index_frontier_handoffs'].append({}),
                lambda s: s['index_owner_sessions'][0].update(owner_epoch=2),
                lambda s: s['owner_epochs'][-1].update(state='active'),
                lambda s: s['schema_versions'][0].update(checksum='\\x' + '00' * 32),
                lambda s: s['producer_epochs'][0].update(state='closing' if phase == 'terminal' else 'retired'),
                lambda s: s.pop('management_commands'),
            ):
                changed = copy.deepcopy(original)
                change(changed)
                with self.assertRaises(Mismatch):
                    runtime.validate_handoff_seed(changed, phase, tables, MIGRATION)
            self.assertEqual(seed(phase)[0], original)

    def test_case_inventory_matches_current_sql_and_keeps_refusal_classifications(self):
        program = (ROOT / runtime.HANDOFF_SQL).read_text()
        labels = re.findall(r"SELECT pg_temp\.(?:assert_true|expect_error)\('([^']+)'", program)
        self.assertEqual(len(labels), len(set(labels)))
        self.assertEqual(set(labels), runtime.HANDOFF_TERMINAL | runtime.HANDOFF_NONTERMINAL)
        self.assertEqual(len(runtime.HANDOFF_TERMINAL), 23)
        self.assertEqual(len(runtime.HANDOFF_NONTERMINAL), 3)
        self.assertIn('unsealed competing intent', program)
        self.assertIn('earlier complete batch receipt guard', program)
        self.assertIn('genuine closed unretired successor', program)
        self.assertNotRegex(program, r'(?i)(DISABLE TRIGGER|session_replication_role|SECURITY DEFINER)')
        self.assertLessEqual(len(program.encode()), 64 * 1024)

    def test_summary_requires_every_unique_case_and_actual_refusal_state(self):
        for phase in ('terminal', 'nonterminal'):
            valid = summary(phase)
            self.assertEqual(runtime.handoff_summary({'lines': [json.dumps(valid)]}, phase), valid)
            for change in (
                lambda s: s['cases'].pop(),
                lambda s: s['cases'].append(s['cases'][0]),
                lambda s: s.update(terminal=not s['terminal']),
                lambda s: s.update(kind='NATIVE_PROOF'),
                lambda s: s['cases'][-1].update(name='different'),
                lambda s: next(r for r in s['cases'] if r['state'] is not None).update(state='00000'),
                lambda s: next(r for r in s['cases'] if r['state'] is not None).update(message=None),
            ):
                changed = copy.deepcopy(valid)
                change(changed)
                with self.assertRaises(Mismatch):
                    runtime.handoff_summary({'lines': [json.dumps(changed)]}, phase)
            with self.assertRaises(Mismatch):
                runtime.handoff_summary({'lines': [json.dumps(valid)] * 2}, phase)

    def test_active_seed_refuses_before_clone_or_psql(self):
        with tempfile.TemporaryDirectory() as home, patch.object(runtime, 'Session') as session:
            owner = Mock(source=ROOT, pg={'id': 'owned-pg'}, resources={})
            owner.sql.return_value = '1'
            with self.assertRaisesRegex(Mismatch, 'backend-remains'):
                runtime.run_handoff_clone(owner, Path(home) / 'case', phase='terminal')
            session.assert_not_called()
            owner.docker.assert_not_called()
            self.assertFalse(any('CREATE DATABASE' in call.args[1] for call in owner.sql.call_args_list))
            self.assertEqual(owner.resources, {})
            receipt = json.loads((Path(home) / 'case/result.json').read_text())
            self.assertEqual(receipt['status'], 'FAIL')

    def test_final_persistence_failure_keeps_first_error_and_never_returns_success(self):
        original, tables = seed()
        original['schema_versions'][0]['checksum'] = '\\x' + runtime.sha(ROOT / 'migrations/postgres/0041_index_publication.sql')
        write_json = runtime.lab.write_json
        for prior_failure in ('sql', 'cleanup', None):
            for persistence in ('report', 'resources'):
                with self.subTest(prior_failure=prior_failure, persistence=persistence), tempfile.TemporaryDirectory() as home:
                    owner = Mock(source=ROOT, pg={'id': 'owned-pg'}, resources={}, tables=tables)
                    owner.rows.return_value = original
                    owner.docker.return_value.stdout = runtime.sha(ROOT / runtime.HANDOFF_SQL)
                    created, final_save = False, False
                    original_error = ValueError('original SQL failure')
                    save_error = OSError('diagnostic persistence unavailable')
                    response = dict(sqlstate='00000', rows=1, lines=[json.dumps(summary('terminal'))])
                    session = Mock(backend={'pid': 41}, buffer=b'', pending=None, total_bytes=0,
                                   last_response_lines=response['lines'])
                    session.process.pid = 42
                    session.process.poll.return_value = 0
                    session.finish.side_effect = original_error if prior_failure == 'sql' else None
                    session.finish.return_value = response
                    session.expect.side_effect = Session.expect
                    if prior_failure == 'cleanup':
                        session.close.side_effect = RuntimeError('backend cleanup refused')

                    def sql(database, command):
                        nonlocal created
                        if command.startswith('CREATE DATABASE '):
                            created = True
                            return ''
                        if 'pg_stat_activity' in command:
                            return '0'
                        if 'pg_database_size' in command:
                            return '1024'
                        if 'pg_database WHERE' in command:
                            clone = 'bb_index_handoff_terminal' in command
                            return json.dumps([] if clone and not created else [dict(oid=2 if clone else 1,
                                name='bb_index_handoff_terminal' if clone else 'bigbrotr_control', owner='bigbrotr_migrator')])
                        self.fail('unexpected fixture SQL')

                    def persist(path, value):
                        nonlocal final_save
                        if path.name == 'result.json' and 'cleanup' in value:
                            final_save = True
                            if persistence == 'report':
                                raise save_error
                        write_json(path, value)

                    def resources():
                        if final_save and persistence == 'resources':
                            raise save_error

                    def new_session(state, database, name, sessions, *, role):
                        sessions.append(session)
                        return session

                    owner.sql.side_effect = sql
                    owner.record_resources.side_effect = resources
                    expected_type = ValueError if prior_failure == 'sql' else RuntimeError if prior_failure == 'cleanup' else OSError
                    with patch.object(runtime, 'Session', side_effect=new_session), patch.object(runtime.lab, 'write_json', side_effect=persist):
                        with self.assertRaises(expected_type) as caught:
                            runtime.run_handoff_clone(owner, Path(home) / 'case', phase='terminal')
                    self.assertTrue(final_save)
                    session.close.assert_called_once()
                    self.assertEqual(owner.resources['index_handoff_sql_clones'][0]['status'], 'retained')
                    if prior_failure == 'sql':
                        self.assertIs(caught.exception, original_error)
                    elif prior_failure == 'cleanup':
                        self.assertEqual(str(caught.exception), 'handoff-sql-cleanup-incomplete')
                    else:
                        self.assertIs(caught.exception, save_error)
                    if prior_failure is not None:
                        self.assertEqual(caught.exception.__notes__, ['Handoff SQL diagnostic persistence failed: OSError'])

    def test_existing_clone_refuses_without_mutating_seed(self):
        original, tables = seed()
        original['schema_versions'][0]['checksum'] = '\\x' + runtime.sha(ROOT / 'migrations/postgres/0041_index_publication.sql')
        with tempfile.TemporaryDirectory() as home, patch.object(runtime, 'Session') as session:
            owner = Mock(source=ROOT, pg={'id': 'owned-pg'}, resources={}, tables=tables)
            owner.rows.return_value = original

            def sql(database, command):
                if 'pg_stat_activity' in command:
                    return '0'
                if 'pg_database_size' in command:
                    return '1024'
                if 'pg_database WHERE' in command:
                    name = 'bb_index_handoff_terminal' if 'bb_index_handoff_terminal' in command else 'bigbrotr_control'
                    return json.dumps([dict(oid=1, name=name, owner='bigbrotr_migrator')])
                self.fail('unexpected SQL before refusal')

            owner.sql.side_effect = sql
            with self.assertRaisesRegex(Mismatch, 'clone-name-exists'):
                runtime.run_handoff_clone(owner, Path(home) / 'case', phase='terminal')
            session.assert_not_called()
            owner.docker.assert_not_called()
            self.assertEqual(owner.resources, {})
            self.assertEqual(json.loads((Path(home) / 'case/seed-after.json').read_text()), original)


if __name__ == '__main__':
    unittest.main()
