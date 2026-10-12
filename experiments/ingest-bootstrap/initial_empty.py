"""Exact empty-attempt retirement on new copies of the qualified prepared seed.

Every process reconstructs from config and retained state. A guarded SQL fault
runs after the actual native marker write; a fresh process completes its gap.
Originals and every failed derivative are retained under the outer lab owner.
"""
import json
from pathlib import Path
import shutil

import lab
import initial_history as history
from initial_gap import configuration
from initial_succession import census, digest, physical
from initial_gap_policy import GUARDS, require_guards
from initial_empty_oracle import verify, negative_controls
from initial_empty_guards import run_prepared, run_terminal

CASES = ('empty-normal', 'empty-native-first-failure', 'empty-inspect', 'empty-cancelled',
    'empty-missing-input', 'empty-missing-outcome', 'empty-nonempty-target',
    'empty-missing-intent', 'empty-old-schema')


def upgrade(pg, client, output, database, config, label, rows):
    before, files = rows(database), physical(pg, database)
    assert {r['migration'] for r in before['schema_versions']} >= {29, 30}
    assert all(r['migration'] != 31 for r in before['schema_versions'])
    def apply():
        return lab.cli(dict(pg, client=client), 'migrate', 'apply', '--file', config,
            '--user', 'bigbrotr_migrator', '--password-ref', 'file:/run/secrets/migrator', '--target', '31')
    response = apply()
    lab.write_json(output / (label + '-migration.json'), dict(returncode=response.returncode,
        stdout=response.stdout, stderr=response.stderr))
    assert response.returncode == 0, response.stderr
    after = rows(database)
    assert set(after) == set(before) and len(after) == 71
    assert all(after[t] == before[t] for t in before if t != 'schema_versions')
    assert all(r in after['schema_versions'] for r in before['schema_versions'])
    added, = [r for r in after['schema_versions'] if r not in before['schema_versions']]
    migration = Path(__file__).resolve().parents[2] / 'migrations/postgres/0031_initial_empty_retirement.sql'
    assert added['migration'] == 31 and added['checksum'] == '\\x' + digest(migration)
    assert apply().returncode == 0 and rows(database) == after
    final_files = physical(pg, database)
    assert files == final_files
    lab.write_json(output / (label + '-upgrade.json'), dict(before=before, after=after,
        physical_before=files, physical_after=final_files, exact_reapply=True))
    lab.sql(pg, database, 'GRANT EXECUTE ON FUNCTION '
        'control.initial_empty_retirement_digest(control.initial_ingest_successions),'
        'control.lock_initial_empty_retirement(bytea,bytea),'
        'control.require_initial_empty_claims(control.initial_ingest_successions),'
        'control.require_empty_initial_retirement(bytea,bytea),'
        'control.require_initial_empty_transition(bytea,bytea,bigint),'
        'control.require_complete_initial_empty_retirement() TO bigbrotr_control;')
    assert rows(database) == after


def guarded_fault(pg, database, output, label, *, remove=False):
    """Isolated fault only; the sequence proves the real UPDATE was reached."""
    if remove:
        hit = lab.sql(pg, database,
            'SELECT last_value::text||is_called::text FROM empty_retirement_fixture.source_hit')
        assert hit == '1true'
        statement = '''DROP TRIGGER fixture_empty_source_rollback ON control.source_streams;
DROP FUNCTION empty_retirement_fixture.reject_source();
DROP SEQUENCE empty_retirement_fixture.source_hit;
DROP SCHEMA empty_retirement_fixture;'''
    else:
        hit = None
        statement = '''CREATE SCHEMA empty_retirement_fixture;
CREATE SEQUENCE empty_retirement_fixture.source_hit;
GRANT USAGE ON SCHEMA empty_retirement_fixture TO bigbrotr_control;
GRANT USAGE ON SEQUENCE empty_retirement_fixture.source_hit TO bigbrotr_control;
CREATE FUNCTION empty_retirement_fixture.reject_source() RETURNS trigger
LANGUAGE plpgsql SET search_path=pg_catalog AS $$ BEGIN
    PERFORM nextval('empty_retirement_fixture.source_hit');
    RAISE EXCEPTION 'qualification empty retirement source rollback' USING ERRCODE='P0001';
END $$;
CREATE TRIGGER fixture_empty_source_rollback BEFORE UPDATE ON control.source_streams
    FOR EACH ROW WHEN (NEW.owner_epoch=2 AND OLD.state='open' AND NEW.state='closing')
    EXECUTE FUNCTION empty_retirement_fixture.reject_source();'''
    lab.sql(pg, database, statement)
    if not remove:
        assert lab.sql(pg, database, 'SELECT is_called FROM empty_retirement_fixture.source_hit') == 'f'
    observed = json.loads(lab.sql(pg, database, 'SELECT ' + GUARDS))
    require_guards(observed)
    lab.write_json(output / (label + ('-fault-removed.json' if remove else '-fault-installed.json')),
        dict(statement=statement, hit=hit, guards=observed))
    return observed


def run(pg, client, output, rows, clone):
    basis = output.parent.parent / history.SEED / 'bindings'
    store = basis / 'successor-normal/store'
    assert all(digest(basis / n) == h for n, h in history.FROZEN.items())
    retained = json.loads((basis / 'result.json').read_text())
    assert retained['status'] == 'PASS_INITIAL_SUCCESSOR_PREPARATION'
    assert retained['source'] == history.SOURCE and retained['owner'] == history.OWNER
    assert not retained['cleanup_errors'] and not retained['unrelated_changed'] and not retained['missing_owned']
    containers = json.loads(lab.docker('inspect', *lab.docker('ps', '-aq').stdout.split()).stdout)
    old, = [c for c in containers if c['Id'] == retained['client']['id']]
    assert old['Config']['Labels'][retained['label']] == history.OWNER
    assert not old['State']['Running'] and not old['State']['Pid'] and not old['State']['OOMKilled']
    assert not any(c['State']['Running'] and any(str(basis) in m.get('Source', '')
        for m in c['Mounts']) for c in containers)
    original = rows(history.DATABASE)
    assert original == json.loads((basis / 'successor-normal-after.json').read_text())
    baseline = json.loads((basis / 'successor-normal-after-native.json').read_text())
    files, inputs = census(store), history.inputs_at(store)
    assert sum(v['bytes'] for v in files.values()) < 1024**2
    assert len(original) == 71 and len(baseline['rows']) == 52 and len(inputs) == 4
    source_events = [json.loads(line) for line in json.loads(
        (basis / 'successor-normal-response.json').read_text())['stdout'].splitlines()]
    final, = [e for e in source_events if e['event'] == 'successor-state' and e['stage'] == 'final']
    assert final['rows'] == baseline['rows'] and dict(final['state']['originals']) == inputs
    assert {t: json.loads(v) for t, v in final['state']['control_tables']} == original
    assert final['state']['sequence'] == 62
    seed = dict(control=original, native=baseline, inputs=inputs, sequence=62)
    absence = history.observe_backend_absence(pg, history.DATABASE, output, 'empty-source')
    lab.write_json(output / 'empty-source.json', dict(**seed, files=files,
        frozen=history.FROZEN, backend_absence=absence))
    cases = []

    def dump(label, directory):
        response, _ = history.capture(client, output, label + '-dump', 'dump', '/lab/' + directory.name + '/store')
        assert response.returncode == 0 and not response.stderr
        (output / (label + '-native.json')).write_text(response.stdout)
        return json.loads(response.stdout)

    def execute(label, mode, database, config, directory, predecessor=None):
        before, native, present = rows(database), dump(label + '-before', directory), history.inputs_at(directory / 'store')
        lab.write_json(output / (label + '-before.json'), before)
        lab.write_json(output / (label + '-inputs.json'), present)
        window = dict(before_us=history.server_clock(pg, database))
        lab.write_json(output / (label + '-clock.json'), window)
        command = {'empty-inspect': 'empty-inspect', 'empty-cancelled': 'empty-cancelled'}.get(mode, 'empty-retire')
        response, seconds = history.capture(client, output, label, command, database,
            '/run/secrets/application', config)
        window['after_us'] = history.server_clock(pg, database)
        lab.write_json(output / (label + '-clock.json'), window)
        absence = history.observe_backend_absence(pg, database, output, label)
        # Preserve external terminal observations before any oracle assertion.
        after, final_native = rows(database), dump(label + '-after', directory)
        lab.write_json(output / (label + '-after.json'), after)
        assert response.returncode == 0 and not response.stderr, (label, response.stderr)
        events = [json.loads(line) for line in response.stdout.splitlines()]
        backends = [e for e in events if e['event'] == 'successor-backend']
        assert len(backends) == 2 and all(e['database'] == database for e in backends)
        context = dict(seed=seed, server_window=window, predecessor=predecessor)
        checked = verify(events, before, native, present, mode, **context)
        mutations = negative_controls(events, before, native, present, mode, **context)
        final, = [e for e in events if e['event'] == 'successor-state' and e['stage'] == 'empty-final']
        assert after == {t: json.loads(v) for t, v in final['state']['control_tables']}
        assert final_native == dict(native, rows=final['rows'])
        assert history.inputs_at(directory / 'store') == present
        prior = dict(events=events, original=before, native=native, inputs=present, mode=mode,
            seed=seed, server_window=window)
        lab.write_json(output / (label + '-verification.json'), prior)
        return dict(name=label, mode=mode, status='PASS', seconds=seconds, oracle=checked,
            negative_controls=mutations, server_window=window, backend_absence=absence,
            verification_sha256=digest(output / (label + '-verification.json'))), prior

    try:
        for mode in CASES:
            database = clone(mode, history.DATABASE)
            directory = output / mode
            directory.mkdir(mode=0o700)
            shutil.copytree(store, directory / 'store')
            assert dump(mode + '-seed', directory) == baseline
            assert history.inputs_at(directory / 'store') == inputs
            config = configuration(pg, client, output, database, directory, original)
            if mode != 'empty-old-schema':
                upgrade(pg, client, output, database, config, mode, rows)
            guard_before = json.loads(lab.sql(pg, database, 'SELECT ' + GUARDS))
            require_guards(guard_before)
            guards = run_prepared(pg, database, output, rows, mode + '-prepared') if mode == 'empty-normal' else None
            statement = {
                'empty-missing-intent': 'DELETE FROM control.initial_ingest_successions',
                'empty-missing-outcome': 'DELETE FROM control.producer_outcomes WHERE producer_epoch=1 AND request_sequence=3',
            }.get(mode)
            if statement:
                # Explicit corruption of only this new derivative, never an application write.
                lab.sql(pg, database, 'BEGIN; SET LOCAL session_replication_role=replica; ' + statement + '; COMMIT;')
            removed = None
            if mode == 'empty-missing-input':
                name, = [n for n in inputs if n.endswith('-0000000000000004.cbor')]
                removed = directory / 'removed-input.cbor'
                (directory / 'store/producer-inputs-v1' / name).rename(removed)
            if mode == 'empty-nonempty-target':
                response, _ = history.capture(client, output, mode + '-corruption', 'history-corrupt',
                    '/lab/' + mode + '/store', 'target-nonzero')
                assert response.returncode == 0 and not response.stderr
                change = [json.loads(line) for line in response.stdout.splitlines()]
                assert [e['phase'] for e in change] == ['before', 'after']
                assert change[0]['rows'] == baseline['rows'] and change[0]['sequence'] == 62 and change[1]['sequence'] == 63
                assert change[1]['rows'] == history.expected_native('history-nonempty-target', seed)['rows']
            if mode == 'empty-native-first-failure':
                guarded_fault(pg, database, output, mode)
            attempt, prior = execute(mode, mode, database, config, directory)
            attempts = [attempt]
            if mode == 'empty-native-first-failure':
                assert guarded_fault(pg, database, output, mode, remove=True) == guard_before
                attempt, _ = execute(mode + '-recovered', 'empty-fresh-completion', database, config, directory, prior)
                attempts.append(attempt)
            if mode == 'empty-normal':
                terminal = run_terminal(pg, database, output, rows, mode + '-terminal')
                attempt, _ = execute(mode + '-terminal', 'empty-terminal-retry', database, config, directory, prior)
                attempts.append(attempt)
                guards = dict(prepared=guards, terminal=terminal)
            if removed:
                assert dict(bytes=removed.stat().st_size, sha256=digest(removed)) == inputs[name]
            guard_after = json.loads(lab.sql(pg, database, 'SELECT ' + GUARDS))
            require_guards(guard_after)
            assert guard_after == guard_before
            cases.append(dict(name=mode, status='PASS', attempts=attempts, guards=guards,
                removed_input_retained=removed is not None, guard_catalog_unchanged=True))
            lab.write_json(output / 'empty-cases.json', cases)
        # Shared original-input resolver changed; both old history and gap paths
        # are exercised on their existing independent fixtures with these binaries.
        from initial_gap import run as gap_regression
        regression = gap_regression(pg, client, output, rows, clone)
        assert regression['status'] == 'PASS'
        lab.write_json(output / 'empty-gap-regression.json', regression)
    finally:
        preserved = dict(control=rows(history.DATABASE) == original, files=census(store) == files,
            artifacts=all(digest(basis / n) == h for n, h in history.FROZEN.items()))
        lab.write_json(output / 'empty-source-preserved.json', preserved)
        assert all(preserved.values()), preserved
    return dict(name='initial-empty-retirement', status='PASS', cases=cases,
        source_lab=str(basis.parent), source_revision=history.SOURCE, source_owner=history.OWNER,
        source_preserved=preserved, source_artifacts=history.FROZEN, gap_regression=regression,
        scope='exact empty epoch2 retirement and config-only recovery after native close; no attempted incarnation activation',
        excluded='ambiguous retirement COMMIT, concurrent claim races, native I/O/power loss, next incarnation and installed restart/resend/M1')
