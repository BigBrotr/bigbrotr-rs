"""Finite live activation from a fresh local candidate and retired signed seed.

Five isolated copies qualify zero activation, bounded local admission with both
provenance settings, and the actual activation COMMIT response boundary. They
do not qualify renewal, raw SQL adversaries, network intake or installed CLI.
The outer bindings runner owns all containers and unknown-timeout cleanup.
"""
from datetime import datetime
import json
from pathlib import Path
import re
import shutil
import subprocess

import lab
import initial_succession as preparing
from initial_gap import configuration
from initial_gap_policy import GUARDS, require_guards
from initial_history import capture, inputs_at, observe_backend_absence, server_clock
from initial_live_oracle import verify, negative_controls

CASES = ('successor-live-zero', 'successor-live-normal', 'successor-live-off',
    'successor-live-commit-before', 'successor-live-commit-after')
FUNCTIONS = (
    'veto_unactivated_producer()', 'require_prepared_initial_successor(bytea)',
    'guard_initial_ingest_succession()',
    'initial_empty_retirement_digest(control.initial_ingest_successions)',
    'lock_initial_empty_retirement(bytea,bytea)',
    'require_initial_empty_claims(control.initial_ingest_successions)',
    'require_empty_initial_retirement(bytea,bytea)',
    'require_initial_empty_transition(bytea,bytea,bigint)',
    'require_complete_initial_empty_retirement()', 'require_active_initial_writer(bytea)',
    'guard_initial_ingest_activation()', 'require_initial_activation_commit()',
    'guard_initial_activation_checkpoint()', 'guard_initial_activation_target()',
)


def catalog(pg, database):
    observed = json.loads(lab.sql(pg, database, 'SET search_path=pg_catalog; SELECT ' + GUARDS))
    require_guards(observed)
    selected = {(t['table'], t['name']) for t in observed['triggers'] if not t['internal']}
    assert {('initial_ingest_activations', n) for n in (
        'initial_ingest_activation_guard', 'initial_ingest_activation_truncate_guard',
        'initial_activation_complete')} <= selected
    assert {('producer_checkpoints', 'initial_activation_checkpoint_guard'),
        ('producer_requests', 'producer_request_activation_guard'),
        ('producer_input_bindings', 'input_binding_activation_guard')} <= selected
    assert {(t, 'initial_activation_target_guard') for t in ('datasets', 'owner_epochs',
        'role_instances', 'allocation_domains', 'source_streams', 'producer_epochs')} <= selected
    complete, = [c for c in observed['constraints'] if c['name'] == 'initial_activation_complete']
    assert complete['deferrable'] and complete['deferred']
    names = ','.join("'control." + f + "'::regprocedure" for f in FUNCTIONS)
    observed['functions'] = json.loads(lab.sql(pg, database,
        "SET search_path=pg_catalog; SELECT json_agg(json_build_object('name',p.oid::regprocedure::text,"
        "'definition',pg_get_functiondef(p.oid),'execute',has_function_privilege("
        "'bigbrotr_control',p.oid,'EXECUTE')) ORDER BY p.oid::regprocedure::text) "
        'FROM pg_proc p WHERE p.oid IN (' + names + ')'))
    assert len(observed['functions']) == len(FUNCTIONS)
    assert all(f['execute'] is True for f in observed['functions'])
    observed['tables'] = json.loads(lab.sql(pg, database,
        "SELECT json_build_object('succession_select',has_table_privilege('bigbrotr_control',"
        "'control.initial_ingest_successions','SELECT'),'succession_insert',has_table_privilege("
        "'bigbrotr_control','control.initial_ingest_successions','INSERT'),"
        "'succession_lock',has_table_privilege('bigbrotr_control','control.initial_ingest_successions','UPDATE'),"
        "'activation_select',has_table_privilege('bigbrotr_control','control.initial_ingest_activations','SELECT'),"
        "'activation_insert',has_table_privilege('bigbrotr_control','control.initial_ingest_activations','INSERT'))"))
    assert all(v is True for v in observed['tables'].values())
    return observed


def upgrade(pg, client, output, database, config, mode, rows):
    """Apply the exact additive catalog; preserve every prior row and filenode."""
    before, files = rows(database), preparing.physical(pg, database)
    assert len(before) == 70
    lab.write_json(output / (mode + '-upgrade-before.json'), dict(control=before, physical=files))
    after = None
    for suffix in ('migration', 'reapply'):
        try:
            response = lab.cli(dict(pg, client=client), 'migrate', 'apply', '--file', config,
                '--user', 'bigbrotr_migrator', '--password-ref', 'file:/run/secrets/migrator', '--target', '32')
        except subprocess.TimeoutExpired as error:
            def partial(value):
                return value.decode(errors='replace') if isinstance(value, bytes) else value or ''
            lab.write_json(output / (mode + '-' + suffix + '.json'), dict(returncode=None,
                outcome='UNKNOWN_TIMEOUT', stdout=partial(error.stdout), stderr=partial(error.stderr),
                cleanup='outer owner-checked client stop; migration COMMIT outcome is not inferred'))
            raise
        lab.write_json(output / (mode + '-' + suffix + '.json'), dict(returncode=response.returncode,
            stdout=response.stdout, stderr=response.stderr))
        assert response.returncode == 0, response.stderr
        current = rows(database)
        lab.write_json(output / (mode + '-' + suffix + '-rows.json'), current)
        if after is not None:
            assert current == after
        after = current
    assert set(after) == set(before) | {'initial_ingest_successions', 'initial_ingest_activations'}
    assert len(after) == 72 and not after['initial_ingest_successions'] and not after['initial_ingest_activations']
    assert all(after[t] == before[t] for t in before if t != 'schema_versions')
    assert all(r in after['schema_versions'] for r in before['schema_versions'])
    added = [r for r in after['schema_versions'] if r not in before['schema_versions']]
    assert {r['migration'] for r in added} == {30, 31, 32} and len(added) == 3
    assert len({r['xmin'] for r in added}) == 1
    assert all(r['xmin'] != added[0]['xmin'] for entries in before.values() for r in entries)
    root = Path(__file__).resolve().parents[2]
    for number, name in ((30, '0030_initial_succession.sql'),
                         (31, '0031_initial_empty_retirement.sql'), (32, '0032_initial_activation.sql')):
        entry, = [r for r in added if r['migration'] == number]
        assert entry['name'] == name
        assert entry['checksum'] == '\\x' + preparing.digest(root / 'migrations/postgres' / name)
        assert entry['reader_min'] == entry['reader_max'] == entry['writer_floor'] == 1
        assert entry['engine'] == 'postgres-control' and entry['applied_by'] == 'bigbrotr_migrator'
    final_files = preparing.physical(pg, database)
    assert all(final_files[t] == value for t, value in files.items())
    # Explicit fixture provisioning, not an implicit product application grant.
    # UPDATE on succession is required by row-locking SELECT, despite immutability.
    grants = ('GRANT SELECT,INSERT,UPDATE ON control.initial_ingest_successions TO bigbrotr_control;\n'
        'GRANT SELECT,INSERT ON control.initial_ingest_activations TO bigbrotr_control;\n'
        'GRANT EXECUTE ON FUNCTION ' + ','.join('control.' + f for f in FUNCTIONS) + ' TO bigbrotr_control;\n')
    (output / (mode + '-grants.sql')).write_text(grants)
    lab.sql(pg, database, grants)
    assert rows(database) == after and preparing.physical(pg, database) == final_files
    observed = catalog(pg, database)
    lab.write_json(output / (mode + '-upgrade.json'), dict(before=before, after=after,
        physical_before=files, physical_after=final_files, exact_reapply=True, catalog=observed))
    return observed


def exact_absence(pg, database, output, mode, after, before):
    """Independently exclude the frozen actual Control backend from SQL rows."""
    assert re.fullmatch('[a-z][a-z0-9_]+', database)
    previous = {(r['resource_id'], r['epoch']) for r in before['owner_epochs'] if r['role'] == 'control'}
    current, = [r for r in after['owner_epochs'] if r['role'] == 'control'
        and (r['resource_id'], r['epoch']) not in previous]
    expected = dict(pid=current['control_backend_pid'], backend_start=current['control_backend_start'],
        database=database, control_epoch=current['epoch'])
    assert type(expected['pid']) is int and expected['pid'] > 0
    assert datetime.fromisoformat(expected['backend_start']).tzinfo is not None
    assert "'" not in expected['backend_start']
    answer = lab.docker('exec', '-i', pg['postgres'], 'psql', '-X', '-A', '-t', '-q',
        '--no-password', '-v', 'ON_ERROR_STOP=1', '-U', 'postgres', '-d', database,
        data="SELECT EXISTS(SELECT 1 FROM pg_catalog.pg_stat_activity WHERE pid=" + str(expected['pid'])
        + " AND backend_start='" + expected['backend_start'] + "'::timestamptz AND datname='" + database + "')",
        check=False, timeout=5)
    result = dict(expected=expected, returncode=answer.returncode, stdout=answer.stdout,
        stderr=answer.stderr, present=answer.stdout.strip() == 't')
    lab.write_json(output / (mode + '-backend-exact.json'), result)
    assert answer.returncode == 0 and not answer.stderr and answer.stdout.strip() == 'f'
    return result


def run(pg, client, output, rows, clone):
    basis = output.parent.parent / preparing.SEED / 'bindings'
    store = basis / 'retire_signed/store'
    assert all(preparing.digest(basis / n) == h for n, h in preparing.FROZEN.items())
    retained = json.loads((basis / 'result.json').read_text())
    assert retained['status'] == 'PASS_INSTALLED_INITIAL_RETIREMENT_FIRST_WAVE'
    assert retained['source'] == preparing.SOURCE and retained['owner'] == preparing.OWNER
    assert not retained['cleanup_errors'] and not retained['unrelated_changed']
    containers = json.loads(lab.docker('inspect', *lab.docker('ps', '-aq').stdout.split()).stdout)
    old, = [c for c in containers if c['Id'] == retained['client']['id']]
    assert old['Config']['Labels'][retained['label']] == preparing.OWNER
    assert not old['State']['Running'] and not old['State']['Pid'] and not old['State']['OOMKilled']
    assert not any(c['State']['Running'] and any(str(basis) in m.get('Source', '')
        for m in c['Mounts']) for c in containers)
    absent = observe_backend_absence(pg, preparing.DATABASE, output, 'live-source')
    original = rows(preparing.DATABASE)
    assert original == json.loads((basis / 'retire-signed-retry-after.json').read_text())
    baseline = json.loads((basis / 'retire-signed-retry-native.json').read_text())
    files, inputs = preparing.census(store), inputs_at(store)
    assert len(original) == 70 and len(baseline['rows']) == 49 and len(inputs) == 4
    assert sum(v['bytes'] for v in files.values()) < 1024**2
    seed = dict(control=original, native=baseline, inputs=inputs, sequence=58)
    lab.write_json(output / 'live-source.json', dict(**seed, files=files,
        frozen=preparing.FROZEN, backend_absence=absent, client_state=old['State']))
    cases = []

    def dump(label, directory):
        response, _ = capture(client, output, label, 'dump-state', '/lab/' + directory.name + '/store')
        assert response.returncode == 0 and not response.stderr, response.stderr
        observed = json.loads(response.stdout)
        lab.write_json(output / (label + '-native.json'), observed)
        assert observed['event'] == 'dump-state' and type(observed['sequence']) is int
        return observed

    try:
        for mode in CASES:
            database = clone(mode, preparing.DATABASE)
            assert rows(database) == original
            directory = output / mode
            directory.mkdir(mode=0o700)
            shutil.copytree(store, directory / 'store')
            observe_backend_absence(pg, database, output, mode + '-before')
            before_native = dump(mode + '-before', directory)
            assert before_native == dict(event='dump-state', rows=baseline['rows'], sequence=58)
            assert inputs_at(directory / 'store') == inputs
            config = configuration(pg, client, output, database, directory, original)
            guards = upgrade(pg, client, output, database, config, mode, rows)
            before = rows(database)
            lab.write_json(output / (mode + '-before.json'), before)
            lab.write_json(output / (mode + '-inputs-before.json'), inputs)
            wire_mode = {'successor-live-commit-before': 'before-forward',
                'successor-live-commit-after': 'after-complete'}.get(mode)
            clock = dict(before_us=server_clock(pg, database))
            lab.write_json(output / (mode + '-clock.json'), clock)
            try:
                response, elapsed, wire = preparing.invoke(client, output, database, config, mode,
                    wire_mode, commit_number=13)
            finally:
                # Wire qualification can fail after a known process exit. Save
                # independent terminal state even on that exception path.
                receipt = output / (mode + '-response.json')
                completed = receipt.exists() and json.loads(receipt.read_text())['returncode'] is not None
                if completed:
                    clock['after_us'] = server_clock(pg, database)
                    lab.write_json(output / (mode + '-clock.json'), clock)
                    after = rows(database)
                    lab.write_json(output / (mode + '-after.json'), after)
                    after_inputs = inputs_at(directory / 'store')
                    lab.write_json(output / (mode + '-inputs-after.json'), after_inputs)
                    absence = observe_backend_absence(pg, database, output, mode)
                    # Known process exit plus observed absence of every Control
                    # backend permits reopening even if an early failure never
                    # claimed a Control row. Preserve native state before the
                    # stricter tuple-census qualification can reject that run.
                    after_native = dump(mode + '-after', directory)
                    excluded_backend = exact_absence(pg, database, output, mode, after, before)
                else:
                    lab.write_json(output / (mode + '-external-unavailable.json'), dict(
                        status='NOT_RUN_UNKNOWN_PROCESS_OUTCOME',
                        reason='Native directory may still have an owner; outer cleanup must complete first'))
            assert response.returncode == 0 and not response.stderr, (mode, response.stderr)
            events = [json.loads(line) for line in response.stdout.splitlines()]
            announced = [e for e in events if e['event'] == 'successor-backend']
            assert announced == [dict(event='successor-backend', stage='initial',
                **excluded_backend['expected'], present=value) for value in (True, False)]
            assert 0 < clock['before_us'] <= clock['after_us'] <= clock['before_us'] + 120_000_000
            final, = [e for e in events if e['event'] == 'successor-state' and e['stage'] == 'final']
            assert after == {t: json.loads(v) for t, v in final['state']['control_tables']}
            assert after_native == dict(event='dump-state', rows=final['rows'], sequence=final['state']['sequence'])
            assert after_inputs == dict(final['state']['originals'])
            assert all(after_inputs[n] == identity for n, identity in inputs.items())
            after_guards = catalog(pg, database)
            lab.write_json(output / (mode + '-catalog-after.json'), after_guards)
            assert after_guards == guards
            context = dict(events=events, original=before, native=baseline, inputs=inputs,
                mode=mode, seed=seed, server_window=clock)
            lab.write_json(output / (mode + '-verification.json'), context)
            evidence = verify(**context)
            mutations = negative_controls(**context)
            if wire_mode:
                assert wire['status'] == 'PASS' and not wire['forced_wrapper_stop']
            cases.append(dict(name=mode, status='PASS', seconds=elapsed, oracle=evidence,
                negative_controls=mutations, commit_wire=wire, backend_absence=absence,
                exact_backend=excluded_backend, external_final_matches=True, catalog_unchanged=True))
            lab.write_json(output / 'live-cases.json', cases)
    finally:
        preserved = dict(control=rows(preparing.DATABASE) == original,
            files=preparing.census(store) == files, inputs=inputs_at(store) == inputs,
            artifacts=all(preparing.digest(basis / n) == h for n, h in preparing.FROZEN.items()))
        lab.write_json(output / 'live-source-preserved.json', preserved)
        assert all(preserved.values()), preserved
    return dict(name='live-initial-activation', status='PASS', cases=cases,
        source_lab=str(basis.parent), source_revision=preparing.SOURCE, source_owner=preparing.OWNER,
        source_preserved=preserved, source_artifacts=preparing.FROZEN,
        scope='fresh local candidate activation, exact first admissions and actual activation COMMIT loss',
        not_run=['raw SQL activation adversaries', 'controlled renewal', 'network acquisition',
            'installed CLI live writer', 'activated writer replacement', 'ACK/GC', 'full M1'])
