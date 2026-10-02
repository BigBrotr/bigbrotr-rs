"""Finite job-count boundaries and preserving an already populated gap ledger."""
import hashlib
import json
from pathlib import Path

import lab
from acquisition_abandonment import duplicate_intent


def run(pg, client, output, rows, prepare, upgrade, invoke, cases):
    for count in (64, 65):
        name = f'abandon_limit_{count}'
        database, config, directory = prepare(name)
        upgrade(database, config, name)
        original = rows(database)
        assert len(original['acquisition_jobs']) == 1 and not original['acquisition_gaps']
        added = [duplicate_intent(pg, database, original, True, serial=n) for n in range(count-1)]
        seeded = rows(database)
        assert len(set(added)) == count-1 and len(seeded['acquisition_jobs']) == count
        assert all(r['acquisition_job_id'] not in added for r in seeded['producer_requests'])
        if count == 64:
            invoke(database, config, directory, 'abandon-limit-64', expected=64)
            invoke(database, config, directory, 'abandon-limit-64-retry', expected=64)
            # Retained completed gaps and a new original-owner claim share the
            # same finite budget. Checking only one query's count would accept65.
            duplicate_intent(pg, database, original, True, serial=63)
            invoke(database, config, directory, 'abandon-limit-mixed-65', error='InsufficientReservation')
        else:
            invoke(database, config, directory, 'abandon-limit-65', error='InsufficientReservation')

    parent = 'bb_scope_bind_bff0bc3_abandon_signed'
    basis = output.parent.parent / 'ingest-recovery-engine-bff0bc3' / 'bindings'
    retained = rows(parent)
    assert retained == json.loads((basis / 'abandon-signed-retry-after.json').read_text())
    assert len(retained['acquisition_gaps']) == 1
    assert 27 in {r['migration'] for r in retained['schema_versions']}
    assert 28 not in {r['migration'] for r in retained['schema_versions']}
    database, config, directory = prepare('gap_populated_upgrade', template=parent)
    try:
        def physical():
            return json.loads(lab.sql(pg, database, """
                SELECT jsonb_object_agg(relname,pg_relation_filenode(c.oid))
                FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
                WHERE n.nspname='control' AND c.relkind='r'
            """))
        before = rows(database); files = physical()
        assert before == retained
        def apply():
            return lab.cli(dict(pg, client=client), 'migrate', 'apply', '--file', config,
                '--user', 'bigbrotr_migrator', '--password-ref', 'file:/run/secrets/migrator', '--target', '28')
        response = apply()
        lab.write_json(output / 'gap-populated-migration.json', dict(returncode=response.returncode,
            stdout=response.stdout, stderr=response.stderr))
        assert response.returncode == 0 and not response.stderr, response
        after = rows(database)
        assert before.keys() == after.keys()
        assert all(before[t] == after[t] for t in before if t != 'schema_versions')
        added, = [r for r in after['schema_versions'] if r not in before['schema_versions']]
        assert added['migration'] == 28
        sql = Path(__file__).resolve().parents[2] / 'migrations/postgres/0028_acquisition_gap_guard.sql'
        assert added['checksum'] == '\\x' + hashlib.sha256(sql.read_bytes()).hexdigest()
        assert all(r in after['schema_versions'] for r in before['schema_versions'])
        assert physical() == files
        repeat = apply()
        assert repeat.returncode == 0 and rows(database) == after and physical() == files
        lab.write_json(output / 'gap-populated-upgrade.json', dict(before=before, after=after,
            physical_before=files, physical_after=physical(), exact_reapply=True))
        cases.append(dict(name='gap-populated-upgrade', status='PASS', control_tables=70,
            prior_rows_xmin_filenodes_preserved=True, exact_reapply=True))
        invoke(database, config, directory, 'gap-populated-upgrade-retry', expected=1)
    finally:
        preserved = rows(parent) == retained
        lab.write_json(output / 'gap-populated-parent-preserved.json', dict(database=parent, preserved=preserved))
        assert preserved
