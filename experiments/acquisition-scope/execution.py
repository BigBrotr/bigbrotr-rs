"""Job-bound real admission under the existing isolated binding laboratory.

The launcher owns lifecycle and original template audits. This module supplies
only this wave's setup, independent byte/row expectations and probe invocation.
"""
import hashlib
import json


def run(lab_context):
    lab, pg, client, output, source, rows, clone, physical, report, save, require = (
        lab_context[key] for key in ('lab', 'pg', 'client', 'output', 'SOURCE', 'rows',
                                    'clone', 'physical', 'report', 'save', 'require'))
    sha = lambda value: hashlib.sha256(value).hexdigest()
    report['not_run'] = ['job-bound socket polling and deadline delivery',
        'lost COMMIT, cancellation, authority races and historical recovery',
        'raw SQL relational negatives and legacy job adoption',
        'installed Sync/M1, load/aged-state, native arm64/release']

    def upgrade(database, name, target):
        before, files = rows(database), physical(database)
        config = lab.config(output, dict(pg, client=client), database,
                            endpoint='127.0.0.1:5432', boundary='loopback')
        args = ('migrate', 'apply', '--file', config, '--user', 'bigbrotr_migrator',
                '--password-ref', 'file:/run/secrets/migrator', '--target', str(target))
        result = lab.cli(dict(pg, client=client), *args)
        (output / f'{name}-migration.stdout').write_text(result.stdout)
        (output / f'{name}-migration.stderr').write_text(result.stderr)
        require(result.returncode == 0, 'actual-execution-upgrade')
        after = rows(database)
        comparable = json.loads(json.dumps(after))
        for request in comparable['producer_requests']:
            for key in ('acquisition_scope_id', 'acquisition_job_id'):
                if key not in (before['producer_requests'][0] if before['producer_requests'] else {}):
                    require(request.pop(key, None) is None, 'no-historical-attribution')
        for job in comparable['jobs']:
            if 'acquisition_job_protocol' not in (before['jobs'][0] if before['jobs'] else {}):
                require(job.pop('acquisition_job_protocol', None) is None, 'no-historical-job-adoption')
        require(all(comparable[t] == before[t] for t in before if t != 'schema_versions'),
                'upgrade-rewrote-old-rows-xmin')
        added = [r for r in after['schema_versions'] if r not in before['schema_versions']]
        catalog = {r['version']: r['name'] for r in json.loads((source / 'migrations/manifest.json').read_text())['application_order']}
        prior = {r['migration'] for r in before['schema_versions']}
        wanted = [v for v in (4,21,22,23,24) if v not in prior and (target == 24 or v != 24)]
        require({r['migration'] for r in added} == set(wanted), 'exact-additive-ledger')
        require(all(r['checksum'] == '\\x' + sha((source / 'migrations/postgres' / catalog[r['migration']]).read_bytes())
                    for r in added), 'exact-migration-checksums')
        require(all(row in after['schema_versions'] for row in before['schema_versions']), 'old-ledger-preserved')
        current_files = physical(database)
        require(all(current_files[t] == value for t, value in files.items()), 'unexpected-heap-rewrite')
        repeated = lab.cli(dict(pg, client=client), *args)
        (output / f'{name}-reapply.stdout').write_text(repeated.stdout)
        (output / f'{name}-reapply.stderr').write_text(repeated.stderr)
        require(repeated.returncode == 0 and rows(database) == after and physical(database) == current_files,
                'identical-reapply')
        lab.write_json(output / f'{name}-upgrade.json', dict(before=before, after=after,
            physical_before=files, physical_after=current_files))
        lab.sql(pg, database, 'GRANT SELECT,INSERT ON control.acquisition_selectors,control.acquisition_scopes,'
            'control.acquisition_jobs TO bigbrotr_control; GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA control TO bigbrotr_control;')

    populated = clone('execution-populated', 'bb_scope_54d1a18_basic')
    require(rows(populated)['producer_requests'], 'nonempty-upgrade')
    upgrade(populated, 'execution-populated', 24)
    report['cases'].append(dict(name='populated-upgrade', status='PASS', prior_rows_xmin_filenodes_unchanged=True))
    save()
    cases = ['pass','changed','omitted','unbound','invalid-unbound','certify-unbound',
             'accepted','scope','expired','certify-expired','withdrawn','old-schema','dispatch']
    for name in cases:
        database = clone('execution-' + name, 'bb_source_template')
        upgrade(database, name, 23 if name == 'old-schema' else 24)
        lab_context['arrival_policy'](database, name)
        lab.sql(pg, database, "INSERT INTO control.principal_grants SELECT dataset_id,principal_id,"
            "'roles.control','dataset',dataset_id FROM control.local_accounts WHERE username='alice';")
        if name == 'dispatch':
            # A committed reservation from an earlier transaction is the barrier.
            # The original helper still runs at every call and deferred COMMIT.
            lab.sql(pg, database, """
                ALTER FUNCTION control.require_acquisition_execution(bytea,bytea,bytea,bytea,bytea)
                    RENAME TO require_acquisition_execution_original;
                CREATE FUNCTION control.require_acquisition_execution(d bytea,j bytea,s bytea,e bytea,a bytea)
                RETURNS bigint LANGUAGE plpgsql AS $$ DECLARE remaining bigint; BEGIN
                    remaining := control.require_acquisition_execution_original(d,j,s,e,a);
                    IF EXISTS (SELECT 1 FROM control.producer_requests
                        WHERE dataset_id=d AND acquisition_job_id=j
                            AND xmin::text <> pg_current_xact_id()::text) THEN
                        RAISE EXCEPTION 'isolated post-reservation refusal' USING ERRCODE='BB001';
                    END IF;
                    RETURN remaining;
                END $$;
                REVOKE ALL ON FUNCTION control.require_acquisition_execution(bytea,bytea,bytea,bytea,bytea) FROM PUBLIC;
                GRANT EXECUTE ON FUNCTION control.require_acquisition_execution(bytea,bytea,bytea,bytea,bytea) TO bigbrotr_control;
            """)
        before = rows(database)
        lab.write_json(output / f'{name}-before.json', before)
        result = lab.docker('exec', client, '/build/ingest_bootstrap_probe', 'source-job-' + name,
            database, '/run/secrets/application', '/lab/' + name, check=False, timeout=90)
        (output / f'{name}.stdout').write_text(result.stdout)
        (output / f'{name}.stderr').write_text(result.stderr)
        require(result.returncode == 0 and not result.stderr, 'actual-execution-' + name)
        messages = [json.loads(line) for line in result.stdout.splitlines()]
        finished = messages[-1]
        require(finished['event'] == 'execution-finished' and finished['case'] == name
            and finished['status'] == 'PASS' and finished['zero_writes'] == (name != 'dispatch'), 'effect-comparison')
        after = rows(database)
        lab.write_json(output / f'{name}-after.json', after)
        committed = name in ('pass','changed','omitted','certify-expired','withdrawn')
        dataset = bytes.fromhex(after['datasets'][0]['dataset_id'][2:])
        expected, inputs, _ = lab_context['complete_source'](dataset, committed)
        if name == 'dispatch':
            _, inputs, _ = lab_context['complete_source'](dataset, True)
        dumped = lab.docker('exec', client, '/build/ingest_bootstrap_probe', 'dump', '/lab/' + name)
        (output / f'{name}-native.json').write_text(dumped.stdout)
        actual = [(r['cf'], bytes.fromhex(r['key']), bytes.fromhex(r['value'])) for r in json.loads(dumped.stdout)['rows']]
        require(actual == expected, 'complete-independent-native-bytes')
        originals = sorted((output / name / 'producer-inputs-v1').glob('*.cbor'))
        require(len(originals) == len(inputs) and all(p.read_bytes() == inputs[int(p.stem[-16:],16)] for p in originals),
                'unchanged-original-inputs')
        require(len(after['producer_requests']) == len(inputs)
            and len(after['producer_outcomes']) == (1 if name == 'dispatch' else len(inputs)), 'operational-counts')
        identities = next(m for m in messages if m['event'] == 'execution-identities')
        jobs, scopes = [['\\x' + value for value in identities[key]] for key in ('jobs','scopes')]
        require(len(set(jobs)) == 2 and len(set(scopes)) == 2, 'distinct-fixture-identities')
        require(len(after['jobs']) == len(after['acquisition_jobs']) == 2, 'exact-job-count')
        for job in after['jobs']:
            require(job['job_id'] in jobs and job['state'] == ('accepted' if name == 'accepted' else 'queued')
                and job['job_revision'] == (1 if name == 'accepted' else 2) and job['checkpoint_revision'] == 0,
                'job-state-not-execution-progress')
        for detail in after['acquisition_jobs']:
            job = next(j for j in after['jobs'] if j['job_id'] == detail['job_id'])
            require(detail['scope_id'] == scopes[0] and detail['actor_id'] == '\\x' + '10'*16
                and detail['duration_us'] == (5_000_000 if 'expired' in name else 120_000_000)
                and detail['deadline_at_us'] == job['created_at_us'] + detail['duration_us'], 'original-job-attribution')
        for row in after['producer_requests']:
            sequence = int(row['request_sequence'])
            require(row['request_digest'] == '\\x' + sha(inputs[sequence]) and row['input_bytes'] == len(inputs[sequence]),
                    'original-request-digest')
            require(row.get('acquisition_job_id') == (None if sequence == 1 else jobs[0]), 'original-job-binding')
            if sequence == 2:
                require(row['acquisition_scope_id'] == scopes[0], 'original-scope-binding')
        initial = next(m['state'] for m in messages if m['event'] == 'execution-before')
        if name == 'dispatch':
            old, new = [{n: json.loads(v) for n,v in state['control_tables']} for state in (initial,finished['state'])]
            require({t for t in old if old[t] != new[t]} == {'producer_requests','producer_epochs'}, 'dispatch-only-reservation')
            previous, current = [dict(t['producer_epochs'][0]) for t in (old,new)]
            require(previous.pop('reserved_through_request') == 1 and current.pop('reserved_through_request') == 2, 'reserved-prefix')
            previous.pop('xmin'); current.pop('xmin')
            require(previous == current, 'other-producer-mutation')
        else:
            require(initial == finished['state'], 'all-operation-state-unchanged')
        require(lab.sql(pg,database,"SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND usename='bigbrotr_control'") == '0',
                'unjoined-actual-driver')
        report['cases'].append(dict(name=name,status='PASS',actual_pg_rocks=True,
            complete_native_rows=len(actual),original_inputs=len(inputs)))
        save()
