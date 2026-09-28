"""Original job deadlines through real sockets and PostgreSQL cleanup replies.

The existing transport laboratory owns resources, transcripts and native oracles.
This wave does not inject a deadline or derive expected policy from product code.
"""
import json
import time


def run(context):
    (lab, pg, client, output, source, revision, rows, report, save, require,
     process_type, processes, next_event, rollback_status, verify_final) = (
        context[key] for key in ('lab', 'pg', 'client', 'output', 'SOURCE', 'revision',
            'rows', 'report', 'save', 'require', 'Process', 'processes', 'next_event',
            'rollback_status', 'verify_final'))
    report['evidence_limits'] = [
        'Two-second jobs and five-second source calls qualify finite lifetime, not renewable sessions or capacity.',
        'The public-visibility refusal remains prior schema20 evidence; this wave uses private ordinal2.']
    report['not_run'] = ['job-bound reservation COMMIT faults, cancellation and historical recovery',
        'installed Sync/M1', 'load/aged state', 'native arm64/release']

    def prepare(name):
        database = 'bb_job_transport_' + revision[:7] + '_' + name
        report['active_case'] = name
        save()
        lab.sql(pg, 'postgres', f'CREATE DATABASE {database} OWNER bigbrotr_migrator TEMPLATE bb_source_template')
        report['databases'].append(database)
        require(rows(database) == context['template_before'], 'original-clone-state')
        config = lab.config(output, dict(pg, client=client), database,
                            endpoint='127.0.0.1:5432', boundary='loopback')
        result = lab.cli(dict(pg, client=client), 'migrate', 'apply', '--file', config,
            '--user', 'bigbrotr_migrator', '--password-ref', 'file:/run/secrets/migrator', '--target', '24')
        (output / f'{name}-migration.stdout').write_text(result.stdout)
        (output / f'{name}-migration.stderr').write_text(result.stderr)
        require(result.returncode == 0 and len(rows(database)) == 67, 'actual-schema24')
        lab.sql(pg, database, '''
            GRANT SELECT, INSERT ON control.acquisition_selectors, control.acquisition_scopes,
                control.acquisition_jobs TO bigbrotr_control;
            GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA control TO bigbrotr_control;
            INSERT INTO control.principal_grants
                SELECT dataset_id, principal_id, 'roles.control', 'dataset', dataset_id
                FROM control.local_accounts WHERE username='alice';
        ''')
        return database

    def check_job(state, ready, name):
        require(len(state) == 67 and len(state['jobs']) == len(state['acquisition_jobs']) == 1,
                'one-job-complete-state')
        job, detail = state['jobs'][0], state['acquisition_jobs'][0]
        require(job['job_id'] == detail['job_id'] == '\\x' + ready['job']
            and detail['scope_id'] == '\\x' + ready['scope']
            and detail['actor_id'] == '\\x' + '10' * 16
            and detail['duration_us'] == 2_000_000
            and detail['deadline_at_us'] == job['created_at_us'] + 2_000_000,
            'authentic-original-job-duration')
        require(job['state'] == ('accepted' if name == 'unclaimed' else 'queued')
            and job['job_revision'] == (1 if name == 'unclaimed' else 2)
            and job['checkpoint_revision'] == 0, 'no-execution-progress-from-socket')
        require(len(state['producer_requests']) == len(state['producer_outcomes']) == 1,
                'bootstrap-only-producer-effects')

    baseline = None
    for name in ('pass', 'pending', 'held', 'ready', 'omitted', 'unclaimed', 'expired'):
        database = prepare(name)
        target = baseline + 1 if name in ('held', 'ready') else 0
        proxy = process_type(['docker', 'exec', '-i', client, 'python3', '-B',
            '/source/experiments/relay-source/rollback_proxy.py', '--upstream-port', '5432',
            '--rollback-number', str(target)], input_pipe=True)
        processes.append((name + '-proxy', proxy))
        proxy_ready = next_event(proxy, 'ready')
        require(proxy_ready['proxy_sha256'] == context['sha'](
            (source / 'experiments/control-migrations/fault_proxy.py').read_bytes()), 'actual-proxy-source')
        probe = process_type(['docker', 'exec', client, '/build/ingest_bootstrap_probe',
            'source-transport-job-' + name, database, '/run/secrets/application', '/lab/' + name],
            limit=8 * 1024**2)
        processes.append((name + '-probe', probe))
        next_event(probe, 'initialized')
        ready = next_event(probe, 'job-source-ready')
        before = rows(database)
        require(before == {n: json.loads(v) for n, v in ready['state']['control_tables']}, 'ready-state')
        check_job(before, ready, name)
        lab.write_json(output / f'{name}-before.json', before)
        owner = next(r for r in before['owner_epochs'] if r['role'] == 'control')
        require(owner['control_backend_pid'] == ready['pid'] and owner['state'] == 'active', 'actual-Control')
        proxy.send('status')
        initial = next_event(proxy, 'status')
        counts = [initial[k] for k in ('rollback_requests', 'rollback_completions',
                                       'rollback_released', 'rollback_ready')]
        require(len(set(counts)) == 1, 'setup-rollback-complete')
        if baseline is None:
            baseline = counts[0]
        require(counts == [baseline] * 4, 'stable-setup-rollback-count')
        (output / (name + '.go')).touch(exist_ok=False)
        start = next_event(probe, 'job-source-start')
        require(start['remaining_us'][1] > 5_000_000, 'live-Control-before-source')
        require(start['remaining_us'][0] <= 0 if name == 'expired'
                else start['remaining_us'][0] > 1_000_000, 'original-job-before-source')
        value = peer = held = released = forwarded = observation = expiry = None
        if name in ('pass', 'held', 'ready'):
            value = next_event(probe, 'job-source-value')
            require(value['elapsed_us'] < 500_000, 'actual-socket-before-job-expiry')
        if name in ('pass', 'pending', 'held', 'ready'):
            peer = next_event(probe, 'job-source-peer-closed')
            lower, upper = (900_000, 3_000_000) if name == 'pending' else (0, 500_000)
            require(lower <= peer['elapsed_us'] < upper, 'finite-socket-close')
        if target:
            held = next_event(proxy, 'held')
            require([held[k] for k in ('rollback_requests', 'rollback_completions',
                    'rollback_released', 'rollback_ready')] == [target, target, target-1, target-1],
                    'exact-rollback-hold')
            observation = json.loads(lab.sql(pg, database, f'''
                SELECT json_build_object('pid', pid, 'backend_start', backend_start::text,
                    'state', state, 'xact_start', xact_start)
                FROM pg_stat_activity WHERE pid={ready['pid']}
            '''))
            require(observation and observation['backend_start'] == ready['backend_start']
                and observation['state'] == 'idle' and observation['xact_start'] is None,
                'actual-server-rollback-complete')
            if name == 'held':
                end = time.monotonic() + 3
                while True:
                    expiry = json.loads(lab.sql(pg, database, '''
                        SELECT json_build_array(a.deadline_at_us-now.us, o.lease_expires_at_us-now.us)
                        FROM control.acquisition_jobs a JOIN control.owner_epochs o
                            ON o.dataset_id=a.dataset_id AND o.role='control'
                        CROSS JOIN LATERAL (
                            SELECT (EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint AS us
                        ) now
                    '''))
                    require(expiry[1] > 5_000_000, 'Control-must-not-explain-refusal')
                    if expiry[0] <= 0:
                        break
                    require(time.monotonic() < end, 'original-job-not-expired')
                    time.sleep(.03)
            proxy.send('release')
            released = next_event(proxy, 'released')
            forwarded = rollback_status(proxy, target)
            if name == 'ready':
                require(forwarded['elapsed_ns'] - initial['elapsed_ns'] < 500_000_000,
                        'rollback-not-forwarded-before-job-expiry')
        finished = next_event(probe, 'job-source-finished')
        require(finished['state'] == ready['state'], 'unchanged-durable-operation-state')
        require(finished['returned'] == finished['disposed'] == (name in ('pass', 'held', 'ready')),
                'exact-value-disposal')
        require(finished['remaining_us'][1] > 5_000_000, 'Control-still-live-after-source')
        if name in ('held', 'ready', 'expired'):
            require(finished['remaining_us'][0] <= 0, 'original-job-expired')
        lower, upper = {'pass': (0, 500_000), 'pending': (900_000, 3_000_000),
            'held': (900_000, 4_000_000), 'ready': (3_000_000, 4_000_000),
            'omitted': (0, 500_000), 'unclaimed': (0, 500_000), 'expired': (0, 500_000)}[name]
        require(lower <= finished['elapsed_us'] < upper, 'result-before-outer-deadline')
        (output / (name + '.done')).touch(exist_ok=False)
        require(probe.wait(10) == 0 and not probe.stderr, 'actual-job-source-probe')
        receipt = next_event(proxy, 'receipt')
        require(proxy.wait(5) == 0 and not proxy.stderr, 'actual-job-source-proxy')
        require([receipt[k] for k in ('rollback_requests', 'rollback_completions',
                'rollback_released', 'rollback_ready')] == [baseline + 1] * 4, 'final-rollback-count')
        final = rows(database)
        require(final == before, 'no-teardown-durable-effects')
        verify_final(name, database, final)
        report['cases'].append(dict(name=name, status='PASS', database=database,
            baseline=baseline, held=held, released=released, forwarded=forwarded, receipt=receipt,
            server_after_rollback=observation, expiry_remaining_us=expiry, start=start,
            value=value, peer=peer, finished=finished, full_native_bytes=True,
            complete_control_tables=67))
        save()
