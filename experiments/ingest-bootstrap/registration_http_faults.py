"""Installed registration over the actual initial native owner and finite PG proxy."""
import datetime
import hashlib
import json
from pathlib import Path
import shutil
import time
import lab
from fault_cases import Process
from commits import backend, require_held
from installed import messages, native_oracle, policy_setup, set_value, start, stop, terminal, wait_for
from registration_faults import DOMAIN_TABLES, event, finish, go, receipt, retire, start_probe
from registrations import upgrade
from sessions import configuration

CASES = {f'register-http-{mode}': mode for mode in ('pass', 'before', 'after', 'disconnect', 'signal')}
TABLES = DOMAIN_TABLES | {'relay_endpoints'}


def effects(before, after, created):
    assert before.keys() == after.keys()
    assert {t for t in before if before[t] != after[t]} == (TABLES if created else set())
    if created:
        for table in TABLES:
            assert len(after[table]) == len(before[table]) + 1
            assert all(row in after[table] for row in before[table])


def run(pg, client, output, database, name, rows):
    mode = CASES[name]
    upgrade(pg, client, output, database, name, rows)
    policy_setup(pg, database, name, rows, output, administration=True)
    # A clone-only nontransactional marker proves that the selected COMMIT
    # belongs to this registration even if a slow lab lets a renewal become due.
    # Durable effects are still checked independently from all Control rows.
    lab.sql(pg, database, """
        CREATE SCHEMA registration_wire_fixture;
        CREATE FUNCTION registration_wire_fixture.cut() RETURNS trigger
        LANGUAGE plpgsql SET search_path=pg_catalog AS $$ BEGIN
            RAISE LOG 'bigbrotr registration cut db=% pid=%',current_database(),pg_backend_pid();
            RETURN NULL;
        END $$;
        CREATE TRIGGER zz_registration_wire_cut AFTER INSERT ON control.relay_registration_commands
            FOR EACH ROW WHEN (NEW.request_key=decode(repeat('31',16),'hex'))
            EXECUTE FUNCTION registration_wire_fixture.cut();
        GRANT USAGE ON SCHEMA registration_wire_fixture TO bigbrotr_control;
        GRANT EXECUTE ON FUNCTION registration_wire_fixture.cut() TO bigbrotr_control;
    """)
    log_since = datetime.datetime.now(datetime.timezone.utc).isoformat()
    original = rows(database)
    assert not original['owner_epochs'] and not original['relay_endpoints']
    script = output / 'installed_client.py'
    if not script.exists():
        shutil.copy2(Path(__file__).with_name(script.name), script)
    directory = output / name
    directory.mkdir(mode=0o700)
    documents = [original[t][0]['document'] for t in ('config_revisions', 'policy_revisions')]
    lab.write_json(directory / 'policies.json', documents)
    config = lab.config(output, dict(pg, client=client), database,
        endpoint='127.0.0.1:5432', boundary='loopback')
    config_path = output / Path(config).name
    text = configuration(config_path.read_text(), original, name)
    # Authorization still expires within the actual five-second ceiling.
    # Counts and the server marker reject a renewal mistakenly used as the cut.
    for key, value in [('lease_ttl', '30s'), ('authorization_ttl', '5s'),
                       ('heartbeat_interval', '10s'), ('renewal_timeout', '5s')]:
        text = set_value(text, 'runtime', key, value)
    direct = '/lab/' + name + '-direct.toml'
    (output / Path(direct).name).write_text(text)
    processes = []
    result = dict(name=name, status='RUNNING', mode=mode)
    prefix = None

    def snapshot(label):
        state = rows(database)
        lab.write_json(directory / (label + '.json'), state)
        return state

    try:
        wire_mode = {'before':'before-forward', 'after':'after-complete',
            'signal':'hold', 'disconnect':'hold', 'pass':'pass'}[mode]
        proxy = Process(['docker', 'exec', '-i', client, 'python3', '-B',
            '/source/experiments/control-migrations/fault_proxy.py', '--upstream-port', '5432',
            '--mode', wire_mode, '--commit-number', '9', '--later-commits', '4',
            '--deadline-seconds', '40'], input_pipe=True)
        processes.append(('proxy', proxy))
        proxy_ready = proxy.event('ready', 5)
        assert proxy_ready['proxy_sha256'] == hashlib.sha256(
            (Path(__file__).resolve().parents[1] / 'control-migrations/fault_proxy.py').read_bytes()).hexdigest()
        config_path.write_text(set_value(text, 'postgres.control', 'endpoint',
            '127.0.0.1:' + str(proxy_ready['listen_port'])))
        validated = lab.cli(dict(pg, client=client), 'config', 'validate', '--file', config)
        (directory / 'validate.stdout').write_text(validated.stdout)
        (directory / 'validate.stderr').write_text(validated.stderr)
        assert validated.returncode == 0, validated.stderr
        cli_started = time.monotonic()
        prefix = start(client, config, output, name)
        first = wait_for(lambda: messages(prefix) or Path(str(prefix)+'.terminal.json').exists(), 'installed startup')
        assert isinstance(first, list) and len(first) == 1 and first[0]['state'] == 'initialized'
        initialized = first[0]
        assert not initialized['ready'] and initialized['acquisition'] == 'not-started'
        assert initialized['listeners'][0]['profile'] == 'initial-source-commands'
        http = Process(['docker', 'exec', client, 'python3', '-B',
            '/source/experiments/ingest-bootstrap/registration_http_client.py', '/lab/' + name, mode])
        processes.append(('http', http))
        event(http, 'authenticated')
        before = snapshot('before')
        control, = [r for r in before['owner_epochs'] if r['role'] == 'control']
        session, = before['browser_sessions']
        ready = dict(epoch=1, pid=control['control_backend_pid'], session_expires=session['expires_at_us'])
        assert control['epoch'] == 1 and session['revoked_at_us'] is None
        proxy.send('status')
        status = proxy.event('status', 5)
        assert [status[k] for k in ('commits_seen', 'commits_forwarded',
            'server_commit_complete', 'client_commit_complete')] == [8,8,8,8]
        go(directory / 'send')
        if wire_mode == 'hold':
            proxy.event('held', 5)
            assert rows(database) == before
            held = require_held(pg, database, before)
            busy, _ = start_probe(client, database, directory, 'busy', 12, processes)
            assert event(busy, 'busy') == dict(event='busy', commit_started=False)
            assert busy.wait(5) == 0 and not busy.stderr and rows(database) == before
            result['held_backend'] = held
            if mode == 'disconnect':
                go(directory / 'disconnect')
                event(http, 'disconnected')
                assert rows(database) == before
                proxy.send('release')
                proxy.event('released', 3)
                wait_for(lambda: rows(database)['relay_registration_commands'], 'known COMMIT', 3)
                accepted_state = snapshot('retry-before')
                go(directory / 'retry')
            else:
                signal_time = time.monotonic()
                stop(client, prefix)
                result['start_to_signal_sent_seconds'] = time.monotonic() - cli_started
                # Refresh starts after cli_started. With TTL5s, any phase using
                # half the remaining lease has a deadline at least2.5s after
                # that lower bound. A later signal could merely hit driver drain
                # after an earlier command timeout and cannot qualify this case.
                assert result['start_to_signal_sent_seconds'] < 2.0, 'signal too late to isolate cancellation'
                ended = terminal(prefix)
                result['signal_to_reap_seconds'] = time.monotonic() - signal_time
                assert result['signal_to_reap_seconds'] < 3.5 and ended['exit_code'] == 4
                assert rows(database) == before
                result['upstream_after_cli_death'] = require_held(pg, database, before)
                proxy.send('close')
        http_answer = event(http, 'done', 10)['outcome']
        assert http.wait(5) == 0 and not http.stderr
        healthy = mode in ('pass', 'disconnect')
        if healthy:
            accepted_state = snapshot('accepted')
            effects(before, accepted_state, True)
            original_receipt = receipt(accepted_state, ready)
            response = dict(original_receipt, accepted_at_us=str(original_receipt['accepted_at_us']),
                created=mode == 'pass', format='bigbrotr-relay-registration/1', authority='identity-only')
            assert http_answer == dict(status=202 if mode == 'pass' else 200, body=response)
            if mode == 'disconnect':
                assert accepted_state == json.loads((directory / 'retry-before.json').read_text())
            stop(client, prefix)
        ended = terminal(prefix)
        assert ended['exit_code'] == (0 if healthy else 4)
        reports = messages(prefix)
        if healthy:
            assert reports[-1]['native'] == 'closed' and reports[-1]['owner_release'] == 'committed'
            assert not Path(str(prefix)+'.stderr').read_bytes()
        else:
            assert len(reports) == 1 and Path(str(prefix)+'.stderr').read_bytes()
            if mode == 'signal':
                assert 'closed' in http_answer
                assert Path(str(prefix)+'.stderr').read_text() == (
                    'error: initial Ingest cancelled; original attempt may need recovery\n')
        wire = proxy.event('receipt', 5)
        assert proxy.wait(5) == 0 and not proxy.stderr
        counts = [wire[k] for k in ('commits_seen','commits_forwarded','server_commit_complete',
                                    'client_commit_complete','faults')]
        wanted = [10,10,10,10,int(mode=='disconnect')] if healthy else [9,8,8,8,1] if mode in ('before','signal') else [9,9,9,8,1]
        assert counts == wanted, counts
        wait_for(lambda: not backend(pg, database), 'original backend retired', 3)
        logs = lab.docker('logs', '--since', log_since, pg['postgres'])
        marker = f'bigbrotr registration cut db={database} pid={ready["pid"]}'
        markers = [line for line in (logs.stdout + logs.stderr).splitlines() if marker in line]
        assert len(markers) == 1 and markers[0].endswith(marker), 'wrong registration COMMIT cut'
        lab.write_json(directory / 'commit-cut.json', dict(marker=markers[0], before=status, selected=9))
        after = snapshot('after')
        committed = mode in ('pass', 'disconnect', 'after')
        if healthy:
            assert {t for t in after if after[t] != accepted_state[t]} == {'owner_epochs','role_instances'}
            old_ingest = [r for r in accepted_state['owner_epochs'] if r['role']=='ingest']
            assert [r for r in after['owner_epochs'] if r['role']=='ingest'] == old_ingest
            final_control, = [r for r in after['owner_epochs'] if r['role']=='control']
            assert final_control['state'] == 'released' and final_control['exclusion_method'] == 'storage_fenced'
            assert final_control['released_at_us'] is not None and final_control['exclusion_digest'] is not None
        else:
            effects(before, after, committed)
        accepted = receipt(after, ready)
        assert (accepted is not None) == committed
        native_oracle(pg, client, output, database, name, after, initialized,
            probe='/native-inspect/ingest_bootstrap_probe')
        if not healthy:
            retry = lab.cli(dict(pg, client=client), 'serve', '--file', direct,
                '--initialize-ingest', '--roles', 'control,ingest')
            (directory / 'restart.stdout').write_text(retry.stdout)
            (directory / 'restart.stderr').write_text(retry.stderr)
            assert retry.returncode == 3 and not retry.stdout and 'target exists' in retry.stderr
            assert rows(database) == after
        recovery, token = start_probe(client, database, directory, 'accept', 13, processes)
        recovered_owner = event(recovery, 'ready')
        assert recovered_owner['epoch'] == 2
        before_retry = snapshot('recovery-before')
        go(token)
        recovered = finish(recovery)
        after_retry = snapshot('recovery-after')
        effects(before_retry, after_retry, not committed)
        durable = receipt(after_retry, ready if committed else recovered_owner)
        assert recovered == dict(receipt=dict(durable, created=not committed), commit_started=not committed)
        assert not committed or durable == accepted
        assert not backend(pg, database)
        result.update(status='PASS', initialized=initialized, ready=ready, http=http_answer,
            terminal=ended, wire=wire, committed=committed, recovery_owner=recovered_owner,
            recovered=recovered, durable=durable, all_control_tables=68, native_rows=12)
    except BaseException as error:
        result.update(status='FAIL', failure=type(error).__name__ + ': ' + str(error))
        raise
    finally:
        # The exact wrapper owns/reaps the installed process. On fixture failure
        # stop it before joining proxy/helper processes or the common host stops PG.
        if prefix is not None and not Path(str(prefix)+'.terminal.json').exists():
            try:
                stop(client, prefix)
                terminal(prefix)
            except BaseException as error:
                result['cli_cleanup_error'] = type(error).__name__
                result['status'] = 'INCOMPLETE_CLEANUP'
        retire(directory, processes, result)
    assert result['status'] == 'PASS'
    return result
