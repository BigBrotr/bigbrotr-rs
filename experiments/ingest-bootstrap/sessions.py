"""Installed session surface under the original initial Control/native owner."""
import json
from pathlib import Path
import shutil
import lab
from installed import (configure, messages, native_oracle, policy_setup, set_value,
                       start, stop, terminal, wait_for)


def run(pg, client, output, database, name, rows):
    assert name in ('sessions-journey', 'sessions-capacity', 'sessions-hash-stop')
    for filename in ('installed_client.py', 'session_client.py'):
        target = output / filename
        if not target.exists():
            shutil.copy2(Path(__file__).with_name(filename), target)
    # The original template stays untouched. These two independently hashed
    # accepted documents enable administration and bound producer work to32.
    policy_setup(pg, database, name, rows, output, administration=True)
    before = rows(database)
    lab.write_json(output / (name + '-before.json'), before)
    state = dict(pg, client=client)
    config = lab.config(output, state, database, endpoint='127.0.0.1:5432', boundary='loopback')
    path = output / Path(config).name
    text = configure(path.read_text(), before, name)
    text = set_value(text, 'administration.listen', 'port', 18081)
    # Retain the disabled startup seed: both accepted documents, not that seed,
    # decide whether this managed listener is selected.
    if name != 'sessions-capacity':
        for key, value in [('payload_bytes', '704MiB'), ('baseline_bytes', '64MiB'),
                           ('native_bytes', '192MiB'), ('completion_reserve_bytes', '64MiB')]:
            text = set_value(text, 'runtime.processes.application', key, value)
    path.write_text(text)
    validated = lab.cli(state, 'config', 'validate', '--file', config)
    (output / (name + '-validate.stdout')).write_text(validated.stdout)
    (output / (name + '-validate.stderr')).write_text(validated.stderr)
    assert validated.returncode == 0, 'sessions-config-invalid'
    (output / name).mkdir(mode=0o700)
    prefix = start(client, config, output, name)
    if name == 'sessions-capacity':
        ended = terminal(prefix)
        assert ended['exit_code'] == 3 and not messages(prefix), ended
        assert 'private management cannot reserve one complete login' in Path(str(prefix)+'.stderr').read_text()
        check = lab.docker('exec', client, 'python3', '-c',
            'import socket; s=socket.socket(); assert s.connect_ex(("127.0.0.1",18081))!=0; s.close()')
        assert check.returncode == 0
        initialized = None
    else:
        first = wait_for(lambda: messages(prefix) or Path(str(prefix)+'.terminal.json').exists(), 'session listener')
        assert isinstance(first, list) and len(first) == 1 and first[0]['state'] == 'initialized', first
        initialized = first[0]
        assert initialized['ready'] is False and initialized['acquisition'] == 'not-started'
        assert initialized['listeners'] == [dict(kind='private-session-http', host='127.0.0.1',
                                                 port=18081, profile='initial-sessions-only')]
        initial = rows(database)
        lab.write_json(output / (name + '-initialized.json'), initial)
        owner = next(r for r in initial['owner_epochs'] if r['role'] == 'control')
        original_deadline = next(r['lease_expires_at_us'] for r in initial['owner_epochs'] if r['role'] == 'ingest')
        mode = 'journey' if name == 'sessions-journey' else 'hash-stop'
        result = lab.docker('exec', client, 'python3', '/lab/session_client.py', '/lab/' + name, mode,
                            check=False, timeout=20)
        (output / (name + '-client.stdout')).write_text(result.stdout)
        (output / (name + '-client.stderr')).write_text(result.stderr)
        assert result.returncode == 0, 'actual-session-client-' + name
        observation = json.loads((output / name / 'client-result.json').read_text())
        assert observation['status'] == 'PASS'
        if mode == 'journey':
            current = rows(database)
            now = int(lab.sql(pg, database, 'SELECT (EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint'))
            renewed = [r['lease_expires_at_us'] for r in current['owner_epochs']]
            assert now > original_deadline and len(renewed) == 2 and min(renewed) > now
            current_owner = next(r for r in current['owner_epochs'] if r['role'] == 'control')
            assert all(current_owner[k] == owner[k] for k in [
                'control_backend_pid', 'control_backend_start', 'process_incarnation', 'resource_id', 'epoch'])
            for old in initial['owner_epochs']:
                new = next(r for r in current['owner_epochs'] if r['role'] == old['role'])
                stable = lambda row: {k: v for k, v in row.items() if k not in ('lease_expires_at_us', 'xmin')}
                assert stable(old) == stable(new), 'owner identity changed during commands'
            assert {t for t in initial if initial[t] != current[t]} <= {
                'owner_epochs', 'browser_sessions', 'login_buckets', 'management_audit'}
            assert len(current['browser_sessions']) == 1 and current['browser_sessions'][0]['revoked_at_us'] is not None
            assert len(current['management_audit']) == 2
            lab.write_json(output / (name + '-renewed.json'), dict(original_deadline=original_deadline,
                server_now=now, leases=renewed, initial=initial, current=current))
            stop(client, prefix)
        ended = terminal(prefix)
        assert ended['exit_code'] == 0 and not Path(str(prefix)+'.stderr').read_text(), ended
        final = messages(prefix)
        assert final[-1]['native'] == 'closed' and final[-1]['owner_release'] == 'committed'
    after = rows(database)
    lab.write_json(output / (name + '-after.json'), after)
    assert len(after['owner_epochs']) == 2 and len(after['role_instances']) == 2
    assert len(after['producer_requests']) == len(after['producer_outcomes']) == 1
    if initialized:
        native_oracle(pg, client, output, database, name, after, initialized)
        control = next(r for r in after['owner_epochs'] if r['role'] == 'control')
        assert control['state'] == 'released' and control['exclusion_method'] == 'storage_fenced'
        assert control['exclusion_digest'] is not None
    else:
        # Startup refusal follows committed bootstrap, never deletes/reinitializes it.
        assert (output / name / 'store/CURRENT').is_file()
        assert next(r for r in after['owner_epochs'] if r['role'] == 'control')['state'] == 'active'
        from commits import native_oracle as retained_oracle
        retained_oracle(client, output, name, after, committed=True, outcome=True)
    if name != 'sessions-journey':
        assert not after['browser_sessions'] and not after['management_audit']
    allowed = {'datasets', 'owner_epochs', 'role_instances', 'topology_generations', 'topology_streams',
               'source_streams', 'allocation_domains', 'producer_slots', 'producer_epochs',
               'producer_requests', 'producer_outcomes', 'browser_sessions', 'login_buckets',
               'management_audit'}
    assert {table for table in before if before[table] != after[table]} <= allowed
    assert lab.sql(pg, database, "SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND usename='bigbrotr_control'") == '0'
    return dict(name=name, status='PASS', actual_cli=True, actual_pg_rocks=True,
                control_tables=len(after), terminal=ended)
