#!/usr/bin/env python3
"""MON-001/004 executable regressions and MON-005 real JSONB investigation."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import time
sys.dont_write_bytecode = True
SOURCE = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(SOURCE / 'experiments/control-migrations'))
import lab


def require(value, message):
    if not value:
        raise RuntimeError(message)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, required=True)
    root = parser.parse_args().root.resolve(strict=True)
    state = lab.load(root)
    require(not lab.command(['git', '-C', str(SOURCE), 'status', '--porcelain']).stdout, 'dirty source')
    revision = lab.command(['git', '-C', str(SOURCE), 'rev-parse', 'HEAD']).stdout.strip()
    require(revision == state['build']['build_info']['source']['revision'], 'source mismatch')
    require(json.loads(lab.cli(state, 'build-info').stdout) == state['build']['build_info'], 'artifact metadata')
    mounts = json.loads(lab.docker('inspect', state['client']).stdout)[0]['Mounts']
    build = Path(next(m['Source'] for m in mounts if m['Destination'] == '/build' and not m['RW']))
    require(hashlib.sha256((build / 'bigbrotr').read_bytes()).hexdigest() == state['binary_sha256'], 'artifact bytes')
    report_path = root / 'monitor-regressions.json'
    require(not report_path.exists(), 'existing report')
    report = {'status': 'RUNNING', 'source': revision, 'binary_sha256': state['binary_sha256'], 'cases': [], 'processes': []}
    def save():
        lab.write_json(report_path, report)
    def check(name, **values):
        report['cases'].append(dict(name=name, result='PASS', **values)); save()
    def wait_for(predicate, message, seconds=12):
        deadline = time.monotonic() + seconds
        while time.monotonic() < deadline:
            value = predicate()
            if value:
                return value
            time.sleep(.04)
        raise RuntimeError(message)
    def prepare(name, initialize=True):
        db = 'bb_monitor_' + name
        lab.sql(state, 'postgres', f'CREATE DATABASE {db} OWNER bigbrotr_migrator;')
        path = lab.config(root, state, db)
        local = root / Path(path).name
        local.write_text(lab.change(lab.change(local.read_text(), 'postgres.control', 'password_ref',
                                             'file:/lab/secrets/application'), 'administration', 'enabled', False))
        require(lab.migrate(state, 'apply', path, target=7).returncode == 0, 'migration')
        lab.sql(state, db, 'GRANT USAGE ON SCHEMA control TO bigbrotr_control; GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA control TO bigbrotr_control; GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA control TO bigbrotr_control;')
        if initialize:
            require(lab.cli(state, 'config', 'initialize', '--file', path).returncode == 0, 'initialize')
            require(lab.cli(state, 'config', 'activate-initial', '--file', path, '--request-id', '71' * 16,
                            '--expected-revision', '1').returncode == 0, 'activate')
        return db, path, local
    def rows(db):
        names = json.loads(lab.sql(state, db, "SELECT json_agg(tablename ORDER BY tablename) FROM pg_tables WHERE schemaname='control';"))
        require(len(names) == 30, 'relation inventory')
        return {n: json.loads(lab.sql(state, db, f"SELECT coalesce(json_agg(t),'[]') FROM control.{n} t;")) for n in names}
    def backend_count(db):
        return lab.sql(state, db, "SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND usename='bigbrotr_control';")
    def start(path, label):
        name = state['client'] + '-mon-' + label
        require(lab.docker('inspect', name, check=False).returncode != 0, 'existing container')
        record = {'name': name, 'state': 'creating'}; report['processes'].append(record); save()
        record['id'] = lab.docker('run', '-d', '--init', '--name', name,
            '--label', 'org.bigbrotr.monitor-regression=' + state['owner'], '--network', state['network'],
            '--user', f'{os.getuid()}:{os.getgid()}', '--cpus', '2', '--memory', '1g', '--memory-swap', '1g',
            '--pids-limit', '128', '--read-only', '--cap-drop', 'ALL', '--security-opt', 'no-new-privileges',
            '--mount', f'type=bind,src={root},dst=/lab,readonly', '--mount', f'type=bind,src={build},dst=/build,readonly',
            state['client_image'], '/build/bigbrotr', 'serve', '--file', path, '--roles', 'control').stdout.strip()
        save(); return record
    def info(record):
        value = json.loads(lab.docker('inspect', record['name']).stdout)[0]
        require(value['Id'] == record['id'] and value['Config']['Labels'].get('org.bigbrotr.monitor-regression') == state['owner'], 'container ownership')
        return value
    def idle(record):
        def observed():
            require(info(record)['State']['Running'], 'early process exit')
            lines = lab.docker('logs', record['name']).stdout.splitlines()
            return lines and json.loads(lines[0])['state'] == 'idle'
        wait_for(observed, 'idle timeout')
    def release(record, db):
        started = time.monotonic(); lab.docker('kill', '--signal', 'TERM', record['name'])
        wait_for(lambda: not info(record)['State']['Running'], 'termination timeout', 6)
        terminal = info(record)['State']; record.update(state='stopped-retained', terminal=terminal)
        logs = lab.docker('logs', record['name'])
        (root / (record['name'] + '.stdout')).write_text(logs.stdout)
        (root / (record['name'] + '.stderr')).write_text(logs.stderr)
        require(terminal['ExitCode'] == 0 and not terminal['OOMKilled'] and not logs.stderr, 'termination failure')
        require([json.loads(line)['state'] for line in logs.stdout.splitlines()] == ['idle', 'stopped'], 'lifecycle output')
        require(backend_count(db) == '0', 'backend leak')
        observed = rows(db); require(observed['owner_epochs'][0]['state'] != 'active', 'unreleased owner')
        lab.write_json(root / (db + '.rows.json'), observed)
        return time.monotonic() - started
    def blocker(db):
        process = subprocess.Popen(['docker', 'exec', '-i', state['postgres'], 'psql', '-X', '-q', '-v', 'ON_ERROR_STOP=1', '-U', 'postgres', '-d', db],
                                   stdin=subprocess.PIPE, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, text=True)
        process.stdin.write("BEGIN; SET LOCAL application_name='bigbrotr-monitor-blocker'; LOCK TABLE control.datasets IN ACCESS EXCLUSIVE MODE; SELECT pg_sleep(60); ROLLBACK;\n")
        process.stdin.close()
        wait_for(lambda: lab.sql(state, db, "SELECT count(*) FROM pg_stat_activity WHERE application_name='bigbrotr-monitor-blocker' AND wait_event='PgSleep';") == '1', 'blocker lock')
        return process
    def unblock(db, process):
        lab.sql(state, db, "SELECT pg_terminate_backend(pid,5000) FROM pg_stat_activity WHERE application_name='bigbrotr-monitor-blocker' AND datname=current_database();")
        process.wait(timeout=8)
    save()
    try:
        for phase in ['acquisition', 'poll']:
            db, path, _ = prepare(phase)
            held = None
            try:
                if phase == 'acquisition': held = blocker(db)
                record = start(path, phase)
                if phase == 'poll': idle(record); held = blocker(db)
                wait_for(lambda: lab.sql(state, db, "SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND usename='bigbrotr_control' AND wait_event_type='Lock';") == '1', 'operation not blocked')
                started = time.monotonic(); time.sleep(3.2)
                unblock(db, held); held = None
                if phase == 'acquisition': idle(record)
                # The old fixed 2.5s post-response sleep would now expire.
                time.sleep(6)
                require(info(record)['State']['Running'], 'valid delayed response lost authority')
                check('MON-004-' + phase, delay_seconds=3.2, observed_seconds=time.monotonic()-started,
                      shutdown_seconds=release(record, db))
            finally:
                if held is not None: unblock(db, held)
        db, path, _ = prepare('blocked_stdout')
        output = root / 'blocked-stdout.json'
        lab.docker('exec', '-d', state['client'], 'python3', '/source/experiments/control-runtime/blocked_output.py',
                   '--binary', '/build/bigbrotr', '--config', path, '--stream', 'stdout', '--output', '/lab/' + output.name)
        wait_for(lambda: output.exists(), 'output helper startup')
        child = json.loads(output.read_text())
        require(child['status'] == 'RUNNING' and child['blocking'] and child['filled_bytes'] > 0, 'not a full blocking pipe')
        def owner_deadline():
            return lab.sql(state, db, "SELECT lease_expires_at_us FROM control.owner_epochs WHERE state='active';")
        original = int(wait_for(owner_deadline, 'blocked stdout prevented acquisition'))
        renewed = wait_for(lambda: (value := owner_deadline()) and int(value) > original, 'blocked stdout prevented heartbeat', 28)
        started = time.monotonic()
        lab.docker('exec', state['client'], 'python3', '-c', 'import os,signal,sys;os.kill(int(sys.argv[1]),signal.SIGTERM)', str(child['pid']))
        wait_for(lambda: json.loads(output.read_text())['status'] != 'RUNNING', 'blocked stdout prevented termination', 6)
        result = json.loads(output.read_text())
        require(result['status'] == 'TERMINAL' and result['exit'] == 1 and result['unchanged'] and result['eof_after_child_exit'], 'blocked pipe process/descriptor leak')
        require(backend_count(db) == '0', 'blocked output backend leak')
        observed = rows(db); require(observed['owner_epochs'][0]['state'] != 'active', 'blocked output owner not released')
        lab.write_json(root / (db + '.rows.json'), observed)
        check('MON-001-blocked-stdout', shutdown_seconds=time.monotonic()-started, heartbeat_observed=bool(renewed), **result)
        result = lab.docker('exec', state['client'], 'python3', '/source/experiments/control-runtime/blocked_output.py',
            '--binary', '/build/bigbrotr', '--config', '/nonexistent-bigbrotr-monitor.toml', '--stream', 'stderr', '--output', '/lab/blocked-stderr.json')
        observed = json.loads(result.stdout)
        require(observed['status'] == 'TERMINAL' and observed['exit'] == 1 and observed['elapsed_seconds'] < 6
                and observed['unchanged'] and observed['eof_after_child_exit'], 'blocked stderr termination')
        check('MON-001-blocked-stderr', **observed)

        # Reproduce, do not pre-label, the separate maximum-document finding.
        db, path, local = prepare('document', initialize=False)
        urls = [('wss://relay.example/' + str(n).zfill(6) + '/').ljust(49, 'x') for n in range(100000)]
        require(all(len(url) == 49 for url in urls), 'independent endpoint length')
        text = local.read_text()
        for role in ['sync', 'monitor']:
            text += '\n[' + role + '.endpoints]\nmode="fixed"\nurls=' + json.dumps(urls, separators=(',', ':')) + '\n'
        local.write_text(text)
        validated = lab.cli(state, 'config', 'validate', '--file', path)
        require(validated.returncode == 0, 'maximum candidate rejected before reproduction')
        candidate = json.loads(validated.stdout)['initial_policy']
        policy = json.loads(lab.sql(state, 'bb_monitor_acquisition', 'SELECT document FROM control.config_revisions;'))
        for role in ['sync', 'monitor']:
            policy['settings'][role + '.endpoints.mode'] = 'fixed'
            policy['settings'][role + '.endpoints.urls'] = urls
        compact = json.dumps(policy, ensure_ascii=False, separators=(',', ':'), sort_keys=True)
        digest = hashlib.sha256(b'bigbrotr/product-policy/1\0' + compact.encode()).hexdigest()
        require(candidate['sha256'] == digest and int(candidate['encoded_bytes']) == len(compact.encode()), 'independent candidate identity')
        rendered = int(lab.sql(state, db, "SELECT octet_length($monitor_json$" + compact + "$monitor_json$::jsonb::text);"))
        before = lab.schema_digest(state, db)
        initialized = lab.cli(state, 'config', 'initialize', '--file', path)
        (root / 'document-initialize.stderr').write_text(initialized.stderr)
        require(initialized.returncode != 0 and before == lab.schema_digest(state, db), 'expected bounded rollback reproduction')
        require(len(compact.encode()) < 10485760 < rendered, 'JSONB size counterexample not reproduced')
        report['document_finding'] = {'finding': 'MON-005', 'status': 'CONFIRMED_UNFIXED', 'source_bytes': len(text.encode()),
            'compact_bytes': len(compact.encode()), 'jsonb_rendered_bytes': rendered, 'policy_sha256': digest,
            'static_exit': validated.returncode, 'initialize_exit': initialized.returncode, 'unchanged_database': True,
            'inspection_activation': 'NOT_RUN: initialization refused; required after correction'}
        report['status'] = 'REGRESSIONS_PASS_DOCUMENT_FIX_REQUIRED'
    except BaseException as error:
        report.update(status='FAIL', failure=str(error)); raise
    finally:
        for record in report['processes']:
            if 'id' in record:
                if info(record)['State']['Running']: lab.docker('stop', '--time', '3', record['name'])
                record.update(state='stopped-retained', terminal=info(record)['State'])
        save()
    print(json.dumps({'status': report['status'], 'cases': len(report['cases'])}))


if __name__ == '__main__':
    main()
