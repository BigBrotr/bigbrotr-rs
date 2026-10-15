#!/usr/bin/env python3
"""Finite connected history on three fresh copies; no original native open.

The existing terminal2 origins remain immutable. Unknown process custody never
permits a native inspection; the owner-checked outer cleanup reaps the client.
"""
import datetime
import importlib.util
import json
import os
from pathlib import Path
import shutil
import stat
import subprocess
import sys
import time

sys.dont_write_bytecode = True
import schema
from history_replay import CASES, PINS, negative_controls, verify

SOURCE = Path(__file__).resolve().parents[2]
bootstrap_spec = importlib.util.spec_from_file_location(
    'bootstrap_oracle', SOURCE / 'experiments/ingest-bootstrap/oracle.py')
bootstrap_oracle = importlib.util.module_from_spec(bootstrap_spec)
sys.modules['bootstrap_oracle'] = bootstrap_oracle
bootstrap_spec.loader.exec_module(bootstrap_oracle)
sys.path.insert(0, str(SOURCE / 'experiments/ingest-bootstrap'))
from initial_history import capture, inputs_at, observe_backend_absence, server_clock
from initial_succession import census
from installed import set_value

lab = schema.lab
read, digest, normalized = schema.read, schema.digest, schema.normalized
projection = schema.projection
LABEL = 'org.bigbrotr.succession-history'
STATUS = 'PASS_SUCCESSOR_HISTORY_FIRST_WAVE'


def content_census(files):
    """A copy preserves paths/types/modes and file bytes, never inode identity."""
    return {name: dict(mode=value['mode'], **(
        dict(bytes=value['bytes'], sha256=value['sha256'])
        if stat.S_ISREG(value['mode']) else {})) for name, value in files.items()}


def catalog(pg, database):
    # Unlike schema_digest, retain ACLs as well as all schema definitions.
    result = lab.docker('exec', pg['postgres'], 'pg_dump', '--no-password', '-U', 'postgres',
        '-d', database, '--schema=control', '--no-owner')
    return '\n'.join(line for line in result.stdout.splitlines()
        if not line.startswith(('\\restrict ', '\\unrestrict ')))


def configuration(output, state, database, mode, original):
    config = lab.config(output, state, database, endpoint='127.0.0.1:5432', boundary='loopback')
    path = output / Path(config).name
    text = path.read_text()
    dataset, = original['datasets']
    for section, key, value in [
        ('topology', 'dataset_id', dataset['dataset_id'][2:]),
        ('topology', 'instance_id', dataset['instance_id'][2:]),
        ('topology', 'local_roles', ['control', 'ingest', 'archive']),
        ('postgres.control', 'password_ref', 'file:/run/secrets/application'),
        ('storage', 'ingest_root', '/lab/' + mode + '/store'),
        ('runtime', 'lease_ttl', '20s'), ('runtime', 'renewal_timeout', '1s'),
        ('runtime', 'heartbeat_interval', '1s'), ('runtime', 'renewal_safety_margin', '1s'),
        ('runtime', 'authorization_ttl', '5s'), ('runtime', 'graceful_shutdown_timeout', '2s'),
        ('runtime.processes.application', 'memory_max_bytes', '1536MiB'),
        ('runtime.processes.application', 'baseline_bytes', '128MiB'),
        ('runtime.processes.application', 'payload_bytes', '1GiB'),
        ('runtime.processes.application', 'native_bytes', '256MiB'),
        ('runtime.processes.application', 'completion_reserve_bytes', '32MiB'),
    ]:
        text = set_value(text, section, key, value)
    path.write_text(text)
    answer = lab.cli(state, 'config', 'validate', '--file', config)
    lab.write_json(output / (mode + '-config-validation.json'), dict(
        returncode=answer.returncode, stdout=answer.stdout, stderr=answer.stderr))
    assert answer.returncode == 0 and not answer.stderr
    return config


def dump(client, output, label, directory):
    answer, _ = capture(client, output, label + '-dump', 'dump-state',
        '/lab/' + directory.name + '/store')
    assert answer.returncode == 0 and not answer.stderr
    result = json.loads(answer.stdout)
    lab.write_json(output / (label + '-native.json'), result)
    return result


def closed_and_joined(response, database):
    """Positive native/process telemetry, separate from scientific acceptance."""
    assert response['returncode'] == 0
    events = [json.loads(line) for line in response['stdout'].splitlines()]
    assert not any(e.get('event') == 'lineage-history-timeout' for e in events)
    native, = [i for i, e in enumerate(events) if e == dict(event='lineage-history-native', state='closed')]
    control, = [i for i, e in enumerate(events) if e == dict(event='lineage-history-driver',
        driver='control', failed=False, joined=True)]
    observer, = [i for i, e in enumerate(events) if e == dict(event='lineage-history-driver',
        driver='observer', failed=False, joined=True)]
    assert [e for e in events if e.get('event') == 'lineage-history-driver'] == [events[control], events[observer]]
    present, absent = [e for e in events if e.get('event') == 'lineage-history-backend']
    assert present['present'] is True and absent == dict(present, present=False)
    assert present['database'] == database and type(present['pid']) is int and present['pid'] > 0
    assert isinstance(present['backend_start'], str) and present['backend_start']
    absent_index = events.index(absent)
    complete = events[-1]
    assert complete['event'] == 'lineage-history-complete' and complete['native'] == 'closed'
    assert complete['observer'] == 'joined' and complete['control'] in ('released-and-joined', 'dropped-and-joined')
    assert native < control < absent_index < observer < len(events) - 1
    return events


def external(pg, client, output, case, directory, response):
    """Persist each observation before rejecting missing custody or semantics."""
    name, database = case['name'], case['database']
    errors = []
    custody = 'UNKNOWN_PROCESS_OUTCOME'
    events = None
    try:
        events = closed_and_joined(response, database)
        custody = 'OBSERVED_CLOSED_AND_JOINED'
    except BaseException as error:
        errors.append(dict(stage='probe-custody', error=repr(error)))
    absent = False
    try:
        observe_backend_absence(pg, database, output, name + '-control')
        backends = schema.absence(pg, database)
        lab.write_json(output / (name + '-backends.json'), backends)
        assert backends == []
        absent = True
    except BaseException as error:
        errors.append(dict(stage='backend-absence', error=repr(error)))
    # These reads can preserve provisional evidence even when no native owner
    # exclusion was established. The status below never relabels it as final.
    after = final_inputs = final_native = None
    for stage, function, suffix in [
        ('sql', lambda: schema.rows(pg, database), 'after'),
        ('inputs', lambda: inputs_at(directory / 'store'), 'after-inputs'),
    ]:
        try:
            value = function()
            lab.write_json(output / (name + '-' + suffix + '.json'), value)
            if stage == 'sql':
                after = value
            else:
                final_inputs = value
        except BaseException as error:
            errors.append(dict(stage=stage, error=repr(error)))
    if custody == 'OBSERVED_CLOSED_AND_JOINED' and absent:
        try:
            final_native = dump(client, output, name + '-after', directory)
        except BaseException as error:
            errors.append(dict(stage='native-capture', error=repr(error)))
    else:
        lab.write_json(output / (name + '-external-unavailable.json'), dict(
            status='NOT_RUN_UNKNOWN_CUSTODY', native='NOT_RUN', sql_and_inputs='provisional',
            cleanup='outer owner-checked client stop; no inferred rollback'))
    evidence = dict(status=custody, database=database, backend_absent=absent, errors=errors)
    lab.write_json(output / (name + '-custody.json'), evidence)
    case['custody'] = custody
    assert not errors, evidence
    return events, after, final_native, final_inputs


def execute(pg, client, output, case, directory, config):
    name, database = case['name'], case['database']
    before = schema.rows(pg, database)
    lab.write_json(output / (name + '-before.json'), before)
    inputs = inputs_at(directory / 'store')
    lab.write_json(output / (name + '-inputs.json'), inputs)
    native = dump(client, output, name + '-before', directory)
    window = dict(before_us=server_clock(pg, database))
    lab.write_json(output / (name + '-clock.json'), window)
    try:
        capture(client, output, name, 'successor-lineage-history', database,
            '/run/secrets/application', config, str(case['metadata_bytes']))
    finally:
        path = output / (name + '-response.json')
        if not path.exists():
            lab.write_json(path, dict(returncode=None, stdout='', stderr='',
                error='wrapper failed without terminal receipt', outcome='UNKNOWN'))
        response = read(path)
        case['returncode'] = response['returncode']
        try:
            window['after_us'] = server_clock(pg, database)
        except BaseException as error:
            window['error'] = repr(error)
        lab.write_json(output / (name + '-clock.json'), window)
        events, after, final_native, final_inputs = external(pg, client, output, case, directory, response)
    assert response['returncode'] == 0 and not response['stderr']
    checked = verify(events, before, native, inputs, name, window)
    controls = negative_controls(events, before, native, inputs, name, window)
    final, = [e for e in events if e['event'] == 'lineage-history-state' and e['stage'] == 'final']
    assert normalized(after) == normalized({t: json.loads(v) for t, v in final['state']['control_tables']})
    assert final_native == native and final_inputs == inputs
    assert checked['database'] == database
    case.update(status='PASS', oracle=checked, negative_controls=controls,
        response_bytes=len(response['stdout'].encode()), server_window=window)


def run(root, build):
    assert SOURCE == root / 'source'
    revision = lab.command(['git', '-C', str(SOURCE), 'rev-parse', 'HEAD']).stdout.strip()
    assert not lab.command(['git', '-C', str(SOURCE), 'status', '--porcelain', '--untracked-files=all']).stdout
    built = read(build / 'build-result.json')
    component = read(build / 'component-result.json')
    qualification = read(build.parent / 'qualification.json')
    assert qualification['status'] == 'PASS_SUCCESSION_HISTORY_PROBE_BUILD' and component['status'] == 'PASS'
    assert component['source'] == built['source'] == qualification['source']
    for field in ('tree', 'source_manifest_sha256', 'lockfile_sha256'):
        assert component[field] == qualification[field] == built[field], field
    binaries = component['executables_sha256']
    assert {'bigbrotr', 'ingest_bootstrap_probe'} <= set(binaries)
    assert all(digest(build / name) == value for name, value in binaries.items())
    assert binaries['bigbrotr'] == built['binary_sha256']
    manifest_path = build.parent / 'build/source-manifest.json'
    manifest = read(manifest_path)
    assert digest(manifest_path) == built['source_manifest_sha256']
    assert manifest['revision'] == built['source'] and manifest['tree'] == built['tree']
    changes = lab.command(['git', '-C', str(SOURCE), 'diff', '--name-only', built['source'], revision]).stdout.splitlines()
    assert all(name.startswith(('docs/', 'experiments/')) for name in changes), changes
    assert all(digest(SOURCE / name) == wanted for name, wanted in manifest['files_sha256'].items()
        if name.startswith(('apps/', 'crates/', 'migrations/')) or name in ('Cargo.toml', 'Cargo.lock', 'rust-toolchain.toml'))
    pg_root = root.parent / 'relay-source-recheck-e06f580/pg'
    pg = lab.load(pg_root)
    assert pg['owner'] == '5e69b0f060b1607c25230d1dbd3e5608' and pg['state'] == 'stopped-retained'
    containers = schema.containers()
    borrowed, = [c for c in containers if c['Name'].lstrip('/') == pg['postgres']]
    assert borrowed['Id'] == '7177550705c49b14237e20790af411af58f62f22d110cd0dfa1afddaf78861ec'
    assert borrowed['State']['Status'] == 'exited' and not borrowed['State']['Pid'] and not borrowed['State']['OOMKilled']
    assert borrowed['HostConfig']['Memory'] == 1536 * 1024**2 and borrowed['HostConfig']['NanoCpus'] == 2_000_000_000
    assert not any(c['State']['Running'] and 'bigbrotr' in c['Name'] for c in containers)
    assert shutil.disk_usage(root).free > 2 * 1024**3
    output = root / 'history'
    output.mkdir(mode=0o700)
    owner = schema.secrets.token_hex(16)
    client = 'bigbrotr-successor-history-' + revision[:7]
    assert not any(c['Name'].lstrip('/') == client for c in containers)
    tree = lab.command(['git', '-C', str(SOURCE), 'rev-parse', 'HEAD^{tree}']).stdout.strip()
    report = dict(status='RUNNING', source=revision, tree=tree, build=built, component=component, owner=owner,
        label=LABEL, postgres=borrowed['Id'], postgres_owner=pg['owner'], client=None, databases=[],
        started_at=datetime.datetime.now(datetime.timezone.utc).isoformat(), origins={}, cases=[],
        free_bytes_before=shutil.disk_usage(root).free,
        scope='Finite read-only terminal2 whole-lineage history and metadata refusal on fresh copies',
        not_run=['Installed succession, preparation, gap completion, activation or writer delivery',
                 'New-family empty retirement; PostgreSQL 255-epoch capacity; RSS qualification'])
    lab.write_json(output / 'preflight-containers.json', containers)
    lab.write_json(output / 'result.json', report)
    origins = {}
    try:
        for origin in schema.ORIGINS:
            name, pin = origin['name'], PINS[origin['name']]
            assert all(origin[k] == pin[k] for k in pin if k in origin)
            basis = root.parent / origin['run'] / 'bindings'
            for filename, expected in [('result.json', pin['result_sha256']),
                    (pin['snapshot'], pin['snapshot_sha256']), (pin['native_file'], pin['native_sha256']),
                    (pin['inputs_file'], pin['inputs_sha256'])]:
                assert digest(basis / filename) == expected
            retained = read(basis / 'result.json')
            assert retained['status'] == origin['status']
            assert not any(retained[k] for k in ('cleanup_errors', 'missing_owned', 'unrelated_changed'))
            old, = [c for c in containers if c['Id'] == retained['client']['id']]
            assert old['Config']['Labels'][retained['label']] == retained['owner']
            assert not old['State']['Running'] and not old['State']['Pid'] and not old['State']['OOMKilled']
            assert not any(c['State']['Running'] and any(str(basis) in m.get('Source', '')
                for m in c['Mounts']) for c in containers)
            store = basis / pin['store']
            files = census(store)
            assert len(files) <= 128 and sum(v['bytes'] for v in files.values()) < 4 * 1024**2
            inputs = inputs_at(store)
            frozen_inputs = read(basis / pin['inputs_file'])
            assert inputs == frozen_inputs.get('inputs', frozen_inputs)
            origins[name] = dict(store=store, basis=basis, files=files, inputs=inputs,
                native=read(basis / pin['native_file']), rows=read(basis / pin['snapshot']))
            report['origins'][name] = dict(origin, pins=pin, store=str(store), files=files,
                inputs=inputs, native=origins[name]['native'], previous_client=retained['client'])
            lab.write_json(output / (name + '-original.json'), origins[name]['rows'])
        lab.docker('start', pg['postgres'])
        deadline = time.monotonic() + 30
        while lab.docker('exec', pg['postgres'], 'pg_isready', '-U', 'postgres', check=False).returncode:
            assert time.monotonic() < deadline
            time.sleep(.2)
        assert lab.sql(pg, 'postgres', 'SHOW server_version_num;') == '160015'
        report['database_names_before'] = lab.sql(pg, 'postgres', 'SELECT datname FROM pg_database ORDER BY datname;').splitlines()
        for origin in schema.ORIGINS:
            name, database = origin['name'], origin['database']
            assert schema.absence(pg, database) == []
            observed = schema.rows(pg, database)
            assert normalized(observed) == normalized(origins[name]['rows'])
            origins[name]['catalog'] = catalog(pg, database)
            report['origins'][name].update(rows=observed, catalog=origins[name]['catalog'])
            lab.write_json(output / (name + '-original.json'), observed)
        ident = lab.docker('create', '--init', '--name', client, '--label', LABEL + '=' + owner,
            '--cpus', '2', '--memory', '1536m', '--memory-swap', '1536m', '--pids-limit', '128',
            '--read-only', '--cap-drop', 'ALL', '--security-opt', 'no-new-privileges',
            '--user', f'{os.getuid()}:{os.getgid()}', '--network', 'container:' + borrowed['Id'],
            '--tmpfs', '/tmp:rw,exec,nosuid,nodev,size=128m',
            '--mount', f'type=bind,src={output},dst=/lab',
            '--mount', f'type=bind,src={build},dst=/build,readonly',
            '--mount', f'type=bind,src={pg_root}/secrets,dst=/run/secrets,readonly',
            lab.CLIENT_IMAGE, 'python3', '-c', 'import time; time.sleep(1800)').stdout.strip()
        report['client'] = ident
        lab.write_json(output / 'result.json', report)
        lab.docker('start', ident)
        state = dict(pg, client=client)
        for mode in CASES:
            name = 'empty' if mode == 'empty-terminal2' else 'activated'
            original = origins[name]
            database = 'bb_successor_' + revision[:7] + '_' + mode.replace('-', '_')
            assert database not in report['database_names_before']
            case = dict(name=mode, origin=name, database=database, status='RUNNING', migrations=[],
                metadata_bytes=132095 if mode == 'activated-metadata-short' else 132608)
            report['cases'].append(case)
            lab.write_json(output / 'result.json', report)
            lab.sql(pg, 'postgres', f'CREATE DATABASE {database} OWNER bigbrotr_migrator TEMPLATE {PINS[name]["database"]};')
            report['databases'].append(database)
            copied = schema.rows(pg, database)
            lab.write_json(output / (mode + '-copy-before.json'), copied)
            assert normalized(copied) == normalized(original['rows'])
            assert catalog(pg, database) == original['catalog']
            directory = output / mode
            directory.mkdir(mode=0o700)
            shutil.copytree(original['store'], directory / 'store')
            assert content_census(census(directory / 'store')) == content_census(original['files'])
            config = configuration(output, state, database, mode, copied)
            case['config'] = config
            for action in ('plan', 'apply', 'apply'):
                try:
                    answer = lab.cli(state, 'migrate', action, '--file', config, '--user', 'bigbrotr_migrator',
                        '--password-ref', 'file:/run/secrets/migrator', '--target', '38')
                except subprocess.TimeoutExpired as error:
                    def partial(value):
                        return value.decode(errors='replace') if isinstance(value, bytes) else value or ''
                    case['migrations'].append(dict(action=action, exit_code=None,
                        stdout=partial(error.stdout), stderr=partial(error.stderr)))
                    case['migration_outcome'] = 'UNKNOWN_TIMEOUT'
                    lab.write_json(output / 'result.json', report)
                    raise
                case['migrations'].append(dict(action=action, exit_code=answer.returncode, stdout=answer.stdout, stderr=answer.stderr))
                lab.write_json(output / 'result.json', report)
                assert answer.returncode == 0 and not answer.stderr
            migrated = schema.rows(pg, database)
            lab.write_json(output / (mode + '-migrated.json'), migrated)
            statements = [(SOURCE / 'experiments/ingest-succession/schema_grants.sql').read_text(),
                'GRANT SELECT ON control.initial_ingest_activations TO bigbrotr_control;']
            case['grants'] = []
            for statement in statements:
                case['grants'].append(dict(statement=statement, stdout=lab.sql(pg, database, statement)))
            case['catalog_before'] = catalog(pg, database)
            lab.write_json(output / 'result.json', report)
            execute(pg, client, output, case, directory, config)
            case['catalog_after'] = catalog(pg, database)
            assert case['catalog_after'] == case['catalog_before']
            lab.write_json(output / 'result.json', report)
        report['status'] = STATUS
    except BaseException as error:
        report.update(status='FAIL', error=repr(error))
        raise
    finally:
        errors = []
        # Never inspect a clone natively here: a failed wrapper may still own it.
        for case in report['cases']:
            try:
                lab.write_json(output / (case['name'] + '-final-sql.json'), schema.rows(pg, case['database']))
                case['final_backends'] = schema.absence(pg, case['database'])
                case['final_catalog'] = catalog(pg, case['database'])
                assert case['final_backends'] == []
            except BaseException as error:
                errors.append(dict(copy=case['database'], error=repr(error)))
        for origin in schema.ORIGINS:
            name = origin['name']
            if name not in origins:
                continue
            try:
                original = origins[name]
                after_files = census(original['store'])
                preserved = dict(rows_unchanged=normalized(schema.rows(pg, origin['database'])) == normalized(original['rows']),
                    catalog_unchanged='catalog' in original and catalog(pg, origin['database']) == original['catalog'],
                    native_unchanged=content_census(after_files) == content_census(original['files']),
                    inputs_unchanged=inputs_at(original['store']) == original['inputs'],
                    physical_unchanged=after_files == original['files'])
                report['origins'][name]['preserved'] = preserved
                lab.write_json(output / (name + '-preserved.json'), preserved)
                assert all(preserved.values()) and schema.absence(pg, origin['database']) == []
            except BaseException as error:
                errors.append(dict(origin=name, error=repr(error)))
        try:
            names = lab.sql(pg, 'postgres', 'SELECT datname FROM pg_database ORDER BY datname;').splitlines()
            report['database_names_after'] = names
            assert set(names) == set(report['database_names_before']) | set(report['databases'])
            assert all(digest(build / name) == value for name, value in binaries.items())
        except BaseException as error:
            errors.append(dict(inventory_or_build=repr(error)))
        # A lost create reply can leave our uniquely named, labelled container
        # without its returned ID. Recover only that exact owned resource.
        if report['client'] is None:
            try:
                answer = lab.docker('inspect', client, check=False)
                if answer.returncode == 0:
                    item, = json.loads(answer.stdout)
                    assert item['Config']['Labels'].get(LABEL) == owner
                    report['client'] = item['Id']
            except BaseException as error:
                errors.append(dict(client_creation=repr(error)))
        for ident, label, expected in [(report['client'], LABEL, owner), (borrowed['Id'], lab.LABEL, pg['owner'])]:
            if ident is None:
                continue
            try:
                item, = json.loads(lab.docker('inspect', ident).stdout)
                assert item['Config']['Labels'][label] == expected
                if item['State']['Running']:
                    lab.docker('stop', '--time', '10', ident)
                item, = json.loads(lab.docker('inspect', ident).stdout)
                assert not item['State']['Running'] and not item['State']['Pid'] and not item['State']['OOMKilled']
                lab.write_json(output / ('postgres-terminal.json' if ident == borrowed['Id'] else 'client-terminal.json'), projection(item))
            except BaseException as error:
                errors.append(dict(container=ident, error=repr(error)))
        try:
            after = {c['Id']: projection(c) for c in schema.containers()}
            lab.write_json(output / 'terminal-containers.json', after)
            report['unrelated_changed'] = [c['Id'] for c in containers if c['Id'] != borrowed['Id'] and after.get(c['Id']) != projection(c)]
            report['unexpected_containers'] = sorted(set(after) - {c['Id'] for c in containers} - {report['client']})
            report['missing_owned'] = [ident for ident in (report['client'], borrowed['Id']) if ident is not None and ident not in after]
            logs = lab.docker('logs', '--since', report['started_at'], borrowed['Id'])
            (output / 'postgres.log').write_text(logs.stdout + logs.stderr)
        except BaseException as error:
            errors.append(dict(terminal_inventory=repr(error)))
        report.update(cleanup_errors=errors, finished_at=datetime.datetime.now(datetime.timezone.utc).isoformat(),
            free_bytes_after=shutil.disk_usage(root).free)
        if errors or any(report.get(k) for k in ('missing_owned', 'unrelated_changed', 'unexpected_containers')):
            report['status'] = 'INCOMPLETE_CUSTODY'
        lab.write_json(output / 'result.json', report)
    assert report['status'] == STATUS
    print(json.dumps(dict(status=STATUS, cases=[c['name'] for c in report['cases']], root=str(root))))


if __name__ == '__main__':
    assert len(sys.argv) == 3
    run(Path(sys.argv[1]).resolve(strict=True), Path(sys.argv[2]).resolve(strict=True))
