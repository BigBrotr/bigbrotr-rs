"""Installed activated recovery refusals in nine independently owned copies.

Operator corruption is explicit fixture setup, never a product mutation API.
The original installed off seed and all preimages remain retained. A refusal
may follow a durable closing barrier; only the complete captured state decides.
"""
import copy
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re
import shutil
import stat

import lab
import activated_recovery as recovery
from activated_recovery_oracle import known
from activated_recovery_refusals_oracle import verify, negative_controls
from activated_recovery_zero import invoke
from fault_cases import Process
from initial_gap import configuration
from initial_history import capture, inputs_at, observe_backend_absence
from initial_live import exact_absence
from initial_succession import census, digest
from installed import wait_for
from installed_empty import native_ready


CASES = tuple('installed-activated-' + name for name in (
    'missing-input', 'corrupt-input', 'missing-old-dependency', 'corrupt-old-dependency',
    'wrong-activation', 'wrong-handoff', 'extra-producer', 'foreign-stream', 'held-native'))
NATIVE = frozenset(('missing-old-dependency', 'corrupt-old-dependency',
    'wrong-handoff', 'extra-producer', 'foreign-stream'))
WRONG_ACTIVATION = """BEGIN;
SET LOCAL statement_timeout='10s';
SET LOCAL lock_timeout='5s';
SET LOCAL session_replication_role=replica;
UPDATE control.initial_ingest_activations SET target_manifest_sha256=
    set_byte(target_manifest_sha256,0,get_byte(target_manifest_sha256,0) # 1)
WHERE target_epoch=2
RETURNING (to_jsonb(initial_ingest_activations)||jsonb_build_object('xmin',xmin::text))::text;
COMMIT;
"""


def corrupt_input(directory, output, mode, preimage):
    slot = preimage['control']['initial_ingest_successions'][0]['producer_slot_id'][2:]
    name = slot + '-0000000000000002-0000000000000002.cbor'
    path = directory / 'store/producer-inputs-v1' / name
    metadata = path.lstat()
    assert stat.S_ISREG(metadata.st_mode) and metadata.st_nlink == 1
    assert stat.S_IMODE(metadata.st_mode) == 0o600
    original = path.read_bytes()
    assert original.hex() == preimage['contents'][name]
    retained = output / (mode + '-input-original.cbor')
    with retained.open('xb') as stream:
        os.fchmod(stream.fileno(), 0o600)
        stream.write(original)
        stream.flush()
        os.fsync(stream.fileno())
    operation = dict(kind='input', action='remove' if mode.endswith('missing-input') else 'replace',
        name=name, retained=dict(path='/lab/' + retained.name, bytes=retained.stat().st_size,
            sha256=digest(retained), hex=retained.read_bytes().hex(), mode=stat.S_IMODE(retained.stat().st_mode)),
        before_mode=0o600, after_mode=None, replacement_hex=None)
    # Persist the complete preimage before changing the owned clone.
    lab.write_json(output / (mode + '-input-operation.json'), operation)
    if operation['action'] == 'remove':
        path.unlink()
    else:
        changed = original[:-1] + bytes([original[-1] ^ 1])
        with path.open('r+b') as stream:
            stream.write(changed)
            stream.flush()
            os.fsync(stream.fileno())
        operation.update(after_mode=stat.S_IMODE(path.stat().st_mode), replacement_hex=path.read_bytes().hex())
    lab.write_json(output / (mode + '-input-operation.json'), operation)
    return operation


def activation_diagnostic(pg, backend, window):
    """Only the named validator error for the actual fresh backend and run."""
    since, until = [datetime.fromtimestamp(window[key] / 1_000_000, timezone.utc).isoformat()
        for key in ('before_us', 'after_us')]
    answer = lab.docker('logs', '--timestamps', '--since', since, '--until', until,
        pg['postgres'], check=False, timeout=5)
    output = answer.stdout + answer.stderr
    assert len(output.encode()) <= 256 * 1024
    message = 'activated recovery requires exact committed preparation and activation'
    pattern = rf'\[{backend["pid"]}\] ERROR:  (?:(?P<sqlstate>[0-9A-Z]{{5}}): )?' + re.escape(message) + '$'
    errors = [line for line in output.splitlines() if re.search(pattern, line)]
    contexts = [line for line in output.splitlines() if f'[{backend["pid"]}] CONTEXT:  ' in line
        and re.search(r'function (?:control\.)?require_activated_initial_recovery\(bytea\)', line)]
    observed = re.search(pattern, errors[0]).group('sqlstate') if len(errors) == 1 else None
    return dict(backend=backend, since=since, until=until, returncode=answer.returncode,
        observed_sqlstate=observed, errors=errors, contexts=contexts)


def held_invoke(pg, client, output, database, config, directory, mode, helper, mutation):
    operation = mutation['operation']
    holder = None
    response = dict(returncode=None, stdout='', stderr='', outcome='OUTCOME_UNKNOWN')
    clock = {}
    try:
        dataset, = mutation['after']['control']['datasets']
        command = ['docker', 'exec', '-i', client, '/build/ingest_bootstrap_probe', 'history-hold',
            database, '/run/secrets/application', '/lab/' + directory.name + '/store',
            dataset['dataset_id'][2:], dataset['instance_id'][2:]]
        operation.update(command=command, joined=False, forced=False, cleanup_errors=[])
        holder = Process(command, input_pipe=True)
        operation['ready'] = wait_for(lambda: native_ready(holder), 'activated recovery native holder', 5)
        lab.write_json(output / (mode + '-mutation.json'), mutation)
        response, clock = invoke(pg, client, output, database, config, mode, helper)
        operation['alive_during_cli'] = holder.process.poll() is None
    except Exception as error:
        response['holder_error'] = type(error).__name__
    finally:
        if holder is not None:
            try:
                holder.send('release')
                operation.update(exit_code=holder.wait(5), stdout=bytes(holder.stdout).decode(),
                    stderr=bytes(holder.stderr).decode(), joined=True)
            except BaseException as error:
                operation['forced'] = True
                operation['cleanup_errors'].append(type(error).__name__)
            finally:
                try:
                    holder.stop()
                except Exception as error:
                    operation['cleanup_errors'].append(type(error).__name__)
                operation.update(stdout=bytes(holder.stdout).decode(errors='replace'),
                    stderr=bytes(holder.stderr).decode(errors='replace'))
        lab.write_json(output / (mode + '-mutation.json'), mutation)
        lab.write_json(output / (mode + '-response.json'), response)
    return response, clock


def run_all(pg, client, output, rows, clone):
    helper = Path(__file__).with_name('activated_recovery_client.py')
    target = output / helper.name
    if not target.exists():
        shutil.copy2(helper, target)
    assert digest(target) == digest(helper)
    spec = recovery.SOURCES['off']
    basis = output.parent.parent / spec['run'] / 'bindings'
    store = basis / spec['prefix'] / 'store'
    frozen = {'result.json': spec['result'], spec['sql_file']: spec['sql'],
        spec['prefix'] + '-after-native.json': spec['native'],
        spec['prefix'] + '-verification.json': spec['verification']}
    source, cases = None, []

    def dump(label, directory):
        answer, _ = capture(client, output, label + '-dump', 'dump-state', '/lab/' + directory.name + '/store')
        assert answer.returncode == 0 and not answer.stderr
        value = json.loads(answer.stdout)
        lab.write_json(output / (label + '-native.json'), value)
        return value

    def state(label, database, directory):
        # Callers establish exclusion before final observations. Persist the
        # independently available SQL/inputs before a native inspector can fail.
        value = dict(control=rows(database))
        lab.write_json(output / (label + '-sql.json'), value['control'])
        value.update(inputs=inputs_at(directory / 'store'), contents=recovery.contents_at(directory / 'store'))
        lab.write_json(output / (label + '-inputs.json'), dict(inputs=value['inputs'], contents=value['contents']))
        try:
            value['native'] = dump(label, directory)
        except Exception as error:
            lab.write_json(output / (label + '-state-incomplete.json'), dict(**value,
                native='UNAVAILABLE', status='INCOMPLETE', error=type(error).__name__))
            raise
        lab.write_json(output / (label + '-state.json'), value)
        return value

    def exclude(label, database, directory):
        try:
            return observe_backend_absence(pg, database, output, label)
        except Exception as error:
            value = dict(status='OUTCOME_UNKNOWN', native='NOT_RUN', error=type(error).__name__,
                detail='Backend absence unproved; available SQL and input captures are provisional')
            lab.write_json(output / (label + '-external-unavailable.json'), value)
            for name, read in [('sql', lambda: rows(database)), ('inputs', lambda:
                    dict(inputs=inputs_at(directory / 'store'), contents=recovery.contents_at(directory / 'store')))]:
                try:
                    lab.write_json(output / (label + '-uncertain-' + name + '.json'), read())
                except Exception as capture_error:
                    value[name + '_capture_error'] = type(capture_error).__name__
            lab.write_json(output / (label + '-external-unavailable.json'), value)
            raise

    def mutate(mode, database, directory):
        variant = mode.removeprefix('installed-activated-')
        mutation = dict(preimage=state(mode + '-operator-before', database, directory), operation=None)
        lab.write_json(output / (mode + '-mutation.json'), mutation)
        known_exit = True
        try:
            if variant in NATIVE:
                command = ['/build/ingest_bootstrap_probe', 'activated-corrupt', '/lab/' + directory.name + '/store', variant]
                mutation['operation'] = dict(kind='native', variant=variant, command=command)
                known_exit = False
                answer, _ = capture(client, output, mode + '-corruption', *command[1:])
                known_exit = True
                mutation['operation']['response'] = dict(returncode=answer.returncode, stdout=answer.stdout, stderr=answer.stderr)
            elif variant in ('missing-input', 'corrupt-input'):
                mutation['operation'] = corrupt_input(directory, output, mode, mutation['preimage'])
            elif variant == 'wrong-activation':
                original, = mutation['preimage']['control']['initial_ingest_activations']
                mutation['operation'] = dict(kind='sql', statement=WRONG_ACTIVATION, original=original)
                lab.write_json(output / (mode + '-mutation.json'), mutation)
                known_exit = False
                mutation['operation']['response'] = recovery.statement(pg, database, output, mode + '-activation-corruption', WRONG_ACTIVATION)
                known_exit = True
            else:
                assert variant == 'held-native'
                mutation['operation'] = dict(kind='native-held')
        finally:
            if known_exit:
                mutation['after'] = state(mode + '-operator-after', database, directory)
            else:
                mutation['outcome'] = 'OUTCOME_UNKNOWN'
                mutation['native'] = 'NOT_RUN'
            lab.write_json(output / (mode + '-mutation.json'), mutation)
        operation = mutation['operation']
        # Preserve the actual whole state before parsing/checking mutation output.
        if operation['kind'] in ('native', 'sql'):
            answer = operation['response']
            assert answer['returncode'] == 0 and answer['stderr'] == ''
            if operation['kind'] == 'native':
                operation['events'] = [json.loads(line) for line in answer['stdout'].splitlines()]
            else:
                operation['changed'] = json.loads(answer['stdout'])
            lab.write_json(output / (mode + '-mutation.json'), mutation)
        return mutation

    def execute(mode, database, config, directory, seed, setup, mutation):
        exclude(mode + '-before', database, directory)
        before = state(mode + '-before', database, directory)
        current_setup = dict(copy.deepcopy(setup), catalog_before=recovery.catalog(pg, database), diagnostic=None)
        if mode.endswith('held-native'):
            response, clock = held_invoke(pg, client, output, database, config, directory, mode, helper, mutation)
        else:
            response, clock = invoke(pg, client, output, database, config, mode, helper)
        operation = mutation['operation']
        held_known = operation['kind'] != 'native-held' or (operation.get('joined') is True
            and operation.get('forced') is False and not operation.get('cleanup_errors'))
        if response['outcome'] != 'PROCESS_EXITED' or not held_known:
            lab.write_json(output / (mode + '-external-unavailable.json'), dict(status='OUTCOME_UNKNOWN',
                native='NOT_RUN', reason='CLI or native holder custody unavailable; outer owner must join'))
            raise AssertionError('installed refusal native custody unavailable')
        absent = exclude(mode, database, directory)
        after = state(mode + '-after', database, directory)
        current_setup['catalog_after'] = recovery.catalog(pg, database)
        lab.write_json(output / (mode + '-external.json'), dict(**after,
            catalog=current_setup['catalog_after'], absence=absent))
        old = {(r['resource_id'], r['epoch']) for r in before['control']['owner_epochs'] if r['role'] == 'control'}
        added = [r for r in after['control']['owner_epochs'] if r['role'] == 'control'
            and (r['resource_id'], r['epoch']) not in old]
        exact = exact_absence(pg, database, output, mode, after['control'], before['control']) if added else None
        context = dict(before=before['control'], after=after['control'], native_before=before['native'],
            native_after=after['native'], inputs_before=before['inputs'], inputs_after=after['inputs'],
            input_contents=after['contents'], response=response, mode=mode, seed=seed,
            setup=current_setup, server_window=clock, backend=dict(exact=exact, absence=absent), mutation=mutation)
        lab.write_json(output / (mode + '-verification.json'), context)
        if mode.endswith('wrong-activation'):
            current_setup['diagnostic'] = activation_diagnostic(pg, exact['expected'], clock)
            lab.write_json(output / (mode + '-diagnostic.json'), current_setup['diagnostic'])
            lab.write_json(output / (mode + '-verification.json'), context)
        evidence = verify(**context)
        controls = negative_controls(**context)
        return dict(name=mode, status='PASS', oracle=evidence, negative_controls=controls,
            verification_sha256=digest(output / (mode + '-verification.json')))

    try:
        assert all(digest(basis / name) == sha for name, sha in frozen.items())
        retained = json.loads((basis / 'result.json').read_text())
        assert retained['status'] == spec['status'] and retained['source'] == spec['source'] and retained['owner'] == spec['owner']
        assert not any(retained[k] for k in ('cleanup_errors', 'unrelated_changed', 'missing_owned'))
        containers = json.loads(lab.docker('inspect', *lab.docker('ps', '-aq').stdout.split()).stdout)
        old, = [c for c in containers if c['Id'] == retained['client']['id']]
        assert old['Config']['Labels'][retained['label']] == spec['owner']
        assert not old['State']['Running'] and not old['State']['Pid'] and not old['State']['OOMKilled']
        assert not any(c['State']['Running'] and any(str(basis) in m.get('Source', '') for m in c['Mounts']) for c in containers)
        absent = observe_backend_absence(pg, spec['database'], output, 'activated-refusal-source')
        original = rows(spec['database'])
        assert original == json.loads((basis / spec['sql_file']).read_text())
        seed = dict(name='off', control=original,
            native=json.loads((basis / (spec['prefix'] + '-after-native.json')).read_text()),
            inputs=inputs_at(store), contents=recovery.contents_at(store))
        known(seed, 'off')
        source = dict(seed=seed, files=census(store), frozen=frozen,
            catalog=recovery.previous_catalog(pg, spec['database']), backend_absence=absent,
            client_id=old['Id'], client_state=old['State'], label=retained['label'])
        lab.write_json(output / 'activated-refusal-source.json', source)
        for mode in CASES:
            # The full old-dependency label exceeds PostgreSQL's63-byte name
            # once the outer run prefix is added; retain it in artifact labels.
            database = clone(mode.removeprefix('installed-'), spec['database'])
            assert rows(database) == seed['control']
            directory = output / mode
            directory.mkdir(mode=0o700)
            shutil.copytree(store, directory / 'store')
            assert dump(mode + '-seed', directory) == seed['native']
            assert recovery.contents_at(directory / 'store') == seed['contents']
            config = configuration(pg, client, output, database, directory, seed['control'])
            migration = recovery.upgrade(pg, client, output, database, config, mode, rows)
            schema = Path(__file__).resolve().parents[2] / 'migrations/postgres/0033_activated_initial_recovery.sql'
            setup = dict(migration=migration, migration_sha256=digest(schema),
                correction_sha256=digest(schema.with_name('0034_activated_control_retirement.sql')),
                policy=recovery.advance_policy(pg, database, output, mode, rows), removed_copy=None)
            mutation = mutate(mode, database, directory)
            cases.append(execute(mode, database, config, directory, seed, setup, mutation))
            lab.write_json(output / 'activated-refusal-cases.json', cases)
    finally:
        if source is not None:
            old, = json.loads(lab.docker('inspect', source['client_id']).stdout)
            preserved = dict(control=rows(spec['database']) == source['seed']['control'],
                files=census(store) == source['files'], inputs=inputs_at(store) == source['seed']['inputs'],
                contents=recovery.contents_at(store) == source['seed']['contents'],
                catalog=recovery.previous_catalog(pg, spec['database']) == source['catalog'],
                client=old['State'] == source['client_state'] and old['Config']['Labels'][source['label']] == spec['owner'],
                artifacts=all(digest(basis / name) == sha for name, sha in frozen.items()))
            lab.write_json(output / 'activated-refusal-source-preserved.json', preserved)
            assert all(preserved.values()), preserved
    assert [c['name'] for c in cases] == list(CASES)
    return dict(name='installed-activated-recovery-refusals', status='PASS', cli_invocations=9,
        cases=cases, source_preserved=preserved, helper_sha256=digest(helper),
        scope='installed refusal with explicit corruption preimages and observed durable closing state',
        not_run=['same-session poison or driver-result telemetry', 'corruption repair',
            'source2 closure/retirement', 'repeated lifetimes and Archive restart', 'full M1'])
