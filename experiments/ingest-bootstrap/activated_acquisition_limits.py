"""Genuine 64/65 claims, combined retained bound and unsupported-last atomicity.

Original labs remain read-only. Unknown process/backend custody forbids native
inspection; known exits retain SQL and original inputs even if native capture fails.
The unsupported last job is an explicit privileged negative mutation of a genuine
claim's checkpoint. It creates no claim, owner, gap or authority.
"""
import copy
import json
from pathlib import Path
import shutil
import subprocess
import time
import hashlib

import lab
import activated_recovery as recovery
import initial_live as live
import initial_succession as preparing
from activated_acquisition_limits_oracle import CASES, FENCE, verify, negative_controls, verify_seed, seed_negative_controls
from initial_empty_guards import _setup
from initial_history import capture, inputs_at, observe_backend_absence, server_clock
from initial_succession import census, digest
from activated_source_durable import configure
import activated_acquisition as abandonment
from activated_recovery_zero import invoke
import activated_source_closure as closure

def operator_sql(before, kind, label):
    """Bounded explicit operator changes; never resurrect a historical owner."""
    activation, = before['initial_ingest_activations']
    jobs = sorted((j for j in before['jobs'] if j['kind'] == 'acquisition' and
        (j['owner_resource_id'], j['owner_epoch'], j['owner_incarnation']) ==
        (activation['activated_by_resource'], 8, activation['activated_by_incarnation'])), key=lambda j: j['job_id'])
    if kind == 'unsupported-last':
        # Preserve the genuine job and owner; only make its final batch member
        # unsupported, with the complete preimage and guarded catalog retained.
        job = jobs[-1]['job_id'][2:]
        return ("BEGIN;\nSET LOCAL session_replication_role=replica;\n"
            "UPDATE control.jobs SET checkpoint_revision=1 WHERE job_id=decode('" + job + "','hex')\n"
            "RETURNING (to_jsonb(jobs)||jsonb_build_object('xmin',xmin::text))::text;\nCOMMIT;\n")
    assert kind == 'retained-gap'
    intent, = before['initial_ingest_successions']
    source, = [s for s in before['source_streams'] if s['owner_epoch'] == 2]
    prefix, identity = _setup(before, intent, label, 'bigbrotr_control')
    previous = max((r for r in before['owner_epochs'] if r['role'] == 'control'), key=lambda r: r['epoch'])
    resource = previous['resource_id'][2:]
    inc = identity['incarnation']
    epoch = identity['control_epoch']
    job = jobs[0]
    jid = job['job_id'][2:]
    ds = job['dataset_id'][2:]
    slot = intent['producer_slot_id'][2:]
    oldinc = activation['activated_by_incarnation'][2:]
    proof = hashlib.sha256(b'bigbrotr/control-exclusion/1\0' + bytes.fromhex(ds) + bytes.fromhex(resource)
        + epoch.to_bytes(8, 'big') + bytes.fromhex(inc) * 2).hexdigest()
    return prefix + f"""SELECT control.require_activated_initial_source(decode('{intent['owner_resource_id'][2:]}','hex'));
INSERT INTO control.acquisition_gaps
 (job_id,dataset_id,producer_slot_id,producer_epoch,shard_id,source_owner_epoch,
 last_complete_sequence,source_closure_digest,previous_job_revision,previous_owner_resource,
 previous_owner_epoch,previous_owner_incarnation,issuer_resource,issuer_epoch,issuer_incarnation,
 abandoned_at_us,reason)
VALUES (decode('{jid}','hex'),decode('{ds}','hex'),decode('{slot}','hex'),2,0,2,0,
 decode('{source['closure_digest'][2:]}','hex'),2,decode('{resource}','hex'),8,decode('{oldinc}','hex'),
 decode('{resource}','hex'),{epoch},decode('{inc}','hex'),:fixture_time,'activated_initial_producer_abandoned');
UPDATE control.jobs SET state='cancel_requested',job_revision=3,
 owner_resource_id=decode('{resource}','hex'),owner_epoch={epoch},owner_incarnation=decode('{inc}','hex'),
 reason_code='activated_initial_producer_abandoned' WHERE job_id=decode('{jid}','hex');
UPDATE control.jobs SET state='cancelled',job_revision=4,completed_at_us=:fixture_time WHERE job_id=decode('{jid}','hex');
SET CONSTRAINTS ALL IMMEDIATE;
UPDATE control.owner_epochs SET state='released',released_at_us=(EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint,
 exclusion_method='storage_fenced',exclusion_digest=decode('{proof}','hex')
 WHERE resource_id=decode('{resource}','hex') AND epoch={epoch};
UPDATE control.role_instances SET retired_at_us=(EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint
 WHERE role_instance_id=decode('{identity['role_instance']}','hex');
SELECT json_build_object('event','operator-complete','transaction_id',pg_current_xact_id()::text);
COMMIT;
"""


def run_all(pg, client, output, rows, clone):
    helper = Path(__file__).with_name('activated_acquisition_client.py')
    target = output / helper.name
    if not target.exists():
        shutil.copy2(helper, target)
    assert digest(target) == digest(helper)
    basis = output.parent.parent / preparing.SEED / 'bindings'
    store = basis / 'retire_signed/store'
    source = None
    cases = []
    closure_helper = Path(__file__).with_name('activated_source_closure_client.py')
    closure_target = output / closure_helper.name
    if not closure_target.exists():
        shutil.copy2(closure_helper, closure_target)
    assert digest(closure_target) == digest(closure_helper)

    def state(label, database, directory):
        value = dict(control=rows(database))
        lab.write_json(output / (label + '-sql.json'), value['control'])
        value.update(inputs=inputs_at(directory / 'store'), contents=recovery.contents_at(directory / 'store'))
        lab.write_json(output / (label + '-inputs.json'), dict(inputs=value['inputs'], contents=value['contents']))
        try:
            answer, _ = capture(client, output, label + '-dump', 'dump-state', '/lab/' + directory.name + '/store')
            assert answer.returncode == 0 and not answer.stderr
            value['native'] = json.loads(answer.stdout)
            lab.write_json(output / (label + '-native.json'), value['native'])
        except Exception as error:
            lab.write_json(output / (label + '-state-incomplete.json'), dict(**value,
                native='UNAVAILABLE', status='INCOMPLETE', error=type(error).__name__))
            raise
        lab.write_json(output / (label + '-state.json'), value)
        return value

    def exclude(label, database, directory):
        try:
            return observe_backend_absence(pg, database, output, label)
        except BaseException as error:
            observation = dict(status='OUTCOME_UNKNOWN', native='NOT_RUN', reason=type(error).__name__,
                detail='Backend absence not established; available SQL/inputs are provisional, not final')
            lab.write_json(output / (label + '-external-unavailable.json'), observation)
            for name, read in [('sql', lambda: rows(database)),
                    ('inputs', lambda: dict(inputs=inputs_at(directory / 'store'),
                        contents=recovery.contents_at(directory / 'store')))]:
                try:
                    lab.write_json(output / (label + '-uncertain-' + name + '.json'), read())
                except Exception as capture_error:
                    observation[name + '_capture_error'] = type(capture_error).__name__
            lab.write_json(output / (label + '-external-unavailable.json'), observation)
            raise

    def execute(mode, database, config, directory, seed_context, setup, predecessor=None, operator=None):
        exclude(mode + '-before', database, directory)
        before = state(mode + '-before', database, directory)
        current_setup = copy.deepcopy(setup)
        current_setup['catalog_before'] = abandonment.catalog(pg, database)
        response, clock = invoke(pg, client, output, database, config, mode, closure_helper if mode == FENCE else helper)
        if response['outcome'] != 'PROCESS_EXITED':
            lab.write_json(output / (mode + '-external-unavailable.json'), dict(status='OUTCOME_UNKNOWN',
                native='NOT_RUN', reason='CLI custody unknown; outer owner must join before reopening'))
            raise AssertionError('installed acquisition-limit custody unavailable')
        absent = exclude(mode, database, directory)
        after = state(mode + '-after', database, directory)
        current_setup['catalog_after'] = abandonment.catalog(pg, database)
        lab.write_json(output / (mode + '-external.json'), dict(**after,
            catalog=current_setup['catalog_after'], absence=absent))
        exact = live.exact_absence(pg, database, output, mode, after['control'], before['control'])
        context = dict(before=before['control'], after=after['control'], native_before=before['native'],
            native_after=after['native'], inputs_before=before['inputs'], inputs_after=after['inputs'],
            input_contents=after['contents'], response=response, mode=mode, seed_context=seed_context,
            setup=current_setup, server_window=clock, backend=dict(exact=exact, absence=absent), predecessor=predecessor, operator=operator)
        lab.write_json(output / (mode + '-verification.json'), context)
        evidence = verify(**context)
        controls = negative_controls(**context)
        return dict(name=mode, status='PASS', oracle=evidence, negative_controls=controls,
            verification_sha256=digest(output / (mode + '-verification.json'))), context

    def operate(kind, mode, database, directory):
        exclude(mode + '-operator-before', database, directory)
        before = state(mode + '-operator-before', database, directory)
        label = mode + '-operator'
        sql = operator_sql(before['control'], kind, label)
        (output / (label + '.sql')).write_text(sql)
        operation = dict(kind=kind, label=label, database=database, before=before,
            statement=sql, statement_sha256=hashlib.sha256(sql.encode()).hexdigest(),
            server_window=dict(before_us=server_clock(pg, database)), catalog_before=abandonment.catalog(pg, database),
            response=dict(outcome='OUTCOME_UNKNOWN', returncode=None, stdout='', stderr=''),
            capture_errors=[], backend=None)
        role = 'postgres' if kind == 'unsupported-last' else 'bigbrotr_control'
        command = ['exec', '-i', '--env', 'PGCONNECT_TIMEOUT=5', pg['postgres'], 'psql', '-X', '-A', '-t', '-q',
            '--no-password', '-v', 'ON_ERROR_STOP=1', '-v', 'VERBOSITY=verbose', '-U', role, '-d', database]
        operation['command'] = command
        started = time.monotonic()
        try:
            answer = lab.docker(*command, data=sql, check=False, timeout=30)
            operation['response'].update(outcome='PROCESS_EXITED', returncode=answer.returncode,
                stdout=answer.stdout, stderr=answer.stderr)
        except subprocess.TimeoutExpired as error:
            def partial(v):
                return v.decode(errors='replace') if isinstance(v, bytes) else v or ''
            operation['response'].update(outcome='UNKNOWN_TIMEOUT', stdout=partial(error.stdout), stderr=partial(error.stderr))
        finally:
            operation['response']['seconds'] = time.monotonic() - started
            lab.write_json(output / (label + '-response.json'), operation['response'])
            lab.write_json(output / (label + '.json'), operation)
            try:
                operation['server_window']['after_us'] = server_clock(pg, database)
            except Exception as error:
                operation['capture_errors'].append('clock:' + type(error).__name__)
        if operation['response']['outcome'] != 'PROCESS_EXITED':
            lab.write_json(output / (label + '-external-unavailable.json'), dict(outcome='OUTCOME_UNKNOWN', native='NOT_RUN'))
            for name, read in [('sql', lambda: rows(database)), ('inputs', lambda: dict(
                    inputs=inputs_at(directory / 'store'), contents=recovery.contents_at(directory / 'store')))]:
                try:
                    lab.write_json(output / (label + '-uncertain-' + name + '.json'), read())
                except Exception as error:
                    operation['capture_errors'].append(name + ':' + type(error).__name__)
            lab.write_json(output / (label + '.json'), operation)
            raise AssertionError('operator custody unknown')
        try:
            operation['absence'] = exclude(label, database, directory)
            operation['after'] = state(label + '-after', database, directory)
            operation['catalog_after'] = abandonment.catalog(pg, database)
            events = [json.loads(line) for line in operation['response']['stdout'].splitlines() if line.startswith('{')]
            backends = [e for e in events if e.get('event') == 'backend']
            operation['backend'] = backends[0] if len(backends) == 1 else None
        finally:
            lab.write_json(output / (label + '.json'), operation)
        assert operation['response']['returncode'] == 0 and not operation['response']['stderr']
        assert operation['capture_errors'] == []
        return operation

    def seed(mode, total, database, directory, retired, original, frozen_native):
        config = configure(pg, client, output, database, directory, original)
        label = mode + '-seed'
        catalog_before = live.upgrade(pg, client, output, database, config, label, rows)
        exclude(label + '-before', database, directory)
        before = state(label + '-before', database, directory)
        resolution = lab.docker('exec', client, 'python3', '-c',
            "import json,time; n=round(time.clock_getres(time.CLOCK_REALTIME)*1000000000); print(json.dumps(dict(clock='CLOCK_REALTIME',resolution_ns=n,resolution_us=(n+999)//1000)))", timeout=5)
        lab.write_json(output / (label + '-receive-clock.json'), dict(returncode=resolution.returncode, stdout=resolution.stdout, stderr=resolution.stderr))
        assert resolution.returncode == 0 and not resolution.stderr
        receive_clock = json.loads(resolution.stdout)
        clock = dict(before_us=server_clock(pg, database))
        lab.write_json(output / (label + '-clock.json'), clock)
        command = 'successor-live-acquisition-claims' if total == 2 else 'successor-live-acquisition-limit' + str(total)
        answer = None
        try:
            answer, _ = capture(client, output, label, command, database, '/run/secrets/application', config)
        finally:
            if answer is None:
                lab.write_json(output / (label + '-external-unavailable.json'), dict(status='OUTCOME_UNKNOWN', native='NOT_RUN', reason='Finite seed process custody unknown'))
        try:
            clock['after_us'] = server_clock(pg, database)
        except Exception as error:
            clock['after_error'] = type(error).__name__
        lab.write_json(output / (label + '-clock.json'), clock)
        absent = exclude(label, database, directory)
        after = state(label + '-after', database, directory)
        catalog_after = live.catalog(pg, database)
        lab.write_json(output / (label + '-external.json'), dict(**after, catalog=catalog_after, absence=absent))
        exact = live.exact_absence(pg, database, output, label, after['control'], before['control'])
        context = dict(events=[json.loads(line) for line in answer.stdout.splitlines()], original=before['control'], native=frozen_native,
            inputs=retired['inputs'], retired=retired, server_window=clock, backend=dict(exact=exact, absence=absent),
            receive_clock=receive_clock, response=dict(returncode=answer.returncode, stdout=answer.stdout, stderr=answer.stderr),
            after=after['control'], native_after=after['native'], inputs_after=after['inputs'], input_contents=after['contents'],
            catalog_before=catalog_before, catalog_after=catalog_after)
        if total != 2:
            context['total'] = total
        lab.write_json(output / (label + '-verification.json'), context)
        checked = dict(oracle=verify_seed(**context), negative_controls=seed_negative_controls(**context),
            verification_sha256=digest(output / (label + '-verification.json')))
        lab.write_json(output / (label + '-oracle.json'), checked)
        root = Path(__file__).resolve().parents[2]
        setup = dict(migration=recovery.upgrade(pg, client, output, database, config, mode, rows),
            migration_sha256=digest(root / 'migrations/postgres/0033_activated_initial_recovery.sql'),
            correction_sha256=digest(root / 'migrations/postgres/0034_activated_control_retirement.sql'),
            source_migration_sha256=digest(root / 'migrations/postgres/0035_activated_source_closure.sql'),
            source_upgrade=closure.upgrade(pg, client, output, database, config, mode, rows),
            grant=closure.grant_source(pg, database, output, mode),
            abandonment_migration_sha256=digest(root / 'migrations/postgres/0036_activated_acquisition_abandonment.sql'),
            abandonment_upgrade=abandonment.upgrade(pg, client, output, database, config, mode + '-abandonment', rows))
        return context, setup, checked, config

    try:
        assert all(digest(basis / name) == sha for name, sha in preparing.FROZEN.items())
        retained = json.loads((basis / 'result.json').read_text())
        assert retained['status'] == 'PASS_INSTALLED_INITIAL_RETIREMENT_FIRST_WAVE'
        assert retained['source'] == preparing.SOURCE and retained['owner'] == preparing.OWNER
        assert not any(retained[k] for k in ('cleanup_errors', 'missing_owned', 'unrelated_changed'))
        containers = json.loads(lab.docker('inspect', *lab.docker('ps', '-aq').stdout.split()).stdout)
        old, = [c for c in containers if c['Id'] == retained['client']['id']]
        assert old['Config']['Labels'][retained['label']] == preparing.OWNER
        assert not old['State']['Running'] and not old['State']['Pid'] and not old['State']['OOMKilled']
        assert not any(c['State']['Running'] and any(str(basis) in m.get('Source', '') for m in c['Mounts']) for c in containers)
        absent = observe_backend_absence(pg, preparing.DATABASE, output, 'acquisition-limits-original')
        original = rows(preparing.DATABASE)
        assert original == json.loads((basis / 'retire-signed-retry-after.json').read_text())
        frozen_native = json.loads((basis / 'retire-signed-retry-native.json').read_text())
        retired = dict(control=original, native=frozen_native, inputs=inputs_at(store), sequence=58)
        source = dict(retired=retired, files=census(store), contents=recovery.contents_at(store), client_id=old['Id'],
            client_state=old['State'], label=retained['label'], backend_absence=absent)
        lab.write_json(output / 'acquisition-limits-original.json', source)
        for total, mode in ((64, CASES[0]), (65, CASES[1]), (2, CASES[3])):
            database = clone(mode, preparing.DATABASE)
            assert rows(database) == original
            directory = output / mode
            directory.mkdir(mode=0o700)
            shutil.copytree(store, directory / 'store')
            baseline = state(mode + '-retired', database, directory)
            assert baseline == dict(control=original, native=dict(event='dump-state', rows=frozen_native['rows'], sequence=58),
                inputs=retired['inputs'], contents=source['contents'])
            context, setup, checked, config = seed(mode, total, database, directory, retired, original, frozen_native)
            operator = operate('unsupported-last', mode, database, directory) if total == 2 else None
            first, prior = execute(mode, database, config, directory, context, setup, operator=operator)
            attempts = [first]
            if total == 64:
                retry, _ = execute(mode + '-retry', database, config, directory, context, setup, prior)
                attempts.append(retry)
            elif total == 65:
                fence, prior = execute(FENCE, database, config, directory, context, setup, prior)
                attempts.append(fence)
            cases.append(dict(name=mode, status='PASS', seed=checked, attempts=attempts))
            lab.write_json(output / 'acquisition-limits-cases.json', cases)
            if total == 65:
                target = CASES[2]
                copydb = clone(target, database)
                assert rows(copydb) == prior['after']
                copydir = output / target
                copydir.mkdir(mode=0o700)
                shutil.copytree(directory / 'store', copydir / 'store')
                copyconfig = configure(pg, client, output, copydb, copydir, prior['after'])
                operator = operate('retained-gap', target, copydb, copydir)
                final, _ = execute(target, copydb, copyconfig, copydir, context, setup, prior, operator)
                cases.append(dict(name=target, status='PASS', attempts=[final]))
                lab.write_json(output / 'acquisition-limits-cases.json', cases)
    finally:
        if source is not None:
            old, = json.loads(lab.docker('inspect', source['client_id']).stdout)
            preserved = dict(control=rows(preparing.DATABASE) == original, files=census(store) == source['files'],
                inputs=inputs_at(store) == retired['inputs'], contents=recovery.contents_at(store) == source['contents'],
                client=old['State'] == source['client_state'] and old['Config']['Labels'][source['label']] == preparing.OWNER,
                artifacts=all(digest(basis / name) == sha for name, sha in preparing.FROZEN.items()))
            lab.write_json(output / 'acquisition-limits-original-preserved.json', preserved)
            assert all(preserved.values()), preserved
    assert [c['name'] for c in cases] == list(CASES) and sum(len(c['attempts']) for c in cases) == 6
    return dict(name='installed-activated-acquisition-limits', status='PASS', cases=cases, cli_invocations=6,
        finite_seed_invocations=3, source_preserved=preserved, helper_sha256=digest(helper),
        closure_helper_sha256=digest(closure_helper), scope='genuine64/65 and retained1+selected64; full batch validates before writes',
        not_run=['job-wait cancellation/expiry', 'remote worker exclusion', 'producer retirement', 'full M1'])
