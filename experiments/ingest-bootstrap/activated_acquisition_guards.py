"""Actual application-login SQL counterexamples for activated acquisition gaps.

Each transaction creates a fresh actual-backend Control and preserves the real
historical activator claim. All product guards remain enabled. This is relational
qualification, not an opaque native-proof or authenticated HTTP journey.
"""
import json
from pathlib import Path
import shutil
import subprocess
import time

import lab
import activated_acquisition as acquisition
import activated_recovery as recovery
from activated_acquisition_guards_oracle import CASES, verify, verify_case, negative_controls
from activated_source_prefix import CLOSED
from initial_empty_guards import _setup, _absence, _literal
from initial_gap import configuration
from initial_history import capture, inputs_at, observe_backend_absence, server_clock
from initial_succession import census, digest

MODE = 'activated-acquisition-guards'
GRANT = 'GRANT UPDATE,DELETE,TRUNCATE ON control.acquisition_gaps TO bigbrotr_control;\n'
REVOKE = 'REVOKE UPDATE,DELETE,TRUNCATE ON control.acquisition_gaps FROM bigbrotr_control;\n'


def statements(before):
    intent, = before['initial_ingest_successions']
    activation, = before['initial_ingest_activations']
    source, = [r for r in before['source_streams'] if r['owner_epoch'] == 2]
    job, = [r for r in before['jobs'] if r['kind'] == 'acquisition' and (
        r['owner_resource_id'], r['owner_epoch'], r['owner_incarnation']) == (
        activation['activated_by_resource'], activation['activated_by_epoch'], activation['activated_by_incarnation'])]
    slot, = before['producer_slots']
    output = {}
    for mode in CASES:
        prefix, owner = _setup(before, intent, MODE + '-' + mode, 'bigbrotr_control')
        resource, job_id = _literal(activation['activated_by_resource']), _literal(job['job_id'])
        epoch, incarnation = owner['control_epoch'], "decode('" + owner['incarnation'] + "','hex')"
        def state(stage):
            return f"""SELECT json_build_object('event','state','stage','{stage}',
 'transaction_id',pg_current_xact_id()::text,
 'observed_us',(EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint,
 'control',(SELECT to_jsonb(o)||jsonb_build_object('xmin',o.xmin::text) FROM control.owner_epochs o
   WHERE o.resource_id={resource} AND o.epoch={epoch}),
 'role',(SELECT to_jsonb(r)||jsonb_build_object('xmin',r.xmin::text) FROM control.role_instances r
   WHERE r.role_instance_id=decode('{owner['role_instance']}','hex')),
 'job',(SELECT to_jsonb(j)||jsonb_build_object('xmin',j.xmin::text) FROM control.jobs j WHERE j.job_id={job_id}),
 'gap',(SELECT to_jsonb(g)||jsonb_build_object('xmin',g.xmin::text) FROM control.acquisition_gaps g WHERE g.job_id={job_id}));
"""
        values = dict(producer='2', source='2', reason='activated_initial_producer_abandoned',
            previous_resource=_literal(activation['activated_by_resource']), previous_epoch='8',
            previous_incarnation=_literal(activation['activated_by_incarnation']),
            cut=str(source['last_complete_sequence']), digest=_literal(source['closure_digest']))
        if mode == 'wrong-producer':
            values['producer'] = '1'
        elif mode == 'wrong-source':
            values['source'] = '1'
        elif mode == 'wrong-reason':
            values['reason'] = 'initial_producer_abandoned'
        elif mode == 'registrar-not-activator':
            values.update(previous_resource=_literal(slot['registered_by_resource_id']),
                previous_epoch=str(slot['registered_by_epoch']), previous_incarnation=_literal(slot['registered_by_incarnation']))
        elif mode == 'wrong-cut':
            values['cut'] = str(source['last_complete_sequence'] + 1)
        elif mode == 'wrong-digest':
            raw = bytearray.fromhex(source['closure_digest'][2:])
            raw[0] ^= 1
            values['digest'] = "decode('" + raw.hex() + "','hex')"
        insert = f"""INSERT INTO control.acquisition_gaps
 (job_id,dataset_id,producer_slot_id,producer_epoch,shard_id,source_owner_epoch,
  last_complete_sequence,source_closure_digest,previous_job_revision,
  previous_owner_resource,previous_owner_epoch,previous_owner_incarnation,
  issuer_resource,issuer_epoch,issuer_incarnation,abandoned_at_us,reason)
VALUES ({job_id},{_literal(job['dataset_id'])},{_literal(intent['producer_slot_id'])},{values['producer']},0,{values['source']},
 {values['cut']},{values['digest']},2,{values['previous_resource']},{values['previous_epoch']},{values['previous_incarnation']},
 {resource},{epoch},{incarnation},:fixture_time,'{values['reason']}');
"""
        request = f"""UPDATE control.jobs SET state='cancel_requested',job_revision=3,
 owner_resource_id={resource},owner_epoch={epoch},owner_incarnation={incarnation},
 reason_code='activated_initial_producer_abandoned' WHERE job_id={job_id};
"""
        complete = f"""UPDATE control.jobs SET state='cancelled',job_revision=4,
 completed_at_us=:fixture_time WHERE job_id={job_id};
"""
        sql = prefix + f'SELECT control.require_activated_initial_source({_literal(intent["owner_resource_id"])});\n' + state('baseline')
        if mode == 'immediate-before-insert':
            sql += "SET CONSTRAINTS ALL IMMEDIATE;\nSELECT json_build_object('event','immediate');\n"
        sql += insert + state('gap')
        if mode not in ('gap-only-commit', 'immediate-before-insert') and not mode.startswith('wrong-') and mode != 'registrar-not-activator':
            sql += request + state('requested')
            if mode != 'requested-only-commit':
                sql += complete + state('complete')
                sql += 'SET CONSTRAINTS ALL IMMEDIATE;\n' + state('checked')
        mutation = {
            'terminal-job-update': f'UPDATE control.jobs SET job_revision=job_revision+1 WHERE job_id={job_id};\n',
            'terminal-gap-update': f'UPDATE control.acquisition_gaps SET reason=reason WHERE job_id={job_id};\n',
            'terminal-gap-delete': f'DELETE FROM control.acquisition_gaps WHERE job_id={job_id};\n',
            'terminal-gap-truncate': 'TRUNCATE control.acquisition_gaps;\n',
        }.get(mode, '')
        sql += mutation
        if mode == 'complete-rollback':
            sql += "ROLLBACK;\nSELECT json_build_object('event','rolled-back');\n"
        else:
            sql += "SELECT json_build_object('event','before-commit');\nCOMMIT;\n"
        output[mode] = sql
    return output


def execute(pg, database, output, rows, directory, mode, sql, shared):
    label = MODE + '-' + mode
    response = dict(returncode=None, stdout='', stderr='', outcome='OUTCOME_UNKNOWN')
    case = dict(mode=mode, database=database, statement=sql, statement_sha256='',
        response=response, events=[], server_window={}, capture_errors=[], backend_absence=None)
    path = output / (label + '-verification.json')
    (output / (label + '.sql')).write_text(sql)
    case['statement_sha256'] = digest(output / (label + '.sql'))
    case.update(before=rows(database), catalog_before=acquisition.catalog(pg, database),
        files_before=census(directory / 'store'), inputs_before=inputs_at(directory / 'store'),
        contents_before=recovery.contents_at(directory / 'store'))
    case['server_window']['before_us'] = server_clock(pg, database)
    started = time.monotonic()
    command = ['exec', '-i', '--env', 'PGCONNECT_TIMEOUT=5', pg['postgres'],
        'psql', '-X', '-A', '-t', '-q', '--no-password', '-v', 'ON_ERROR_STOP=1',
        '-v', 'VERBOSITY=verbose', '-U', 'bigbrotr_control', '-d', database]
    case['command'] = command
    try:
        answer = lab.docker(*command, data=sql, check=False, timeout=30)
        response.update(returncode=answer.returncode, stdout=answer.stdout, stderr=answer.stderr, outcome='PROCESS_EXITED')
    except subprocess.TimeoutExpired as error:
        def text(raw):
            return raw.decode(errors='replace') if isinstance(raw, bytes) else raw or ''
        response.update(stdout=text(error.stdout), stderr=text(error.stderr), outcome='UNKNOWN_TIMEOUT')
    finally:
        response['seconds'] = time.monotonic() - started
        lab.write_json(output / (label + '-response.json'), response)
        lab.write_json(path, case)
        # Independent terminal observations precede every semantic assertion.
        try:
            case['events'] = [json.loads(line) for line in response['stdout'].splitlines() if line.startswith('{')]
        except (ValueError, TypeError) as error:
            case['capture_errors'].append('events:' + type(error).__name__)
        backends = [e for e in case['events'] if e.get('event') == 'backend']
        observations = [('clock', lambda: server_clock(pg, database)), ('after', lambda: rows(database)),
            ('catalog_after', lambda: acquisition.catalog(pg, database)),
            ('files_after', lambda: census(directory / 'store')), ('inputs_after', lambda: inputs_at(directory / 'store')),
            ('contents_after', lambda: recovery.contents_at(directory / 'store')),
            ('backend_absence', lambda: _absence(pg, database, 'bigbrotr_control', backends[0] if len(backends) == 1 else None))]
        for name, read in observations:
            try:
                value = read()
                if name == 'clock':
                    case['server_window']['after_us'] = value
                else:
                    case[name] = value
                lab.write_json(output / (label + '-' + name + '.json'), value)
            except Exception as error:
                case['capture_errors'].append(name + ':' + type(error).__name__)
            lab.write_json(path, case)
    verify_case(case, **shared)
    return case


def run_all(pg, client, output, rows, clone):
    spec = CLOSED
    basis = output.parent.parent / spec['run'] / 'bindings'
    store = basis / spec['prefix'] / 'store'
    frozen = {'result.json': spec['result'], spec['sql_file']: spec['sql'],
        spec['native_file']: spec['native'], spec['verification_file']: spec['verification']}
    origin, context, restored = None, None, None
    database = None
    granted = False
    try:
        assert all(digest(basis / name) == value for name, value in frozen.items())
        retained = json.loads((basis / 'result.json').read_text())
        assert all(retained[k] == spec[k] for k in ('source', 'owner', 'status'))
        assert not any(retained[k] for k in ('cleanup_errors', 'unrelated_changed', 'missing_owned'))
        containers = json.loads(lab.docker('inspect', *lab.docker('ps', '-aq').stdout.split()).stdout)
        old, = [c for c in containers if c['Id'] == retained['client']['id']]
        assert old['Config']['Labels'][retained['label']] == spec['owner']
        assert not old['State']['Running'] and not old['State']['Pid'] and not old['State']['OOMKilled']
        assert not any(c['State']['Running'] and any(str(basis) in m.get('Source', '') for m in c['Mounts']) for c in containers)
        absent = observe_backend_absence(pg, spec['database'], output, MODE + '-original')
        original = rows(spec['database'])
        assert original == json.loads((basis / spec['sql_file']).read_text())
        seed = dict(name='off', verification=json.loads((basis / spec['verification_file']).read_text()), control=original,
            native=json.loads((basis / spec['native_file']).read_text()), inputs=inputs_at(store),
            contents=recovery.contents_at(store), catalog=acquisition.catalog(pg, spec['database']))
        origin = dict(seed=seed, files=census(store), frozen=frozen, backend_absence=absent,
            client_id=old['Id'], client_state=old['State'], label=retained['label'])
        lab.write_json(output / (MODE + '-original.json'), origin)
        database = clone(MODE, spec['database'])
        assert rows(database) == original
        directory = output / MODE
        directory.mkdir(mode=0o700)
        shutil.copytree(store, directory / 'store')
        observe_backend_absence(pg, database, output, MODE + '-before')
        answer, _ = capture(client, output, MODE + '-before-dump', 'dump-state', '/lab/' + MODE + '/store')
        assert answer.returncode == 0 and not answer.stderr
        native = json.loads(answer.stdout)
        assert native == seed['native']
        files = census(directory / 'store')
        config = configuration(pg, client, output, database, directory, original)
        upgrade = acquisition.upgrade(pg, client, output, database, config, MODE, rows)
        setup = dict(migration_sha256=digest(Path(__file__).resolve().parents[2] /
            'migrations/postgres/0036_activated_acquisition_abandonment.sql'), migration=upgrade,
            catalog_before=acquisition.catalog(pg, database), catalog_after=acquisition.catalog(pg, database))
        grants = dict(statement=GRANT, before=acquisition.catalog(pg, database), response=None, after=None)
        granted = True  # Even unknown DCL is retained and restoration attempted.
        grants['response'] = recovery.statement(pg, database, output, MODE + '-grant', GRANT)
        grants['after'] = acquisition.catalog(pg, database)
        baseline = rows(database)
        context = dict(seed=seed, setup=setup, grants=grants, baseline=baseline, native=native, files=files, cases=[])
        lab.write_json(output / (MODE + '-verification.json'), context)
        shared = {k: context[k] for k in ('seed', 'setup', 'grants', 'baseline', 'native', 'files')}
        for mode, statement in statements(baseline).items():
            case = execute(pg, database, output, rows, directory, mode, statement, shared)
            context['cases'].append(case)
            lab.write_json(output / (MODE + '-verification.json'), context)
        # No native open occurred during any raw transaction. Inspect only after
        # all fourteen actual backends have been independently excluded.
        answer, _ = capture(client, output, MODE + '-after-dump', 'dump-state', '/lab/' + MODE + '/store')
        assert answer.returncode == 0 and not answer.stderr
        context['native_after'] = json.loads(answer.stdout)
        lab.write_json(output / (MODE + '-verification.json'), context)
    finally:
        if granted:
            restored = dict(statement=REVOKE, response=None, catalog=None, errors=[])
            try:
                restored['response'] = recovery.statement(pg, database, output, MODE + '-revoke', REVOKE)
                restored['catalog'] = acquisition.catalog(pg, database)
            except Exception as error:
                restored['errors'].append(type(error).__name__)
            lab.write_json(output / (MODE + '-restored.json'), restored)
        if context is not None:
            context['restored'] = restored
            lab.write_json(output / (MODE + '-verification.json'), context)
        if origin is not None:
            old, = json.loads(lab.docker('inspect', origin['client_id']).stdout)
            preserved = dict(control=rows(spec['database']) == seed['control'], files=census(store) == origin['files'],
                inputs=inputs_at(store) == seed['inputs'], contents=recovery.contents_at(store) == seed['contents'],
                catalog=acquisition.catalog(pg, spec['database']) == seed['catalog'],
                client=old['State'] == origin['client_state'] and old['Config']['Labels'][origin['label']] == spec['owner'],
                artifacts=all(digest(basis / name) == value for name, value in frozen.items()))
            lab.write_json(output / (MODE + '-source-preserved.json'), preserved)
            assert all(preserved.values()), preserved
    evidence, controls = verify(**context), negative_controls(**context)
    return dict(name='activated-acquisition-sql-guards', status='PASS', sql_attempts=14,
        cases=[dict(name=c['mode'], status='PASS') for c in context['cases']], oracle=evidence,
        negative_controls=controls, source_preserved=preserved,
        not_run=['native-proof dispatch', 'HTTP authentication or grants', 'new historical claims',
            'retirement', 'Archive restart and full M1'])
