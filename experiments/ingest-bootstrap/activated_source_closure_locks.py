"""Actual installed source closure versus an ordinary guarded relational job.

The privileged job writer does not impersonate an authenticated export adapter.
Only copied fixture databases receive the removable source-COMMIT barrier.
"""
import copy
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import json
from pathlib import Path
import re
import shutil

import lab
import activated_recovery as recovery
import activated_source_closure as source
from activated_recovery_zero import invoke
from activated_source_closure_locks_oracle import CASES, verify, negative_controls
from concurrency_cases import Session
from initial_gap import configuration
from initial_history import capture, inputs_at, observe_backend_absence, server_clock
from initial_live import exact_absence
from initial_succession import census, digest
from installed import wait_for
from source_closure_faults import LOCK, install_barrier

JOB, ACTOR, REQUEST, DIGEST = 'fa'*16, 'f9'*16, 'fb'*16, 'fc'*32
INSERT = f"""INSERT INTO control.jobs(job_id,dataset_id,principal_id,request_key,request_digest,
    kind,config_revision,policy_revision,created_at_us,input_topology_generation)
SELECT decode('{JOB}','hex'),dataset_id,decode('{ACTOR}','hex'),decode('{REQUEST}','hex'),
    decode('{DIGEST}','hex'),'export',config_revision,policy_revision,1,topology_generation
FROM control.datasets;"""
SEAL = f"""INSERT INTO control.job_input_bounds
SELECT decode('{JOB}','hex'),dataset_id,topology_generation,shard_id,owner_epoch,
    CASE owner_epoch WHEN 1 THEN 12 WHEN 2 THEN 6 END
FROM control.topology_streams WHERE topology_generation=1;
UPDATE control.jobs SET input_complete=true WHERE job_id=decode('{JOB}','hex');"""
BARRIER = f"""
        CREATE SCHEMA source_closure_fixture;
        CREATE FUNCTION source_closure_fixture.wait_for_observer() RETURNS trigger
        LANGUAGE plpgsql AS $$ BEGIN
            PERFORM pg_advisory_xact_lock({LOCK[0]}, {LOCK[1]});
            RETURN NEW;
        END $$;
        CREATE CONSTRAINT TRIGGER source_commit_barrier AFTER UPDATE ON control.source_streams DEFERRABLE INITIALLY DEFERRED FOR EACH ROW WHEN (NEW.state = 'closed')
            EXECUTE FUNCTION source_closure_fixture.wait_for_observer();
    """
REMOVE = "DROP TRIGGER source_commit_barrier ON control.source_streams; DROP FUNCTION source_closure_fixture.wait_for_observer(); DROP SCHEMA source_closure_fixture;"


def guard_bodies(pg, database):
    return json.loads(lab.sql(pg, database, "SET search_path=pg_catalog; SELECT json_agg(json_build_object("
        "'name',p.proname,'source',p.prosrc,'language',l.lanname,'security_definer',p.prosecdef,"
        "'configuration',p.proconfig) ORDER BY p.proname) FROM pg_proc p JOIN pg_language l ON l.oid=p.prolang "
        "WHERE p.oid IN ('control.require_complete_job_input()'::regprocedure,"
        "'control.guard_source_closure_bounds()'::regprocedure,'control.lock_job_admission()'::regprocedure);"))


def barrier_objects(pg, database):
    return json.loads(lab.sql(pg, database, "SELECT json_build_object('schema',"
        "EXISTS(SELECT 1 FROM pg_namespace WHERE nspname='source_closure_fixture'),"
        "'functions',(SELECT coalesce(json_agg(json_build_object('name',p.proname,'source',p.prosrc,"
        "'language',l.lanname,'security_definer',p.prosecdef,'configuration',p.proconfig)),'[]') "
        "FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace JOIN pg_language l ON l.oid=p.prolang "
        "WHERE n.nspname='source_closure_fixture'));"))


def identity(session):
    answer = session.execute("SELECT json_build_object('session_replication_role',current_setting('session_replication_role'),"
        "'isolation',current_setting('transaction_isolation'),'user',session_user,'role',current_user);")
    value, = map(json.loads, answer['lines'])
    assert value == dict(session_replication_role='origin', isolation='read committed', user='postgres', role='postgres')
    return value


def private_job(session):
    answer = session.execute(f"""SELECT json_build_object('transaction_id',pg_current_xact_id()::text,
      'jobs',(SELECT json_agg(to_jsonb(t)||jsonb_build_object('xmin',xmin::text)) FROM control.jobs t WHERE job_id=decode('{JOB}','hex')),
      'job_input_bounds',(SELECT json_agg(to_jsonb(t)||jsonb_build_object('xmin',xmin::text) ORDER BY owner_epoch) FROM control.job_input_bounds t WHERE job_id=decode('{JOB}','hex')),
      'job_admissions',(SELECT json_agg(to_jsonb(t)||jsonb_build_object('xmin',xmin::text)) FROM control.job_admissions t WHERE job_id=decode('{JOB}','hex')));""")
    value, = map(json.loads, answer['lines'])
    return value


def blocked(pg, database, holder, waiter=None):
    predicate = f"a.pid={waiter['pid']} AND a.backend_start='{waiter['backend_start']}'::timestamptz" if waiter else "a.usename='bigbrotr_control'"
    query = f"""SELECT coalesce(json_agg(json_build_object('pid',a.pid,'backend_start',a.backend_start,
        'database',a.datname,'user',a.usename,'query',a.query,'wait_event_type',a.wait_event_type,
        'wait_event',a.wait_event,'blockers',pg_blocking_pids(a.pid),
        'observed_at_us',(EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint,
        'holder_start',h.backend_start,'holder_user',h.usename,
        'waiting_locks',(SELECT coalesce(json_agg(json_build_object('type',locktype,'mode',mode,
            'relation',relation::regclass::text,'transaction',transactionid::text)),'[]') FROM pg_locks WHERE pid=a.pid AND NOT granted))), '[]')
        FROM pg_stat_activity a JOIN pg_stat_activity h ON h.pid={holder['pid']}
        AND h.backend_start='{holder['backend_start']}'::timestamptz AND h.datname=current_database()
        WHERE a.datname=current_database() AND {predicate} AND a.wait_event_type='Lock'
        AND h.pid=ANY(pg_blocking_pids(a.pid));"""
    return json.loads(lab.sql(pg, database, query)) or None


def diagnostic(pg, backend, clock, job_first):
    function, message = (('guard_source_closure_bounds','source closure is below an already recorded source cut')
        if job_first else ('require_complete_job_input','initial job cut exceeds authoritative source closure'))
    since, until = (datetime.fromtimestamp(clock[k]/1_000_000, timezone.utc).isoformat() for k in ('before_us','after_us'))
    response = lab.docker('logs','--timestamps','--since',since,'--until',until,pg['postgres'],timeout=5)
    raw = response.stdout + response.stderr
    assert len(raw.encode()) <= 256*1024
    pattern = rf'\[{backend["pid"]}\] ERROR:  (?:(?P<sqlstate>[0-9A-Z]{{5}}): )?' + re.escape(message) + '$'
    errors = [line for line in raw.splitlines() if re.search(pattern,line)]
    contexts = [line for line in raw.splitlines() if f'[{backend["pid"]}] CONTEXT:  ' in line and re.search(r'function (?:control\.)?'+function+r'\(\)',line)]
    code = re.search(pattern,errors[0]).group('sqlstate') if len(errors)==1 else None
    return dict(backend=backend,since=since,until=until,returncode=response.returncode,observed_sqlstate=code,errors=errors,contexts=contexts)


def run(pg, client, output, rows, clone):
    helper = Path(__file__).with_name('activated_source_closure_client.py')
    for path in (helper,):
        target = output / path.name
        if not target.exists():
            shutil.copy2(path, target)
        assert digest(target) == digest(path)
    spec = recovery.SOURCES['off']
    basis = output.parent.parent / spec['run'] / 'bindings'
    store = basis / spec['prefix'] / 'store'
    frozen = {'result.json': spec['result'], spec['sql_file']: spec['sql'],
        spec['prefix'] + '-after-native.json': spec['native'],
        spec['prefix'] + '-verification.json': spec['verification']}
    original_source, cases = None, []

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
        except Exception as error:
            value = dict(status='OUTCOME_UNKNOWN', native='NOT_RUN', error=type(error).__name__,
                detail='Backend absence unproved; available SQL and inputs are provisional')
            lab.write_json(output / (label + '-external-unavailable.json'), value)
            for name, read in [('sql', lambda: rows(database)), ('inputs', lambda:
                    dict(inputs=inputs_at(directory / 'store'), contents=recovery.contents_at(directory / 'store')))]:
                try:
                    lab.write_json(output / (label + '-uncertain-' + name + '.json'), read())
                except Exception as capture_error:
                    value[name + '_capture_error'] = type(capture_error).__name__
            lab.write_json(output / (label + '-external-unavailable.json'), value)
            raise

    def execute(mode, database, config, directory, seed, setup):
        job_first = mode.endswith('job-first')
        exclude(mode+'-before',database,directory)
        before = state(mode+'-before',database,directory)
        local = dict(copy.deepcopy(setup),catalog_before=source.catalog(pg,database),diagnostic=None)
        fixture = dict(kind='privileged-relational-job',product_job_acceptance=False,
            window=dict(before_us=server_clock(pg,database)),job=None,barrier=None,locks={},sessions=[],cleanup_errors=[],
            guard_bodies_before=guard_bodies(pg,database),barrier_objects_before=barrier_objects(pg,database))
        save = lambda: lab.write_json(output/(mode+'-fixture.json'),fixture)
        save()
        sessions, errors = [], []
        response, clock = dict(outcome='OUTCOME_UNKNOWN'), {}
        future = None
        def owned():
            observed, = json.loads(lab.docker('inspect',pg['postgres']).stdout)
            assert observed['State']['Running'] and observed['Config']['Labels'][lab.LABEL]==pg['owner']
        def action(session, name, statement, expected='00000'):
            value = dict(name=name,statement=statement)
            fixture['job']['actions'].append(value);save()
            session.start(statement)
            value['response']=session.finish();save()
            session.expect(value['response'],expected)
            return value['response']
        with ThreadPoolExecutor(max_workers=1) as executor:
            try:
                job = Session(pg,database,mode+'_job',sessions,role='postgres')
                fixture['job']=dict(backend=job.backend,identity=identity(job),actions=[])
                save()
                action(job,'begin','BEGIN;')
                if job_first:
                    action(job,'insert',INSERT)
                    action(job,'seal',SEAL)
                    action(job,'force-constraints','SET CONSTRAINTS ALL IMMEDIATE;')
                    fixture['job']['private']=private_job(job)
                    fixture['uncommitted_external']=rows(database)
                    assert fixture['uncommitted_external']==before['control']
                else:
                    fixture['barrier']=dict(statement=BARRIER,remove_statement=REMOVE)
                    (output/(mode+'-barrier.sql')).write_text(BARRIER)
                    save()
                    assert install_barrier(pg,database,deferred=True)==BARRIER
                    fixture['barrier'].update(installed_catalog=source.catalog(pg,database),objects=barrier_objects(pg,database))
                    barrier=Session(pg,database,mode+'_barrier',sessions,role='postgres')
                    fixture['barrier'].update(backend=barrier.backend,identity=identity(barrier))
                    fixture['barrier']['lock']=barrier.execute(f'SELECT pg_advisory_lock({LOCK[0]},{LOCK[1]});')
                save()
                future=executor.submit(invoke,pg,client,output,database,config,mode,helper)
                wait_for(lambda:(output/(mode+'.pid.json')).exists(),'source-lock CLI pid',5)
                if job_first:
                    observed=wait_for(lambda:blocked(pg,database,job.backend),'job blocks actual CLI',2)
                    fixture['locks']['job_blocks_cli']=observed;save()
                    action(job,'commit','COMMIT;')
                else:
                    observed=wait_for(lambda:blocked(pg,database,barrier.backend),'source COMMIT barrier',2)
                    fixture['locks']['source_commit']=observed;save()
                    owner,=observed
                    # Starting the INSERT is asynchronous; capture its response
                    # after releasing the observed source transaction promptly.
                    entry=dict(name='insert',statement=INSERT)
                    fixture['job']['actions'].append(entry);save();job.start(INSERT)
                    observed=wait_for(lambda:blocked(pg,database,owner,job.backend),'source blocks actual job',2)
                    fixture['locks']['source_blocks_job']=observed;save()
                    fixture['barrier']['unlock']=barrier.execute(f'SELECT pg_advisory_unlock({LOCK[0]},{LOCK[1]});')
                    entry['response']=job.finish();save();job.expect(entry['response'])
                    action(job,'seal',SEAL)
                    fixture['job']['private']=private_job(job);save()
                    action(job,'commit','COMMIT;',expected='23514')
                    action(job,'rollback','ROLLBACK;')
            except Exception as error:
                errors.append(type(error).__name__)
                fixture['schedule_error']=type(error).__name__;save()
            finally:
                # Release only these exact fixture backends. A pending CLI is
                # still owned by its wrapper/supervisor and is never inferred dead.
                for session in reversed(sessions):
                    item=dict(backend=session.backend,role=session.role,name=session.name)
                    fixture['sessions'].append(item);save()
                    try:
                        session.close(owned)
                        b=session.backend
                        absent=lab.sql(pg,'postgres',f"SELECT count(*) FROM pg_stat_activity WHERE pid={b['pid']} AND backend_start='{b['backend_start']}'::timestamptz AND datname='{database}';")
                        item.update(wrapper_exit=session.process.returncode,absence=absent)
                        assert absent=='0'
                    except Exception as error:
                        item['error']=type(error).__name__;fixture['cleanup_errors'].append(type(error).__name__)
                    save()
                if future is not None:
                    try: response,clock=future.result(timeout=55)
                    except Exception as error: fixture['invoke_error']=type(error).__name__;save()
        try: fixture['window']['after_us']=server_clock(pg,database)
        except Exception as error: fixture['clock_error']=type(error).__name__
        save()
        if response.get('outcome')!='PROCESS_EXITED':
            lab.write_json(output/(mode+'-external-unavailable.json'),dict(status='OUTCOME_UNKNOWN',native='NOT_RUN',reason='CLI custody unknown; outer owner must join'))
            raise AssertionError('source lock CLI custody unavailable')
        absent=exclude(mode,database,directory)
        # Preserve all actual terminal bytes before barrier-removal or semantic
        # assertions; a failed removal must not erase known native/SQL effects.
        after=state(mode+'-after',database,directory)
        fixture['catalog_terminal']=source.catalog(pg,database)
        lab.write_json(output/(mode+'-external.json'),dict(**after,catalog=fixture['catalog_terminal'],absence=absent))
        if fixture['barrier'] is not None:
            try: fixture['barrier']['remove_response']=recovery.statement(pg,database,output,mode+'-barrier-remove',REMOVE)
            except Exception as error:fixture['barrier']['remove_error']=type(error).__name__
        local['catalog_after']=source.catalog(pg,database)
        fixture['barrier_objects_after']=barrier_objects(pg,database)
        fixture['guard_bodies_after']=guard_bodies(pg,database)
        save()
        exact=exact_absence(pg,database,output,mode,after['control'],before['control'])
        observed=exact['expected'] if job_first else {k:fixture['job']['backend'][k] for k in ('pid','backend_start','database')}
        fixture['diagnostic']=diagnostic(pg,observed,fixture['window'],job_first);save()
        context=dict(before=before['control'],after=after['control'],native_before=before['native'],native_after=after['native'],
            inputs_before=before['inputs'],inputs_after=after['inputs'],input_contents=after['contents'],response=response,mode=mode,
            seed=seed,setup=local,server_window=clock,backend=dict(exact=exact,absence=absent),fixture=fixture)
        lab.write_json(output/(mode+'-verification.json'),context)
        assert not errors and not fixture['cleanup_errors'],fixture
        checked,controls=verify(**context),negative_controls(**context)
        return dict(name=mode,status='PASS',oracle=checked,negative_controls=controls,
            verification_sha256=digest(output/(mode+'-verification.json')))

    try:
        assert all(digest(basis / name) == sha for name, sha in frozen.items())
        retained = json.loads((basis / 'result.json').read_text())
        assert all(retained[k] == spec[k] for k in ('source', 'owner', 'status'))
        assert not any(retained[k] for k in ('cleanup_errors', 'unrelated_changed', 'missing_owned'))
        containers = json.loads(lab.docker('inspect', *lab.docker('ps', '-aq').stdout.split()).stdout)
        old, = [c for c in containers if c['Id'] == retained['client']['id']]
        assert old['Config']['Labels'][retained['label']] == spec['owner']
        assert not old['State']['Running'] and not old['State']['Pid'] and not old['State']['OOMKilled']
        assert not any(c['State']['Running'] and any(str(basis) in m.get('Source', '') for m in c['Mounts']) for c in containers)
        absent = observe_backend_absence(pg, spec['database'], output, 'source-lock-original')
        original = rows(spec['database'])
        assert original == json.loads((basis / spec['sql_file']).read_text())
        seed = dict(name='off', control=original,
            native=json.loads((basis / (spec['prefix'] + '-after-native.json')).read_text()),
            inputs=inputs_at(store), contents=recovery.contents_at(store))
        recovery.known(seed, 'off')
        original_source = dict(seed=seed, files=census(store), frozen=frozen, backend_absence=absent,
            catalog=recovery.previous_catalog(pg, spec['database']), client_id=old['Id'],
            client_state=old['State'], label=retained['label'])
        lab.write_json(output / 'activated-source-lock-original.json', original_source)
        for mode in CASES:
            database = clone(mode, spec['database'])
            assert rows(database) == seed['control']
            directory = output / mode
            directory.mkdir(mode=0o700)
            shutil.copytree(store, directory / 'store')
            assert state(mode + '-seed', database, directory) == {k:seed[k] for k in ('control','native','inputs','contents')}
            config = configuration(pg, client, output, database, directory, seed['control'])
            root = Path(__file__).resolve().parents[2]
            setup = dict(recovery_upgrade=recovery.upgrade(pg, client, output, database, config, mode, rows),
                policy=recovery.advance_policy(pg, database, output, mode, rows),
                migration_sha256=digest(root / 'migrations/postgres/0033_activated_initial_recovery.sql'),
                correction_sha256=digest(root / 'migrations/postgres/0034_activated_control_retirement.sql'),
                source_migration_sha256=digest(root / 'migrations/postgres/0035_activated_source_closure.sql'),
                source_upgrade=source.upgrade(pg, client, output, database, config, mode, rows),
                grant=source.grant_source(pg, database, output, mode), operator=None)
            checked=execute(mode,database,config,directory,seed,setup)
            cases.append(checked)
            lab.write_json(output/'activated-source-closure-lock-cases.json',cases)
    finally:
        if original_source is not None:
            old, = json.loads(lab.docker('inspect', original_source['client_id']).stdout)
            preserved = dict(control=rows(spec['database']) == original_source['seed']['control'],
                files=census(store) == original_source['files'], inputs=inputs_at(store) == original_source['seed']['inputs'],
                contents=recovery.contents_at(store) == original_source['seed']['contents'],
                catalog=recovery.previous_catalog(pg, spec['database']) == original_source['catalog'],
                client=old['State'] == original_source['client_state'] and old['Config']['Labels'][original_source['label']] == spec['owner'],
                artifacts=all(digest(basis / name) == sha for name, sha in frozen.items()))
            lab.write_json(output / 'activated-source-closure-lock-source-preserved.json', preserved)
            assert all(preserved.values()), preserved
    assert [c['name'] for c in cases]==list(CASES)
    return dict(name='installed-activated-source-closure-locks',status='PASS',cli_invocations=2,
        cases=cases,source_preserved=preserved,helper_sha256=digest(helper),
        scope='actual installed source closure versus privileged guarded relational job in both lock orders',
        not_run=['authenticated job acceptance','driver-result or same-session poison telemetry',
            'workflow retirement','Archive restart and full M1'])
