"""Actual-preparer SQL guard probes and typed live deadline synchronization.

Only a new retired-seed clone is used. An invoker trigger runs the finite SQL
cases inside the retained Control backend's real refresh. Every test mutation,
including the positive receipt, is rolled back in a subtransaction. Product
guards remain enabled; no raw client or synthetic Control identity is exposed.
"""
import copy
import hashlib
import json
import shutil
import subprocess
import time

import lab
import initial_live as live
import initial_succession as preparing
from initial_empty_locks import Probe, literal
from initial_gap import configuration
from initial_history import capture, inputs_at, observe_backend_absence, server_clock

CASES = ('successor-live-guards',)
GUARD_CASES = (
    'activation-positive', 'request-immediate-before', 'request-immediate-after',
    'binding-immediate-after', 'checkpoint-zero', 'target-owner-lease',
    'target-role-retire', 'target-domain-close', 'target-source-close',
    'target-producer-close', 'control-expire', 'control-release',
    'control-role-retire', 'policy-head-advance',
)
ADMISSION = 'target admission requires a previously committed activation'
CHECKPOINT = 'target checkpoint requires a previously committed activation'
TARGET = 'activation authority and target must remain unchanged through its commit'
TRIGGER = 'live_guard_fixture_refresh'


def setup_sql(control, event):
    """Static SQL envelopes: refusal by the selected guard, not native intake."""
    assert control['role'] == 'control' and control['state'] == 'active' and control['epoch'] == 8
    assert len(event) > 0
    names = ','.join("'" + n + "'" for n in GUARD_CASES)
    script = r"""
BEGIN;
SET LOCAL search_path=pg_catalog;
SET LOCAL lock_timeout='5s';
SET LOCAL statement_timeout='5s';
CREATE SCHEMA live_guard_fixture;
CREATE TABLE live_guard_fixture.run (
    singleton boolean PRIMARY KEY CHECK (singleton), status text NOT NULL, executions integer NOT NULL);
INSERT INTO live_guard_fixture.run VALUES (true,'armed',0);
CREATE TABLE live_guard_fixture.cases (
    case_index integer PRIMARY KEY, name text UNIQUE NOT NULL, status text NOT NULL,
    sqlstate text, message text, context text, backend_pid integer NOT NULL,
    backend_start timestamptz NOT NULL, database_name text NOT NULL, database_user text NOT NULL,
    replication_role text NOT NULL, control_epoch numeric NOT NULL, transaction_id text NOT NULL,
    observed_at_us bigint NOT NULL, detail jsonb NOT NULL);
GRANT USAGE ON SCHEMA live_guard_fixture TO bigbrotr_control;
GRANT SELECT,UPDATE ON live_guard_fixture.run TO bigbrotr_control;
GRANT SELECT,INSERT ON live_guard_fixture.cases TO bigbrotr_control;
CREATE FUNCTION live_guard_fixture.exercise() RETURNS trigger LANGUAGE plpgsql
SECURITY INVOKER SET search_path=pg_catalog AS $fixture$
DECLARE
    s control.initial_ingest_successions%ROWTYPE;
    c control.owner_epochs%ROWTYPE;
    dictionary_request control.producer_requests%ROWTYPE;
    label text;
    n integer := 0;
    code text; message text; context text;
    wanted_message text; wanted_function text;
    detail jsonb;
    receipt jsonb;
    remaining bigint;
    now_us bigint;
    hit integer;
BEGIN
    -- The helper itself is single-use. Nested mutations still execute every
    -- product trigger, but do not recursively enter this qualification hook.
    IF pg_trigger_depth() <> 1 THEN RETURN NEW; END IF;
    UPDATE live_guard_fixture.run SET status='running',executions=executions+1
        WHERE singleton AND status='armed' AND executions=0;
    GET DIAGNOSTICS hit=ROW_COUNT;
    IF hit<>1 THEN RAISE EXCEPTION 'live guard fixture was not armed exactly once'; END IF;
    SELECT * INTO STRICT c FROM control.owner_epochs
        WHERE resource_id=NEW.resource_id AND epoch=NEW.epoch;
    IF current_user<>'bigbrotr_control' OR session_user<>'bigbrotr_control'
        OR current_setting('session_replication_role')<>'origin'
        OR current_setting('transaction_isolation')<>'read committed'
        OR c.role<>'control' OR c.state<>'active' OR c.epoch<>8
        OR c.control_backend_pid<>pg_backend_pid()
        OR c.control_backend_start IS DISTINCT FROM
            (SELECT backend_start FROM pg_stat_activity WHERE pid=pg_backend_pid()) THEN
        RAISE EXCEPTION 'live guard fixture lacks actual preparing backend';
    END IF;
    SELECT * INTO STRICT s FROM control.initial_ingest_successions
        WHERE prepared_by_resource=c.resource_id AND prepared_by_epoch=c.epoch
            AND prepared_by_incarnation=c.process_incarnation;
    FOREACH label IN ARRAY ARRAY[__NAMES__] LOOP
        n:=n+1; code:=NULL; message:=NULL; context:=NULL; receipt:=NULL; remaining:=NULL;
        wanted_message:=CASE WHEN n IN (2,3,4) THEN '__ADMISSION__'
            WHEN n=5 THEN '__CHECKPOINT__' ELSE '__TARGET__' END;
        wanted_function:=CASE WHEN n IN (2,3,4) THEN 'veto_unactivated_producer()'
            WHEN n=5 THEN 'guard_initial_activation_checkpoint()'
            ELSE 'guard_initial_activation_target()' END;
        now_us:=(EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint;
        IF c.lease_expires_at_us<=now_us OR NOT EXISTS (
            SELECT 1 FROM control.owner_epochs WHERE resource_id=s.owner_resource_id
                AND epoch=2 AND lease_expires_at_us>now_us) THEN
            RAISE EXCEPTION 'live guard fixture scheduling expired before case';
        END IF;
        BEGIN
            SET CONSTRAINTS control.initial_activation_complete DEFERRED;
            IF n=14 THEN
                -- Guarded, disposable copied revisions make the head advance
                -- relationally valid. This is not product policy activation.
                INSERT INTO control.config_revisions(revision,dataset_id,previous_revision,schema_version,
                    authority_mode,document,document_digest,accepted_source_digest,accepted_at_us,actor_id)
                    SELECT 2,dataset_id,1,schema_version,authority_mode,document,document_digest,
                        accepted_source_digest,accepted_at_us+1,actor_id
                    FROM control.config_revisions WHERE dataset_id=s.dataset_id AND revision=1;
                INSERT INTO control.policy_revisions(revision,dataset_id,previous_revision,config_revision,
                    schema_version,document,document_digest,accepted_at_us,actor_id)
                    SELECT 2,dataset_id,1,2,schema_version,document,document_digest,accepted_at_us+1,actor_id
                    FROM control.policy_revisions WHERE dataset_id=s.dataset_id AND revision=1;
            END IF;
            IF n=2 THEN SET CONSTRAINTS control.initial_activation_complete IMMEDIATE; END IF;
            INSERT INTO control.initial_ingest_activations(dataset_id,owner_resource_id,target_epoch,target_incarnation,
                target_role_instance_id,producer_slot_id,producer_epoch,previous_manifest_sha256,target_manifest_sha256,
                handoff_sha256,prepared_by_resource,prepared_by_epoch,prepared_by_incarnation,activated_by_resource,
                activated_by_epoch,activated_by_incarnation,config_revision,policy_revision,config_digest,policy_digest,
                activated_at_us,activation_transaction)
                VALUES(s.dataset_id,s.owner_resource_id,s.target_epoch,s.target_incarnation,s.target_role_instance_id,
                    s.producer_slot_id,s.producer_epoch,s.previous_manifest_sha256,s.target_manifest_sha256,s.handoff_sha256,
                    s.prepared_by_resource,s.prepared_by_epoch,s.prepared_by_incarnation,c.resource_id,c.epoch,c.process_incarnation,
                    s.config_revision,s.policy_revision,s.config_digest,s.policy_digest,0,'1'::xid8);
            SELECT to_jsonb(a)||jsonb_build_object('xmin',xmin::text) INTO STRICT receipt
                FROM control.initial_ingest_activations a WHERE owner_resource_id=s.owner_resource_id;
            IF receipt->>'activation_transaction'<>pg_current_xact_id()::text
                OR (receipt->>'activated_at_us')::bigint<now_us THEN
                RAISE EXCEPTION 'activation did not overwrite caller transaction/time';
            END IF;
            IF n<>2 THEN SET CONSTRAINTS control.initial_activation_complete IMMEDIATE; END IF;
            remaining:=control.require_active_initial_writer(s.owner_resource_id);
            IF remaining<=0 THEN RAISE EXCEPTION 'activation positive control lacks scheduling'; END IF;
            IF n=1 THEN
                RAISE EXCEPTION 'positive activation intentionally rolled back' USING ERRCODE='ZG001';
            ELSIF n IN (2,3) THEN
                -- Retain the original dictionary-only SQL envelope. Request4
                -- carries a retired acquisition job and is ineligible before
                -- this activation guard. No native request is dispatched here.
                SELECT * INTO STRICT dictionary_request FROM control.producer_requests
                    WHERE producer_slot_id=s.producer_slot_id AND producer_epoch=1 AND request_sequence=3
                        AND num_nonnulls(relay_endpoint_id,relay_access_scope_id,relay_enrollment_generation,
                            relay_route_id,acquisition_scope_id,acquisition_job_id)=0;
                INSERT INTO control.producer_requests
                    SELECT (jsonb_populate_record(NULL::control.producer_requests,to_jsonb(dictionary_request)||jsonb_build_object(
                        'producer_epoch',2,'request_sequence',1,'manifest_sha256',s.target_manifest_sha256,
                        'issuer_resource',c.resource_id,'issuer_epoch',c.epoch,'issuer_incarnation',c.process_incarnation,
                        'reserved_at_us',now_us))).*;
            ELSIF n=4 THEN
                -- The activation guard runs before the separate import/job
                -- bridge. This bounded envelope does not claim a running import.
                INSERT INTO control.producer_input_bindings(job_id,dataset_id,source_namespace,shard_id,
                    source_bytes,source_sha256,parser_profile,batch_first_record,batch_records,obligation_index,
                    obligation_kind,logical_record,byte_start,byte_end,record_sha256,producer_slot_id,producer_epoch,
                    request_sequence,request_digest,input_bytes,input_profile,manifest_sha256,issuer_resource,issuer_epoch,
                    issuer_incarnation)
                    SELECT (SELECT job_id FROM control.jobs ORDER BY job_id LIMIT 1),s.dataset_id,
                        sha256(convert_to('live-guard-source/1','UTF8')),s.shard_id,__EVENT_BYTES__,decode('__EVENT_SHA__','hex'),
                        1,1,1,0,1,1,0,__EVENT_BYTES__,decode('__EVENT_SHA__','hex'),s.producer_slot_id,2,1,
                        r.request_digest,r.input_bytes,r.input_profile,s.target_manifest_sha256,c.resource_id,c.epoch,
                        c.process_incarnation FROM control.producer_requests r WHERE producer_slot_id=s.producer_slot_id
                            AND producer_epoch=1 AND request_sequence=4;
            ELSIF n=5 THEN
                INSERT INTO control.producer_checkpoints VALUES(s.producer_slot_id,2,s.dataset_id,1,0,
                    sha256(convert_to('live-guard-checkpoint/1','UTF8')),now_us);
            ELSIF n=6 THEN
                UPDATE control.owner_epochs SET lease_expires_at_us=lease_expires_at_us+1
                    WHERE resource_id=s.owner_resource_id AND epoch=2;
            ELSIF n=7 THEN
                UPDATE control.role_instances SET retired_at_us=now_us WHERE role_instance_id=s.target_role_instance_id;
            ELSIF n=8 THEN
                UPDATE control.allocation_domains SET state='closed',closed_at_us=now_us,
                    closure_digest=sha256(convert_to('live-guard-domain/1','UTF8'))
                    WHERE dataset_id=s.dataset_id AND owner_resource_id=s.owner_resource_id AND owner_epoch=2
                        AND dictionary_kind=1;
            ELSIF n=9 THEN
                UPDATE control.source_streams SET state='closing'
                    WHERE dataset_id=s.dataset_id AND shard_id=s.shard_id AND owner_epoch=2;
            ELSIF n=10 THEN
                UPDATE control.producer_epochs SET state='closing' WHERE producer_slot_id=s.producer_slot_id AND producer_epoch=2;
            ELSIF n=11 THEN
                UPDATE control.owner_epochs SET lease_expires_at_us=acquired_at_us+1
                    WHERE resource_id=c.resource_id AND epoch=c.epoch;
            ELSIF n=12 THEN
                UPDATE control.owner_epochs SET state='released',released_at_us=now_us,exclusion_method='storage_fenced',
                    exclusion_digest=sha256(decode('62696762726f74722f636f6e74726f6c2d6578636c7573696f6e2f3100','hex')
                        ||c.dataset_id::bytea||c.resource_id::bytea||int8send(c.epoch::bigint)
                        ||c.process_incarnation::bytea||c.process_incarnation::bytea)
                    WHERE resource_id=c.resource_id AND epoch=c.epoch;
            ELSIF n=13 THEN
                UPDATE control.role_instances SET retired_at_us=now_us WHERE role_instance_id=c.role_instance_id;
            ELSIF n=14 THEN
                UPDATE control.datasets SET config_revision=2,policy_revision=2 WHERE dataset_id=s.dataset_id;
            END IF;
            RAISE EXCEPTION 'live guard mutation unexpectedly accepted' USING ERRCODE='ZG002';
        EXCEPTION WHEN OTHERS THEN
            GET STACKED DIAGNOSTICS code=RETURNED_SQLSTATE,message=MESSAGE_TEXT,context=PG_EXCEPTION_CONTEXT;
            IF n=1 AND code='ZG001' THEN
                code:=NULL; message:=NULL; context:=NULL;
            ELSIF n=1 OR code<>'23514' OR message<>wanted_message
                OR position(wanted_function in context)=0 THEN
                RAISE;
            END IF;
        END;
        -- SQL subtransaction rollback must have removed its receipt and all
        -- target obligations before another case can test the same prepared state.
        IF EXISTS(SELECT 1 FROM control.initial_ingest_activations WHERE owner_resource_id=s.owner_resource_id)
            OR EXISTS(SELECT 1 FROM control.producer_requests WHERE producer_slot_id=s.producer_slot_id AND producer_epoch=2)
            OR EXISTS(SELECT 1 FROM control.producer_input_bindings WHERE producer_slot_id=s.producer_slot_id AND producer_epoch=2)
            OR EXISTS(SELECT 1 FROM control.producer_checkpoints WHERE producer_slot_id=s.producer_slot_id AND producer_epoch=2) THEN
            RAISE EXCEPTION 'live guard subtransaction left target state';
        END IF;
        detail:=jsonb_build_object('receipt',receipt,'remaining_us',remaining,
            'constraint_order',CASE WHEN n=2 THEN 'before-receipt' ELSE 'after-receipt' END,
            'rolled_back',true,'expected_function',CASE WHEN n=1 THEN NULL ELSE wanted_function END);
        INSERT INTO live_guard_fixture.cases VALUES(n,label,'PASS',code,message,context,pg_backend_pid(),
            c.control_backend_start,current_database(),current_user,current_setting('session_replication_role'),
            c.epoch,pg_current_xact_id()::text,(EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint,detail);
    END LOOP;
    UPDATE live_guard_fixture.run SET status='complete' WHERE singleton AND status='running' AND executions=1;
    RETURN NEW;
END $fixture$;
REVOKE ALL ON FUNCTION live_guard_fixture.exercise() FROM PUBLIC;
GRANT EXECUTE ON FUNCTION live_guard_fixture.exercise() TO bigbrotr_control;
CREATE TRIGGER live_guard_fixture_refresh AFTER UPDATE OF lease_expires_at_us ON control.owner_epochs
    FOR EACH ROW WHEN (NEW.role='control' AND NEW.resource_id=__RESOURCE__ AND NEW.epoch=8
        AND NEW.process_incarnation=__INCARNATION__)
    EXECUTE FUNCTION live_guard_fixture.exercise();
COMMIT;
"""
    return (script.replace('__NAMES__', names).replace('__ADMISSION__', ADMISSION)
        .replace('__CHECKPOINT__', CHECKPOINT).replace('__TARGET__', TARGET)
        .replace('__EVENT_BYTES__', str(len(event))).replace('__EVENT_SHA__', hashlib.sha256(event).hexdigest())
        .replace('__RESOURCE__', literal(control['resource_id']))
        .replace('__INCARNATION__', literal(control['process_incarnation'])))


def wait_gate(probe, gate):
    deadline = time.monotonic() + 10
    while True:
        probe.observe()
        found = [e for e in probe.parsed if e.get('event') == 'live-guards-gate'
            and e.get('gate') == gate and e.get('phase') == 'waiting']
        if found:
            assert len(found) == 1
            return
        assert probe.process.poll() is None and time.monotonic() < deadline, 'live guard gate unobserved'
        with probe.condition:
            probe.condition.wait(.02)


def statement(pg, database, output, label, sql):
    (output / (label + '.sql')).write_text(sql)
    try:
        response = lab.docker('exec', '-i', pg['postgres'], 'psql', '-X', '-A', '-t', '-q',
            '--no-password', '-v', 'ON_ERROR_STOP=1', '-U', 'postgres', '-d', database,
            data=sql, check=False, timeout=8)
    except subprocess.TimeoutExpired as error:
        def partial(value):
            return value.decode(errors='replace') if isinstance(value, bytes) else value or ''
        lab.write_json(output / (label + '-response.json'), dict(returncode=None,
            outcome='UNKNOWN_TIMEOUT', stdout=partial(error.stdout), stderr=partial(error.stderr),
            cleanup='outer owner-checked cleanup; fixture DDL COMMIT outcome is not inferred'))
        raise
    lab.write_json(output / (label + '-response.json'), dict(returncode=response.returncode,
        stdout=response.stdout, stderr=response.stderr))
    assert response.returncode == 0 and not response.stderr, 'live guard fixture SQL did not complete'


def product_catalog(installed):
    expected = copy.deepcopy(installed)
    extra, = [t for t in expected['triggers'] if t['name'] == TRIGGER]
    assert extra == dict(table='owner_epochs', name=TRIGGER, enabled='O', internal=False,
        function='live_guard_fixture.exercise()')
    expected['triggers'].remove(extra)
    return expected


def check_cases(guards, database):
    control = guards['control']
    assert guards['run']['status'] == 'complete' and guards['run']['executions'] == 1
    assert guards['run']['singleton'] is True
    observed = guards['cases']
    assert len(observed) == 14
    outer, = {r['transaction_id'] for r in observed}
    assert outer.isdecimal() and int(outer) > 0
    current, = [r for r in guards['after_refresh']['owner_epochs']
        if r['resource_id'] == control['resource_id'] and r['epoch'] == control['epoch']]
    assert int(outer) % 2**32 == int(current['xmin'])
    for index, (name, row) in enumerate(zip(GUARD_CASES, observed, strict=True), 1):
        assert row['case_index'] == index and row['name'] == name and row['status'] == 'PASS'
        assert row['backend_pid'] == control['control_backend_pid']
        assert row['backend_start'] == control['control_backend_start']
        assert row['database_name'] == database and row['database_user'] == 'bigbrotr_control'
        assert row['replication_role'] == 'origin' and row['control_epoch'] == control['epoch'] == 8
        assert row['xmin'] == current['xmin']
        assert guards['install_after_us'] <= row['observed_at_us'] <= guards['remove_before_us']
        detail = row['detail']
        assert detail['rolled_back'] is True and type(detail['remaining_us']) is int and detail['remaining_us'] > 0
        assert detail['constraint_order'] == ('before-receipt' if index == 2 else 'after-receipt')
        assert detail['receipt']['activation_transaction'] == outer
        assert detail['receipt']['activated_at_us'] <= row['observed_at_us']
        if index == 1:
            assert row['sqlstate'] is row['message'] is row['context'] is detail['expected_function'] is None
        else:
            message = ADMISSION if index in (2, 3, 4) else CHECKPOINT if index == 5 else TARGET
            function = ('veto_unactivated_producer()' if index in (2, 3, 4)
                else 'guard_initial_activation_checkpoint()' if index == 5 else 'guard_initial_activation_target()')
            assert row['sqlstate'] == '23514' and row['message'] == message
            assert function in row['context'] and detail['expected_function'] == function


def exercise(pg, client, output, rows, database, config, directory, before, inputs, event):
    mode, = CASES
    probe = None
    guards = dict(status='INCOMPLETE', gates=[])
    window = dict(before_us=server_clock(pg, database))
    started = time.monotonic()
    prefix = output / (mode + '-gate')
    assert all(not prefix.with_suffix(s).exists() for s in ('.go', '.done'))
    def save():
        lab.write_json(output / (mode + '-guards.json'), guards)
        lab.write_json(output / (mode + '-clock.json'), window)
    def note(gate, phase):
        guards['gates'].append(dict(gate=gate, phase=phase, server_us=server_clock(pg, database)))
        save()
    try:
        save()
        probe = Probe(['docker', 'exec', client, '/build/ingest_bootstrap_probe', mode,
            database, '/run/secrets/application', config, '/lab/' + prefix.name])
        wait_gate(probe, 'install')
        note('install', 'waiting')
        prepared, = [e for e in probe.parsed if e.get('event') == 'successor-state' and e.get('stage') == 'prepared']
        guards['before'] = rows(database)
        assert guards['before'] == {t: json.loads(v) for t, v in prepared['state']['control_tables']}
        assert len(prepared['rows']) == 52 and prepared['state']['sequence'] == 62
        assert dict(prepared['state']['originals']) == inputs
        control, = [r for r in guards['before']['owner_epochs'] if r['role'] == 'control' and r['state'] == 'active']
        guards['control'] = control
        guards['catalog_before'] = live.catalog(pg, database)
        guards['install_sql'] = setup_sql(control, event)
        guards['install_sha256'] = hashlib.sha256(guards['install_sql'].encode()).hexdigest()
        guards['install_before_us'] = server_clock(pg, database)
        assert guards['install_before_us'] < control['lease_expires_at_us']
        save()
        statement(pg, database, output, mode + '-install', guards['install_sql'])
        guards['install_after_us'] = server_clock(pg, database)
        guards['catalog_installed'] = live.catalog(pg, database)
        assert product_catalog(guards['catalog_installed']) == guards['catalog_before']
        guards['fixture_function'] = json.loads(lab.sql(pg, database,
            "SELECT json_build_object('security_definer',prosecdef,'definition',pg_get_functiondef(oid)) "
            "FROM pg_proc WHERE oid='live_guard_fixture.exercise()'::regprocedure"))
        assert guards['fixture_function']['security_definer'] is False
        assert rows(database) == guards['before']
        save()
        note('install', 'released')
        with prefix.with_suffix('.go').open('x') as stream:
            stream.write('continue\n')
        wait_gate(probe, 'remove')
        note('remove', 'waiting')
        refreshed, = [e for e in probe.parsed if e.get('event') == 'successor-state' and e.get('stage') == 'guards-refreshed']
        guards['after_refresh'] = rows(database)
        assert guards['after_refresh'] == {t: json.loads(v) for t, v in refreshed['state']['control_tables']}
        assert refreshed['rows'] == prepared['rows'] and refreshed['state']['sequence'] == 62
        assert dict(refreshed['state']['originals']) == inputs
        guards['cases'] = json.loads(lab.sql(pg, database,
            "SELECT jsonb_agg(to_jsonb(c)||jsonb_build_object('xmin',xmin::text) ORDER BY case_index) "
            'FROM live_guard_fixture.cases c'))
        guards['run'] = json.loads(lab.sql(pg, database,
            "SELECT to_jsonb(r)||jsonb_build_object('xmin',xmin::text) FROM live_guard_fixture.run r WHERE singleton"))
        guards['remove_before_us'] = server_clock(pg, database)
        save()
        check_cases(guards, database)
        guards['remove_sql'] = ('BEGIN; SET LOCAL lock_timeout=\'5s\'; '
            'DROP TRIGGER live_guard_fixture_refresh ON control.owner_epochs; '
            'DROP FUNCTION live_guard_fixture.exercise(); COMMIT;')
        save()
        statement(pg, database, output, mode + '-remove', guards['remove_sql'])
        guards['remove_after_us'] = server_clock(pg, database)
        guards['catalog_removed'] = live.catalog(pg, database)
        guards['clean'] = rows(database)
        assert guards['catalog_removed'] == guards['catalog_before']
        assert guards['clean'] == guards['after_refresh']
        save()
        note('remove', 'released')
        with prefix.with_suffix('.done').open('x') as stream:
            stream.write('continue\n')
        code = probe.wait(30)
        probe.observe()
        assert code == 0 and not probe.stderr
        guards['status'] = 'CAPTURED'
    except BaseException as error:
        guards['failure'] = type(error).__name__
        raise
    finally:
        code = None if probe is None else probe.process.poll()
        if probe is not None and code is not None:
            for thread in probe.threads:
                thread.join(1)
        lab.write_json(output / (mode + '-response.json'), dict(returncode=code,
            stdout='' if probe is None else bytes(probe.stdout).decode(errors='replace'),
            stderr='' if probe is None else bytes(probe.stderr).decode(errors='replace'),
            seconds=time.monotonic() - started,
            outcome='UNKNOWN_PROCESS_OUTCOME' if code is None else 'PROCESS_EXITED'))
        try:
            if code is None:
                lab.write_json(output / (mode + '-external-unavailable.json'), dict(
                    status='NOT_RUN_UNKNOWN_PROCESS_OUTCOME', reason='outer owner-checked cleanup must complete first'))
            else:
                window['after_us'] = server_clock(pg, database)
                after = rows(database)
                lab.write_json(output / (mode + '-after.json'), after)
                final_inputs = inputs_at(directory / 'store')
                lab.write_json(output / (mode + '-inputs-after.json'), final_inputs)
                absence = observe_backend_absence(pg, database, output, mode)
                final_native = dump(client, output, mode + '-after', directory)
                exact = live.exact_absence(pg, database, output, mode, after, before)
        finally:
            errors = []
            if probe is not None:
                guards['forced_wrapper_stop'] = probe.process.poll() is None
                try:
                    probe.stop()
                except BaseException as error:
                    errors.append(type(error).__name__)
            guards['cleanup_errors'] = errors
            save()
            assert not errors, errors
    return probe.parsed, after, final_native, final_inputs, window, guards, absence, exact


def dump(client, output, label, directory):
    response, _ = capture(client, output, label, 'dump-state', '/lab/' + directory.name + '/store')
    assert response.returncode == 0 and not response.stderr
    observed = json.loads(response.stdout)
    lab.write_json(output / (label + '-native.json'), observed)
    assert observed['event'] == 'dump-state' and type(observed['sequence']) is int
    return observed


def run(pg, client, output, rows, clone):
    # Import the separate reducer here so setup generation can be reviewed and
    # parsed before that independent file is integrated. Execution requires it.
    from initial_live_guards_oracle import verify, negative_controls

    mode, = CASES
    basis = output.parent.parent / preparing.SEED / 'bindings'
    store = basis / 'retire_signed/store'
    frozen = dict(preparing.FROZEN,
        **{'signed-seed/event.jsonl': 'c30658a75b2b913e509b7618f36ebae75fa1d5680dc8fe4e322aae638d8affae'})
    assert all(preparing.digest(basis / name) == value for name, value in frozen.items())
    retained = json.loads((basis / 'result.json').read_text())
    assert retained['status'] == 'PASS_INSTALLED_INITIAL_RETIREMENT_FIRST_WAVE'
    assert retained['source'] == preparing.SOURCE and retained['owner'] == preparing.OWNER
    assert not retained['cleanup_errors'] and not retained['unrelated_changed']
    containers = json.loads(lab.docker('inspect', *lab.docker('ps', '-aq').stdout.split()).stdout)
    old, = [c for c in containers if c['Id'] == retained['client']['id']]
    assert old['Config']['Labels'][retained['label']] == preparing.OWNER
    assert not old['State']['Running'] and not old['State']['Pid'] and not old['State']['OOMKilled']
    assert not any(c['State']['Running'] and any(str(basis) in m.get('Source', '')
        for m in c['Mounts']) for c in containers)
    original = rows(preparing.DATABASE)
    assert original == json.loads((basis / 'retire-signed-retry-after.json').read_text())
    baseline = json.loads((basis / 'retire-signed-retry-native.json').read_text())
    files, inputs = preparing.census(store), inputs_at(store)
    assert len(original) == 70 and len(baseline['rows']) == 49 and len(inputs) == 4
    assert sum(v['bytes'] for v in files.values()) < 1024**2
    seed = dict(control=original, native=baseline, inputs=inputs, sequence=58)
    absent = observe_backend_absence(pg, preparing.DATABASE, output, 'live-guards-source')
    lab.write_json(output / 'live-guards-source.json', dict(**seed, files=files,
        frozen=frozen, backend_absence=absent, client_state=old['State']))
    try:
        database = clone(mode, preparing.DATABASE)
        assert rows(database) == original
        directory = output / mode
        directory.mkdir(mode=0o700)
        shutil.copytree(store, directory / 'store')
        observe_backend_absence(pg, database, output, mode + '-before')
        assert dump(client, output, mode + '-before', directory) == dict(
            event='dump-state', rows=baseline['rows'], sequence=58)
        assert inputs_at(directory / 'store') == inputs
        config = configuration(pg, client, output, database, directory, original)
        catalog = live.upgrade(pg, client, output, database, config, mode, rows)
        before = rows(database)
        lab.write_json(output / (mode + '-before.json'), before)
        lab.write_json(output / (mode + '-inputs-before.json'), inputs)
        events, after, native, final_inputs, window, guards, absent, exact = exercise(
            pg, client, output, rows, database, config, directory, before,
            inputs, (basis / 'signed-seed/event.jsonl').read_bytes())
        assert guards['status'] == 'CAPTURED' and not guards['cleanup_errors'] and not guards['forced_wrapper_stop']
        assert guards['catalog_before'] == guards['catalog_removed'] == catalog == live.catalog(pg, database)
        backend_events = [e for e in events if e.get('event') == 'successor-backend']
        assert backend_events == [dict(event='successor-backend', stage='initial',
            **exact['expected'], present=p) for p in (True, False)]
        final, = [e for e in events if e.get('event') == 'successor-state' and e.get('stage') == 'final']
        assert after == {t: json.loads(v) for t, v in final['state']['control_tables']}
        assert native == dict(event='dump-state', rows=final['rows'], sequence=final['state']['sequence'])
        assert final_inputs == dict(final['state']['originals'])
        assert all(final_inputs[n] == identity for n, identity in inputs.items())
        context = dict(events=events, original=before, native=baseline, inputs=inputs,
            mode=mode, seed=seed, server_window=window, guards=guards)
        lab.write_json(output / (mode + '-verification.json'), context)
        evidence = verify(**context)
        mutations = negative_controls(**context)
        case = dict(name=mode, status='PASS', oracle=evidence, negative_controls=mutations,
            raw_cases=len(guards['cases']), backend_absence=absent, exact_backend=exact,
            catalog_unchanged=True, external_final_matches=True,
            verification_sha256=preparing.digest(output / (mode + '-verification.json')))
        lab.write_json(output / 'live-guards-cases.json', [case])
    finally:
        preserved = dict(control=rows(preparing.DATABASE) == original,
            files=preparing.census(store) == files, inputs=inputs_at(store) == inputs,
            artifacts=all(preparing.digest(basis / n) == h for n, h in frozen.items()))
        lab.write_json(output / 'live-guards-source-preserved.json', preserved)
        assert all(preserved.values()), preserved
    return dict(name='live-initial-guards', status='PASS', cases=[case],
        source_lab=str(basis.parent), source_revision=preparing.SOURCE, source_owner=preparing.OWNER,
        source_preserved=preserved, source_artifacts=frozen,
        scope='actual preparing backend SQL mutation fences, typed live scheduling synchronization and admission',
        not_run=['first-manifest mismatch after committed activation', 'activated-zero old recovery routes',
            'arbitrary raw-SQL physical COMMIT-time clock fencing', 'network acquisition',
            'installed CLI live writer', 'activated writer replacement', 'ACK/GC', 'full M1'])
