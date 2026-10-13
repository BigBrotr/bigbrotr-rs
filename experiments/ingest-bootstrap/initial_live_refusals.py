"""Finite manifest and renewal refusals under a continuously held live writer.

Only isolated retired-seed copies are opened. The manifest fixture runs in the
actual activating Control backend, with all product guards enabled. Its input
binding is an early-guard envelope, not an import job or native dispatch. The
outer runner owns engines and unknown remote-process cleanup.
"""
import copy
from datetime import datetime, timezone
import hashlib
import json
import re
import shutil
import time

import lab
import initial_live as live
import initial_succession as preparing
from initial_empty_locks import Probe, literal
from initial_gap import configuration
from initial_history import inputs_at, observe_backend_absence, server_clock
from initial_live_guards import dump, statement

CASES = ('successor-live-manifest', 'successor-live-renew-control',
    'successor-live-renew-expired', 'successor-live-renew-before', 'successor-live-renew-after')
MANIFEST_CASES = ('producer-request-wrong-manifest', 'input-binding-wrong-manifest')
MANIFEST_ERROR = 'request requires its activated target manifest and actual activating Control'
EXPIRY_ERROR = 'active initial writer scheduling authority expired'
TRIGGER = 'live_manifest_fixture_refresh'


def setup_sql(control, event):
    assert control['role'] == 'control' and control['state'] == 'active' and control['epoch'] == 8
    assert event
    script = r"""
BEGIN;
SET LOCAL search_path=pg_catalog;
SET LOCAL lock_timeout='5s';
SET LOCAL statement_timeout='5s';
CREATE SCHEMA live_manifest_fixture;
CREATE TABLE live_manifest_fixture.run (
    singleton boolean PRIMARY KEY CHECK(singleton),status text NOT NULL,executions integer NOT NULL);
INSERT INTO live_manifest_fixture.run VALUES(true,'armed',0);
CREATE TABLE live_manifest_fixture.cases (
    case_index integer PRIMARY KEY,name text UNIQUE NOT NULL,status text NOT NULL,
    sqlstate text,message text,context text,backend_pid integer NOT NULL,
    backend_start timestamptz NOT NULL,database_name text NOT NULL,database_user text NOT NULL,
    replication_role text NOT NULL,control_epoch numeric NOT NULL,transaction_id text NOT NULL,
    observed_at_us bigint NOT NULL,detail jsonb NOT NULL);
GRANT USAGE ON SCHEMA live_manifest_fixture TO bigbrotr_control;
GRANT SELECT,UPDATE ON live_manifest_fixture.run TO bigbrotr_control;
GRANT SELECT,INSERT ON live_manifest_fixture.cases TO bigbrotr_control;
CREATE FUNCTION live_manifest_fixture.exercise() RETURNS trigger LANGUAGE plpgsql
SECURITY INVOKER SET search_path=pg_catalog AS $fixture$
DECLARE
    c control.owner_epochs%ROWTYPE;
    s control.initial_ingest_successions%ROWTYPE;
    a control.initial_ingest_activations%ROWTYPE;
    r control.producer_requests%ROWTYPE;
    job bytea;
    receipt jsonb; attempted jsonb;
    label text; code text; message text; context text;
    n integer:=0; hit integer; remaining bigint; now_us bigint;
BEGIN
    IF pg_trigger_depth()<>1 THEN RETURN NEW; END IF;
    UPDATE live_manifest_fixture.run SET status='running',executions=executions+1
        WHERE singleton AND status='armed' AND executions=0;
    GET DIAGNOSTICS hit=ROW_COUNT;
    IF hit<>1 THEN RAISE EXCEPTION 'manifest fixture not armed exactly once'; END IF;
    SELECT * INTO STRICT c FROM control.owner_epochs WHERE resource_id=NEW.resource_id AND epoch=NEW.epoch;
    IF current_user<>'bigbrotr_control' OR session_user<>'bigbrotr_control'
        OR current_setting('session_replication_role')<>'origin'
        OR current_setting('transaction_isolation')<>'read committed'
        OR c.role<>'control' OR c.state<>'active' OR c.epoch<>8
        OR c.control_backend_pid<>pg_backend_pid()
        OR c.control_backend_start IS DISTINCT FROM
            (SELECT backend_start FROM pg_stat_activity WHERE pid=pg_backend_pid()) THEN
        RAISE EXCEPTION 'manifest fixture lacks actual activating backend';
    END IF;
    SELECT * INTO STRICT s FROM control.initial_ingest_successions
        WHERE prepared_by_resource=c.resource_id AND prepared_by_epoch=c.epoch
            AND prepared_by_incarnation=c.process_incarnation;
    SELECT * INTO STRICT a FROM control.initial_ingest_activations WHERE owner_resource_id=s.owner_resource_id;
    SELECT to_jsonb(x)||jsonb_build_object('xmin',xmin::text) INTO STRICT receipt
        FROM control.initial_ingest_activations x WHERE owner_resource_id=s.owner_resource_id;
    IF a.activation_transaction=pg_current_xact_id()
        OR a.previous_manifest_sha256<>s.previous_manifest_sha256
        OR a.target_manifest_sha256<>s.target_manifest_sha256
        OR a.previous_manifest_sha256=a.target_manifest_sha256
        OR ROW(a.activated_by_resource,a.activated_by_epoch,a.activated_by_incarnation)
            IS DISTINCT FROM ROW(c.resource_id,c.epoch,c.process_incarnation) THEN
        RAISE EXCEPTION 'manifest fixture requires previously committed exact activation';
    END IF;
    FOREACH label IN ARRAY ARRAY[__NAMES__] LOOP
        n:=n+1;
        remaining:=control.require_active_initial_writer(s.owner_resource_id);
        now_us:=(EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint;
        IF remaining<=0 THEN RAISE EXCEPTION 'manifest fixture scheduling unavailable'; END IF;
        IF n=1 THEN
            -- Request3 has no source/job envelope; every earlier source guard
            -- therefore passes without consulting the retired acquisition job.
            SELECT * INTO STRICT r FROM control.producer_requests
                WHERE producer_slot_id=s.producer_slot_id AND producer_epoch=1 AND request_sequence=3
                    AND num_nonnulls(relay_endpoint_id,relay_access_scope_id,relay_enrollment_generation,
                        relay_route_id,acquisition_scope_id,acquisition_job_id)=0;
            attempted:=to_jsonb(r)||jsonb_build_object('producer_epoch',2,'request_sequence',1,
                'manifest_sha256',s.previous_manifest_sha256,'issuer_resource',c.resource_id,
                'issuer_epoch',c.epoch,'issuer_incarnation',c.process_incarnation,'reserved_at_us',now_us);
        ELSE
            SELECT * INTO STRICT r FROM control.producer_requests
                WHERE producer_slot_id=s.producer_slot_id AND producer_epoch=1 AND request_sequence=4;
            SELECT job_id INTO STRICT job FROM control.jobs
                WHERE dataset_id=s.dataset_id AND job_id=r.acquisition_job_id;
            attempted:=jsonb_build_object('job_id',job,'dataset_id',s.dataset_id,
                'source_namespace',sha256(convert_to('live-manifest-source/1','UTF8')),'shard_id',s.shard_id,
                'source_bytes',__EVENT_BYTES__,'source_sha256',decode('__EVENT_SHA__','hex'),
                'parser_profile',1,'batch_first_record',1,'batch_records',1,'obligation_index',0,
                'obligation_kind',1,'logical_record',1,'byte_start',0,'byte_end',__EVENT_BYTES__,
                'record_sha256',decode('__EVENT_SHA__','hex'),'producer_slot_id',s.producer_slot_id,
                'producer_epoch',2,'request_sequence',1,'request_digest',r.request_digest,
                'input_bytes',r.input_bytes,'input_profile',r.input_profile,
                'manifest_sha256',s.previous_manifest_sha256,'issuer_resource',c.resource_id,
                'issuer_epoch',c.epoch,'issuer_incarnation',c.process_incarnation);
        END IF;
        BEGIN
            IF n=1 THEN
                INSERT INTO control.producer_requests
                    SELECT (jsonb_populate_record(NULL::control.producer_requests,attempted)).*;
            ELSE
                -- Activation guard precedes the separate import/job bridge.
                -- This envelope does not claim to be an eligible running import.
                INSERT INTO control.producer_input_bindings
                    SELECT (jsonb_populate_record(NULL::control.producer_input_bindings,attempted)).*;
            END IF;
            RAISE EXCEPTION 'wrong manifest unexpectedly accepted' USING ERRCODE='ZM001';
        EXCEPTION WHEN OTHERS THEN
            GET STACKED DIAGNOSTICS code=RETURNED_SQLSTATE,message=MESSAGE_TEXT,context=PG_EXCEPTION_CONTEXT;
            IF code<>'23514' OR message<>'__ERROR__'
                OR position('veto_unactivated_producer()' in context)=0 THEN RAISE; END IF;
        END;
        IF EXISTS(SELECT 1 FROM control.producer_requests WHERE producer_slot_id=s.producer_slot_id AND producer_epoch=2)
            OR EXISTS(SELECT 1 FROM control.producer_input_bindings WHERE producer_slot_id=s.producer_slot_id AND producer_epoch=2)
            OR NOT EXISTS(SELECT 1 FROM control.producer_epochs WHERE producer_slot_id=s.producer_slot_id
                AND producer_epoch=2 AND reserved_through_request=0)
            OR receipt IS DISTINCT FROM (SELECT to_jsonb(x)||jsonb_build_object('xmin',xmin::text)
                FROM control.initial_ingest_activations x WHERE owner_resource_id=s.owner_resource_id) THEN
            RAISE EXCEPTION 'manifest subtransaction changed retained target state';
        END IF;
        INSERT INTO live_manifest_fixture.cases VALUES(n,label,'PASS',code,message,context,pg_backend_pid(),
            c.control_backend_start,current_database(),current_user,current_setting('session_replication_role'),
            c.epoch,pg_current_xact_id()::text,(EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint,
            jsonb_build_object('receipt',receipt,'attempted',attempted,'remaining_us',remaining,
                'attempted_at_us',now_us,'rolled_back',true,'expected_function','veto_unactivated_producer()'));
    END LOOP;
    UPDATE live_manifest_fixture.run SET status='complete' WHERE singleton AND status='running' AND executions=1;
    RETURN NEW;
END $fixture$;
REVOKE ALL ON FUNCTION live_manifest_fixture.exercise() FROM PUBLIC;
GRANT EXECUTE ON FUNCTION live_manifest_fixture.exercise() TO bigbrotr_control;
CREATE TRIGGER live_manifest_fixture_refresh AFTER UPDATE OF lease_expires_at_us ON control.owner_epochs
    FOR EACH ROW WHEN(NEW.role='control' AND NEW.resource_id=__RESOURCE__ AND NEW.epoch=8
        AND NEW.process_incarnation=__INCARNATION__)
    EXECUTE FUNCTION live_manifest_fixture.exercise();
COMMIT;
"""
    return (script.replace('__NAMES__', ','.join("'" + name + "'" for name in MANIFEST_CASES))
        .replace('__ERROR__', MANIFEST_ERROR).replace('__RESOURCE__', literal(control['resource_id']))
        .replace('__INCARNATION__', literal(control['process_incarnation']))
        .replace('__EVENT_BYTES__', str(len(event))).replace('__EVENT_SHA__', hashlib.sha256(event).hexdigest()))


def product_catalog(installed):
    expected = copy.deepcopy(installed)
    extra, = [t for t in expected['triggers'] if t['name'] == TRIGGER]
    assert extra == dict(table='owner_epochs', name=TRIGGER, enabled='O', internal=False,
        function='live_manifest_fixture.exercise()')
    expected['triggers'].remove(extra)
    return expected


def wait_gate(probe, gate):
    deadline = time.monotonic() + 10
    while True:
        probe.observe()
        found = [e for e in probe.parsed if e.get('event') == 'live-guards-gate'
            and e.get('gate') == gate and e.get('phase') == 'waiting']
        if found:
            assert len(found) == 1
            return
        assert probe.process.poll() is None and time.monotonic() < deadline, 'manifest gate unobserved'
        with probe.condition:
            probe.condition.wait(.02)


def manifest_invoke(pg, client, output, rows, database, config, event, inputs, fixture):
    mode = CASES[0]
    prefix = output / (mode + '-gate')
    assert all(not prefix.with_suffix(s).exists() for s in ('.go', '.done'))
    started = time.monotonic()
    probe = None
    guards = dict(status='INCOMPLETE', gates=[])
    fixture['manifest'] = guards
    def save():
        lab.write_json(output / (mode + '-fixture.json'), fixture)
    def note(gate, phase):
        guards['gates'].append(dict(gate=gate, phase=phase, server_us=server_clock(pg, database)))
        save()
    try:
        save()
        probe = Probe(['docker', 'exec', client, '/build/ingest_bootstrap_probe', mode,
            database, '/run/secrets/application', config, '/lab/' + prefix.name])
        wait_gate(probe, 'manifest-install')
        note('manifest-install', 'waiting')
        activated, = [e for e in probe.parsed if e.get('event') == 'successor-state'
            and e.get('stage') == 'activated-zero']
        guards['before'] = rows(database)
        assert guards['before'] == {t: json.loads(v) for t, v in activated['state']['control_tables']}
        assert len(activated['rows']) == 52 and activated['state']['sequence'] == 62
        assert dict(activated['state']['originals']) == inputs
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
            "FROM pg_proc WHERE oid='live_manifest_fixture.exercise()'::regprocedure"))
        assert guards['fixture_function']['security_definer'] is False
        assert rows(database) == guards['before']
        note('manifest-install', 'released')
        with prefix.with_suffix('.go').open('x') as stream:
            stream.write('continue\n')
        wait_gate(probe, 'manifest-remove')
        note('manifest-remove', 'waiting')
        refreshed, = [e for e in probe.parsed if e.get('event') == 'successor-state'
            and e.get('stage') == 'manifest-refreshed']
        guards['after_refresh'] = rows(database)
        assert guards['after_refresh'] == {t: json.loads(v) for t, v in refreshed['state']['control_tables']}
        assert refreshed['rows'] == activated['rows'] and refreshed['state']['sequence'] == 62
        assert dict(refreshed['state']['originals']) == inputs
        guards['cases'] = json.loads(lab.sql(pg, database,
            "SELECT jsonb_agg(to_jsonb(c)||jsonb_build_object('xmin',xmin::text) ORDER BY case_index) "
            'FROM live_manifest_fixture.cases c'))
        guards['run'] = json.loads(lab.sql(pg, database,
            "SELECT to_jsonb(r)||jsonb_build_object('xmin',xmin::text) FROM live_manifest_fixture.run r WHERE singleton"))
        guards['remove_before_us'] = server_clock(pg, database)
        save()
        assert guards['run']['status'] == 'complete' and guards['run']['executions'] == 1
        assert len(guards['cases']) == 2
        for index, (name, case) in enumerate(zip(MANIFEST_CASES, guards['cases'], strict=True), 1):
            assert case['case_index'] == index and case['name'] == name and case['status'] == 'PASS'
            assert case['sqlstate'] == '23514' and case['message'] == MANIFEST_ERROR
            assert 'veto_unactivated_producer()' in case['context']
            assert case['backend_pid'] == control['control_backend_pid']
            assert case['backend_start'] == control['control_backend_start']
            assert case['database_name'] == database and case['database_user'] == 'bigbrotr_control'
            assert case['control_epoch'] == 8 and case['replication_role'] == 'origin'
        guards['remove_sql'] = ("BEGIN; SET LOCAL lock_timeout='5s'; "
            'DROP TRIGGER live_manifest_fixture_refresh ON control.owner_epochs; '
            'DROP FUNCTION live_manifest_fixture.exercise(); COMMIT;')
        save()
        statement(pg, database, output, mode + '-remove', guards['remove_sql'])
        guards['remove_after_us'] = server_clock(pg, database)
        guards['catalog_removed'] = live.catalog(pg, database)
        guards['clean'] = rows(database)
        assert guards['catalog_removed'] == guards['catalog_before']
        assert guards['clean'] == guards['after_refresh']
        note('manifest-remove', 'released')
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


def exact_backends(pg, database, output, mode, before, after):
    """Independently query all newly claimed actual Control incarnations."""
    assert re.fullmatch('[a-z][a-z0-9_]+', database)
    prior = {(r['resource_id'], r['epoch']) for r in before['owner_epochs'] if r['role'] == 'control'}
    current = sorted((r for r in after['owner_epochs'] if r['role'] == 'control'
        and (r['resource_id'], r['epoch']) not in prior), key=lambda r: r['epoch'])
    results = []
    for control in current:
        expected = dict(pid=control['control_backend_pid'], backend_start=control['control_backend_start'],
            database=database, control_epoch=control['epoch'])
        assert type(expected['pid']) is int and expected['pid'] > 0
        assert datetime.fromisoformat(expected['backend_start']).tzinfo is not None
        assert "'" not in expected['backend_start']
        answer = lab.docker('exec', '-i', pg['postgres'], 'psql', '-X', '-A', '-t', '-q',
            '--no-password', '-v', 'ON_ERROR_STOP=1', '-U', 'postgres', '-d', database,
            data="SELECT EXISTS(SELECT 1 FROM pg_catalog.pg_stat_activity WHERE pid=" + str(expected['pid'])
                + " AND backend_start='" + expected['backend_start'] + "'::timestamptz AND datname='" + database + "')",
            check=False, timeout=5)
        results.append(dict(expected=expected, returncode=answer.returncode, stdout=answer.stdout,
            stderr=answer.stderr, present=answer.stdout.strip() == 't'))
        lab.write_json(output / (mode + '-backend-exact.json'), results)
    assert [r['expected']['control_epoch'] for r in results] == ([8, 9] if mode == CASES[1] else [8])
    assert all(r['returncode'] == 0 and not r['stderr'] and r['stdout'].strip() == 'f' for r in results)
    return results


def expiry_diagnostic(pg, backend, window):
    since, until = [datetime.fromtimestamp(window[k] / 1_000_000, timezone.utc).isoformat()
        for k in ('before_us', 'after_us')]
    answer = lab.docker('logs', '--timestamps', '--since', since, '--until', until,
        pg['postgres'], check=False, timeout=5)
    text = answer.stdout + answer.stderr
    assert len(text.encode()) <= 256 * 1024
    pattern = (rf'\[{backend["pid"]}\] ERROR:  (?:(?P<sqlstate>[0-9A-Z]{{5}}): )?'
        + re.escape(EXPIRY_ERROR) + '$')
    errors = [line for line in text.splitlines() if re.search(pattern, line)]
    contexts = [line for line in text.splitlines() if f'[{backend["pid"]}] CONTEXT:  ' in line
        and re.search(r'function (?:control\.)?require_active_initial_writer\(bytea\)', line)]
    observed = re.search(pattern, errors[0]).group('sqlstate') if len(errors) == 1 else None
    return dict(backend=backend, since=since, until=until, returncode=answer.returncode,
        observed_sqlstate=observed, errors=errors, contexts=contexts)


def run(pg, client, output, rows, clone):
    from initial_live_refusals_oracle import verify, negative_controls

    basis = output.parent.parent / preparing.SEED / 'bindings'
    store = basis / 'retire_signed/store'
    frozen = dict(preparing.FROZEN,
        **{'signed-seed/event.jsonl': 'c30658a75b2b913e509b7618f36ebae75fa1d5680dc8fe4e322aae638d8affae'})
    assert all(preparing.digest(basis / n) == h for n, h in frozen.items())
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
    absent = observe_backend_absence(pg, preparing.DATABASE, output, 'live-refusals-source')
    original = rows(preparing.DATABASE)
    assert original == json.loads((basis / 'retire-signed-retry-after.json').read_text())
    baseline = json.loads((basis / 'retire-signed-retry-native.json').read_text())
    files, inputs = preparing.census(store), inputs_at(store)
    assert len(original) == 70 and len(baseline['rows']) == 49 and len(inputs) == 4
    assert sum(v['bytes'] for v in files.values()) < 1024**2
    seed = dict(control=original, native=baseline, inputs=inputs, sequence=58)
    lab.write_json(output / 'live-refusals-source.json', dict(**seed, files=files,
        frozen=frozen, backend_absence=absent, client_state=old['State']))
    cases = []
    try:
        for mode in CASES:
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
            fixture = dict(manifest=None, wire=dict(status='NOT_RUN'), backend=None,
                diagnostic=None, catalog_before=catalog, catalog_after=None)
            window = dict(before_us=server_clock(pg, database))
            lab.write_json(output / (mode + '-clock.json'), window)
            try:
                if mode == CASES[0]:
                    manifest_invoke(pg, client, output, rows, database, config,
                        (basis / 'signed-seed/event.jsonl').read_bytes(), inputs, fixture)
                else:
                    wire_mode = {CASES[3]: 'before-forward', CASES[4]: 'after-complete'}.get(mode)
                    _, _, wire = preparing.invoke(client, output, database, config, mode,
                        wire_mode, commit_number=15)
                    fixture['wire'] = wire
            finally:
                receipt = output / (mode + '-response.json')
                terminal = receipt.exists() and json.loads(receipt.read_text())['returncode'] is not None
                wire_path = output / (mode + '-wire.json')
                if wire_path.exists():
                    fixture['wire'] = json.loads(wire_path.read_text())
                try:
                    if terminal:
                        window['after_us'] = server_clock(pg, database)
                        lab.write_json(output / (mode + '-clock.json'), window)
                        after = rows(database)
                        lab.write_json(output / (mode + '-after.json'), after)
                        final_inputs = inputs_at(directory / 'store')
                        lab.write_json(output / (mode + '-inputs-after.json'), final_inputs)
                        absence = observe_backend_absence(pg, database, output, mode)
                        native = dump(client, output, mode + '-after', directory)
                        exact = exact_backends(pg, database, output, mode, before, after)
                        fixture['backend'] = dict(exact=exact, absence=absence)
                        if mode == CASES[2]:
                            fixture['diagnostic'] = expiry_diagnostic(pg, exact[0]['expected'], window)
                        fixture['catalog_after'] = live.catalog(pg, database)
                    else:
                        lab.write_json(output / (mode + '-external-unavailable.json'), dict(
                            status='NOT_RUN_UNKNOWN_PROCESS_OUTCOME',
                            reason='Native may remain owned; outer owner-checked cleanup must complete first'))
                finally:
                    lab.write_json(output / (mode + '-fixture.json'), fixture)
            response = json.loads(receipt.read_text())
            assert response['returncode'] == 0 and not response['stderr']
            events = [json.loads(line) for line in response['stdout'].splitlines()]
            final, = [e for e in events if e['event'] == 'successor-state' and e['stage'] == 'final']
            assert after == {t: json.loads(v) for t, v in final['state']['control_tables']}
            assert native == dict(event='dump-state', rows=final['rows'], sequence=final['state']['sequence'])
            assert final_inputs == dict(final['state']['originals'])
            assert all(final_inputs[n] == identity for n, identity in inputs.items())
            assert fixture['catalog_after'] == catalog
            if mode == CASES[0]:
                guard = fixture['manifest']
                assert guard['status'] == 'CAPTURED' and not guard['cleanup_errors'] and not guard['forced_wrapper_stop']
                assert guard['catalog_before'] == guard['catalog_removed'] == catalog
            if mode in CASES[3:]:
                assert fixture['wire']['status'] == 'PASS' and not fixture['wire']['forced_wrapper_stop']
            context = dict(events=events, original=before, native=baseline, inputs=inputs,
                mode=mode, seed=seed, server_window=window, fixture=fixture)
            lab.write_json(output / (mode + '-verification.json'), context)
            evidence, mutations = verify(**context), negative_controls(**context)
            cases.append(dict(name=mode, status='PASS', seconds=response['seconds'], oracle=evidence,
                negative_controls=mutations, catalog_unchanged=True, external_final_matches=True,
                backend=fixture['backend'], commit_wire=fixture['wire'], diagnostic=fixture['diagnostic']))
            lab.write_json(output / 'live-refusals-cases.json', cases)
    finally:
        preserved = dict(control=rows(preparing.DATABASE) == original,
            files=preparing.census(store) == files, inputs=inputs_at(store) == inputs,
            artifacts=all(preparing.digest(basis / n) == h for n, h in frozen.items()))
        lab.write_json(output / 'live-refusals-source-preserved.json', preserved)
        assert all(preserved.values()), preserved
    return dict(name='live-initial-refusals', status='PASS', cases=cases,
        source_lab=str(basis.parent), source_revision=preparing.SOURCE, source_owner=preparing.OWNER,
        source_preserved=preserved, source_artifacts=frozen,
        scope='committed target-manifest fence, foreign Control and expired/lost live renewal',
        not_run=['activated writer replacement', 'network intake', 'installed live writer', 'ACK/GC', 'full M1'])
