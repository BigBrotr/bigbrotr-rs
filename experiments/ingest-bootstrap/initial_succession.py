"""Actual prepared-only successor, on isolated copies of the retired signed seed.

This fixture retains the proof in one probe process. It does not claim process
reconstruction, activation, import admission, ACK/GC or the complete M1 journey.
"""
import hashlib
import json
from pathlib import Path
import shutil
import stat
import subprocess
import time

import lab
from installed import set_value
from initial_succession_oracle import verify, negative_controls

SEED = 'ingest-recovery-engine-f12dbf0'
DATABASE = 'bb_scope_bind_f12dbf0_retire_signed'
SOURCE = 'f12dbf04677f8afa8c62aa1c8638e199dad0abc1'
OWNER = '0413e7dbcebefbf84c90b3948f4bb24b'
FROZEN = {
    'result.json': '93eb62bbef23f00a64c2d3441ccd0b63309f3249d35300d0ade70cae654db357',
    'retire-signed-retry-after.json': '9aa8a79a0d4889ff13644675d9a2e9cf083b057b733321976da1cad672b3b25e',
    'retire-signed-retry-native.json': 'fef491ee502b2a1fc548165d0a2a2a7cb6411269b2eb6355fed546f831d93465',
}


def digest(path):
    with path.open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def census(root):
    result = {}
    for path in [root, *sorted(root.rglob('*'))]:
        metadata = path.lstat()
        assert stat.S_ISDIR(metadata.st_mode) or stat.S_ISREG(metadata.st_mode)
        result[str(path.relative_to(root))] = dict(mode=metadata.st_mode,
            inode=metadata.st_ino, device=metadata.st_dev, links=metadata.st_nlink,
            bytes=metadata.st_size, sha256=digest(path) if path.is_file() else None)
    return result


def physical(pg, database):
    return json.loads(lab.sql(pg, database, "SELECT jsonb_object_agg(relname,pg_relation_filenode(c.oid)) "
        "FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace "
        "WHERE n.nspname='control' AND c.relkind='r'"))


def upgrade(pg, client, output, database, config, name, rows):
    before, files = rows(database), physical(pg, database)
    def apply():
        return lab.cli(dict(pg, client=client), 'migrate', 'apply', '--file', config,
            '--user', 'bigbrotr_migrator', '--password-ref', 'file:/run/secrets/migrator', '--target', '30')
    response = apply()
    lab.write_json(output / (name + '-migration.json'), dict(returncode=response.returncode,
        stdout=response.stdout, stderr=response.stderr))
    assert response.returncode == 0, response.stderr
    after = rows(database)
    assert set(after) == set(before) | {'initial_ingest_successions'} and len(after) == 71
    assert not after['initial_ingest_successions']
    assert all(after[t] == before[t] for t in before if t != 'schema_versions')
    assert all(r in after['schema_versions'] for r in before['schema_versions'])
    added, = [r for r in after['schema_versions'] if r not in before['schema_versions']]
    migration = Path(__file__).resolve().parents[2] / 'migrations/postgres/0030_initial_succession.sql'
    assert added['migration'] == 30 and added['checksum'] == '\\x' + digest(migration)
    repeat = apply()
    assert repeat.returncode == 0 and rows(database) == after
    final_files = physical(pg, database)
    assert all(final_files[t] == value for t, value in files.items())
    lab.write_json(output / (name + '-upgrade.json'), dict(before=before, after=after,
        physical_before=files, physical_after=final_files, exact_reapply=True))
    # Row-locking SELECT needs UPDATE privilege; immutable guards still veto
    # mutations. This is explicit lab provisioning, not an implicit product grant.
    lab.sql(pg, database, 'GRANT SELECT,INSERT,UPDATE ON control.initial_ingest_successions TO bigbrotr_control; '
        'GRANT EXECUTE ON FUNCTION control.veto_unactivated_producer(),'
        'control.require_prepared_initial_successor(bytea),control.guard_initial_ingest_succession() TO bigbrotr_control;')


def activation_veto(pg, database, state, target_manifest, event):
    """Prove the named capability fence, not merely an unrelated SQL failure.

    The input-binding envelope is deliberately not a running import job. Its
    earlier activation trigger must reject it before those later prerequisites.
    """
    identity = json.loads(lab.sql(pg, database, "SELECT json_build_object('replication_role',"
        "current_setting('session_replication_role'),'triggers',(SELECT json_agg(json_build_object("
        "'name',tgname,'enabled',tgenabled) ORDER BY tgname) FROM pg_trigger WHERE tgname IN "
        "('producer_request_activation_guard','input_binding_activation_guard')),'function',"
        "pg_get_functiondef('control.veto_unactivated_producer()'::regprocedure))"))
    assert identity['replication_role'] == 'origin'
    assert identity['triggers'] == [dict(name=n, enabled='O') for n in
        ['input_binding_activation_guard', 'producer_request_activation_guard']]
    patch = json.dumps(dict(producer_epoch=2, request_sequence=1,
        manifest_sha256='\\x' + hashlib.sha256(bytes.fromhex(target_manifest)).hexdigest())).encode().hex()
    request = ("INSERT INTO control.producer_requests SELECT (jsonb_populate_record("
        "NULL::control.producer_requests,to_jsonb(r)||convert_from(decode('" + patch + "','hex'),'UTF8')::jsonb)).* "
        "FROM control.producer_requests r WHERE producer_epoch=1 AND request_sequence=1")
    original = next(r for r in state['producer_requests'] if r['request_sequence'] == 4)
    binding = {k: original[k] for k in ['dataset_id', 'producer_slot_id', 'request_digest',
        'input_bytes', 'input_profile', 'issuer_resource', 'issuer_epoch', 'issuer_incarnation']}
    binding.update(job_id=state['jobs'][0]['job_id'], producer_epoch=2, request_sequence=1,
        manifest_sha256='\\x' + hashlib.sha256(bytes.fromhex(target_manifest)).hexdigest(),
        source_namespace='\\x' + hashlib.sha256(b'successor-veto-fixture/1').hexdigest(),
        shard_id=0, source_bytes=len(event), source_sha256='\\x' + hashlib.sha256(event).hexdigest(),
        parser_profile=1, batch_first_record=1, batch_records=1, obligation_index=0,
        obligation_kind=1, logical_record=1, byte_start=0, byte_end=len(event),
        record_sha256='\\x' + hashlib.sha256(event.rstrip(b'\n')).hexdigest())
    encoded = json.dumps(binding).encode().hex()
    input_binding = ("INSERT INTO control.producer_input_bindings SELECT (jsonb_populate_record("
        "NULL::control.producer_input_bindings,convert_from(decode('" + encoded + "','hex'),'UTF8')::jsonb)).*")
    answers = []
    for name, statement in [('producer-request', request), ('input-binding-envelope', input_binding)]:
        answer = json.loads(lab.sql(pg, database, """
            BEGIN;
            CREATE TEMP TABLE succession_veto_observation (value jsonb);
            GRANT SELECT,INSERT ON succession_veto_observation TO bigbrotr_control;
            SET LOCAL ROLE bigbrotr_control;
            DO $fixture$ DECLARE code text; message text; context text; refused boolean := false;
            BEGIN
                BEGIN
                    """ + statement + """;
                EXCEPTION WHEN OTHERS THEN
                    GET STACKED DIAGNOSTICS code = RETURNED_SQLSTATE,
                        message = MESSAGE_TEXT, context = PG_EXCEPTION_CONTEXT;
                    IF code <> '23514' OR message <> 'successor admission requires an explicit activation protocol'
                        OR position('control.veto_unactivated_producer()' in context) = 0 THEN
                        RAISE;
                    END IF;
                    refused := true;
                    INSERT INTO succession_veto_observation VALUES
                        (jsonb_build_object('sqlstate',code,'message',message,'context',context));
                END;
                IF NOT refused THEN RAISE EXCEPTION 'activation veto did not fire'; END IF;
            END $fixture$;
            SELECT value FROM succession_veto_observation;
            ROLLBACK;
        """))
        answers.append(dict(name=name, **answer))
    return dict(identity=identity, cases=answers, scope='named prepared-only SQL capability fences')


def run(pg, client, output, rows, clone):
    basis = output.parent.parent / SEED / 'bindings'
    store = basis / 'retire_signed/store'
    assert all(digest(basis / name) == value for name, value in FROZEN.items())
    retained = json.loads((basis / 'result.json').read_text())
    assert retained['status'] == 'PASS_INSTALLED_INITIAL_RETIREMENT_FIRST_WAVE'
    assert retained['source'] == SOURCE and retained['owner'] == OWNER
    assert not retained['cleanup_errors'] and not retained['unrelated_changed']
    containers = json.loads(lab.docker('inspect', *lab.docker('ps', '-aq').stdout.split()).stdout)
    old, = [c for c in containers if c['Id'] == retained['client']['id']]
    assert old['Config']['Labels'][retained['label']] == OWNER
    assert not old['State']['Running'] and not old['State']['OOMKilled']
    assert not any(c['State']['Running'] and any(str(basis) in m.get('Source', '')
        for m in c['Mounts']) for c in containers)
    original = rows(DATABASE)
    assert original == json.loads((basis / 'retire-signed-retry-after.json').read_text())
    assert len(original) == 70 and len(original['producer_requests']) == 4
    baseline = json.loads((basis / 'retire-signed-retry-native.json').read_text())
    assert len(baseline['rows']) == 49
    files = census(store)
    assert sum(v['bytes'] for v in files.values()) < 1024**2
    inputs = {p.name: dict(bytes=p.stat().st_size, sha256=digest(p))
        for p in (store / 'producer-inputs-v1').iterdir()}
    assert len(inputs) == 4
    event = (basis / 'signed-seed/event.jsonl').read_bytes()
    lab.write_json(output / 'successor-source.json', dict(control=original, native=baseline,
        files=files, inputs=inputs, frozen=FROZEN))
    cases = []

    def dump(name, directory):
        response = lab.docker('exec', client, '/build/ingest_bootstrap_probe', 'dump',
            '/lab/' + directory.name + '/store')
        assert response.returncode == 0 and not response.stderr
        (output / (name + '-native.json')).write_text(response.stdout)
        return json.loads(response.stdout)

    try:
        for mode in ['normal', 'gap', 'old-schema', 'rollback']:
            name = 'successor-' + mode
            database = clone(name, DATABASE)
            directory = output / name
            directory.mkdir(mode=0o700)
            shutil.copytree(store, directory / 'store')
            assert dump(name + '-before', directory) == baseline
            config = lab.config(output, dict(pg, client=client), database,
                endpoint='127.0.0.1:5432', boundary='loopback')
            path = output / Path(config).name
            text = path.read_text()
            for section, key, value in [
                ('topology', 'dataset_id', original['datasets'][0]['dataset_id'][2:]),
                ('topology', 'instance_id', original['datasets'][0]['instance_id'][2:]),
                ('topology', 'local_roles', ['control', 'ingest', 'archive']),
                ('postgres.control', 'password_ref', 'file:/run/secrets/application'),
                ('storage', 'ingest_root', '/lab/' + name + '/store'),
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
            valid = lab.cli(dict(pg, client=client), 'config', 'validate', '--file', config)
            assert valid.returncode == 0, valid.stderr
            if mode != 'old-schema':
                upgrade(pg, client, output, database, config, name, rows)
            if mode == 'rollback':
                # A sequence proves the actual insert trigger was reached even
                # though all transactional metadata writes roll back.
                lab.sql(pg, database, """
                    CREATE SCHEMA successor_fixture;
                    CREATE SEQUENCE successor_fixture.intent_hit;
                    GRANT USAGE ON SCHEMA successor_fixture TO bigbrotr_control;
                    GRANT USAGE ON SEQUENCE successor_fixture.intent_hit TO bigbrotr_control;
                    CREATE FUNCTION successor_fixture.reject_intent() RETURNS trigger
                    LANGUAGE plpgsql SET search_path=pg_catalog AS $$ BEGIN
                        PERFORM nextval('successor_fixture.intent_hit');
                        RAISE EXCEPTION 'qualification intent insert rollback' USING ERRCODE='P0001';
                    END $$;
                    CREATE TRIGGER fixture_intent_rollback BEFORE INSERT ON control.initial_ingest_successions
                        FOR EACH ROW EXECUTE FUNCTION successor_fixture.reject_intent();
                """)
                assert lab.sql(pg, database, 'SELECT is_called FROM successor_fixture.intent_hit') == 'f'
            before = rows(database)
            lab.write_json(output / (name + '-before.json'), before)
            started = time.monotonic()
            try:
                response = lab.docker('exec', client, '/build/ingest_bootstrap_probe', name,
                    database, '/run/secrets/application', config, check=False, timeout=90)
            except subprocess.TimeoutExpired as error:
                def partial(value):
                    return value.decode(errors='replace') if isinstance(value, bytes) else value or ''
                lab.write_json(output / (name + '-response.json'), dict(returncode=None,
                    stdout=partial(error.stdout), stderr=partial(error.stderr),
                    seconds=time.monotonic() - started, outcome='UNKNOWN_TIMEOUT',
                    cleanup='outer owner-checked client stop; no inferred SQL/native rollback'))
                raise
            elapsed = time.monotonic() - started
            lab.write_json(output / (name + '-response.json'), dict(returncode=response.returncode,
                stdout=response.stdout, stderr=response.stderr, seconds=elapsed))
            after = rows(database)
            final_native = dump(name + '-after', directory)
            lab.write_json(output / (name + '-after.json'), after)
            assert response.returncode == 0 and not response.stderr, (name, response.stderr)
            events = [json.loads(line) for line in response.stdout.splitlines()]
            evidence = verify(events, before, baseline, inputs, name)
            mutations = negative_controls(events, before, baseline, inputs, name)
            final, = [e for e in events if e.get('stage') == 'final' and e['event'] == 'successor-state']
            assert after == {table: json.loads(value) for table, value in final['state']['control_tables']}
            assert final_native == dict(baseline, rows=final['rows'])
            assert inputs == {p.name: dict(bytes=p.stat().st_size, sha256=digest(p))
                for p in (directory / 'store/producer-inputs-v1').iterdir()}
            veto = None
            if mode == 'normal':
                plan, = [e for e in events if e['event'] == 'successor-plan']
                veto = activation_veto(pg, database, after, plan['target_manifest'], event)
                assert rows(database) == after and dump(name + '-veto', directory) == final_native
            if mode == 'rollback':
                assert lab.sql(pg, database, 'SELECT last_value::text||is_called::text FROM successor_fixture.intent_hit') == '1true'
                lab.sql(pg, database, 'DROP TRIGGER fixture_intent_rollback ON control.initial_ingest_successions; '
                    'DROP SCHEMA successor_fixture CASCADE;')
                assert rows(database) == after
            assert lab.sql(pg, database, "SELECT count(*) FROM pg_stat_activity WHERE "
                "datname=current_database() AND usename='bigbrotr_control'") == '0'
            cases.append(dict(name=name, status='PASS', seconds=elapsed, oracle=evidence,
                negative_controls=mutations, activation_veto=veto, rollback_trigger_hit=mode == 'rollback'))
            lab.write_json(output / 'successor-cases.json', cases)
    finally:
        preserved = dict(control=rows(DATABASE) == original, files=census(store) == files,
            artifacts=all(digest(basis / name) == value for name, value in FROZEN.items()))
        lab.write_json(output / 'successor-source-preserved.json', preserved)
        assert all(preserved.values()), preserved
    return dict(name='initial-successor-preparation', status='PASS', cases=cases,
        source_lab=str(basis.parent), source_revision=SOURCE, source_owner=OWNER,
        source_preserved=preserved, source_artifacts=FROZEN,
        scope='public typed preparation with retained in-process proof, actual PG and RocksDB',
        excluded='process-loss reconstruction, writer activation, ACK/GC, restart/resend and full M1')
