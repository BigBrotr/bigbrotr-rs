"""Seven isolated retained histories, eight installed retirement processes.

The claims seed was produced under its real activator. The binding fixture is
privileged synthetic retained history, never successful import evidence.
"""
import copy
from datetime import datetime, timezone
import json
from pathlib import Path
import re
import shutil

import lab
import activated_recovery as recovery
import activated_retirement as retirement
from activated_recovery_zero import invoke
from activated_retirement_workflows_oracle import CASES, PREFIX, verify, negative_controls, mutation_checked
from initial_gap import configuration
from initial_history import capture, inputs_at, observe_backend_absence
from initial_live import exact_absence
from initial_succession import census, digest

SOURCES = dict(off=retirement.SOURCES['off'],
    claims=dict(run='ingest-activated-acquisition-claims-engine-da839b7',
        source='da839b70602fdd83876736ddb310ae0ae8f410f3', owner='4a884dd9a60fbe0c09dce7e4eda52a39',
        status='PASS_INSTALLED_ACTIVATED_ACQUISITION_CLAIMS',
        result='47ba9f897e7bc7a8c6fcc965bfd4ca56901811f059e406580ef7a2c895a46df6',
        prefix='installed-activated-acquisition-claims', database='bb_scope_bind_da839b7_installed_activated_acquisition_claims',
        sql_file='installed-activated-acquisition-claims-retry-after-sql.json',
        sql='0385ab5bf63dea06b4201ead0399d161bf511aceb3c5653ffa7ea44099af7c6f',
        native_file='installed-activated-acquisition-claims-retry-after-native.json',
        native='48346ec2d3305e780d82ba37ecf917cb821a8771983946f3a61027895ac1e3d6',
        verification_file='installed-activated-acquisition-claims-retry-verification.json',
        verification='21407b2f55eb0e0a427343bab980a460e5813e87b8c44f14df79c117aa7a062c'),
    terminal=dict(run='ingest-activated-retirement-engine-9bdc86a',
        source='9bdc86a2316e0aa0d1078ededb80a8d9c9502a2f', owner='1979cce97962b0239e79e3fa47b2e5ae',
        status='PASS_INSTALLED_ACTIVATED_RETIREMENT_FIRST_WAVE',
        result='48506a152b393854f2f50cbedfedb71b66e53ed9578753d7e6b4df3bd76c8e09',
        prefix='installed-activated-retirement-off', database='bb_scope_bind_9bdc86a_installed_activated_retirement_off',
        sql_file='installed-activated-retirement-off-retry-after-sql.json',
        sql='7ac690057de370a00144ffd415bb1f504b05ff4b5599d6458b728820a98acbad',
        native_file='installed-activated-retirement-off-retry-after-native.json',
        native='cd49f8bfc44c5c08ae307f845f1aaf1da7f66079bf14f8ee54f9c4c1a3819238',
        verification_file='installed-activated-retirement-off-retry-verification.json',
        verification='b1e0b4eaada03b9c169592750d70285e8234abfa7ebbebc61877255b3b046d82'))


def operator_sql(kind):
    prefix = "BEGIN;\nSET LOCAL statement_timeout='10s';\nSET LOCAL lock_timeout='5s';\nSET LOCAL max_parallel_workers_per_gather=0;\nSET LOCAL search_path=pg_catalog;\n"
    if kind != 'checkpoint':
        prefix += 'SET LOCAL session_replication_role=replica;\n'
    prefix += "SELECT json_build_object('actor',current_user,'replication_role',current_setting('session_replication_role'),'transaction',pg_current_xact_id()::text)::text;\n"
    returning = lambda table: " RETURNING (to_jsonb(" + table + ")||jsonb_build_object('xmin',xmin::text))::text;\n"
    if kind == 'checkpoint':
        body = """INSERT INTO control.producer_checkpoints
(producer_slot_id,producer_epoch,dataset_id,checkpoint_revision,complete_through_request,checkpoint_digest,committed_at_us)
SELECT p.producer_slot_id,2,p.dataset_id,1,0,decode(repeat('c1',32),'hex'),s.closed_at_us
FROM control.producer_epochs p JOIN control.source_streams s ON s.dataset_id=p.dataset_id AND s.owner_epoch=2
WHERE p.producer_epoch=2""" + returning('producer_checkpoints')
    elif kind == 'binding':
        # No historical owner is assigned: this accepted import row merely gives
        # the deliberately unsupported binding its retained relational parent.
        body = """INSERT INTO control.jobs SELECT (jsonb_populate_record(NULL::control.jobs,
(to_jsonb(j)-'xmin') || jsonb_build_object('job_id','\\xb1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1',
'request_key','\\xb2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2',
'request_digest','\\xb3b3b3b3b3b3b3b3b3b3b3b3b3b3b3b3b3b3b3b3b3b3b3b3b3b3b3b3b3b3b3b3',
'kind','import','state','accepted','job_revision',1,'checkpoint_revision',0,'reason_code',NULL,
'result_manifest_digest',NULL,'completed_at_us',NULL,'acquisition_job_protocol',NULL,'export_job_protocol',NULL,
'input_topology_generation',NULL,'input_complete',true,'owner_resource_id',NULL,'owner_epoch',NULL,'owner_incarnation',NULL))).*
FROM control.jobs j JOIN control.acquisition_gaps g USING(job_id) WHERE g.producer_epoch=2""" + returning('jobs')
        body += """INSERT INTO control.job_admissions SELECT decode(repeat('b1',16),'hex'),max(admission_ordinal)+1 FROM control.job_admissions""" + returning('job_admissions')
        # Fixed signed JSONL fixture bytes. Independent oracle reconstructs all
        # binding fields; this SQL never rewrites the existing reservation.
        payload = Path(__file__).resolve().parents[2] / 'tests/fixtures/ingest/import-event.jsonl'
        raw = payload.read_bytes()
        assert digest(payload) == 'c30658a75b2b913e509b7618f36ebae75fa1d5680dc8fe4e322aae638d8affae'
        body += """INSERT INTO control.producer_input_bindings
(job_id,dataset_id,source_namespace,shard_id,source_bytes,source_sha256,parser_profile,batch_first_record,batch_records,
obligation_index,obligation_kind,logical_record,byte_start,byte_end,record_sha256,producer_slot_id,producer_epoch,
request_sequence,request_digest,input_bytes,input_profile,manifest_sha256,issuer_resource,issuer_epoch,issuer_incarnation)
SELECT decode(repeat('b1',16),'hex'),r.dataset_id,
sha256(decode('7265746972656d656e742f696d706f72742d666978747572652f3100','hex')||p.bytes),0,octet_length(p.bytes),sha256(p.bytes),1,1,1,
0,1,1,0,octet_length(p.bytes),sha256(p.bytes),r.producer_slot_id,2,r.request_sequence,r.request_digest,r.input_bytes,r.input_profile,
r.manifest_sha256,r.issuer_resource,r.issuer_epoch,r.issuer_incarnation
FROM control.producer_requests r CROSS JOIN (SELECT decode('""" + raw.hex() + """','hex') AS bytes) p
WHERE r.producer_epoch=2 AND r.request_sequence=2""" + returning('producer_input_bindings')
    elif kind == 'missing-copy':
        body = 'DELETE FROM control.producer_outcomes WHERE producer_epoch=2 AND request_sequence=2' + returning('producer_outcomes')
    elif kind == 'copy-conflict':
        changed = 'set_byte(outcome_bytes,octet_length(outcome_bytes)-1,get_byte(outcome_bytes,octet_length(outcome_bytes)-1) # 1)'
        body = 'UPDATE control.producer_outcomes SET outcome_bytes=' + changed + ',outcome_sha256=sha256(' + changed + ') WHERE producer_epoch=2 AND request_sequence=2' + returning('producer_outcomes')
    else:
        assert kind == 'mixed-terminal'
        body = 'UPDATE control.role_instances SET retired_at_us=NULL WHERE role_instance_id=(SELECT target_role_instance_id FROM control.initial_ingest_activations)' + returning('role_instances')
    return prefix + body + "COMMIT;\nSELECT json_build_object('actor',current_user,'replication_role',current_setting('session_replication_role'))::text;\n"


def state(pg, client, output, rows, database, directory, label):
    value = {}
    try:
        value['control'] = rows(database)
        lab.write_json(output / (label + '-sql.json'), value['control'])
        value.update(inputs=inputs_at(directory / 'store'), contents=recovery.contents_at(directory / 'store'))
        lab.write_json(output / (label + '-inputs.json'), dict(inputs=value['inputs'], contents=value['contents']))
        answer, _ = capture(client, output, label + '-dump', 'dump-state', '/lab/' + directory.name + '/store')
        assert answer.returncode == 0 and not answer.stderr
        value['native'] = json.loads(answer.stdout)
        lab.write_json(output / (label + '-native.json'), value['native'])
        return value
    except BaseException as error:
        lab.write_json(output / (label + '-incomplete.json'), dict(**value, status='INCOMPLETE', error=type(error).__name__))
        raise
    finally:
        lab.write_json(output / (label + '-state.json'), value)


def uncertain(output, rows, database, directory, label, reason):
    value = dict(status='OUTCOME_UNKNOWN', native='NOT_RUN', reason=reason)
    lab.write_json(output / (label + '-unavailable.json'), value)
    for key, read in [('control', lambda: rows(database)), ('inputs', lambda: inputs_at(directory / 'store')),
            ('contents', lambda: recovery.contents_at(directory / 'store'))]:
        try: value[key] = read()
        except BaseException as error: value[key + '_error'] = type(error).__name__
        lab.write_json(output / (label + '-unavailable.json'), value)


def exclude(pg, output, rows, database, directory, label):
    try:
        return observe_backend_absence(pg, database, output, label)
    except BaseException as error:
        uncertain(output, rows, database, directory, label, 'backend absence: ' + type(error).__name__)
        raise


def mutate(pg, client, output, rows, database, directory, mode, before):
    kind = mode.removeprefix(PREFIX)
    value = dict(before=before, operation=None)
    try:
        if kind == 'missing-input':
            name, = [n for n in before['inputs'] if n.endswith('-0000000000000002-0000000000000002.cbor')]
            source = directory / 'store/producer-inputs-v1' / name
            retained = output / (mode + '-input-original.cbor')
            assert source.stat().st_mode & 0o777 == 0o600 and not retained.exists()
            shutil.copy2(source, retained)
            value['operation'] = dict(kind='input', action='remove', name=name, before_mode=source.stat().st_mode & 0o777,
                retained=dict(path='/lab/' + retained.name, mode=retained.stat().st_mode & 0o777,
                    hex=retained.read_bytes().hex(), bytes=retained.stat().st_size, sha256=digest(retained)))
            lab.write_json(output / (mode + '-mutation.json'), value)
            source.unlink()
            value['operation']['after_mode'] = None if not source.exists() else source.stat().st_mode & 0o777
        elif kind != 'claims':
            statement = operator_sql(kind)
            value['operation'] = dict(kind='sql', actor='privileged-isolated-fixture', statement=statement)
            lab.write_json(output / (mode + '-mutation.json'), value)
            value['operation']['response'] = recovery.statement(pg, database, output, mode + '-operator', statement)
    finally:
        lab.write_json(output / (mode + '-mutation.json'), value)
        value['after'] = state(pg, client, output, rows, database, directory, mode + '-mutated')
        lab.write_json(output / (mode + '-mutation.json'), value)
    mutation_checked(value, before['control'], before, kind)
    return value


def diagnostic(pg, expected, clock, kind):
    message, function = ('activated retirement requires exactly ready or complete terminal metadata',
        'require_activated_initial_retirement') if kind == 'mixed-terminal' else (
        'activated retirement requires exact copies and supported workflow obligations', 'lock_activated_initial_retirement')
    since, until = [datetime.fromtimestamp(clock[k] / 1_000_000, timezone.utc).isoformat() for k in ('before_us', 'after_us')]
    answer = lab.docker('logs', '--timestamps', '--since', since, '--until', until, pg['postgres'], check=False, timeout=5)
    raw = answer.stdout + answer.stderr
    assert len(raw.encode()) <= 256 * 1024
    pattern = rf'\[{expected["pid"]}\] ERROR:  (?:(?P<code>[0-9A-Z]{{5}}): )?' + re.escape(message) + '$'
    errors = [line for line in raw.splitlines() if re.search(pattern, line)]
    contexts = [line for line in raw.splitlines() if f'[{expected["pid"]}] CONTEXT:  ' in line
        and 'function control.' + function + '(bytea)' in line]
    return dict(backend=expected, since=since, until=until, returncode=answer.returncode,
        observed_sqlstate=re.search(pattern, errors[0]).group('code') if len(errors) == 1 else None,
        errors=errors, contexts=contexts)


def execute(pg, client, output, rows, database, config, directory, helper, mode, seed, setup, mutation, predecessor=None):
    exclude(pg, output, rows, database, directory, mode + '-before')
    before = state(pg, client, output, rows, database, directory, mode + '-before')
    local = dict(copy.deepcopy(setup), catalog_before=retirement.catalog(pg, database), diagnostic=None)
    response, clock = invoke(pg, client, output, database, config, mode, helper, action='--retire-activated-initial')
    if response['outcome'] != 'PROCESS_EXITED':
        uncertain(output, rows, database, directory, mode, 'CLI custody unknown; outer owner must join')
        raise AssertionError('installed retirement custody unavailable')
    absence = exclude(pg, output, rows, database, directory, mode)
    after = state(pg, client, output, rows, database, directory, mode + '-after')
    local['catalog_after'] = retirement.catalog(pg, database)
    lab.write_json(output / (mode + '-external.json'), dict(**after, catalog=local['catalog_after'], absence=absence))
    exact = exact_absence(pg, database, output, mode, after['control'], before['control'])
    kind = mode.removeprefix(PREFIX)
    if kind in ('checkpoint', 'binding', 'missing-copy', 'mixed-terminal') and clock is not None:
        local['diagnostic'] = diagnostic(pg, exact['expected'], clock, kind)
        lab.write_json(output / (mode + '-diagnostic.json'), local['diagnostic'])
    context = dict(before=before['control'], after=after['control'], native_before=before['native'], native_after=after['native'],
        inputs_before=before['inputs'], inputs_after=after['inputs'], input_contents=after['contents'], response=response,
        mode=mode, seed=seed, setup=local, server_window=clock, backend=dict(exact=exact, absence=absence),
        mutation=mutation, predecessor=predecessor)
    lab.write_json(output / (mode + '-verification.json'), context)
    checked, controls = verify(**context), negative_controls(**context)
    return dict(name=mode, status='PASS', oracle=checked, negative_controls=controls,
        verification_sha256=digest(output / (mode + '-verification.json'))), context


def run_all(pg, client, output, rows, clone):
    helper = Path(__file__).with_name('activated_acquisition_client.py')
    if not (output / helper.name).exists():
        shutil.copy2(helper, output / helper.name)
    assert digest(output / helper.name) == digest(helper)
    origins, cases = {}, []
    try:
        containers = json.loads(lab.docker('inspect', *lab.docker('ps', '-aq').stdout.split()).stdout)
        for mode in CASES:
            name = 'claims' if mode.endswith('-claims') else 'terminal' if mode.endswith('-mixed-terminal') else 'off'
            spec = SOURCES[name]
            if name not in origins:
                basis = output.parent.parent / spec['run'] / 'bindings'
                frozen = {'result.json': spec['result'], spec['sql_file']: spec['sql'],
                    spec['native_file']: spec['native'], spec['verification_file']: spec['verification']}
                assert all(digest(basis / n) == h for n, h in frozen.items())
                result = json.loads((basis / 'result.json').read_text())
                assert all(result[k] == spec[k] for k in ('source', 'owner', 'status'))
                assert not any(result[k] for k in ('cleanup_errors', 'unrelated_changed', 'missing_owned'))
                old, = [c for c in containers if c['Id'] == result['client']['id']]
                assert old['Config']['Labels'][result['label']] == spec['owner']
                assert not old['State']['Running'] and not old['State']['Pid'] and not old['State']['OOMKilled']
                assert not any(c['State']['Running'] and any(str(basis) in m.get('Source', '') for m in c['Mounts']) for c in containers)
                absence = observe_backend_absence(pg, spec['database'], output, mode + '-original')
                original = rows(spec['database'])
                assert original == json.loads((basis / spec['sql_file']).read_text())
                store = basis / spec['prefix'] / 'store'
                seed = dict(name=name, verification=json.loads((basis / spec['verification_file']).read_text()),
                    control=original, native=json.loads((basis / spec['native_file']).read_text()), inputs=inputs_at(store),
                    contents=recovery.contents_at(store), catalog=retirement.catalog(pg, spec['database']))
                origin = dict(seed=seed, files=census(store), frozen=frozen, backend_absence=absence,
                    client_id=old['Id'], client_state=old['State'], label=result['label'])
                origins[name] = origin, basis, store, spec
                lab.write_json(output / (name + '-workflow-original.json'), origin)
            origin, basis, store, spec = origins[name]
            seed = origin['seed']
            database = clone('retire-wf-' + mode.removeprefix(PREFIX), spec['database'])
            assert rows(database) == seed['control']
            directory = output / mode
            directory.mkdir(mode=0o700)
            shutil.copytree(store, directory / 'store')
            copied = state(pg, client, output, rows, database, directory, mode + '-seed')
            assert copied == {k: seed[k] for k in ('control', 'native', 'inputs', 'contents')}
            config = configuration(pg, client, output, database, directory, seed['control'])
            setup = dict(migration_sha256=digest(Path(__file__).resolve().parents[2] / 'migrations/postgres/0037_activated_initial_retirement.sql'),
                migration=retirement.upgrade(pg, client, output, database, config, mode, rows))
            baseline = state(pg, client, output, rows, database, directory, mode + '-prepared')
            mutation = mutate(pg, client, output, rows, database, directory, mode, baseline)
            first, prior = execute(pg, client, output, rows, database, config, directory, helper, mode, seed, setup, mutation)
            attempts = [first]
            if mode == CASES[0]:
                retry, _ = execute(pg, client, output, rows, database, config, directory, helper, mode + '-retry', seed, setup, mutation, prior)
                attempts.append(retry)
            cases.append(dict(name=mode, status='PASS', attempts=attempts))
            lab.write_json(output / 'activated-retirement-workflows-cases.json', cases)
    finally:
        preserved = {}
        for name, (origin, basis, store, spec) in origins.items():
            seed = origin['seed']
            old, = json.loads(lab.docker('inspect', origin['client_id']).stdout)
            preserved[name] = dict(control=rows(spec['database']) == seed['control'], files=census(store) == origin['files'],
                inputs=inputs_at(store) == seed['inputs'], contents=recovery.contents_at(store) == seed['contents'],
                catalog=retirement.catalog(pg, spec['database']) == seed['catalog'],
                client=old['State'] == origin['client_state'] and old['Config']['Labels'][origin['label']] == spec['owner'],
                artifacts=all(digest(basis / n) == h for n, h in origin['frozen'].items()))
        lab.write_json(output / 'activated-retirement-workflows-preserved.json', preserved)
        assert all(all(v.values()) for v in preserved.values()), preserved
    assert [c['name'] for c in cases] == list(CASES) and sum(len(c['attempts']) for c in cases) == 8
    return dict(name='installed-activated-retirement-workflows', status='PASS', cli_invocations=8, cases=cases,
        source_preserved=preserved, helper_sha256=digest(helper),
        scope='Genuine zero-request acquisition and six isolated retained-obligation/copy/input/terminal refusals',
        not_run=['live import claim and import execution', 'new source closure or abandonment',
            'retirement cancellation/UTC lock wait', 'ACK/GC, epoch3, Archive and full M1'])
