#!/usr/bin/env python3
"""Bounded schema41 SQL, runtime-role and source-ordering checks in a fresh PG lab.

Uses the existing lab setup/stop owner and the maintained0041 fixture. The
explicit --sql-only-diagnostic mode applies exact migration SQL and receipts;
it does NOT qualify the product migration CLI or private Rust/CH proofs.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re

import lab
import index_publication_races as races
from concurrency_cases import Session, identity, require, sha, wait_blocked


SOURCE = Path(__file__).resolve().parents[2]
FIXTURE_SCHEMA = "index_publication_fixture"
ROLE = "bigbrotr_control"
FIXTURE_HELPERS = (
    "id(integer)", "digest(integer)", "assert_true(text,boolean)", "snapshot()",
    "expect_error(text,text,text)", "prepare(integer,integer,integer,text,boolean,boolean)",
    "transfer(boolean)", "publish(integer,boolean,boolean)",
)
PRIVILEGE_REFUSALS = (
    "rewrite_sealed_source", "rewrite_membership", "rewrite_incarnation",
    "timeout_cannot_invalidate", "publication_receipt_is_immutable", "prepared_plan_cannot_be_deleted",
)
HANDOFF_SQL = 'experiments/control-migrations/index_handoff_cases.sql'
HANDOFF_COMMON = {'exact_current_seed', 'actual_terminal_classification'}
HANDOFF_TERMINAL = HANDOFF_COMMON | {
    'stale_owner', 'stale_head', 'competing_plan_before_handoff', 'competing_plan_after_handoff',
    'fake_batch_handoff_overlap', 'missing_typed_receipt', 'wrong_succession_digest',
    'changed_predecessor_cut', 'changed_predecessor_attestation', 'missing_predecessor_attestation',
    'missing_predecessor_bound', 'missing_successor_bound', 'missing_atomic_head',
    'positive_bridge_committed', 'all_prior_rows_and_attestation_preserved',
    'immutable_handoff_update', 'immutable_handoff_delete', 'immutable_handoff_truncate',
    'wrong_handoff_recovery_capture', 'retry_captures_typed_handoff_head',
    'historical_handoff_replays_after_publisher_release',
}
HANDOFF_NONTERMINAL = HANDOFF_COMMON | {'nonterminal_successor_refuses_bridge'}
HANDOFF_POSITIVE = HANDOFF_COMMON | {'positive_bridge_committed', 'all_prior_rows_and_attestation_preserved',
    'retry_captures_typed_handoff_head', 'historical_handoff_replays_after_publisher_release'}


def validate_handoff_seed(rows, phase, tables, migration_sha):
    """Closed installed provenance-on seed; no inferred alternate lifecycle."""
    require(phase in ('terminal', 'nonterminal'), 'unknown-handoff-seed-phase')
    require(len(tables) == 82 and set(rows) == set(tables), 'exact82-seed-tables-required')

    def only(name, **fields):
        selected = [row for row in rows[name] if all(row.get(k) == v for k, v in fields.items())]
        require(len(selected) == 1, 'ambiguous-handoff-seed-' + name)
        return selected[0]

    dataset = only('datasets')
    generation = only('projection_generations')
    consumer = only('consumers', consumer_role='index')
    succession = only('initial_ingest_successions')
    session = only('index_owner_sessions')
    require(dataset['topology_generation'] == 1 and generation['dataset_id'] == dataset['dataset_id']
            and generation['owner_resource_id'] == consumer['consumer_id'] == session['resource_id']
            and generation['generation_id'] == session['generation_id']
            and generation['owner_epoch'] == session['owner_epoch'] == 1
            and generation['provenance'] == 'local_first_witness'
            and consumer['current_revision'] == 4 and consumer['current_topology_generation'] == 0
            and not rows['index_frontier_handoffs'], 'exact-unbridged-index-seed-required')
    require(len(rows['projection_plans']) == len(rows['index_frontier_plans']) == 4
            and {row['frontier_revision'] for row in rows['projection_plans']} == {1, 2, 3, 4}
            and all(row['sealed'] for row in rows['projection_plans'])
            and len(rows['projection_chunks']) == 40
            and all(row['state'] == 'committed' for row in rows['projection_chunks']), 'complete-old-index-prefix-required')
    old = only('source_streams', owner_epoch=1)
    new = only('source_streams', owner_epoch=2)
    producer = only('producer_epochs', producer_epoch=2)
    ingest = only('owner_epochs', role='ingest', epoch=2)
    require(len(rows['source_streams']) == 2 and old['state'] == new['state'] == 'closed'
            and old['last_complete_sequence'] == 12 and new['last_complete_sequence'] == 10
            and old['shard_id'] == new['shard_id'] == succession['shard_id'] == 0
            and old['owner_resource_id'] == new['owner_resource_id'] == succession['owner_resource_id']
            and producer['state'] == ('retired' if phase == 'terminal' else 'closing')
            and (ingest['state'] == 'released' if phase == 'terminal' else ingest['state'] in ('active', 'draining')),
            'genuine-closed-successor-phase-required')
    require(all(row['state'] == 'released' for row in rows['owner_epochs'] if row['role'] == 'control'),
            'released-control-seed-required')
    require(only('schema_versions', migration=41)['checksum'] == '\\x' + migration_sha,
            'current-consolidated-schema-required')
    require(len(json.dumps(rows).encode()) <= 4 * 1024**2, 'seed-capture4MiB-ceiling')


def handoff_summary(answer, phase):
    summaries = [json.loads(line) for line in answer['lines']
                 if line.startswith('{') and 'bigbrotr-index-handoff-sql-cases/1' in line]
    require(len(summaries) == 1, 'one-complete-handoff-summary-required')
    summary = summaries[0]
    expected = HANDOFF_TERMINAL if phase == 'terminal' else HANDOFF_NONTERMINAL
    require(summary['format'] == 'bigbrotr-index-handoff-sql-cases/1'
            and summary['kind'] == 'SQL_ONLY_NO_NATIVE_PROOF'
            and summary['terminal'] is (phase == 'terminal')
            and summary['case_count'] == len(summary['cases']) == len(expected)
            and {row['name'] for row in summary['cases']} == expected,
            'handoff-sql-cases-incomplete')
    for row in summary['cases']:
        state = None if row['name'] in HANDOFF_POSITIVE else ('P0002' if row['name'] == 'missing_typed_receipt' else '23514')
        require(row['state'] == state and isinstance(row['classification'], str) and row['classification']
                and (row['message'] is None if state is None else isinstance(row['message'], str) and bool(row['message'])),
                'handoff-case-outcome-incomplete')
    return summary


def run_handoff_clone(runtime, output, *, phase):
    """Opt-in installed Index hook; normal migration and other journeys unchanged.

    Runtime owns the existing PG container and bounded command capture. We own
    only one fixed, absent database name and one Session in that container. The
    original database remains read-only throughout. Clones are retained with PG.
    """
    require(phase in ('nonterminal', 'terminal'), 'unknown-handoff-seed-phase')
    output.mkdir(mode=0o700)
    runtime.source_unchanged()
    runtime.inspect(runtime.pg)
    pg = {'postgres': runtime.pg['id']}
    seed, database = 'bigbrotr_control', 'bb_index_handoff_' + phase
    migration_sha = sha(runtime.source / 'migrations/postgres/0041_index_publication.sql')
    program = (runtime.source / HANDOFF_SQL).read_text()
    require(len(program.encode()) <= 64 * 1024, 'handoff-script64KiB-ceiling')
    (output / 'cases.sql').write_text(program)
    report = dict(format='bigbrotr-index-handoff-sql-copy/1', status='RUNNING', phase=phase,
                  seed=seed, database=database, postgres_id=pg['postgres'], source_sha256=sha(runtime.source / HANDOFF_SQL),
                  migration_sha256=migration_sha, not_run=['native/ClickHouse proof', 'product handoff COMMIT faults'])
    sessions, original, clone_identity, primary_error = [], None, None, None
    resource = dict(database=database, seed=seed, phase=phase, status='reserved-absent', postgres_id=pg['postgres'])

    def save():
        lab.write_json(output / 'result.json', report)
        runtime.record_resources()

    def owned():
        runtime.inspect(runtime.pg)
        require(runtime.pg['id'] == pg['postgres'], 'handoff-owned-pg-changed')
        if clone_identity is not None:
            require(identity_of(database) == clone_identity, 'handoff-clone-identity-changed')

    def identity_of(name):
        return json.loads(runtime.sql('postgres', "SELECT coalesce(jsonb_agg(jsonb_build_object('oid',oid,'name',datname,"
            "'owner',pg_get_userbyid(datdba))),'[]') FROM pg_database WHERE datname='" + name + "'"))

    def absent(name):
        require(runtime.sql('postgres', "SELECT count(*) FROM pg_stat_activity WHERE datname='" + name + "'") == '0',
                'handoff-database-backend-remains-' + name)

    try:
        absent(seed)
        original = runtime.rows(pg, seed)
        validate_handoff_seed(original, phase, runtime.tables, migration_sha)
        lab.write_json(output / 'seed-before.json', original)
        report['seed_identity'] = identity_of(seed)
        require(len(report['seed_identity']) == 1, 'unique-seed-database-required')
        require(int(runtime.sql('postgres', "SELECT pg_database_size('" + seed + "')")) <= 64 * 1024**2,
                'handoff-seed64MiB-ceiling')
        require(identity_of(database) == [], 'handoff-clone-name-exists')
        runtime.resources.setdefault('index_handoff_sql_clones', []).append(resource)
        save()
        owned()
        absent(seed)
        runtime.sql('postgres', 'CREATE DATABASE ' + database + ' OWNER bigbrotr_migrator TEMPLATE ' + seed)
        clone_identity = identity_of(database)
        require(len(clone_identity) == 1 and clone_identity[0]['owner'] == 'bigbrotr_migrator', 'clone-owner-mismatch')
        resource.update(status='created', identity=clone_identity)
        save()
        require(runtime.rows(pg, database) == original, 'clone-does-not-preserve-complete-seed')
        script = '/tmp/bigbrotr-index-handoff-' + phase + '.sql'
        runtime.docker('exec', '-i', pg['postgres'], 'sh', '-c', 'umask 077; set -C; cat > "$1"', 'sh', script,
                       data=program, timeout=10, label='handoff-sql-script')
        require(runtime.docker('exec', pg['postgres'], 'sha256sum', script).stdout.split()[0] == report['source_sha256'],
                'container-sql-script-differs')
        session = Session(pg, database, 'index_handoff_' + phase, sessions, role='bigbrotr_migrator')
        report['backend'] = session.backend
        resource['psql_pid'] = session.process.pid
        save()
        session.start('\\set expected_terminal ' + ('true' if phase == 'terminal' else 'false')
                      + '\n\\set expected_migration_41_sha256 ' + migration_sha + '\n\\i ' + script)
        answer = session.finish(timeout=30)
        lab.write_json(output / 'session-output.json', answer)
        session.expect(answer)
        report['summary'] = handoff_summary(answer, phase)
        report['status'] = 'PASS_SQL_ONLY'
    except BaseException as error:
        primary_error = error
        report.update(status='FAIL', failure_class=type(error).__name__, failure=str(error))
        raise
    finally:
        errors = []
        for session in sessions:
            try:
                session.close(owned)
            except BaseException as error:
                errors.append(dict(stage='session-close', error=type(error).__name__))
        report['psql_processes'] = [dict(pid=session.process.pid, exit_code=session.process.poll(),
            reaped=session.process.poll() is not None) for session in sessions]
        report['session_capture'] = [dict(backend=session.backend, lines=session.last_response_lines,
            remaining_bytes_hex=session.buffer.hex(), pending=session.pending, total_bytes=session.total_bytes)
            for session in sessions]
        try:
            owned()
            absent(database)
            absent(seed)
            if 'seed_identity' in report:
                require(identity_of(seed) == report['seed_identity'], 'seed-database-identity-changed')
            if original is not None:
                after = runtime.rows(pg, seed)
                lab.write_json(output / 'seed-after.json', after)
                require(after == original, 'handoff-fixture-mutated-seed')
            observed_clone = identity_of(database)
            if clone_identity is None and resource in runtime.resources.get('index_handoff_sql_clones', []) and observed_clone:
                require(len(observed_clone) == 1 and observed_clone[0]['owner'] == 'bigbrotr_migrator', 'unknown-create-owner-mismatch')
                resource['identity_after_unknown_create'] = observed_clone
            if clone_identity is not None or 'identity_after_unknown_create' in resource:
                final = runtime.rows(pg, database)
                require(len(json.dumps(final).encode()) <= 4 * 1024**2, 'clone-capture4MiB-ceiling')
                lab.write_json(output / 'clone-final.json', final)
        except BaseException as error:
            errors.append(dict(stage='final-observation', error=type(error).__name__))
        resource.update(status='retained', backend_absence=not errors)
        report['cleanup'] = dict(status='FAIL' if errors else 'PASS', errors=errors)
        cleanup_error = RuntimeError('handoff-sql-cleanup-incomplete') if errors and primary_error is None else None
        if cleanup_error is not None:
            report['status'] = 'FAIL'
        try:
            save()
        except BaseException as error:
            report['status'] = 'FAIL'
            first_error = primary_error if primary_error is not None else cleanup_error
            if first_error is None:
                raise
            first_error.add_note('Handoff SQL diagnostic persistence failed: ' + type(error).__name__)
        if cleanup_error is not None:
            raise cleanup_error
    return report


def psql(state, database, sql, *, role="bigbrotr_migrator", migration_sha=None):
    return lab.docker("exec", "-i", "--env",
        "PGOPTIONS=-c statement_timeout=30000 -c lock_timeout=5000 -c synchronous_commit=on",
        state["postgres"], "psql", "-X", "-A", "-t", "-q", "--no-password",
        "-U", role, "-d", database, "-v", "ON_ERROR_STOP=1", "-v", "VERBOSITY=sqlstate",
        *(["-v", "expected_migration_41_sha256=" + migration_sha] if migration_sha else []),
        data=sql, check=False)


def runtime_fixture(source):
    """Reuse exact fixture SQL bodies under invoker rights, without runtime TEMP.

    Only the fixture owner creates this separate namespace and eight helpers.
    Product relations receive only the maintained provision.sql grants. Negative
    attempts lacking a column privilege must return42501 before their row guard.
    """
    functions = re.findall(r"CREATE FUNCTION pg_temp\.(\w+)\(.*?\n\$\$;", source, re.S)
    require(functions == [value.split("(")[0] for value in FIXTURE_HELPERS], "closed-fixture-helpers-changed")
    definitions = re.findall(r"CREATE FUNCTION pg_temp\.\w+\(.*?\n\$\$;", source, re.S)
    program = source
    for definition in definitions:
        require(program.count(definition) == 1, "fixture-helper-not-unique")
        program = program.replace(definition, "")
    results = "CREATE TEMP TABLE results(case_name text PRIMARY KEY);"
    original = "CREATE TEMP TABLE original_prepared AS SELECT pg_temp.snapshot() AS original;"
    require(program.count(results) == program.count(original) == 1, "fixture-tables-changed")
    program = program.replace(results, "").replace(original,
        "INSERT INTO " + FIXTURE_SCHEMA + ".original_prepared SELECT pg_temp.snapshot();")
    program = program.replace("FROM original_prepared", "FROM " + FIXTURE_SCHEMA + ".original_prepared")
    for name in PRIVILEGE_REFUSALS:
        before = "'" + name + "','23514'"
        require(program.count(before) == 1, "fixture-refusal-changed")
        program = program.replace(before, "'" + name + "','42501'")
    program = program.replace("pg_temp.", FIXTURE_SCHEMA + ".")
    require("CREATE " not in program and "pg_temp." not in program, "runtime-fixture-still-owns-ddl")
    setup = "CREATE SCHEMA " + FIXTURE_SCHEMA + ";\n"
    setup += "CREATE TABLE " + FIXTURE_SCHEMA + ".results(case_name text PRIMARY KEY);\n"
    setup += "CREATE TABLE " + FIXTURE_SCHEMA + ".original_prepared(original jsonb NOT NULL);\n"
    setup += "\n".join(definitions).replace("pg_temp.", FIXTURE_SCHEMA + ".") + "\n"
    setup += "REVOKE ALL ON SCHEMA " + FIXTURE_SCHEMA + " FROM PUBLIC;\n"
    setup += "GRANT USAGE ON SCHEMA " + FIXTURE_SCHEMA + " TO " + ROLE + ";\n"
    setup += "GRANT SELECT,INSERT ON " + FIXTURE_SCHEMA + ".results," + FIXTURE_SCHEMA + ".original_prepared TO " + ROLE + ";\n"
    helpers = ",".join(FIXTURE_SCHEMA + "." + signature for signature in FIXTURE_HELPERS)
    setup += "REVOKE ALL ON FUNCTION " + helpers + " FROM PUBLIC;\n"
    setup += "GRANT EXECUTE ON FUNCTION " + helpers + " TO " + ROLE + ";\n"
    return setup, program


def source_order(state, database, name, prepare_first, report):
    sessions = []
    entry = dict(name=name, database=database, role=ROLE, status="RUNNING", commands=[])
    report["cases"].append(entry)
    original_state = lab.load(Path(state["root"]))
    container_ids = {key: json.loads(lab.docker("inspect", state[key]).stdout)[0]["Id"]
                     for key in ("postgres", "client")}

    def owned():
        require(lab.load(Path(state["root"])) == original_state, "owned-lab-changed")
        require(all(json.loads(lab.docker("inspect", state[key]).stdout)[0]["Id"] == value
                    for key, value in container_ids.items()), "owned-container-changed")

    def execute(session, sql, label):
        answer = session.execute(sql)
        entry["commands"].append(dict(label=label, sql=sql, answer=answer))
        return answer

    def snapshot():
        return json.loads(lab.sql(state, database, "SELECT " + FIXTURE_SCHEMA + ".snapshot();"))

    close = f"""DO $close$ BEGIN
UPDATE control.source_streams SET state='closing' WHERE dataset_id={identity(1)} AND shard_id=7 AND owner_epoch=1;
UPDATE control.source_streams SET state='closed',closed_at_us=10,last_complete_sequence=5,closure_digest={identity(930,32)}
 WHERE dataset_id={identity(1)} AND shard_id=7 AND owner_epoch=1;
END $close$;"""
    prepare = "DO $prepare$ BEGIN PERFORM " + FIXTURE_SCHEMA + ".prepare(3,5,6); SET CONSTRAINTS ALL IMMEDIATE; END $prepare$;"
    try:
        owner = Session(state, database, name + "_owner", sessions, role=ROLE)
        closer = Session(state, database, name + "_closer", sessions, role=ROLE)
        entry["backends"] = [owner.backend, closer.backend]
        races.claim_control(owner)
        races.transfer_index(owner, 3)
        execute(owner, "SELECT " + FIXTURE_SCHEMA + ".publish(2);", "publish-original-pending")
        if prepare_first:
            execute(owner, "BEGIN;", "begin-prepare")
            execute(owner, prepare, "prepare-before-close")
            closer.start(close)
            entry["commands"].append(dict(label="waiting-close", sql=close))
            entry["barrier"] = wait_blocked(state, database, closer, owner)
            execute(owner, "COMMIT;", "commit-prepare")
            before = snapshot()
            entry["refusal"] = closer.expect(closer.finish(), "23514")
            expected_plans, expected_chunks, expected_state, expected_bound = 3, 30, "open", None
        else:
            execute(closer, "BEGIN;", "begin-close")
            execute(closer, close, "close-before-prepare")
            owner.start(prepare)
            entry["commands"].append(dict(label="waiting-prepare", sql=prepare))
            entry["barrier"] = wait_blocked(state, database, owner, closer)
            execute(closer, "COMMIT;", "commit-close")
            before = snapshot()
            entry["refusal"] = owner.expect(owner.finish(), "23514")
            expected_plans, expected_chunks, expected_state, expected_bound = 2, 20, "closed", 5
        after = snapshot()
        require(after == before, "source-order-refusal-changed-recorded-state")
        stream, = [row for row in after["source_streams"] if row["shard_id"] == 7]
        require(len(after["projection_plans"]) == expected_plans
                and len(after["projection_chunks"]) == expected_chunks
                and after["consumers"][0]["current_revision"] == 2
                and stream["state"] == expected_state and stream["last_complete_sequence"] == expected_bound,
                "source-order-independent-outcome-mismatch")
        entry.update(status="PASS", source=stream, plans=expected_plans, chunks=expected_chunks,
                     unchanged_snapshot_sha256=hashlib.sha256(json.dumps(after, sort_keys=True).encode()).hexdigest())
    finally:
        errors = []
        for session in sessions:
            try:
                session.close(owned)
            except BaseException as error:
                errors.append(type(error).__name__)
        entry["cleanup"] = dict(status="FAIL" if errors else "PASS", errors=errors)
        require(not errors, "source-order-backend-cleanup-incomplete")


def run(root, name, sql_only):
    state = lab.load(root)
    output = root / (name + "-index-runtime.json")
    with output.open("x", encoding="utf-8"):
        os.chmod(output, 0o600)
    commands = root / (name + "-commands")
    commands.mkdir(mode=0o700)
    report = dict(format="bigbrotr-index-publication-runtime/1", status="RUNNING", cases=[],
        mode="direct-sql-diagnostic" if sql_only else "product-migration-cli",
        source_revision=lab.command(["git", "-C", str(SOURCE), "rev-parse", "HEAD"]).stdout.strip(),
        source_sha256=sha(Path(__file__)),
        not_run=["Rust Index API and private physical proofs", "ClickHouse verification", "lost PG COMMIT"])
    if sql_only:
        report["not_run"].append("product schema41 migration CLI qualification")
    original_command = lab.command
    serial = 0
    container_ids = {key: json.loads(lab.docker("inspect", state[key]).stdout)[0]["Id"]
                     for key in ("postgres", "client")}
    report["container_ids"] = container_ids

    def owned():
        require(lab.load(root) == state, "owned-lab-changed")
        require(all(json.loads(lab.docker("inspect", state[key]).stdout)[0]["Id"] == value
                    for key, value in container_ids.items()), "owned-container-changed")

    def disk_bound():
        size = int(original_command(["du", "-sb", str(root)]).stdout.split()[0])
        require(size < 2 * 1024**3, "diagnostic-disk-envelope-exceeded")
        report["retained_bytes"] = size

    def recorded(args, *, data=None, timeout=90, check=True):
        nonlocal serial
        serial += 1
        prefix = commands / f"{serial:04}"
        receipt = dict(argv=args, timeout_seconds=timeout,
                       stdin_sha256=hashlib.sha256(data.encode()).hexdigest() if data else None)
        # No password SQL is generated by this runner; bootstrap secrets stay in
        # the setup owner's private files. Retain every exact diagnostic input.
        if data is not None:
            prefix.with_suffix(".stdin").write_text(data)
        try:
            result = original_command(args, data=data, timeout=timeout, check=False)
        except BaseException as error:
            receipt.update(status="UNKNOWN", error=type(error).__name__)
            lab.write_json(prefix.with_suffix(".json"), receipt)
            raise
        prefix.with_suffix(".stdout").write_text(result.stdout)
        prefix.with_suffix(".stderr").write_text(result.stderr)
        receipt.update(status="JOINED", exit_code=result.returncode)
        lab.write_json(prefix.with_suffix(".json"), receipt)
        require(not check or result.returncode == 0, "diagnostic-command-failed-" + prefix.name)
        return result

    lab.command = recorded

    def save():
        lab.write_json(output, report)

    def require_command(result, label):
        require(result.returncode == 0, label)

    def migrate(database):
        owned()
        disk_bound()
        lab.sql(state, "postgres", "CREATE DATABASE " + database + " OWNER bigbrotr_migrator;")
        lab.sql(state, "postgres", "REVOKE ALL ON DATABASE " + database + " FROM PUBLIC;")
        if sql_only:
            sql = "BEGIN; SET LOCAL search_path=pg_catalog; SET LOCAL synchronous_commit=on;\n"
            sql += "SELECT pg_advisory_xact_lock(1111651442,1296648018);\n"
            for migration in manifest["application_order"]:
                path = SOURCE / "migrations/postgres" / migration["name"]
                sql += path.read_text() + "\n"
                sql += ("INSERT INTO control.schema_versions(engine,migration,name,checksum,reader_min,reader_max,writer_floor,applied_at_us,applied_by) VALUES"
                    "('postgres-control'," + str(migration["version"]) + ",'" + migration["name"] + "',decode('" + sha(path)
                    + "','hex'),1,1,1,(EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint,CURRENT_USER);\n")
            require_command(psql(state, database, sql + "COMMIT;"), "direct-sql-migration-failed")
        else:
            result = lab.migrate(state, "apply", lab.config(root, state, database), target=41)
            require_command(result, "product-migration-failed")
            require(json.loads(result.stdout)["target_version"] == "41", "wrong-product-migration-target")
        observed = json.loads(lab.sql(state, database,
            "SELECT json_agg(json_build_object('version',migration,'name',name,'sha256',encode(checksum,'hex')) ORDER BY migration) FROM control.schema_versions;"))
        expected = [dict(entry, sha256=sha(SOURCE / "migrations/postgres" / entry["name"]))
                    for entry in manifest["application_order"]]
        require(observed == sorted(expected, key=lambda row: row["version"]), "exact-migration-ledger-mismatch")
        report["cases"].append(dict(name="migration_" + database, status="PASS", mode=report["mode"], ledger=observed))
        disk_bound()
        save()

    def fixture(database, runtime):
        migrate(database)
        program = fixture_source
        role = "bigbrotr_migrator"
        if runtime:
            control = provision.split("\\if :control\n", 1)[1].split("\\endif", 1)[0]
            control = control.replace("\\connect bigbrotr_control", "\\connect " + database).replace(
                "GRANT CONNECT ON DATABASE bigbrotr_control TO", "GRANT CONNECT ON DATABASE " + database + " TO")
            require_command(psql(state, "postgres", control, role="postgres"), "runtime-provision-failed")
            setup, program = runtime_fixture(fixture_source)
            require_command(psql(state, database, setup), "runtime-fixture-helper-setup-failed")
            role = ROLE
        result = psql(state, database, program, role=role, migration_sha=migration_sha)
        require_command(result, "fixture-failed-" + database)
        summary, = [json.loads(line) for line in result.stdout.splitlines()
                    if line.startswith('{"') and "bigbrotr-index-publication-sql-cases/1" in line]
        require(summary["status"] == "PASS" and summary["case_count"] == 31
                and all(case in summary["cases"] for case in
                    ("legacy_wrong_generation_owner_at_41", "legacy_same_generation_owner_at_41",
                     "old_plan_published_by_current_owner", "new_chunks_keep_new_origin")), "fixture-cases-incomplete")
        report["cases"].append(dict(name="fixture_" + database, status="PASS", role=role, summary=summary,
            fixture_namespace_extra_grants="USAGE; SELECT/INSERT2 fixture tables; EXECUTE8 invoker fixture helpers" if runtime else None,
            runtime_temp_grant=False, privilege_refusals=list(PRIVILEGE_REFUSALS) if runtime else []))
        disk_bound()
        save()

    try:
        require(not original_command(["git", "-C", str(SOURCE), "status", "--porcelain"]).stdout, "clean-source-required")
        require(lab.sql(state, "postgres", "SELECT count(*) FROM pg_database WHERE NOT datistemplate AND datname<>'postgres';") == "0",
                "fresh-owned-lab-required")
        manifest = json.loads((SOURCE / "migrations/manifest.json").read_text())
        require(len(manifest["application_order"]) == 57 and manifest["application_order"][-1]["version"] == 41,
                "exact-schema41-catalog-required")
        fixture_source = (SOURCE / "experiments/control-migrations/index_publication_cases.sql").read_text()
        provision = (SOURCE / "packaging/compose/provision.sql").read_text()
        migration_sha = sha(SOURCE / "migrations/postgres/0041_index_publication.sql")
        report["inputs"] = {relative: sha(SOURCE / relative) for relative in
            ("migrations/manifest.json", "migrations/postgres/0041_index_publication.sql",
             "packaging/compose/provision.sql", "experiments/control-migrations/index_publication_cases.sql",
             "experiments/control-migrations/index_publication_races.py", "experiments/control-migrations/concurrency_cases.py")}
        lab.sql(state, "postgres", "ALTER ROLE bigbrotr_control NOINHERIT NOREPLICATION NOBYPASSRLS;")
        fixture("bb_index41_owner", False)
        fixture("bigbrotr_control", True)
        sessions = []
        try:
            app = Session(state, "bigbrotr_control", name + "_privileges", sessions, role=ROLE)
            role_check = app.execute("SELECT current_user,session_user,has_database_privilege(current_database(),'TEMP'),has_schema_privilege('control','CREATE');")
            require(role_check["lines"] == ["bigbrotr_control|bigbrotr_control|f|f"], "runtime-identity-or-privileges-wrong")
            refusals = []
            for sql in ("CREATE TABLE control.runtime_owned(id integer);", "CREATE TEMP TABLE runtime_temporary(id integer);",
                        "TRUNCATE control.projection_plans;", "UPDATE control.schema_versions SET applied_at_us=0;",
                        "UPDATE control.projection_generations SET created_at_us=0;", "SELECT control.guard_index_owner_session();"):
                refusals.append(dict(sql=sql, result=app.execute(sql, expected="42501")))
            report["cases"].append(dict(name="runtime_forbidden_privileges", status="PASS", identity=role_check, refusals=refusals))
        finally:
            for session in sessions:
                session.close(owned)
        save()
        for database, prepare_first in (("bb_index41_prepare_first", True), ("bb_index41_close_first", False)):
            fixture(database, True)
            source_order(state, database, "prepare_first" if prepare_first else "close_first", prepare_first, report)
            save()
        races.run(root, "bigbrotr_control", name + "_expiry", ROLE)
        report["cases"].append(dict(name="post_wait_expiry", status="PASS", receipt=str(root / (name + "_expiry-index-races.json"))))
        disk_bound()
        report["status"] = "PASS"
    except BaseException as error:
        report.update(status="FAIL", failure_class=type(error).__name__, failure=str(error))
        raise
    finally:
        save()
        try:
            owned()
            for container in (state["client"], state["postgres"]):
                lab.docker("logs", container)
            lab.stop(root)
            report["cleanup"] = "stopped-retained"
        except BaseException as error:
            report.update(status="INCOMPLETE", cleanup=type(error).__name__)
            raise
        finally:
            lab.command = original_command
            save()
    print(json.dumps(dict(status=report["status"], mode=report["mode"], receipt=str(output))))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", required=True, type=Path)
    parser.add_argument("--name", required=True)
    parser.add_argument("--sql-only-diagnostic", action="store_true")
    args = parser.parse_args()
    if not args.root.is_absolute() or not re.fullmatch(r"[a-z][a-z0-9_]{0,15}", args.name):
        parser.error("absolute owned root and bounded diagnostic name required")
    run(args.root.resolve(), args.name, args.sql_only_diagnostic)
