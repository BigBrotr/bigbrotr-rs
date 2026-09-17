#!/usr/bin/env python3
"""Actual 0002 migration/constraint cases in an already owned, isolated PG lab."""
import argparse
import hashlib
import json
from pathlib import Path
import re

import lab


INGEST_TABLES = ["allocation_domains", "source_streams", "producer_slots", "producer_epochs",
                 "producer_checkpoints", "consumers", "consumer_frontier_revisions", "consumer_frontiers",
                 "projection_generations", "projection_chunks", "projection_chunk_bounds",
                 "snapshot_leases", "retention_claims"]
CONTROL_TABLES = ["schema_versions", "datasets", "config_revisions", "policy_revisions", "role_instances",
                  "owner_epochs", "topology_generations", "topology_streams", "jobs", "job_input_bounds"]


def fixed(value, size=16):
    return "\\x" + value.to_bytes(size, "big").hex()


def expected_ingest_rows():
    """Independent literal final rows, including every column of the 13 new tables."""
    dataset = {"dataset_id": fixed(1)}
    owner = dict(dataset, owner_resource_id=fixed(30), owner_epoch=1,
                 owner_role="ingest", owner_incarnation=fixed(31))
    expected = {
        "allocation_domains": [dict(owner, dictionary_kind=8, allocation_domain=fixed(60),
            created_at_us=0, state="closed", closed_at_us=5, closure_digest=fixed(83, 32))],
        "source_streams": [dict(owner, shard_id=0, opened_at_us=0, state="open", closed_at_us=None,
            last_complete_sequence=None, closure_digest=None),
            dict(owner, shard_id=7, opened_at_us=0, state="closed", closed_at_us=2,
                 last_complete_sequence=6, closure_digest=fixed(80, 32))],
        "producer_slots": [dict(dataset, producer_slot_id=fixed(61), registered_by_resource_id=fixed(20),
            registered_by_epoch=1, registered_by_role="control", registered_by_incarnation=fixed(21), created_at_us=0)],
        "producer_epochs": [dict(owner, producer_slot_id=fixed(61), producer_epoch=epoch,
            producer_role_instance_id=fixed(12), producer_role="discoverer", producer_incarnation=fixed(incarnation),
            outstanding_request_limit=2, opened_at_us=opened, state=state, retired_at_us=retired,
            closure_digest=digest) for epoch, incarnation, opened, state, retired, digest in
            [(1, 62, 0, "retired", 3, fixed(81, 32)), (2, 63, 4, "open", None, None)]],
        "producer_checkpoints": [dict(dataset, producer_slot_id=fixed(61), producer_epoch=1,
            checkpoint_revision=2, complete_through_request=2, checkpoint_digest=fixed(62, 32), committed_at_us=2)],
        "consumers": [dict(dataset, consumer_id=fixed(70), consumer_role="archive", created_at_us=0,
            current_revision=3, current_topology_generation=0),
            dict(dataset, consumer_id=fixed(71), consumer_role="index", created_at_us=0,
                 current_revision=None, current_topology_generation=None)],
        "consumer_frontier_revisions": [dict(dataset, consumer_id=fixed(70), frontier_revision=revision,
            consumer_role="archive", topology_generation=0, previous_revision=previous,
            owner_resource_id=fixed(40), owner_epoch=1, owner_incarnation=fixed(41), created_at_us=revision-1,
            sealed=True) for revision, previous in [(1, None), (2, 1), (3, 2)]],
        "consumer_frontiers": [dict(dataset, consumer_id=fixed(70), frontier_revision=revision,
            topology_generation=0, shard_id=shard, owner_epoch=1, last_complete_sequence=sequence,
            attestation_kind="archive-batch" if sequence else None,
            attestation_digest=fixed(digest, 32) if sequence else None)
            for revision, shard, sequence, digest in [(1, 0, 0, 0), (1, 7, 5, 71),
                (2, 0, 0, 0), (2, 7, 6, 76), (3, 0, 0, 0), (3, 7, 6, 78)]],
        "projection_generations": [dict(dataset, generation_id=fixed(generation), storage_incarnation=fixed(storage),
            table_identity_digest=fixed(table, 32), owner_resource_id=fixed(resource), owner_epoch=1,
            owner_role="index", owner_incarnation=fixed(incarnation), created_at_us=0)
            for generation, storage, table, resource, incarnation in [(72, 73, 72, 50, 51), (74, 75, 79, 52, 53)]],
        "projection_chunks": [dict(dataset, generation_id=fixed(generation), chunk_id=fixed(chunk, 32),
            storage_incarnation=fixed(storage), table_identity_digest=fixed(table, 32), target=target,
            target_schema_version=1, topology_generation=0, chunk_ordinal=0, row_count=0,
            owner_resource_id=fixed(resource), owner_epoch=1, owner_role="index", owner_incarnation=fixed(incarnation),
            prepared_at_us=0, state=state, committed_at_us=committed, engine_receipt_digest=receipt, invalid_reason=reason)
            for generation, chunk, storage, table, target, resource, incarnation, state, committed, receipt, reason in
            [(72, 73, 73, 72, "events", 50, 51, "committed", 1, fixed(74, 32), None),
             (74, 79, 75, 79, "event_tags", 52, 53, "invalid", None, None, "drained")]],
        "projection_chunk_bounds": [dict(dataset, generation_id=fixed(72), chunk_id=fixed(73, 32),
            topology_generation=0, shard_id=shard, owner_epoch=1, last_complete_sequence=sequence)
            for shard, sequence in [(0, 0), (7, 5)]],
        "snapshot_leases": [dict(dataset, lease_id=fixed(80), consumer_id=fixed(70), frontier_revision=1,
            topology_generation=0, holder_kind="role_instance", holder_job_id=None, holder_role_instance_id=fixed(13),
            holder_role="archive", holder_incarnation=fixed(81), lease_revision=3, acquired_at_us=0, updated_at_us=400,
            expires_at_us=300, state="released", released_at_us=400, release_reason="completed")],
        "retention_claims": [dict(dataset, claim_id=fixed(82), lease_id=fixed(80), topology_generation=0,
            shard_id=7, owner_epoch=1, first_sequence=1, last_sequence=5, purpose="backup", created_at_us=1,
            state="released", released_at_us=401, release_reason="completed")],
    }
    return expected


def rows(state, database, tables):
    # Table identifiers come only from the closed source lists above.
    return {table: json.loads(lab.sql(state, database,
        f"SELECT coalesce(jsonb_agg(to_jsonb(t)), '[]'::jsonb) FROM control.{table} t;")) for table in tables}


def normalize(value):
    return {table: sorted(items, key=lambda row: json.dumps(row, sort_keys=True)) for table, items in value.items()}


def run(root, name):
    state = lab.load(root)
    source = Path(__file__).resolve().parents[2]
    if lab.command(["git", "-C", str(source), "status", "--porcelain", "--untracked-files=all"]).stdout:
        raise RuntimeError("ingest harness source must be clean")
    output = root / (name + "-ingest.json")
    if output.exists():
        raise RuntimeError("refusing to overwrite ingest evidence")
    mounted_binary = lab.docker("exec", state["client"], "sha256sum", "/build/bigbrotr").stdout.split()[0]
    mounted_receipt = json.loads(lab.docker("exec", state["client"], "cat", "/build/build-result.json").stdout)
    mounted_manifest = lab.docker("exec", state["client"], "sha256sum", "/build/source-manifest.json").stdout.split()[0]
    actual_info = lab.cli(state, "build-info")
    if (mounted_binary != state["binary_sha256"] or mounted_binary != mounted_receipt["binary_sha256"]
            or mounted_receipt != state["build"]
            or mounted_manifest != mounted_receipt["source_manifest_sha256"]
            or actual_info.returncode != 0 or json.loads(actual_info.stdout) != mounted_receipt["build_info"]):
        raise RuntimeError("mounted executable, build identity or source manifest differs from the owned artifact receipt")
    checksums = {str(i): hashlib.sha256((source / "migrations/postgres" / filename).read_bytes()).hexdigest()
                 for i, filename in [(1, "0001_control.sql"), (2, "0002_ingest.sql")]}
    report = {"format": "bigbrotr-ingest-schema-cases/1", "status": "RUNNING", "cases": [],
        "source": state["build"]["build_info"]["source"], "binary_sha256": state["binary_sha256"],
        "harness_revision": lab.command(["git", "-C", str(source), "rev-parse", "HEAD"]).stdout.strip(),
        "harness_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "migration_sha256": checksums, "server": state["server"],
        "not_run": ["real external-engine receipts", "native writer exclusion", "new relational races", "ARM PG"]}

    def record(case, condition, observed):
        report["cases"].append({"name": case, "status": "PASS" if condition else "FAIL", "observed": observed})
        lab.write_json(output, report)
        if not condition:
            raise RuntimeError("ingest acceptance failed: " + case)

    def create(suffix):
        database = "bb_" + name + "_" + suffix
        lab.sql(state, "postgres", f"CREATE DATABASE {database} OWNER bigbrotr_migrator;")
        return database, lab.config(root, state, database)

    def migration(database, path, action, target, expected_status, count, prior=None):
        result = lab.migrate(state, action, path, target=target)
        value = json.loads(result.stdout) if result.stdout else {}
        resolved_target = 2 if target is None else target
        current = resolved_target if count else prior
        expected = {"format": "bigbrotr-control-migration/1", "scope": "postgres-control",
            "status": expected_status, "source_version": str(prior) if prior is not None else None,
            "current_version": str(current) if current is not None else None,
            "target_version": str(resolved_target), "applied_count": str(count),
            "steps": [{"version": str(version), "name": filename, "sha256": checksums[str(version)]}
                      for version, filename in [(1, "0001_control.sql"), (2, "0002_ingest.sql")]
                      if (prior or 0) < version <= resolved_target],
            "dataset_initialization": "not-performed", "release_upgrade_qualification": "not-performed"}
        record(database + "_" + action + "_" + str(target), result.returncode == 0
               and value == expected and not result.stderr,
               {"exit": result.returncode, "output": value, "expected": expected, "stderr": result.stderr})
        return value

    def exact_receipts(database):
        ledger = rows(state, database, ["schema_versions"])["schema_versions"]
        expected = {(1, "0001_control.sql", "\\x" + checksums["1"]),
                    (2, "0002_ingest.sql", "\\x" + checksums["2"])}
        return len(ledger) == 2 and {(r["migration"], r["name"], r["checksum"]) for r in ledger} == expected \
            and all(r["engine"] == "postgres-control" and r["reader_min"] == r["reader_max"] == r["writer_floor"] == 1
                    and r["applied_by"] == "bigbrotr_migrator" for r in ledger)

    try:
        database, path = create("fresh")
        planned = migration(database, path, "plan", None, "planned", 0)
        record("latest_default_is_two_without_namespace", planned.get("target_version") == "2"
               and [step["version"] for step in planned.get("steps", [])] == ["1", "2"]
               and lab.sql(state, database, "SELECT count(*) FROM pg_namespace WHERE nspname='control';") == "0", planned)
        migration(database, path, "apply", None, "applied", 2)
        actual_tables = json.loads(lab.sql(state, database,
            "SELECT json_agg(tablename ORDER BY tablename) FROM pg_tables WHERE schemaname='control';"))
        empty = rows(state, database, [t for t in CONTROL_TABLES + INGEST_TABLES if t != "schema_versions"])
        record("two_exact_receipts_and_no_implicit_app_data", exact_receipts(database)
               and actual_tables == sorted(CONTROL_TABLES + INGEST_TABLES)
               and all(not contents for contents in empty.values()), {"tables": actual_tables, "empty_rows": empty})
        before = lab.schema_digest(state, database)
        migration(database, path, "apply", 2, "current", 0, prior=2)
        record("reapply_preserves_schema_and_data", before == lab.schema_digest(state, database), {"before": before})
        fixture = source / "experiments/control-migrations/ingest_schema_cases.sql"
        result = lab.docker("exec", "-i", state["postgres"], "psql", "-X", "-A", "-t", "-q", "--no-password",
            "-U", "bigbrotr_migrator", "-d", database, "-v", "ON_ERROR_STOP=1",
            "-v", "expected_migration_1_sha256=" + checksums["1"],
            "-v", "expected_migration_2_sha256=" + checksums["2"], data=fixture.read_text(), check=False)
        (root / (name + "-ingest.stdout")).write_text(result.stdout)
        (root / (name + "-ingest.stderr")).write_text(result.stderr)
        summaries = [json.loads(line) for line in result.stdout.splitlines() if line.startswith('{"')]
        summary = summaries[-1] if summaries else {}
        record("independent_constraint_fixture", result.returncode == 0 and summary.get("result") == "PASS"
               and summary.get("executed") == 74 and all(summary.get(k) == 0 for k in ["failed", "skipped", "not_run"]),
               {"exit": result.returncode, "summary": summary, "fixture_sha256": hashlib.sha256(fixture.read_bytes()).hexdigest()})
        observed = rows(state, database, INGEST_TABLES)
        expected = expected_ingest_rows()
        lab.write_json(root / (name + "-ingest-observed.json"), observed)
        lab.write_json(root / (name + "-ingest-expected.json"), expected)
        record("external_exact_rows_all_thirteen_ingest_relations", normalize(observed) == normalize(expected)
               and exact_receipts(database), {"row_counts": {table: len(values) for table, values in observed.items()}})

        database, path = create("eligible")
        migration(database, path, "apply", 1, "applied", 1)
        lab.sql(state, database, "INSERT INTO control.datasets(dataset_id,instance_id,created_at_us) VALUES "
            "(decode(lpad('2a',32,'0'),'hex'),decode(lpad('2b',32,'0'),'hex'),-2);"
            "INSERT INTO control.role_instances VALUES (decode(lpad('2c',32,'0'),'hex'),"
            "decode(lpad('2a',32,'0'),'hex'),'control',-1,NULL);")
        retained = rows(state, database, CONTROL_TABLES)
        migration(database, path, "apply", 2, "applied", 1, prior=1)
        after = rows(state, database, CONTROL_TABLES)
        old_receipt = retained.pop("schema_versions")[0]
        receipts = after.pop("schema_versions")
        record("eligible_populated_upgrade_preserves_existing_rows", normalize(retained) == normalize(after)
               and old_receipt in receipts and exact_receipts(database), {"old_receipt_unchanged": old_receipt in receipts})

        database, path = create("unregistered")
        migration(database, path, "apply", 1, "applied", 1)
        lab.sql(state, database, "BEGIN; INSERT INTO control.datasets(dataset_id,instance_id,created_at_us) VALUES "
            "(decode(lpad('2a',32,'0'),'hex'),decode(lpad('2b',32,'0'),'hex'),0);"
            "INSERT INTO control.topology_generations VALUES (decode(lpad('2a',32,'0'),'hex'),0,NULL,0,false);"
            "INSERT INTO control.topology_streams VALUES (decode(lpad('2a',32,'0'),'hex'),0,0,1,NULL);"
            "UPDATE control.topology_generations SET sealed=true; COMMIT;")
        before = lab.schema_digest(state, database)
        refusal = lab.migrate(state, "apply", path, target=2)
        after = lab.schema_digest(state, database)
        count = lab.sql(state, database, "SELECT count(*) FROM pg_tables WHERE schemaname='control';")
        record("populated_topology_refused_atomically", refusal.returncode == 1 and not refusal.stdout
               and "requires explicit verified source registration" in refusal.stderr and before == after and count == "10",
               {"exit": refusal.returncode, "stderr": refusal.stderr, "before_sha256": before,
                "after_sha256": after, "table_count": count})
        migration(database, path, "inspect", 1, "inspected", 0, prior=1)
        report["status"] = "PASS"
    except Exception:
        report["status"] = "FAIL"
        raise
    finally:
        lab.write_json(output, report)
    print(json.dumps({"status": report["status"], "cases": len(report["cases"]), "output": str(output)}))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--run-name", default="ingest")
    args = parser.parse_args()
    if not args.root.is_absolute() or not re.fullmatch(r"[a-z_]{1,20}", args.run_name):
        parser.error("absolute owned root and bounded lowercase run name required")
    run(args.root, args.run_name)
