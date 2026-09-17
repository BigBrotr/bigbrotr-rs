#!/usr/bin/env python3
"""Finite real PostgreSQL 0002 ingest transaction races in an owned laboratory."""
import argparse
import copy
import fcntl
import json
import os
from pathlib import Path
import re
import stat
import time

import lab
from concurrency_cases import Incomplete, Mismatch, Session, bytea, identity, require, sha, wait_blocked


ROLE = "bigbrotr_migrator"
TABLES = (
    "schema_versions", "datasets", "config_revisions", "policy_revisions",
    "role_instances", "owner_epochs", "topology_generations", "topology_streams",
    "jobs", "job_input_bounds", "allocation_domains", "source_streams",
    "producer_slots", "producer_epochs", "producer_checkpoints", "consumers",
    "consumer_frontier_revisions", "consumer_frontiers", "projection_generations",
    "projection_chunks", "projection_chunk_bounds", "snapshot_leases", "retention_claims",
)
CASES = (
    "frontier_seal_before_lower_close",
    "lower_close_before_chunk_commit",
    "close_before_mismatched_handoff",
    "drain_before_new_chunk",
    "chunk_complete_before_release",
    "frontier_publish_cas",
    "claim_before_lease_release",
    "lease_release_before_claim",
)


def fixture_attestations():
    return "synthetic-unvalidated"


def migration_args(path, action="apply"):
    return ["migrate", action, "--file", path, "--user", ROLE,
            "--password-ref", "file:/lab/secrets/migrator", "--target", "2"]


def digest(n):
    return "\\x" + format(n, "064x")


def migration_result(result, action, checksums):
    applying = action == "apply"
    expected = {
        "format": "bigbrotr-control-migration/1", "scope": "postgres-control",
        "status": "applied" if applying else "inspected",
        "source_version": None if applying else "2", "current_version": "2",
        "target_version": "2", "applied_count": "2" if applying else "0",
        "steps": [{"version": str(version), "name": filename,
                   "sha256": checksums[f"{version:04d}"]}
                  for version, filename in ((1, "0001_control.sql"), (2, "0002_ingest.sql"))]
        if applying else [],
        "dataset_initialization": "not-performed", "release_upgrade_qualification": "not-performed",
    }
    require(result.returncode == 0 and not result.stderr
            and json.loads(result.stdout) == expected, "real-cli-target-2-" + action)


def snapshot(state, database):
    parts = [
        f"'{table}',(SELECT coalesce(jsonb_agg(v ORDER BY v::text COLLATE \"C\"),'[]'::jsonb) "
        f"FROM (SELECT to_jsonb(t) v FROM control.{table} t) s)"
        for table in TABLES
    ]
    return json.loads(lab.sql(
        state, database, "SELECT jsonb_build_object(" + ",".join(parts) + ");"))


def normalized(rows):
    return {
        key: sorted(value, key=lambda row: json.dumps(row, sort_keys=True))
        for key, value in rows.items()
    }


def exact_rows(state, database, expected):
    actual = snapshot(state, database)
    require(normalized(actual) == normalized(expected), "external-durable-rows-mismatch")
    return actual


def find_row(rows, table, **keys):
    found = [row for row in rows[table]
             if all(row.get(key) == value for key, value in keys.items())]
    require(len(found) == 1, "fixture-row-identity")
    return found[0]


def changed(before, table, keys, **values):
    expected = copy.deepcopy(before)
    find_row(expected, table, **keys).update(values)
    return expected


def appended(before, table, *rows):
    expected = copy.deepcopy(before)
    expected[table].extend(copy.deepcopy(rows))
    return expected


def frontier_bound(consumer, revision, shard, sequence, receipt):
    return {
        "consumer_id": bytea(consumer), "frontier_revision": revision,
        "dataset_id": bytea(1), "topology_generation": 0, "shard_id": shard,
        "owner_epoch": 1, "last_complete_sequence": sequence,
        "attestation_kind": "archive-batch" if sequence else None,
        "attestation_digest": digest(receipt) if sequence else None,
    }


def frontier_values(consumer, revision, sequences, first_receipt):
    values = []
    for shard in range(1, 5):
        sequence = sequences.get(shard, 0)
        if sequence:
            values.append(
                f"({identity(consumer)},{revision},{identity(1)},0,{shard},1,{sequence},"
                f"'archive-batch',{identity(first_receipt + shard, 32)})")
        else:
            values.append(
                f"({identity(consumer)},{revision},{identity(1)},0,{shard},1,0,NULL,NULL)")
    return ",".join(values)


def projection_bounds(generation, chunk, sequences):
    return ",".join(
        f"({identity(generation)},{identity(chunk, 32)},{identity(1)},0,{shard},1,"
        f"{sequences.get(shard, 0)})"
        for shard in range(1, 5)
    )


def setup_fixture(session):
    session.execute("BEGIN;")
    session.execute(f"""
INSERT INTO control.datasets(dataset_id,instance_id,created_at_us)
VALUES({identity(1)},{identity(2)},0);
INSERT INTO control.config_revisions VALUES
(1,{identity(1)},NULL,1,'managed','{{}}',{identity(1,32)},{identity(2,32)},0,{identity(3)});
INSERT INTO control.policy_revisions VALUES
(1,{identity(1)},NULL,1,1,'{{}}',{identity(3,32)},0,{identity(3)});
INSERT INTO control.role_instances VALUES
({identity(11)},{identity(1)},'ingest',0,NULL),
({identity(13)},{identity(1)},'archive',0,NULL),
({identity(14)},{identity(1)},'index',0,NULL);
INSERT INTO control.owner_epochs VALUES
({identity(30)},1,{identity(1)},'ingest',{identity(11)},{identity(31)},0,-1,'active',NULL,NULL,NULL),
({identity(40)},1,{identity(1)},'archive',{identity(13)},{identity(41)},0,-1,'active',NULL,NULL,NULL),
({identity(42)},1,{identity(1)},'archive',{identity(13)},{identity(43)},0,-1,'active',NULL,NULL,NULL),
({identity(50)},1,{identity(1)},'index',{identity(14)},{identity(51)},0,-1,'active',NULL,NULL,NULL),
({identity(52)},1,{identity(1)},'index',{identity(14)},{identity(53)},0,-1,'active',NULL,NULL,NULL),
({identity(54)},1,{identity(1)},'index',{identity(14)},{identity(55)},0,-1,'active',NULL,NULL,NULL);""")
    session.execute(f"""
INSERT INTO control.topology_generations VALUES({identity(1)},0,NULL,0,false);
INSERT INTO control.topology_streams VALUES
({identity(1)},0,1,1,NULL),({identity(1)},0,2,1,NULL),
({identity(1)},0,3,1,NULL),({identity(1)},0,4,1,NULL);
INSERT INTO control.source_streams(dataset_id,shard_id,owner_epoch,owner_resource_id,
owner_incarnation,opened_at_us) VALUES
({identity(1)},1,1,{identity(30)},{identity(31)},0),
({identity(1)},2,1,{identity(30)},{identity(31)},0),
({identity(1)},3,1,{identity(30)},{identity(31)},0),
({identity(1)},4,1,{identity(30)},{identity(31)},0);
UPDATE control.topology_generations SET sealed=true WHERE generation=0;
UPDATE control.datasets SET config_revision=1,policy_revision=1,topology_generation=0;""")
    session.execute(f"""
INSERT INTO control.consumers VALUES({identity(70)},{identity(1)},'archive',0,NULL,NULL);
INSERT INTO control.consumer_frontier_revisions(consumer_id,frontier_revision,dataset_id,
consumer_role,topology_generation,previous_revision,owner_resource_id,owner_epoch,
owner_incarnation,created_at_us) VALUES
({identity(70)},1,{identity(1)},'archive',0,NULL,{identity(40)},1,{identity(41)},0);
INSERT INTO control.consumer_frontiers VALUES {frontier_values(70,1,{},200)};
UPDATE control.consumer_frontier_revisions SET sealed=true
WHERE consumer_id={identity(70)} AND frontier_revision=1;
UPDATE control.consumers SET current_revision=1,current_topology_generation=0
WHERE consumer_id={identity(70)};""")
    session.execute(f"""
INSERT INTO control.consumers VALUES({identity(71)},{identity(1)},'archive',0,NULL,NULL);
INSERT INTO control.consumer_frontier_revisions(consumer_id,frontier_revision,dataset_id,
consumer_role,topology_generation,previous_revision,owner_resource_id,owner_epoch,
owner_incarnation,created_at_us) VALUES
({identity(71)},1,{identity(1)},'archive',0,NULL,{identity(42)},1,{identity(43)},0);
INSERT INTO control.consumer_frontiers VALUES {frontier_values(71,1,{4:5},220)};
UPDATE control.consumer_frontier_revisions SET sealed=true
WHERE consumer_id={identity(71)} AND frontier_revision=1;
UPDATE control.consumers SET current_revision=1,current_topology_generation=0
WHERE consumer_id={identity(71)};
INSERT INTO control.consumer_frontier_revisions(consumer_id,frontier_revision,dataset_id,
consumer_role,topology_generation,previous_revision,owner_resource_id,owner_epoch,
owner_incarnation,created_at_us) VALUES
({identity(71)},2,{identity(1)},'archive',0,1,{identity(42)},1,{identity(43)},1);
INSERT INTO control.consumer_frontiers VALUES {frontier_values(71,2,{4:5},230)};
UPDATE control.consumer_frontier_revisions SET sealed=true
WHERE consumer_id={identity(71)} AND frontier_revision=2;""")
    session.execute(f"""
INSERT INTO control.projection_generations VALUES
({identity(80)},{identity(1)},{identity(81)},{identity(82,32)},{identity(50)},1,'index',{identity(51)},0),
({identity(84)},{identity(1)},{identity(85)},{identity(86,32)},{identity(52)},1,'index',{identity(53)},0),
({identity(88)},{identity(1)},{identity(89)},{identity(90,32)},{identity(54)},1,'index',{identity(55)},0);
INSERT INTO control.projection_chunks(generation_id,chunk_id,dataset_id,storage_incarnation,
table_identity_digest,target,target_schema_version,topology_generation,chunk_ordinal,row_count,
owner_resource_id,owner_epoch,owner_incarnation,prepared_at_us) VALUES
({identity(80)},{identity(83,32)},{identity(1)},{identity(81)},{identity(82,32)},'events',1,0,0,5,{identity(50)},1,{identity(51)},0),
({identity(88)},{identity(91,32)},{identity(1)},{identity(89)},{identity(90,32)},'events',1,0,0,0,{identity(54)},1,{identity(55)},0);
INSERT INTO control.projection_chunk_bounds VALUES {projection_bounds(80,83,{2:5})};
INSERT INTO control.projection_chunk_bounds VALUES {projection_bounds(88,91,{})};
UPDATE control.owner_epochs SET state='draining'
WHERE resource_id={identity(54)} AND epoch=1;""")
    session.execute(f"""
INSERT INTO control.snapshot_leases(lease_id,dataset_id,consumer_id,frontier_revision,
topology_generation,holder_kind,holder_role_instance_id,holder_role,holder_incarnation,
acquired_at_us,updated_at_us,expires_at_us) VALUES
({identity(100)},{identity(1)},{identity(71)},1,0,'role_instance',{identity(13)},'archive',{identity(41)},0,0,100),
({identity(101)},{identity(1)},{identity(71)},1,0,'role_instance',{identity(13)},'archive',{identity(41)},0,0,100);
COMMIT;""")


def fixture_summary(state, database):
    return json.loads(lab.sql(state, database, """SELECT json_build_object(
'schema_versions',(SELECT count(*) FROM control.schema_versions),
'sources',(SELECT json_object_agg(shard_id::text,state ORDER BY shard_id) FROM control.source_streams),
'topology_members',(SELECT count(*) FROM control.topology_streams),
'owners',(SELECT count(*) FROM control.owner_epochs),
'consumers',(SELECT count(*) FROM control.consumers),
'frontier_revisions',(SELECT count(*) FROM control.consumer_frontier_revisions),
'frontier_bounds',(SELECT count(*) FROM control.consumer_frontiers),
'projection_generations',(SELECT count(*) FROM control.projection_generations),
'projection_chunks',(SELECT count(*) FROM control.projection_chunks),
'projection_bounds',(SELECT count(*) FROM control.projection_chunk_bounds),
'snapshot_leases',(SELECT count(*) FROM control.snapshot_leases),
'retention_claims',(SELECT count(*) FROM control.retention_claims));"""))


def run(root, name):
    source = Path(__file__).resolve().parents[2]
    require(root.is_absolute() and root.resolve() == root
            and re.fullmatch(r"[a-z_]{1,20}", name), "invalid-run-root-or-name")
    require(Path(lab.__file__).resolve().parent == Path(__file__).resolve().parent,
            "foreign-lab-import")
    require(not lab.command(["git", "-C", str(source), "status", "--porcelain",
                             "--untracked-files=all"]).stdout, "dirty-source")
    state = lab.load(root)
    require(state["state"] == "running" and state["source"] == str(source),
            "lab-not-running-or-foreign-source")
    info = root.lstat()
    require(stat.S_ISDIR(info.st_mode) and info.st_uid == os.geteuid()
            and stat.S_IMODE(info.st_mode) == 0o700, "lab-root-not-private")
    lock = os.open(root / "ingest-concurrency-mutation.lock",
                   os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    directory = root / (name + "-ingest-concurrency")
    directory.mkdir(mode=0o700)
    output = directory / "result.json"
    database = "bb_" + name + "_ingest_concurrency"
    revision = lab.command(["git", "-C", str(source), "rev-parse", "HEAD"]).stdout.strip()
    application_source = state["build"]["build_info"]["source"]
    require(application_source.get("dirty") is False
            and application_source.get("revision") == revision, "application-source-mismatch")
    report = {
        "format": "bigbrotr-pg-ingest-concurrency/1", "status": "RUNNING",
        "started_at_unix": time.time(), "database": database,
        "binary_sha256": state["binary_sha256"], "application_source": application_source,
        "harness_revision": revision, "harness_sha256": sha(Path(__file__)),
        "shared_harness_sha256": sha(Path(__file__).with_name("concurrency_cases.py")),
        "lab_sha256": sha(Path(lab.__file__)),
        "migration_sha256": {
            "0001": sha(source / "migrations/postgres/0001_control.sql"),
            "0002": sha(source / "migrations/postgres/0002_ingest.sql"),
        },
        "fixture_attestations": fixture_attestations(),
        "cases": {case: {"status": "NOT_RUN"} for case in CASES},
        "sessions": [], "cleanup": "NOT_RUN",
    }
    lab.write_json(output, report)
    ids, sessions = {}, []
    active_case = None

    def owned():
        current = lab.load(root)
        if current["owner"] != state["owner"]:
            raise Incomplete("lab-owner-changed")
        for key, container_id in ids.items():
            current_info = json.loads(lab.docker("inspect", state[key]).stdout)[0]
            if current_info["Id"] != container_id or not current_info["State"]["Running"]:
                raise Incomplete("container-incarnation-changed")

    def session(suffix):
        owned()
        entry = {"application": "bb_" + name + "_ingest_" + suffix,
                 "database": database, "user": ROLE, "pid": None, "backend_start": None}
        report["sessions"].append(entry)
        lab.write_json(output, report)
        value = Session(state, database, entry["application"], sessions)
        entry.update(value.backend)
        lab.write_json(output, report)
        return value

    def complete(case, observed):
        report["cases"][case] = {"status": "PASS", "observed": observed}
        lab.write_json(output, report)

    def close_sessions():
        errors = []
        while sessions:
            current = sessions.pop(0)
            try:
                current.close(owned)
            except BaseException as error:
                errors.append(type(error).__name__)
        if errors:
            report["unresolved_session_cleanup"] = errors
        if report.get("unresolved_session_cleanup"):
            raise Incomplete("session-cleanup-not-proven")

    try:
        report["containers"] = {}
        for key in ("postgres", "client"):
            container = json.loads(lab.docker("inspect", state[key]).stdout)[0]
            ids[key] = container["Id"]
            report["containers"][key] = {"id": container["Id"], "image": container["Image"]}
        owned()
        actual_hash = lab.docker("exec", state["client"], "sha256sum", "/build/bigbrotr").stdout.split()[0]
        mounted_receipt = json.loads(lab.docker(
            "exec", state["client"], "cat", "/build/build-result.json").stdout)
        mounted_manifest = lab.docker(
            "exec", state["client"], "sha256sum", "/build/source-manifest.json").stdout.split()[0]
        actual_info = lab.cli(state, "build-info")
        require(actual_hash == state["binary_sha256"] == mounted_receipt["binary_sha256"]
                and mounted_receipt == state["build"]
                and mounted_manifest == mounted_receipt["source_manifest_sha256"]
                and actual_info.returncode == 0 and not actual_info.stderr
                and json.loads(actual_info.stdout) == mounted_receipt["build_info"],
                "mounted-artifact-identity-mismatch")
        report["server"] = json.loads(lab.sql(state, "postgres", """SELECT json_build_object(
'version',current_setting('server_version_num'),'fsync',current_setting('fsync'),
'full_page_writes',current_setting('full_page_writes'),'architecture',version());"""))
        require(report["server"]["version"] == "160015"
                and report["server"]["fsync"] == report["server"]["full_page_writes"] == "on",
                "engine-prerequisite")
        require(not (root / (database + ".toml")).exists(), "existing-config")
        lab.sql(state, "postgres", f"CREATE DATABASE {database} OWNER {ROLE};")
        candidate = lab.config(root, state, database)
        applied = lab.cli(state, *migration_args(candidate))
        migration_result(applied, "apply", report["migration_sha256"])
        initial = snapshot(state, database)
        require(len(initial["schema_versions"]) == 2
                and all(not initial[table] for table in TABLES if table != "schema_versions"),
                "nonempty-initial-schema")
        for migration, filename in ((1, "0001_control.sql"), (2, "0002_ingest.sql")):
            receipt = find_row(initial, "schema_versions", migration=migration)
            require(receipt["name"] == filename
                    and receipt["checksum"] == "\\x" + report["migration_sha256"][f"{migration:04d}"]
                    and receipt["engine"] == "postgres-control" and receipt["applied_by"] == ROLE
                    and receipt["reader_min"] == receipt["reader_max"] == receipt["writer_floor"] == 1,
                    "migration-identity")

        setup = session("setup")
        setup_fixture(setup)
        close_sessions()
        summary = fixture_summary(state, database)
        require(summary == {
            "schema_versions": 2, "sources": {"1": "open", "2": "open", "3": "open", "4": "open"},
            "topology_members": 4, "owners": 6, "consumers": 2, "frontier_revisions": 3,
            "frontier_bounds": 12, "projection_generations": 3, "projection_chunks": 2,
            "projection_bounds": 8, "snapshot_leases": 2, "retention_claims": 0,
        }, "fixture-summary")
        bootstrap = snapshot(state, database)
        expected_owners = [{
            "resource_id": bytea(resource), "epoch": 1, "dataset_id": bytea(1),
            "role": role, "role_instance_id": bytea(instance),
            "process_incarnation": bytea(incarnation), "acquired_at_us": 0,
            "lease_expires_at_us": -1, "state": state_name,
            "released_at_us": None, "exclusion_method": None, "exclusion_digest": None,
        } for resource, role, instance, incarnation, state_name in (
            (30, "ingest", 11, 31, "active"), (40, "archive", 13, 41, "active"),
            (42, "archive", 13, 43, "active"), (50, "index", 14, 51, "active"),
            (52, "index", 14, 53, "active"), (54, "index", 14, 55, "draining"),
        )]
        require(normalized({"owners": bootstrap["owner_epochs"]})
                == normalized({"owners": expected_owners}), "fixture-owner-tuples")

        active_case = "frontier_seal_before_lower_close"
        before = snapshot(state, database)
        a, b = session("frontier_a"), session("frontier_b")
        a.execute("BEGIN;")
        a.execute(f"""INSERT INTO control.consumer_frontier_revisions(
consumer_id,frontier_revision,dataset_id,consumer_role,topology_generation,previous_revision,
owner_resource_id,owner_epoch,owner_incarnation,created_at_us) VALUES
({identity(70)},2,{identity(1)},'archive',0,1,{identity(40)},1,{identity(41)},2);
INSERT INTO control.consumer_frontiers VALUES {frontier_values(70,2,{1:5},240)};
UPDATE control.consumer_frontier_revisions SET sealed=true
WHERE consumer_id={identity(70)} AND frontier_revision=2;
SET CONSTRAINTS ALL IMMEDIATE;""")
        b.execute("BEGIN;")
        b.start(f"""UPDATE control.source_streams SET state='closing'
WHERE dataset_id={identity(1)} AND shard_id=1 AND owner_epoch=1;
UPDATE control.source_streams SET state='closed',closed_at_us=4,last_complete_sequence=4,
closure_digest={identity(301,32)} WHERE dataset_id={identity(1)} AND shard_id=1 AND owner_epoch=1;""")
        barrier = wait_blocked(state, database, b, a)
        a.execute("COMMIT;")
        refused = b.expect(b.finish(), "23514")
        b.execute("ROLLBACK;")
        expected_revision = {
            "consumer_id": bytea(70), "frontier_revision": 2, "dataset_id": bytea(1),
            "consumer_role": "archive", "topology_generation": 0, "previous_revision": 1,
            "owner_resource_id": bytea(40), "owner_epoch": 1,
            "owner_incarnation": bytea(41), "created_at_us": 2, "sealed": True,
        }
        expected = appended(before, "consumer_frontier_revisions", expected_revision)
        expected["consumer_frontiers"].extend(
            frontier_bound(70, 2, shard, 5 if shard == 1 else 0, 240 + shard)
            for shard in range(1, 5))
        exact_rows(state, database, expected)
        complete(active_case, {"barrier": barrier, "close_sqlstate": refused["sqlstate"],
                               "sealed_bound": 5, "source_state": "open"})
        close_sessions()

        active_case = "lower_close_before_chunk_commit"
        before = snapshot(state, database)
        a, b = session("chunk_a"), session("chunk_b")
        a.execute("BEGIN;")
        a.execute(f"""UPDATE control.source_streams SET state='closing'
WHERE dataset_id={identity(1)} AND shard_id=2 AND owner_epoch=1;
UPDATE control.source_streams SET state='closed',closed_at_us=5,last_complete_sequence=4,
closure_digest={identity(302,32)} WHERE dataset_id={identity(1)} AND shard_id=2 AND owner_epoch=1;
SET CONSTRAINTS ALL IMMEDIATE;""")
        b.execute("BEGIN;")
        b.start(f"""UPDATE control.projection_chunks SET state='committed',committed_at_us=6,
engine_receipt_digest={identity(303,32)}
WHERE generation_id={identity(80)} AND chunk_id={identity(83,32)};
SET CONSTRAINTS ALL IMMEDIATE;""")
        barrier = wait_blocked(state, database, b, a)
        a.execute("COMMIT;")
        refused = b.expect(b.finish(), "23514")
        b.execute("ROLLBACK;")
        expected = changed(before, "source_streams",
                           {"dataset_id": bytea(1), "shard_id": 2, "owner_epoch": 1},
                           state="closed", closed_at_us=5, last_complete_sequence=4,
                           closure_digest=digest(302))
        exact_rows(state, database, expected)
        complete(active_case, {"barrier": barrier, "commit_sqlstate": refused["sqlstate"],
                               "source_final": 4, "chunk_state": "prepared"})
        close_sessions()

        active_case = "close_before_mismatched_handoff"
        before = snapshot(state, database)
        a, b = session("handoff_a"), session("handoff_b")
        a.execute("BEGIN;")
        a.execute(f"""UPDATE control.source_streams SET state='closing'
WHERE dataset_id={identity(1)} AND shard_id=3 AND owner_epoch=1;
UPDATE control.source_streams SET state='closed',closed_at_us=7,last_complete_sequence=5,
closure_digest={identity(304,32)} WHERE dataset_id={identity(1)} AND shard_id=3 AND owner_epoch=1;
SET CONSTRAINTS ALL IMMEDIATE;""")
        b.execute("BEGIN;")
        b.start(f"""INSERT INTO control.topology_generations VALUES({identity(1)},1,0,7,false);
INSERT INTO control.topology_streams VALUES({identity(1)},1,3,1,4);
UPDATE control.topology_generations SET sealed=true WHERE dataset_id={identity(1)} AND generation=1;
SET CONSTRAINTS ALL IMMEDIATE;""")
        barrier = wait_blocked(state, database, b, a)
        a.execute("COMMIT;")
        refused = b.expect(b.finish(), "23514")
        b.execute("ROLLBACK;")
        expected = changed(before, "source_streams",
                           {"dataset_id": bytea(1), "shard_id": 3, "owner_epoch": 1},
                           state="closed", closed_at_us=7, last_complete_sequence=5,
                           closure_digest=digest(304))
        exact_rows(state, database, expected)
        complete(active_case, {"barrier": barrier, "handoff_sqlstate": refused["sqlstate"],
                               "source_final": 5, "attempted_handoff": 4})
        close_sessions()

        active_case = "drain_before_new_chunk"
        before = snapshot(state, database)
        a, b = session("drain_a"), session("drain_b")
        a.execute("BEGIN;")
        a.execute(f"UPDATE control.owner_epochs SET state='draining' WHERE resource_id={identity(52)} AND epoch=1;")
        b.execute("BEGIN;")
        b.start(f"""INSERT INTO control.projection_chunks(generation_id,chunk_id,dataset_id,
storage_incarnation,table_identity_digest,target,target_schema_version,topology_generation,
chunk_ordinal,row_count,owner_resource_id,owner_epoch,owner_incarnation,prepared_at_us) VALUES
({identity(84)},{identity(87,32)},{identity(1)},{identity(85)},{identity(86,32)},
'events',1,0,0,0,{identity(52)},1,{identity(53)},8);""")
        barrier = wait_blocked(state, database, b, a)
        a.execute("COMMIT;")
        refused = b.expect(b.finish(), "23514")
        b.execute("ROLLBACK;")
        expected = changed(before, "owner_epochs", {"resource_id": bytea(52), "epoch": 1},
                           state="draining")
        exact_rows(state, database, expected)
        complete(active_case, {"barrier": barrier, "insert_sqlstate": refused["sqlstate"],
                               "owner_state": "draining", "new_chunk_count": 0})
        close_sessions()

        active_case = "chunk_complete_before_release"
        before = snapshot(state, database)
        require(find_row(before, "owner_epochs", resource_id=bytea(54), epoch=1)["state"]
                == "draining", "resolve-owner-not-draining")
        require(find_row(before, "projection_chunks", generation_id=bytea(88),
                         chunk_id=digest(91))["state"] == "prepared", "resolve-chunk-not-prepared")
        a, b = session("resolve_a"), session("resolve_b")
        a.execute("BEGIN;")
        a.execute(f"""UPDATE control.projection_chunks SET state='committed',committed_at_us=9,
engine_receipt_digest={identity(305,32)}
WHERE generation_id={identity(88)} AND chunk_id={identity(91,32)};
SET CONSTRAINTS ALL IMMEDIATE;""")
        b.execute("BEGIN;")
        b.start(f"""UPDATE control.owner_epochs SET state='released',released_at_us=10,
exclusion_method='joined',exclusion_digest={identity(306,32)}
WHERE resource_id={identity(54)} AND epoch=1;""")
        barrier = wait_blocked(state, database, b, a)
        a.execute("COMMIT;")
        b.expect(b.finish(), "00000", 1)
        b.execute("COMMIT;")
        expected = changed(before, "projection_chunks",
                           {"generation_id": bytea(88), "chunk_id": digest(91)},
                           state="committed", committed_at_us=9,
                           engine_receipt_digest=digest(305))
        expected = changed(expected, "owner_epochs", {"resource_id": bytea(54), "epoch": 1},
                           state="released", released_at_us=10, exclusion_method="joined",
                           exclusion_digest=digest(306))
        exact_rows(state, database, expected)
        complete(active_case, {"barrier": barrier, "chunk_state": "committed",
                               "owner_state": "released", "exclusion_is_synthetic": True})
        close_sessions()

        active_case = "frontier_publish_cas"
        before = snapshot(state, database)
        a, b = session("publish_a"), session("publish_b")
        for current in (a, b):
            current.execute("BEGIN;")
            require(current.execute(f"SELECT current_revision::text FROM control.consumers WHERE consumer_id={identity(71)};")["lines"] == ["1"],
                    "frontier-cas-common-head")
        publish = (f"UPDATE control.consumers SET current_revision=2,current_topology_generation=0 "
                   f"WHERE consumer_id={identity(71)} AND current_revision=1;")
        a.execute(publish, rows=1)
        b.start(publish)
        barrier = wait_blocked(state, database, b, a)
        a.execute("COMMIT;")
        loser = b.expect(b.finish(), "00000", 0)
        b.execute("ROLLBACK;")
        expected = changed(before, "consumers", {"consumer_id": bytea(71)},
                           current_revision=2, current_topology_generation=0)
        exact_rows(state, database, expected)
        complete(active_case, {"barrier": barrier, "committed_winners": 1,
                               "loser_rows": loser["rows"], "published_revision": 2})
        close_sessions()

        active_case = "claim_before_lease_release"
        before = snapshot(state, database)
        a, b = session("claim_a"), session("claim_b")
        a.execute("BEGIN;")
        a.execute(f"""INSERT INTO control.retention_claims(claim_id,lease_id,dataset_id,
topology_generation,shard_id,owner_epoch,first_sequence,last_sequence,purpose,created_at_us)
VALUES({identity(110)},{identity(100)},{identity(1)},0,4,1,1,5,'backup',10);""")
        b.execute("BEGIN;")
        b.start(f"""UPDATE control.snapshot_leases SET lease_revision=2,updated_at_us=20,
state='released',released_at_us=20,release_reason='complete'
WHERE lease_id={identity(100)};""")
        barrier = wait_blocked(state, database, b, a)
        a.execute("COMMIT;")
        b.expect(b.finish(), "00000", 1)
        b.execute("COMMIT;")
        expected = changed(before, "snapshot_leases", {"lease_id": bytea(100)},
                           lease_revision=2, updated_at_us=20, state="released",
                           released_at_us=20, release_reason="complete")
        expected["retention_claims"].append({
            "claim_id": bytea(110), "lease_id": bytea(100), "dataset_id": bytea(1),
            "topology_generation": 0, "shard_id": 4, "owner_epoch": 1,
            "first_sequence": 1, "last_sequence": 5, "purpose": "backup",
            "created_at_us": 10, "state": "active", "released_at_us": None,
            "release_reason": None,
        })
        exact_rows(state, database, expected)
        complete(active_case, {"barrier": barrier, "lease_state": "released",
                               "claim_state": "active", "claim_survived_release": True})
        close_sessions()

        active_case = "lease_release_before_claim"
        before = snapshot(state, database)
        a, b = session("release_a"), session("release_b")
        a.execute("BEGIN;")
        a.execute(f"""UPDATE control.snapshot_leases SET lease_revision=2,updated_at_us=21,
state='released',released_at_us=21,release_reason='complete'
WHERE lease_id={identity(101)};""")
        b.execute("BEGIN;")
        b.start(f"""INSERT INTO control.retention_claims(claim_id,lease_id,dataset_id,
topology_generation,shard_id,owner_epoch,first_sequence,last_sequence,purpose,created_at_us)
VALUES({identity(111)},{identity(101)},{identity(1)},0,4,1,1,5,'backup',11);""")
        barrier = wait_blocked(state, database, b, a)
        a.execute("COMMIT;")
        refused = b.expect(b.finish(), "23514")
        b.execute("ROLLBACK;")
        expected = changed(before, "snapshot_leases", {"lease_id": bytea(101)},
                           lease_revision=2, updated_at_us=21, state="released",
                           released_at_us=21, release_reason="complete")
        exact_rows(state, database, expected)
        complete(active_case, {"barrier": barrier, "claim_sqlstate": refused["sqlstate"],
                               "lease_state": "released", "new_claim_count": 0})
        close_sessions()

        final = lab.cli(state, *migration_args(candidate, "inspect"))
        migration_result(final, "inspect", report["migration_sha256"])
        actual = snapshot(state, database)
        require(actual["schema_versions"] == initial["schema_versions"], "ledger-mutated")
        lab.write_json(directory / "observed-rows.json", actual)
        report["final"] = {"status": "PASS", "inspect_exit": final.returncode,
                           "receipt_unchanged": True,
                           "schema_and_data_sha256": lab.schema_digest(state, database),
                           "relations_compared": len(TABLES)}
    except BaseException as error:
        status = "FAIL" if isinstance(error, Mismatch) else "UNKNOWN"
        report["status"] = status
        report["failure_class"] = type(error).__name__
        report["failure_reason"] = (str(error) if isinstance(error, (Mismatch, Incomplete))
                                    else "unclassified-or-interrupted")
        if active_case:
            report["cases"][active_case] = {"status": status, "reason": report["failure_reason"]}
        raise
    finally:
        try:
            close_sessions()
            report["cleanup"] = "PASS"
        except BaseException as error:
            report["cleanup"] = "UNKNOWN"
            report["cleanup_failure_class"] = type(error).__name__
            report["status"] = "UNKNOWN"
        counts = {status: sum(case["status"] == status for case in report["cases"].values())
                  for status in ("PASS", "FAIL", "UNKNOWN", "NOT_RUN")}
        report["counts"] = counts
        if counts["PASS"] == len(CASES) and report["cleanup"] == "PASS" \
                and report.get("final", {}).get("status") == "PASS":
            report["status"] = "PASS"
        report["finished_at_unix"] = time.time()
        lab.write_json(output, report)
        os.close(lock)
    require(report["status"] == "PASS", "ingest-concurrency-run-incomplete")
    print(json.dumps({"status": report["status"], "cases": len(CASES), "output": str(output)}))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--run-name", default="ingest_races")
    arguments = parser.parse_args()
    run(arguments.root, arguments.run_name)
