#!/usr/bin/env python3
"""Two post-lock expiry controls on the retained 0041 synthetic SQL fixture.

Reuses the owned PG lab and its exact-backend Session cleanup. Creates no engine,
build, image, network or database. This is SQL coordination evidence, not a CH
verification or local custody proof. Run after index_publication_cases.sql.
"""
import argparse
import fcntl
import json
import os
from pathlib import Path
import re
import time

import lab
from concurrency_cases import Incomplete, Session, identity, require, sha, wait_blocked


def claim_control(owner):
    owner.execute("SELECT pg_advisory_lock(1111651442,1129599564);")
    owner.execute(f"""BEGIN;
UPDATE control.owner_epochs SET state='released',released_at_us=10,exclusion_method='storage_fenced',
 exclusion_digest={identity(910,32)} WHERE resource_id={identity(20)} AND epoch=1;
INSERT INTO control.role_instances VALUES({identity(14)},{identity(1)},'control',10,NULL);
INSERT INTO control.owner_epochs(resource_id,epoch,dataset_id,role,role_instance_id,process_incarnation,
 acquired_at_us,lease_expires_at_us,state,control_fence_version,control_backend_pid,control_backend_start)
 VALUES({identity(20)},2,{identity(1)},'control',{identity(14)},{identity(22)},10,
 (EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint+300000000,'active',1,pg_backend_pid(),
 (SELECT backend_start FROM pg_stat_activity WHERE pid=pg_backend_pid()));
COMMIT;""")
    owner.execute(f"SELECT control.require_control_owner({identity(1)},{identity(20)},2,{identity(22)});")


def transfer_index(owner, epoch):
    # Finite, explicitly synthetic recovery records. This never constructs the
    # application's private physical proof or calls native storage operations.
    role, incarnation = 12 + epoch, 50 + epoch
    owner.execute(f"""BEGIN;
UPDATE control.owner_epochs SET state='released',released_at_us=10,exclusion_method='storage_fenced',
 exclusion_digest={identity(920+epoch,32)} WHERE resource_id={identity(50)} AND epoch={epoch-1};
INSERT INTO control.role_instances VALUES({identity(role)},{identity(1)},'index',10,NULL);
INSERT INTO control.owner_epochs(resource_id,epoch,dataset_id,role,role_instance_id,process_incarnation,
 acquired_at_us,lease_expires_at_us,state,index_publication_protocol)
 VALUES({identity(50)},{epoch},{identity(1)},'index',{identity(role)},{identity(incarnation)},10,
 (EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint+300000000,'active',1);
INSERT INTO control.index_owner_sessions(dataset_id,resource_id,owner_epoch,owner_incarnation,generation_id,
 issuer_resource,issuer_epoch,issuer_incarnation,previous_epoch,recovery_digest,recovered_revision,recovered_plan,recovered_pending_plan)
 VALUES({identity(1)},{identity(50)},{epoch},{identity(incarnation)},{identity(60)},
 {identity(20)},2,{identity(22)},{epoch-1},{identity(920+epoch,32)},1,{identity(101,32)},{identity(102,32)});
COMMIT;""")
    owner.execute(f"SELECT control.require_index_owner({identity(1)},{identity(50)},{epoch},{identity(incarnation)});")
    return role, incarnation


def retained(state, database):
    # Independent exact scientific metadata, excluding only intentionally
    # transferred/expired owner/session and role rows.
    names = ("projection_generations", "projection_plans", "projection_plan_chunks",
             "projection_chunks", "projection_chunk_bounds", "index_frontier_plans",
             "consumers", "consumer_frontier_revisions", "consumer_frontiers")
    return {name: json.loads(lab.sql(state, database,
        f"SELECT coalesce(jsonb_agg(v ORDER BY v::text COLLATE \"C\"),'[]'::jsonb) "
        f"FROM(SELECT to_jsonb(t) v FROM control.{name} t)s;")) for name in names}


def run(root, database, name):
    state = lab.load(root)
    output = root / (name + "-index-races.json")
    with output.open("x", encoding="utf-8"):
        os.chmod(output, 0o600)
    lock = os.open(root / ".index-publication.lock", os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    sessions = []
    container_id = json.loads(lab.docker("inspect", state["postgres"]).stdout)[0]["Id"]
    report = {"format": "bigbrotr-index-publication-races/1", "status": "RUNNING",
              "database": database, "source_sha256": sha(Path(__file__)), "cases": [],
              "not_run": ["physical CH verification", "native custody", "lost PG COMMIT"]}

    def owned():
        current = lab.load(root)
        require(current == state and json.loads(lab.docker("inspect", state["postgres"]).stdout)[0]["Id"] == container_id,
                "owned-container-changed")

    def save():
        lab.write_json(output, report)

    try:
        migration = Path(__file__).resolve().parents[2] / "migrations/postgres/0041_index_publication.sql"
        report["migration_sha256"] = sha(migration)
        require(lab.sql(state, database, "SELECT encode(checksum,'hex') FROM control.schema_versions WHERE migration=41;")
                == report["migration_sha256"], "exact-migration-required")
        require(lab.sql(state, database, "SELECT current_setting('server_version_num');") == "160015", "exact-pg-required")
        require(lab.sql(state, database, f"SELECT (SELECT count(*) FROM control.projection_plans)=2 "
                    f"AND (SELECT count(*) FROM control.projection_chunks)=20 "
                    f"AND (SELECT max(owner_epoch) FROM control.index_owner_sessions WHERE resource_id={identity(50)})=2 "
                    f"AND (SELECT current_revision FROM control.consumers WHERE consumer_id={identity(50)})=1;") == "t",
                "exact-unconsumed-synthetic-fixture-required")
        before = retained(state, database)
        owner = Session(state, database, name + "_owner", sessions)
        blocker = Session(state, database, name + "_blocker", sessions)
        report["backends"] = [owner.backend, blocker.backend]
        claim_control(owner)
        for target, epoch in (("index", 3), ("control", 4)):
            role, incarnation = transfer_index(owner, epoch)
            resource, owner_epoch, blocked_role = (50, epoch, role) if target == "index" else (20, 2, 14)
            deadline = int(owner.execute(f"UPDATE control.owner_epochs SET lease_expires_at_us="
                f"(EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint+2000000 "
                f"WHERE resource_id={identity(resource)} AND epoch={owner_epoch} RETURNING lease_expires_at_us;")["lines"][0])
            blocker.execute(f"BEGIN; SELECT role_instance_id FROM control.role_instances WHERE role_instance_id={identity(blocked_role)} FOR UPDATE;")
            owner.start(f"SELECT control.require_index_owner({identity(1)},{identity(50)},{epoch},{identity(incarnation)});")
            barrier = wait_blocked(state, database, owner, blocker)
            started = int(lab.sql(state, database,
                f"SELECT (EXTRACT(EPOCH FROM query_start)*1000000)::bigint FROM pg_stat_activity WHERE pid={owner.backend['pid']};"))
            require(started < deadline, "operation-did-not-start-before-expiry")
            bound = time.monotonic() + 5
            while lab.sql(state, database,
                    f"SELECT (EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint >= {deadline};") != "t":
                if time.monotonic() >= bound:
                    raise Incomplete("expiry-observation-deadline")
                time.sleep(0.05)
            blocker.execute("ROLLBACK;")
            refusal = owner.expect(owner.finish(), "23514")
            require(retained(state, database) == before, "wait-refusal-mutated-retained-work")
            report["cases"].append({"name": target + "_expires_during_observed_row_lock",
                                    "status": "PASS", "deadline_us": deadline,
                                    "query_started_us": started, "barrier": barrier, "refusal": refusal})
            save()
        report["status"] = "PASS"
    except BaseException as error:
        report["status"] = "INCOMPLETE" if isinstance(error, Incomplete) else "FAIL"
        report["failure_class"] = type(error).__name__
        raise
    finally:
        errors = []
        for session in sessions:
            try:
                session.close(owned)
            except BaseException as error:
                errors.append(type(error).__name__)
        report["cleanup"] = {"status": "FAIL" if errors else "PASS", "errors": errors}
        if errors:
            report["status"] = "INCOMPLETE"
        save()
        os.close(lock)
    require(report["status"] == "PASS", "race-campaign-not-complete")
    print(json.dumps({"status": report["status"], "receipt": str(output)}))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", required=True, type=Path)
    parser.add_argument("--database", required=True)
    parser.add_argument("--name", required=True)
    args = parser.parse_args()
    if not re.fullmatch(r"bb_[a-z0-9_]{1,40}", args.database) or not re.fullmatch(r"[a-z][a-z0-9_]{0,23}", args.name):
        parser.error("bounded owned database/case identifiers required")
    run(args.root.resolve(), args.database, args.name)
