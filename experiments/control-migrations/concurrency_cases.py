#!/usr/bin/env python3
"""Finite real PostgreSQL 0001 transaction races in an existing owned laboratory."""
import argparse
import copy
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import select
import stat
import subprocess
import time

import lab

ROLE = "bigbrotr_migrator"
TABLES = ("schema_versions", "datasets", "config_revisions", "policy_revisions",
          "role_instances", "owner_epochs", "topology_generations", "topology_streams",
          "jobs", "job_input_bounds")
CASES = ("revision_cas", "owner_acquisition", "topology_uncommitted_parent",
         "topology_late_append", "job_before_release", "release_before_job", "final_inspection")


class Incomplete(Exception):
    """Observation/transport failed; transaction outcome is not inferred."""


class Mismatch(Exception):
    """An observed result contradicts the fixture's independent expectation."""


def require(condition, reason):
    if not condition:
        raise Mismatch(reason)


def identity(n, size=16):
    return "decode('" + format(n, "0" + str(size * 2) + "x") + "','hex')"


def bytea(n):
    return "\\x" + format(n, "032x")


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def snapshot(state, database):
    # Independent finite relation list, including receipts and unrelated rows.
    parts = [f"'{table}',(SELECT coalesce(jsonb_agg(v ORDER BY v::text COLLATE \"C\"),'[]'::jsonb) FROM (SELECT to_jsonb(t) v FROM control.{table} t) s)" for table in TABLES]
    return json.loads(lab.sql(state, database, "SELECT jsonb_build_object(" + ",".join(parts) + ");"))


def normalized(rows):
    return {key: sorted(value, key=lambda row: json.dumps(row, sort_keys=True)) for key, value in rows.items()}


def unchanged_except(state, database, expected):
    actual = snapshot(state, database)
    require(normalized(actual) == normalized(expected), "external-durable-rows-mismatch")
    return actual


class Session:
    """One bounded psql pipe; the observer fences backend cleanup separately."""
    def __init__(self, state, database, name, sessions, *, role=None):
        self.state, self.database, self.name = state, database, name
        self.role = ROLE if role is None else role
        self.backend = None
        self.pending = None
        self.buffer = b""
        self.sequence = 0
        self.total_bytes = 0
        self.process = subprocess.Popen(
            ["docker", "exec", "-i", "--env", "PGAPPNAME=" + name, "--env", "PGCONNECT_TIMEOUT=5",
             "--env", "PGOPTIONS=-c statement_timeout=30000 -c lock_timeout=20000 -c idle_in_transaction_session_timeout=60000 -c synchronous_commit=on",
             state["postgres"], "psql", "-X", "-A", "-t", "-q",
             "--no-password", "-h", "/var/run/postgresql", "-U", self.role, "-d", database,
             "-v", "ON_ERROR_STOP=off", "-v", "VERBOSITY=sqlstate", "-P", "pager=off"],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, bufsize=0)
        sessions.append(self)
        answer = self.execute("SELECT json_build_object('pid',pid,'backend_start',backend_start::text,'database',datname,'user',usename,'application',application_name,'isolation',current_setting('transaction_isolation'),'synchronous_commit',current_setting('synchronous_commit')) FROM pg_stat_activity WHERE pid=pg_backend_pid();")
        self.backend = json.loads(answer["lines"][0])
        require(self.backend["database"] == database and self.backend["user"] == self.role
                and self.backend["application"] == name and self.backend["isolation"] == "read committed"
                and self.backend["synchronous_commit"] == "on",
                "session-identity-mismatch")

    def start(self, sql):
        if self.pending is not None:
            raise Incomplete("session-already-busy")
        self.sequence += 1
        self.pending = "bb_result_" + str(self.sequence)
        # One command response marker is printed by psql after execution, including
        # exact SQLSTATE/ROW_COUNT. SQL fixtures never contain credentials.
        data = (sql + "\n\\echo " + self.pending + " :SQLSTATE :ROW_COUNT\n").encode()
        require(len(data) <= 4096, "fixture-command-size")
        require(self.process.stdin.write(data) == len(data), "fixture-command-short-write")
        self.process.stdin.flush()

    def finish(self, timeout=12):
        deadline = time.monotonic() + timeout
        lines = []
        while True:
            while b"\n" in self.buffer:
                line, self.buffer = self.buffer.split(b"\n", 1)
                text = line.decode("utf-8")
                if text.startswith(self.pending + " "):
                    pieces = text.split()
                    if len(pieces) != 3 or not re.fullmatch(r"[0-9A-Z]{5}", pieces[1]) or not pieces[2].isdigit():
                        raise Incomplete("psql-response-format")
                    self.pending = None
                    return {"sqlstate": pieces[1], "rows": int(pieces[2]), "lines": lines}
                lines.append(text)
            left = deadline - time.monotonic()
            if left <= 0:
                raise Incomplete("psql-response-deadline")
            ready, _, _ = select.select([self.process.stdout], [], [], min(left, 0.2))
            if ready:
                data = os.read(self.process.stdout.fileno(), 4096)
                if not data:
                    raise Incomplete("psql-response-eof")
                self.total_bytes += len(data)
                if self.total_bytes > 65536:
                    raise Incomplete("psql-output-limit")
                self.buffer += data

    def execute(self, sql, expected="00000", rows=None):
        self.start(sql)
        return self.expect(self.finish(), expected, rows)

    @staticmethod
    def expect(answer, expected="00000", rows=None):
        require(answer["sqlstate"] == expected, "unexpected-sqlstate")
        require(rows is None or answer["rows"] == rows, "unexpected-row-count")
        return answer

    def close(self, owned):
        # Closing docker's shell/pipe does not attest PostgreSQL rollback. Target
        # only the exact recorded backend incarnation, then observe its absence.
        try:
            owned()
            if self.backend is None:
                raise Incomplete("backend-identity-unavailable-for-cleanup")
            b = self.backend
            predicate = (f"pid={b['pid']} AND backend_start='{b['backend_start']}'::timestamptz "
                         f"AND datname='{self.database}' AND usename='{self.role}' AND application_name='{self.name}'")
            lab.sql(self.state, "postgres", "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE " + predicate + ";")
            deadline = time.monotonic() + 5
            while lab.sql(self.state, "postgres", "SELECT count(*) FROM pg_stat_activity WHERE " + predicate + ";") != "0":
                if time.monotonic() > deadline:
                    raise Incomplete("backend-termination-unobserved")
                time.sleep(0.05)
        finally:
            if self.process.stdin and not self.process.stdin.closed:
                self.process.stdin.close()
            try:
                self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.process.terminate()
                try:
                    self.process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    self.process.kill()
                    self.process.wait(timeout=5)
            self.process.stdout.close()


def wait_blocked(state, database, waiter, holder):
    deadline = time.monotonic() + 10
    while True:
        observation = json.loads(lab.sql(state, database, f"""SELECT json_build_object(
            'waiter',a.pid,'backend_start',a.backend_start::text,'state',a.state,
            'wait_type',a.wait_event_type,'wait_event',a.wait_event,
            'blockers',pg_blocking_pids(a.pid),
            'holder_start',(SELECT backend_start::text FROM pg_stat_activity WHERE pid={holder.backend['pid']} AND datname='{database}' AND usename='{ROLE}' AND application_name='{holder.name}'),
            'waiting_locks',(SELECT coalesce(json_agg(json_build_object('type',locktype,'mode',mode,'transaction',transactionid::text,'relation',relation::regclass::text)),'[]') FROM pg_locks WHERE pid=a.pid AND NOT granted))
            FROM pg_stat_activity a WHERE a.pid={waiter.backend['pid']};"""))
        if observation and observation["wait_type"] == "Lock" and holder.backend["pid"] in observation["blockers"] and observation["waiting_locks"]:
            if observation["backend_start"] != waiter.backend["backend_start"] or observation["holder_start"] != holder.backend["backend_start"]:
                raise Incomplete("barrier-incarnation-changed")
            return observation
        if waiter.process.poll() is not None or time.monotonic() > deadline:
            raise Incomplete("required-lock-barrier-not-observed")
        time.sleep(0.05)


def bootstrap():
    d, actor = identity(1), identity(3)
    text = f"""BEGIN;
INSERT INTO control.datasets(dataset_id,instance_id,created_at_us) VALUES({d},{identity(2)},0);
INSERT INTO control.config_revisions VALUES(1,{d},NULL,1,'managed','{{}}',{identity(1,32)},{identity(2,32)},0,{actor});
INSERT INTO control.policy_revisions VALUES(1,{d},NULL,1,1,'{{}}',{identity(3,32)},0,{actor});
INSERT INTO control.topology_generations VALUES({d},0,NULL,0,true);
UPDATE control.datasets SET config_revision=1,policy_revision=1,topology_generation=0;
INSERT INTO control.role_instances VALUES({identity(10)},{d},'control',0,NULL);
"""
    for resource in (20, 21, 22):
        text += owner_insert(resource, 1, 30) + "\n"
    text += release(20) + "\n"
    for job in (40, 41):
        text += f"INSERT INTO control.jobs(job_id,dataset_id,principal_id,request_key,request_digest,kind,config_revision,policy_revision,created_at_us,input_complete) VALUES({identity(job)},{d},{actor},decode('{job:02x}','hex'),{identity(40,32)},'export',1,1,0,true);\n"
    return text + "COMMIT;"


def expected_bootstrap(receipts):
    # Literal fixture data, independent of production triggers and observed rows.
    digest = lambda n: "\\x" + format(n, "064x")
    result = {table: [] for table in TABLES}
    result["schema_versions"] = copy.deepcopy(receipts)
    result["datasets"] = [dict(dataset_id=bytea(1), instance_id=bytea(2), singleton=True,
                               created_at_us=0, topology_generation=0, config_revision=1, policy_revision=1)]
    result["config_revisions"] = [dict(revision=1, dataset_id=bytea(1), previous_revision=None,
        schema_version=1, authority_mode="managed", document={}, document_digest=digest(1),
        accepted_source_digest=digest(2), accepted_at_us=0, actor_id=bytea(3))]
    result["policy_revisions"] = [dict(revision=1, dataset_id=bytea(1), previous_revision=None,
        config_revision=1, schema_version=1, document={}, document_digest=digest(3), accepted_at_us=0, actor_id=bytea(3))]
    result["topology_generations"] = [dict(dataset_id=bytea(1), generation=0, previous_generation=None,
                                         created_at_us=0, sealed=True)]
    result["role_instances"] = [dict(role_instance_id=bytea(10), dataset_id=bytea(1), role="control",
                                    created_at_us=0, retired_at_us=None)]
    for resource in (20,21,22):
        released = resource == 20
        result["owner_epochs"].append(dict(resource_id=bytea(resource), epoch=1, dataset_id=bytea(1),
            role="control", role_instance_id=bytea(10), process_incarnation=bytea(30), acquired_at_us=0,
            lease_expires_at_us=-1, state="released" if released else "active",
            released_at_us=2 if released else None, exclusion_method="joined" if released else None,
            exclusion_digest=digest(60) if released else None))
    for job in (40,41):
        result["jobs"].append(dict(job_id=bytea(job), dataset_id=bytea(1), principal_id=bytea(3),
            request_key="\\x" + format(job,"02x"), request_digest=digest(40), kind="export",
            config_revision=1, policy_revision=1, created_at_us=0, state="accepted", job_revision=1,
            checkpoint_revision=0, reason_code=None, result_manifest_digest=None, owner_resource_id=None,
            owner_epoch=None, owner_role="control", owner_incarnation=None, completed_at_us=None,
            input_topology_generation=None, input_complete=True))
    return result


def owner_insert(resource, epoch, incarnation):
    return f"INSERT INTO control.owner_epochs(resource_id,epoch,dataset_id,role,role_instance_id,process_incarnation,acquired_at_us,lease_expires_at_us,state) VALUES({identity(resource)},{epoch},{identity(1)},'control',{identity(10)},{identity(incarnation)},0,-1,'active');"


def release(resource):
    return f"UPDATE control.owner_epochs SET state='released',released_at_us=2,exclusion_method='joined',exclusion_digest={identity(60,32)} WHERE resource_id={identity(resource)} AND epoch=1;"


def queue(job, resource):
    return f"UPDATE control.jobs SET state='queued',job_revision=2,owner_resource_id={identity(resource)},owner_epoch=1,owner_incarnation={identity(30)} WHERE job_id={identity(job)} AND job_revision=1;"


def row(rows, table, key, value):
    found = [r for r in rows[table] if r[key] == value]
    require(len(found) == 1, "fixture-row-identity")
    return found[0]


def run(root, name):
    source = Path(__file__).resolve().parents[2]
    require(root.is_absolute() and root.resolve() == root and re.fullmatch(r"[a-z_]{1,20}", name), "invalid-run-root-or-name")
    require(Path(lab.__file__).resolve().parent == Path(__file__).resolve().parent, "foreign-lab-import")
    require(not lab.command(["git", "-C", str(source), "status", "--porcelain", "--untracked-files=all"]).stdout, "dirty-source")
    state = lab.load(root)
    require(state["state"] == "running", "lab-not-running")
    info = root.lstat()
    require(stat.S_ISDIR(info.st_mode) and info.st_uid == os.geteuid() and stat.S_IMODE(info.st_mode) == 0o700, "lab-root-not-private")
    lock = os.open(root / "concurrency-mutation.lock", os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    directory = root / (name + "-concurrency")
    directory.mkdir(mode=0o700)
    output = directory / "result.json"
    database = "bb_" + name + "_concurrency"
    report = {"format": "bigbrotr-pg-concurrency/1", "status": "RUNNING", "started_at_unix": time.time(),
              "database": database, "binary_sha256": state["binary_sha256"],
              "application_source": state["build"]["build_info"]["source"],
              "harness_revision": lab.command(["git", "-C", str(source), "rev-parse", "HEAD"]).stdout.strip(),
              "harness_sha256": sha(Path(__file__)), "lab_sha256": sha(Path(lab.__file__)),
              "migration_sha256": sha(source / "migrations/postgres/0001_control.sql"),
              "cases": {case: {"status": "NOT_RUN"} for case in CASES}, "sessions": [], "cleanup": "NOT_RUN"}
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
        entry = {"application":"bb_" + name + "_" + suffix, "database":database, "user":ROLE, "pid":None, "backend_start":None}
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
        actual_hash = lab.docker("exec", state["client"], "python3", "-c", "import hashlib; print(hashlib.sha256(open('/build/bigbrotr','rb').read()).hexdigest())").stdout.strip()
        require(actual_hash == state["binary_sha256"], "binary-receipt-mismatch")
        report["server"] = json.loads(lab.sql(state, "postgres", "SELECT json_build_object('version',current_setting('server_version_num'),'fsync',current_setting('fsync'),'full_page_writes',current_setting('full_page_writes'),'architecture',version());"))
        require(report["server"]["version"] == "160015" and report["server"]["fsync"] == report["server"]["full_page_writes"] == "on", "engine-prerequisite")
        require(not (root / (database + ".toml")).exists(), "existing-config")
        lab.sql(state, "postgres", f"CREATE DATABASE {database} OWNER {ROLE};")
        candidate = lab.config(root, state, database)
        applied = lab.migrate(state, "apply", candidate)
        require(applied.returncode == 0 and json.loads(applied.stdout).get("status") == "applied", "real-cli-apply")
        initial = snapshot(state, database)
        require(all(not initial[t] for t in TABLES if t != "schema_versions"), "nonempty-initial-schema")
        require(len(initial["schema_versions"]) == 1, "migration-receipt-count")
        receipt = initial["schema_versions"][0]
        for field, value in {"engine":"postgres-control", "migration":1, "name":"0001_control.sql",
                             "checksum":"\\x" + report["migration_sha256"], "reader_min":1,
                             "reader_max":1, "writer_floor":1}.items():
            require(receipt[field] == value, "migration-identity")
        setup = session("setup")
        setup.execute(bootstrap())
        close_sessions()
        expected = expected_bootstrap(initial["schema_versions"])
        unchanged_except(state, database, expected)
        initial_receipt = copy.deepcopy(expected["schema_versions"])

        active_case = "revision_cas"
        a, b = session("cas_a"), session("cas_b")
        for s in (a,b):
            s.execute("BEGIN;")
            require(s.execute("SELECT config_revision::text FROM control.datasets;")["lines"] == ["1"], "cas-common-expected-head")
        # Deferred FK permits head-first CAS. Only its winner inserts revision 2;
        # loser rolls back and never inserts a conflicting candidate revision.
        a.execute("UPDATE control.datasets SET config_revision=2 WHERE config_revision=1;", rows=1)
        a.execute(f"INSERT INTO control.config_revisions VALUES(2,{identity(1)},1,1,'managed','{{}}',{identity(5,32)},{identity(6,32)},1,{identity(3)});", rows=1)
        b.start("UPDATE control.datasets SET config_revision=2 WHERE config_revision=1;")
        barrier = wait_blocked(state, database, b, a)
        a.execute("COMMIT;")
        loser = b.expect(b.finish(), rows=0)
        b.execute("ROLLBACK;")
        expected["datasets"][0]["config_revision"] = 2
        revision = copy.deepcopy(expected["config_revisions"][0])
        revision.update(revision=2, previous_revision=1, document_digest="\\x" + format(5,"064x"), accepted_source_digest="\\x" + format(6,"064x"), accepted_at_us=1)
        expected["config_revisions"].append(revision)
        unchanged_except(state, database, expected)
        complete(active_case, {"barrier": barrier, "committed_winners": 1, "loser_cas_rows": loser["rows"], "loser_action": "ROLLBACK", "history_revisions": [1,2]})
        close_sessions()

        active_case = "owner_acquisition"
        a, b = session("epoch_a"), session("epoch_b")
        a.execute("BEGIN;")
        b.execute("BEGIN;")
        a.execute(owner_insert(20,2,31), rows=1)
        b.start(owner_insert(20,2,32))
        barrier = wait_blocked(state, database, b, a)
        a.execute("COMMIT;")
        loser = b.expect(b.finish(), "23505")
        b.execute("ROLLBACK;")
        successor = copy.deepcopy(row(expected,"owner_epochs","resource_id",bytea(20)))
        successor.update(epoch=2, process_incarnation=bytea(31), state="active", released_at_us=None, exclusion_method=None, exclusion_digest=None)
        expected["owner_epochs"].append(successor)
        unchanged_except(state, database, expected)
        complete(active_case, {"barrier": barrier, "committed_winners": 1, "loser_sqlstate": loser["sqlstate"], "successor_incarnation": bytea(31)})
        close_sessions()

        active_case = "topology_uncommitted_parent"
        a, b = session("topology_a"), session("topology_b")
        a.execute("BEGIN;")
        a.execute(f"INSERT INTO control.topology_generations VALUES({identity(1)},1,0,1,false);")
        a.execute(f"INSERT INTO control.topology_streams VALUES({identity(1)},1,0,1,0);")
        a.execute("UPDATE control.topology_generations SET sealed=true WHERE generation=1;", rows=1)
        require(lab.sql(state,database,"SELECT count(*) FROM control.topology_generations WHERE generation=1;") == "0", "uncommitted-parent-visible")
        b.execute("BEGIN;")
        hidden = b.execute(f"INSERT INTO control.topology_streams VALUES({identity(1)},1,7,1,NULL);", "P0002")
        b.execute("ROLLBACK;")
        a.execute("COMMIT;")
        expected["topology_generations"].append({"dataset_id":bytea(1),"generation":1,"previous_generation":0,"created_at_us":1,"sealed":True})
        expected["topology_streams"].append({"dataset_id":bytea(1),"topology_generation":1,"shard_id":0,"owner_epoch":1,"handoff_last_complete_sequence":0})
        unchanged_except(state, database, expected)
        complete(active_case, {"uncommitted_parent_count":0,"append_sqlstate":hidden["sqlstate"],"committed_shards":[0],"closed_bound":0})
        close_sessions()

        active_case = "topology_late_append"
        a, b = session("sealed_a"), session("sealed_b")
        a.execute("BEGIN;")
        a.execute("SELECT generation::text FROM control.topology_generations WHERE generation=1 FOR UPDATE;")
        b.execute("BEGIN;")
        b.start(f"INSERT INTO control.topology_streams VALUES({identity(1)},1,7,1,NULL);")
        barrier = wait_blocked(state,database,b,a)
        a.execute("COMMIT;")
        late = b.expect(b.finish(),"23514")
        b.execute("ROLLBACK;")
        unchanged_except(state,database,expected)
        complete(active_case,{"barrier":barrier,"append_sqlstate":late["sqlstate"],"all_rows_unchanged":True})
        close_sessions()

        for case, job, resource, release_first in [("job_before_release",40,21,False),("release_before_job",41,22,True)]:
            active_case = case
            a,b = session(case + "_a"),session(case + "_b")
            a.execute("BEGIN;")
            b.execute("BEGIN;")
            a.execute(release(resource) if release_first else queue(job,resource),rows=1)
            b.start(queue(job,resource) if release_first else release(resource))
            barrier = wait_blocked(state,database,b,a)
            a.execute("COMMIT;")
            after = b.expect(b.finish(),"23514" if release_first else "00000",None if release_first else 1)
            b.execute("ROLLBACK;" if release_first else "COMMIT;")
            owner = row(expected,"owner_epochs","resource_id",bytea(resource))
            owner.update(state="released",released_at_us=2,exclusion_method="joined",exclusion_digest="\\x" + format(60,"064x"))
            if not release_first:
                job_row = row(expected,"jobs","job_id",bytea(job))
                job_row.update(state="queued",job_revision=2,owner_resource_id=bytea(resource),owner_epoch=1,owner_incarnation=bytea(30))
            unchanged_except(state,database,expected)
            # A later independent transaction also cannot change the job after release.
            stale = a.execute(f"UPDATE control.jobs SET job_revision=job_revision+1,owner_resource_id={identity(resource)},owner_epoch=1,owner_incarnation={identity(30)} WHERE job_id={identity(job)};","23514")
            unchanged_except(state,database,expected)
            complete(active_case,{"barrier":barrier,"contender_sqlstate":after["sqlstate"],"later_update_sqlstate":stale["sqlstate"],"job_state":"accepted" if release_first else "queued"})
            close_sessions()

        active_case = "final_inspection"
        final = lab.migrate(state,"inspect",candidate)
        actual = unchanged_except(state,database,expected)
        require(final.returncode == 0 and json.loads(final.stdout).get("current_version") == "1", "real-cli-final-inspect")
        require(actual["schema_versions"] == initial_receipt,"ledger-mutated")
        lab.write_json(directory / "expected-rows.json", expected)
        lab.write_json(directory / "observed-rows.json", actual)
        complete(active_case,{"exit":final.returncode,"receipt_unchanged":True,"full_relation_rows_match":True,"schema_and_data_sha256":lab.schema_digest(state,database)})
    except BaseException as error:
        status = "FAIL" if isinstance(error,Mismatch) else "UNKNOWN"
        report["status"] = status
        report["failure_class"] = type(error).__name__
        report["failure_reason"] = str(error) if isinstance(error,(Mismatch,Incomplete)) else "unclassified-or-interrupted"
        if active_case:
            report["cases"][active_case] = {"status":status,"reason":str(error) if isinstance(error,(Mismatch,Incomplete)) else "unclassified-or-interrupted"}
        raise
    finally:
        try:
            close_sessions()
            report["cleanup"] = "PASS"
        except BaseException as error:
            report["cleanup"] = "UNKNOWN"
            report["cleanup_failure_class"] = type(error).__name__
            report["status"] = "UNKNOWN"
        counts = {status:sum(c["status"] == status for c in report["cases"].values()) for status in ("PASS","FAIL","UNKNOWN","NOT_RUN")}
        report["counts"] = counts
        if counts["PASS"] == len(CASES) and report["cleanup"] == "PASS":
            report["status"] = "PASS"
        report["finished_at_unix"] = time.time()
        lab.write_json(output,report)
        os.close(lock)
    require(report["status"] == "PASS","concurrency-run-incomplete")
    print(json.dumps({"status":report["status"],"cases":len(CASES),"output":str(output)}))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root",type=Path,required=True)
    parser.add_argument("--run-name",default="races")
    arguments = parser.parse_args()
    run(arguments.root,arguments.run_name)
