#!/usr/bin/env python3
"""Real enrollment COMMIT and lock schedules on clones of the retained database."""

import datetime
import hashlib
import json
import os
from pathlib import Path
import re
import secrets
import shutil
import sys
import time

sys.dont_write_bytecode = True
SOURCE = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(SOURCE / "experiments/control-migrations"))
import lab
from fault_cases import Process
from concurrency_cases import Session

sys.path.insert(0, str(SOURCE / "experiments/network-transport"))
from inventory import projection

assert len(sys.argv) in (2, 3)
root = Path(sys.argv[1]).resolve(strict=True)
assert SOURCE == root / "source"
revision = lab.command(["git", "-C", str(SOURCE), "rev-parse", "HEAD"]).stdout.strip()
assert not lab.command(
    ["git", "-C", str(SOURCE), "status", "--porcelain", "--untracked-files=all"]
).stdout
assert not lab.command(
    [
        "git",
        "-C",
        str(SOURCE),
        "diff",
        "--name-only",
        "dae4d45",
        revision,
        "--",
        "apps/bigbrotr/src",
        "crates",
        "migrations",
        "Cargo.toml",
        "Cargo.lock",
        "rust-toolchain.toml",
    ]
).stdout
build = Path(sys.argv[2]).resolve(strict=True) if len(sys.argv) == 3 else root / "out/artifacts"
built = json.loads((build / "build-result.json").read_text())
assert built["probe_name"] == "relay_enrollment_fault_probe"
if built["source"] != revision:
    changed = lab.command(
        ["git", "-C", str(SOURCE), "diff", "--name-only", built["source"], revision]
    ).stdout.splitlines()
    assert changed and all(name.startswith(("docs/", "experiments/")) for name in changed), changed
assert (
    hashlib.sha256((build / built["probe_name"]).read_bytes()).hexdigest() == built["probe_sha256"]
)
original_root = root.parent / "relay-enrollment-recheck-49e7ed4"
pg = lab.load(original_root / "pg")
assert pg["owner"] == "aa5b1b49157bd12de8aa040cd3076a72" and pg["state"] == "stopped-retained"
original_file = original_root / "pg/enrollment-result.json"
original_bytes = original_file.read_bytes()
original = json.loads(original_bytes)
assert original["status"] == "PASS_INITIAL_ENROLLMENT_BOUNDARY"
expected_original = next(
    c["after"] for c in original["cases"] if c["name"] == "whole-ledger-effects-are-contained"
)
output = root / "faults"
output.mkdir(mode=0o700)
owner, label = secrets.token_hex(16), "org.bigbrotr.relay-enrollment-faults"
client = "bigbrotr-relay-enrollment-faults-" + revision[:7]
report = dict(
    format="bigbrotr-relay-enrollment-faults/1",
    source=revision,
    application_source=built["source"],
    build=built,
    status="RUNNING",
    owner=owner,
    label=label,
    cases=[],
    databases=[],
    client=None,
    postgres=pg["postgres"],
    postgres_owner=pg["owner"],
    original_report_sha256=hashlib.sha256(original_bytes).hexdigest(),
    not_run=[
        "all relational omission/corruption schedules",
        "installed HTTP/CLI enrollment",
        "current action issuer/restrictive drain",
        "signed socket acquisition/M1",
        "load/aged state",
        "arm64/release",
    ],
)
processes, sessions = [], []


def save():
    lab.write_json(output / "result.json", report)


def require(value, reason):
    if not value:
        raise RuntimeError(reason)


def record(name, **details):
    report["cases"].append(dict(name=name, status="PASS", **details))
    save()


def containers():
    return json.loads(lab.docker("inspect", *lab.docker("ps", "-aq").stdout.split()).stdout)


def tables(db):
    names = json.loads(
        lab.sql(
            pg,
            db,
            "SELECT json_agg(tablename ORDER BY tablename) FROM pg_tables WHERE schemaname='control'",
        )
    )
    parts = []
    for name in names:
        require(re.fullmatch("[a-z_]+", name), "table-name")
        parts.append(f"""SELECT '{name}' AS name,jsonb_build_object('rows',count(*),'sha256',
            encode(sha256(convert_to(coalesce(jsonb_agg(to_jsonb(t) ORDER BY to_jsonb(t)::text),'[]')::text,'UTF8')),'hex')) AS value
            FROM control.{name} t""")
    return json.loads(
        lab.sql(
            pg, db, "SELECT jsonb_object_agg(name,value) FROM (" + " UNION ALL ".join(parts) + ") s"
        )
    )


def unchanged(before, after, allowed=()):
    changed = {k for k in before if before[k] != after[k]}
    require(before.keys() == after.keys() and changed <= set(allowed), "unexpected-table-effects")
    return sorted(changed)


def prepare(name):
    report["active_case"] = name
    save()
    db = "bb_enroll_fault_" + revision[:7] + "_" + name
    require(re.fullmatch("[a-z0-9_]{1,63}", db), "database-name")
    require(
        lab.sql(pg, "postgres", f"SELECT count(*) FROM pg_database WHERE datname='{db}'") == "0",
        "existing-database",
    )
    lab.sql(pg, "postgres", f"CREATE DATABASE {db} OWNER bigbrotr_migrator TEMPLATE bb_enrollment")
    report["databases"].append(db)
    require(tables(db) == expected_original, "clone-differs-from-original")
    save()
    return db


def app(db, mode, nonce, port=5432):
    token = db + "-" + str(nonce)
    process = Process(
        [
            "docker",
            "exec",
            client,
            "/build/relay_enrollment_fault_probe",
            db,
            "/run/secrets/application",
            "/lab/policy.json",
            mode,
            str(port),
            str(nonce),
            "/lab/" + token,
        ]
    )
    processes.append((token, process))
    return process, token


def event(process, name, seconds=10):
    deadline = time.monotonic() + seconds
    with process.condition:
        while True:
            require(not process.overflow, "probe-output-overflow")
            end = process.stdout.find(b"\n", process.offset)
            if end >= 0:
                value = json.loads(bytes(process.stdout[process.offset : end]))
                process.offset = end + 1
                require(value.get("event") == name, "unexpected-probe-event")
                return value
            require(
                time.monotonic() < deadline
                and (process.process.poll() is None or process.threads[0].is_alive()),
                "missing-probe-event-" + name,
            )
            process.condition.wait(0.02)


def go(token, suffix="go"):
    (output / (token + "." + suffix)).touch(exist_ok=False)


def finish(process):
    value = event(process, "done")
    require(
        process.wait(10) == 0 and not process.stderr and value["driver_joined"], "probe-terminal"
    )
    return value["outcome"]


def observe(db, ready):
    pid, expires = ready["pid"], ready["session_expires"]
    value = json.loads(
        lab.sql(
            pg,
            db,
            f"""SELECT json_build_object(
        'pid',a.pid,'start',a.backend_start::text,'epoch',o.epoch,
        'owner_expired',o.lease_expires_at_us<=(EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint,
        'session_expired',s.expires_at_us<=(EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint,
        'session',encode(s.session_id,'hex'),'session_xmin',s.xmin::text,'revoked',s.revoked_at_us IS NOT NULL,
        'fenced',EXISTS(SELECT 1 FROM pg_locks WHERE pid=a.pid AND locktype='advisory'
            AND classid=1111651442::oid AND objid=1129599564::oid AND objsubid=2
            AND mode='ExclusiveLock' AND granted))
        FROM pg_stat_activity a JOIN control.owner_epochs o ON o.control_backend_pid=a.pid
            AND o.control_backend_start=a.backend_start
        CROSS JOIN control.browser_sessions s WHERE a.pid={pid} AND a.datname='{db}'
            AND o.state='active' AND s.principal_id=decode(repeat('10',16),'hex') AND s.expires_at_us={expires}""",
        )
    )
    require(
        value and value["pid"] == pid and value["fenced"] and not value["revoked"],
        "observed-live-custody",
    )
    return value


def enrollment(db):
    rows = json.loads(
        lab.sql(
            pg,
            db,
            """SELECT coalesce(json_agg(json_build_object(
        'command_key',encode(c.request_key,'hex'),'endpoint_id',encode(d.endpoint_id,'hex'),
        'access_scope_id',encode(d.access_scope_id,'hex'),'route_id',encode(d.route_id,'hex'),
        'enabled',d.enabled,'generation',d.generation::text,'accepted_at_us',c.accepted_at_us,
        'dataset',encode(d.dataset_id,'hex'),'epoch',d.owner_epoch,'domain',encode(a.allocation_domain,'hex'),
        'ordinal',a.ordinal::text,'route_bytes',encode(r.route_bytes,'hex'),'digest',encode(c.request_digest,'hex'),
        'config',c.expected_config_revision::text,'policy',c.policy_revision::text,
        'current_matches',(e.dataset_id,e.endpoint_id,e.access_scope_id,e.route_id,e.enabled,e.generation)
            IS NOT DISTINCT FROM (d.dataset_id,d.endpoint_id,d.access_scope_id,d.route_id,d.enabled,d.generation),
        'identity_matches',a.dataset_id=d.dataset_id AND a.dictionary_kind=4 AND a.access_class='public'
            AND r.dataset_id=d.dataset_id AND r.endpoint_id=d.endpoint_id AND r.route_version=1,
        'atomic',c.xmin=d.xmin AND d.xmin=e.xmin AND e.xmin=a.xmin AND a.xmin=r.xmin AND r.xmin=m.xmin,
        'audit',m.actor_id=c.actor_id AND m.dataset_id=c.dataset_id AND m.subject_id=c.request_key
            AND m.action=c.action AND m.created_at_us=c.accepted_at_us AND m.outcome='accepted'
            AND m.config_revision=c.expected_config_revision AND m.policy_revision=c.policy_revision)), '[]')
        FROM control.management_commands c JOIN control.relay_enrollment_changes d USING(actor_id,request_key)
        JOIN control.relay_enrollments e USING(actor_id,request_key)
        JOIN control.relay_access_contexts a ON a.access_scope_id=d.access_scope_id
        JOIN control.network_routes r ON r.route_id=d.route_id
        JOIN control.management_audit m ON m.audit_id=c.audit_id
        WHERE c.actor_id=decode(repeat('10',16),'hex') AND c.request_key=decode(repeat('31',16),'hex')""",
        )
    )
    for row in rows:
        require(
            row["domain"] == "37" * 16
            and row["ordinal"] == "99"
            and row["route_bytes"] == "0104180a020300"
            and row["generation"] == "1"
            and row["enabled"]
            and row["atomic"]
            and row["audit"]
            and row["current_matches"]
            and row["identity_matches"],
            "new-command-closure",
        )
        data = b"bigbrotr/relay-enrollment-change/1\0" + bytes.fromhex(
            row["dataset"] + "10" * 16 + "31" * 16 + row["endpoint_id"] + row["dataset"]
        )
        data += (
            bytes.fromhex("0004" + "37" * 16)
            + (99).to_bytes(8, "big")
            + b"\x01"
            + (0).to_bytes(8, "big")
        )
        data += b"".join(
            int(row[n]).to_bytes(8, "big") for n in ["config", "policy"]
        ) + bytes.fromhex("0104180a020300")
        require(hashlib.sha256(data).hexdigest() == row["digest"], "independent-literal-digest")
    return rows


def verify_receipt(result, rows, created):
    require(len(rows) == 1, "one-command")
    expected = {
        k: rows[0][k]
        for k in [
            "command_key",
            "endpoint_id",
            "access_scope_id",
            "route_id",
            "enabled",
            "generation",
            "accepted_at_us",
        ]
    }
    expected["created"] = created
    require(result == dict(receipt=expected, commit_started=created), "exact-recovered-receipt")


def blocked(db, waiter, holder):
    deadline = time.monotonic() + 3
    while True:
        row = json.loads(
            lab.sql(
                pg,
                db,
                f"SELECT json_build_object('pid',pid,'start',backend_start::text,'wait',wait_event_type,'blockers',pg_blocking_pids(pid)) FROM pg_stat_activity WHERE datname='{db}' AND pid={waiter}",
            )
        )
        if row and row["wait"] == "Lock" and holder in row["blockers"]:
            return row
        require(time.monotonic() < deadline, "required-lock-barrier-not-observed")
        time.sleep(0.02)


def mutation_for(kind, observed):
    """One existing row lock per injected writer; no reverse acquisition."""
    if kind == "policy":
        lock = "SELECT dataset_id FROM control.datasets FOR UPDATE;"
        mutation = """INSERT INTO control.config_revisions SELECT revision+1,dataset_id,revision,schema_version,authority_mode,document,document_digest,accepted_source_digest,accepted_at_us+1,actor_id FROM control.config_revisions WHERE revision=1;
            INSERT INTO control.policy_revisions SELECT revision+1,dataset_id,revision,2,schema_version,document,document_digest,accepted_at_us+1,actor_id FROM control.policy_revisions WHERE revision=1;
            UPDATE control.datasets SET config_revision=2,policy_revision=2;"""
        allowed = ["datasets", "config_revisions", "policy_revisions"]
    elif kind == "revoked":
        lock = f"SELECT session_id FROM control.browser_sessions WHERE session_id=decode('{observed['session']}','hex') FOR UPDATE;"
        mutation = f"UPDATE control.browser_sessions SET revoked_at_us=(EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint WHERE session_id=decode('{observed['session']}','hex');"
        allowed = ["browser_sessions"]
    else:
        lock = "SELECT principal_id FROM control.local_accounts WHERE username='alice' FOR UPDATE;"
        if kind == "permission":
            mutation = "DELETE FROM control.principal_grants WHERE action='policy.change'; UPDATE control.local_accounts SET grant_revision=grant_revision+1 WHERE username='alice';"
            allowed = ["local_accounts", "principal_grants"]
        else:
            mutation = "UPDATE control.local_accounts SET active=false WHERE username='alice';"
            if kind == "authentication":
                mutation += "UPDATE control.local_accounts SET active=true WHERE username='alice';"
            allowed = ["local_accounts", "browser_sessions"]
    return lock, mutation, allowed


prior = containers()
target = next(c for c in prior if c["Name"] == "/" + pg["postgres"])
assert (
    target["Config"]["Labels"][lab.LABEL] == pg["owner"] and target["State"]["Status"] == "exited"
)
assert not any(c["State"]["Running"] and "bigbrotr" in c["Name"] for c in prior)
assert shutil.disk_usage(root).free > 2 * 1024**3
assert lab.docker("inspect", client, check=False).returncode != 0
report.update(
    started_at=datetime.datetime.now(datetime.timezone.utc).isoformat(),
    free_bytes_before=shutil.disk_usage(root).free,
    postgres_id=target["Id"],
)
lab.write_json(output / "preflight.json", [projection(c) for c in prior])
save()
try:
    lab.docker("start", target["Id"])
    deadline = time.monotonic() + 30
    while lab.docker("exec", target["Id"], "pg_isready", "-U", "postgres", check=False).returncode:
        require(time.monotonic() < deadline, "postgres-readiness")
        time.sleep(0.1)
    require(tables("bb_enrollment") == expected_original, "original-state-before")
    # The initial enrollment journey acquired Control once. Its baseline differs
    # from the older registry fixture, which deliberately replaced its owner.
    baseline_epochs = json.loads(
        lab.sql(
            pg,
            "bb_enrollment",
            "SELECT json_agg(epoch ORDER BY epoch) FROM control.owner_epochs WHERE role='control'",
        )
    )
    require(baseline_epochs == [1], "original-owner-epochs")
    report["original_owner_epochs"] = baseline_epochs
    save()
    shutil.copyfile(original_root / "pg/policy.json", output / "policy.json")
    ident = lab.docker(
        "create",
        "--init",
        "--name",
        client,
        "--label",
        label + "=" + owner,
        "--network",
        "container:" + pg["postgres"],
        "--user",
        f"{os.getuid()}:{os.getgid()}",
        "--cpus",
        "2",
        "--memory",
        "1g",
        "--memory-swap",
        "1g",
        "--pids-limit",
        "128",
        "--read-only",
        "--cap-drop",
        "ALL",
        "--security-opt",
        "no-new-privileges",
        "--mount",
        f"type=bind,src={output},dst=/lab",
        "--mount",
        f"type=bind,src={original_root}/pg/secrets,dst=/run/secrets,readonly",
        "--mount",
        f"type=bind,src={build},dst=/build,readonly",
        "--mount",
        f"type=bind,src={SOURCE},dst=/source,readonly",
        pg["client_image"],
        "python3",
        "-c",
        "import time; time.sleep(900)",
    ).stdout.strip()
    report["client"] = dict(id=ident, name=client)
    save()
    lab.docker("start", ident)
    domain_tables = [
        "relay_access_contexts",
        "network_routes",
        "relay_enrollments",
        "relay_enrollment_changes",
        "management_commands",
        "management_audit",
    ]
    for name, proxy_mode, mode in [
        ("pass", "pass", "accept"),
        ("before", "before-forward", "unknown"),
        ("after", "after-complete", "unknown"),
        ("cancel_close", "hold", "cancel"),
        ("cancel_release", "hold", "cancel"),
        ("owner_expiry", "hold", "owner-expiry"),
        ("session_expiry", "hold", "session-expiry"),
    ]:
        db = prepare(name)
        proxy = Process(
            [
                "docker",
                "exec",
                "-i",
                client,
                "python3",
                "-B",
                "/source/experiments/control-migrations/fault_proxy.py",
                "--upstream-port",
                "5432",
                "--mode",
                proxy_mode,
                "--commit-number",
                "4",
                "--deadline-seconds",
                "40",
            ],
            input_pipe=True,
        )
        processes.append((db + "-proxy", proxy))
        ready = proxy.event("ready", 10)
        require(
            ready["proxy_sha256"]
            == hashlib.sha256(
                (SOURCE / "experiments/control-migrations/fault_proxy.py").read_bytes()
            ).hexdigest(),
            "proxy-source",
        )
        process, token = app(db, mode, 11, ready["listen_port"])
        acquired = event(process, "ready")
        require(acquired["epoch"] == 2, "first-new-owner-epoch")
        before = tables(db)
        observations = [observe(db, acquired)]
        require(
            not observations[0]["owner_expired"] and not observations[0]["session_expired"],
            "already-expired-before-operation",
        )
        go(token)
        if proxy_mode == "hold":
            proxy.event("held", 10)
            require(tables(db) == before and enrollment(db) == [], "uncommitted-changes-visible")
            if mode == "cancel":
                require(event(process, "cancelled", 5)["commit_started"], "not-cancelled-at-COMMIT")
            else:
                field = "owner_expired" if mode == "owner-expiry" else "session_expired"
                limit = time.monotonic() + 5
                while True:
                    observations.append(observe(db, acquired))
                    if observations[-1][field]:
                        break
                    require(time.monotonic() < limit, "expiry-not-observed")
                    time.sleep(0.05)
                other = "session_expired" if field == "owner_expired" else "owner_expired"
                require(not observations[-1][other], "expiry-cause-not-isolated")
            contender, _ = app(db, "busy", 12)
            require(
                event(contender, "busy")["commit_started"] is False
                and contender.wait(10) == 0
                and not contender.stderr,
                "competing-owner-not-refused",
            )
            require(tables(db) == before, "held-or-busy-owner-changed-rows")
            proxy.send("close" if name == "cancel_close" else "release")
            if mode == "cancel":
                go(token, "finish")
        result = finish(process)
        wire = proxy.event("receipt", 10)
        require(proxy.wait(5) == 0 and not proxy.stderr, "proxy-terminal")
        counts = [
            wire[k]
            for k in [
                "commits_seen",
                "commits_forwarded",
                "server_commit_complete",
                "client_commit_complete",
                "faults",
            ]
        ]
        wanted = {
            "pass": [4, 4, 4, 4, 0],
            "before": [4, 3, 3, 3, 1],
            "after": [4, 4, 4, 3, 1],
            "cancel_close": [4, 3, 3, 3, 1],
            "cancel_release": [4, 4, 4, 4, 1],
            "owner_expiry": [4, 4, 3, 3, 1],
            "session_expiry": [4, 4, 3, 3, 1],
        }[name]
        require(counts == wanted, "exact-selected-COMMIT-boundary")
        rows = enrollment(db)
        committed = name in ("pass", "after", "cancel_release")
        require(len(rows) == int(committed), "durable-command-outcome")
        after = tables(db)
        changed = unchanged(before, after, domain_tables if committed else [])
        if committed:
            require(
                rows[0]["epoch"] == 2 and rows[0]["config"] == rows[0]["policy"] == "1",
                "first-attribution",
            )
            for relation in domain_tables:
                require(
                    after[relation]["rows"] == before[relation]["rows"] + 1, "one-atomic-insertion"
                )
        if name == "pass":
            verify_receipt(result, rows, True)
        recovered, recovery_token = app(db, "accept", 13)
        new_owner = event(recovered, "ready")
        require(new_owner["epoch"] == 3, "recovery-owner-epoch")
        recovery_before = tables(db)
        go(recovery_token)
        recovery = finish(recovered)
        durable = enrollment(db)
        verify_receipt(recovery, durable, not committed)
        recovery_after = tables(db)
        unchanged(recovery_before, recovery_after, [] if committed else domain_tables)
        require(
            durable == rows if committed else durable[0]["epoch"] == 3,
            "original-receipt-attribution",
        )
        record(
            "COMMIT-" + name,
            outcome=result,
            wire=wire,
            observations=observations,
            before=before,
            after=after,
            changed=changed,
            rows=rows,
            recovery_owner=new_owner,
            recovery=recovery,
            recovery_before=recovery_before,
            recovery_after=recovery_after,
            durable=durable,
        )
    # Writers are explicit privileged fault injections, not an installed policy
    # command. Each requests only its recorded row; it never reverses lock order.
    for kind in ["policy", "permission", "revoked", "inactive", "authentication"]:
        db = prepare(kind)
        process, token = app(db, kind, 11)
        acquired = event(process, "ready")
        initial = tables(db)
        observed = observe(db, acquired)
        writer = Session(pg, db, "enrollment-" + kind, sessions)
        writer.execute("BEGIN;")
        lock, mutation, allowed = mutation_for(kind, observed)
        writer.execute(lock)
        writer.execute(mutation)
        go(token)
        barrier = blocked(db, acquired["pid"], writer.backend["pid"])
        require(tables(db) == initial, "uncommitted-revocation-visible")
        writer.execute("COMMIT;")
        outcome = finish(process)
        after = tables(db)
        changed = unchanged(initial, after, allowed)
        require(enrollment(db) == [], "stale-command-accepted")
        record(
            "writer-first-" + kind,
            barrier=barrier,
            outcome=outcome,
            before=initial,
            after=after,
            changed=changed,
        )
        writer.close(lambda: lab.load(original_root / "pg"))
        sessions.remove(writer)
    for kind in ["policy", "permission", "revoked", "inactive", "authentication"]:
        db = prepare("accept_first_" + kind)
        proxy = Process(
            [
                "docker",
                "exec",
                "-i",
                client,
                "python3",
                "-B",
                "/source/experiments/control-migrations/fault_proxy.py",
                "--upstream-port",
                "5432",
                "--mode",
                "hold",
                "--commit-number",
                "4",
                "--deadline-seconds",
                "40",
            ],
            input_pipe=True,
        )
        processes.append((db + "-proxy", proxy))
        proxy_ready = proxy.event("ready", 10)
        require(
            proxy_ready["proxy_sha256"]
            == hashlib.sha256(
                (SOURCE / "experiments/control-migrations/fault_proxy.py").read_bytes()
            ).hexdigest(),
            "proxy-source",
        )
        process, token = app(db, "accept", 11, proxy_ready["listen_port"])
        acquired = event(process, "ready")
        initial = tables(db)
        observed = observe(db, acquired)
        go(token)
        proxy.event("held", 10)
        require(tables(db) == initial, "held-command-visible")
        writer = Session(pg, db, "enrollment-after-" + kind, sessions)
        writer.execute("BEGIN;")
        lock, mutation, allowed = mutation_for(kind, observed)
        writer.start(lock + mutation + "COMMIT;")
        barrier = blocked(db, writer.backend["pid"], acquired["pid"])
        require(tables(db) == initial, "blocked-writer-visible")
        proxy.send("release")
        outcome = finish(process)
        writer.expect(writer.finish())
        wire = proxy.event("receipt", 10)
        require(proxy.wait(5) == 0 and not proxy.stderr, "proxy-terminal")
        require(
            [
                wire[k]
                for k in [
                    "commits_seen",
                    "commits_forwarded",
                    "server_commit_complete",
                    "client_commit_complete",
                    "faults",
                ]
            ]
            == [4, 4, 4, 4, 1],
            "accept-first-COMMIT-counts",
        )
        rows = enrollment(db)
        verify_receipt(outcome, rows, True)
        require(
            rows[0]["epoch"] == 2 and rows[0]["config"] == rows[0]["policy"] == "1",
            "accept-before-writer-attribution",
        )
        after = tables(db)
        changed = unchanged(initial, after, domain_tables + allowed)
        require(all(initial[k] != after[k] for k in allowed), "writer-had-no-durable-effect")
        record(
            "accept-first-" + kind,
            barrier=barrier,
            outcome=outcome,
            wire=wire,
            rows=rows,
            before=initial,
            after=after,
            changed=changed,
        )
        writer.close(lambda: lab.load(original_root / "pg"))
        sessions.remove(writer)
    db = prepare("writer_rollback")
    process, token = app(db, "accept", 11)
    acquired = event(process, "ready")
    initial = tables(db)
    writer = Session(pg, db, "enrollment-writer-rollback", sessions)
    writer.execute("BEGIN;")
    lock, mutation, _ = mutation_for("policy", None)
    writer.execute(lock)
    writer.execute(mutation)
    go(token)
    barrier = blocked(db, acquired["pid"], writer.backend["pid"])
    writer.execute("ROLLBACK;")
    outcome = finish(process)
    rows = enrollment(db)
    verify_receipt(outcome, rows, True)
    require(rows[0]["config"] == rows[0]["policy"] == "1", "rolled-back-policy-observed")
    after = tables(db)
    changed = unchanged(initial, after, domain_tables)
    record(
        "writer-rollback",
        barrier=barrier,
        outcome=outcome,
        rows=rows,
        before=initial,
        after=after,
        changed=changed,
    )
    writer.close(lambda: lab.load(original_root / "pg"))
    sessions.remove(writer)
    require(
        tables("bb_enrollment") == expected_original
        and original_file.read_bytes() == original_bytes,
        "original-state-or-report-changed",
    )
    require(
        lab.sql(
            pg, "postgres", "SELECT count(*) FROM pg_stat_activity WHERE usename='bigbrotr_control'"
        )
        == "0",
        "application-backends-remain",
    )
    require(
        len(report["cases"]) == 18 and all(c["status"] == "PASS" for c in report["cases"]),
        "incomplete-case-set",
    )
    report["active_case"] = None
    report["status"] = "PASS_SCOPED_ENROLLMENT_FAULTS"
except BaseException as error:
    report.update(status="FAIL", failure=type(error).__name__ + ": " + str(error))
    raise
finally:
    errors = []
    for session in sessions:
        try:
            session.close(lambda: lab.load(original_root / "pg"))
        except BaseException as error:
            errors.append("session-" + type(error).__name__)
    if report["client"]:
        try:
            item = json.loads(lab.docker("inspect", report["client"]["id"]).stdout)[0]
            require(item["Config"]["Labels"][label] == owner, "client-owner")
            if item["State"]["Running"]:
                lab.docker("stop", "--time", "3", item["Id"])
        except BaseException as error:
            errors.append("client-" + type(error).__name__)
    for name, process in processes:
        try:
            process.stop()
        except BaseException as error:
            errors.append("process-" + type(error).__name__)
        (output / (name + ".stdout")).write_bytes(process.stdout)
        (output / (name + ".stderr")).write_bytes(process.stderr)
    try:
        item = json.loads(lab.docker("inspect", target["Id"]).stdout)[0]
        require(item["Config"]["Labels"][lab.LABEL] == pg["owner"], "postgres-owner")
        if item["State"]["Running"]:
            lab.docker("stop", "--time", "10", item["Id"])
    except BaseException as error:
        errors.append("postgres-" + type(error).__name__)
    after = {c["Id"]: c for c in containers()}
    report["unrelated_changed"] = [
        c["Id"]
        for c in prior
        if c["Id"] != target["Id"]
        and (c["Id"] not in after or projection(c) != projection(after[c["Id"]]))
    ]
    owned_ids = [target["Id"]] + ([report["client"]["id"]] if report["client"] else [])
    report["missing_owned"] = [i for i in owned_ids if i not in after]
    report["terminal"] = [projection(after[i]) for i in owned_ids if i in after]
    report["cleanup_errors"] = errors
    if (
        errors
        or report["unrelated_changed"]
        or report["missing_owned"]
        or any(
            c["State"]["Running"] or c["State"]["Pid"] or c["State"]["OOMKilled"]
            for c in report["terminal"]
        )
    ):
        report["status"] = "INCOMPLETE_LAB_AUDIT"
    report.update(
        finished_at=datetime.datetime.now(datetime.timezone.utc).isoformat(),
        free_bytes_after=shutil.disk_usage(root).free,
    )
    save()
print(json.dumps(dict(status=report["status"], cases=len(report["cases"]), output=str(output))))
require(report["status"] == "PASS_SCOPED_ENROLLMENT_FAULTS", "incomplete-enrollment-faults")
