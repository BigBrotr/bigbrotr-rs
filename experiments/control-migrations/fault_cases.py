#!/usr/bin/env python3
"""Real CLI/PG fault orchestration for an existing owned lab; no engine simulation."""
import argparse
import hashlib
import json
import os
import platform
from pathlib import Path
import re
import secrets
import subprocess
import threading
import time

import lab

FAULT_LABEL = "org.bigbrotr.control-migration-fault"
CASES = ("pass", "before", "after", "release", "close", "race")
TABLES = sorted(("schema_versions", "datasets", "topology_generations", "topology_streams",
                 "role_instances", "owner_epochs", "config_revisions", "policy_revisions",
                 "jobs", "job_input_bounds"))
LOCK_NAMESPACE, LOCK_MIGRATIONS = 0x42427472, 0x4D494752


class Incomplete(Exception):
    """A fixed reason code; the database outcome can remain unknown."""


class Failed(Exception):
    """A fixed reason code for a concrete mismatched observation."""


def require(condition, reason):
    if not condition:
        raise Failed(reason)


def digest(path):
    result = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            result.update(chunk)
    return result.hexdigest()


def exclusive(path, text):
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, "w") as output:
        output.write(text)


def safe_event(item):
    allowed = {"format", "event", "mode", "elapsed_ns", "held", "hold_action", "faults",
               "commits_seen", "commits_forwarded", "server_commit_complete", "client_commit_complete",
               "listen_host", "listen_port", "upstream_port", "pid", "proxy_sha256", "max_frame_bytes",
               "max_wire_bytes", "max_messages", "deadline_seconds", "reason", "frames", "tags",
               "declared_wire_bytes"}
    if not isinstance(item, dict) or set(item) - allowed:
        raise Incomplete("proxy-metadata-fields")
    if item.get("format") != "bigbrotr-pg-fault-proxy/1" or item.get("event") not in {
        "ready", "held", "released", "fault", "status", "receipt"
    } or item.get("mode") not in {"pass", "before-forward", "after-complete", "hold"}:
        raise Incomplete("proxy-metadata-format")
    for key, value in item.items():
        if key in {"format", "event", "mode"}:
            continue
        if key in {"reason", "hold_action"}:
            if value is not None and (not isinstance(value, str) or not re.fullmatch(r"[a-z-]{1,40}", value)):
                raise Incomplete("proxy-metadata-reason")
        elif key == "listen_host":
            if value != "127.0.0.1":
                raise Incomplete("proxy-address")
        elif key == "proxy_sha256":
            if not isinstance(value, str) or not re.fullmatch(r"[0-9a-f]{64}", value):
                raise Incomplete("proxy-source")
        elif key == "frames":
            if not isinstance(value, dict) or set(value) - {"frontend", "backend"} or any(type(n) is not int or not 0 <= n <= 1000000 for n in value.values()):
                raise Incomplete("proxy-frame-counts")
        elif key == "tags":
            if not isinstance(value, dict) or set(value) != {"frontend", "backend"}:
                raise Incomplete("proxy-tag-counts")
            for counts in value.values():
                if not isinstance(counts, dict) or len(counts) > 256 or any(not re.fullmatch(r"[0-9a-f]{2}", tag) or type(n) is not int or not 0 <= n <= 1000000 for tag, n in counts.items()):
                    raise Incomplete("proxy-tag-counts")
        elif not isinstance(value, (int, float)) or not 0 <= value <= 10**18:
            raise Incomplete("proxy-metadata-number")
    return item


class Process:
    """Finite local docker-exec wrapper; output is capped and never printed raw."""
    def __init__(self, args, *, input_pipe=False, limit=65536):
        self.process = subprocess.Popen(args, stdin=subprocess.PIPE if input_pipe else subprocess.DEVNULL,
                                        stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        self.stdout, self.stderr = bytearray(), bytearray()
        self.condition = threading.Condition()
        self.overflow = False
        self.offset = 0
        self.events = []
        self.threads = []
        for stream, data in ((self.process.stdout, self.stdout), (self.process.stderr, self.stderr)):
            def consume(stream=stream, data=data):
                try:
                    while True:
                        chunk = stream.read1(4096)
                        if not chunk:
                            break
                        with self.condition:
                            if len(data) + len(chunk) > limit:
                                self.overflow = True
                                self.condition.notify_all()
                                return
                            data.extend(chunk)
                            self.condition.notify_all()
                finally:
                    stream.close()
                    with self.condition:
                        self.condition.notify_all()
            thread = threading.Thread(target=consume, daemon=True)
            thread.start()
            self.threads.append(thread)

    def event(self, wanted, timeout):
        end = time.monotonic() + timeout
        with self.condition:
            while True:
                if self.overflow:
                    raise Incomplete("process-output-limit")
                newline = self.stdout.find(b"\n", self.offset)
                if newline >= 0:
                    raw = bytes(self.stdout[self.offset:newline])
                    self.offset = newline + 1
                    try:
                        event = safe_event(json.loads(raw))
                    except (ValueError, UnicodeError, TypeError):
                        raise Incomplete("proxy-output-format") from None
                    self.events.append(event)
                    if event["event"] == wanted:
                        return event
                else:
                    remaining = end - time.monotonic()
                    if remaining <= 0:
                        raise Incomplete("proxy-event-timeout")
                    if self.process.poll() is not None and not self.threads[0].is_alive():
                        raise Incomplete("proxy-event-missing")
                    self.condition.wait(min(remaining, 0.1))

    def send(self, action):
        if action not in ("status", "release", "close"):
            raise Incomplete("invalid-proxy-control")
        try:
            self.process.stdin.write((action + "\n").encode())
            self.process.stdin.flush()
        except (BrokenPipeError, OSError):
            raise Incomplete("proxy-control-closed") from None

    def wait(self, timeout):
        end = time.monotonic() + timeout
        while self.process.poll() is None:
            if self.overflow:
                raise Incomplete("process-output-limit")
            if time.monotonic() >= end:
                raise Incomplete("process-timeout")
            time.sleep(0.02)
        for thread in self.threads:
            thread.join(max(0, end - time.monotonic()))
        if self.overflow or any(thread.is_alive() for thread in self.threads):
            raise Incomplete("process-output-incomplete")
        return self.process.returncode

    def stop(self):
        # Terminating docker exec alone is NOT remote-process or PG exclusion.
        # The enclosing finally also stops the exactly labeled owned container.
        if self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(timeout=1)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=1)
        if self.process.stdin:
            self.process.stdin.close()
        for thread in self.threads:
            thread.join(1)


def migration_args(container, action, path):
    return ["docker", "exec", container, "/build/bigbrotr", "migrate", action, "--file", path,
            "--user", "bigbrotr_migrator", "--password-ref", "file:/lab/secrets/migrator", "--target", "1"]


def validate_cli(output):
    allowed = {"format", "scope", "status", "source_version", "current_version", "target_version",
               "applied_count", "steps", "dataset_initialization", "release_upgrade_qualification", "resolution"}
    if output is not None:
        require(isinstance(output, dict) and not set(output) - allowed
                and output.get("format") == "bigbrotr-control-migration/1"
                and output.get("scope") == "postgres-control" and output.get("target_version") == "1", "cli-output-envelope")
    return output


def validate_inspection(result, present, expected):
    value = result["output"] or {}
    require(result["exit"] == 0 and value.get("status") == "inspected"
            and value.get("source_version") == ("1" if present else None)
            and value.get("current_version") == ("1" if present else None)
            and value.get("applied_count") == "0", "resolution-mismatch")
    expected_steps = [] if present else [{"version": "1", "name": "0001_control.sql", "sha256": expected}]
    require(value.get("steps") == expected_steps, "inspection-plan-mismatch")


def client_result(process, timeout=80):
    code = process.wait(timeout)
    try:
        output = json.loads(process.stdout) if process.stdout else None
    except (ValueError, UnicodeError):
        raise Incomplete("cli-output-format") from None
    # The selected CLI reports only this closed migration envelope. Retain no
    # unknown fields or raw stderr, which may contain future sensitive details.
    validate_cli(output)
    return {"exit": code, "output": output, "stderr_nonempty": bool(process.stderr),
            "backend_refusal": b"control migration backend unavailable or refused the operation" in process.stderr}


def direct(state, action, path):
    result = lab.migrate(state, action, path)
    try:
        output = json.loads(result.stdout) if result.stdout else None
    except ValueError:
        raise Incomplete("direct-cli-format") from None
    return {"exit": result.returncode, "output": validate_cli(output), "stderr_nonempty": bool(result.stderr)}


def observed(state, database):
    count = lab.sql(state, database, "SELECT count(*) FROM pg_namespace WHERE nspname='control';")
    require(count in ("0", "1"), "namespace-count")
    if count == "0":
        return {"namespace_count": 0, "ledger": [], "tables": [], "schema_and_data_sha256": None}
    tables = json.loads(lab.sql(state, database, "SELECT coalesce(json_agg(tablename ORDER BY tablename),'[]') FROM pg_tables WHERE schemaname='control';"))
    ledger = json.loads(lab.sql(state, database, "SELECT coalesce(json_agg(v ORDER BY migration),'[]') FROM (SELECT engine,migration::text,name,encode(checksum,'hex') AS checksum,reader_min::text,reader_max::text,writer_floor::text,applied_at_us::text,applied_by FROM control.schema_versions LIMIT 2) v;"))
    return {"namespace_count": 1, "ledger": ledger, "tables": tables,
            "schema_and_data_sha256": lab.schema_digest(state, database)}


def validate_present(value, expected):
    require(value["namespace_count"] == 1 and value["tables"] == TABLES and len(value["ledger"]) == 1, "external-schema")
    row = value["ledger"][0]
    require(all(row[key] == field for key, field in {
        "engine": "postgres-control", "migration": "1", "name": "0001_control.sql", "checksum": expected,
        "reader_min": "1", "reader_max": "1", "writer_floor": "1", "applied_by": "bigbrotr_migrator",
    }.items()), "external-receipt")


def backends(state, database):
    # Names are generated from the closed run-name grammar; no query text or
    # backend authentication key is selected. Separate psql sees fresh snapshots.
    return json.loads(lab.sql(state, "postgres", f"""
SELECT coalesce(json_agg(json_build_object('pid',a.pid,'state',a.state,
    'wait_type',a.wait_event_type,'wait_event',a.wait_event,'locks',(
      SELECT coalesce(json_agg(json_build_object('mode',l.mode,'granted',l.granted)),'[]')
      FROM pg_locks l WHERE l.pid=a.pid AND l.locktype='advisory'
        AND l.classid={LOCK_NAMESPACE} AND l.objid={LOCK_MIGRATIONS} AND l.objsubid=2
    )) ORDER BY a.pid),'[]') FROM pg_stat_activity a
WHERE a.datname='{database}' AND a.usename='bigbrotr_migrator';
"""))


def lock_holders(rows):
    return [row for row in rows if row["state"] == "idle in transaction"
            and {"mode": "ExclusiveLock", "granted": True} in row["locks"]]


def await_waiter(state, database, owner, mode="ShareLock", timeout=2.5):
    end = time.monotonic() + timeout
    while True:
        rows = backends(state, database)
        waiting = [row for row in rows if row["pid"] != owner and row["wait_type"] == "Lock"
                   and row["wait_event"] == "advisory" and {"mode": mode, "granted": False} in row["locks"]]
        if waiting and any(row["pid"] == owner for row in lock_holders(rows)):
            return rows
        if time.monotonic() >= end:
            raise Incomplete("inspector-wait-not-observed")
        time.sleep(0.05)


def await_no_backends(state, database):
    end = time.monotonic() + 5
    while True:
        rows = backends(state, database)
        if not rows:
            return rows
        if time.monotonic() >= end:
            raise Incomplete("database-backend-still-present")
        time.sleep(0.05)


def configs(root, state, database, proxy_port):
    # lab.config owns the TOML recipe. Move each newly created result to a
    # distinct name using no-overwrite link semantics, never rewrite a live path.
    paths = []
    for suffix, endpoint, boundary in (("direct", "127.0.0.1:5432", "loopback"),
                                       ("proxy", f"127.0.0.1:{proxy_port}", "loopback")):
        original = root / (database + ".toml")
        target = root / (database + "-" + suffix + ".toml")
        require(not os.path.lexists(original) and not os.path.lexists(target), "configuration-exists")
        # Claim only a new output name before the existing recipe writer fills
        # it. A failed generation retains this owned placeholder for diagnosis.
        exclusive(original, "")
        lab.config(root, state, database, endpoint, boundary)
        os.link(original, target)
        original.unlink()
        paths.append("/lab/" + target.name)
    return paths


def run_case(root, state, container, case, save, source, expected):
    database = case["database"]
    mode = {"pass": "pass", "before": "before-forward", "after": "after-complete",
            "release": "hold", "close": "hold", "race": "hold"}[case["name"]]
    processes = []
    proxy = None
    case["status"] = "RUNNING"
    case["started_ns"] = time.monotonic_ns()
    save()
    try:
        case["stage"] = "create-fresh-database"
        save()
        # CREATE, not IF NOT EXISTS, preserves interrupted/previous databases.
        lab.sql(state, "postgres", f"CREATE DATABASE {database} OWNER bigbrotr_migrator;")
        case["stage"] = "proxy-start"
        proxy = Process(["docker", "exec", "-i", container, "python3", "-B",
                         "/source/experiments/control-migrations/fault_proxy.py", "--upstream-port", "5432",
                         "--mode", mode, "--deadline-seconds", "90"], input_pipe=True)
        processes.append(proxy)
        ready = proxy.event("ready", 10)
        require(ready.get("proxy_sha256") == digest(source / "experiments/control-migrations/fault_proxy.py"), "proxy-source-mismatch")
        direct_path, proxy_path = configs(root, state, database, ready["listen_port"])
        case["configuration_sha256"] = {Path(path).name: digest(root / Path(path).name) for path in (direct_path, proxy_path)}
        case["initial"] = observed(state, database)
        require(case["initial"]["namespace_count"] == 0, "database-not-clean")
        application = Process(migration_args(container, "apply", proxy_path))
        processes.append(application)
        case["stage"] = "actual-apply"
        save()
        if mode == "hold":
            proxy.event("held", 15)
            case["stage"] = "commit-held"
            case["held_backends"] = backends(state, database)
            owners = lock_holders(case["held_backends"])
            require(len(owners) == 1, "held-owner-not-observed")
            owner = owners[0]["pid"]
            racing = case["name"] == "race"
            inspector = Process(migration_args(state["client"], "apply" if racing else "inspect", direct_path))
            processes.append(inspector)
            case["waiting_backends"] = await_waiter(state, database, owner, "ExclusiveLock" if racing else "ShareLock")
            require(inspector.process.poll() is None and not inspector.stdout, "inspector-not-blocked")
            save()
            if case["name"] in ("release", "race"):
                proxy.send("release")
                case["blocked_inspector"] = client_result(inspector, 15)
                if racing:
                    other = case["blocked_inspector"]
                    require(other["exit"] == 0 and other["output"].get("status") == "current"
                            and other["output"].get("source_version") == "1"
                            and other["output"].get("current_version") == "1"
                            and other["output"].get("applied_count") == "0"
                            and other["output"].get("steps") == [], "racing-apply-not-current")
                else:
                    validate_inspection(case["blocked_inspector"], True, expected)
            else:
                case["blocked_inspector"] = client_result(inspector, 7)
                require(case["blocked_inspector"]["exit"] == 1 and case["blocked_inspector"]["output"] is None
                        and case["blocked_inspector"]["backend_refusal"], "locked-inspector-did-not-refuse")
                case["still_held_backends"] = backends(state, database)
                require(any(row["pid"] == owner for row in lock_holders(case["still_held_backends"])), "old-owner-no-longer-held")
                proxy.send("status")
                status = proxy.event("status", 1)
                require(status["held"] and status["commits_forwarded"] == 0, "hold-was-released")
                proxy.send("close")
        case["client"] = client_result(application)
        case["proxy_receipt"] = proxy.event("receipt", 10)
        case["proxy_exit"] = proxy.wait(5)
        case["proxy_events"] = proxy.events
        require(case["proxy_exit"] == 0 and not proxy.stderr, "proxy-incomplete")
        receipt = case["proxy_receipt"]
        counts = {key: receipt[key] for key in ("commits_seen", "commits_forwarded", "server_commit_complete", "client_commit_complete", "faults")}
        expected_counts = {
            "pass": (1, 1, 1, 1, 0), "before": (1, 0, 0, 0, 1), "after": (1, 1, 1, 0, 1),
            "release": (1, 1, 1, 1, 1), "close": (1, 0, 0, 0, 1), "race": (1, 1, 1, 1, 1),
        }[case["name"]]
        require(tuple(counts.values()) == expected_counts, "proxy-boundary-mismatch")
        lost = case["name"] in ("before", "after", "close")
        require(case["client"]["exit"] == (4 if lost else 0)
                and case["client"]["output"].get("status") == ("outcome-unknown" if lost else "applied"), "client-outcome-mismatch")
        case["stage"] = "fresh-serialized-resolution"
        save()
        case["inspection"] = direct(state, "inspect", direct_path)
        present = case["name"] not in ("before", "close")
        validate_inspection(case["inspection"], present, expected)
        case["resolved"] = observed(state, database)
        if present:
            validate_present(case["resolved"], expected)
        else:
            require(case["resolved"]["namespace_count"] == 0, "uncommitted-ddl-present")
        case["backends_after_resolution"] = await_no_backends(state, database)
        case["stage"] = "retry-same-target-once"
        save()
        case["retry"] = direct(state, "apply", direct_path)
        require(case["retry"]["exit"] == 0 and case["retry"]["output"].get("status") == ("current" if present else "applied")
                and case["retry"]["output"].get("applied_count") == ("0" if present else "1"), "retry-mismatch")
        case["after_retry"] = observed(state, database)
        validate_present(case["after_retry"], expected)
        if present:
            require(case["after_retry"] == case["resolved"], "retry-changed-receipt-or-schema")
        case["backends_after_retry"] = await_no_backends(state, database)
        case["status"], case["stage"] = "PASS", "complete"
    except Failed as error:
        case["status"], case["reason"] = "FAIL", str(error)
        raise
    except Incomplete as error:
        case["status"], case["reason"] = "UNKNOWN", str(error)
        raise
    except KeyboardInterrupt:
        case["status"], case["reason"] = "UNKNOWN", "interrupted"
        raise
    except Exception:
        case["status"], case["reason"] = "UNKNOWN", "execution-incomplete"
        raise
    finally:
        if proxy:
            case["proxy_events"] = proxy.events
        case["ended_ns"] = time.monotonic_ns()
        save()
        for process in processes:
            process.stop()


def inspect_container(name):
    return json.loads(lab.docker("inspect", name).stdout)[0]


def preflight(root, build, source):
    state = lab.load(root)
    require(state["source"] == str(source) and source == Path(__file__).resolve().parents[2], "harness-source-path")
    require(not lab.command(["git", "-C", str(source), "status", "--porcelain", "--untracked-files=all"]).stdout, "dirty-harness-source")
    require(hasattr(lab, "schema_digest"), "lab-helper-version")
    build_receipt = json.loads((build / "build-result.json").read_text())
    require(build_receipt == state["build"] and digest(build / "bigbrotr") == state["binary_sha256"] == build_receipt["binary_sha256"], "artifact-identity")
    original = inspect_container(state["client"])
    postgres = inspect_container(state["postgres"])
    require(original["State"]["Running"] and postgres["State"]["Running"], "lab-not-running")
    require(original["Image"] == state["client_image"] == lab.CLIENT_IMAGE, "client-image")
    require(postgres["Config"]["Image"] == state["postgres_image"] == lab.PG_IMAGE, "postgres-image")
    mounts = {entry["Destination"]: entry for entry in original["Mounts"]}
    for target, expected, writable in (("/build", build, False), ("/source", source, False), ("/lab", root, True)):
        require(target in mounts and Path(mounts[target]["Source"]).resolve() == expected
                and mounts[target]["RW"] is writable, "client-mount-identity")
    server = json.loads(lab.sql(state, "postgres", "SELECT json_build_object('version',current_setting('server_version_num'),'fsync',current_setting('fsync'),'full_page_writes',current_setting('full_page_writes'));"))
    require(server == {"version": "160015", "fsync": "on", "full_page_writes": "on"}, "server-settings")
    require(lab.sql(state, "postgres", "SELECT bool_and(auth_method='scram-sha-256') FROM pg_hba_file_rules WHERE type='host';") == "t", "host-auth-not-scram")
    application_source = state["build"]["build_info"]["source"]
    require(application_source.get("dirty") is False and re.fullmatch(r"[0-9a-f]{40}", application_source["revision"]), "application-source")
    migration = lab.command(["git", "-C", str(source), "show", application_source["revision"] + ":migrations/postgres/0001_control.sql"]).stdout.encode()
    expected = hashlib.sha256(migration).hexdigest()
    require(expected == digest(source / "migrations/postgres/0001_control.sql"), "migration-source-drift")
    server.update({"container_id": postgres["Id"], "image_id": postgres["Image"],
                   "image_manifest": state["postgres_image"], "started_at": postgres["State"]["StartedAt"]})
    return state, original, server, expected


def run(root, build, run_name):
    source = Path(__file__).resolve().parents[2]
    state, original, server, expected = preflight(root, build, source)
    output = root / (run_name + "-faults")
    output.mkdir(mode=0o700, exist_ok=False)
    token = secrets.token_hex(16)
    name = state["client"] + "-fault-" + run_name
    report = {"format": "bigbrotr-pg-fault-cases/1", "status": "RUNNING", "owner": state["owner"],
              "fault_owner": token, "container": name, "container_state": "not-created", "published_ports": [],
              "binary_sha256": state["binary_sha256"], "application_source": state["build"]["build_info"]["source"],
              "build_receipt_sha256": digest(build / "build-result.json"), "server": server,
              "harness_revision": lab.command(["git", "-C", str(source), "rev-parse", "HEAD"]).stdout.strip(),
              "sources": {filename: digest(source / "experiments/control-migrations" / filename)
                          for filename in ("fault_cases.py", "fault_proxy.py", "lab.py")},
              "host": {"system": platform.system(), "machine": platform.machine(), "python": platform.python_version()},
              "migration_sha256": expected, "cases": [{"name": case, "database": "bb_" + run_name + "_fault_" + case,
                                                         "status": "NOT_RUN"} for case in CASES]}
    def save():
        temporary = output / "report.tmp"
        temporary.write_text(json.dumps(report, indent=2) + "\n")
        temporary.replace(output / "report.json")
    exclusive(output / "report.json", json.dumps(report, indent=2) + "\n")
    attempted = False
    try:
        existing = lab.docker("ps", "-a", "--filter", "name=^/" + name + "$", "--format", "{{.Names}}")
        require(not existing.stdout.strip(), "fault-container-exists")
        # Validate all database and config names before any case mutates them.
        for case in report["cases"]:
            database = case["database"]
            require(re.fullmatch(r"[a-z_]{1,63}", database) is not None, "database-name")
            require(lab.sql(state, "postgres", f"SELECT count(*) FROM pg_database WHERE datname='{database}';") == "0", "database-exists")
            require(not list(root.glob(database + "*.toml")), "configuration-exists")
        report["container_state"] = "creating"
        save()
        attempted = True
        lab.docker("run", "-d", "--init", "--name", name,
                   "--label", f"{lab.LABEL}={state['owner']}", "--label", f"{FAULT_LABEL}={token}",
                   "--network", "container:" + state["postgres"], "--user", original["Config"]["User"],
                   "--cpus", "2", "--memory", "4g", "--memory-swap", "4g", "--pids-limit", "256",
                   "--read-only", "--cap-drop", "ALL", "--security-opt", "no-new-privileges",
                   "--tmpfs", "/tmp:rw,exec,nosuid,nodev,size=128m",
                   "--mount", f"type=bind,src={root},dst=/lab",
                   "--mount", f"type=bind,src={build},dst=/build,readonly",
                   "--mount", f"type=bind,src={source},dst=/source,readonly",
                   state["client_image"], "python3", "-c", "import time; time.sleep(86400)")
        created = inspect_container(name)
        require(created["Config"]["Labels"].get(lab.LABEL) == state["owner"]
                and created["Config"]["Labels"].get(FAULT_LABEL) == token, "fault-container-owner")
        host = created["HostConfig"]
        limits = {key: host[key] for key in ("NanoCpus", "Memory", "MemorySwap", "PidsLimit", "ReadonlyRootfs",
                                             "CapDrop", "CapAdd", "SecurityOpt", "NetworkMode", "PortBindings", "PublishAllPorts")}
        report["container_limits"] = limits
        require(limits["NanoCpus"] == 2000000000 and limits["Memory"] == limits["MemorySwap"] == 4294967296
                and limits["PidsLimit"] == 256 and limits["ReadonlyRootfs"]
                and limits["CapDrop"] == ["ALL"] and not limits["CapAdd"]
                and "no-new-privileges" in limits["SecurityOpt"]
                and not limits["PortBindings"] and not limits["PublishAllPorts"], "container-resource-boundary")
        postgres_id = inspect_container(state["postgres"])["Id"]
        require(limits["NetworkMode"] in ("container:" + state["postgres"], "container:" + postgres_id)
                and created["Image"] == original["Image"], "fault-network-or-image")
        report["container_id"], report["container_state"] = created["Id"], "running"
        save()
        for case in report["cases"]:
            run_case(root, {**state, "client": name}, name, case, save, source, expected)
        require(len(report["cases"]) == len(CASES) and all(case["status"] == "PASS" for case in report["cases"]), "case-set-incomplete")
        require(all(digest(source / "experiments/control-migrations" / filename) == sha
                    for filename, sha in report["sources"].items())
                and digest(build / "bigbrotr") == report["binary_sha256"]
                and lab.command(["git", "-C", str(source), "rev-parse", "HEAD"]).stdout.strip() == report["harness_revision"],
                "source-or-artifact-changed-during-run")
        final_server = inspect_container(state["postgres"])
        require(final_server["Id"] == server["container_id"] and final_server["State"]["StartedAt"] == server["started_at"],
                "engine-changed-during-fault-run")
        report["status"] = "PASS"
    except Failed as error:
        report["status"], report["reason"] = "FAIL", str(error)
    except Incomplete as error:
        report["status"], report["reason"] = "UNKNOWN", str(error)
    except KeyboardInterrupt:
        report["status"], report["reason"] = "UNKNOWN", "interrupted"
    except Exception:
        report["status"], report["reason"] = "UNKNOWN", "execution-incomplete"
    finally:
        if attempted:
            try:
                current = inspect_container(name)
                require(current["Config"]["Labels"].get(lab.LABEL) == state["owner"]
                        and current["Config"]["Labels"].get(FAULT_LABEL) == token, "cleanup-owner-mismatch")
                lab.docker("stop", "--time", "5", name, timeout=15)
                current = inspect_container(name)
                require(not current["State"]["Running"], "container-not-stopped")
                report["container_state"] = "stopped-retained"
                report["container_exit"] = current["State"]["ExitCode"]
                report["container_oom_killed"] = current["State"]["OOMKilled"]
                require(not current["State"]["OOMKilled"], "fault-container-oom")
            except Exception:
                report["status"], report["cleanup"] = "UNKNOWN", "owned-container-closure-unconfirmed"
        save()
    print(json.dumps({"status": report["status"], "output": str(output / "report.json"),
                      "cases": {case["name"]: case["status"] for case in report["cases"]}}))
    return 0 if report["status"] == "PASS" else 1


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", required=True, type=Path)
    parser.add_argument("--build", required=True, type=Path)
    parser.add_argument("--run-name", required=True)
    args = parser.parse_args()
    if not args.root.is_absolute() or not args.build.is_absolute() or not re.fullmatch(r"[a-z_]{1,20}", args.run_name):
        parser.error("absolute root/build and 1..20 lowercase letters/underscores run-name required")
    try:
        result = run(args.root.resolve(), args.build.resolve(), args.run_name)
    except Exception:
        # Exceptions from subprocess/JSON/SQL may contain private inputs.
        print(json.dumps({"status": "UNKNOWN", "reason": "fault-preflight-or-report-incomplete"}))
        result = 1
    raise SystemExit(result)
