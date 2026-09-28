#!/usr/bin/env python3
"""Isolated Linux laboratory for the actual control migration executable.

This is acceptance orchestration, not an installation or product service.
Resources are never silently reused or removed. Credentials stay in the owned
0700 run directory and are never emitted in command arguments or reports.
"""
import argparse
import hashlib
import ipaddress
import json
import os
from pathlib import Path
import re
import secrets
import subprocess
import time


PG_IMAGE = "postgres@sha256:f1c3376c26f2609ab9f29f71f824103fe2fcd8ee0346485cb6122a4f93df6f94"
CLIENT_IMAGE = "sha256:beb1b1d81ab841af53b71bf545d4184f37ce3e20fd7505eaf1f7f16a74469dd2"
PREFIX = "bigbrotr-implementation-pg-20260917"
LABEL = "org.bigbrotr.control-migration-lab"


def command(args, *, data=None, timeout=90, check=True):
    result = subprocess.run(args, input=data, capture_output=True, text=True, timeout=timeout)
    if check and result.returncode:
        # Database error output can echo SQL including a bootstrap password.
        raise RuntimeError(f"laboratory command failed: {args[0]} (exit {result.returncode})")
    return result


def docker(*args, **kwargs):
    return command(["docker", *args], **kwargs)


def write_json(path, data):
    path.write_text(json.dumps(data, indent=2) + "\n")


def load(root):
    state = json.loads((root / "resources.json").read_text())
    if state["root"] != str(root) or state["format"] != "bigbrotr-pg-lab/1":
        raise RuntimeError("laboratory ownership mismatch")
    for name in [state["postgres"], state["client"]]:
        info = json.loads(docker("inspect", name).stdout)[0]
        if info["Config"]["Labels"].get(LABEL) != state["owner"]:
            raise RuntimeError("container ownership mismatch")
    return state


def setup(root, build, source, run_name, *, subnet=None):
    if source.resolve() != Path(__file__).resolve().parents[2]:
        raise RuntimeError("execute the harness from its selected Git source checkout")
    if command(["git", "-C", str(source), "status", "--porcelain", "--untracked-files=all"]).stdout:
        raise RuntimeError("laboratory harness source must be clean")
    # The root is new even when the separately built artifact already exists.
    root.mkdir(mode=0o700, parents=False, exist_ok=False)
    prefix = PREFIX + "-" + run_name
    state = {"format": "bigbrotr-pg-lab/1", "root": str(root), "owner": secrets.token_hex(16),
             "postgres": prefix, "client": prefix + "-client", "network": prefix,
             "postgres_image": PG_IMAGE, "client_image": CLIENT_IMAGE,
             "binary_sha256": hashlib.sha256((build / "bigbrotr").read_bytes()).hexdigest(),
             "build": json.loads((build / "build-result.json").read_text()),
             "source": str(source), "state": "creating", "published_ports": []}
    state["harness_revision"] = command(["git", "-C", str(source), "rev-parse", "HEAD"]).stdout.strip()
    state["harness_sha256"] = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    if state["binary_sha256"] != state["build"]["binary_sha256"]:
        raise RuntimeError("binary differs from its exact-source build receipt")
    write_json(root / "resources.json", state)
    for kind, names in [("container", [state["postgres"], state["client"]]), ("network", [state["network"]])]:
        for name in names:
            if docker(kind, "inspect", name, check=False).returncode == 0:
                raise RuntimeError("refusing pre-existing laboratory resource")
    protected = root / "secrets"
    protected.mkdir(mode=0o700)
    for name in ["bootstrap", "migrator", "application"]:
        path = protected / name
        descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(descriptor, "w") as output:
            output.write(secrets.token_hex(32))
    (root / "data").mkdir(mode=0o700)
    common = ["--label", f"{LABEL}={state['owner']}", "--user", f"{os.getuid()}:{os.getgid()}",
              "--cpus", "2", "--pids-limit", "256", "--read-only", "--cap-drop", "ALL",
              "--security-opt", "no-new-privileges", "--tmpfs", "/tmp:rw,exec,nosuid,nodev,size=128m"]
    network_args = []
    if subnet is not None:
        selected = ipaddress.ip_network(subnet, strict=True)
        if selected.version != 4 or not selected.is_private or selected.prefixlen != 28:
            raise RuntimeError("explicit laboratory subnet must be one private IPv4 /28")
        network_ids = docker("network", "ls", "-q").stdout.split()
        networks = json.loads(docker("network", "inspect", *network_ids).stdout)
        routes = json.loads(command(["ip", "-j", "route", "show", "table", "all"]).stdout)
        prefixes = [entry["Subnet"] for network in networks for entry in network["IPAM"].get("Config") or []]
        prefixes += [route["dst"] for route in routes if route.get("dst", "default") != "default"]
        for value in prefixes:
            prefix = ipaddress.ip_network(value, strict=False)
            if prefix.version == selected.version and prefix.overlaps(selected):
                raise RuntimeError("explicit laboratory subnet overlaps an existing network or route")
        state["explicit_subnet"] = str(selected)
        state["network_preflight"] = dict(networks=len(networks), routes=len(routes), overlap=False)
        write_json(root / "resources.json", state)
        network_args = ["--subnet", str(selected)]
    network = docker("network", "create", "--internal", *network_args,
                     "--label", f"{LABEL}={state['owner']}", state["network"], check=False)
    if network.returncode:
        # Unlike SQL errors, this diagnostic cannot contain database credentials.
        write_json(root / "network-failure.json", dict(exit_code=network.returncode, stderr=network.stderr))
        raise RuntimeError("isolated network creation failed; see network-failure.json")
    docker("run", "-d", "--name", state["postgres"], *common,
           "--memory", "1536m", "--memory-swap", "1536m", "--network", state["network"],
           "--network-alias", "postgres", "--tmpfs", "/var/run/postgresql:rw,nosuid,nodev,size=16m,mode=0777",
           "--mount", f"type=bind,src={root / 'data'},dst=/var/lib/postgresql/data",
           "--mount", f"type=bind,src={protected},dst=/run/secrets,readonly",
           "--env", "POSTGRES_PASSWORD_FILE=/run/secrets/bootstrap", "--env", "POSTGRES_DB=postgres",
           "--env", "POSTGRES_INITDB_ARGS=--auth-local=trust --auth-host=scram-sha-256",
           PG_IMAGE, "postgres", "-c", "max_connections=32", "-c", "shared_buffers=128MB",
           "-c", "work_mem=4MB", "-c", "log_statement=none", "-c", "log_min_error_statement=panic",
           "-c", "log_parameter_max_length_on_error=0")
    deadline = time.monotonic() + 60
    while True:
        ready = docker("exec", state["postgres"], "pg_isready", "-h", "127.0.0.1", "-U", "postgres", check=False)
        if ready.returncode == 0:
            break
        if time.monotonic() > deadline:
            raise RuntimeError("PostgreSQL readiness deadline exceeded")
        time.sleep(0.2)
    # psql inside the owned PG container uses its isolated local socket. Host
    # connections use SCRAM passwords; no broad host trust exception is enabled.
    for role, filename in [("bigbrotr_migrator", "migrator"), ("bigbrotr_control", "application")]:
        password = (protected / filename).read_text()
        sql(state, "postgres", f"CREATE ROLE {role} LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE PASSWORD '{password}';")
    docker("run", "-d", "--init", "--name", state["client"], *common,
           "--memory", "4g", "--memory-swap", "4g", "--network", state["network"],
           "--add-host", "outside:203.0.113.1", "--add-host", "mixed:172.18.0.2", "--add-host", "mixed:203.0.113.2",
           "--mount", f"type=bind,src={root},dst=/lab",
           "--mount", f"type=bind,src={build},dst=/build,readonly",
           "--mount", f"type=bind,src={source},dst=/source,readonly",
           CLIENT_IMAGE, "python3", "-c", "import time; time.sleep(86400)")
    state["state"] = "running"
    state["server"] = json.loads(sql(state, "postgres", "SELECT json_build_object('version',current_setting('server_version_num'),'fsync',current_setting('fsync'),'full_page_writes',current_setting('full_page_writes'),'architecture',version());"))
    write_json(root / "resources.json", state)
    if state["server"]["version"] != "160015":
        raise RuntimeError("unqualified engine version")
    print(json.dumps({"status": "ready", "root": str(root), "server": state["server"]}))


def sql(state, database, statement):
    return docker("exec", "-i", state["postgres"], "psql", "-X", "-A", "-t", "-q", "--no-password",
                  "-v", "ON_ERROR_STOP=1", "-U", "postgres", "-d", database, data=statement).stdout.strip()


def cli(state, *args):
    return docker("exec", state["client"], "/build/bigbrotr", *args, check=False)


def change(text, section, key, value):
    pattern = re.compile(r"(?ms)(^\[" + re.escape(section) + r"\]\n)(.*?)(?=^\[|\Z)")
    matches = list(pattern.finditer(text))
    if len(matches) != 1:
        raise RuntimeError("configuration section expectation failed")
    match = matches[0]
    body, count = re.subn(r"(?m)^" + re.escape(key) + r" = .*?$", key + " = " + json.dumps(value), match[2])
    if count != 1:
        raise RuntimeError("configuration field expectation failed")
    return text[:match.start()] + match[1] + body + text[match.end():]


def config(root, state, database, endpoint="postgres:5432", boundary="private-bridge"):
    template = root / "template.toml"
    if not template.exists():
        result = cli(state, "config", "init", "--recipe", "archive-only", "--output", "/lab/template.toml")
        if result.returncode:
            raise RuntimeError("actual configuration initialization failed")
    text = change(template.read_text(), "postgres.control", "database", database)
    text = change(text, "postgres.control", "endpoint", endpoint)
    text = change(text, "postgres.control.tls", "local_boundary", boundary)
    path = root / (database + ".toml")
    path.write_text(text)
    return "/lab/" + path.name


def migrate(state, action, path, *, user="bigbrotr_migrator", password="migrator", target=1):
    # Existing suites explicitly retain their 0001 boundary as the catalog grows.
    # A caller may select 2, or None to exercise the executable's current default.
    return cli(state, "migrate", action, "--file", path, "--user", user,
               "--password-ref", "/".join(["file:/lab/secrets", password]),
               *([] if target is None else ["--target", str(target)]))


def schema_digest(state, database):
    dump = docker("exec", state["postgres"], "pg_dump", "--no-password", "-U", "postgres", "-d", database,
                  "--schema=control", "--no-owner", "--no-privileges").stdout
    # Recent pg_dump patches randomize their psql restrict token. It is neither
    # database content nor schema; retain every other byte in this comparison.
    stable = "\n".join(line for line in dump.splitlines() if not line.startswith(("\\restrict ", "\\unrestrict ")))
    return hashlib.sha256(stable.encode()).hexdigest()


def auth_failures(state):
    output = docker("logs", state["postgres"])
    return (output.stdout + output.stderr).count('password authentication failed for user "bigbrotr_migrator"')


def schema_cases(root, run_name):
    state = load(root)
    output = root / (run_name + "-schema.json")
    if output.exists():
        raise RuntimeError("refusing to overwrite schema evidence")
    database = "bb_" + run_name + "_schema"
    sql(state, "postgres", f"CREATE DATABASE {database} OWNER bigbrotr_migrator;")
    result = migrate(state, "apply", config(root, state, database))
    if result.returncode or json.loads(result.stdout)["status"] != "applied":
        raise RuntimeError("schema fixture prerequisite migration failed")
    source = Path(__file__).resolve().parents[2]
    fixture = source / "experiments/control-migrations/schema_cases.sql"
    expected = hashlib.sha256((source / "migrations/postgres/0001_control.sql").read_bytes()).hexdigest()
    result = docker("exec", "-i", state["postgres"], "psql", "-X", "-A", "-t", "-q", "--no-password",
                    "-U", "bigbrotr_migrator", "-d", database, "-v", "ON_ERROR_STOP=1",
                    "-v", "expected_migration_sha256=" + expected, data=fixture.read_text(), check=False)
    (root / (run_name + "-schema.stdout")).write_text(result.stdout)
    (root / (run_name + "-schema.stderr")).write_text(result.stderr)
    summaries = [json.loads(line) for line in result.stdout.splitlines() if line.startswith('{"')]
    summary = summaries[-1] if summaries else {}
    observed = json.loads(sql(state, database, "SELECT json_build_object('datasets',(SELECT count(*) FROM control.datasets),'config',(SELECT count(*) FROM control.config_revisions),'policy',(SELECT count(*) FROM control.policy_revisions),'topologies',(SELECT count(*) FROM control.topology_generations WHERE sealed),'streams',(SELECT count(*) FROM control.topology_streams),'owners',(SELECT count(*) FROM control.owner_epochs),'jobs',(SELECT json_object_agg(encode(job_id,'hex'),state) FROM control.jobs));"))
    expected_rows = {"datasets": 1, "config": 1, "policy": 1, "topologies": 2, "streams": 3, "owners": 2,
                     "jobs": {"00000000000000000000000000000028": "succeeded", "00000000000000000000000000000029": "cancelled", "0000000000000000000000000000002a": "accepted"}}
    passed = result.returncode == 0 and summary.get("result") == "PASS" and summary.get("executed") == 104 and observed == expected_rows
    report = {"status": "PASS" if passed else "FAIL", "exit": result.returncode, "summary": summary,
              "external_rows": observed, "fixture_sha256": hashlib.sha256(fixture.read_bytes()).hexdigest(),
              "binary_sha256": state["binary_sha256"], "application_source": state["build"]["build_info"]["source"],
              "harness_revision": command(["git", "-C", str(source), "rev-parse", "HEAD"]).stdout.strip()}
    write_json(output, report)
    print(json.dumps(report))
    if not passed:
        raise RuntimeError("actual schema fixture failed; inspect retained stdout/stderr")


def smoke(root, run_name):
    state = load(root)
    output = root / (run_name + ".json")
    clean = "bb_" + run_name + "_clean"
    if output.exists():
        raise RuntimeError("refusing to overwrite prior smoke evidence")
    report = {"format": "bigbrotr-pg-smoke/1", "binary_sha256": state["binary_sha256"],
              "source": state["build"]["build_info"]["source"], "server": state["server"], "cases": [], "status": "RUNNING",
              "harness_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
              "harness_revision": command(["git", "-C", str(Path(__file__).resolve().parents[2]), "rev-parse", "HEAD"]).stdout.strip()}
    def record(name, condition, observed):
        report["cases"].append({"name": name, "status": "PASS" if condition else "FAIL", "observed": observed})
        write_json(output, report)
        if not condition:
            raise RuntimeError("acceptance case failed: " + name)
    def create(name, template=None):
        name = "bb_" + run_name + "_" + name
        assert re.fullmatch(r"bb_[a-z_]+", name)
        sql(state, "postgres", f"CREATE DATABASE {name} OWNER bigbrotr_migrator" + (f" TEMPLATE {template}" if template else "") + ";")
        return config(root, state, name)
    def receipt(database):
        return sql(state, database, "SELECT row_to_json(v) FROM control.schema_versions v;")
    try:
        path = create("clean")
        for action in ["inspect", "plan"]:
            result = migrate(state, action, path)
            value = json.loads(result.stdout) if result.stdout else {}
            count = sql(state, clean, "SELECT count(*) FROM pg_namespace WHERE nspname='control';")
            record("clean_" + action, result.returncode == 0 and value.get("source_version") is None and count == "0", {"exit": result.returncode, "output": value, "control_namespaces": count, "stderr": result.stderr})
        result = migrate(state, "apply", path)
        value = json.loads(result.stdout) if result.stdout else {}
        record("first_apply", result.returncode == 0 and value.get("status") == "applied", {"exit": result.returncode, "output": value, "stderr": result.stderr})
        first = receipt(clean)
        expected = hashlib.sha256((Path(state["source"]) / "migrations/postgres/0001_control.sql").read_bytes()).hexdigest()
        saved = json.loads(first)
        counts = json.loads(sql(state, clean, "SELECT json_build_object('datasets',(SELECT count(*) FROM control.datasets),'jobs',(SELECT count(*) FROM control.jobs),'owners',(SELECT count(*) FROM control.owner_epochs),'config',(SELECT count(*) FROM control.config_revisions),'policy',(SELECT count(*) FROM control.policy_revisions));"))
        record("exact_receipt_and_no_implicit_initialization", saved["checksum"] == "\\x" + expected and saved["migration"] == 1 and all(n == 0 for n in counts.values()), {"receipt": saved, "rows": counts})
        result = migrate(state, "apply", path)
        value = json.loads(result.stdout) if result.stdout else {}
        record("reapply_is_current", result.returncode == 0 and value.get("status") == "current" and receipt(clean) == first, {"exit": result.returncode, "output": value})
        for name, preparation, template in [
            ("namespace", "CREATE SCHEMA control;", None),
            ("view", "CREATE SCHEMA control; CREATE VIEW control.schema_versions AS SELECT 1;", None),
            ("unlogged", "CREATE SCHEMA control; CREATE UNLOGGED TABLE control.schema_versions(a integer);", None),
            ("empty", "ALTER TABLE control.schema_versions DISABLE TRIGGER USER; DELETE FROM control.schema_versions;", clean),
            ("checksum", "ALTER TABLE control.schema_versions DISABLE TRIGGER USER; UPDATE control.schema_versions SET checksum=decode(repeat('00',32),'hex');", clean),
            ("unknown", "ALTER TABLE control.schema_versions DISABLE TRIGGER USER; UPDATE control.schema_versions SET migration=999;", clean),
            ("metadata", "ALTER TABLE control.schema_versions DISABLE TRIGGER USER; UPDATE control.schema_versions SET reader_max=2;", clean),
        ]:
            database = "bb_" + run_name + "_" + name
            candidate = create(name, template)
            sql(state, database, preparation)
            before = schema_digest(state, database)
            result = migrate(state, "apply", candidate)
            after = schema_digest(state, database)
            expected_reason = {
                "namespace": "lacks a recognized migration ledger", "view": "lacks a recognized migration ledger",
                "unlogged": "lacks a recognized migration ledger", "empty": "invalid bounded metadata",
                "checksum": "migration 1 has a different checksum", "unknown": "unknown applied control migration 999",
                "metadata": "migration 1 has different metadata",
            }[name]
            record("refuse_" + name, result.returncode == 1 and expected_reason in result.stderr and before == after,
                   {"exit": result.returncode, "stderr": result.stderr, "before_schema_sha256": before, "after_schema_sha256": after})
        for hostname in ["outside", "mixed"]:
            candidate = config(root, state, clean, hostname + ":5432")
            result = migrate(state, "inspect", candidate)
            record("refuse_plaintext_route_" + hostname, result.returncode == 1 and "resolved PostgreSQL plaintext route" in result.stderr, {"exit": result.returncode, "stderr": result.stderr})
        candidate = config(root, state, clean)
        before_positive = migrate(state, "inspect", candidate)
        failures_before = auth_failures(state)
        result = migrate(state, "inspect", candidate, password="application")
        failures_after = auth_failures(state)
        after_positive = migrate(state, "inspect", candidate)
        hba = sql(state, "postgres", "SELECT bool_and(auth_method='scram-sha-256') FROM pg_hba_file_rules WHERE type='host';")
        record("scram_rejects_wrong_password", result.returncode == 1 and not result.stdout
               and before_positive.returncode == after_positive.returncode == 0
               and failures_after == failures_before + 1 and hba == "t",
               {"exit": result.returncode, "stderr": result.stderr, "positive_controls": [before_positive.returncode, after_positive.returncode],
                "server_auth_failures_added": failures_after - failures_before, "all_host_rules_scram": hba})
        # A controlled isolated engine crash, not host power loss or fsync fault.
        docker("kill", "--signal", "KILL", state["postgres"])
        docker("start", state["postgres"])
        deadline = time.monotonic() + 30
        while docker("exec", state["postgres"], "pg_isready", "-h", "127.0.0.1", "-U", "postgres", check=False).returncode:
            if time.monotonic() > deadline:
                raise RuntimeError("engine restart deadline exceeded")
            time.sleep(0.2)
        result = migrate(state, "inspect", candidate)
        record("engine_crash_restart_preserves_receipt", result.returncode == 0 and receipt(clean) == first, {"exit": result.returncode, "receipt_unchanged": receipt(clean) == first})
        report["status"] = "PASS"
    except Exception:
        report["status"] = "FAIL"
        raise
    finally:
        write_json(output, report)
    print(json.dumps({"status": report["status"], "cases": len(report["cases"]), "output": str(output)}))


def stop(root):
    state = load(root)
    for name in [state["client"], state["postgres"]]:
        docker("stop", "--time", "10", name)
    state["state"] = "stopped-retained"
    write_json(root / "resources.json", state)
    print(json.dumps({"status": state["state"], "root": str(root)}))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=["setup", "smoke", "schema", "stop"])
    parser.add_argument("--root", required=True, type=Path)
    parser.add_argument("--run-name", default="smoke")
    parser.add_argument("--build", type=Path)
    parser.add_argument("--source", type=Path)
    args = parser.parse_args()
    if not re.fullmatch(r"[a-z_]{1,20}", args.run_name):
        parser.error("run-name must contain 1..20 lowercase letters/underscores")
    if not args.root.is_absolute():
        parser.error("root must be absolute")
    if args.action == "setup":
        if not args.build or not args.source or not args.build.is_absolute() or not args.source.is_absolute():
            parser.error("setup requires absolute build and source paths")
        setup(args.root, args.build, args.source, args.run_name)
    elif args.action == "smoke":
        smoke(args.root, args.run_name)
    elif args.action == "schema":
        schema_cases(args.root, args.run_name)
    else:
        stop(args.root)
