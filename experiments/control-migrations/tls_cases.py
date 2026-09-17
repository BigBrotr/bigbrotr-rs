#!/usr/bin/env python3
"""Actual BigBrotr/PostgreSQL TLS cases in an existing owned isolated laboratory.

No deployment credentials, external endpoints or new containers are used. Run
alone while other lab actions are stopped. See README-TLS.md for restoration.
"""
import argparse
from datetime import datetime, timezone
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import tempfile
import time

import lab


SERVER_NAME = "db.tls.bigbrotr.invalid"
ROLE = "bigbrotr_migrator"
BACKEND_ERROR = "error: control migration backend unavailable or refused the operation\n"
SETTINGS = ("ssl", "ssl_cert_file", "ssl_key_file", "ssl_ca_file", "log_connections", "log_line_prefix", "lc_messages")
CASE_NAMES = (
    "plaintext_baseline", "tls_exact_ca_and_explicit_name", "tls_optional_client_certificate",
    "tls_wrong_ca", "tls_wrong_name", "mtls_correct_identity", "mtls_missing_certificate",
    "mtls_wrong_identity", "mtls_wrong_client_ca", "restored_config_and_plaintext",
)


def sha(data):
    return hashlib.sha256(data).hexdigest()


def private_write(path, data):
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, "wb") as output:
        output.write(data)
        output.flush()
        os.fsync(output.fileno())


def replace_file(path, data, mode):
    """Atomic replacement only of previously verified, backed-up lab files."""
    descriptor, temporary = tempfile.mkstemp(prefix=".bigbrotr-tls-", dir=path.parent)
    try:
        with os.fdopen(descriptor, "wb") as output:
            os.fchmod(output.fileno(), mode)
            output.write(data)
            output.flush()
            os.fsync(output.fileno())
        os.replace(temporary, path)
        directory = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def generate_pki(directory):
    """Ephemeral RSA identities; no private bytes are returned or logged."""
    directory.mkdir(mode=0o700, exist_ok=False)
    version = lab.command(["openssl", "version"]).stdout.strip()
    common = "basicConstraints=critical,CA:FALSE\nkeyUsage=critical,digitalSignature,keyEncipherment\nsubjectKeyIdentifier=hash\nauthorityKeyIdentifier=keyid,issuer\n"
    for stem, name in [("ca", "BigBrotr laboratory CA"), ("wrong_ca", "BigBrotr other laboratory CA")]:
        config = directory / (stem + ".cnf")
        private_write(config, ("[req]\ndistinguished_name=dn\nx509_extensions=ext\nprompt=no\n[dn]\nCN=" + name
                              + "\n[ext]\nbasicConstraints=critical,CA:TRUE\nkeyUsage=critical,keyCertSign,cRLSign\nsubjectKeyIdentifier=hash\n").encode())
        lab.command(["openssl", "req", "-new", "-x509", "-newkey", "rsa:2048", "-nodes", "-sha256", "-days", "2",
                     "-config", str(config), "-keyout", str(directory / (stem + ".key")), "-out", str(directory / (stem + ".crt"))])
    leaves = [("server", SERVER_NAME, "ca", "serverAuth", 1),
              ("client", ROLE, "ca", "clientAuth", 2),
              ("wrong_identity", "bigbrotr_wrong", "ca", "clientAuth", 3),
              ("wrong_client_ca", ROLE, "wrong_ca", "clientAuth", 1)]
    for stem, name, issuer, purpose, serial in leaves:
        config = directory / (stem + ".cnf")
        private_write(config, ("[req]\ndistinguished_name=dn\nprompt=no\n[dn]\nCN=" + name
                              + "\n[ext]\n" + common + "extendedKeyUsage=" + purpose + "\n"
                              + ("subjectAltName=DNS:" + SERVER_NAME + "\n" if stem == "server" else "")).encode())
        lab.command(["openssl", "req", "-new", "-newkey", "rsa:2048", "-nodes", "-sha256", "-config", str(config),
                     "-keyout", str(directory / (stem + ".key")), "-out", str(directory / (stem + ".csr"))])
        lab.command(["openssl", "x509", "-req", "-sha256", "-days", "2", "-in", str(directory / (stem + ".csr")),
                     "-CA", str(directory / (issuer + ".crt")), "-CAkey", str(directory / (issuer + ".key")),
                     "-set_serial", str(serial), "-extfile", str(config), "-extensions", "ext", "-out", str(directory / (stem + ".crt"))])
        lab.command(["openssl", "verify", "-CAfile", str(directory / (issuer + ".crt")), "-purpose",
                     "sslserver" if purpose == "serverAuth" else "sslclient", str(directory / (stem + ".crt"))])
    for path in directory.iterdir():
        path.chmod(0o600)
        if path.is_symlink() or not path.is_file() or path.stat().st_uid != os.geteuid():
            raise RuntimeError("unexpected PKI file ownership")
    return {"openssl": version, "certificate_sha256": {p.name: sha(p.read_bytes()) for p in directory.glob("*.crt")}}


def tls_config(template, database, secret_directory, *, ca="ca", name=SERVER_NAME, client=None):
    text = lab.change(template, "postgres.control", "database", database)
    text = lab.change(text, "postgres.control", "endpoint", "postgres:5432")
    section = {"mode": "verify-full", "ca_file": f"/lab/secrets/{secret_directory}/{ca}.crt", "server_name": name}
    if client:
        section["client_cert_ref"] = f"file:/lab/secrets/{secret_directory}/{client}.crt"
        section["client_key_ref"] = f"file:/lab/secrets/{secret_directory}/{client}.key"
    block = "[postgres.control.tls]\n" + "".join(f"{key} = {json.dumps(value)}\n" for key, value in section.items()) + "\n"
    text, count = re.subn(r"(?ms)^\[postgres\.control\.tls\]\n.*?(?=^\[|\Z)", lambda _: block, text)
    if count != 1:
        raise RuntimeError("expected one control TLS configuration block")
    return text


def log_snapshot(state):
    result = lab.docker("logs", "--tail", "2000", state["postgres"])
    return result.stdout + result.stderr


def log_observation(delta, database):
    marker = f"connection authorized: user={ROLE} database={database}"
    authorized = [line for line in delta.splitlines() if marker in line]
    tls = []
    for line in authorized:
        match = re.search(r"SSL enabled \(protocol=(TLSv[0-9.]+), cipher=([A-Za-z0-9_-]+), bits=([0-9]+)\)", line)
        if match:
            tls.append({"protocol": match[1], "cipher": match[2], "bits": int(match[3])})
    lines = delta.lower().splitlines()
    ssl_errors = [line for line in lines if "could not accept ssl connection:" in line]
    return {"authorized": len(authorized), "tls": tls,
            "unknown_ca_alert": sum("unknown ca" in line for line in ssl_errors),
            "bad_certificate_alert": sum("bad certificate" in line for line in ssl_errors),
            "client_certificate_untrusted": sum("certificate verify failed" in line for line in ssl_errors),
            "client_certificate_missing": sum("requires a valid client certificate" in line for line in lines),
            "client_identity_mismatch": sum("cn mismatch" in line and "clientcert=verify-full" in line for line in lines)}


def observe_call(state, database, config):
    before = log_snapshot(state)
    result = lab.migrate(state, "inspect", config)
    observed = {}
    deadline = time.monotonic() + 3
    while True:
        after = log_snapshot(state)
        if not after.startswith(before):
            raise RuntimeError("server log window changed; cannot attribute TLS observation")
        observed = log_observation(after[len(before):], database)
        if observed["authorized"] or any(value for key, value in observed.items() if key not in ("authorized", "tls")):
            break
        if time.monotonic() >= deadline:
            break
        time.sleep(0.1)
    try:
        value = json.loads(result.stdout)
    except (ValueError, TypeError):
        value = {}
    # Do not retain unexpected raw CLI/error/log text that might contain secrets.
    observed.update({"exit": result.returncode, "inspected_v1": value.get("status") == "inspected"
                     and value.get("current_version") == "1", "stdout_empty": not result.stdout,
                     "safe_backend_error": result.stderr == BACKEND_ERROR})
    return observed


def successful(observed):
    return (observed["exit"] == 0 and observed["inspected_v1"] and observed["authorized"] == 1
            and len(observed["tls"]) == 1 and observed["tls"][0]["protocol"] in ("TLSv1.2", "TLSv1.3")
            and observed["tls"][0]["bits"] >= 128)


def settings(state):
    names = ",".join("'" + name + "'" for name in SETTINGS)
    return json.loads(lab.sql(state, "postgres", f"SELECT json_object_agg(name,setting) FROM pg_settings WHERE name IN ({names});"))


def run(root, name):
    if root.resolve() != root or not re.fullmatch(r"[a-z_]{1,20}", name):
        raise RuntimeError("expected a canonical absolute lab root and bounded run name")
    source = Path(__file__).resolve().parents[2]
    if Path(lab.__file__).resolve().parent != Path(__file__).resolve().parent:
        raise RuntimeError("TLS cases require lab.py from the same source revision")
    if lab.command(["git", "-C", str(source), "status", "--porcelain", "--untracked-files=all"]).stdout:
        raise RuntimeError("TLS harness source must be clean")
    state = lab.load(root)
    if state["state"] != "running":
        raise RuntimeError("owned lab must be running")
    lock = os.open(root / "tls-mutation.lock", os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    directory = root / (name + "-tls")
    try:
        directory.mkdir(mode=0o700, exist_ok=False)
    except BaseException:
        os.close(lock)
        raise
    report_path = directory / "result.json"
    report = {"format": "bigbrotr-pg-tls/1", "status": "RUNNING", "restoration": "NOT_RUN",
              "started_at": datetime.now(timezone.utc).isoformat(),
              "cases": {case: {"status": "NOT_RUN"} for case in CASE_NAMES},
              "harness_revision": lab.command(["git", "-C", str(source), "rev-parse", "HEAD"]).stdout.strip(),
              "harness_sha256": sha(Path(__file__).read_bytes()), "lab_sha256": sha(Path(lab.__file__).read_bytes()),
              "application_source": state["build"]["build_info"]["source"], "binary_sha256": state["binary_sha256"],
              "server": state["server"], "containers": {}, "database": "bb_" + name + "_tls",
              "resources_retained": [str(directory), str(root / "secrets" / (name + "-tls")), str(root / "tls-mutation.lock")]}
    lab.write_json(report_path, report)
    container_ids = {}

    def owned(require_running=True):
        checked = lab.load(root)
        if checked["owner"] != state["owner"]:
            raise RuntimeError("lab owner changed during TLS run")
        for key in container_ids:
            info = json.loads(lab.docker("inspect", state[key]).stdout)[0]
            if info["Id"] != container_ids[key] or (require_running and not info["State"]["Running"]):
                raise RuntimeError("owned container identity/state changed")

    def record(case, passed, observations):
        report["cases"][case] = {"status": "PASS" if passed else "FAIL", "observed": observations}
        lab.write_json(report_path, report)
        if not passed:
            raise RuntimeError("TLS acceptance case failed: " + case)

    database = report["database"]
    backups = {}
    mutated = False
    original_settings = None
    baseline = None
    schema_before = None
    failure = None
    try:
        for key in ("postgres", "client"):
            info = json.loads(lab.docker("inspect", state[key]).stdout)[0]
            if not info["State"]["Running"]:
                raise RuntimeError("owned container is not running")
            container_ids[key] = info["Id"]
            report["containers"][key] = {"id": info["Id"], "image": info["Image"]}
        owned()
        actual_hash = lab.docker("exec", state["client"], "python3", "-c",
                                "import hashlib; print(hashlib.sha256(open('/build/bigbrotr','rb').read()).hexdigest())").stdout.strip()
        if actual_hash != state["binary_sha256"]:
            raise RuntimeError("mounted executable changed since lab setup")
        for private_root in (root, root / "secrets", root / "data"):
            info = private_root.lstat()
            if not stat.S_ISDIR(info.st_mode) or private_root.resolve() != private_root or info.st_uid != os.geteuid():
                raise RuntimeError("lab directory ownership/type changed")
        if stat.S_IMODE((root / "secrets").stat().st_mode) != 0o700:
            raise RuntimeError("lab secret directory must remain private")
        network = json.loads(lab.docker("network", "inspect", state["network"]).stdout)[0]
        if not network["Internal"] or network["Labels"].get(lab.LABEL) != state["owner"]:
            raise RuntimeError("TLS laboratory network ownership/isolation changed")
        report["network"] = {"id": network["Id"], "name": state["network"], "internal": True}
        for key in ("postgres", "client"):
            info = json.loads(lab.docker("inspect", state[key]).stdout)[0]
            if set(info["NetworkSettings"]["Networks"]) != {state["network"]} or info["HostConfig"]["PortBindings"]:
                raise RuntimeError("TLS laboratory must have one internal network and no published ports")
        report["server"] = json.loads(lab.sql(state, "postgres", "SELECT json_build_object('version',current_setting('server_version_num'),'fsync',current_setting('fsync'),'full_page_writes',current_setting('full_page_writes'),'architecture',version());"))
        original_settings = settings(state)
        if original_settings["ssl"] != "off" or report["server"]["version"] != "160015":
            raise RuntimeError("TLS fixture requires the fresh PG16.15 plaintext lab baseline")
        paths = json.loads(lab.sql(state, "postgres", "SELECT json_build_object('hba',current_setting('hba_file'),'data',current_setting('data_directory'),'config',current_setting('config_file'));"))
        if paths != {"hba": "/var/lib/postgresql/data/pg_hba.conf", "data": "/var/lib/postgresql/data", "config": "/var/lib/postgresql/data/postgresql.conf"}:
            raise RuntimeError("unexpected PostgreSQL configuration paths")
        pg = json.loads(lab.docker("inspect", state["postgres"]).stdout)[0]
        data_mount = [m for m in pg["Mounts"] if m["Destination"] == paths["data"]]
        if len(data_mount) != 1 or data_mount[0]["Type"] != "bind" or data_mount[0]["Source"] != str(root / "data"):
            raise RuntimeError("PostgreSQL configuration is not in this owned root")
        secret_mount = [m for m in pg["Mounts"] if m["Destination"] == "/run/secrets"]
        if len(secret_mount) != 1 or secret_mount[0]["Type"] != "bind" or secret_mount[0]["Source"] != str(root / "secrets") or secret_mount[0]["RW"]:
            raise RuntimeError("server secret mount does not match the protected lab directory")
        client = json.loads(lab.docker("inspect", state["client"]).stdout)[0]
        lab_mount = [m for m in client["Mounts"] if m["Destination"] == "/lab"]
        if len(lab_mount) != 1 or lab_mount[0]["Type"] != "bind" or lab_mount[0]["Source"] != str(root):
            raise RuntimeError("client does not use this lab root")
        for filename in ("pg_hba.conf", "postgresql.auto.conf", "postgresql.conf"):
            path = root / "data" / filename
            info = path.lstat()
            if not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid():
                raise RuntimeError("unexpected configuration file ownership/type")
            data = path.read_bytes()
            backups[filename] = (data, stat.S_IMODE(info.st_mode))
            private_write(directory / ("original-" + filename), data)
        report["original_config_sha256"] = {key: sha(value[0]) for key, value in backups.items()}
        report["pki"] = generate_pki(root / "secrets" / (name + "-tls"))
        owned()
        lab.sql(state, "postgres", f"CREATE DATABASE {database} OWNER {ROLE};")
        if (root / (database + ".toml")).exists():
            raise RuntimeError("refusing an existing TLS fixture config")
        baseline = lab.config(root, state, database)
        applied = lab.migrate(state, "apply", baseline)
        if applied.returncode or json.loads(applied.stdout).get("status") != "applied":
            raise RuntimeError("TLS database migration prerequisite failed")
        schema_before = lab.schema_digest(state, database)
        first = lab.migrate(state, "inspect", baseline)
        record("plaintext_baseline", first.returncode == 0 and json.loads(first.stdout).get("current_version") == "1",
               {"exit": first.returncode, "ssl": original_settings["ssl"], "schema_sha256": schema_before})
        template = (root / "template.toml").read_text()
        configurations = {}
        for stem, kwargs in [("tls", {}), ("client", {"client": "client"}), ("wrong_ca", {"ca": "wrong_ca"}),
                             ("wrong_name", {"name": "wrong.tls.bigbrotr.invalid"}),
                             ("wrong_identity", {"client": "wrong_identity"}), ("wrong_client_ca", {"client": "wrong_client_ca"})]:
            path = directory / (stem + ".toml")
            private_write(path, tls_config(template, database, name + "-tls", **kwargs).encode())
            configurations[stem] = "/lab/" + path.relative_to(root).as_posix()
        report["static_candidates"] = {}
        for stem, path in configurations.items():
            validated = lab.cli(state, "config", "validate", "--file", path)
            report["static_candidates"][stem] = {"exit": validated.returncode}
            lab.write_json(report_path, report)
            if validated.returncode != 0:
                raise RuntimeError("actual CLI refused TLS candidate before server changes: " + stem)
        mutated = True  # Every path after this point attempts restoration.
        for key, value in {"ssl": "on", "ssl_cert_file": f"/run/secrets/{name}-tls/server.crt",
                           "ssl_key_file": f"/run/secrets/{name}-tls/server.key", "ssl_ca_file": f"/run/secrets/{name}-tls/ca.crt",
                           "log_connections": "on", "log_line_prefix": "%m [%p] ", "lc_messages": "C"}.items():
            owned()
            lab.sql(state, "postgres", f"ALTER SYSTEM SET {key} = '{value}';")

        def reload_hba(mandatory):
            owned()
            hba = (f"hostnossl {database} {ROLE} all reject\n"
                   f"hostssl {database} {ROLE} all scram-sha-256" + (" clientcert=verify-full" if mandatory else "") + "\n").encode()
            replace_file(root / "data" / "pg_hba.conf", hba + backups["pg_hba.conf"][0], backups["pg_hba.conf"][1])
            if lab.sql(state, "postgres", "SELECT pg_reload_conf();") != "t":
                raise RuntimeError("owned PostgreSQL reload refused")
            deadline = time.monotonic() + 5
            while settings(state)["ssl"] != "on" or settings(state)["log_connections"] != "on":
                if time.monotonic() >= deadline:
                    raise RuntimeError("TLS settings did not become active")
                time.sleep(0.1)
            errors = lab.sql(state, "postgres", "SELECT count(*) FROM pg_hba_file_rules WHERE error IS NOT NULL;")
            options = lab.sql(state, "postgres", "SELECT array_to_string(options,',') FROM pg_hba_file_rules WHERE line_number=2;")
            if errors != "0" or ("clientcert=verify-full" in options) != mandatory:
                raise RuntimeError("TLS HBA configuration was not parsed as expected")

        def positive(case, config):
            observed = observe_call(state, database, config)
            record(case, successful(observed), observed)

        def negative(case, config, good_config, signal):
            owned()
            before = observe_call(state, database, good_config)
            bad = observe_call(state, database, config)
            after = observe_call(state, database, good_config)
            passed = (successful(before) and successful(after) and bad["exit"] == 1 and bad["stdout_empty"]
                      and bad["safe_backend_error"] and bad["authorized"] == 0 and bad[signal] == 1)
            record(case, passed, {"before": before, "negative": bad, "after": after, "required_server_signal": signal})

        reload_hba(False)
        positive("tls_exact_ca_and_explicit_name", configurations["tls"])
        positive("tls_optional_client_certificate", configurations["client"])
        negative("tls_wrong_ca", configurations["wrong_ca"], configurations["tls"], "unknown_ca_alert")
        negative("tls_wrong_name", configurations["wrong_name"], configurations["tls"], "bad_certificate_alert")
        reload_hba(True)
        positive("mtls_correct_identity", configurations["client"])
        negative("mtls_missing_certificate", configurations["tls"], configurations["client"], "client_certificate_missing")
        negative("mtls_wrong_identity", configurations["wrong_identity"], configurations["client"], "client_identity_mismatch")
        negative("mtls_wrong_client_ca", configurations["wrong_client_ca"], configurations["client"], "client_certificate_untrusted")
    except BaseException as error:
        failure = type(error).__name__
        report["status"] = "FAIL"
        report["failure_class"] = failure
        raise
    finally:
        try:
            if mutated:
                owned(require_running=False)
                for filename in ("pg_hba.conf", "postgresql.auto.conf"):
                    data, mode = backups[filename]
                    replace_file(root / "data" / filename, data, mode)
                pg_state = json.loads(lab.docker("inspect", state["postgres"]).stdout)[0]
                if not pg_state["State"]["Running"]:
                    lab.docker("start", state["postgres"])
                    deadline = time.monotonic() + 30
                    while lab.docker("exec", state["postgres"], "pg_isready", "-U", "postgres", check=False).returncode:
                        if time.monotonic() >= deadline:
                            raise RuntimeError("restored owned server did not restart")
                        time.sleep(0.2)
                if lab.sql(state, "postgres", "SELECT pg_reload_conf();") != "t":
                    raise RuntimeError("restoration reload refused")
                deadline = time.monotonic() + 5
                while settings(state) != original_settings:
                    if time.monotonic() >= deadline:
                        raise RuntimeError("original PostgreSQL settings not restored")
                    time.sleep(0.1)
                file_match = all((root / "data" / filename).read_bytes() == data
                                 and stat.S_IMODE((root / "data" / filename).stat().st_mode) == mode
                                 for filename, (data, mode) in backups.items())
                last = lab.migrate(state, "inspect", baseline)
                schema_after = lab.schema_digest(state, database)
                record("restored_config_and_plaintext", file_match and last.returncode == 0 and schema_after == schema_before,
                       {"exact_original_files": file_match, "original_settings": settings(state) == original_settings,
                        "plaintext_exit": last.returncode, "schema_unchanged": schema_after == schema_before})
                report["restoration"] = "PASS"
            else:
                report["restoration"] = "NOT_NEEDED"
        except BaseException as restore_error:
            report["restoration"] = "FAIL"
            report["restore_failure_class"] = type(restore_error).__name__
            report["status"] = "FAIL"
            raise RuntimeError("TLS restoration failed; retain owned resources and protected backups") from None
        finally:
            if not failure and report["restoration"] == "PASS" and all(v["status"] == "PASS" for v in report["cases"].values()):
                report["status"] = "PASS"
            report["finished_at"] = datetime.now(timezone.utc).isoformat()
            totals = {key: sum(v["status"] == key for v in report["cases"].values())
                      for key in ("PASS", "FAIL", "NOT_RUN")}
            report["counts"] = {"collected": len(CASE_NAMES), "executed": totals["PASS"] + totals["FAIL"],
                                "passed": totals["PASS"], "failed": totals["FAIL"], "skipped": 0, "not_run": totals["NOT_RUN"]}
            lab.write_json(report_path, report)
            os.close(lock)
    print(json.dumps({"status": report["status"], "output": str(report_path), "counts": report["counts"]}))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--run-name", required=True)
    args = parser.parse_args()
    if not args.root.is_absolute():
        parser.error("root must be absolute")
    run(args.root, args.run_name)
