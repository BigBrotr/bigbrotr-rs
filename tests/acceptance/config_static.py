"""Actual executable static configuration slice; no live authority qualification."""
import hashlib
import json
from pathlib import Path
import platform
import re
import shutil
import stat
import tempfile

RECIPES = ("full", "archive-only", "sync-only", "monitor-only", "serve-existing", "split-role")
CANARY = b"BB_CONFIG_SECRET_CANARY"


def decoded(result):
    try:
        value = json.loads(result.stdout)
        return value if isinstance(value, dict) else {}
    except (ValueError, UnicodeError):
        return {}


def report_matches(result, expected, document):
    value = decoded(result)
    dependencies = value.get("required_dependencies", [])
    return (result.returncode == 0 and not result.stderr
            and value.get("format") == "bigbrotr-config-static/1"
            and value.get("status") == "valid-static"
            and value.get("document_sha256") == hashlib.sha256(document).hexdigest()
            and value.get("authority") == {"mode": "managed", "status": "unavailable-offline",
                 "desired_revision": None, "applied_revision": None, "activation": "not-performed"}
            and value.get("local_roles") == expected["roles"]
            and isinstance(dependencies, list) and all(isinstance(d, dict) for d in dependencies)
            and sorted(d.get("name", "") for d in dependencies) == expected["dependencies"]
            and all(d.get("runtime_identity") == "not-checked" for d in dependencies)
            and value.get("fixed_endpoint_counts") == expected["fixed"]
            and all(type(v) is int for v in value["fixed_endpoint_counts"].values())
            and value.get("full_signed_events") is True
            and value.get("listeners") == {"public": False}
            and value["listeners"]["public"] is False
            and value.get("signing") is False and value.get("directory_discovery") is False)


def secret_open_check(checks, trace, traces, expected_count, secret):
    # Full strings are requested from strace. Match attempted opens, including
    # failed opens; a self-reported "secret_contents_opened=false" is not evidence.
    opened = any(re.search(r"^(?:\d+\s+)?open\w*\(", line)
                 and (str(secret) in line or secret.name in line or "/run/secrets/" in line)
                 for text in traces for line in text.splitlines())
    observed = trace and len(traces) == expected_count and all(t.strip() for t in traces)
    checks.append({"name": "config_no_secret_open_syscalls",
                   "result": ("FAIL" if opened else "PASS") if observed else "NOT_RUN"})


def configuration(args, checks, inputs, runner):
    fixture = args.source_root / "tests/fixtures/config/static-expectations.json"
    expectations = json.loads(fixture.read_text())
    if (expectations.get("format") != "bigbrotr-static-config-expectations/1"
            or set(expectations["recipes"]) != set(RECIPES)):
        raise ValueError("missing static configuration expectations")
    expected = expectations["recipes"]
    helper = Path(__file__)
    selected_helper = args.source_root / "tests/acceptance/config_static.py"
    inputs["config_acceptance_helper_sha256"] = runner.sha256(helper)
    checks.append({"name": "config_acceptance_helper_identity", "result": "PASS" if selected_helper.is_file()
                   and runner.sha256(selected_helper) == runner.sha256(helper) else "FAIL"})
    files = [fixture, args.source_root / "contracts/config/schema.json",
             args.source_root / "docs/reference/configuration.md"]
    files += [args.source_root / "config/recipes" / (name + ".toml") for name in RECIPES]
    inputs["config_fixtures_sha256"] = {str(p.relative_to(args.source_root)): runner.sha256(p) for p in files}
    trace = shutil.which("strace") if platform.system() == "Linux" else None
    traces, init_traces = [], []
    invocations = 0

    def check(name, valid, **detail):
        checks.append({"name": "config_" + name, "result": "PASS" if valid else "FAIL", **detail})

    def invoke(working, label, arguments):
        nonlocal invocations
        invocations += 1
        return runner.observed_process(args, [str(args.binary.resolve()), *arguments], working,
                                       "config-" + label, trace, traces)

    with tempfile.TemporaryDirectory(prefix="bigbrotr-config-inputs-") as temporary:
        inputs_root = Path(temporary)
        secret = inputs_root / CANARY.decode()
        secret.write_bytes(CANARY + b"-CONTENTS\n")
        secret.chmod(0o400)
        documents = {name: (args.source_root / "config/recipes" / (name + ".toml")).read_bytes()
                     for name in RECIPES}
        staged = {}

        def stage(name, data):
            path = inputs_root / (name + ".toml")
            path.write_bytes(data)
            path.chmod(0o400)
            staged[path.name] = runner.sha256(path)
            return path

        paths = {name: stage(name, data) for name, data in documents.items()}
        base = documents["sync-only"].decode()
        hidden = base.replace("file:/run/secrets/bigbrotr-control", "file:" + str(secret))
        hidden = hidden.replace("wss://relay.example/", "wss://relay.example/" + CANARY.decode() + "?token=" + CANARY.decode())
        hidden = hidden.replace("/var/lib/bigbrotr", str(inputs_root / "uncreated-storage"))
        hidden += "\n[monitor.active_write]\nsigner=" + json.dumps("file:" + str(secret)) + "\n"
        hidden = hidden.replace("[sync]\n", "[sync]\nrequest_timeout='45s'\nlinear_limit=250\n")
        hidden_path = stage("redacted", hidden.encode())
        split = documents["split-role"].replace(b"owner_epoch = 1", b"owner_epoch = 18446744073709551615")
        split_path = stage("u64", split)
        invalid = {
            "unknown_key": (base + "\n[unknown]\npassword='" + CANARY.decode() + "'\n").encode(),
            "duplicate_key": base.replace('authority_mode = "managed"', 'authority_mode = "managed"\nauthority_mode = "managed"').encode(),
            "empty_fixed": base.replace('"wss://relay.example/"', "").encode(),
            "forced_neg_without_reader": base.replace("[sync]\n", "[sync]\nmode='negentropy'\n").encode(),
            "unsafe_memory": (base + "\n[runtime]\nhost_memory_bytes='1GiB'\n").encode(),
            "disabled_proxy": (base + "\n[network.tor]\nproxy='socks5://user:" + CANARY.decode() + "@127.0.0.1:9050'\n").encode(),
            "disabled_engine": documents["archive-only"].replace(b"http://127.0.0.1:8123", b"http://user:" + CANARY + b"@db.example:8123"),
            "unconfigured_signer": (base + "\n[monitor.active_write]\nenabled=true\n").encode(),
            "integer_overflow": split.replace(b"18446744073709551615", b"18446744073709551616"),
            "invalid_utf8": b"\xff" + CANARY,
            "excess_input": b" " * (10 * 1024 * 1024 + 1),
            "nested_input": re.sub(r"local_roles = \[[^]]*\]", "local_roles = " + "[" * 128 + '"sync"' + "]" * 128, base).encode(),
        }
        # Its raw list exceeds 8MiB despite collapsing to a tiny canonical URL.
        endpoint = '"wss://relay.example/' + "./" * 4000 + '"'
        invalid["raw_endpoint_expansion"] = base.replace('"wss://relay.example/"', ",".join([endpoint] * 1100)).encode()
        invalid_paths = {name: stage("invalid-" + name, data) for name, data in invalid.items()}
        inputs["config_generated_inputs_sha256"] = dict(staged)
        with runner.read_only_directory(checks, "config_read_only_cwd") as working:
            for name in RECIPES:
                result = invoke(working, "example-" + name, ["config", "example", "--recipe", name])
                check("example_" + name, result.returncode == 0 and not result.stderr and result.stdout == documents[name])
                for command in ("validate", "show"):
                    result = invoke(working, command + "-" + name, ["config", command, "--file", str(paths[name])])
                    check(command + "_" + name, report_matches(result, expected[name], documents[name]))
            for command, path in [("schema", files[1]), ("reference", files[2])]:
                result = invoke(working, command, ["config", command])
                check("generated_" + command, result.returncode == 0 and not result.stderr and result.stdout == path.read_bytes())
            result = invoke(working, "redacted", ["config", "show", "--file", str(hidden_path)])
            settings = decoded(result).get("settings", {})
            check("redacted_values_and_attribution", report_matches(result, expected["sync-only"], hidden.encode())
                  and CANARY not in result.stdout + result.stderr
                  and all(settings.get(key, {}).get("value") == "[redacted]" for key in
                          ("postgres.control.password_ref", "monitor.active_write.signer", "sync.endpoints.urls"))
                  and all(settings.get("sync.request_timeout", {}).get(key) == value for key, value in
                          {"value": "45000000", "source": "file", "reload": "connection", "applicability": "selected"}.items())
                  and settings.get("sync.request_timeout", {}).get("deployment_locked") is False
                  and settings.get("sync.linear_limit", {}).get("value") == "250"
                  and settings.get("monitor.active_write.signer", {}).get("applicability") == "staged-unselected"
                  and settings.get("clickhouse.write_password_ref", {}).get("applicability") == "staged-unselected")
            result = invoke(working, "u64", ["config", "show", "--file", str(split_path)])
            check("lossless_owner_epoch", report_matches(result, expected["split-role"], split)
                  and decoded(result).get("settings", {}).get("topology.remote.ingest.owner_epoch", {}).get("value") == "18446744073709551615")
            result = invoke(working, "doctor", ["doctor", "--file", str(hidden_path)])
            doctor = decoded(result)
            observed = {c.get("name"): c.get("status") for c in doctor.get("checks", []) if isinstance(c, dict)}
            check("doctor_incomplete", result.returncode == 4 and not result.stderr
                  and doctor.get("format") == "bigbrotr-doctor/1" and doctor.get("status") == "incomplete"
                  and doctor.get("ready") is False and doctor.get("secret_contents_opened") is False
                  and doctor.get("network_checks") == "not-run" and observed.get("static-configuration") == "PASS"
                  and all(observed.get(d) == "NOT_RUN" for d in expected["sync-only"]["dependencies"] + ["policy-authority"])
                  and all(observed.get(d) == "UNAVAILABLE" for d in ["storage.ingest_root", "storage.archive_root", "storage.scratch_root"])
                  and CANARY not in result.stdout + result.stderr)
            for name, path in invalid_paths.items():
                result = invoke(working, "invalid-" + name, ["config", "validate", "--file", str(path)])
                check("invalid_" + name, result.returncode == 3 and not result.stdout
                      and result.stderr.startswith(b"error:") and CANARY not in result.stderr)
            result = invoke(working, "missing", ["config", "validate", "--file", str(inputs_root / "missing.toml")])
            check("missing_input", result.returncode == 1 and not result.stdout and b"missing" in result.stderr)
            result = invoke(working, "usage", ["config", "validate"])
            check("explicit_file_required", result.returncode == 2 and not result.stdout and b"--file" in result.stderr)
            check("read_only_cwd_unchanged", not list(working.iterdir()))
        check("inputs_and_secret_unchanged", set(p.name for p in inputs_root.iterdir()) == set(staged) | {secret.name}
              and all(runner.sha256(inputs_root / name) == digest for name, digest in staged.items())
              and secret.read_bytes() == CANARY + b"-CONTENTS\n" and stat.S_IMODE(secret.stat().st_mode) == 0o400)

    with tempfile.TemporaryDirectory(prefix="bigbrotr-config-init-") as temporary:
        working = Path(temporary)
        created = []
        for number in range(2):
            path = working / ("instance-" + str(number) + ".toml")
            result = runner.observed_process(args, [str(args.binary.resolve()), "config", "init", "--recipe", "sync-only", "--output", str(path)],
                                             working, "config-init-" + str(number), trace, init_traces)
            valid = result.returncode == 0 and not result.stderr and decoded(result) == {"status": "created-candidate", "recipe": "sync-only", "activated": False}
            valid = valid and path.is_file() and not path.is_symlink()
            if path.is_file() and not path.is_symlink():
                contents = path.read_bytes()
                ids = re.findall(rb'^(?:dataset_id|instance_id) = "([0-9a-f]{32})"$', contents, re.M)
                valid = valid and len(ids) == 2 and len(set(ids)) == 2 and all(v != b"0" * 32 for v in ids)
                valid = valid and stat.S_IMODE(path.stat().st_mode) == 0o600
                created.append((contents, ids))
                validation = invoke(working, "init-validate-" + str(number), ["config", "validate", "--file", str(path)])
                valid = valid and report_matches(validation, expected["sync-only"], contents)
            check("init_fresh_" + str(number), valid)
        check("init_distinct_identities", len(created) == 2 and len(set(created[0][1] + created[1][1])) == 4)
        path = working / "instance-0.toml"
        before = path.read_bytes() if path.is_file() else None
        result = runner.observed_process(args, [str(args.binary.resolve()), "config", "init", "--recipe", "full", "--output", str(path)],
                                         working, "config-init-existing", trace, init_traces)
        check("init_no_overwrite", before is not None and result.returncode == 1 and not result.stdout and path.read_bytes() == before)
        check("init_only_requested_files", set(p.name for p in working.iterdir()) == {"instance-0.toml", "instance-1.toml"})
        # Init is the explicit write operation. Its output membership/contents and
        # permissions are checked above; read-only syscall assertions never hide it.
        runner.syscall_checks(checks, trace, init_traces, 3, "config_init_", check_mutations=False)
    runner.syscall_checks(checks, trace, traces, invocations, "config_")
    secret_open_check(checks, trace, traces, invocations, secret)
    inputs["config_invocations"] = {"read_only": invocations, "initialization": 3}
