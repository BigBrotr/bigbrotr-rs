#!/usr/bin/env python3
"""Run implemented practical cases against an explicitly supplied executable."""
import argparse
from contextlib import contextmanager
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import shutil
import signal
import subprocess
import sys
import tempfile


def sha256(path):
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def run_process(command, working, payload=None):
    # The tracer and its child share an owned process group. Losing the tracer's
    # reply must not leave a candidate or one of its descendants running.
    process = subprocess.Popen(command, cwd=working, env={"LANG": "C", "LC_ALL": "C"},
                               stdin=subprocess.PIPE if payload is not None else subprocess.DEVNULL,
                               stdout=subprocess.PIPE, stderr=subprocess.PIPE, start_new_session=True)
    try:
        stdout, stderr = process.communicate(input=payload, timeout=10)
        return subprocess.CompletedProcess(command, process.returncode, stdout, stderr)
    finally:
        try:
            os.killpg(process.pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
        try:
            process.wait(timeout=1)
        except subprocess.TimeoutExpired:
            pass
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        process.wait()


@contextmanager
def read_only_directory(checks, name):
    with tempfile.TemporaryDirectory(prefix="bigbrotr-acceptance-") as temporary:
        working = Path(temporary)
        working.chmod(0o555)
        try:
            checks.append({"name": name, "result": "PASS" if not os.access(working, os.W_OK) else "NOT_RUN",
                           "detail": "runtime must use an unprivileged account"})
            yield working
        finally:
            working.chmod(0o700)


def observed_process(args, command, working, label, trace, traces, payload=None):
    trace_file = args.evidence / (label + ".trace")
    if trace:
        command = [trace, "-f", "-s", "4096", "-e", "trace=network,%file,%desc", "-o", str(trace_file), *command]
    result = run_process(command, working, payload)
    (args.evidence / (label + ".stdout")).write_bytes(result.stdout)
    (args.evidence / (label + ".stderr")).write_bytes(result.stderr)
    if trace:
        traces.append(trace_file.read_text())
    return result


def syscall_checks(checks, trace, traces, expected_count, label="", check_mutations=True):
    calls = [line for trace_text in traces for line in trace_text.splitlines()]
    prefix = r"^(?:\d+\s+)?"
    network_calls = any(re.search(prefix + r"(?:socket|socketpair|connect|bind|listen|accept\d*|send\w*|recv\w*)\(", line) for line in calls)
    mutation = r"(?:creat|truncate|ftruncate|rename\w*|unlink\w*|mkdir\w*|rmdir|link\w*|symlink\w*|mknod\w*|[fl]?chmod\w*|[fl]?chown\w*|utime\w*)\("
    file_changes = any(re.search(prefix + mutation, line)
                       or (re.search(prefix + r"open\w*\(", line) and re.search(r"O_CREAT|O_TRUNC|O_TMPFILE|O_WRONLY|O_RDWR", line))
                       or re.search(prefix + r"(?:pwrite\w*|write\w*)\((?:[3-9]|\d{2,}),", line) for line in calls)
    observed = trace and len(traces) == expected_count and all(text.strip() for text in traces)
    checks.append({"name": label + "no_network_syscalls", "result": ("FAIL" if network_calls else "PASS") if observed else "NOT_RUN",
                   "detail": "requires Linux strace; disabled networking alone does not observe attempted calls"})
    if check_mutations:
        checks.append({"name": label + "no_file_mutation_syscalls", "result": ("FAIL" if file_changes else "PASS") if observed else "NOT_RUN"})


def foundation(args, checks, inputs):
    binary = args.binary.resolve()
    if not binary.is_file() or not os.access(binary, os.X_OK):
        checks.append({"name": "executable", "result": "NOT_RUN", "detail": "missing executable"})
        return
    inputs["binary_sha256"] = sha256(binary)
    inputs["acceptance_runner_sha256"] = sha256(Path(__file__))
    selected_runner = args.source_root / "tests/acceptance/run.py"
    checks.append({"name": "acceptance_runner_identity", "result": "PASS" if selected_runner.is_file()
                   and inputs["acceptance_runner_sha256"] == sha256(selected_runner) else "FAIL"})
    trace = shutil.which("strace") if platform.system() == "Linux" else None
    traces = []
    source_valid = False
    artifact_valid = False
    with read_only_directory(checks, "read_only_cwd") as working:
        for number, arguments in enumerate([["--help"], ["--version"], ["--invalid-option"], [], ["serve"], ["build-info"]]):
            command = [str(binary), *arguments]
            result = observed_process(args, command, working, "cli-" + str(number), trace, traces)
            if arguments in [["--help"], ["--version"]]:
                valid = result.returncode == 0 and b"BigBrotr" in result.stdout and not result.stderr
            elif arguments == ["build-info"]:
                valid = result.returncode == 0 and not result.stderr
                try:
                    info = json.loads(result.stdout)
                    inputs["build_info"] = info
                    valid = valid and info["format"] == "bigbrotr-build/1" and info["product"] == "BigBrotr"
                    version = re.search(r'^version = "([^"]+)"$', (args.source_root / "Cargo.toml").read_text(), re.M).group(1)
                    compiler = re.search(r'^channel = "([^"]+)"$', (args.source_root / "rust-toolchain.toml").read_text(), re.M).group(1)
                    target = {("Linux", "x86_64"): "x86_64-unknown-linux-gnu", ("Linux", "aarch64"): "aarch64-unknown-linux-gnu",
                              ("Darwin", "arm64"): "aarch64-apple-darwin", ("Darwin", "x86_64"): "x86_64-apple-darwin"}.get((platform.system(), platform.machine()))
                    valid = valid and info["version"] == version and info["rustc"].startswith("rustc " + compiler + " ")
                    valid = valid and target is not None and info["target"] == target
                    versions = json.loads((args.source_root / "contracts/protocol/versions.json").read_text())
                    valid = valid and versions["format"] == "bigbrotr-protocol-versions/1"
                    valid = valid and info["protocol_registry_format"] == versions["format"] and info["capability_scope"] == versions["scope"]
                    valid = valid and all(info[name] == versions[name] for name in ("capabilities", "protocols", "schemas"))
                    expected = {}
                    for name in ("Cargo.lock", "rust-toolchain.toml", "contracts/release/dependencies.json", "contracts/protocol/versions.json", "contracts/protocol/nip01-event.json"):
                        expected[name] = sha256(args.source_root / name)
                    inputs["expected_inputs_sha256"] = expected
                    valid = valid and info["inputs_sha256"] == expected
                    if args.source_manifest:
                        source = json.loads(args.source_manifest.read_text())
                        inputs["source_manifest_sha256"] = sha256(args.source_manifest)
                        actual_files = {str(path.relative_to(args.source_root)): sha256(path)
                                        for path in sorted(args.source_root.rglob("*")) if path.is_file()}
                        source_valid = source["format"] == "bigbrotr-source/1" and source["files_sha256"] == actual_files
                        source_valid = source_valid and info["source"] == {"state": "builder-supplied", "revision": source["revision"], "tree": source["tree"], "dirty": False, "manifest_sha256": inputs["source_manifest_sha256"]}
                        valid = valid and source_valid
                    if args.build_result:
                        build = json.loads(args.build_result.read_text())
                        inputs["build_result_sha256"] = sha256(args.build_result)
                        artifact_valid = build["format"] == "bigbrotr-build-result/1" and build["binary_sha256"] == inputs["binary_sha256"] and build["build_info"] == info
                        artifact_valid = artifact_valid and build["source_manifest_sha256"] == inputs.get("source_manifest_sha256")
                        artifact_valid = artifact_valid and build["helper_sha256"] == sha256(args.source_root / "tools/build.py")
                        artifact_valid = artifact_valid and info["profile_assertion"] == "release" and info["optimization"] == "3" and info["debug_info"] == "false"
                        valid = valid and artifact_valid
                except (ValueError, KeyError, TypeError, AttributeError, OSError):
                    valid = False
            else:
                valid = result.returncode == 2 and not result.stdout and b"Usage:" in result.stderr
            checks.append({"name": "cli " + " ".join(arguments), "result": "PASS" if valid else "FAIL",
                           "exit_code": result.returncode})
        checks.append({"name": "no_created_files", "result": "PASS" if not list(working.iterdir()) else "FAIL"})
    syscall_checks(checks, trace, traces, 6)
    checks.append({"name": "clean_source_identity", "result": ("PASS" if source_valid else "FAIL") if args.source_manifest else "NOT_RUN",
                   "detail": "requires the clean builder's source manifest"})
    checks.append({"name": "expected_artifact", "result": ("PASS" if artifact_valid else "FAIL") if args.build_result else "NOT_RUN",
                   "detail": "equality against supplied build result; not release authenticity"})


def signed_events(args, checks, inputs):
    # Literal independently signed fixtures are decoded by Python's integer-
    # preserving JSON parser, never by the candidate used as the oracle.
    historical = args.source_root / "experiments/identity-contract/signed-fixture.json"
    extremes = args.source_root / "tests/fixtures/protocol/signed-extremes.json"
    inputs["event_fixtures_sha256"] = {str(path.relative_to(args.source_root)): sha256(path)
                                      for path in (historical, extremes)}
    corpus = json.loads(historical.read_text())["events"]
    extra = json.loads(extremes.read_text())
    corpus.update({name: row["event"] for name, row in extra["events"].items()})
    checks.append({"name": "nonempty_frozen_event_corpus", "result": "PASS" if len(corpus) == 17 else "FAIL"})
    trace = shutil.which("strace") if platform.system() == "Linux" else None
    traces = []
    with read_only_directory(checks, "event_read_only_cwd") as directory:
        for number, (name, event) in enumerate(corpus.items()):
            result = observed_process(args, [str(args.binary.resolve()), "event", "verify"], directory,
                                      "event-valid-" + str(number), trace, traces,
                                      json.dumps(event, ensure_ascii=False, separators=(",", ":")).encode())
            try:
                equal = json.loads(result.stdout) == event
            except (ValueError, UnicodeError):
                equal = False
            checks.append({"name": "signed_event " + name, "result": "PASS" if result.returncode == 0 and not result.stderr and equal else "FAIL"})
        original = corpus["E_public"]
        changed = dict(original, content=original["content"] + "!")
        forged = dict(original, sig="00" * 64)
        empty_tag = extra["invalid_structure"]["empty-tag"]
        ordinary = json.dumps(original, separators=(",", ":")).encode()
        negatives = [("changed_payload", json.dumps(changed).encode(), 3, b"event-id-mismatch"),
                     ("forged_signature", json.dumps(forged).encode(), 3, b"invalid-signature"),
                     ("signed_empty_tag", json.dumps(empty_tag).encode(), 3, b"empty-tag"),
                     ("invalid_unknown_utf8", b'{"unknown":"\xff",' + ordinary[1:], 3, b"invalid-json"),
                     ("excess_wire", b" " * (2 * 1024 * 1024 + 1), 4, b"wire-limit")]
        for number, (name, payload, exit_code, reason) in enumerate(negatives):
            result = observed_process(args, [str(args.binary.resolve()), "event", "verify"], directory,
                                      "event-invalid-" + str(number), trace, traces, payload)
            checks.append({"name": name, "result": "PASS" if result.returncode == exit_code and not result.stdout and reason in result.stderr else "FAIL"})
        checks.append({"name": "event_command_no_created_files", "result": "PASS" if not list(directory.iterdir()) else "FAIL"})

    syscall_checks(checks, trace, traces, 22, "event_")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("case", choices=["p01-t01", "p01-event-cli", "p01-config-static", "p01-first-launch", "p01-first-launch-tutorial", "p01-first-launch-restart", "p01-first-launch-restart-provenance-off", "p01-first-launch-index", "p01-first-launch-index-short-pin", "p01-first-launch-index-restart", "p01-first-launch-index-pin-release-before", "p01-first-launch-index-pin-release-after", "p01-first-launch-index-commit-before", "p01-first-launch-index-commit-after", "p01-first-launch-index-topology", "p01-first-launch-index-live-topology", "p01-first-launch-archive-outage", "p01-first-launch-index-outage", "p01-first-launch-shared-budget", "p01-first-launch-retained-boundary"])
    parser.add_argument("--binary", type=Path)
    parser.add_argument("--source-root", required=True, type=Path)
    parser.add_argument("--source-manifest", type=Path)
    parser.add_argument("--build-result", type=Path)
    parser.add_argument("--candidate-manifest", type=Path,
                        help="p01-first-launch: detached candidate manifest beside its bundle")
    parser.add_argument("--native-observer-build", type=Path,
                        help="finite64 or Index outage: separately declared exact-source native observer build")
    parser.add_argument("--compose-network",
                        help="p01-first-launch: borrow an existing local bridge; never remove it")
    parser.add_argument("--evidence", required=True, type=Path)
    args = parser.parse_args()
    if args.native_observer_build is not None and args.case not in ("p01-first-launch-retained-boundary", "p01-first-launch-index-outage"):
        parser.error("--native-observer-build is only for finite64 or Index outage")
    installed = args.case in ("p01-first-launch", "p01-first-launch-tutorial", "p01-first-launch-restart", "p01-first-launch-restart-provenance-off", "p01-first-launch-index", "p01-first-launch-index-short-pin", "p01-first-launch-index-restart", "p01-first-launch-index-pin-release-before", "p01-first-launch-index-pin-release-after", "p01-first-launch-index-commit-before", "p01-first-launch-index-commit-after", "p01-first-launch-index-topology", "p01-first-launch-index-live-topology", "p01-first-launch-archive-outage", "p01-first-launch-index-outage", "p01-first-launch-shared-budget", "p01-first-launch-retained-boundary")
    if not installed and args.binary is None:
        parser.error("--binary is required for offline cases")
    if not installed and (args.candidate_manifest or args.compose_network):
        parser.error("candidate and Compose arguments are only for p01-first-launch")
    if installed and any((args.binary, args.source_manifest, args.build_result)):
        parser.error("p01-first-launch selects its artifact through --candidate-manifest")
    if args.case == 'p01-first-launch-tutorial':
        from cases.first_launch_tutorial import canonical_inputs
        try:
            args.tutorial_paths = canonical_inputs(args)
        except (OSError, ValueError, subprocess.SubprocessError) as error:
            parser.error(str(error))
    args.evidence = args.evidence.resolve()
    args.evidence.mkdir(mode=0o700, parents=True, exist_ok=False)
    checks, inputs = [], {}
    try:
        if installed:
            from cases.first_launch import first_launch
            first_launch(args, checks, inputs)
        else:
            foundation(args, checks, inputs)
        if args.case == "p01-event-cli" and "binary_sha256" in inputs:
            signed_events(args, checks, inputs)
        if args.case == "p01-config-static" and "binary_sha256" in inputs:
            from config_static import configuration
            configuration(args, checks, inputs, sys.modules[__name__])
    except (OSError, subprocess.SubprocessError, KeyboardInterrupt, ValueError, KeyError, TypeError, AttributeError, AssertionError, RuntimeError) as error:
        checks.append({"name": "execution", "result": "FAIL", "detail": type(error).__name__})
    result = "FAIL" if any(check["result"] == "FAIL" for check in checks) else (
        "NOT_RUN" if not checks or any(check["result"] == "NOT_RUN" for check in checks) else "PASS")
    report = {"format": "bigbrotr-acceptance/1", "case": args.case, "required": True,
              "result": result, "environment": {"os": platform.system(), "architecture": platform.machine()},
              "inputs": inputs, "checks": checks,
              "counts": {"collected": len(checks), "executed": sum(c["result"] in ("PASS", "FAIL") for c in checks),
                         "failed": sum(c["result"] == "FAIL" for c in checks), "skipped": 0,
                         "not_run": sum(c["result"] == "NOT_RUN" for c in checks)},
              "cleanup": getattr(args, "case_cleanup", "Owned process groups terminated/joined; only runner-owned temporary directories removed; evidence retained."),
              "limits": getattr(args, "case_limits", ["No storage, WebSocket/protocol-suite, runtime grant, release bundle or complete product qualification."])}
    (args.evidence / "result.json").write_text(json.dumps(report, indent=2) + "\n")
    print(result + ": " + str(args.evidence / "result.json"))
    return {"PASS": 0, "FAIL": 1, "NOT_RUN": 2}[result]


if __name__ == "__main__":
    sys.exit(main())
