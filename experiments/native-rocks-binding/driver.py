#!/usr/bin/env python3
"""Process/lifecycle oracle for the native Rust RocksDB binding experiment."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import selectors
import signal
import subprocess
import sys
import time


TIMEOUT_SECONDS = 30


def run(binary, *args, expect=0, timeout=TIMEOUT_SECONDS):
    completed = subprocess.run(
        [str(binary), *map(str, args)],
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=timeout,
        check=False,
    )
    if completed.returncode != expect:
        raise AssertionError(
            f"command {args!r} exited {completed.returncode}, expected {expect}; "
            f"stdout={completed.stdout!r}; stderr={completed.stderr!r}"
        )
    return completed


def spawn(binary, *args):
    return subprocess.Popen(
        [str(binary), *map(str, args)],
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        start_new_session=True,
    )


def wait_line(process, expected, timeout=TIMEOUT_SECONDS):
    selector = selectors.DefaultSelector()
    selector.register(process.stdout, selectors.EVENT_READ)
    ready = selector.select(timeout)
    selector.close()
    if not ready:
        terminate(process)
        raise AssertionError(f"timeout waiting for {expected!r}")
    line = process.stdout.readline().rstrip("\n")
    if line != expected:
        stderr = process.stderr.read() if process.poll() is not None else ""
        terminate(process)
        raise AssertionError(f"expected barrier {expected!r}, got {line!r}; stderr={stderr!r}")


def terminate(process):
    if process.poll() is None:
        os.killpg(process.pid, signal.SIGKILL)
    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired as error:
        raise AssertionError("owned process group did not terminate") from error


def mask(binary, db, token):
    fields = run(binary, "inspect", db, token).stdout.strip().split("\t")
    if len(fields) != 4 or fields[0] not in {"000", "111"}:
        raise AssertionError(f"atomic token {token!r} returned invalid inspection {fields!r}")
    observed, actual = fields[0], fields[1:]
    expected = [f"sequence:{token}", f"fact:{token}", f"receipt:{token}"]
    if observed == "111" and actual != expected:
        raise AssertionError(f"atomic token {token!r} values {actual!r} != {expected!r}")
    if observed == "000" and actual != ["-", "-", "-"]:
        raise AssertionError(f"absent token {token!r} returned values {actual!r}")
    return observed


def file_sha256(path):
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def command_text(*command):
    try:
        completed = subprocess.run(
            command,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            timeout=10,
            check=False,
        )
    except FileNotFoundError:
        # bindgen needs libclang, not the clang executable. The pinned Linux
        # image compiles C++ with GCC; optional diagnostics must report absence.
        return {"command": list(command), "exit_code": None, "status": "NOT_AVAILABLE"}
    return {"command": list(command), "exit_code": completed.returncode, "output": completed.stdout.strip()}


def inspect_native_log(path):
    text = path.read_text(errors="strict")
    required = {
        "rocksdb_version": ("RocksDB version: 11.8.1", 1),
        "use_fsync": ("Options.use_fsync: 1", 1),
        "atomic_flush": ("Options.atomic_flush: 1", 1),
        "max_background_jobs": ("Options.max_background_jobs: 2", 1),
        "max_total_wal_size": ("Options.max_total_wal_size: 16777216", 1),
        "max_open_files": ("Options.max_open_files: 64", 1),
        "cache_capacity": ("capacity : 33554432", 3),
        "block_size": ("block_size: 16384", 3),
        "write_buffer_size": ("Options.write_buffer_size: 2097152", 3),
        "zstd": ("Options.compression: ZSTD", 3),
        "bottommost_zstd": ("Options.bottommost_compression: ZSTD", 3),
    }
    for name, (fragment, count) in required.items():
        observed = text.count(fragment)
        if observed != count:
            raise AssertionError(
                f"native LOG {name} count {observed}, expected {count}: {fragment!r}"
            )
    cache_ids = re.findall(r"^\s*block_cache: (0x[0-9a-fA-F]+)$", text, re.MULTILINE)
    if len(cache_ids) != 3 or len(set(cache_ids)) != 1 or cache_ids[0] == "0x0":
        raise AssertionError(f"native LOG did not show one shared non-null cache: {cache_ids!r}")
    managers = re.findall(
        r"Options\.write_buffer_manager: (0x[0-9a-fA-F]+)", text
    )
    if len(managers) != 1 or managers[0] == "0x0":
        raise AssertionError(f"native LOG did not show an active write-buffer manager: {managers!r}")
    return {
        "rocksdb_version": "11.8.1",
        "validated_option_fields": sorted(required),
        "column_family_descriptors_with_same_block_cache": len(cache_ids),
        "write_buffer_manager": "active",
    }



CALLBACK_CASES = (
    ("nospace-no-error", 0), ("nospace-soft", 0), ("nospace-hard", 0),
    ("nospace-fatal", 0), ("nospace-unrecoverable", 0), ("okay", 0),
    ("different-error", 0), ("io-flags", 0), ("recovery-veto", 0),
    ("sticky-lifetime", 0), ("guard-drop-first", 0), ("allocation-positive", 0),
    ("allocation-no-copy", 0), ("allocation-termination", 86),
)


def guard_callback_controls(args):
    required = ("source_root", "source_manifest", "native_context", "rocksdb_archive", "zstd_archive")
    if any(getattr(args, name) is None for name in required):
        raise SystemExit("guard-callback-controls requires source, manifest, native context and both archives")
    source = args.source_root.resolve(strict=True)
    paths = {"binary": args.binary, "cargo_lock": args.cargo_lock,
             "source_manifest": args.source_manifest, "native_context": args.native_context,
             "rocksdb_archive": args.rocksdb_archive, "zstd_archive": args.zstd_archive}
    for name, path in paths.items():
        if path.is_symlink() or not path.is_file():
            raise AssertionError(f"{name} must be a regular non-symlink input")
        paths[name] = path.resolve(strict=True)
    if paths["cargo_lock"] != source / "Cargo.lock":
        raise AssertionError("callback controls require the selected root Cargo lock")
    manifest = json.loads(paths["source_manifest"].read_text())
    context = json.loads(paths["native_context"].read_text())
    if manifest["format"] != "bigbrotr-source/1":
        raise AssertionError("expected the selected source manifest")
    if (context["format"] != "bigbrotr-native-c-api-archive-context/1"
            or context["status"] != "ARCHIVES_CAPTURED_C_TEST_NOT_RUN"
            or context["source"] != manifest["revision"] or context["tree"] != manifest["tree"]
            or context["profile"] != "release" or context["target"] != "x86_64-unknown-linux-gnu"
            or context["source_manifest_sha256"] != file_sha256(paths["source_manifest"])
            or context["cargo_lock_sha256"] != file_sha256(paths["cargo_lock"])):
        raise AssertionError("callback context does not bind this source/profile/lock")
    source_names = (
        "experiments/native-rocks-binding/background_guard_test.cc",
        "experiments/native-rocks-binding/driver.py",
        "vendor/librocksdb-sys/rocksdb/db/c_options_internal.h",
        "vendor/librocksdb-sys/rocksdb/db/c.cc",
        "vendor/librocksdb-sys/rocksdb/include/rocksdb/status.h",
        "vendor/librocksdb-sys/rocksdb/util/status.cc",
        "vendor/librocksdb-sys/rocksdb/include/rocksdb/io_status.h",
        "vendor/librocksdb-sys/rocksdb/include/rocksdb/listener.h",
        "vendor/librocksdb-sys/rocksdb/include/rocksdb/c.h",
        "vendor/librocksdb-sys/rocksdb/include/rocksdb/options.h",
        "vendor/librocksdb-sys/rocksdb/include/rocksdb/advanced_options.h",
        "vendor/librocksdb-sys/rocksdb/include/rocksdb/rocksdb_namespace.h",
        "vendor/librocksdb-sys/rocksdb/tools/c_api_gen/c_base.cc",
        "vendor/librocksdb-sys/build.rs", "vendor/librocksdb-sys.provenance.json",
    )
    for name in source_names:
        path = source / name
        if path.is_symlink() or file_sha256(path) != manifest["files_sha256"][name]:
            raise AssertionError(f"callback source differs from manifest: {name}")
        paths[name] = path
    if file_sha256(Path(__file__)) != manifest["files_sha256"]["experiments/native-rocks-binding/driver.py"]:
        raise AssertionError("executing driver differs from the selected source")
    expected_archives = {
        "librocksdb-sys": ("rocksdb_archive", "0.19.0+11.8.1", ["bindgen-runtime", "static", "zstd", "zstd-sys"]),
        "zstd-sys": ("zstd_archive", "2.1.2+zstd.1.5.7", ["legacy", "zdict_builder"]),
    }
    if len(context["archives"]) != 2 or {item["package"] for item in context["archives"]} != set(expected_archives):
        raise AssertionError("callback controls require exactly the two selected native archives")
    for item in context["archives"]:
        key, version, features = expected_archives[item["package"]]
        archive = paths[key]
        with archive.open("rb") as stream:
            magic = stream.read(8)
        if item["package"] == "librocksdb-sys":
            if (not item["package_id"].startswith("path+file://")
                    or not item["package_id"].endswith("/vendor/librocksdb-sys#0.19.0+11.8.1")
                    or Path(item["manifest_path"]).parts[-3:] != ("vendor", "librocksdb-sys", "Cargo.toml")):
                raise AssertionError("callback context must select the local sys package")
        elif item["package_id"] != "registry+https://github.com/rust-lang/crates.io-index#zstd-sys@2.1.2+zstd.1.5.7":
            raise AssertionError("callback context must select the locked zstd package")
        if (item["version"] != version or item["features"] != features
                or item["bytes"] != archive.stat().st_size or item["sha256"] != file_sha256(archive)
                or magic != b"!<arch>\n"):
            raise AssertionError("native archive bytes or selected features differ")
    hashes = {name: file_sha256(path) for name, path in paths.items()}
    if (args.lab_root.exists() or args.lab_root.is_symlink()
            or args.output.exists() or args.output.is_symlink()):
        raise SystemExit("callback controls require a fresh lab root and report")
    args.lab_root.mkdir(parents=True)
    report = {"format": "bigbrotr-native-guard-callback-controls/1", "result": "RUNNING",
              "source": manifest["revision"], "tree": manifest["tree"], "profile": "release",
              "scope": "Direct actual installed listener callbacks/allocation/lifetime only; no running DB or real NoSpace",
              "expected_case_count": len(CALLBACK_CASES),
              "artifact_identity": hashes, "native_toolchain": context["native_toolchain"],
              "compiler": context["compiler"], "cases": [],
              "not_run": ["real POSIX NoSpace and SFM/Resume suppression", "concurrent fault/query/close",
                          "application guard/write/ACK/funding wiring", "purge credit and physical bounds",
                          "new platform, full product, M1 or release qualification"]}

    def write_report():
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")

    def verify_inputs():
        if any(file_sha256(path) != hashes[name] for name, path in paths.items()):
            raise AssertionError("callback binary/source/lock/context/archive changed")

    write_report()
    active_case = "profile-and-inventory"
    try:
        identity = run(paths["binary"], "--identity")
        if identity.stdout.splitlines() != ["GUARD_CALLBACK_PROFILE\tcplusplus=202002\tNDEBUG=1\tstatus_checked=0\tnamespace=rocksdb"] or identity.stderr:
            raise AssertionError(f"callback frontend ABI/profile identity differs: {identity.stdout!r} / {identity.stderr!r}")
        report["frontend_profile"] = identity.stdout.strip()
        listed = run(paths["binary"], "--list")
        if listed.stdout.splitlines() != [f"{name}\t{code}" for name, code in CALLBACK_CASES] or listed.stderr:
            raise AssertionError(f"callback case inventory is empty, extra or different: {listed.stdout!r} / {listed.stderr!r}")
        (args.lab_root / "inventory.txt").write_text(listed.stdout)
        for name, expected_exit in CALLBACK_CASES:
            active_case = name
            verify_inputs()
            completed = run(paths["binary"], "--case", name, expect=expected_exit)
            output = completed.stdout.splitlines()
            expected_output = (["PHASE allocation-termination prearmed",
                                "PHASE allocation-termination injected-and-latched"]
                               if name == "allocation-termination" else [f"PASS {name}"])
            if output != expected_output or completed.stderr:
                raise AssertionError(f"{name} returned wrong callback/termination proof: {completed.stdout!r} / {completed.stderr!r}")
            record = {"name": name, "status": "PASS", "expected_exit": expected_exit,
                      "actual_exit": completed.returncode, "stdout": completed.stdout, "stderr": completed.stderr}
            report["cases"].append(record)
            (args.lab_root / (name + ".json")).write_text(json.dumps(record, indent=2) + "\n")
            verify_inputs()
            write_report()
        if len(report["cases"]) != len(CALLBACK_CASES):
            raise AssertionError("nonempty complete callback execution required")
        report["result"] = "PASS"
        write_report()
    except BaseException as error:
        report["result"] = "FAIL"
        report["failure"] = {"case": active_case, "kind": type(error).__name__, "detail": str(error),
                             "stdout": str(getattr(error, "stdout", "")),
                             "stderr": str(getattr(error, "stderr", ""))}
        write_report()
        raise
    print(json.dumps(report, indent=2, sort_keys=True))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--profile", choices=("binding", "guard-callback-controls"), default="binding")
    parser.add_argument("--binary", required=True, type=Path)
    parser.add_argument("--lab-root", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--cargo-lock", required=True, type=Path)
    parser.add_argument("--source-root", type=Path)
    parser.add_argument("--source-manifest", type=Path)
    parser.add_argument("--native-context", type=Path)
    parser.add_argument("--rocksdb-archive", type=Path)
    parser.add_argument("--zstd-archive", type=Path)
    args = parser.parse_args()
    if args.profile == "guard-callback-controls":
        return guard_callback_controls(args)

    binary = args.binary.resolve(strict=True)
    if args.lab_root.exists():
        raise SystemExit(f"refusing non-fresh lab root: {args.lab_root}")
    args.lab_root.mkdir(parents=True)
    cases = []

    before = args.lab_root / "kill-before-call"
    process = spawn(binary, "write-barrier", before, "before", "before-call")
    try:
        wait_line(process, "BEFORE_CALL")
    finally:
        terminate(process)
    cases.append({"name": "kill-before-call", "pre_resolution": "NOT_CALLED", "mask_after_reopen": mask(binary, before, "before"), "status": "PASS"})
    if cases[-1]["mask_after_reopen"] != "000":
        raise AssertionError("before-call kill created an effect")

    after = args.lab_root / "kill-after-ok"
    process = spawn(binary, "write-barrier", after, "after", "after-ok")
    try:
        wait_line(process, "AFTER_OK_UNKNOWN")
    finally:
        terminate(process)
    cases.append({"name": "kill-after-ok-before-reply", "pre_resolution": "UNKNOWN", "mask_after_reopen": mask(binary, after, "after"), "status": "PASS"})
    if cases[-1]["mask_after_reopen"] != "111":
        raise AssertionError("write returned OK but was absent after reopen")

    loop_results = []
    for index, delay in enumerate((0.0, 0.002, 0.01, 0.03, 0.1)):
        db = args.lab_root / f"ambiguous-{index}"
        prefix = f"loop-{index}-"
        process = spawn(binary, "write-loop", db, prefix)
        try:
            wait_line(process, "LOOP_READY")
            time.sleep(delay)
        finally:
            terminate(process)
        scanned = run(binary, "scan", db, prefix).stdout.splitlines()
        observed_tokens = []
        for row in scanned:
            fields = row.split("\t")
            if len(fields) != 5:
                raise AssertionError(f"ambiguous loop returned invalid row: {fields!r}")
            token, observed, *actual = fields
            if observed != "111":
                raise AssertionError(f"ambiguous loop found partial token {token}: {observed}")
            expected_values = [
                f"sequence:{token}",
                f"fact:{token}",
                f"receipt:{token}",
            ]
            if actual != expected_values:
                raise AssertionError(
                    f"ambiguous loop token {token} values {actual!r} != {expected_values!r}"
                )
            observed_tokens.append(token)
        expected_tokens = [f"{prefix}{sequence:016}" for sequence in range(len(scanned))]
        if observed_tokens != expected_tokens:
            raise AssertionError(
                f"ambiguous loop sequence has a hole or unexpected token: {observed_tokens!r}"
            )
        loop_results.append({"delay_seconds": delay, "resolved_present_batches": len(scanned)})
    if sum(row["resolved_present_batches"] for row in loop_results) == 0:
        raise AssertionError("ambiguous loops observed no completed batch")
    cases.append({"name": "kill-during-synced-write-loop", "pre_resolution": "UNKNOWN", "trials": loop_results, "status": "PASS"})

    snapshot = run(binary, "snapshot", args.lab_root / "snapshot").stdout.strip()
    if snapshot != "SNAPSHOT_BEFORE_AFTER_REOPEN_OK":
        raise AssertionError(snapshot)
    cases.append({"name": "snapshot-before-after-reopen", "observed": snapshot, "status": "PASS"})

    lock_db = args.lab_root / "exclusive-lock"
    holder = spawn(binary, "hold", lock_db)
    alias = args.lab_root / "exclusive-lock-alias"
    try:
        wait_line(holder, "LOCK_HELD")
        alias.symlink_to(lock_db.resolve(), target_is_directory=True)
        real_refusal = run(binary, "validate", lock_db, expect=2)
        alias_refusal = run(binary, "validate", alias, expect=2)
        if "LOCK" not in real_refusal.stderr.upper() or "LOCK" not in alias_refusal.stderr.upper():
            raise AssertionError("second process failed without reporting the RocksDB lock")
    finally:
        terminate(holder)
    same_process = run(binary, "alias-control", lock_db, alias).stdout.strip()
    if same_process != "CANONICAL_ALIAS_REFUSED":
        raise AssertionError(same_process)
    cases.append({"name": "exclusive-writer-real-and-alias", "cross_process_real": "REFUSED", "cross_process_alias": "REFUSED", "same_process_alias": "REFUSED_BY_CANONICAL_REGISTRY", "status": "PASS"})

    for layout in ("missing", "unknown"):
        db = args.lab_root / f"schema-{layout}"
        run(binary, "seed-schema", db, layout)
        refused = run(binary, "validate", db, expect=2)
        if "COLUMN FAMILIES" not in refused.stderr.upper():
            raise AssertionError(f"{layout} schema was not rejected for exact CF mismatch")
        cases.append({"name": f"{layout}-column-family", "observed": "REFUSED", "status": "PASS"})

    missing_current = args.lab_root / "schema-missing-current"
    run(binary, "seed-schema", missing_current, "expected")
    (missing_current / "CURRENT").unlink()
    refused = run(binary, "validate", missing_current, expect=2)
    if "WITHOUT CURRENT" not in refused.stderr.upper():
        raise AssertionError("missing CURRENT was not rejected at the explicit reopen boundary")
    if (missing_current / "CURRENT").exists():
        raise AssertionError("failed reopen recreated CURRENT")
    cases.append({"name": "missing-current", "observed": "REFUSED_WITHOUT_RECREATE", "status": "PASS"})

    compression_db = args.lab_root / "compression"
    compression = run(binary, "compression", compression_db).stdout.strip()
    if not compression.startswith("ZSTD_SST_OK\t"):
        raise AssertionError(compression)
    cases.append({"name": "explicit-caps-zstd-flush", "observed": compression, "status": "PASS"})
    native_log = inspect_native_log(compression_db / "LOG")

    identity = run(binary, "identity").stdout.strip().splitlines()
    report = {
        "format": "bigbrotr-native-rocks-binding-experiment/1",
        "scope": "Actual rust-rocksdb binding behavior only; isolated research, not product storage/runtime authority.",
        "result": "PASS",
        "binding_identity": identity,
        "native_log_observation": native_log,
        "artifact_identity": {
            "binary_sha256": file_sha256(binary),
            "cargo_lock_sha256": file_sha256(args.cargo_lock),
        },
        "host": {
            "platform": platform.platform(),
            "machine": platform.machine(),
            "python": sys.version,
            "rustc": command_text("rustc", "-vV"),
            "cxx": command_text("c++", "--version"),
            "clang": command_text("clang", "--version"),
            "libclang_path": os.environ.get("LIBCLANG_PATH"),
        },
        "cases": cases,
        "not_run": [
            "system power loss or firmware-cache loss",
            "dedicated-filesystem ENOSPC/fsync failure injection",
            "multi-host fencing",
            "same-process bind-mount or device/inode alias enforcement",
            "P02 admission semantics, runtime grants, receipts, or recovery authority",
        ],
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    print(json.dumps(report, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
