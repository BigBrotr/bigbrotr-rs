#!/usr/bin/env python3
"""Process/lifecycle oracle for the native Rust RocksDB binding experiment."""

import argparse
import functools
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import selectors
import signal
import shutil
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


POSIX_CASES = ("full_guard", "retry0_only", "promotion_only", "recovery_veto_only")
POSIX_NATIVE_SOURCE = "d3dc8d5e781fee3298a0cbbcea5f4a2e2061c6dd"
POSIX_VENDOR_README_SHA256 = (
    "7b9dd6d588d37761afe494e80516b5d8ac54e3dc458b847daef6e2c319726382",
    "2c8c27b20f11738759c33b1bdd8d378af765be5d96c0cff0211d99c59b6a0f4b",
)
POSIX_IDENTITY = (
    "GUARD_REAL_POSIX_PROFILE\tcplusplus=202002\tNDEBUG=1\tstatus_checked=0\tnamespace=rocksdb"
    "\tmount_bytes=134217728\tmin_free_bytes=67108864\tmax_input_bytes=16777216"
    "\twrite_buffer_bytes=262144\tkeys=1024\tvalue_bytes=1024\ttrigger=2"
    "\trecords=1024\tfiles_per_job=8\twitness_seconds=25"
)
POSIX_FATAL_RESUME = "DB resume requested but failed due to Fatal/Unrecoverable error"
POSIX_KINDS = set("""identity mount_before opened accepted_generation accepted_kick
    flush_begin flush_end compaction_begin compaction_end table_start table_created
    table_deleted healthy_complete data_checked fault_gate submission_stopped
    admitted_inputs mount_prefill filler_enospc mount_full background_error
    recovery_begin io_error recovery_end native_log fatal_resume sfm_space_wait mount_restored
    terminal_query failure gate_timeout close_begin close_return destroyed final job_pressure""".split())


@functools.lru_cache(maxsize=2)
def posix_data_digest(generation, kick):
    """Independent literal-data oracle; unsigned xorshift32 and FNV-1a64."""
    digest = 14695981039346656037
    for index in range(1025):
        if index == 1024:
            key, value = b"kick", kick.encode("ascii")
        else:
            key = f"key-{index:08}".encode("ascii")
            state = ((generation * 0x9E3779B9) & 0xFFFFFFFF) ^ (index + 1)
            value = bytearray()
            for _ in range(1024):
                state ^= (state << 13) & 0xFFFFFFFF
                state ^= state >> 17
                state ^= (state << 5) & 0xFFFFFFFF
                value.append(state & 255)
        for byte in key + value:
            digest = ((digest ^ byte) * 1099511628211) & 0xFFFFFFFFFFFFFFFF
    return digest


def validate_posix_events(name, events):
    """Require positive, attributable witnesses; never infer zero SFM calls."""
    def check(condition, message):
        if not condition:
            raise AssertionError(f"{name}: {message}")

    check(name in POSIX_CASES and 0 < len(events) <= 1024, "wrong/empty/overflowed case")
    for index, event in enumerate(events):
        check(event["seq"] == index and event["kind"] in POSIX_KINDS and event["tid"] > 0,
              "invalid event inventory/order/thread")
        check(len(event["inputs"]) <= 8 and len(event["outputs"]) <= 8,
              "file list overflow")
    def rows(kind):
        return [e for e in events if e["kind"] == kind]
    def one(kind):
        found = rows(kind)
        check(len(found) == 1, f"expected exactly one {kind}")
        return found[0]
    def okay(status):
        return status is not None and status == {"code": 0, "subcode": 0, "severity": 0, "message": None}
    def nospace(status, severity=None):
        return (status is not None and status["code"] == 5 and status["subcode"] == 4
                and isinstance(status["message"], str) and len(status["message"]) > 0
                and (severity is None or status["severity"] == severity))
    def nontrivial(event):
        return (event["reason"] == 1 and event["manual"] == 0 and event["remote"] == 0
                and event["base_level"] == 0 and event["output_level"] == 1
                and len(event["inputs"]) >= 2 and event["trivial_files"] == 0)
    def successful_output(event):
        return (nontrivial(event) and okay(event["before"]) and event["input_records"] > 0
                and event["output_records"] > 0 and event["output_bytes"] > 0
                and len(event["outputs"]) > 0)

    check(not rows("failure") and not rows("gate_timeout"), "frontend failure/timeout")
    final = one("final")
    check(final is events[-1] and final["flag_after"] == 1 and final["latch_after"] == 0,
          "missing success or observation overflow/truncation")
    identity = one("identity")
    check(identity["bytes"] == 0, "retry count changed")
    pointers = re.fullmatch(r"installed=(0x[0-9a-f]+) observer=(0x[0-9a-f]+) env=(0x[0-9a-f]+) "
                            r"uid=(\d+) gid=(\d+) dev=(\d+) inode=(\d+)", identity["text"])
    check(pointers is not None and pointers[1] != pointers[2]
          and all(int(pointers[i], 16) > 0 for i in (1, 2, 3)), "installed/observer identity missing")
    filler_path = Path(identity["path"])
    check(filler_path.is_absolute() and filler_path.name == "owned-filler", "filler ownership path missing")
    check(okay(one("opened")["before"]), "open failed")
    generations = rows("accepted_generation")
    check([e["bytes"] for e in generations] == [1, 2, 3, 4]
          and all(e["extra"] == 1024 and okay(e["before"]) for e in generations),
          "fixed synced input generations missing")
    kicks = rows("accepted_kick")
    check([e["text"] for e in kicks] == ["healthy", "fault"]
          and all(okay(e["before"]) for e in kicks), "fixed synced kick inputs missing")
    healthy = one("healthy_complete")
    healthy_jobs = [e for e in rows("compaction_end") if e["seq"] < healthy["seq"]]
    check(len(healthy_jobs) == 1 and successful_output(healthy_jobs[0]),
          "healthy automatic nontrivial output missing")
    healthy_job = healthy_jobs[0]
    healthy_tables = [e for e in rows("table_created") if e["job"] == healthy_job["job"]]
    check(any(okay(e["before"]) and e["bytes"] > 0 for e in healthy_tables),
          "healthy compaction has no actual SST creation")
    gate = one("fault_gate")
    begin = [e for e in rows("compaction_begin") if e["job"] == gate["job"]]
    check(len(begin) == 1 and nontrivial(begin[0]) and begin[0]["phase"] == 1,
          "fault job is not the selected automatic nontrivial compaction")
    begin = begin[0]
    check(len(begin["inputs"]) >= 3 and set(healthy_job["outputs"]) & set(begin["inputs"]),
          "fault inputs do not overlap the healthy SST")
    stopped = one("submission_stopped")
    prefill = one("mount_prefill")
    full = one("mount_full")
    restored = one("mount_restored")
    check(healthy["seq"] < begin["seq"] < gate["seq"] < stopped["seq"] < prefill["seq"]
          < full["seq"] < restored["seq"], "wrong preparation/fill/restore order")
    # A submitted kick can return after the background callback enters its
    # gate. No new foreground submission follows it; before filling it returned.
    check(all(e["seq"] < stopped["seq"] for e in generations + kicks),
          "workload continued after arming")
    flushes = [e for e in rows("flush_end") if e["seq"] < prefill["seq"]]
    check(len(flushes) == 4 and all(e["reason"] == 6 for e in flushes)
          and sum(e["seq"] < healthy["seq"] for e in flushes) == 2,
          "four completed automatic write-buffer flushes required")
    check(any(e["phase"] == 1 and flushes[-1]["seq"] < e["seq"] < stopped["seq"]
              and e["flush_scheduled"] == 0 and e["flush_running"] == 0
              and e["compaction_scheduled"] == 1 for e in rows("job_pressure")),
          "final flush tail did not publish its completed lifecycle snapshot")
    data = rows("data_checked")
    check(len(data) == 3 and [e["input_bytes"] for e in data] == [2, 4, 4]
          and all(e["bytes"] == 1025 for e in data), "accepted scientific data census missing")
    check(data[0]["extra"] == posix_data_digest(2, "healthy")
          and all(e["extra"] == posix_data_digest(4, "fault") for e in data[1:])
          and data[0]["seq"] < healthy["seq"] and stopped["seq"] < data[1]["seq"] < prefill["seq"]
          and data[2]["seq"] > restored["seq"], "accepted data bytes or phase differ")
    admitted = one("admitted_inputs")
    check(admitted["job"] == gate["job"] and 0 < admitted["bytes"] <= 16 * 1024**2
          and admitted["extra"] == 2 * admitted["bytes"] + 262144, "input reservation/headroom differs")
    for event in (one("mount_before"), prefill, full, restored):
        check(event["extra"] == 128 * 1024**2 and event["output_bytes"] == 4096,
              "mount capacity/page size changed")
    check(min(one("mount_before")["bytes"], prefill["bytes"], restored["bytes"]) >= 64 * 1024**2
          and restored["bytes"] >= admitted["extra"] and full["bytes"] == 0,
          "missing sufficient actual headroom or genuine exhaustion")
    filler = one("filler_enospc")
    check(filler["operation"] == 28 and filler["bytes"] > 0 and filler["extra"] > 0
          and filler["extra"] <= 128 * 1024**2 and filler["seq"] < full["seq"],
          "filler raw ENOSPC/allocation witness missing")

    errors = rows("io_error")
    check(len(errors) > 0, "no actual IOStatus")
    first = errors[0]
    check(nospace(first["before"]) and first["retryable"] == 1 and first["data_loss"] == 0
          and first["scope"] == 0 and first["operation"] == 8 and first["bytes"] > 0,
          "first actual I/O failure is not retryable POSIX SST Append NoSpace")
    check(first["tid"] == begin["tid"] and first["path"].endswith(".sst")
          and Path(first["path"]).parent == filler_path.parent / "db"
          and full["seq"] < first["seq"] < restored["seq"], "I/O failure attribution differs")
    attempts = [e for e in rows("table_start") if e["job"] == gate["job"]
                and e["path"] == first["path"] and gate["seq"] < e["seq"] < first["seq"]]
    check(len(attempts) == 1 and attempts[0]["reason"] == 1, "not a compaction SST output attempt")
    # Refuse a preceding flush/WAL/MANIFEST/open error, even if a later target
    # callback also matches. Logger text is diagnostic, never injected status.
    prior_errors = [e for e in events if e["seq"] < first["seq"] and e["before"] is not None
                    and e["before"]["code"] != 0]
    check(not prior_errors, "another error preceded the selected SST failure")
    failed = [e for e in rows("compaction_end") if e["job"] == gate["job"]]
    check(len(failed) == 1 and nospace(failed[0]["before"]), "target compaction did not fail NoSpace")
    bg = rows("background_error")[0] if rows("background_error") else None
    recovery = rows("recovery_begin")[0] if rows("recovery_begin") else None
    check(bg is not None and recovery is not None, "real error callbacks missing")
    check(bg["reason"] == 1 and recovery["reason"] == 1 and bg["tid"] == begin["tid"]
          and recovery["tid"] == bg["tid"]
          and first["seq"] < failed[0]["seq"] < bg["seq"] < recovery["seq"] < restored["seq"],
          "wrong background reason/thread/callback/restore order")
    check(nospace(bg["before"], 1) and bg["before"]["message"] == first["before"]["message"],
          "real compaction NoSpace did not enter soft with original I/O identity")
    promote = name in ("full_guard", "promotion_only")
    veto = name in ("full_guard", "recovery_veto_only")
    expected = dict(bg["before"], severity=3 if promote else 1)
    check(bg["after"] == expected and bg["latch_before"] == 0 and bg["latch_after"] == int(promote),
          "actual promotion/latch or preserved status fields differ")
    check(recovery["before"] == expected and recovery["after"] == expected
          and recovery["flag_before"] == 1 and recovery["flag_after"] == int(not veto)
          and recovery["latch_before"] == int(promote)
          and recovery["latch_after"] == int(promote or veto), "actual recovery veto/latch differs")
    terminal = one("terminal_query")
    check(terminal["seq"] > restored["seq"] and terminal["latch_after"] == int(name != "retry0_only"),
          "terminal sticky query differs")
    witness_jobs = []
    if name in ("retry0_only", "recovery_veto_only"):
        for candidate in rows("compaction_end"):
            starts = [e for e in rows("compaction_begin") if e["job"] == candidate["job"]]
            tables = [e for e in rows("table_created") if e["job"] == candidate["job"]]
            if (candidate["job"] != gate["job"] and candidate["seq"] > restored["seq"]
                    and len(starts) == 1 and starts[0]["seq"] > recovery["seq"]
                    and set(begin["inputs"]).issubset(starts[0]["inputs"])
                    and successful_output(candidate)
                    and any(okay(e["before"]) and e["bytes"] > 0 for e in tables)):
                witness_jobs.append(candidate["job"])
        check(witness_jobs, "no new automatic job with actual output on the failed input set")
    if name == "retry0_only":
        check(any("Cannot clear soft error" in e["text"] and recovery["seq"] < e["seq"] < restored["seq"]
                  for e in rows("sfm_space_wait")), "no positive full-filesystem SFM soft-error poll")
        ends = [e for e in rows("recovery_end") if nospace(e["before"], 1) and okay(e["after"])
                and restored["seq"] < e["seq"] < terminal["seq"]]
        check(ends and okay(terminal["before"]), "no positive automatic recovery End(old NoSpace,new OK)")
    elif name == "recovery_veto_only":
        check(nospace(terminal["before"], 1), "veto-only did not retain the soft native error")
    else:
        check(nospace(terminal["before"], 3), "fatal native error was not retained")
        if name == "promotion_only":
            check(any(POSIX_FATAL_RESUME in e["text"] and recovery["seq"] < e["seq"] < terminal["seq"]
                      for e in rows("fatal_resume")), "native fatal-resume invocation witness missing")
    close_begin, close_return, destroyed = one("close_begin"), one("close_return"), one("destroyed")
    check(terminal["seq"] < data[2]["seq"] < close_begin["seq"] < close_return["seq"]
          < destroyed["seq"] < final["seq"] and okay(close_return["before"])
          and destroyed["latch_after"] == int(name != "retry0_only"),
          "actual close/destruction and retained query handle missing")
    return {"fault_job": gate["job"], "fault_thread": begin["tid"],
            "progress_jobs": witness_jobs, "event_count": len(events),
            "input_sst_bytes": admitted["bytes"], "restored_free_bytes": restored["bytes"],
            "target_raw_errno": "INFERRED_FROM_DEFAULT_POSIX_SOURCE_NOT_SYSCALL_TRACED",
            "full_guard_sfm_exclusion": "SOURCE_BACKED_NOT_A_MEASURED_INVOCATION_COUNT",
            "accepted_logical_fnv1a64": data[-1]["extra"], "actual_close_and_destruction": True}


def posix_mount_identity(root, evidence):
    root = root.resolve(strict=True)
    evidence = evidence.resolve(strict=True)
    if root == evidence or root in evidence.parents or evidence in root.parents:
        raise AssertionError("tmpfs and evidence roots must be disjoint")
    if root.stat().st_dev == evidence.stat().st_dev or any(root.iterdir()):
        raise AssertionError("require a fresh dedicated tmpfs and evidence on another filesystem")
    matches = []
    for line in Path("/proc/self/mountinfo").read_text().splitlines():
        before, after = line.split(" - ", 1)
        fields, fs = before.split(), after.split()
        # Escaped/ambiguous paths are refused for this bounded fixture.
        if fields[4] == str(root):
            matches.append({"mount_id": fields[0], "parent_id": fields[1], "device": fields[2],
                            "mountpoint": fields[4], "mount_options": fields[5],
                            "fs_type": fs[0], "source": fs[1], "super_options": fs[2]})
    stats = os.statvfs(root)
    if (len(matches) != 1 or matches[0]["fs_type"] != "tmpfs" or stats.f_frsize != 4096
            or stats.f_blocks * stats.f_frsize != 128 * 1024**2):
        raise AssertionError("require an exact 128 MiB Linux tmpfs mountpoint")
    return matches[0] | {"capacity_bytes": stats.f_blocks * stats.f_frsize,
                         "available_bytes": stats.f_bavail * stats.f_frsize,
                         "uid": root.stat().st_uid, "gid": root.stat().st_gid}


def preserve_posix_db(case_root, destination):
    """Copy bounded regular DB files only, after the child has been reaped."""
    destination.mkdir()
    records, total = [], 0
    db_root = case_root / "db"
    if not db_root.exists():
        return records
    for path in sorted(db_root.iterdir()):
        if path.is_symlink() or not path.is_file() or path.stat().st_nlink != 1:
            raise AssertionError("unexpected non-regular/aliased DB preservation input")
        info = path.stat()
        total += info.st_size
        if len(records) >= 128 or total > 64 * 1024**2:
            raise AssertionError("DB preservation exceeds frozen file/byte bound")
        copied = destination / path.name
        shutil.copyfile(path, copied)
        digest = file_sha256(path)
        if file_sha256(copied) != digest:
            raise AssertionError("retained DB copy differs")
        records.append({"name": path.name, "bytes": info.st_size, "allocated_bytes": info.st_blocks * 512,
                        "mode": info.st_mode, "sha256": digest})
    return records


def posix_archive_source_deltas(archive_files, current_files):
    """Reconcile only this test increment and the exact e871 evidence doc delta."""
    readme = "vendor/README.md"
    if (archive_files.get(readme), current_files.get(readme)) != POSIX_VENDOR_README_SHA256:
        raise AssertionError("vendor/README.md differs from the reviewed d3/e871 documentation pair")
    allowed_changes = {"experiments/native-rocks-binding/driver.py",
                       "experiments/native-rocks-binding/README.md", readme}
    changes = []
    for name, digest in archive_files.items():
        current_digest = current_files.get(name)
        if current_digest != digest:
            if name not in allowed_changes or current_digest is None:
                raise AssertionError(f"archive source changed outside the bounded test increment: {name}")
            changes.append({"path": name, "archive_file_sha256": digest,
                            "current_file_sha256": current_digest})
    return changes


def guard_real_posix(args):
    required = ("source_root", "source_manifest", "archive_source_manifest", "native_context",
                "rocksdb_archive", "zstd_archive", "posix_root")
    if any(getattr(args, name) is None for name in required):
        raise SystemExit("guard-real-posix requires both source manifests, native context/archives and --posix-root")
    source = args.source_root.resolve(strict=True)
    paths = {name: getattr(args, name) for name in
             ("binary", "cargo_lock", "source_manifest", "archive_source_manifest",
              "native_context", "rocksdb_archive", "zstd_archive")}
    for name, path in paths.items():
        if path.is_symlink() or not path.is_file():
            raise AssertionError(f"{name} must be a regular non-symlink input")
        paths[name] = path.resolve(strict=True)
    manifest = json.loads(paths["source_manifest"].read_text())
    archive_manifest = json.loads(paths["archive_source_manifest"].read_text())
    context = json.loads(paths["native_context"].read_text())
    if (manifest["format"] != "bigbrotr-source/1" or archive_manifest["format"] != "bigbrotr-source/1"
            or context["format"] != "bigbrotr-native-c-api-archive-context/1"
            or context["status"] != "ARCHIVES_CAPTURED_C_TEST_NOT_RUN"
            or archive_manifest["revision"] != POSIX_NATIVE_SOURCE or context["source"] != POSIX_NATIVE_SOURCE
            or context["tree"] != archive_manifest["tree"] or context["profile"] != "release"
            or context["target"] != "x86_64-unknown-linux-gnu"
            or context["source_manifest_sha256"] != file_sha256(paths["archive_source_manifest"])
            or paths["cargo_lock"] != source / "Cargo.lock"
            or context["cargo_lock_sha256"] != file_sha256(paths["cargo_lock"])):
        raise AssertionError("wrong selected frontend/native archive source/profile/lock binding")
    source_deltas = posix_archive_source_deltas(archive_manifest["files_sha256"], manifest["files_sha256"])
    source_names = tuple("experiments/native-rocks-binding/" + name for name in
                         ("driver.py", "background_guard_real_posix_test.cc")) + (
        "Cargo.lock", "vendor/README.md", "vendor/librocksdb-sys.provenance.json", "vendor/librocksdb-sys/build.rs",
        "vendor/librocksdb-sys/rocksdb/db/c_options_internal.h", "vendor/librocksdb-sys/rocksdb/db/c.cc",
        "vendor/librocksdb-sys/rocksdb/db/error_handler.cc", "vendor/librocksdb-sys/rocksdb/db/error_handler.h",
        "vendor/librocksdb-sys/rocksdb/db/event_helpers.cc", "vendor/librocksdb-sys/rocksdb/file/sst_file_manager_impl.cc",
        "vendor/librocksdb-sys/rocksdb/file/sst_file_manager_impl.h", "vendor/librocksdb-sys/rocksdb/env/io_posix.cc",
        "vendor/librocksdb-sys/rocksdb/db/db_impl/db_impl.cc", "vendor/librocksdb-sys/rocksdb/db/db_impl/db_impl_open.cc",
        "vendor/librocksdb-sys/rocksdb/db/db_impl/db_impl_compaction_flush.cc",
        "vendor/librocksdb-sys/rocksdb/file/writable_file_writer.cc", "vendor/librocksdb-sys/rocksdb/file/writable_file_writer.h",
        "vendor/librocksdb-sys/rocksdb/include/rocksdb/listener.h", "vendor/librocksdb-sys/rocksdb/include/rocksdb/status.h",
        "vendor/librocksdb-sys/rocksdb/include/rocksdb/io_status.h", "vendor/librocksdb-sys/rocksdb/include/rocksdb/options.h",
    )
    for name in source_names:
        path = source / name
        if path.is_symlink() or file_sha256(path) != manifest["files_sha256"][name]:
            raise AssertionError(f"current source differs from manifest: {name}")
        paths[name] = path
    if file_sha256(Path(__file__)) != file_sha256(source / "experiments/native-rocks-binding/driver.py"):
        raise AssertionError("executing driver differs from selected source")
    expected_archives = {
        "librocksdb-sys": ("rocksdb_archive", "0.19.0+11.8.1", ["bindgen-runtime", "static", "zstd", "zstd-sys"]),
        "zstd-sys": ("zstd_archive", "2.1.2+zstd.1.5.7", ["legacy", "zdict_builder"]),
    }
    if len(context["archives"]) != 2 or {e["package"] for e in context["archives"]} != set(expected_archives):
        raise AssertionError("exactly the selected two archives are required")
    for item in context["archives"]:
        key, version, features = expected_archives[item["package"]]
        archive = paths[key]
        with archive.open("rb") as stream:
            magic = stream.read(8)
        local = item["package"] == "librocksdb-sys"
        package_ok = ((item["package_id"].startswith("path+file://")
                       and item["package_id"].endswith("/vendor/librocksdb-sys#0.19.0+11.8.1")
                       and Path(item["manifest_path"]).parts[-3:] == ("vendor", "librocksdb-sys", "Cargo.toml"))
                      if local else item["package_id"] ==
                      "registry+https://github.com/rust-lang/crates.io-index#zstd-sys@2.1.2+zstd.1.5.7")
        if (not package_ok or item["version"] != version or item["features"] != features
                or magic != b"!<arch>\n" or item["bytes"] != archive.stat().st_size
                or item["sha256"] != file_sha256(archive)):
            raise AssertionError("selected archive/package/features/bytes differ")
    if any(path.exists() or path.is_symlink() for path in (args.lab_root, args.output)):
        raise AssertionError("fresh outside-mount lab/report required")
    if args.posix_root.is_symlink():
        raise AssertionError("tmpfs root must be the actual mountpoint")
    args.posix_root = args.posix_root.resolve(strict=True)
    args.lab_root.mkdir(parents=True)
    mount = posix_mount_identity(args.posix_root, args.lab_root)
    hashes = {name: file_sha256(path) for name, path in paths.items()}
    report = {"format": "bigbrotr-native-guard-real-posix/1", "result": "RUNNING",
              "source": manifest["revision"], "tree": manifest["tree"],
              "native_archive_source": POSIX_NATIVE_SOURCE, "artifact_identity": hashes,
              "archive_source_deltas": source_deltas,
              "native_toolchain": context["native_toolchain"], "compiler": context["compiler"],
              "mount": mount, "expected_case_count": 4, "case_timeout_seconds": TIMEOUT_SECONDS,
              "cases": [], "scope": "Linux tmpfs default-POSIX automatic nontrivial compaction component path",
              "container_exit_oom_custody": "ROOT_MUST_RECORD_AFTER_CONTAINER_EXIT",
              "not_run": ["application guard/dispatch/ACK/funding wiring", "OPEN65/CW-N1/physical coefficients",
                          "persistent-media crash/fsync/ext4/XFS behavior", "concurrent query/drop/close",
                          "all other error producers", "measured exact SFM/Resume invocation counts",
                          "forced nonzero partial SST/failed unlink/purge-credit qualification"]}
    def write_report():
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    def unchanged():
        if any(file_sha256(path) != hashes[name] for name, path in paths.items()):
            raise AssertionError("frontend/source/lock/context/archive input changed")
    write_report()
    active = "identity-and-inventory"
    try:
        identity = run(paths["binary"], "--identity")
        inventory = run(paths["binary"], "--list")
        if identity.stdout.splitlines() != [POSIX_IDENTITY] or identity.stderr:
            raise AssertionError("wrong frozen frontend ABI/fixture identity")
        if inventory.stdout.splitlines() != [f"{name}\t0" for name in POSIX_CASES] or inventory.stderr:
            raise AssertionError("wrong nonempty four-case inventory")
        report["frontend_profile"] = identity.stdout.strip()
        for name in POSIX_CASES:
            active = name
            unchanged()
            case_root = args.posix_root / name
            case_root.mkdir(mode=0o700)
            evidence = args.lab_root / name
            evidence.mkdir()
            record = {"name": name, "status": "RUNNING", "db_root": str(case_root / "db")}
            report["cases"].append(record)
            write_report()
            process = None
            try:
                with (evidence / "events.jsonl").open("xb") as stdout, (evidence / "stderr.txt").open("xb") as stderr:
                    process = subprocess.Popen([str(paths["binary"]), "--case", name, "--root", str(case_root)],
                                               stdout=stdout, stderr=stderr, start_new_session=True)
                    try:
                        record["exit_code"] = process.wait(timeout=TIMEOUT_SECONDS)
                    except subprocess.TimeoutExpired:
                        terminate(process)
                        record["exit_code"] = process.returncode
                        raise AssertionError("30-second case deadline expired; no absence/recovery conclusion")
                check_stdout = evidence / "events.jsonl"
                if check_stdout.stat().st_size > 32 * 1024**2 or (evidence / "stderr.txt").stat().st_size > 65536:
                    raise AssertionError("bounded frontend evidence overflow")
                events = [json.loads(line) for line in check_stdout.read_text().splitlines()]
                if record["exit_code"] != 0 or (evidence / "stderr.txt").stat().st_size:
                    raise AssertionError(f"frontend failed/terminated: exit={record['exit_code']}")
                record["witnesses"] = validate_posix_events(name, events)
                record["status"] = "PASS"
            finally:
                if process is not None:
                    terminate(process)
                    record["reaped"] = process.poll() is not None
                record["retained_db_files"] = preserve_posix_db(case_root, evidence / "retained-db")
                record["events_sha256"] = file_sha256(evidence / "events.jsonl")
                write_report()
            unchanged()
        if len(report["cases"]) != 4 or any(e["status"] != "PASS" or not e["reaped"] for e in report["cases"]):
            raise AssertionError("all four reaped positive cases required")
        report["result"] = "PASS"
        write_report()
    except BaseException as error:
        report["result"] = "FAIL"
        report["failure"] = {"case": active, "kind": type(error).__name__, "detail": str(error)}
        if report["cases"] and report["cases"][-1]["status"] == "RUNNING":
            report["cases"][-1]["status"] = "FAIL"
        write_report()
        raise
    print(json.dumps(report, indent=2, sort_keys=True))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--profile", choices=("binding", "guard-callback-controls", "guard-real-posix"), default="binding")
    parser.add_argument("--binary", required=True, type=Path)
    parser.add_argument("--lab-root", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--cargo-lock", required=True, type=Path)
    parser.add_argument("--source-root", type=Path)
    parser.add_argument("--source-manifest", type=Path)
    parser.add_argument("--native-context", type=Path)
    parser.add_argument("--rocksdb-archive", type=Path)
    parser.add_argument("--zstd-archive", type=Path)
    parser.add_argument("--archive-source-manifest", type=Path)
    parser.add_argument("--posix-root", type=Path)
    args = parser.parse_args()
    if args.profile == "guard-callback-controls":
        return guard_callback_controls(args)
    if args.profile == "guard-real-posix":
        return guard_real_posix(args)

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
