#!/usr/bin/env python3
# Copyright 2026 BigBrotr contributors. MIT license in the repository root.
"""Pure event-oracle controls. These do not execute or simulate RocksDB."""
import importlib.util
from pathlib import Path
import unittest

SPEC = importlib.util.spec_from_file_location("binding_driver", Path(__file__).with_name("driver.py"))
DRIVER = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(DRIVER)


def status(severity=None):
    if severity is None:
        return {"code": 0, "subcode": 0, "severity": 0, "message": None}
    return {"code": 5, "subcode": 4, "severity": severity,
            "message": "While appending to file: /tmpfs/case/db/000099.sst: No space left on device"}


def fixture(name, recovery_first=False):
    events = []
    phase = 0
    def emit(kind, **fields):
        event = dict(seq=len(events), kind=kind, tid=111, phase=phase,
                     job=-1, reason=-1, operation=-1, flag_before=0, flag_after=0,
                     latch_before=0, latch_after=0, retryable=0, data_loss=0, scope=-1,
                     manual=0, remote=0, base_level=-1, output_level=-1, bytes=0, extra=0,
                     input_records=0, output_records=0, input_bytes=0, output_bytes=0,
                     trivial_files=0, path="", text="", before=None, after=None, inputs=[], outputs=[])
        event.update(fields)
        events.append(event)
        return event
    def mount(kind, free):
        emit(kind, bytes=free, extra=134217728, output_bytes=4096)
    def compaction(kind, job, inputs, outputs=(), error=None):
        return emit(kind, tid=222, job=job, reason=1, base_level=0, output_level=1,
                    inputs=list(inputs), outputs=list(outputs), before=status(error),
                    input_records=2048 if kind == "compaction_end" else 0,
                    output_records=1024 if outputs else 0, output_bytes=1048576 if outputs else 0)
    def table(job, path, error=None):
        emit("table_created", job=job, path=path, reason=1, before=status(error),
             bytes=1048576 if error is None else 0)
    def data(generation, kick):
        emit("data_checked", bytes=1025, input_bytes=generation,
             extra=DRIVER.posix_data_digest(generation, kick))
    emit("identity", bytes=0, path="/tmpfs/case/owned-filler",
         text="installed=0x1 observer=0x2 env=0x3 uid=1000 gid=1000 dev=4 inode=5")
    mount("mount_before", 120 * 1024**2)
    emit("opened", before=status())
    for generation in (1, 2):
        emit("accepted_generation", bytes=generation, extra=1024, before=status())
    emit("flush_end", reason=6)
    emit("accepted_kick", text="healthy", before=status())
    emit("flush_end", reason=6)
    compaction("compaction_begin", 12, (90, 91))
    table(12, "/tmpfs/case/db/000100.sst")
    compaction("compaction_end", 12, (90, 91), (100,))
    data(2, "healthy")
    emit("healthy_complete")
    phase = 1
    for generation in (3, 4):
        emit("accepted_generation", bytes=generation, extra=1024, before=status())
    emit("flush_end", reason=6)
    emit("accepted_kick", text="fault", before=status())
    emit("flush_end", reason=6)
    compaction("compaction_begin", 22, (100, 101, 102))
    emit("fault_gate", job=22)
    emit("job_pressure", flush_scheduled=0, flush_running=0, compaction_scheduled=1, compaction_running=1)
    emit("submission_stopped")
    data(4, "fault")
    emit("admitted_inputs", job=22, bytes=3 * 1024**2, extra=6 * 1024**2 + 262144)
    mount("mount_prefill", 116 * 1024**2)
    emit("filler_enospc", bytes=116 * 1024**2, extra=116 * 1024**2, operation=28)
    mount("mount_full", 0)
    target = "/tmpfs/case/db/000099.sst"
    emit("table_start", tid=222, job=22, path=target, reason=1)
    emit("io_error", tid=222, path=target, before=status(0), retryable=1, scope=0, operation=8, bytes=65536)
    table(22, target, 0)
    compaction("compaction_end", 22, (100, 101, 102), error=0)
    promote = name in ("full_guard", "promotion_only")
    veto = name in ("full_guard", "recovery_veto_only")
    severity = 3 if promote else 1
    emit("background_error", tid=222, reason=1, before=status(1), after=status(severity),
         latch_after=int(promote))
    emit("recovery_begin", tid=222, reason=1, before=status(severity), after=status(severity),
         latch_before=int(promote), latch_after=int(promote or veto), flag_before=1, flag_after=int(not veto))
    if name == "retry0_only":
        emit("sfm_space_wait", text="Cannot clear soft error")
    mount("mount_restored", 116 * 1024**2)
    if name == "retry0_only" and recovery_first:
        emit("recovery_end", before=status(1), after=status())
    if name in ("retry0_only", "recovery_veto_only"):
        compaction("compaction_begin", 23, (100, 101, 102))
        table(23, "/tmpfs/case/db/000103.sst")
        compaction("compaction_end", 23, (100, 101, 102), (103,))
    if name == "retry0_only" and not recovery_first:
        emit("recovery_end", before=status(1), after=status())
    if name == "promotion_only":
        emit("fatal_resume", text=DRIVER.POSIX_FATAL_RESUME)
    emit("terminal_query", before=status(None if name == "retry0_only" else severity),
         latch_after=int(name != "retry0_only"))
    data(4, "fault")
    emit("close_begin")
    emit("close_return", before=status())
    emit("destroyed", latch_after=int(name != "retry0_only"))
    emit("final", flag_after=1)
    return events


class PosixOracleControls(unittest.TestCase):
    def reject(self, name, mutate):
        events = fixture(name)
        mutate(events)
        for index, event in enumerate(events):
            event["seq"] = index
        with self.assertRaises(AssertionError):
            DRIVER.validate_posix_events(name, events)

    def test_all_four_positive_witness_sets(self):
        for name in DRIVER.POSIX_CASES:
            with self.subTest(name=name):
                DRIVER.validate_posix_events(name, fixture(name))

    def test_recovery_end_and_progress_have_no_artificial_order(self):
        DRIVER.validate_posix_events("retry0_only", fixture("retry0_only", recovery_first=True))

    def test_no_recovery_end_is_not_success(self):
        self.reject("retry0_only", lambda rows: rows.__setitem__(slice(None),
                    [r for r in rows if r["kind"] != "recovery_end"]))

    def test_no_new_output_is_not_veto_counterexample(self):
        self.reject("recovery_veto_only", lambda rows: next(r for r in rows
                    if r["kind"] == "compaction_end" and r["job"] == 23).update(output_bytes=0))

    def test_promotion_requires_native_resume_witness(self):
        self.reject("promotion_only", lambda rows: rows.__setitem__(slice(None),
                    [r for r in rows if r["kind"] != "fatal_resume"]))

    def test_status_code_equality_cannot_hide_identity_loss(self):
        self.reject("full_guard", lambda rows: next(r for r in rows
                    if r["kind"] == "background_error")["after"].update(message="changed"))

    def test_wrong_error_producer_fails(self):
        self.reject("full_guard", lambda rows: next(r for r in rows
                    if r["kind"] == "background_error").update(reason=0))

    def test_sfm_space_limit_cannot_replace_real_nospace(self):
        self.reject("full_guard", lambda rows: next(r for r in rows
                    if r["kind"] == "io_error")["before"].update(subcode=8))

    def test_unrelated_thread_cannot_supply_io_witness(self):
        self.reject("full_guard", lambda rows: next(r for r in rows
                    if r["kind"] == "io_error").update(tid=999))

    def test_no_restored_headroom_fails(self):
        self.reject("retry0_only", lambda rows: next(r for r in rows
                    if r["kind"] == "mount_restored").update(bytes=0))

    def test_missing_actual_close_fails(self):
        self.reject("full_guard", lambda rows: rows.__setitem__(slice(None),
                    [r for r in rows if r["kind"] != "close_return"]))

    def test_overflow_invalidates_otherwise_positive_events(self):
        self.reject("full_guard", lambda rows: rows[-1].update(latch_after=1))

    def test_data_mutation_fails(self):
        self.reject("full_guard", lambda rows: next(r for r in rows
                    if r["kind"] == "data_checked").update(extra=0))

    def test_flush_callback_without_completed_tail_fails(self):
        self.reject("full_guard", lambda rows: next(r for r in rows
                    if r["kind"] == "job_pressure").update(flush_running=1))

    def test_successful_output_on_unrelated_inputs_is_not_progress(self):
        self.reject("recovery_veto_only", lambda rows: next(r for r in rows
                    if r["kind"] == "compaction_begin" and r["job"] == 23).update(inputs=[999, 998]))


class PosixArchiveSourceControls(unittest.TestCase):
    def manifests(self):
        original, current = DRIVER.POSIX_VENDOR_README_SHA256
        archive = {"vendor/README.md": original,
                   "vendor/librocksdb-sys/rocksdb/db/c.cc": "a" * 64,
                   "vendor/librocksdb-sys/README.md": "b" * 64}
        return archive, dict(archive, **{"vendor/README.md": current})

    def test_exact_vendor_readme_delta_retains_both_hashes(self):
        archive, current = self.manifests()
        self.assertEqual(DRIVER.posix_archive_source_deltas(archive, current), [
            {"path": "vendor/README.md", "archive_file_sha256": archive["vendor/README.md"],
             "current_file_sha256": current["vendor/README.md"]}])

    def test_vendor_readme_exception_does_not_allow_native_code_change(self):
        archive, current = self.manifests()
        current["vendor/librocksdb-sys/rocksdb/db/c.cc"] = "c" * 64
        with self.assertRaisesRegex(AssertionError, "db/c.cc"):
            DRIVER.posix_archive_source_deltas(archive, current)

    def test_no_broad_vendor_documentation_exception(self):
        archive, current = self.manifests()
        current["vendor/librocksdb-sys/README.md"] = "c" * 64
        with self.assertRaisesRegex(AssertionError, "librocksdb-sys/README.md"):
            DRIVER.posix_archive_source_deltas(archive, current)

    def test_unreviewed_vendor_readme_bytes_fail(self):
        archive, current = self.manifests()
        current["vendor/README.md"] = "c" * 64
        with self.assertRaisesRegex(AssertionError, "reviewed d3/e871"):
            DRIVER.posix_archive_source_deltas(archive, current)


if __name__ == "__main__":
    unittest.main()
