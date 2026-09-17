#!/usr/bin/env python3
"""Owned, quiescent Tux lab backup; not a production backup implementation."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import time


def run(*args, **kwargs):
    return subprocess.run(args, check=True, **kwargs)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--root", type=Path, required=True)
    p.add_argument("--run-id", default="archive02")
    args = p.parse_args()
    os.nice(10)
    root = args.root.resolve()
    assert args.run_id.isalnum() and len(args.run_id) <= 24
    assert root.name == "bigbrotr-qualification-20260917"
    assert (root / "results/baselines-and-verification-complete").exists()
    pg = "bigbrotr-qualification-pg-20260917"
    worker = "bigbrotr-qualification-worker-20260917"
    target = root / "backups" / args.run_id
    target.mkdir()  # Refuse accidental replacement of an earlier evidence run.
    result = root / "results" / ("backup-restore-"+args.run_id+".json")
    assert not result.exists()
    report = {"status": "RUNNING", "scope": "quiescent local archive copy and fresh catalog restore; no host loss or product causal-backup claim"}

    def save():
        result.write_text(json.dumps(report, indent=2) + "\n")

    def sql(query, database="qualification"):
        return run("docker", "exec", pg, "psql", "-U", "postgres", "-d", database,
                   "-v", "ON_ERROR_STOP=1", "-At", "-c", query,
                   capture_output=True, text=True).stdout.strip()

    def inventory():
        values = []
        for f in sorted((root / "data").rglob("*")):
            assert not f.is_symlink()
            if f.is_file():
                s = f.stat()
                values.append((str(f.relative_to(root / "data")), s.st_size, s.st_mtime_ns))
        return values

    save()
    try:
        top = run("docker", "top", worker, "-eo", "pid,args", capture_output=True, text=True).stdout
        assert not any(name in top for name in ("engines.py", "verify.py", "recovery.py")), top
        assert sql("SELECT count(*) FROM pg_stat_activity WHERE datname='qualification' AND pid<>pg_backend_pid() AND state<>'idle'") == "0"
        before = inventory()
        size = sum(item[1] for item in before)
        assert shutil.disk_usage(root).free > size + 120 * 1024**3, "Preserve 120 GiB shared filesystem reserve"
        report["source_snapshot"] = sql("SELECT max(snapshot_id) FROM ducklake_snapshot")
        report["source_files"] = len(before)
        report["source_bytes"] = size
        started = time.monotonic()
        with (target / "catalog.dump").open("xb") as out:
            run("docker", "exec", pg, "pg_dump", "-U", "postgres", "-d", "qualification", "-Fc", stdout=out)
        (target / "data").mkdir()
        report["files"] = []
        for name, length, stamp in before:
            src, dst = root / "data" / name, target / "data" / name
            dst.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(src, dst)
            with src.open("rb") as f:
                original = hashlib.file_digest(f, "sha256").hexdigest()
            with dst.open("rb") as f:
                copied = hashlib.file_digest(f, "sha256").hexdigest()
            assert original == copied and dst.stat().st_size == length
            report["files"].append({"path": name, "bytes": length, "sha256": copied})
            if len(report["files"]) % 8 == 0:
                save()
        assert inventory() == before, "Unexpected source-file mutation during backup"
        assert sql("SELECT max(snapshot_id) FROM ducklake_snapshot") == report["source_snapshot"]
        report["copy_and_hash_seconds"] = time.monotonic() - started
        with (target / "catalog.dump").open("rb") as f:
            report["catalog_sha256"] = hashlib.file_digest(f, "sha256").hexdigest()
        save()
        database = "qualification_restore_"+args.run_id
        sql("CREATE DATABASE "+database, "postgres")
        with (target / "catalog.dump").open("rb") as source:
            run("docker", "exec", "-i", pg, "pg_restore", "-U", "postgres", "-d", database, "--exit-on-error", stdin=source)
        run("docker", "exec", worker, "/lab/venv/bin/python", "/lab/src/storage-qualification/restored_verify.py", "--run-id", args.run_id)
        restored_file = "restored-preservation-"+args.run_id+".json"
        restored = json.loads((root / "results" / restored_file).read_text())
        assert restored["status"] == "PASS_restored_archive_samples_and_population"
        report["restored_evidence"] = restored_file
        report["status"] = "PASS_quiescent_archive_copy_restore"
    except BaseException as exc:
        report["status"] = "FAILED"
        report["error"] = repr(exc)
        raise
    finally:
        save()


if __name__ == "__main__":
    main()
