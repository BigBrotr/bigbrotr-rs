#!/usr/bin/env python3
"""Restart only the owned CH server after all experiments have quiesced."""

import argparse
import datetime
import hashlib
import json
from pathlib import Path
import subprocess
import time
from urllib.error import URLError
from urllib.request import urlopen

from summarize import check_preservation


NAME = "bigbrotr-qualification-ch-20260917"


def command(*args, timeout=60):
    return subprocess.run(
        args, check=True, capture_output=True, text=True, timeout=timeout
    ).stdout.strip()


def inspect():
    raw = json.loads(command("docker", "inspect", NAME))[0]
    return {
        "id": raw["Id"],
        "image": raw["Image"],
        "state": {
            key: raw["State"][key]
            for key in ("Running", "OOMKilled", "ExitCode", "StartedAt", "FinishedAt")
        },
        "mounts": sorted(
            (
                {key: item[key] for key in ("Source", "Destination", "RW")}
                for item in raw["Mounts"]
            ),
            key=lambda item: (item["Destination"], item["Source"], item["RW"]),
        ),
    }


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--root", type=Path, required=True)
    p.add_argument("--run-id", default="")
    args = p.parse_args()
    assert args.root == Path("/home/tux/bigbrotr-qualification-20260917")
    assert not args.run_id or (args.run_id.isalnum() and len(args.run_id) <= 24)
    suffix = "-" + args.run_id if args.run_id else ""
    verification_label = "after-ch-restart" + suffix
    output = args.root / "results" / ("ch-process-restart" + suffix + ".json")
    assert not output.exists()
    for name, expected in (
        (
            "qualification_recovery_recmixed02ch.json",
            "PASS_bounded_ch_mixed_slice",
        ),
        ("ch-backup-restore.json", "PASS_native_CH_backup_restore"),
    ):
        assert (
            json.loads((args.root / "results" / name).read_text())["status"] == expected
        )
    before = inspect()
    processes = command(
        "docker", "top", "bigbrotr-qualification-worker-20260917", "-eo", "pid,args"
    )
    assert not any(
        name in processes
        for name in ("engines.py", "recovery.py", "ch_backup.py", "verify.py")
    ), "Owned engine work has not quiesced"
    assert before["state"]["Running"]
    assert any(
        m["Source"] == str(args.root / "ch-data")
        and m["Destination"] == "/var/lib/clickhouse"
        for m in before["mounts"]
    )
    report = {
        "status": "RUNNING",
        "started_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "scope": "graceful owned CH process stop/start plus large-table population and signed sample rechecks; no power/host/device failure",
        "before": before,
        "source_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
    }

    def save():
        output.write_text(json.dumps(report, indent=2) + "\n")

    save()
    started = time.monotonic()
    try:
        command("docker", "stop", "-t", "30", NAME)
        report["stopped"] = inspect()
        save()
        state = report["stopped"]["state"]
        assert (
            not state["Running"] and not state["OOMKilled"] and state["ExitCode"] == 0
        )
        command("docker", "start", NAME)
        deadline = time.monotonic() + 90
        while True:
            try:
                with urlopen("http://127.0.0.1:48623/ping", timeout=2) as response:
                    assert response.read() == b"Ok.\n"
                break
            except (URLError, TimeoutError, ConnectionError):
                if time.monotonic() >= deadline:
                    raise
                time.sleep(0.5)
        report["seconds_until_healthy"] = time.monotonic() - started
        report["after"] = inspect()
        for key in ("id", "image", "mounts"):
            assert report["after"][key] == before[key]
        assert report["after"]["state"]["Running"]
        save()
        command(
            "docker",
            "exec",
            "bigbrotr-qualification-worker-20260917",
            "/lab/venv/bin/python",
            "/lab/src/storage-qualification/verify.py",
            "--corpus",
            "aged",
            "--only-ch",
            "--run-label",
            verification_label,
            timeout=600,
        )
        path = args.root / "results" / ("aged-" + verification_label + ".json")
        verification = json.loads(path.read_text())
        baseline = json.loads(
            (args.root / "results/aged-preservation-v2.json").read_text()
        )
        check_preservation(verification, ("id", "time"), baseline)
        report["verification"] = {
            "file": path.name,
            "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
            "status": verification["status"],
        }
        report["status"] = "PASS_graceful_CH_process_restart"
    except BaseException as exc:
        report["status"], report["error"] = "FAILED", repr(exc)
        raise
    finally:
        report["elapsed_seconds"] = time.monotonic() - started
        report["finished_utc"] = datetime.datetime.now(
            datetime.timezone.utc
        ).isoformat()
        save()


if __name__ == "__main__":
    main()
