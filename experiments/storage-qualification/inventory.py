#!/usr/bin/env python3
"""Capture only the owned lab and nonsensitive host capacity, not other app data."""

import argparse
import datetime
import hashlib
import json
from pathlib import Path
import shutil
import subprocess


def output(*command):
    return subprocess.run(
        command, check=True, capture_output=True, text=True
    ).stdout.strip()


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--root", type=Path, required=True)
    p.add_argument("--label", required=True)
    args = p.parse_args()
    assert args.root.name == "bigbrotr-qualification-20260917"
    assert args.label.replace("-", "").isalnum()
    result = args.root / "results" / ("inventory-" + args.label + ".json")
    assert not result.exists()
    report = {
        "captured_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "capture_scope": "after workload creation; not a reconstructed pre-run measurement",
        "uname": output("uname", "-a"),
        "cpu": json.loads(output("lscpu", "-J")),
        "meminfo": Path("/proc/meminfo").read_text(),
        "filesystem": output("findmnt", "-J", "-T", str(args.root)),
        "disk_bytes": dict(
            zip(("total", "used", "free"), shutil.disk_usage(args.root))
        ),
        "containers": [],
        "file_identities": [],
    }
    for role in ("worker", "pg", "ch"):
        name = "bigbrotr-qualification-" + role + "-20260917"
        raw = json.loads(output("docker", "inspect", name))[0]
        host = raw["HostConfig"]
        image = json.loads(output("docker", "image", "inspect", raw["Image"]))[0]
        report["containers"].append(
            {
                "name": name,
                "id": raw["Id"],
                "image_id": raw["Image"],
                "repo_digests": image.get("RepoDigests", []),
                "state": {
                    k: raw["State"].get(k)
                    for k in (
                        "Status",
                        "Running",
                        "OOMKilled",
                        "ExitCode",
                        "StartedAt",
                        "FinishedAt",
                    )
                },
                "limits": {
                    k: host.get(k)
                    for k in (
                        "Memory",
                        "MemorySwap",
                        "NanoCpus",
                        "Ulimits",
                        "NetworkMode",
                        "PortBindings",
                    )
                },
                "mounts": [
                    {k: m[k] for k in ("Type", "Source", "Destination", "RW")}
                    for m in raw["Mounts"]
                ],
            }
        )
        for metric in ("memory.peak", "memory.events", "cpu.stat"):
            path = (
                Path("/sys/fs/cgroup/system.slice")
                / ("docker-" + raw["Id"] + ".scope")
                / metric
            )
            if path.exists():
                report["containers"][-1][metric] = path.read_text()
    report["total_running_containers"] = len(output("docker", "ps", "-q").splitlines())
    if report["containers"][0]["state"]["Running"]:
        report["worker_packages"] = json.loads(
            output(
                "docker",
                "exec",
                "bigbrotr-qualification-worker-20260917",
                "/lab/venv/bin/python",
                "-m",
                "pip",
                "list",
                "--format=json",
            )
        )
    else:
        report["worker_packages_status"] = (
            "Not re-read: owned worker stopped; see preceding live capture. "
            "Inventory does not restart containers."
        )
    rustc = shutil.which("rustc") or str(Path.home() / ".cargo/bin/rustc")
    report["rustc"] = output(rustc, "--version")
    for directory in ("extensions", "config", "src/storage-qualification"):
        for path in sorted((args.root / directory).rglob("*")):
            if (
                not path.is_file()
                or "target" in path.parts
                or "__pycache__" in path.parts
            ):
                continue
            with path.open("rb") as source:
                digest = hashlib.file_digest(source, "sha256").hexdigest()
            report["file_identities"].append(
                {
                    "path": str(path.relative_to(args.root)),
                    "bytes": path.stat().st_size,
                    "sha256": digest,
                }
            )
    result.write_text(json.dumps(report, indent=2) + "\n")


if __name__ == "__main__":
    main()
