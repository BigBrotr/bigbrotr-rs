#!/usr/bin/env python3
"""Isolated capability model and daemon-free Compose parser experiment."""

import argparse
import hashlib
import json
import platform
from pathlib import Path
import subprocess


HERE = Path(__file__).resolve().parent


def evaluate(case, wrong=None):
    """Research model only: supplied authorities are fixture assertions, not probes."""
    roles = set(case["roles"])
    supplied = set(case["supply"])
    needed = set()
    issues = []
    targets = set()
    algorithm = None
    if roles & {"sync", "monitor"}:
        needed |= {"control", "registry", "ingest", "archive", "egress"}
        if wrong == "require-validator":
            needed.add("validator")
        targets = set(case.get("enrolled", []))
        if case.get("selection") == "fixed":
            fixed = set(case.get("fixed", []))
            if not fixed:
                issues.append("empty-fixed-selection")
            if wrong != "registry-widens-fixed":
                targets &= fixed
        targets -= set(case.get("blocked", []))
    if "sync" in roles:
        mode = case["mode"]
        if wrong == "always-require-inventory" or mode == "negentropy":
            needed.add("inventory")
        algorithm = "negentropy" if mode != "linear" and "inventory" in supplied else "linear"
        if mode == "negentropy" and "inventory" not in supplied:
            algorithm = "unavailable"
    if "read" in roles:
        needed |= {"control", "ch"}
        if wrong == "ch-alone":
            needed.discard("control")
        if case["pg_dataset"] != case["ch_dataset"] and wrong != "ignore-dataset":
            issues.append("dataset-mismatch")
        if not case["frontier_verified"]:
            issues.append("unverified-frontier")
        if not case["policy_live"]:
            issues.append("policy-expired")
        if case["resource"] == "current" and not case.get("temporal_valid", False):
            issues.append("temporal-unavailable")
        if case["resource"] == "neg":
            needed.add("index")
    for host in case.get("archive_consumers", []):
        if case["archive_kind"] == "local":
            reachable = host == case["archive_owner_host"] or wrong == "same-path-shared"
        else:
            reachable = host in case.get("object_readers", [])
        if not reachable:
            issues.append("archive-unreachable:" + host)
    if (wrong == "split-always-needs-s3" and case.get("archive_kind") == "local"
            and case.get("role_host") != case.get("archive_owner_host")):
        issues.append("missing:s3")
    stores = {}
    for store, incarnation in case.get("rocks_claims", []):
        stores.setdefault(store, set()).add(incarnation)
    issues += ["multiple-rocks-writers:" + s for s, owners in stores.items() if len(owners) > 1]
    route = case.get("route")
    if route:
        if route["private"] and route["origin"] != "operator-root" and wrong != "borrow-infrastructure":
            issues.append("private-acquisition-denied")
        if not route["declared"]:
            issues.append("undeclared-infrastructure")
        if not route["tls_verified"]:
            issues.append("tls-unverified")
    takeover = case.get("takeover")
    if takeover and not takeover["old_excluded"] and wrong != "ttl-is-exclusion":
        issues.append("old-owner-not-excluded")
    issues += ["missing:" + dep for dep in needed - supplied]
    return {"issues": sorted(issues), "targets": sorted(targets), "algorithm": algorithm}


def compose_parser():
    """No daemon operations, image resolution, pulls or container starts."""
    version = subprocess.run(["docker", "compose", "version", "--short"],
                             check=True, capture_output=True, text=True, timeout=20).stdout.strip()
    prefix = ["docker", "compose", "--project-directory", str(HERE),
              "--env-file", "/dev/null", "-f", str(HERE / "compose-fixture.yaml")]
    # An empty inherited profile avoids local shell settings changing the fixture.
    import os
    env = dict(os.environ, COMPOSE_PROFILES="")
    cases = [("default", [], ["engine-placeholder"]),
             ("selected-profile", ["--profile", "selected"],
              ["engine-placeholder", "selected-placeholder"])]
    results = []
    for name, args, expected in cases:
        result = subprocess.run(prefix + args + ["config", "--format", "json"],
                                check=True, capture_output=True, text=True, timeout=20, env=env)
        model = json.loads(result.stdout)
        actual = sorted(model["services"])
        assert actual == expected, (name, actual, expected)
        if name == "selected-profile":
            service = model["services"]["selected-placeholder"]
            assert service["ports"][0]["host_ip"] == "127.0.0.1"
            assert sorted(service["depends_on"]) == ["engine-placeholder"]
        results.append({"name": name, "actual_services": actual})
    return {"status": "PASS", "version": version, "cases": results,
            "scope": "config parser only; no running network, engine or install"}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--evidence", type=Path)
    parser.add_argument("--compose", action="store_true")
    args = parser.parse_args()
    cases = json.loads((HERE / "fixtures.json").read_text())["cases"]
    by_name = {case["name"]: case for case in cases}
    for case in cases:
        actual = evaluate(case)
        assert actual == case["expected"], (case["name"], actual, case["expected"])
    controls = {
        "require-validator": "monitor-does-not-need-validator-or-ch",
        "registry-widens-fixed": "fixed-sync-linear-no-discovery-validator-monitor-or-ch",
        "always-require-inventory": "auto-without-inventory-uses-linear",
        "ch-alone": "ch-alone-is-not-serving",
        "ignore-dataset": "mismatched-pg-ch-is-not-ready",
        "same-path-shared": "split-same-local-path-does-not-share-bytes",
        "split-always-needs-s3": "split-local-archive-jobs-stay-on-storage-host",
        "borrow-infrastructure": "relay-cannot-borrow-private-database-route",
        "ttl-is-exclusion": "expired-lease-does-not-exclude-writer",
    }
    for wrong, name in controls.items():
        case = by_name[name]
        assert evaluate(case, wrong) != case["expected"], ("undetected negative control", wrong)
    result = {
        "status": "MODEL_PASS", "python": platform.python_version(),
        "case_count": len(cases), "cases": [case["name"] for case in cases],
        "negative_controls_detected": sorted(controls),
        "compose": compose_parser() if args.compose else {"status": "NOT_RUN"},
        "source_sha256": {p.name: hashlib.sha256(p.read_bytes()).hexdigest()
                          for p in [HERE / "probe.py", HERE / "fixtures.json", HERE / "compose-fixture.yaml"]},
        "limitations": ["No product code or resolver", "Fixture authority/reachability assertions only",
                        "No engine, native process, fsync, TLS handshake, network isolation or failover execution",
                        "No Tux or legacy repository access", "Not installation or capacity evidence"],
    }
    payload = json.dumps(result, indent=2) + "\n"
    if args.evidence:
        args.evidence.write_text(payload)
    print(payload, end="")


if __name__ == "__main__":
    main()
