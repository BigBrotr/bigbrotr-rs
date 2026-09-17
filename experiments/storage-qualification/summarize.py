#!/usr/bin/env python3
"""Condense retained raw reports without upgrading their tested scope."""

import argparse
import hashlib
import json
import math
from pathlib import Path
import statistics


def timings(values):
    ordered = sorted(values)
    if not ordered:
        return {"count": 0}
    return {
        "count": len(ordered),
        "min": ordered[0],
        "median": statistics.median(ordered),
        "p95_nearest_rank": ordered[math.ceil(0.95 * len(ordered)) - 1],
        "max": ordered[-1],
    }


def check_preservation(report, engines, baseline=None):
    assert report["status"] == "PASS_sampled_signed_preservation_and_population_oracles"
    assert report["rows"] == 20000000 and report["sample_rows"] == 1743
    assert set(report["engines"]) == set(engines)
    digests = set()
    for name, engine in report["engines"].items():
        assert engine["status"] == "PASS" and engine["sample_rows"] == 1743
        assert engine["aggregate_expected"] == [
            20000000,
            20000000,
            199999990000000,
            20000000,
            0,
            19999999,
        ]
        digests.add(engine["complete_result_sha256"])
        if baseline is not None:
            original = baseline["engines"][name]
            for key in ("complete_result_sha256", "chronological_ordinals"):
                assert engine[key] == original[key]
    assert len(digests) == 1


def compact_latencies(value):
    if isinstance(value, list):
        return [compact_latencies(item) for item in value]
    if not isinstance(value, dict):
        return value
    result = {}
    for key, item in value.items():
        if key in ("latency_seconds", "queue_seconds"):
            result[key.replace("_seconds", "_summary_seconds")] = timings(item)
        else:
            result[key] = compact_latencies(item)
    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    root = args.input
    report = {
        "status": "RECORDED_WITH_SCOPE_LIMITS",
        "corpus_rows": 20000000,
        "product_implemented": False,
        "full_fleet_qualified": False,
        "scope": "large signed-body representations; independently sampled preservation; isolated recovery/restore/mixed slices",
        "baselines": {},
        "raw_artifacts": [],
    }
    for engine in ("lake", "lake_zstd", "id", "time"):
        d = json.loads((root / ("aged_" + engine + ".json")).read_text())
        assert d["status"].startswith("PASS") and d["rows"] == 20000000
        out = {
            key: d[key]
            for key in (
                "status",
                "versions",
                "source_bytes",
                "ingest_seconds",
                "files",
                "parts",
                "projection_parts",
                "archive_options",
            )
            if key in d
        }
        out["measurement_scope"] = (
            "summed insert calls; CH default fsync; uncontrolled cache/shared host; not durable-ACK or sustainable throughput"
        )
        if engine in ("id", "time"):
            out["disk_accounting"] = (
                "system.parts parent bytes already include projections; "
                "projection_parts is a component subset, not an additive total"
            )
        out["query_seconds"] = {}
        for name in sorted({q["name"] for q in d["queries"]}):
            canonical = "id_content_length" if name == "id_body" else name
            out["query_seconds"][canonical] = timings(
                [q["seconds"] for q in d["queries"] if q["name"] == name]
            )
        report["baselines"][engine] = out
    d = json.loads((root / "aged-preservation-v2.json").read_text())
    check_preservation(d, ("lake", "lake_zstd", "id", "time"))
    original_preservation = d
    report["preservation"] = {
        key: d[key]
        for key in (
            "status",
            "rows",
            "sample_rows",
            "sample_authors",
            "scope",
            "engines",
            "input_hash_seconds",
            "input_hash_timing",
        )
    }
    report["preservation"]["source_files"] = len(d["input_files"])
    report["layout"] = json.loads((root / "layout-inspection-v2.json").read_text())
    for filename in (
        "backup-restore-archive02.json",
        "restored-preservation-archive02.json",
        "ch-backup-restore.json",
        "aged-after-ch-restart-02.json",
    ):
        d = json.loads((root / filename).read_text())
        assert d["status"].startswith("PASS"), filename
        if filename == "aged-after-ch-restart-02.json":
            check_preservation(d, ("id", "time"), original_preservation)
        report[filename.removesuffix(".json")] = {
            key: value
            for key, value in d.items()
            if key not in ("files", "input_files")
        }
    original = json.loads((root / "qualification_recovery_recmixed01.json").read_text())
    assert original["status"] == "FAILED"
    assert {
        "lake-kill-before-commit",
        "lake-kill-after-commit-before-caller-ack",
        "ch-partial-family",
        "ch-dedup-disabled-replay",
        "pg-activation-response-lost",
        "same-length-corruption-negative",
        "mixed-lake",
    } <= set(original["cases"])
    for worker in original["cases"]["mixed-lake"]["workers"]:
        assert worker["offered"] == worker["started"] == worker["completed"] == 600
        assert worker["not_started"] == 0 and not worker["errors"]
    corrected = json.loads(
        (root / "qualification_recovery_recmixed02ch.json").read_text()
    )
    assert corrected["status"] == "PASS_bounded_ch_mixed_slice"
    mixed = corrected["cases"]["mixed-ch"]
    assert mixed["duration_seconds"] == 600
    assert len(mixed["workers"]) == 3
    assert {w["role"] for w in mixed["workers"]} == {"writer", "reader-a", "reader-b"}
    for worker in mixed["workers"]:
        assert worker["offered"] == 600
        assert 0 <= worker["completed"] <= worker["started"] <= 600
        assert worker["not_started"] == 600 - worker["started"]
    report["bounded_recovery_runs"] = {
        "recmixed01": compact_latencies(original),
        "recmixed02ch": compact_latencies(corrected),
        "interpretation": (
            "Original composite run FAILED in CH mixed readers (SQL alias collision). "
            "Completed crash/replay/DuckLake cases remain separately identified. "
            "Corrected CH-only run does not rewrite the original as PASS. "
            "Original interrupted writer raw not_started=0 is a harness accounting bug: "
            "600 scheduled minus 176 started leaves 424 never-started slots."
        ),
    }
    for name in (
        "inventory-qualification-live.json",
        "inventory-before-stop.json",
        "inventory-final.json",
        "ch-process-restart-02.json",
    ):
        value = json.loads((root / name).read_text())
        if name == "ch-process-restart-02.json":
            assert value["status"] == "PASS_graceful_CH_process_restart"
            verified_file = "aged-after-ch-restart-02.json"
            assert value["verification"]["file"] == verified_file
            assert (
                value["verification"]["sha256"]
                == hashlib.sha256((root / verified_file).read_bytes()).hexdigest()
            )
        report[name.removesuffix(".json")] = value
    report["failed_or_superseded_reports"] = []
    for path in sorted(root.iterdir()):
        if not path.is_file():
            continue
        data = path.read_bytes()
        report["raw_artifacts"].append(
            {
                "name": path.name,
                "bytes": len(data),
                "sha256": hashlib.sha256(data).hexdigest(),
            }
        )
        if path.suffix == ".json":
            try:
                value = json.loads(data)
            except ValueError:
                continue
            if isinstance(value, dict) and (
                value.get("status") in ("FAILED", "RUNNING")
                or ".previous-" in path.name
            ):
                report["failed_or_superseded_reports"].append(
                    {
                        "name": path.name,
                        "status": value.get("status", "UNSPECIFIED"),
                        "error": value.get("error", value.get("failure")),
                    }
                )
    report["not_qualified"] = [
        "full-corpus cryptographic verification",
        "power/host/disk loss",
        "production causal backup/fencing",
        "Rust adapters/native release tuple",
        "aged provenance/search/graph workload",
        "real 5k/10k/50k/100k busy relay fleet",
        "cold cache or equal-durable-ACK comparison",
        "global optimality or release readiness",
    ]
    args.output.write_text(json.dumps(report, indent=2) + "\n")


if __name__ == "__main__":
    main()
