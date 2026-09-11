"""Independent minute enumeration of the documented relay-profile hand fixture."""

import argparse
from collections import Counter
from fractions import Fraction
import hashlib
import json
import math
from pathlib import Path
import platform
import sys


WINDOW_MINUTES = 1440
BASE_CADENCE_MINUTES = 10
# Raw anchors from the normative hand fixture, not its reported S/F/U totals.
ANCHORS = ((-5, "S"), (5, "F"), (10, "U"), (25, "S"),
           (30, "S"), (35, "F"), (45, "S"), (50, "S"))
LATENCIES_US = (200000, 100000, 300000, 900000)


def sha256(data):
    return hashlib.sha256(data).hexdigest()


def enumerate_minutes(anchors, boundary=None):
    counts = Counter({"S": 0, "F": 0, "U": 0})
    minute_states = []
    for minute in range(WINDOW_MINUTES):
        # Integer half-minutes express each midpoint exactly, without floats.
        midpoint = 2 * minute + 1
        preceding = [anchor for anchor in anchors if 2 * anchor[0] <= midpoint]
        state = "U"
        if preceding:
            latest_time, latest_state = max(preceding, key=lambda anchor: anchor[0])
            held_until = latest_time + 2 * BASE_CADENCE_MINUTES
            boundary_blocks = boundary is not None and latest_time < boundary and midpoint >= 2 * boundary
            if midpoint < 2 * held_until and not boundary_blocks:
                state = latest_state
        counts[state] += 1
        minute_states.append(state)
    assert sum(counts.values()) == WINDOW_MINUTES
    return dict(counts), "".join(minute_states)


def fraction_value(value):
    if value is None:
        return None
    return {"fraction": str(value), "float64": float(value)}


def components(counts, classified_attempts, latencies):
    classified_minutes = counts["S"] + counts["F"]
    availability = Fraction(counts["S"], classified_minutes) if classified_minutes else None
    coverage = Fraction(classified_minutes, WINDOW_MINUTES)
    ordered = sorted(latencies)
    median = ordered[(len(ordered) - 1) // 2] if ordered else None
    latency = math.exp(-median / 1_000_000) if median is not None else None
    gate = classified_attempts >= 5
    quality = (0.625 * float(availability) + 0.375 * latency
               if gate and availability is not None and latency is not None else None)
    if quality is not None:
        assert math.isfinite(quality) and 0 <= quality <= 1
    return {"availability": fraction_value(availability), "covered_fraction": fraction_value(coverage),
            "classified_attempts": classified_attempts, "successful_latency_count": len(ordered),
            "sample_gate_met": gate, "p50_us": median, "L": latency, "Q": quality}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--source", type=Path, default=Path(__file__).resolve().parents[2]
                        / "docs/architecture/analytics-detail.md")
    args = parser.parse_args()
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    source_before = args.source.read_bytes()
    lines = source_before.decode("utf-8").splitlines(keepends=True)
    first = next(i for i, line in enumerate(lines)
                 if line.rstrip() == "## Relay profile general-read-relay/1")
    end = next((i for i in range(first + 1, len(lines)) if lines[i].startswith("## ")), len(lines))
    section = "".join(lines[first:end]).encode("utf-8")
    with (output / "source-section.md").open("xb") as handle:
        handle.write(section)

    classified = sum(0 <= time < WINDOW_MINUTES and state in ("S", "F") for time, state in ANCHORS)
    successes = sum(0 <= time < WINDOW_MINUTES and state == "S" for time, state in ANCHORS)
    baseline, baseline_states = enumerate_minutes(ANCHORS)
    boundary, boundary_states = enumerate_minutes(ANCHORS, boundary=60)
    baseline_result = components(baseline, classified, LATENCIES_US)
    boundary_result = components(boundary, classified, LATENCIES_US)
    # Compare independently enumerated values with the prose's stated expectations.
    assert baseline == {"S": 40, "F": 15, "U": 1385}, baseline
    assert boundary == {"S": 30, "F": 15, "U": 1395}, boundary
    assert baseline_result["availability"]["fraction"] == "8/11"
    assert baseline_result["covered_fraction"]["fraction"] == "11/288"
    assert boundary_result["availability"]["fraction"] == "2/3"
    assert boundary_result["covered_fraction"]["fraction"] == "1/32"
    assert classified == 6 and successes == 4 and baseline_result["p50_us"] == 200000
    assert math.isclose(baseline_result["Q"], 0.625 * (8 / 11) + 0.375 * math.exp(-0.2), abs_tol=1e-15)

    wrong_count = Fraction(successes, classified)
    ordered = sorted(LATENCIES_US)
    wrong_median = Fraction(ordered[1] + ordered[2], 2)
    wrong_carry, wrong_states = enumerate_minutes(tuple(anchor for anchor in ANCHORS if anchor[1] != "U"))
    assert wrong_count != Fraction(baseline["S"], baseline["S"] + baseline["F"])
    assert wrong_median == 250000 and wrong_median != baseline_result["p50_us"]
    assert wrong_carry != baseline

    all_failure_anchors = tuple((minute, "F") for minute in (0, 10, 20, 30, 40))
    all_failure, _ = enumerate_minutes(all_failure_anchors)
    all_failure_result = components(all_failure, len(all_failure_anchors), ())
    assert all_failure_result["availability"]["fraction"] == "0"
    assert all_failure_result["sample_gate_met"] and all_failure_result["Q"] is None
    assert args.source.read_bytes() == source_before, "Normative source changed during execution"

    result = {
        "status": "completed", "profile": "general-read-relay/1",
        "command": [sys.executable] + sys.argv, "python_version": platform.python_version(),
        "program_sha256": sha256(Path(__file__).read_bytes()),
        "normative_source": {"path": str(args.source), "sha256_before": sha256(source_before),
                             "sha256_after": sha256(source_before), "section_lines": [first + 1, end],
                             "section_sha256": sha256(section)},
        "input": {"window_minutes": [0, WINDOW_MINUTES], "base_cadence_minutes": BASE_CADENCE_MINUTES,
                  "hold_minutes": 2 * BASE_CADENCE_MINUTES, "anchors": ANCHORS,
                  "successful_in_window_latencies_us": LATENCIES_US},
        "method": "Independently enumerate 1440 minute midpoints, choose latest anchor, apply hold/boundary; all fixture boundaries are minute-aligned",
        "baseline": {"minutes": baseline, "microseconds": {k: v * 60_000_000 for k, v in baseline.items()},
                     "minute_states_sha256": sha256(baseline_states.encode("ascii")), **baseline_result},
        "policy_boundary_minute_60": {"minutes": boundary,
                                     "minute_states_sha256": sha256(boundary_states.encode("ascii")), **boundary_result},
        "incorrect_alternatives": {
            "probe_count_availability": {"value": fraction_value(wrong_count), "disagrees": True},
            "average_middle_latency": {"p50_us": int(wrong_median), "disagrees": True},
            "ignore_unknown_anchor": {"minutes": wrong_carry,
                                      "minute_states_sha256": sha256(wrong_states.encode("ascii")), "disagrees": True}},
        "all_failure_control": {"anchors": all_failure_anchors, "minutes": all_failure, **all_failure_result},
        "limits": "Standard library, two principal 1440-minute enumerations plus small controls; no networking, engines, Tux or product execution",
        "not_established": ["Runtime window/correction/expiry implementation", "Clock correctness or non-minute-aligned boundaries",
                            "Production capacity, statistical confidence or client utility", "NIP-85 publication implementation"],
    }
    with (output / "result.json").open("x") as handle:
        json.dump(result, handle, indent=2)
        handle.write("\n")
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
