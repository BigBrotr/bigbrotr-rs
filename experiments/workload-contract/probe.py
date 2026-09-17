#!/usr/bin/env python3
"""Virtual scheduling/arithmetic oracle; no sockets, engines or product code."""

from __future__ import annotations

import argparse
from collections import defaultdict, deque
from datetime import datetime, timezone
from fractions import Fraction
import hashlib
import json
from pathlib import Path


CLASSES = ("control", "monitor", "live", "backfill", "maintenance")
SHARES = dict(zip(CLASSES, (2, 2, 3, 2, 1)))
ENDPOINTS = 500
PERIOD = 300
TICKS = 600
CAP = 5000


def simulate(fair: bool) -> dict:
    queues = {name: deque() for name in CLASSES}
    offered, completed, refused = defaultdict(int), defaultdict(int), defaultdict(int)
    ages = {name: [] for name in CLASSES}
    monitor_observations = defaultdict(list)
    maximum_queued = 0

    def offer(name: str, due: int, endpoint: int = -1) -> None:
        offered[name] += 1
        if sum(map(len, queues.values())) >= CAP:
            refused[name] += 1
        else:
            queues[name].append((due, endpoint))

    def take(name: str, tick: int) -> bool:
        if not queues[name]:
            return False
        due, endpoint = queues[name].popleft()
        completed[name] += 1
        ages[name].append(tick - due)
        if name == "monitor":
            monitor_observations[endpoint].append((due, tick))
        return True

    for tick in range(TICKS):
        offer("control", tick)
        for _ in range(3):
            offer("live", tick)
        for _ in range(5):
            offer("backfill", tick)
        offer("maintenance", tick)
        if tick % PERIOD == 0:
            for endpoint in range(ENDPOINTS):
                offer("monitor", tick, endpoint)
        maximum_queued = max(maximum_queued, sum(map(len, queues.values())))
        if fair:
            used = 0
            for name in CLASSES:
                for _ in range(SHARES[name]):
                    used += take(name, tick)
            # Unused class shares are borrowed for one bounded work quantum only.
            while used < sum(SHARES.values()) and any(queues.values()):
                name = min((n for n in CLASSES if queues[n]),
                           key=lambda n: (queues[n][0][0], CLASSES.index(n)))
                used += take(name, tick)
        else:
            # Deliberately wrong priority: endless backfill wins before monitor.
            for _ in range(sum(SHARES.values())):
                for name in ("control", "live", "backfill", "maintenance", "monitor"):
                    if take(name, tick):
                        break
    return {
        "offered": dict(offered), "completed": dict(completed),
        "refused": {name: refused[name] for name in CLASSES},
        "remaining": {name: len(queues[name]) for name in CLASSES},
        "maximum_queued": maximum_queued,
        "maximum_wait_ticks": {name: max(ages[name], default=None) for name in CLASSES},
        "monitor_observations": dict(monitor_observations),
    }


def oracle(result: dict) -> None:
    # Expected obligations are literal fixture facts, independent of the scheduler.
    assert result["offered"]["monitor"] == 1000
    assert set(result["monitor_observations"]) == set(range(500)), "cold endpoint starvation"
    for endpoint, observations in result["monitor_observations"].items():
        assert [due for due, _ in observations] == [0, 300], (endpoint, "lost due cycle")
        assert all(start - due < 300 for due, start in observations), "overdue attempt"
    assert result["completed"]["control"] == 600
    assert result["maximum_wait_ticks"]["control"] <= 1
    assert result["completed"]["live"] == 1800
    assert result["completed"]["maintenance"] == 600
    assert result["maximum_queued"] <= CAP
    for name in CLASSES:
        assert (result["completed"].get(name, 0) + result["remaining"][name]
                + result["refused"][name] == result["offered"][name]), "unaccounted offered work"


def expect_failure(call) -> None:
    try:
        call()
    except AssertionError:
        return
    raise AssertionError("negative control escaped its oracle")


def category_oracle(new_body, new_edge, existing_edge, corrections, new_scopes, provenance_quota):
    assert new_body.isdisjoint(new_edge) and new_body.isdisjoint(existing_edge)
    assert new_edge.isdisjoint(existing_edge)
    assert corrections <= existing_edge, "correction must already have its scoped witness"
    assert new_scopes <= new_edge, "new scope requires a new scoped witness"
    assert corrections.isdisjoint(new_scopes), "one delivery cannot correct a newly created scope"
    # Even if a wrong fixture labels S existing-edge, its previously unseen scope
    # physically introduces a witness. The union catches hidden work outside P.
    assert len(new_body | new_edge | new_scopes) == provenance_quota, "new-scope work exceeded fixed P quota"


def run() -> dict:
    fair = simulate(True)
    oracle(fair)
    unfair = simulate(False)
    expect_failure(lambda: oracle(unfair))
    # Percentiles of survivors alone conceal all 1,000 missed observations.
    assert unfair["completed"]["control"] == 600
    assert unfair["maximum_wait_ticks"]["control"] == 0
    assert unfair["remaining"]["monitor"] == 1000

    mean_kib = sum(Fraction(p, 100) * size for p, size in
                   ((80, 1), (15, 4), (4, 32), (1, 256)))
    assert mean_kib == Fraction(131, 25)
    tracks = []
    for r, q in ((5000, 20), (10000, 40), (50000, 100), (100000, 200)):
        deliveries, unique, edges = r // 2, r // 20, r // 4
        assert unique <= edges <= deliveries
        tracks.append({"registered_endpoints": r, "active_target": r // 2,
                       "D_per_s": deliveries, "U_per_s": unique, "P_per_s": edges,
                       "liveness_due_per_s": str(Fraction(r, 300)),
                       "query_per_s": q,
                       "payload_mib_per_s_before_overhead": float(deliveries * mean_kib / 1024)})
    assert tracks[-1]["payload_mib_per_s_before_overhead"] * 1024**2 * 8 > 10**9
    alpha = Fraction(17, 20)
    stationary = [Fraction(400, 1029), Fraction(340, 1029), Fraction(289, 1029)]
    assert sum(stationary) == 1
    assert stationary == [1-alpha+alpha*stationary[2], alpha*stationary[0], alpha*stationary[1]]
    # Another wrong oracle counts completed work only and discards refused/queued.
    incomplete = dict(fair)
    incomplete["remaining"] = {name: 0 for name in CLASSES}
    expect_failure(lambda: oracle(incomplete))
    # A complete quota window avoids rounding C=0.001D and S=0.0005D.
    new_body, new_edge, existing_edge = set(range(200)), set(range(200, 1000)), set(range(1000, 2000))
    corrections, new_scopes = {1000, 1001}, {200}
    category_oracle(new_body, new_edge, existing_edge, corrections, new_scopes, 1000)
    expect_failure(lambda: category_oracle(new_body, new_edge, existing_edge, corrections, {1002}, 1000))
    assert len(new_body | new_edge | {1002}) == 1001  # deliberately wrong S adds hidden P.
    expect_failure(lambda: category_oracle(new_body, new_edge, existing_edge, {200, 1001}, new_scopes, 1000))
    return {
        "kind": "virtual-scheduler-and-workload-arithmetic",
        "status": "PASS", "executed_at_utc": datetime.now(timezone.utc).isoformat(),
        "source_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "fixture": {"endpoints": ENDPOINTS, "ticks": TICKS,
                    "quantum_capacity_per_tick": 10, "queue_cap": CAP, "class_shares": SHARES},
        "fair_result": {key: value for key, value in fair.items() if key != "monitor_observations"},
        "negative_controls_detected": 4,
        "delivery_category_quota": {"D": 2000, "U": 200, "P": 1000, "C": 2, "S": 1,
                                    "C_from_existing_edge": True, "S_from_known_body_new_edge": True,
                                    "incorrect_existing_edge_S_physical_P": 1001},
        "unfair_control_max_wait": unfair["maximum_wait_ticks"]["control"],
        "unfair_unserved_monitor_obligations": unfair["remaining"]["monitor"],
        "tracks_arithmetic": tracks,
        "mean_target_event_kib": str(mean_kib),
        "stationary_three_node_oracle": [str(value) for value in stationary],
        "not_executed": ["real signed network fixture", "BigBrotr product", "actual socket fairness",
                         "native scheduling", "database engines", "throughput/latency capacity", "host-loss recovery"],
        "limits": "Ticks and work quanta are dimensionless; these results do not establish the workload SLOs or fleet capacity.",
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    encoded = json.dumps(run(), indent=2) + "\n"
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(encoded)
    print(encoded, end="")
