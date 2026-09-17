#!/usr/bin/env python3
"""Independent small semantic oracles; no production solver or network claim."""
from fractions import Fraction as F
import json
from pathlib import Path


def stationary(matrix, teleport, alpha):
    n = len(teleport)
    rows = [[F(i == j) - alpha * matrix[i][j] for j in range(n)] +
            [(1 - alpha) * teleport[i]] for i in range(n)]
    for pivot in range(n):
        k = next(i for i in range(pivot, n) if rows[i][pivot])
        rows[pivot], rows[k] = rows[k], rows[pivot]
        scale = rows[pivot][pivot]
        rows[pivot] = [x / scale for x in rows[pivot]]
        for i in range(n):
            if i != pivot:
                scale = rows[i][pivot]
                rows[i] = [a - scale * b for a, b in zip(rows[i], rows[pivot])]
    return [row[-1] for row in rows]


def solve(edges, teleport, initial=None):
    n, alpha, epsilon = len(teleport), F(85, 100), F(1, 100000000)
    matrix = [[F(0) for _ in range(n)] for _ in range(n)]
    for source, targets in enumerate(edges):
        for target in range(n):
            matrix[target][source] = F(target in targets, len(targets)) if targets else teleport[target]
    exact = stationary(matrix, teleport, alpha)
    x = [float(v) for v in (initial or teleport)]
    for passes in range(257):
        # Exact-rational residual of the actual binary floating candidate: independent
        # from the future bounded floating-point residual implementation.
        rational = [F(v) for v in x]
        mapped = [(1-alpha)*teleport[i] + alpha*sum(matrix[i][j]*rational[j] for j in range(n)) for i in range(n)]
        residual = sum(abs(a-b) for a, b in zip(mapped, rational))
        bound = residual / (1-alpha)
        error = sum(abs(a-b) for a, b in zip(exact, rational))
        assert error <= bound
        if bound <= epsilon:
            return {"passes": passes, "error_l1": float(error), "upper_l1": float(bound), "vector": x}
        x = [float(v) for v in mapped]
    raise AssertionError("tiny reference exceeded declared ceiling")


def main():
    graphs = [([[1], [0]], [F(1), F(0)]),
              ([[1, 2], [], [1]], [F(1), F(0), F(0)]),
              ([[1], [2], [3], [1]], [F(1), F(0), F(0), F(0)])]
    results = []
    for edges, teleport in graphs:
        cold = solve(edges, teleport)
        warm = solve(edges, teleport, cold["vector"])
        assert warm["passes"] == 0
        results.append({"cold": cold, "same_graph_warm": warm})
    # A claimed zero residual/one-pass convergence is rejected by the exact oracle.
    assert results[0]["cold"]["passes"] > 1

    events = [dict(id="a", time=40, sequence=1, coordinate="a", scope="public"),
              dict(id="b", time=30, sequence=2, coordinate="b", scope="public"),
              dict(id="c", time=20, sequence=3, coordinate="c", scope="public"),
              dict(id="x", time=28, sequence=4, coordinate="x", scope="public"),
              dict(id="d", time=10, sequence=5, coordinate="d", scope="public"),
              dict(id="b-new", time=35, sequence=6, coordinate="b", scope="public"),
              dict(id="c-private", time=25, sequence=7, coordinate="c", scope="private"),
              dict(id="backdated", time=15, sequence=8, coordinate="e", scope="public")]
    candidates = sorted((e for e in events if e["sequence"] <= 5), key=lambda e: (-e["time"], e["id"]))
    public = [e for e in events if e["scope"] == "public"]
    winners = {}
    for event in sorted(public, key=lambda e: (-e["time"], e["id"])):
        winners.setdefault(event["coordinate"], event["id"])
    expired = {"a", "x"}
    continuation = [e["id"] for e in candidates[1:] if e["id"] not in expired and winners[e["coordinate"]] == e["id"]]
    assert continuation == ["c", "d"]
    wrong_private_winner = [e["id"] for e in candidates[1:] if e["id"] not in {"b", "c"}]
    assert wrong_private_winner != continuation
    wrong_cut_only = [e["id"] for e in candidates[1:]]
    assert wrong_cut_only != continuation
    tied = [{"id": "00", "time": 10}, {"id": "ff", "time": 10}]
    assert sorted(tied, key=lambda e: (-e["time"], e["id"]))[0]["id"] == "00"
    assert sorted(tied, key=lambda e: (e["time"], e["id"]), reverse=True)[0]["id"] != "00"
    report = {"status": "PASS", "scope": "3 exact-rational tiny graph oracles and changing-eligibility traversal; no runtime/capacity claim",
              "rank": results, "traversal": {"first_page_before_changes": ["a"],
              "after_expiry_replacement_private_replacement_backdate": continuation,
              "negative_controls_rejected": ["private input suppresses public", "candidate cut freezes suppression", "one pass means convergence", "greatest ID wins equal timestamp"]}}
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
