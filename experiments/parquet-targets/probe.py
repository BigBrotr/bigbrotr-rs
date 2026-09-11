"""Bounded real-engine counterexample: Parquet targets are not hard limits."""

import argparse
import hashlib
import json
from pathlib import Path

import duckdb


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    root = args.output.resolve()
    root.mkdir(parents=True, exist_ok=False)
    conn = duckdb.connect(config={"threads": "1", "memory_limit": "128MB"})
    path = root / "target.parquet"
    # The output path is a SQL value. No shell interpolation is involved.
    escaped = str(path).replace("'", "''")
    conn.execute(
        f"COPY (SELECT range AS id FROM range(200000)) TO '{escaped}' "
        "(FORMAT parquet, ROW_GROUP_SIZE 64000, COMPRESSION zstd)"
    )
    groups = conn.execute(
        "SELECT DISTINCT row_group_id, row_group_num_rows "
        "FROM parquet_metadata(?) ORDER BY row_group_id", [str(path)]
    ).fetchall()
    observed = conn.execute(
        "SELECT count(*), min(id), max(id), sum(id) FROM read_parquet(?)", [str(path)]
    ).fetchone()
    assert observed == (200000, 0, 199999, 19999900000), observed
    assert sum(n for _, n in groups) == 200000, groups
    assert any(n > 64000 for _, n in groups), groups
    conn.close()
    report = {
        "duckdb": duckdb.__version__,
        "scope": "one local COPY and metadata/readback; not DuckLake/Rust or a memory/performance test",
        "limits": {"threads": 1, "duckdb_memory_limit": "128MB", "input_rows": 200000},
        "requested_row_group_size": 64000,
        "actual_row_groups": groups,
        "independent_arithmetic_readback": list(observed),
        "hard_64000_row_cap_counterexample": "reproduced",
        "parquet_bytes": path.stat().st_size,
        "parquet_sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
        "program_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "status": "PASS",
    }
    (root / "result.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
