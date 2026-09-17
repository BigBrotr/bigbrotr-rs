#!/usr/bin/env python3
"""Inspect actual archive footers and CH plans after the timed baselines."""

import json
from pathlib import Path

import pyarrow.parquet as pq
from engines import ch, lake, literal, save


def main():
    root = Path("/lab")
    destination = root / "results/layout-inspection-v2.json"
    assert not destination.exists()
    report = {
        "status": "RUNNING",
        "scope": "post-load effective options, sampled file footers and explain plans; not full query-path/cold-cache qualification",
        "archive": {},
        "clickhouse": {},
    }
    save(destination, report)
    con = lake(root)
    try:
        report["effective_archive_options"] = con.execute(
            "SELECT option_name,value,scope,scope_entry FROM ducklake_options('lake')"
        ).fetchall()
        for name in ("aged_lake", "aged_lake_zstd"):
            paths = con.execute(
                "SELECT data_file FROM ducklake_list_files('lake',?)", [name]
            ).fetchall()
            path = paths[0][0]
            metadata = pq.read_metadata(path)
            groups = [metadata.row_group(i) for i in range(metadata.num_row_groups)]
            receipt_files = con.execute(
                "SELECT count(*),coalesce(sum(data_file_size_bytes),0) FROM ducklake_list_files('lake',?)",
                [name + "_receipts"],
            ).fetchone()
            report["archive"][name] = {
                "file": str(Path(path).relative_to(root)),
                "sample_rows": metadata.num_rows,
                "row_groups": [g.num_rows for g in groups],
                "codecs": sorted(
                    {
                        g.column(i).compression
                        for g in groups
                        for i in range(g.num_columns)
                    }
                ),
                "receipt_files": receipt_files,
                "receipt_rows": con.execute(
                    "SELECT count(*) FROM lake." + name + "_receipts"
                ).fetchone()[0],
            }
        sample = json.loads((root / "input/aged/manifest.json").read_text())[
            "partitions"
        ][0]["samples"][0]
        for name in ("aged_id", "aged_time"):
            report["clickhouse"][name] = {}
            for key, query in (
                (
                    "id",
                    f"SELECT content FROM {name} WHERE id=unhex({literal(sample['id'])})",
                ),
                (
                    "chronological100",
                    f"SELECT ordinal FROM {name} ORDER BY created_at DESC,id LIMIT 100",
                ),
            ):
                report["clickhouse"][name][key] = ch("EXPLAIN indexes=1,projections=1 " + query)[0]
        report["status"] = "PASS_effective_layout_inspection"
    except BaseException as exc:
        report["status"], report["error"] = "FAILED", repr(exc)
        raise
    finally:
        con.close()
        save(destination, report)


if __name__ == "__main__":
    main()
