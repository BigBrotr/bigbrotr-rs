#!/usr/bin/env python3
"""Disposable engine qualification, not a BigBrotr implementation or full pipeline."""
import argparse
import hashlib
import json
from pathlib import Path
import time
import uuid

import duckdb
import requests


def literal(value):
    return "'" + str(value).replace("'", "''") + "'"


def ch(sql, **settings):
    query_id = "qualification-" + str(uuid.uuid4())
    params = {"query_id": query_id, "max_threads": 4, "max_memory_usage": 7000000000,
              "max_execution_time": 600, "wait_end_of_query": 1,
              "buffer_size": 16777216, **settings}
    started = time.monotonic()
    response = requests.post("http://127.0.0.1:48623/", params=params,
                             data=sql.encode(), timeout=(10, 660))
    if response.status_code != 200 or response.headers.get("X-ClickHouse-Exception-Code"):
        raise RuntimeError(response.text[:4000])
    if sql.lstrip().split()[0].upper() in {"CREATE", "INSERT", "ALTER", "DROP", "SYSTEM"} and response.text.strip():
        raise RuntimeError("Unexpected nonempty mutation response: " + response.text[:4000])
    return response.text, {"seconds": time.monotonic() - started, "query_id": query_id,
                           "summary": json.loads(response.headers.get("X-ClickHouse-Summary", "{}"))}


def lake(root, install=False):
    con = duckdb.connect(config={"threads": "4", "memory_limit": "7GB",
        "extension_directory": str(root / "extensions"),
        "temp_directory": str(root / "tmp"), "max_temp_directory_size": "80GB"})
    if install:
        con.execute("INSTALL ducklake")
        con.execute("INSTALL postgres")
    con.execute("LOAD ducklake")
    con.execute("LOAD postgres")
    con.execute("ATTACH 'ducklake:postgres:dbname=qualification host=127.0.0.1 port=48632 user=postgres' AS lake "
                f"(DATA_PATH {literal(root / 'data')}, DATA_INLINING_ROW_LIMIT 0)")
    return con


def save(path, report):
    path.with_suffix(".pending").write_text(json.dumps(report, indent=2) + "\n")
    path.with_suffix(".pending").replace(path)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, default=Path("/lab"))
    parser.add_argument("--corpus", choices=("calibration", "aged"), required=True)
    parser.add_argument("--engine", choices=("lake", "lake_zstd", "id", "time"), required=True)
    parser.add_argument("--queries-only", action="store_true")
    args = parser.parse_args()
    manifest = json.loads((args.root / "input" / args.corpus / "manifest.json").read_text())
    table = args.corpus + "_" + args.engine
    result = args.root / "results" / (table + ".json")
    assert args.queries_only or not result.exists(), "Use a new table/run identity; do not overwrite evidence"
    report = {"corpus": args.corpus, "engine": args.engine, "rows": manifest["rows"],
              "source_bytes": manifest["source_bytes"], "batches": [], "queries": [],
              "cache_condition": "uncontrolled host page cache; bounded engine memory, not cold-cache proof",
              "scope": "signed-body storage and access paths; no admission/visibility/provenance/host-loss claim"}
    if args.queries_only:
        report = json.loads(result.read_text())
        result.with_suffix(f".previous-{time.time_ns()}.json").write_text(json.dumps(report, indent=2) + "\n")
        report["queries"] = []
        report["harness_correction"] = "DuckDB length counts Unicode characters; measure UTF-8 bytes with octet_length(encode(content))"
    report["status"] = "RUNNING"
    report["required_cases"] = {"batches": (len([f for p in manifest["partitions"] for f in p["files"]]) + 3) // 4,
                                "queries": 3 * (3 + min(8, sum(len(p["samples"]) for p in manifest["partitions"])))}
    report["source_sha256"] = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    report["settings"] = {"threads": 4, "query_memory_bytes": 7000000000,
                          "fsync_settings": "engine defaults; not equal-durable-ACK comparison"}
    files = sorted(f["path"] for p in manifest["partitions"] for f in p["files"])
    samples = [s for p in manifest["partitions"] for s in p["samples"]]
    con = lake(args.root, install=True) if args.engine.startswith("lake") else None
    if con and not args.queries_only:
        report["versions"] = {"duckdb": duckdb.__version__, "extensions": con.execute(
            "SELECT extension_name, extension_version FROM duckdb_extensions() "
            "WHERE extension_name IN ('ducklake','postgres','postgres_scanner')").fetchall()}
        con.execute(f"CREATE TABLE lake.{table} AS SELECT * FROM read_parquet("
                    f"{literal(args.root / 'input' / args.corpus / files[0])}) LIMIT 0")
        con.execute(f"CREATE TABLE lake.{table}_receipts(batch BIGINT, file_set_digest VARCHAR, rows BIGINT)")
        if args.engine == "lake_zstd":
            for key, value in (("parquet_compression", "zstd"), ("parquet_compression_level", "3"),
                               ("parquet_row_group_size", "8192"), ("target_file_size", "256MB")):
                con.execute(f"CALL lake.set_option({literal(key)},{literal(value)},table_name=>{literal(table)})")
            con.execute(f"CALL lake.set_option('data_inlining_row_limit',10,table_name=>{literal(table+'_receipts')})")
            report["archive_options"] = {"compression": "zstd", "level": 3, "row_group_rows": 8192,
                "target_file_size": "256MB", "receipt_inline_limit": 10}
    elif not args.queries_only:
        report["versions"] = {"clickhouse": ch("SELECT version()")[0].strip()}
        order = "id" if args.engine == "id" else "(created_at,id)"
        projection = "created_at,id" if args.engine == "id" else "id"
        ch(f"CREATE TABLE {table} (ordinal UInt64, id FixedString(32), pubkey FixedString(32), "
           "created_at Int64, kind UInt32, tags Array(Array(String)), content String CODEC(ZSTD(3)), "
           f"sig FixedString(64), PROJECTION alternate (SELECT _part_offset ORDER BY {projection})) "
           f"ENGINE=MergeTree ORDER BY {order} SETTINGS index_granularity=8192")
    save(result, report)
    for batch, offset in enumerate(range(0, len(files), 4)):
        if args.queries_only:
            break
        names = files[offset:offset+4]
        expected = sum(f["rows"] for p in manifest["partitions"] for f in p["files"] if f["path"] in names)
        started = time.monotonic()
        if con:
            paths = ",".join(literal(args.root / "input" / args.corpus / name) for name in names)
            con.execute("BEGIN")
            con.execute(f"INSERT INTO lake.{table} SELECT * FROM read_parquet([{paths}])")
            con.execute(f"INSERT INTO lake.{table}_receipts VALUES (?,?,?)", [batch,
                        hashlib.sha256("\n".join(names).encode()).hexdigest(), expected])
            con.execute("COMMIT")
            timing = {"seconds": time.monotonic() - started}
        else:
            glob = "input/" + args.corpus + "/{" + ",".join(names) + "}"
            _, timing = ch(f"INSERT INTO {table} SELECT * FROM file({literal(glob)}, Parquet)")
        report["batches"].append({"batch": batch, "rows": expected, **timing})
        save(result, report)
    report["ingest_seconds"] = sum(b["seconds"] for b in report["batches"])
    full_table = "lake." + table if con else table
    for repetition in range(3):
        queries = [("count", f"SELECT count(*) FROM {full_table}"),
                   ("chronological100", f"SELECT ordinal FROM {full_table} ORDER BY created_at DESC,id LIMIT 100"),
                   ("kind_count", f"SELECT kind,count(*) FROM {full_table} GROUP BY kind ORDER BY kind")]
        for sample in samples[:8]:
            ident = ("from_hex(" if con else "unhex(") + literal(sample["id"]) + ")"
            size = "octet_length(encode(content))" if con else "length(content)"
            queries.append(("id_content_length", f"SELECT ordinal,{size} FROM {full_table} WHERE id={ident}"))
        for name, sql in queries:
            started = time.monotonic()
            if con:
                rows = con.execute(sql).fetchall()
                measurement = {"seconds": time.monotonic() - started}
            else:
                response, measurement = ch(sql + " FORMAT JSONCompact")
                rows = json.loads(response)["data"]
            if name == "count":
                assert int(rows[0][0]) == manifest["rows"]
            if name == "id_content_length":
                assert len(rows) == 1 and int(rows[0][1]) >= manifest["minimum_content_bytes"]
            report["queries"].append({"name": name, "repetition": repetition,
                                      "result_rows": len(rows), **measurement})
            save(result, report)
    if con:
        assert con.execute(f"SELECT sum(rows) FROM lake.{table}_receipts").fetchone()[0] == manifest["rows"]
        report["files"] = con.execute("SELECT count(*),sum(data_file_size_bytes) FROM ducklake_list_files('lake', ?)" , [table]).fetchall()
        con.close()
    else:
        report["parts"] = json.loads(ch("SELECT count() AS parts,sum(rows) AS rows,sum(bytes_on_disk) AS bytes "
            f"FROM system.parts WHERE active AND database='default' AND table={literal(table)} FORMAT JSONCompact")[0])["data"]
        report["projection_parts"] = json.loads(ch("SELECT count() AS parts,sum(bytes_on_disk) AS bytes "
            f"FROM system.projection_parts WHERE active AND database='default' AND table={literal(table)} FORMAT JSONCompact")[0])["data"]
    assert len(report["batches"]) == report["required_cases"]["batches"]
    assert len(report["queries"]) == report["required_cases"]["queries"]
    report["status"] = "PASS_row_count_and_bounded_access_queries"
    save(result, report)
    print(json.dumps({k: report[k] for k in ("engine", "rows", "ingest_seconds", "status")}), flush=True)


if __name__ == "__main__":
    main()
