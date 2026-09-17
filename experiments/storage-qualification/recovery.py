#!/usr/bin/env python3
"""Owned, bounded real-engine recovery slice; never restarts an engine server.

All writes use a fresh qualification_recovery_<run-id> namespace. Existing aged
tables are read neither for recovery nor for mutation. See README-recovery.md.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
import math
import multiprocessing as mp
from pathlib import Path
import re
import time
import traceback
import uuid

import duckdb
import requests


WRITE_MEMORY = 3_000_000_000
READ_MEMORY = 1_500_000_000
FIELDS = ("ordinal", "id", "pubkey", "created_at", "kind", "tags", "content", "sig")
BYTE_FIELDS = {"id", "pubkey", "sig"}
PG_DEFAULT = "dbname=qualification host=127.0.0.1 port=48632 user=postgres"


def sql_string(value):
    return "'" + str(value).replace("'", "''") + "'"


def digest(value):
    return hashlib.sha256(json.dumps(value, ensure_ascii=False, sort_keys=True,
                                     separators=(",", ":")).encode()).hexdigest()


def normalized(row):
    result = {name: row[name] for name in FIELDS}
    for name in BYTE_FIELDS:
        if isinstance(result[name], bytes):
            result[name] = result[name].hex()
        result[name] = result[name].lower()
    for name in ("ordinal", "created_at", "kind"):
        result[name] = int(result[name])
    return result


def write_report(path, report):
    pending = path.with_suffix(".pending.json")
    pending.write_text(json.dumps(report, indent=2) + "\n")
    pending.replace(path)


def application_name(config, label):
    # PostgreSQL truncates application_name to NAMEDATALEN-1. Keep it exact.
    return "qr_" + hashlib.sha256(config["namespace"].encode()).hexdigest()[:16] + "_" + label


def lake(config, memory=WRITE_MEMORY, threads=2, application="reader"):
    # Extensions must already exist: no INSTALL/network download in fault tests.
    con = duckdb.connect(config={"threads": str(threads), "memory_limit": f"{memory}B",
        "extension_directory": str(Path(config["root"]) / "extensions"),
        "temp_directory": str(Path(config["root"]) / "tmp" / config["namespace"] / application),
        "max_temp_directory_size": "4GB"})
    con.execute("LOAD ducklake")
    con.execute("LOAD postgres")
    dsn = config["pg_dsn"] + " application_name=" + application_name(config, application)
    con.execute("ATTACH " + sql_string("ducklake:postgres:" + dsn) + " AS lake "
                "(DATA_PATH " + sql_string(Path(config["root"]) / "data") +
                ", DATA_INLINING_ROW_LIMIT 0)")
    return con


def pg(config, application="control"):
    import psycopg2
    return psycopg2.connect(config["pg_dsn"], connect_timeout=10,
                           application_name=application_name(config, application),
                           options="-c statement_timeout=30000 -c lock_timeout=10000")


def ch_request(config, sql, memory=WRITE_MEMORY, threads=2, stream=False):
    params = {"query_id": config["namespace"] + "_" + uuid.uuid4().hex,
              "max_threads": threads, "max_memory_usage": memory,
              "max_execution_time": 120, "wait_end_of_query": 1,
              "http_response_buffer_size": 1_048_576, "insert_deduplicate": 0}
    response = requests.post(config["ch_url"], params=params, data=sql.encode(),
                             stream=stream, timeout=(10, 150))
    if response.status_code != 200 or response.headers.get("X-ClickHouse-Exception-Code", "0") != "0":
        message = response.text[:2000]
        response.close()
        raise RuntimeError("ClickHouse query failed: " + message)
    return response


def ch_command(config, sql, memory=WRITE_MEMORY, threads=2):
    response = ch_request(config, sql, memory, threads)
    try:
        # HTTP 200 with a late exception is not a successful INSERT/DDL.
        if response.text.strip():
            raise RuntimeError("nonempty INSERT/DDL response: " + response.text[:2000])
    finally:
        response.close()


def ch_rows(config, sql, memory=WRITE_MEMORY, threads=2):
    response = ch_request(config, sql + " FORMAT JSONEachRow", memory, threads, stream=True)
    try:
        for line in response.iter_lines():
            if line:
                value = json.loads(line)  # late non-JSON exceptions fail the run.
                if not isinstance(value, dict) or "exception" in value:
                    raise RuntimeError("exception/invalid row in ClickHouse stream")
                yield value
    finally:
        response.close()


def body_fields():
    return ",".join("lower(hex(" + key + ")) AS " + key if key in BYTE_FIELDS else key
                    for key in FIELDS)


def create_ch_body(config, table):
    ch_command(config, f"CREATE TABLE {config['namespace']}.{table}(ordinal UInt64,id FixedString(32),pubkey FixedString(32),created_at Int64,kind UInt32,tags Array(Array(String)),content String CODEC(ZSTD(3)),sig FixedString(64)) ENGINE=MergeTree ORDER BY (id,ordinal) SETTINGS fsync_after_insert=1,fsync_part_directory=1")


def duck_rows(con, sql):
    cursor = con.execute(sql)
    names = [column[0] for column in cursor.description]
    while True:
        rows = cursor.fetchmany(256)
        if not rows:
            break
        for row in rows:
            yield dict(zip(names, row))


def source_oracle(path, expected_rows):
    import pyarrow.parquet as pq
    from coincurve import PublicKeyXOnly
    file_hash = hashlib.sha256()
    with path.open("rb") as source:
        while block := source.read(1024 * 1024):
            file_hash.update(block)
    parquet = pq.ParquetFile(path)
    if parquet.metadata.num_rows != expected_rows or not 0 < expected_rows <= 65536:
        raise ValueError("source must match manifest and contain 1..65536 rows")
    bodies, tags, sample_rows = {}, {}, []
    for batch in parquet.iter_batches(batch_size=256):
        for item in batch.to_pylist():
            row = normalized(item)
            canonical = [0, row["pubkey"], row["created_at"], row["kind"], row["tags"], row["content"]]
            event_id = hashlib.sha256(json.dumps(canonical, ensure_ascii=False,
                                                 separators=(",", ":")).encode()).digest()
            if event_id.hex() != row["id"] or not PublicKeyXOnly(bytes.fromhex(row["pubkey"])).verify(
                    bytes.fromhex(row["sig"]), event_id):
                raise AssertionError("invalid complete signed source row")
            if row["ordinal"] in bodies:
                raise AssertionError("duplicate source ordinal")
            bodies[row["ordinal"]] = digest(row)
            for position, value in enumerate(row["tags"]):
                tags[(row["ordinal"], position)] = digest({"id": row["id"], "value": value})
            if len(sample_rows) < 16:
                sample_rows.append(row)
    if len(bodies) != expected_rows:
        raise AssertionError("truncated source")
    return {"bodies": bodies, "tags": tags, "samples": sample_rows,
            "file_sha256": file_hash.hexdigest(), "rows": expected_rows,
            "source_digest": digest(sorted(bodies.items())), "ordinals": sorted(bodies)}


def validate_bodies(rows, expected, allow_duplicates=False):
    seen, physical = set(), 0
    for item in rows:
        row = normalized(item)
        ordinal = row["ordinal"]
        if ordinal not in expected or digest(row) != expected[ordinal]:
            raise AssertionError("complete signed body differs from independent source")
        if ordinal in seen and not allow_duplicates:
            raise AssertionError("unexpected duplicate body")
        seen.add(ordinal)
        physical += 1
    if seen != set(expected):
        raise AssertionError("missing body identities")
    return {"logical": len(seen), "physical": physical}


def validate_tags(rows, expected, allow_duplicates=False):
    seen, physical = set(), 0
    for row in rows:
        key = (int(row["ordinal"]), int(row["tag_position"]))
        event_id = row["id"].hex() if isinstance(row["id"], bytes) else row["id"].lower()
        if key not in expected or digest({"id": event_id, "value": row["value"]}) != expected[key]:
            raise AssertionError("tag family differs from ordered source tags")
        if key in seen and not allow_duplicates:
            raise AssertionError("unexpected duplicate tag")
        seen.add(key)
        physical += 1
    if seen != set(expected):
        raise AssertionError("missing required tag family")
    return {"logical": len(seen), "physical": physical}


def verify_lake(config, oracle, empty=False):
    con = lake(config, application="verify")
    ns = "lake." + config["namespace"]
    try:
        bodies = validate_bodies(duck_rows(con, f"SELECT * FROM {ns}.body"), {} if empty else oracle["bodies"])
        tags = validate_tags(duck_rows(con, f"SELECT * FROM {ns}.tags"), {} if empty else oracle["tags"])
        receipts = con.execute(f"SELECT batch_id,file_sha256,source_digest,rows FROM {ns}.receipts").fetchall()
        expected = [] if empty else [("fixture", oracle["file_sha256"], oracle["source_digest"], oracle["rows"])]
        if receipts != expected:
            raise AssertionError("archive all-family receipt mismatch")
        return {"bodies": bodies, "tags": tags, "receipt_count": len(receipts)}
    finally:
        con.close()


def verify_ch(config, oracle):
    ns = config["namespace"]
    bodies = validate_bodies(ch_rows(config, f"SELECT {body_fields()} FROM {ns}.body"),
                             oracle["bodies"], allow_duplicates=True)
    tags = validate_tags(ch_rows(config, f"SELECT ordinal,lower(hex(id)) AS id,tag_position,value FROM {ns}.tags"),
                         oracle["tags"], allow_duplicates=True)
    return {"bodies": bodies, "tags": tags}


def child_commit(config, point, oracle_summary, pipe):
    """Signals a real boundary then waits to be killed; never closes gracefully."""
    if point == "pg-after-commit":
        con = pg(config, "activation")
        with con.cursor() as cursor:
            cursor.execute(f"UPDATE {config['namespace']}.frontier SET generation=1,source_digest=%s "
                           "WHERE singleton=TRUE AND generation=0 RETURNING generation",
                           (oracle_summary["source_digest"],))
            if cursor.fetchone() != (1,):
                raise AssertionError("activation compare-and-swap lost")
        con.commit()
    else:
        con = lake(config, application=point)
        ns = "lake." + config["namespace"]
        con.execute("BEGIN")
        con.execute(f"INSERT INTO {ns}.body SELECT * FROM read_parquet({sql_string(config['source'])})")
        con.execute(f"INSERT INTO {ns}.tags SELECT ordinal,id,position-1 AS tag_position,value "
                    f"FROM {ns}.body, UNNEST(tags) WITH ORDINALITY AS t(value,position)")
        con.execute(f"INSERT INTO {ns}.receipts VALUES (?,?,?,?)",
                    ["fixture", oracle_summary["file_sha256"], oracle_summary["source_digest"], oracle_summary["rows"]])
        if point == "lake-after-commit":
            con.execute("COMMIT")
    pipe.send({"boundary": point, "pid": mp.current_process().pid})
    while True:
        time.sleep(30)


def kill_at_boundary(config, point, oracle):
    context = mp.get_context("spawn")
    receive, send = context.Pipe(duplex=False)
    summary = {key: oracle[key] for key in ("file_sha256", "source_digest", "rows")}
    process = context.Process(target=child_commit, args=(config, point, summary, send))
    process.start()
    send.close()
    try:
        if not receive.poll(180):
            raise RuntimeError("child did not reach requested real commit boundary")
        message = receive.recv()
        app = application_name(config, "activation" if point == "pg-after-commit" else point)
        inspection = pg(config, "pre-kill-inspection")
        try:
            with inspection.cursor() as cursor:
                cursor.execute("SELECT pid FROM pg_stat_activity WHERE application_name=%s", (app,))
                message["pg_backend_pids_before_kill"] = [row[0] for row in cursor.fetchall()]
            if point == "lake-before-commit" and not message["pg_backend_pids_before_kill"]:
                raise RuntimeError("cannot identify the active catalog transaction before kill")
        finally:
            inspection.close()
        process.kill()
        process.join(15)
        if process.is_alive() or process.exitcode is None or process.exitcode >= 0:
            raise RuntimeError("positive child-process termination not established")
        # A healthy absent receipt is insufficient while its original PG session lives.
        deadline = time.monotonic() + 60
        con = pg(config, "session-inspection")
        try:
            while True:
                with con.cursor() as cursor:
                    cursor.execute("SELECT count(*) FROM pg_stat_activity WHERE application_name=%s",
                                   (app,))
                    alive = cursor.fetchone()[0]
                con.commit()  # refresh statistics snapshot and do not retain transaction.
                if alive == 0:
                    break
                if time.monotonic() >= deadline:
                    raise RuntimeError("outcome unknown: old PG session still present; no retry authorized")
                time.sleep(0.1)
        finally:
            con.close()
        return {**message, "exitcode": process.exitcode, "old_pg_sessions": 0}
    finally:
        if process.is_alive():
            process.kill()
            process.join(15)
        receive.close()


def frontier(config):
    con = pg(config)
    try:
        with con.cursor() as cursor:
            cursor.execute(f"SELECT generation,source_digest FROM {config['namespace']}.frontier WHERE singleton=TRUE")
            return cursor.fetchone()
    finally:
        con.close()


def mixed_worker(config, engine, role, duration, start_at, rows, pipe):
    """One writer + two readers; 3GB+1.5GB+1.5GB, never 7GB each."""
    writer = role == "writer"
    memory, threads = (WRITE_MEMORY, 2) if writer else (READ_MEMORY, 1)
    stats = {"role": role, "engine": engine, "memory_limit": memory, "threads": threads,
             "offered": math.ceil(duration), "started": 0, "completed": 0, "not_started": 0,
             "errors": [], "latency_seconds": [], "queue_seconds": [], "written_rows": 0}
    con = None
    try:
        con = lake(config, memory, threads, role) if engine == "lake" else None
        table = ("lake." if con else "") + config["namespace"] + ".mixed_body"
        ticks = math.ceil(duration)
        width = max(1, len(rows["ordinals"]) // ticks)
        for tick in range(ticks):
            due = start_at + tick
            if time.monotonic() < due:
                time.sleep(due - time.monotonic())
            if time.monotonic() >= start_at + duration:
                stats["not_started"] = ticks - tick
                break
            selected = rows["ordinals"][tick*width:min((tick+1)*width, len(rows["ordinals"]))]
            if writer and not selected:
                stats["not_started"] = ticks - tick
                break
            stats["queue_seconds"].append(max(0, time.monotonic() - due))
            stats["started"] += 1
            started = time.monotonic()
            if writer:
                identifiers = ",".join(map(str, selected))
                source = (f"read_parquet({sql_string(config['source'])})" if con else
                          f"file({sql_string(config['ch_source'])},Parquet)")
                sql = f"INSERT INTO {table} SELECT * FROM {source} WHERE ordinal IN ({identifiers})"
                if con:
                    con.execute(sql)
                else:
                    ch_command(config, sql, memory, threads)
                stats["written_rows"] += len(selected)
            else:
                expected = rows["samples"][tick % len(rows["samples"])]
                binary = "from_hex" if con else "unhex"
                # CH otherwise substitutes SELECT's hex alias into WHERE id=...
                sql = f"SELECT DISTINCT {body_fields()} FROM {table} AS stored WHERE stored.id={binary}({sql_string(expected['id'])})"
                actual = list(duck_rows(con, sql)) if con else list(ch_rows(config, sql, memory, threads))
                validate_bodies(actual, {expected["ordinal"]: digest(expected)})
            stats["completed"] += 1
            stats["latency_seconds"].append(time.monotonic() - started)
    except Exception as error:
        stats["errors"].append({"type": type(error).__name__, "message": str(error)[:2000]})
        stats["not_started"] = stats["offered"] - stats["started"]
    finally:
        # Preserve quota accounting even when an external SIGINT interrupts sleep.
        stats["not_started"] = stats["offered"] - stats["started"]
        if con:
            con.close()
        pipe.send(stats)
        pipe.close()


def run_mixed(config, oracle, engine, duration):
    context = mp.get_context("spawn")
    processes, receivers = [], []
    start_at = time.monotonic() + 5
    payload = {key: oracle[key] for key in ("samples", "ordinals")}
    for role in ("writer", "reader-a", "reader-b"):
        receive, send = context.Pipe(duplex=False)
        process = context.Process(target=mixed_worker,
            args=(config, engine, role, duration, start_at, payload, send))
        process.start()
        send.close()
        processes.append(process)
        receivers.append(receive)
    deadline = start_at + duration + 180
    reports = []
    try:
        for receive in receivers:
            if not receive.poll(max(0, deadline - time.monotonic())):
                raise RuntimeError("mixed worker failed to join within bounded drain")
            reports.append(receive.recv())
        for process in processes:
            process.join(15)
            if process.is_alive() or process.exitcode != 0:
                raise RuntimeError("mixed worker process failed")
        if any(report["errors"] for report in reports):
            raise RuntimeError("mixed worker error; inspect structured worker reports")
        if any(report["completed"] == 0 for report in reports):
            raise AssertionError("selected mixed role executed no complete required operation")
        written = next(report["written_rows"] for report in reports if report["role"] == "writer")
        if written > oracle["rows"]:
            raise AssertionError("mixed writer exceeded one extra source-file bound")
        table = ("lake." if engine == "lake" else "") + config["namespace"] + ".mixed_body"
        con = lake(config, application="mixed-verify") if engine == "lake" else None
        try:
            actual = duck_rows(con, f"SELECT * FROM {table}") if con else ch_rows(config, f"SELECT {body_fields()} FROM {table}")
            verified = validate_bodies(actual, oracle["bodies"], allow_duplicates=True)
            if verified["physical"] != oracle["rows"] + written:
                raise AssertionError("mixed physical/logical body count mismatch")
        finally:
            if con:
                con.close()
        return {"duration_seconds": duration, "workers": reports, "final_body_oracle": verified,
                "scope": "fixed offered one operation/worker/second; no capacity/SLO claim; cache uncontrolled"}
    except BaseException as error:
        # Do not bury/truncate per-role outcomes inside one exception string.
        error.mixed_reports = {"engine": engine, "duration_seconds": duration, "workers": reports}
        raise
    finally:
        for process in processes:
            if process.is_alive():
                process.kill()
            process.join(15)
        for receive in receivers:
            receive.close()


def self_test():
    row = {"ordinal": 1, "id": "ab"*32, "pubkey": "cd"*32, "created_at": 10,
           "kind": 1, "tags": [["t", "x"]], "content": "same length", "sig": "ef"*64}
    expected = {1: digest(row)}
    validate_bodies([row], expected)
    caught = 0
    for values in ([dict(row, content="Same length")], [], [row, row]):
        try:
            validate_bodies(values, expected)
        except AssertionError:
            caught += 1
    if caught != 3:
        raise AssertionError("body corruption/missing/duplicate negative control escaped")
    validate_bodies([row, row], expected, allow_duplicates=True)
    if WRITE_MEMORY + 2*READ_MEMORY > 7_000_000_000:
        raise AssertionError("mixed aggregate memory configuration exceeds 7GB")
    print(json.dumps({"status": "PASS_local_oracle_self_test", "negative_controls": caught,
                      "engine_execution": "NOT_RUN"}))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path("/lab"))
    parser.add_argument("--corpus", choices=("calibration", "aged"), default="calibration")
    parser.add_argument("--file", help="One basename already listed in the selected corpus manifest")
    parser.add_argument("--run-id", default=uuid.uuid4().hex[:12])
    parser.add_argument("--pg-dsn", default=PG_DEFAULT)
    parser.add_argument("--ch-url", default="http://127.0.0.1:48623/")
    parser.add_argument("--mixed", action="store_true")
    parser.add_argument("--ch-mixed-only", action="store_true",
                        help="fresh CH namespace and mixed slice only; no archive/crash cases")
    parser.add_argument("--mixed-duration", type=int, default=60, help="60s calibration; select 600s for ten-minute slice")
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()
    if args.self_test:
        self_test()
        return
    if not re.fullmatch(r"[a-z0-9_]{1,24}", args.run_id) or not 1 <= args.mixed_duration <= 600:
        parser.error("run-id must be 1..24 lowercase ASCII letters/digits/underscores; duration 1..600")
    namespace = "qualification_recovery_" + args.run_id
    manifest_path = args.root / "input" / args.corpus / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    files = sorted((item for part in manifest["partitions"] for item in part["files"]), key=lambda item: item["path"])
    name = args.file or files[0]["path"]
    matches = [item for item in files if item["path"] == name]
    if Path(name).name != name or len(matches) != 1 or not 0 < matches[0]["rows"] <= 65536:
        raise ValueError("choose one unambiguous manifest-listed basename with 1..65536 rows")
    source = (manifest_path.parent / name).resolve()
    if source.parent != manifest_path.parent.resolve():
        raise ValueError("source escapes declared input root")
    config = {"root": str(args.root.resolve()), "namespace": namespace, "source": str(source),
              "ch_source": f"input/{args.corpus}/{name}", "pg_dsn": args.pg_dsn, "ch_url": args.ch_url}
    result = args.root / "results" / (namespace + ".json")
    if result.exists():
        raise ValueError("run report already exists; preserve old evidence and choose a new run-id")
    result.parent.mkdir(parents=True, exist_ok=True)
    report = {"status": "RUNNING", "namespace": namespace, "source": name,
              "started_at_utc": datetime.now(timezone.utc).isoformat(),
              "source_script_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
              "manifest_sha256": hashlib.sha256(manifest_path.read_bytes()).hexdigest(),
              "cases": {}, "scope": "real client/worker process kill, transaction/partial-family/replay slice; no server kill, power/host loss, fencing or production adapter proof",
              "resource_settings": {"writer_memory_bytes": WRITE_MEMORY, "reader_memory_bytes": READ_MEMORY,
                                    "mixed_concurrency": "one writer and two readers, one engine at a time", "mixed_engine_total_bytes": 6_000_000_000}}
    write_report(result, report)
    try:
        oracle = source_oracle(source, matches[0]["rows"])
        report["source_identity"] = {key: oracle[key] for key in ("file_sha256", "source_digest", "rows")}
        if args.ch_mixed_only:
            report["scope"] = "fresh CH namespace, bounded mixed correctness slice only; no archive/crash cases or capacity/SLO qualification"
            ch_command(config, f"CREATE DATABASE {namespace}")
            report["clickhouse_version"] = list(ch_rows(config, "SELECT version() AS version"))[0]["version"]
            create_ch_body(config, "mixed_body")
            ch_command(config, f"INSERT INTO {namespace}.mixed_body SELECT * FROM file({sql_string(config['ch_source'])},Parquet)")
            write_report(result, report)
            report["cases"]["mixed-ch"] = run_mixed(config, oracle, "ch", args.mixed_duration)
            report["status"] = "PASS_bounded_ch_mixed_slice"
            report["completed_at_utc"] = datetime.now(timezone.utc).isoformat()
            report["retained_resources"] = {"clickhouse_database": namespace, "cleanup": "None automatic"}
            write_report(result, report)
            print(json.dumps({"status": report["status"], "report": str(result), "namespace": namespace}))
            return
        con = pg(config)
        try:
            with con.cursor() as cursor:
                # No IF NOT EXISTS: accidental namespace reuse refuses before data writes.
                cursor.execute(f"CREATE SCHEMA {namespace}")
                cursor.execute(f"CREATE TABLE {namespace}.frontier(singleton BOOLEAN PRIMARY KEY CHECK(singleton),generation BIGINT NOT NULL,source_digest TEXT)")
                cursor.execute(f"INSERT INTO {namespace}.frontier VALUES(TRUE,0,NULL)")
            con.commit()
        finally:
            con.close()
        con = lake(config, application="setup")
        try:
            report["duckdb_version"] = duckdb.__version__
            report["extension_versions"] = con.execute("SELECT extension_name,extension_version FROM duckdb_extensions() WHERE extension_name IN ('ducklake','postgres','postgres_scanner')").fetchall()
            con.execute(f"CREATE SCHEMA lake.{namespace}")
            con.execute(f"CREATE TABLE lake.{namespace}.body AS SELECT * FROM read_parquet({sql_string(source)}) LIMIT 0")
            con.execute(f"CREATE TABLE lake.{namespace}.tags(ordinal UBIGINT,id BLOB,tag_position UBIGINT,value VARCHAR[])")
            con.execute(f"CREATE TABLE lake.{namespace}.receipts(batch_id VARCHAR,file_sha256 VARCHAR,source_digest VARCHAR,rows BIGINT)")
        finally:
            con.close()
        report["cases"]["lake-kill-before-commit"] = kill_at_boundary(config, "lake-before-commit", oracle)
        report["cases"]["lake-kill-before-commit"]["oracle"] = verify_lake(config, oracle, empty=True)
        write_report(result, report)
        report["cases"]["lake-kill-after-commit-before-caller-ack"] = kill_at_boundary(config, "lake-after-commit", oracle)
        report["cases"]["lake-kill-after-commit-before-caller-ack"]["oracle"] = verify_lake(config, oracle)
        write_report(result, report)
        ch_command(config, f"CREATE DATABASE {namespace}")
        report["clickhouse_version"] = list(ch_rows(config, "SELECT version() AS version"))[0]["version"]
        create_ch_body(config, "body")
        ch_command(config, f"CREATE TABLE {namespace}.tags(ordinal UInt64,id FixedString(32),tag_position UInt32,value Array(String)) ENGINE=MergeTree ORDER BY (id,tag_position) SETTINGS fsync_after_insert=1,fsync_part_directory=1")
        insert = f"INSERT INTO {namespace}.body SELECT * FROM file({sql_string(config['ch_source'])},Parquet)"
        ch_command(config, insert)
        try:
            verify_ch(config, oracle)
        except AssertionError as error:
            if str(error) != "missing required tag family":
                raise
        else:
            raise AssertionError("partial-family negative control was accepted")
        if frontier(config) != (0, None):
            raise AssertionError("partial family advanced PG frontier")
        tag_insert = f"INSERT INTO {namespace}.tags SELECT ordinal,id,toUInt32(position-1) AS tag_position,value FROM file({sql_string(config['ch_source'])},Parquet) ARRAY JOIN arrayEnumerate(tags) AS position,tags AS value"
        ch_command(config, tag_insert)
        report["cases"]["ch-partial-family"] = {"old_frontier": 0, "missing-family-detected": True,
                                                       "complete_oracle": verify_ch(config, oracle)}
        # Force exact replay irrespective of engine token retention.
        ch_command(config, insert)
        ch_command(config, tag_insert)
        replay = verify_ch(config, oracle)
        if replay["bodies"]["physical"] != 2*oracle["rows"] or replay["tags"]["physical"] != 2*len(oracle["tags"]):
            raise AssertionError("dedup-disabled replay did not produce the expected physical copies")
        report["cases"]["ch-dedup-disabled-replay"] = replay
        report["cases"]["pg-activation-response-lost"] = kill_at_boundary(config, "pg-after-commit", oracle)
        if frontier(config) != (1, oracle["source_digest"]):
            raise AssertionError("committed PG frontier was not resolved")
        write_report(result, report)
        bad = dict(oracle["samples"][0])
        bad["content"] = "!" + bad["content"][1:]
        try:
            validate_bodies([bad], {bad["ordinal"]: oracle["bodies"][bad["ordinal"]]})
        except AssertionError:
            report["cases"]["same-length-corruption-negative"] = {"detected": True}
        else:
            raise AssertionError("same-length signed-body corruption escaped oracle")
        if args.mixed:
            for engine in ("lake", "ch"):
                if engine == "lake":
                    con = lake(config, application="mixed-setup")
                    try:
                        con.execute(f"CREATE TABLE lake.{namespace}.mixed_body AS SELECT * FROM lake.{namespace}.body")
                    finally:
                        con.close()
                else:
                    ch_command(config, f"CREATE TABLE {namespace}.mixed_body AS {namespace}.body")
                    ch_command(config, f"INSERT INTO {namespace}.mixed_body SELECT * FROM file({sql_string(config['ch_source'])},Parquet)")
                report["cases"]["mixed-" + engine] = run_mixed(config, oracle, engine, args.mixed_duration)
                write_report(result, report)
        report["status"] = "PASS_bounded_engine_recovery_slice"
        report["completed_at_utc"] = datetime.now(timezone.utc).isoformat()
        report["retained_resources"] = {"ducklake_schema": namespace, "clickhouse_database": namespace,
                                        "postgres_control_schema": namespace,
                                        "cleanup": "None automatic; inspect evidence and use explicit namespace-scoped cleanup."}
        write_report(result, report)
        print(json.dumps({"status": report["status"], "report": str(result), "namespace": namespace}))
    except BaseException as error:
        report["status"] = "FAILED"
        report["completed_at_utc"] = datetime.now(timezone.utc).isoformat()
        if hasattr(error, "mixed_reports"):
            report["mixed_failure"] = error.mixed_reports
        report["failure"] = {"type": type(error).__name__, "message": str(error)[:4000],
                             "traceback": traceback.format_exc()[-8000:]}
        write_report(result, report)
        raise


if __name__ == "__main__":
    main()
