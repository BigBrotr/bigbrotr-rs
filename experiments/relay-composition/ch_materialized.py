"""Three real-ClickHouse MV counterexamples; stdlib HTTP, isolated database only.

The coordinator runs this against an already-started disposable ClickHouse.
This script starts no service and does not measure product performance.
"""
import argparse
import hashlib
import json
from pathlib import Path
import time
from urllib.error import HTTPError
from urllib.parse import urlencode, urlsplit
from urllib.request import Request, urlopen
import uuid


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default="http://127.0.0.1:48723")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--keep-database", action="store_true")
    args = parser.parse_args()
    endpoint = urlsplit(args.url)
    if endpoint.scheme != "http" or endpoint.hostname not in ("127.0.0.1", "localhost", "::1"):
        parser.error("use an explicitly local disposable ClickHouse HTTP endpoint")
    if endpoint.query or endpoint.fragment or endpoint.username or endpoint.path not in ("", "/"):
        parser.error("URL must contain only local scheme, host and port")
    args.output.mkdir(parents=True, exist_ok=False)
    database = "relay_composition_mv_" + uuid.uuid4().hex
    transcript = []
    report = {
        "status": "FAIL",
        "scope": "Three bounded engine-semantics counterexamples with literal expected answers; not product correctness, throughput, crash recovery or MV rejection in general.",
        "database": database,
        "endpoint": args.url,
        "source_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "probes": {},
        "not_executed": ["Rust product", "external relay", "rank solver", "durable derived-state implementation", "capacity benchmark"],
    }
    created = False

    def query(sql, *, rows=False, settings=None):
        wire = sql + (" FORMAT JSONEachRow" if rows else "")
        parameters = {"wait_end_of_query": "1", **(settings or {})}
        request = Request(args.url.rstrip("/") + "/?" + urlencode(parameters),
                          data=wire.encode(), method="POST")
        try:
            with urlopen(request, timeout=10) as response:
                raw = response.read().decode()
                exception_code = response.headers.get("X-ClickHouse-Exception-Code")
                http_status = response.status
        except HTTPError as exc:
            body = exc.read().decode(errors="replace")
            transcript.append({"sql": wire, "settings": parameters, "error": body})
            raise RuntimeError(f"ClickHouse HTTP {exc.code}: {body}") from exc
        transcript.append({"sql": wire, "settings": parameters, "response": raw,
                           "http_status": http_status, "exception_code": exception_code})
        if exception_code not in (None, "0"):
            raise RuntimeError(f"ClickHouse exception {exception_code} despite HTTP {http_status}: {raw}")
        if not rows and raw.strip():
            raise RuntimeError(f"Expected empty successful DDL/INSERT response, received: {raw}")
        return [json.loads(line) for line in raw.splitlines() if line] if rows else raw

    def scalar(sql, key):
        return query(sql, rows=True)[0][key]

    def expect(name, actual, expected):
        report["probes"][name] = {"actual": actual, "expected": expected, "pass": actual == expected}
        if actual != expected:
            raise AssertionError(f"{name}: {actual!r} != independent literal {expected!r}")

    try:
        report["clickhouse_version"] = scalar("SELECT version() AS v", "v")
        query(f"CREATE DATABASE {database}")
        created = True

        # Independent literal: one event identity remains one logical event.
        query(f"CREATE TABLE {database}.replay_source (event_id String, revision UInt64) ENGINE=ReplacingMergeTree(revision) ORDER BY event_id")
        query(f"CREATE TABLE {database}.replay_total (bucket UInt8, n UInt64) ENGINE=SummingMergeTree ORDER BY bucket")
        query(f"CREATE MATERIALIZED VIEW {database}.replay_mv TO {database}.replay_total AS SELECT toUInt8(1) AS bucket, count() AS n FROM {database}.replay_source")
        for _ in range(2):
            query(f"INSERT INTO {database}.replay_source VALUES ('event-a', 1)", settings={"insert_deduplicate": "0"})
        query(f"OPTIMIZE TABLE {database}.replay_source FINAL")
        actual = {
            "logical_events_after_merge": int(scalar(f"SELECT count() AS n FROM {database}.replay_source FINAL", "n")),
            "naive_mv_events": int(scalar(f"SELECT sum(n) AS n FROM {database}.replay_total", "n")),
        }
        expect("replay_not_retracted_by_replacing_merge", actual,
               {"logical_events_after_merge": 1, "naive_mv_events": 2})

        # Updating the right-side dimension without a left insert changes the
        # reference SELECT but cannot retroactively rewrite its old MV output.
        query(f"CREATE TABLE {database}.join_dimension (author_id UInt8, label String) ENGINE=MergeTree ORDER BY author_id")
        query(f"CREATE TABLE {database}.join_source (event_id String, author_id UInt8) ENGINE=MergeTree ORDER BY event_id")
        query(f"CREATE TABLE {database}.join_result (event_id String, label String) ENGINE=MergeTree ORDER BY event_id")
        query(f"INSERT INTO {database}.join_dimension VALUES (1, 'before')")
        join_select = f"SELECT s.event_id AS event_id, d.label AS label FROM {database}.join_source AS s INNER JOIN {database}.join_dimension AS d ON s.author_id=d.author_id"
        query(f"CREATE MATERIALIZED VIEW {database}.join_mv TO {database}.join_result AS {join_select}")
        query(f"INSERT INTO {database}.join_source VALUES ('event-b', 1)")
        query(f"TRUNCATE TABLE {database}.join_dimension")
        query(f"INSERT INTO {database}.join_dimension VALUES (1, 'after')")
        actual = {
            "full_select": query(join_select, rows=True),
            "prior_mv_output": query(f"SELECT event_id, label FROM {database}.join_result", rows=True),
        }
        expect("right_join_change_needs_invalidation", actual,
               {"full_select": [{"event_id": "event-b", "label": "after"}],
                "prior_mv_output": [{"event_id": "event-b", "label": "before"}]})

        # An actual wall-clock boundary passes with no new inserts. No TTL is
        # configured: this is query visibility by expires_at, not TTL scheduling.
        query(f"CREATE TABLE {database}.expiry_source (event_id String, expires_at DateTime64(3)) ENGINE=MergeTree ORDER BY event_id")
        query(f"CREATE TABLE {database}.expiry_total (bucket UInt8, n UInt64) ENGINE=SummingMergeTree ORDER BY bucket")
        query(f"CREATE MATERIALIZED VIEW {database}.expiry_mv TO {database}.expiry_total AS SELECT toUInt8(1) AS bucket, count() AS n FROM {database}.expiry_source WHERE expires_at > now64(3)")
        query(f"INSERT INTO {database}.expiry_source SELECT 'event-c', now64(3) + INTERVAL 2 SECOND")
        initial = int(scalar(f"SELECT sum(n) AS n FROM {database}.expiry_total", "n"))
        if initial != 1:
            raise AssertionError("expiry fixture did not initially contribute exactly one event")
        started = time.monotonic()
        polls = 0
        while True:
            polls += 1
            remaining = int(scalar(f"SELECT count() AS n FROM {database}.expiry_source WHERE expires_at > now64(3)", "n"))
            if remaining == 0:
                break
            if time.monotonic() - started > 8:
                raise AssertionError("expiry clock boundary not observed within bounded polling")
            time.sleep(0.05)
        actual = {
            "initial_mv_events": initial,
            "current_full_select_events": remaining,
            "unchanged_mv_events": int(scalar(f"SELECT sum(n) AS n FROM {database}.expiry_total", "n")),
        }
        expect("clock_expiry_needs_scheduled_retraction", actual,
               {"initial_mv_events": 1, "current_full_select_events": 0, "unchanged_mv_events": 1})
        report["expiry_polling"] = {"queries": polls, "elapsed_seconds": time.monotonic() - started,
                                    "maximum_single_sleep_seconds": 0.05,
                                    "interpretation": "Fixture orchestration only, not a latency benchmark"}
        report["status"] = "PASS"
    except Exception as exc:
        report["error"] = f"{type(exc).__name__}: {exc}"
    finally:
        if created and not args.keep_database:
            try:
                query(f"DROP DATABASE {database}")
                report["database_removed"] = True
            except Exception as exc:
                report["database_removed"] = False
                report["cleanup_error"] = str(exc)
                report["status"] = "FAIL"
        else:
            report["database_removed"] = not created
        (args.output / "queries.json").write_text(json.dumps(transcript, indent=2) + "\n")
        report["transcript_sha256"] = hashlib.sha256((args.output / "queries.json").read_bytes()).hexdigest()
        (args.output / "result.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report, indent=2))
    return 0 if report["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
