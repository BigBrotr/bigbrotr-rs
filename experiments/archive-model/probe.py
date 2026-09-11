"""Isolated columnar fidelity and archival-semantics probe, not product code."""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import sys
from pathlib import Path

import duckdb


def digest(data: bytes) -> bytes:
    return hashlib.sha256(data).digest()


def read_events(path: Path) -> list[dict]:
    con = duckdb.connect()
    con.execute("SET threads=2")
    con.execute("SET memory_limit='256MB'")
    rows = con.execute(
        "SELECT event_id, pubkey, created_at, kind, tags, content, sig "
        "FROM read_parquet(?) ORDER BY created_at", [str(path)]
    ).fetchall()
    con.close()
    return [
        {"id": row[0].hex(), "pubkey": row[1].hex(), "created_at": row[2], "kind": row[3],
         "tags": row[4], "content": row[5], "sig": row[6].hex()}
        for row in rows
    ]


def copy_table(con, table: str, path: Path) -> None:
    # Table names are fixed below. DuckDB accepts a parameter for COPY's path.
    con.execute(f"COPY {table} TO ? (FORMAT PARQUET, COMPRESSION ZSTD)", [str(path)])


def run(output: Path) -> dict:
    output.mkdir(parents=True, exist_ok=False)
    con = duckdb.connect()
    con.execute("SET threads=2")
    con.execute("SET memory_limit='256MB'")
    con.execute("CREATE TABLE events(event_id BLOB, pubkey BLOB, created_at UBIGINT, "
                "kind USMALLINT, tags VARCHAR[][], content VARCHAR, sig BLOB)")
    fixtures = []
    for index, timestamp in enumerate([0, 2**53 + 1, 2**64 - 1]):
        event = {
            "id": digest(f"format-fixture-{index}".encode()).hex(),
            "pubkey": digest(f"format-author-{index}".encode()).hex(),
            "created_at": timestamp, "kind": [1, 30023, 65535][index],
            "tags": [["x"], ["x", ""], ["t", "a,b"], ["t", "a,b"],
                     ["d", "z"], ["d", "a"], ["p", "文字🙂"]],
            "content": json.dumps({"text": "quotes \" and newline\n", "n": index},
                                  ensure_ascii=False),
            "sig": bytes([index] * 64).hex(),
        }
        fixtures.append(event)
        con.execute("INSERT INTO events VALUES (?,?,?,?,?,?,?)",
                    [bytes.fromhex(event["id"]), bytes.fromhex(event["pubkey"]), timestamp, event["kind"],
                     event["tags"], event["content"], bytes.fromhex(event["sig"])])
    copy_table(con, "events", output / "events.parquet")
    con.close()
    # A fresh process reads just the exported file, without this connection/catalog.
    received = json.loads(subprocess.check_output(
        [sys.executable, str(Path(__file__).resolve()),
         "--read-events", str(output / "events.parquet")], text=True))
    required_fields = {"id", "pubkey", "created_at", "kind", "tags", "content", "sig"}
    assert all(set(event) == required_fields for event in received)
    assert received == fixtures
    assert int(float(fixtures[1]["created_at"])) != fixtures[1]["created_at"]
    assert sorted(fixtures[0]["tags"]) != fixtures[0]["tags"]

    con = duckdb.connect()
    con.execute("SET threads=2")
    con.execute("SET memory_limit='256MB'")
    con.execute("CREATE TABLE facts(source_seq UBIGINT, event_id VARCHAR, "
                "relay VARCHAR, origin VARCHAR, observed_at BIGINT)")
    facts = [(1, "E", "R1", "local", 100),
             (2, "E", "R2", "imported", 50),
             (3, "E", "R1", "local", 90)]
    con.executemany("INSERT INTO facts VALUES (?,?,?,?,?)", facts)
    copy_table(con, "facts", output / "facts.parquet")
    def local_min(cut: int):
        return con.execute(
            "SELECT min(observed_at) FROM read_parquet(?) "
            "WHERE source_seq <= ? AND origin='local' AND relay='R1'",
            [str(output / "facts.parquet"), cut]).fetchone()[0]
    before, after = local_min(1), local_min(3)
    assert (before, after) == (100, 90)
    # Wrong last-state compaction cannot answer the old cut at all.
    wrong_old = con.execute(
        "WITH latest AS (SELECT * FROM facts WHERE origin='local' "
        "QUALIFY row_number() OVER(PARTITION BY event_id,relay "
        "ORDER BY observed_at)=1) SELECT min(observed_at) FROM latest "
        "WHERE source_seq<=1").fetchone()[0]
    assert wrong_old is None
    wrong_origin = con.execute("SELECT min(observed_at) FROM facts").fetchone()[0]
    assert wrong_origin == 50 and wrong_origin != after

    con.execute("CREATE TABLE objects(scope VARCHAR, document_id BLOB, body BLOB)")
    con.execute("CREATE TABLE observations(attempt UBIGINT, scope VARCHAR, "
                "document_id BLOB, observed_at BIGINT, outcome VARCHAR)")
    con.execute("CREATE TABLE wide(attempt UBIGINT, scope VARCHAR, "
                "document_id BLOB, observed_at BIGINT, outcome VARCHAR, body BLOB)")
    raw_a = json.dumps({"name": "relay", "data": "unchanged " * 400}).encode()
    raw_b = json.dumps({"name": "relay-v2", "data": "changed " * 400}).encode()
    objects = {("public", digest(raw_a)): raw_a,
               ("public", digest(raw_b)): raw_b,
               ("private", digest(raw_a)): raw_a}
    con.executemany("INSERT INTO objects VALUES (?,?,?)",
                    [(scope, key, value) for (scope, key), value in objects.items()])
    observations = []
    wide = []
    for index in range(1000):
        scope = "private" if index >= 950 else "public"
        body = raw_b if 900 <= index < 950 else raw_a
        row = (index, scope, digest(body), 1_000_000 + index * 300, "success")
        observations.append(row)
        wide.append((*row, body))
    con.executemany("INSERT INTO observations VALUES (?,?,?,?,?)", observations)
    con.executemany("INSERT INTO wide VALUES (?,?,?,?,?,?)", wide)
    for table in ["objects", "observations", "wide"]:
        copy_table(con, table, output / f"{table}.parquet")
    restored = con.execute(
        "SELECT o.attempt,o.scope,o.document_id,o.observed_at,o.outcome,d.body "
        "FROM read_parquet(?) o JOIN read_parquet(?) d "
        "ON o.scope=d.scope AND o.document_id=d.document_id ORDER BY o.attempt",
        [str(output / "observations.parquet"), str(output / "objects.parquet")]
    ).fetchall()
    assert restored == wide
    wrong_join_count = con.execute(
        "SELECT count(*) FROM observations o JOIN objects d "
        "ON o.document_id=d.document_id").fetchone()[0]
    assert wrong_join_count > len(wide)
    sizes = {p.name: p.stat().st_size for p in output.glob("*.parquet")}
    con.close()

    # A transparent reference model illustrates repeat suppression, not RocksDB.
    bodies, firsts, corrections = set(), {}, []
    for timestamp in [100] + [120] * 1000 + [90]:
        bodies.add("E")
        key = ("E", "R1", "observer", "public")
        if key not in firsts or timestamp < firsts[key]:
            firsts[key] = timestamp
            corrections.append(timestamp)
    assert (len(bodies), len(firsts), corrections) == (1, 1, [100, 90])
    # Independent whole-state oracle for the empty-winner/backfill counterexample.
    revisions = [(100, ["R1"]), (200, []), (150, ["R2"])]
    winner = max(revisions, key=lambda row: row[0])
    wrong_entries_only = max((row for row in revisions if row[1]),
                            key=lambda row: row[0])
    assert winner == (200, []) and wrong_entries_only == (150, ["R2"])
    return {
        "status": "PASS", "python": sys.version.split()[0],
        "duckdb": duckdb.__version__,
        "scope": "Real Parquet write/read plus explicit small reference-model counterexamples",
        "not_tested": ["Nostr cryptographic validity (synthetic signatures)",
                       "Rust implementation", "RocksDB/DuckLake/CH durability",
                       "production throughput", "billion-row capacity"],
        "checks": {
            "standalone_fresh_process_full_field_roundtrip": len(fixtures),
            "required_event_fields": sorted(required_fields),
            "before_after_source_cut_minima": [before, after],
            "document_objects": len(objects), "actual_document_observations": 1000,
            "cross_scope_join_wrong_count": wrong_join_count,
            "model_ordinary_repeats": 1000, "model_immutable_receive_facts": 2,
            "empty_winner_preserved": True,
            "negative_controls_distinguished": [
                "float64 timestamp loss", "tag reorder", "latest-only correction",
                "imported claim as local observation", "document join without scope",
                "entries-only current list"
            ],
        },
        "parquet_bytes": sizes,
        "document_representation_bytes": {
            "objects_plus_references": sizes["objects.parquet"] + sizes["observations.parquet"],
            "repeated_wide_rows": sizes["wide.parquet"],
            "interpretation": "Tiny repeated fixture; dictionary compression and file overhead matter. Not a universal dedup compression ratio."
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path)
    parser.add_argument("--read-events", type=Path)
    args = parser.parse_args()
    if args.read_events:
        print(json.dumps(read_events(args.read_events), ensure_ascii=False))
    elif args.output:
        result = run(args.output)
        (args.output / "result.json").write_text(json.dumps(result, indent=2) + "\n")
        print(json.dumps(result, indent=2))
    else:
        parser.error("choose --output or --read-events")


if __name__ == "__main__":
    main()
