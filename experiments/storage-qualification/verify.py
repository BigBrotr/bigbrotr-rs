#!/usr/bin/env python3
"""Full-field sampled preservation plus independent whole-population/order oracles."""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import time

from engines import ch, lake, literal, save
from generate import validate


def fixture(binary, ordinals, size):
    result = subprocess.run([str(binary), "list", str(size), "1"],
                            input=json.dumps(ordinals), text=True, capture_output=True, check=True)
    rows = [json.loads(line) for line in result.stdout.splitlines()]
    assert [r["ordinal"] for r in rows] == ordinals
    for row in rows:
        validate(row)
    return rows


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, default=Path("/lab"))
    parser.add_argument("--corpus", choices=("calibration", "aged"), required=True)
    parser.add_argument("--hash-inputs", action="store_true")
    parser.add_argument("--include-zstd", action="store_true")
    parser.add_argument("--only-ch", action="store_true", help="Recheck only engines affected by a CH restart")
    parser.add_argument("--run-label", default="preservation-v2")
    args = parser.parse_args()
    assert not (args.only_ch and args.include_zstd)
    root, corpus = args.root, args.corpus
    manifest = json.loads((root / "input" / corpus / "manifest.json").read_text())
    count, size = manifest["rows"], manifest["minimum_content_bytes"]
    assert count > 4096, "This qualification profile requires its declared large corpus"
    end = 0
    for partition in sorted(manifest["partitions"], key=lambda p: p["start"]):
        assert partition["start"] == end, "Expected contiguous zero-based fixture"
        end += partition["rows"]
    assert end == count
    binary = root / "src/storage-qualification/fixture/target/release/bigbrotr-research-fixture"
    ordinals = {0, count - 1, 4095, 4096, count // 2}
    for partition in manifest["partitions"]:
        for file in partition["files"]:
            start = int(Path(file["path"]).stem.split("-")[-1])
            assert 0 <= start < start + file["rows"] <= count
            ordinals.update((start, start + file["rows"] - 1))
    for i in range(1024):
        ordinals.add(int.from_bytes(hashlib.sha256(f"qualification-sample-v1:{i}".encode()).digest()[:8], "big") % count)
    # Independently invert the generator's modular time permutation. Gather every
    # ordinal in the newest time buckets, including all ties at the 100th row.
    span, newest, inverse = 90 * 86400, [], pow(7919, -1, 90 * 86400)
    for age in range(span):
        first = ((span - 1 - age) * inverse) % span
        newest.extend(range(first, count, span))
        if len(newest) >= 100:
            break
    ordinals.update(newest)
    expected = fixture(binary, sorted(ordinals), size)
    expected_by_id = {r["id"]: r for r in expected}
    newest_set = set(newest)
    expected_order = [r["ordinal"] for r in sorted(
        (r for r in expected if r["ordinal"] in newest_set),
        key=lambda r: (-r["created_at"], r["id"]))[:100]]
    assert args.run_label.replace("-", "").isalnum() and len(args.run_label) <= 60
    result = root / "results" / (corpus + "-" + args.run_label + ".json")
    assert not result.exists(), "Preserve prior run; choose a new research identity"
    report = {"status": "RUNNING", "rows": count, "sample_rows": len(expected),
              "sample_authors": len({r["pubkey"] for r in expected}), "engines": {},
              "source_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
              "scope": "sampled complete signed fields; full row/distinct-ID/count/sum and independent chronological oracle; no full-corpus signature or durability proof"}
    save(result, report)
    if args.hash_inputs:
        started = time.monotonic()
        hashes = []
        for path in sorted((root / "input" / corpus).glob("*.parquet")):
            with path.open("rb") as source:
                digest = hashlib.file_digest(source, "sha256").hexdigest()
            hashes.append({"path": path.name, "bytes": path.stat().st_size, "sha256": digest})
        report["input_files"] = hashes
        report["input_hash_seconds"] = time.monotonic() - started
        report["input_hash_timing"] = "post-load identity capture; initial generator manifest had sizes only"
        save(result, report)
    engines = ("lake", "id", "time", "lake_zstd") if args.include_zstd else ("lake", "id", "time")
    if args.only_ch:
        engines = ("id", "time")
    for engine in engines:
        con = lake(root) if engine.startswith("lake") else None
        table = ("lake." if con else "") + corpus + "_" + engine
        def query(sql):
            if con:
                return con.execute(sql).fetchall()
            return json.loads(ch(sql + " FORMAT JSONCompact")[0])["data"]
        started = time.monotonic()
        stats = query(f"SELECT count(*),count(DISTINCT id),sum(ordinal),count(DISTINCT ordinal),min(ordinal),max(ordinal) FROM {table}")[0]
        assert list(map(int, stats)) == [count, count, count * (count - 1) // 2, count, 0, count - 1]
        kinds = [tuple(map(int, r)) for r in query(f"SELECT kind,count(*) FROM {table} GROUP BY kind ORDER BY kind")]
        rare = (count + 19) // 20
        assert kinds == [(1, count - rare), (30023, rare)]
        chronological = [int(r[0]) for r in query(f"SELECT ordinal FROM {table} ORDER BY created_at DESC,id LIMIT 100")]
        assert chronological == expected_order, "chronological result differs from modular-time oracle"
        prefix = "from_hex" if con else "unhex"
        ids = ",".join(prefix + "(" + literal(r["id"]) + ")" for r in expected)
        started_fetch = time.monotonic()
        fields = "ordinal,lower(hex(id)),lower(hex(pubkey)),created_at,kind,tags,content,lower(hex(sig))"
        fetched = query(f"SELECT {fields} FROM {table} WHERE id IN ({ids})")
        fetch_seconds = time.monotonic() - started_fetch
        names = ("ordinal", "id", "pubkey", "created_at", "kind", "tags", "content", "sig")
        actual = {}
        for values in fetched:
            row = dict(zip(names, values))
            for name in ("ordinal", "created_at", "kind"):
                row[name] = int(row[name])
            validate(row)
            assert row == expected_by_id[row["id"]], "complete fixture field mismatch"
            assert row["id"] not in actual, "duplicate sampled event"
            actual[row["id"]] = row
        assert set(actual) == set(expected_by_id)
        # Corruption control: same length, changed content must fail even though
        # the earlier length-only check would accept it.
        changed = dict(expected[0])
        changed["content"] = "!" + changed["content"][1:]
        try:
            validate(changed)
        except AssertionError:
            pass
        else:
            raise AssertionError("same-length corruption accepted")
        report["engines"][engine] = {"status": "PASS", "seconds": time.monotonic()-started,
            "full_sample_fetch_seconds": fetch_seconds, "sample_rows": len(actual),
            "complete_result_sha256": hashlib.sha256(json.dumps([actual[k] for k in sorted(actual)],
                ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()).hexdigest(),
            "aggregate_expected": list(map(int, stats)), "chronological_ordinals": chronological,
            "negative_control": "same-length content mutation rejected"}
        if con:
            con.close()
        save(result, report)
    assert len({v["complete_result_sha256"] for v in report["engines"].values()}) == 1
    report["status"] = "PASS_sampled_signed_preservation_and_population_oracles"
    save(result, report)
    print(json.dumps({k: report[k] for k in ("status", "rows", "sample_rows", "sample_authors")}))


if __name__ == "__main__":
    main()
