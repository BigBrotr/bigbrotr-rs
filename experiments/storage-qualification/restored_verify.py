#!/usr/bin/env python3
"""Verify copied archive using a separately restored PG catalog and data root."""

import argparse
import hashlib
import json
from pathlib import Path
import time

import duckdb
from engines import literal, save
from generate import validate
from verify import fixture


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", default="archive02")
    args = parser.parse_args()
    assert args.run_id.isalnum() and len(args.run_id) <= 24
    root = Path("/lab")
    result = root / "results" / ("restored-preservation-" + args.run_id + ".json")
    assert not result.exists()
    report = {
        "status": "RUNNING",
        "tables": {},
        "scope": "full ordinal coverage/ID uniqueness and regenerated complete signed samples; not full-corpus crypto proof",
    }
    save(result, report)
    con = duckdb.connect(
        config={
            "threads": "4",
            "memory_limit": "7GB",
            "extension_directory": "/lab/extensions",
            "temp_directory": "/lab/tmp-restore",
            "max_temp_directory_size": "80GB",
        }
    )
    try:
        con.execute("LOAD ducklake")
        con.execute("LOAD postgres")
        copied_root = "/lab/backups/" + args.run_id + "/data/"
        con.execute(
            "ATTACH 'ducklake:postgres:dbname=qualification_restore_"
            + args.run_id
            + " host=127.0.0.1 port=48632 user=postgres' AS restored "
            "(READ_ONLY, DATA_PATH "
            + literal(copied_root)
            + ", OVERRIDE_DATA_PATH true, CREATE_IF_NOT_EXISTS false)"
        )
        for corpus in ("calibration", "aged"):
            manifest = json.loads(
                (root / "input" / corpus / "manifest.json").read_text()
            )
            count = manifest["rows"]
            ordinals = {0, count - 1, 4095, 4096, count // 2}
            for i in range(256):
                ordinals.add(
                    int.from_bytes(
                        hashlib.sha256(f"restore-sample:{i}".encode()).digest()[:8],
                        "big",
                    )
                    % count
                )
            expected = fixture(
                root
                / "src/storage-qualification/fixture/target/release/bigbrotr-research-fixture",
                sorted(ordinals),
                manifest["minimum_content_bytes"],
            )
            expected_by_id = {r["id"]: r for r in expected}
            ids = ",".join("from_hex(" + literal(r["id"]) + ")" for r in expected)
            for suffix in ("lake", "lake_zstd"):
                started = time.monotonic()
                name = corpus + "_" + suffix
                table = "restored." + name
                rows = con.execute(
                    "SELECT * FROM ducklake_list_files(?,?)", ["restored", name]
                )
                columns = [c[0] for c in rows.description]
                files = [dict(zip(columns, row)) for row in rows.fetchall()]
                paths = [str(r["data_file"]) for r in files]
                assert paths and all(path.startswith(copied_root) for path in paths), (
                    paths[:3]
                )
                stats = list(
                    map(
                        int,
                        con.execute(
                            f"SELECT count(*),count(DISTINCT id),count(DISTINCT ordinal),min(ordinal),max(ordinal),sum(ordinal) FROM {table}"
                        ).fetchone(),
                    )
                )
                assert stats == [
                    count,
                    count,
                    count,
                    0,
                    count - 1,
                    count * (count - 1) // 2,
                ]
                values = con.execute(
                    f"SELECT ordinal,lower(hex(id)),lower(hex(pubkey)),created_at,kind,tags,content,lower(hex(sig)) FROM {table} WHERE id IN ({ids})"
                ).fetchall()
                actual = {}
                for value in values:
                    row = dict(
                        zip(
                            (
                                "ordinal",
                                "id",
                                "pubkey",
                                "created_at",
                                "kind",
                                "tags",
                                "content",
                                "sig",
                            ),
                            value,
                        )
                    )
                    validate(row)
                    assert row == expected_by_id[row["id"]] and row["id"] not in actual
                    actual[row["id"]] = row
                assert set(actual) == set(expected_by_id)
                assert (
                    con.execute(f"SELECT sum(rows) FROM {table}_receipts").fetchone()[0]
                    == count
                )
                report["tables"][name] = {
                    "rows": count,
                    "files": len(paths),
                    "sample_rows": len(actual),
                    "seconds": time.monotonic() - started,
                    "signed_sample_sha256": hashlib.sha256(
                        json.dumps(
                            [actual[k] for k in sorted(actual)],
                            ensure_ascii=False,
                            sort_keys=True,
                            separators=(",", ":"),
                        ).encode()
                    ).hexdigest(),
                }
                save(result, report)
        report["status"] = "PASS_restored_archive_samples_and_population"
    except BaseException as exc:
        report["status"], report["error"] = "FAILED", repr(exc)
        raise
    finally:
        con.close()
        save(result, report)


if __name__ == "__main__":
    main()
