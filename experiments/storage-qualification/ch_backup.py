#!/usr/bin/env python3
"""Small native ClickHouse backup/restore, isolated from the large source tables."""

import hashlib
import json
from pathlib import Path

from engines import ch, literal, save
from generate import validate
from verify import fixture


def main():
    root = Path("/lab")
    result = root / "results/ch-backup-restore.json"
    assert not result.exists()
    report = {
        "status": "RUNNING",
        "scope": "100k-row native backup to a fresh table; full population and sampled signed values; same-server/same-device, no disaster recovery claim",
    }
    save(result, report)
    try:
        assert ch("EXISTS TABLE calibration_id_restored")[0].strip() == "0"
        assert not (root / "backups/ch-calibration01").exists()
        for action, sql, expected in (
            (
                "backup",
                "BACKUP TABLE calibration_id TO File('/backups/ch-calibration01')",
                "BACKUP_CREATED",
            ),
            (
                "restore",
                "RESTORE TABLE calibration_id AS calibration_id_restored FROM File('/backups/ch-calibration01')",
                "RESTORED",
            ),
        ):
            rows, timing = ch(sql + " FORMAT JSONCompact")
            data = json.loads(rows)["data"]
            assert len(data) == 1 and data[0][1] == expected, data
            report[action] = {**timing, "id": data[0][0], "status": data[0][1]}
            save(result, report)
        manifest = json.loads((root / "input/calibration/manifest.json").read_text())
        count = manifest["rows"]
        indices = {0, count - 1, 4095, 4096}
        for i in range(256):
            indices.add(
                int.from_bytes(
                    hashlib.sha256(f"ch-backup:{i}".encode()).digest()[:8], "big"
                )
                % count
            )
        expected = fixture(
            root
            / "src/storage-qualification/fixture/target/release/bigbrotr-research-fixture",
            sorted(indices),
            manifest["minimum_content_bytes"],
        )
        expected_by_id = {r["id"]: r for r in expected}
        ids = ",".join("unhex(" + literal(r["id"]) + ")" for r in expected)
        report["tables"] = {}
        for table in ("calibration_id", "calibration_id_restored"):
            stats = json.loads(
                ch(
                    f"SELECT count(*),uniqExact(id),uniqExact(ordinal),min(ordinal),max(ordinal),sum(ordinal) FROM {table} FORMAT JSONCompact"
                )[0]
            )["data"][0]
            assert list(map(int, stats)) == [
                count,
                count,
                count,
                0,
                count - 1,
                count * (count - 1) // 2,
            ]
            values = json.loads(
                ch(
                    f"SELECT ordinal,lower(hex(id)),lower(hex(pubkey)),created_at,kind,tags,content,lower(hex(sig)) FROM {table} WHERE id IN ({ids}) FORMAT JSONCompact"
                )[0]
            )["data"]
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
                for key in ("ordinal", "created_at", "kind"):
                    row[key] = int(row[key])
                validate(row)
                assert row == expected_by_id[row["id"]] and row["id"] not in actual
                actual[row["id"]] = row
            assert set(actual) == set(expected_by_id)
            report["tables"][table] = {
                "rows": count,
                "sample_rows": len(actual),
                "sample_sha256": hashlib.sha256(
                    json.dumps(
                        [actual[k] for k in sorted(actual)],
                        ensure_ascii=False,
                        sort_keys=True,
                        separators=(",", ":"),
                    ).encode()
                ).hexdigest(),
            }
        report["status"] = "PASS_native_CH_backup_restore"
    except BaseException as exc:
        report["status"], report["error"] = "FAILED", repr(exc)
        raise
    finally:
        save(result, report)


if __name__ == "__main__":
    main()
