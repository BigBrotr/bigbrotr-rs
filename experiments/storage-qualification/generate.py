#!/usr/bin/env python3
"""Bounded signed synthetic corpus; never contact or publish to public relays."""
import argparse
import concurrent.futures
import hashlib
import json
from pathlib import Path
import subprocess
import time

import pyarrow as pa
import pyarrow.parquet as pq
from coincurve import PublicKeyXOnly

SCHEMA = pa.schema([
    ("ordinal", pa.uint64()), ("id", pa.binary(32)),
    ("pubkey", pa.binary(32)), ("created_at", pa.int64()),
    ("kind", pa.uint32()), ("tags", pa.list_(pa.list_(pa.string()))),
    ("content", pa.string()), ("sig", pa.binary(64)),
])


def validate(row):
    canonical = json.dumps([0, row["pubkey"], row["created_at"], row["kind"],
                            row["tags"], row["content"]], ensure_ascii=False,
                           separators=(",", ":")).encode()
    digest = hashlib.sha256(canonical).digest()
    assert digest.hex() == row["id"], "event ID does not bind canonical JSON"
    assert PublicKeyXOnly(bytes.fromhex(row["pubkey"])).verify(
        bytes.fromhex(row["sig"]), digest), "invalid Schnorr signature"


def negative_controls(row):
    for field, value in (("content", row["content"] + "!"), ("sig", "00" * 64)):
        changed = dict(row, **{field: value})
        try:
            validate(changed)
        except (AssertionError, ValueError):
            continue
        raise AssertionError(f"negative control accepted corrupted {field}")


def partition(args):
    binary, directory, start, count, size, threads = args
    started = time.monotonic()
    command = [binary, str(start), str(count), str(size), str(threads)]
    process = subprocess.Popen(command, stdout=subprocess.PIPE, text=True)
    rows, files, samples, verified = [], [], [], 0
    writer = None
    part_rows = 0
    file_path = None
    try:
        for index, line in enumerate(process.stdout):
            row = json.loads(line)
            assert row["ordinal"] == start + index
            if index < 16 or index % 4093 == 0:
                validate(row)
                verified += 1
                if len(samples) < 8:
                    samples.append({k: row[k] for k in ("ordinal", "id", "created_at", "kind")})
                if index == 0:
                    negative_controls(row)
            for key in ("id", "pubkey", "sig"):
                row[key] = bytes.fromhex(row[key])
            rows.append(row)
            if len(rows) == 8192 or index + 1 == count:
                if writer is None:
                    file_path = Path(directory) / f"events-{start + index + 1 - len(rows):012d}.parquet"
                    writer = pq.ParquetWriter(file_path, SCHEMA, compression="zstd",
                                              compression_level=3, use_dictionary=["pubkey", "kind"])
                writer.write_table(pa.Table.from_pylist(rows, schema=SCHEMA), row_group_size=8192)
                part_rows += len(rows)
                rows.clear()
                if part_rows >= 65536 or index + 1 == count:
                    writer.close()
                    with file_path.open("rb") as source:
                        digest = hashlib.file_digest(source, "sha256").hexdigest()
                    files.append({"path": file_path.name, "rows": part_rows,
                                  "bytes": file_path.stat().st_size, "sha256": digest})
                    writer, part_rows = None, 0
        assert process.wait() == 0, "fixture generator failed"
        assert sum(f["rows"] for f in files) == count, "truncated fixture stream"
    finally:
        if writer is not None:
            writer.close()
        if process.poll() is None:
            process.kill()
        process.wait()
    return {"start": start, "rows": count, "verified": verified,
            "negative_controls": 2, "seconds": time.monotonic() - started,
            "files": files, "samples": samples}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--binary", required=True)
    parser.add_argument("--directory", type=Path, required=True)
    parser.add_argument("--rows", type=int, required=True)
    parser.add_argument("--content-bytes", type=int, default=8192)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--start", type=int, default=0)
    args = parser.parse_args()
    assert 0 < args.workers <= 4 and args.rows > 0
    assert 0 <= args.content_bytes <= 131072
    args.directory.mkdir(parents=True, exist_ok=False)
    started = time.monotonic()
    quotient, remainder = divmod(args.rows, args.workers)
    work, offset = [], args.start
    for i in range(args.workers):
        count = quotient + (i < remainder)
        if count:
            work.append((args.binary, str(args.directory), offset, count, args.content_bytes, 2))
        offset += count
    with concurrent.futures.ProcessPoolExecutor(max_workers=args.workers) as pool:
        results = list(pool.map(partition, work))
    manifest = {"fixture": "valid-signed-hash-rich-synthetic-v1",
                "not_representative_of": "real content compression or human search relevance",
                "rows": args.rows, "minimum_content_bytes": args.content_bytes,
                "seconds": time.monotonic() - started, "workers": args.workers,
                "source_bytes": sum(f["bytes"] for r in results for f in r["files"]),
                "partitions": results}
    path = args.directory / "manifest.json"
    path.write_text(json.dumps(manifest, indent=2) + "\n")
    print(json.dumps({k: v for k, v in manifest.items() if k != "partitions"}), flush=True)


if __name__ == "__main__":
    main()
