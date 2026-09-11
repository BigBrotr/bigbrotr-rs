"""Bounded local Parquet layout comparison; no product or capacity claim."""

import argparse
import hashlib
import json
import platform
import struct
from pathlib import Path

import duckdb


ROWS = 16_384
BODY_BYTES = 1_024
ROW_GROUP_TARGET = 16_384
BATCH_ROWS = 128
OUTPUT_LIMIT = 250_000_000
NEXT_FILE_RESERVE = 32 * 1024 * 1024
FIELDS = (
    "observation_id, subject_id, observed_at_us, visibility_namespace, "
    "method_id, outcome, document_digest, document_length, document_bytes"
)
REFERENCE_FIELDS = FIELDS.rsplit(", ", 2)[0]


class OracleMismatch(Exception):
    pass


def digest_file(path):
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(65536), b""):
            digest.update(block)
    return digest.hexdigest()


def sql_string(value):
    return "'" + str(value).replace("'", "''") + "'"


def source_row(observation_id, churn, entropy):
    # Each body occurs in both namespaces. Low churn reuses eight bodies;
    # high churn uses one new body for each adjacent namespace pair.
    body_key = observation_id // 2 if churn == "high" else (observation_id // 2) % 8
    namespace = "public" if observation_id % 2 == 0 else "restricted"
    seed = ("document-layout/v1/%d" % body_key).encode("ascii")
    if entropy == "high":
        body = hashlib.shake_256(seed).digest(BODY_BYTES)
    else:
        prefix = b'{"document":' + str(body_key).encode("ascii") + b',"text":"'
        body = prefix + b"a" * (BODY_BYTES - len(prefix) - 2) + b'"}'
    return (
        observation_id,
        observation_id % 256,
        1_800_000_000_000_000 + observation_id * 1000,
        namespace,
        "fixture-document/1",
        "ok",
        hashlib.sha256(body).digest(),
        len(body),
        body,
    )


def row_digest(row):
    digest = hashlib.sha256(b"document-layout-row/v1\0")
    digest.update(struct.pack(">QQq", *row[:3]))
    for value in row[3:7]:
        encoded = value.encode("utf-8") if isinstance(value, str) else value
        digest.update(struct.pack(">I", len(encoded)))
        digest.update(encoded)
    digest.update(struct.pack(">I", row[7]))
    digest.update(row[8])
    return digest.digest()


def streamed_rows(connection, sql, parameters):
    cursor = connection.cursor()
    try:
        cursor.execute(sql, parameters)
        while True:
            rows = cursor.fetchmany(BATCH_ROWS)
            if not rows:
                return
            yield from rows
    finally:
        cursor.close()


def checked_objects(connection, paths, omit_key=None):
    previous = None
    sql = (
        "SELECT visibility_namespace, document_digest, document_length, document_bytes "
        "FROM read_parquet(?) ORDER BY visibility_namespace, document_digest"
    )
    for row in streamed_rows(connection, sql, [[str(path) for path in paths]]):
        key = row[:2]
        if previous is not None and key <= previous:
            raise OracleMismatch("Object identities are not unique and ordered")
        previous = key
        if len(row[3]) != row[2] or hashlib.sha256(row[3]).digest() != row[1]:
            raise OracleMismatch("Object length/digest does not authenticate its bytes")
        if key != omit_key:
            yield row


def reconstructed_rows(connection, references, objects, omit_key=None):
    # Deliberately perform the reference merge in Python, not in the SQL writer.
    object_rows = checked_objects(connection, objects, omit_key)
    current = next(object_rows, None)
    current_used = False
    sql = (
        "SELECT " + REFERENCE_FIELDS + " FROM read_parquet(?) "
        "ORDER BY visibility_namespace, document_digest, observation_id"
    )
    for reference in streamed_rows(connection, sql, [[str(path) for path in references]]):
        key = (reference[3], reference[6])
        while current is not None and current[:2] < key:
            if not current_used:
                raise OracleMismatch("Unreferenced object")
            current = next(object_rows, None)
            current_used = False
        if current is None or current[:2] != key:
            raise OracleMismatch("Missing namespaced object")
        current_used = True
        yield reference + current[2:]
    if current is not None and not current_used:
        raise OracleMismatch("Unreferenced final object")
    if next(object_rows, None) is not None:
        raise OracleMismatch("Unreferenced object suffix")


def verify_rows(rows, churn, entropy, expected_digest):
    seen = bytearray(ROWS)
    ordered_hashes = bytearray(ROWS * 32)
    count = 0
    for actual in rows:
        observation_id = actual[0]
        if not 0 <= observation_id < ROWS or seen[observation_id]:
            raise OracleMismatch("Duplicate or out-of-domain observation")
        expected = source_row(observation_id, churn, entropy)
        if actual != expected:
            raise OracleMismatch("Reconstructed observation/subject/document bytes differ")
        if hashlib.sha256(actual[8]).digest() != actual[6]:
            raise OracleMismatch("Observation document digest mismatch")
        seen[observation_id] = 1
        ordered_hashes[observation_id * 32:(observation_id + 1) * 32] = row_digest(actual)
        count += 1
    actual_digest = hashlib.sha256(ordered_hashes).hexdigest()
    if count != ROWS or not all(seen) or actual_digest != expected_digest:
        raise OracleMismatch("Logical input set differs")
    return {"exact_rows_checked": count, "ordered_row_digest_sha256": actual_digest}


def output_bytes(root):
    return sum(path.stat().st_size for path in root.rglob("*") if path.is_file())


def copy_parquet(connection, root, query, path):
    if path.exists():
        raise FileExistsError(path)
    if output_bytes(root) + NEXT_FILE_RESERVE >= OUTPUT_LIMIT:
        raise RuntimeError("Insufficient declared output budget before COPY")
    connection.execute(
        "COPY (" + query + ") TO " + sql_string(path)
        + " (FORMAT parquet, COMPRESSION zstd, COMPRESSION_LEVEL 3, "
        + "ROW_GROUP_SIZE " + str(ROW_GROUP_TARGET) + ")"
    )
    if output_bytes(root) >= OUTPUT_LIMIT:
        raise RuntimeError("Declared output limit exceeded")


def file_record(connection, root, path):
    groups = connection.execute(
        "SELECT DISTINCT row_group_id, row_group_num_rows FROM parquet_metadata(?) "
        "ORDER BY row_group_id", [str(path)]
    ).fetchall()
    return {
        "path": str(path.relative_to(root)),
        "bytes": path.stat().st_size,
        "sha256": digest_file(path),
        "actual_row_groups": [
            {"id": group, "rows": rows} for group, rows in groups
        ],
    }


def expect_oracle_failure(call, required_message):
    try:
        call()
    except OracleMismatch as error:
        if required_message not in str(error):
            raise
        return {"expected_rejection_observed": True, "reason": str(error)}
    raise OracleMismatch("Negative control unexpectedly passed")


def run_case(root, churn, entropy, parts, object_order):
    name = "%s-churn_%s-entropy_%02d-files" % (churn, entropy, parts)
    case_root = root / name
    case_root.mkdir(exist_ok=False)
    temporary = case_root / "scratch"
    temporary.mkdir(exist_ok=False)
    connection = duckdb.connect(":memory:", config={
        "threads": "1",
        "memory_limit": "192MB",
        "max_temp_directory_size": "32MB",
        "temp_directory": str(temporary),
        "autoload_known_extensions": "false",
        "autoinstall_known_extensions": "false",
    })
    try:
        connection.execute(
            "CREATE TABLE observations (observation_id UBIGINT, subject_id UBIGINT, "
            "observed_at_us BIGINT, visibility_namespace VARCHAR, method_id VARCHAR, "
            "outcome VARCHAR, document_digest BLOB, document_length UINTEGER, document_bytes BLOB)"
        )
        expected = hashlib.sha256()
        connection.execute("BEGIN")
        for start in range(0, ROWS, BATCH_ROWS):
            batch = [source_row(i, churn, entropy) for i in range(start, start + BATCH_ROWS)]
            for row in batch:
                expected.update(row_digest(row))
            connection.executemany("INSERT INTO observations VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", batch)
        connection.execute("COMMIT")
        expected_digest = expected.hexdigest()
        connection.execute(
            "CREATE TABLE objects AS SELECT visibility_namespace, document_digest, "
            "any_value(document_length) AS document_length, "
            "any_value(document_bytes) AS document_bytes, "
            "min(observation_id) AS first_observation_id FROM observations "
            "GROUP BY visibility_namespace, document_digest"
        )
        wide, references, objects = [], [], []
        rows_per_file = ROWS // parts
        for part in range(parts):
            lo, hi = part * rows_per_file, (part + 1) * rows_per_file
            where = " WHERE observation_id >= %d AND observation_id < %d" % (lo, hi)
            wide_path = case_root / ("wide-%02d.parquet" % part)
            reference_path = case_root / ("references-%02d.parquet" % part)
            copy_parquet(connection, root, "SELECT " + FIELDS + " FROM observations" + where
                         + " ORDER BY observation_id", wide_path)
            copy_parquet(connection, root, "SELECT " + REFERENCE_FIELDS + " FROM observations" + where
                         + " ORDER BY observation_id", reference_path)
            wide.append(wide_path)
            references.append(reference_path)
            object_where = " WHERE first_observation_id >= %d AND first_observation_id < %d" % (lo, hi)
            if connection.execute("SELECT count(*) FROM objects" + object_where).fetchone()[0]:
                object_path = case_root / ("objects-%02d.parquet" % part)
                copy_parquet(connection, root,
                             "SELECT visibility_namespace, document_digest, document_length, document_bytes "
                             "FROM objects" + object_where + " ORDER BY " + object_order,
                             object_path)
                objects.append(object_path)
        wide_sql = "SELECT " + FIELDS + " FROM read_parquet(?) ORDER BY observation_id"
        wide_check = verify_rows(streamed_rows(connection, wide_sql, [[str(p) for p in wide]]),
                                 churn, entropy, expected_digest)
        split_check = verify_rows(reconstructed_rows(connection, references, objects),
                                  churn, entropy, expected_digest)
        key = ("restricted", source_row(0, churn, entropy)[6])
        missing_namespace = expect_oracle_failure(
            lambda: verify_rows(reconstructed_rows(connection, references, objects, key),
                                churn, entropy, expected_digest), "Missing namespaced object")
        tampered = list(source_row(0, churn, entropy))
        tampered[8] = bytes([tampered[8][0] ^ 1]) + tampered[8][1:]
        corrupt_body = expect_oracle_failure(
            lambda: verify_rows(iter([tuple(tampered)]), churn, entropy, expected_digest),
            "Reconstructed observation/subject/document bytes differ")
        wide_records = [file_record(connection, root, p) for p in wide]
        reference_records = [file_record(connection, root, p) for p in references]
        object_records = [file_record(connection, root, p) for p in objects]
        wide_bytes = sum(p["bytes"] for p in wide_records)
        split_bytes = sum(p["bytes"] for p in reference_records + object_records)
        object_count = sum(g["rows"] for p in object_records for g in p["actual_row_groups"])
        expected_objects = ROWS if churn == "high" else 16
        if object_count != expected_objects:
            raise OracleMismatch("Namespaced object inventory differs from fixture")
        result = {
            "case": name, "observations": ROWS, "body_bytes": BODY_BYTES,
            "churn": churn, "entropy": entropy, "observation_files_per_layout": parts,
            "expected_unique_bodies": ROWS // 2 if churn == "high" else 8,
            "namespaced_objects": object_count,
            "wide_bytes": wide_bytes, "split_bytes": split_bytes,
            "split_minus_wide_bytes": split_bytes - wide_bytes,
            "split_over_wide": split_bytes / wide_bytes,
            "source_ordered_row_digest_sha256": expected_digest,
            "wide_oracle": wide_check, "split_oracle": split_check,
            "negative_controls": {"omit_restricted_object": missing_namespace, "tamper_body": corrupt_body},
            "wide_files": wide_records, "reference_files": reference_records, "object_files": object_records,
            "actual_settings": dict(connection.execute(
                "SELECT name, value FROM duckdb_settings() WHERE name IN "
                "('threads','memory_limit','max_temp_directory_size','autoload_known_extensions',"
                "'autoinstall_known_extensions') ORDER BY name").fetchall()),
        }
        print(json.dumps({key: result[key] for key in
                         ("case", "wide_bytes", "split_bytes", "namespaced_objects")}), flush=True)
        return result
    finally:
        connection.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--object-order", choices=("first-observation", "namespace-digest"),
                        default="first-observation")
    args = parser.parse_args()
    object_order = ("first_observation_id" if args.object_order == "first-observation"
                    else "visibility_namespace, document_digest")
    root = args.output.resolve()
    root.mkdir(parents=True, exist_ok=False)
    results = []
    try:
        for churn in ("low", "high"):
            for entropy in ("low", "high"):
                for parts in (1, 16):
                    results.append(run_case(root, churn, entropy, parts, object_order))
        report = {
            "status": "completed", "program_sha256": digest_file(Path(__file__)),
            "python_version": platform.python_version(), "duckdb_version": duckdb.__version__,
            "scope": "Local DuckDB Parquet bytes and streamed logical readback; no RSS, latency, capacity, durability or product claim",
            "constraints": {"threads": 1, "duckdb_memory_setting": "192MB", "engine_scratch_setting": "32MB",
                            "observations_per_case": ROWS, "body_bytes": BODY_BYTES,
                            "python_source_batch_rows": BATCH_ROWS, "python_oracle_hash_bytes": ROWS * 32,
                            "output_limit_bytes": OUTPUT_LIMIT},
            "encoding": {"codec": "zstd", "compression_level": 3, "row_group_target": ROW_GROUP_TARGET,
                         "observation_order": "observation_id", "object_order_per_file": object_order,
                         "object_publication": "First-occurrence batch; no later global object compaction",
                         "wide_columns": FIELDS, "reference_columns": REFERENCE_FIELDS,
                         "object_columns": "visibility_namespace, document_digest, document_length, document_bytes"},
            "oracle": "Python full tuple/body comparison and SHA-256; Python namespaced merge; DuckDB decodes both candidates",
            "cases": results, "parquet_bytes_all_candidates": output_bytes(root),
            "limitations": ["Synthetic bytes are not a real NIP-11 corpus or signed Nostr events",
                            "One writer/reader engine; no independent Parquet decoder",
                            "No elapsed-time, RSS, CPU, I/O, lookup-latency, crash, GC or distributed measurements",
                            "DuckDB memory_limit is a setting, not an observed process RSS bound"],
        }
        result_path = root / "result.json"
        with result_path.open("x") as handle:
            json.dump(report, handle, indent=2)
            handle.write("\n")
        if output_bytes(root) >= OUTPUT_LIMIT:
            raise RuntimeError("Result manifest exceeded total output budget")
        print(json.dumps({"result": str(result_path), "sha256": digest_file(result_path),
                          "total_output_bytes": output_bytes(root)}), flush=True)
    except Exception as error:
        with (root / "failure.json").open("x") as handle:
            json.dump({"status": "failed", "error_type": type(error).__name__, "error": str(error),
                       "completed_cases": results, "program_sha256": digest_file(Path(__file__))}, handle, indent=2)
            handle.write("\n")
        raise


if __name__ == "__main__":
    main()
