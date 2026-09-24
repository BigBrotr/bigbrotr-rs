#!/usr/bin/env python3
"""Public, deliberately fixed keys; independent hashlib/struct wire oracle."""
import hashlib
import json
import pathlib
import struct

DATASET = bytes.fromhex("11" * 16)
RING = bytes.fromhex("22" * 16)
RECORDS = [bytes.fromhex("33" * 16) + bytes(range(1, 33)),
           bytes.fromhex("44" * 16) + bytes(range(33, 65))]


def encode(records, dataset=DATASET, ring=RING, count=None, magic=b"BBCKEY\x00\x01"):
    body = magic + dataset + ring + struct.pack(">H", len(records) if count is None else count)
    body += b"".join(records)
    return body + hashlib.sha256(body).digest()


def case(name, wire, error=None):
    return {"name": name, "hex": wire.hex(), "error": error}


maximum = [bytes([i]) * 16 + bytes([i + 20]) * 32 for i in range(1, 17)]
golden = encode(RECORDS)
cases = [case("two-public-keys", golden), case("sixteen-public-keys", encode(maximum)),
         case("empty", b"", "Mismatch"), case("truncated", golden[:-1], "Mismatch"),
         case("trailing-byte", golden + b"\x00", "Mismatch"),
         case("unknown-version", encode(RECORDS, magic=b"BBCKEY\x00\x02"), "Mismatch"),
         case("foreign-dataset", encode(RECORDS, dataset=bytes([9])*16), "Mismatch"),
         case("zero-ring", encode(RECORDS, ring=bytes(16)), "Mismatch"),
         case("zero-count", encode(RECORDS, count=0), "Mismatch"),
         case("wrong-count", encode(RECORDS, count=1), "Mismatch"),
         case("too-many", encode(maximum + [RECORDS[0]]), "Capacity"),
         case("duplicate-key-id", encode([RECORDS[0], RECORDS[0][:16] + RECORDS[1][16:]]), "Mismatch"),
         case("zero-key-id", encode([bytes(16) + RECORDS[0][16:]]), "Mismatch"),
         case("zero-key-material", encode([RECORDS[0][:16] + bytes(32)]), "Mismatch"),
         case("wrong-checksum", golden[:-1] + bytes([golden[-1] ^ 1]), "Mismatch")]
result = {"format": "bigbrotr-cursor-keyfile-public-fixtures/1",
          "warning": "Public fixed keys for tests only; never provision these keys.",
          "dataset_id": DATASET.hex(), "keyring_id": RING.hex(),
          "golden_key_ids": [record[:16].hex() for record in RECORDS],
          "golden_fingerprint": hashlib.sha256(golden[:-32]).hexdigest(), "cases": cases}
pathlib.Path(__file__).with_name("keyring-vectors.json").write_text(json.dumps(result, indent=2) + "\n")
