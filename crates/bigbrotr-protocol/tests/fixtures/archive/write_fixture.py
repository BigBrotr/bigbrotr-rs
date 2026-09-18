"""Independent single-record private handoff fixture, not signed admission evidence."""
import hashlib
import json
import struct
from pathlib import Path

root = Path(__file__).resolve().parents[5]
fact = json.loads((root / "tests/fixtures/model/core-fingerprints.json").read_text())["vectors"][0]
assert fact["name"] == "event-payload"
payload = bytes.fromhex(fact["preimage_hex"])
digest = bytes.fromhex(fact["sha256"])
assert hashlib.sha256(payload).digest() == digest
dataset = bytes([1]) * 16
# Literal profile-1 canonical CBOR: shard0, epoch1, first=last=sequence2.
source = b"\x84\x50" + dataset + b"\x00\x01\x02"
preimage = b"bigbrotr/admission-batch/1\0\x86\x50" + dataset + b"\x00\x01\x02\x02\x81\x82" + source + b"\x58\x20" + digest
batch_digest = hashlib.sha256(preimage).digest()
wire = b"BBAP\0\0\0\x01" + dataset + struct.pack(">IQQI", 0, 1, 2, 1) + batch_digest + digest + struct.pack(">I", len(payload)) + payload
Path(__file__).with_name("single.json").write_text(json.dumps({"wire_hex": wire.hex(), "batch_digest": batch_digest.hex(), "fact_digest": digest.hex(), "preimage_hex": payload.hex()}, indent=2) + "\n")
