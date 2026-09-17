"""Independently assemble closed dictionary vectors; no product/CBOR library imports."""
import hashlib
import json
from pathlib import Path
import subprocess


def head(major, value):
    if value <= 23:
        return bytes([(major << 5) | value])
    for width, tag in [(1, 24), (2, 25), (4, 26), (8, 27)]:
        if value < 1 << (width * 8):
            return bytes([(major << 5) | tag]) + value.to_bytes(width, "big")
    raise ValueError("out of domain")


def cbor(value):
    if value is None:
        return b"\xf6"
    if isinstance(value, int):
        return head(0, value) if value >= 0 else head(1, -1-value)
    if isinstance(value, bytes):
        return head(2, len(value)) + value
    if isinstance(value, str):
        raw = value.encode("utf-8")
        return head(3, len(raw)) + raw
    if isinstance(value, list):
        return head(4, len(value)) + b"".join(map(cbor, value))
    if isinstance(value, dict):
        entries = sorted((cbor(key), cbor(item)) for key, item in value.items())
        return head(5, len(entries)) + b"".join(key + item for key, item in entries)
    raise ValueError("unsupported fixture value")


def ref(kind):
    return [bytes([1])*16, kind, bytes([2])*16, 1]


method = {
    "algorithm": "sha256-bip340", "algorithm_version": 1,
    "capability": "event-id-and-signature", "failure_taxonomy": "bigbrotr-nip01-verifier/1",
    "input": "nip01-event/1", "output_unit": "verification-result",
    "protocol_revision": "a2494f4f81d46684e5814a9bf35e2b1df978f955",
}
observer = {
    "public_key": bytes([0xbb])*32, "vantage": "lab-a", "route_class": "public",
    "software": "BigBrotr", "software_version": "0.1.0-dev",
    "method_id": ref(5), "clock": "system-utc",
}
rows = [
    ("visibility-public", 4, "visibility", {"class": "public"}),
    ("event", 8, "event", {"event_id": bytes([0xaa])*32}),
    ("endpoint", 1, "endpoint", {"url": "wss://relay.example/path?b=2&a=1"}),
    ("observer-known", 2, "observer", observer),
    ("observer-unknown", 2, "observer", {key: None for key in observer}),
    ("access-public", 3, "access-scope", {"class": "public", "label": None, "visibility_scope_id": ref(4)}),
    ("access-protected", 3, "access-scope", {"class": "protected", "label": "research-a", "visibility_scope_id": ref(4)}),
    ("event-verification", 5, "event-verification", method),
    ("lineage-native", 9, "dataset-lineage", {"format": "bigbrotr-native", "format_version": 1, "source_dataset_id": bytes([3])*16, "source_artifact_digest": bytes([0xcc])*32}),
    ("lineage-jsonl", 9, "dataset-lineage", {"format": "nostr-jsonl", "format_version": 1, "source_dataset_id": None, "source_artifact_digest": bytes([0xdd])*32}),
]
result = []
for name, kind, schema, value in rows:
    entry = [ref(kind), 1, schema, 1, value]
    for family, data in [("dictionary", entry), ("fact", [[bytes([1])*16, 0, 1, 1], "dictionary", 1, ref(4), -1, entry])]:
        encoded = b"bigbrotr/" + family.encode() + b"/1\0" + cbor(data)
        digest = hashlib.sha256(encoded).hexdigest()
        external = subprocess.check_output(["openssl", "dgst", "-sha256"], input=encoded).decode().split()[-1]
        assert external == digest
        result.append({"name": name, "family": family, "preimage_hex": encoded.hex(), "sha256": digest})
Path(__file__).with_name("dictionary-v1.json").write_text(json.dumps({"format": "independent-dictionary-vectors/1", "vectors": result}, indent=2) + "\n")
