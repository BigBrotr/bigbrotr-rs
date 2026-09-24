#!/usr/bin/env python3
"""Independent frozen manifest/JSONL oracle; no product serializers or databases."""
import argparse
import copy
import hashlib
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
FIELDS = ("id", "pubkey", "created_at", "kind", "tags", "content", "sig")


def compact(value):
    return (json.dumps(value, ensure_ascii=False, separators=(",", ":")) + "\n").encode()


def expected():
    fixture = json.loads((ROOT / "tests/fixtures/protocol/signed-extremes.json").read_text())
    event = fixture["events"]["timestamp-zero"]["event"]
    contents = compact({name: event[name] for name in FIELDS})
    return {
        "format": "bigbrotr-signed-events-export/1",
        "artifact_id": "10" * 16,
        "job_id": "20" * 16,
        "visibility": "private",
        "fidelity": "full-signed-events",
        "origin": "retained-scoped-archive",
        "source": {
            "cut": {
                "dataset_id": "01" * 16,
                "topology_generation": "4",
                "bounds": [
                    {"stream": {"shard_id": 0, "owner_epoch": "1"}, "last_complete_sequence": "0"},
                    {"stream": {"shard_id": 1, "owner_epoch": "2"}, "last_complete_sequence": "10"},
                ],
            },
            "catalog_id": "30" * 16,
            "frontier_revision": "5",
            "snapshot_id": "11",
        },
        "selection": {
            "event_id": event["id"],
            "scope": {
                "dataset_id": "01" * 16,
                "dictionary_kind": 4,
                "allocation_domain": "02" * 16,
                "ordinal": "18446744073709551615",
            },
        },
        "accepted_revisions": {"configuration": "7", "policy": "9"},
        "included_families": ["signed-events"],
        "excluded_families": [
            "internal-payload-envelopes", "eligibility-facts", "first-local-receive",
            "relay-provenance", "imported-claims", "measurements", "documents",
            "dictionaries", "coverage", "derived-results", "deployment-state",
        ],
        "files": [{
            "file_id": "events", "path": "events.jsonl", "format": "nostr-jsonl/1",
            "bytes": str(len(contents)), "rows": "1", "sha256": hashlib.sha256(contents).hexdigest(),
        }],
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--write", action="store_true")
    parser.add_argument("--schema", action="store_true", help="also check the closed schema with jsonschema4.25.1")
    args = parser.parse_args()
    path = Path(__file__).with_name("signed-jsonl-manifest.json")
    data = compact(expected())
    if args.write:
        path.write_bytes(data)
    assert path.read_bytes() == data
    if args.schema:
        from importlib.metadata import version
        from jsonschema import Draft202012Validator
        assert version("jsonschema") == "4.25.1"
        schema = json.loads((ROOT / "contracts/artifacts/v1/export.schema.json").read_text())
        Draft202012Validator.check_schema(schema)
        validator = Draft202012Validator(schema)
        validator.validate(expected())
        for name in ("unknown", "format", "zero-id", "path", "extra-file", "empty-digest", "exclusions", "snapshot-zero"):
            value = expected()
            if name == "unknown": value["grant"] = "publish"
            elif name == "format": value["format"] = "bigbrotr-archive/1"
            elif name == "zero-id": value["artifact_id"] = "0" * 32
            elif name == "path": value["files"][0]["path"] = "../events.jsonl"
            elif name == "extra-file": value["files"].append(copy.deepcopy(value["files"][0]))
            elif name == "empty-digest": value["files"][0].update(rows="0", bytes="0")
            elif name == "exclusions": value["excluded_families"] = []
            else: value["source"]["snapshot_id"] = "0"
            assert not validator.is_valid(value), name
        empty = expected()
        empty["files"][0].update(rows="0", bytes="0", sha256=hashlib.sha256(b"").hexdigest())
        validator.validate(empty)
        print("PASS: Draft2020-12 schema, present/empty manifests and eight rejection controls; cross-field source authority and exact integer ceilings are runtime checks")
    print("PASS: independently encoded exact-ID manifest, complete two-stream cut and file digest")


if __name__ == "__main__":
    main()
