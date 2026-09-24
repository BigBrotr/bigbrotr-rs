#!/usr/bin/env python3
"""Independent fixed-width command identity; no product code or SQL consumer."""
import hashlib
import json
from pathlib import Path

vectors = []
for name, revisions in [
    ("ordinary", (4, 9, 10)),
    ("maximum", (2**64 - 1, 2**64 - 1, 2**64 - 1)),
]:
    ids = {key: bytes([index]) * 16 for index, key in enumerate(
        ("dataset", "actor", "command_key", "job_id"), start=1)}
    fields = ("expected_job_revision", "expected_config_revision", "expected_policy_revision")
    raw = b"bigbrotr/export-resume/1\0" + b"".join(ids.values())
    raw += b"".join(revision.to_bytes(8, "big") for revision in revisions)
    vectors.append(dict(name=name, **{key: value.hex() for key, value in ids.items()},
        command=dict(zip(fields, map(str, revisions))), bytes_hex=raw.hex(),
        sha256=hashlib.sha256(raw).hexdigest()))

Path(__file__).with_name("export-resume.json").write_text(json.dumps(dict(
    format="bigbrotr-export-resume-fixture/1", oracle=Path(__file__).name,
    vectors=vectors), indent=2) + "\n")
