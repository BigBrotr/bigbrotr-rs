#!/usr/bin/env python3
"""Research only: validate finite gateway examples against pinned upstream MCP schemas.

No BigBrotr handler, relay, signer, payment, or network call is implemented here.
Run with jsonschema==4.25.1 and an existing pinned gateway-mcp-spec checkout.
"""
import copy
import hashlib
import json
from pathlib import Path
import subprocess

from jsonschema import validators

ROOT = Path(__file__).resolve().parents[2]
SPEC = ROOT / ".research/checkouts/gateway-mcp-spec"
REV = "f56f204f6290f6531b14d5734eb3e0a10f0eb201"
assert subprocess.check_output(["git", "-C", str(SPEC), "rev-parse", "HEAD"], text=True).strip() == REV

schemas = {}
digests = {}
for version in ("2025-11-25", "2026-07-28"):
    raw = (SPEC / "schema" / version / "schema.json").read_bytes()
    schemas[version] = json.loads(raw)
    digests[version] = hashlib.sha256(raw).hexdigest()

def valid(version, definition, value):
    schema = schemas[version]
    key = "$defs" if "$defs" in schema else "definitions"
    selected = {**schema, "$ref": f"#/{key}/{definition}"}
    cls = validators.validator_for(selected)
    return cls(selected).is_valid(value)

checks = []
def check(name, actual, expected):
    checks.append({"name": name, "observed": actual, "expected": expected, "pass": actual == expected})

legacy_request = {"jsonrpc": "2.0", "id": "read-1", "method": "tools/call", "params": {
    "name": "bigbrotr_query", "arguments": {"resource": "events", "filters": [{}], "page_size": 100}}}
modern_request = copy.deepcopy(legacy_request)
modern_request["params"]["_meta"] = {
    "io.modelcontextprotocol/protocolVersion": "2026-07-28",
    "io.modelcontextprotocol/clientCapabilities": {},
    "io.modelcontextprotocol/clientInfo": {"name": "gateway-research", "version": "1"}}
page = {"resource": "events", "rows": [], "next_cursor": None,
        "source": {"cut": "fixture-only", "policy_epoch": "9007199254740993"}}
result = {"content": [{"type": "text", "text": json.dumps(page, separators=(",", ":"))}],
          "structuredContent": page, "isError": False}
modern_result = {**result, "resultType": "complete"}
for version, request, output in (("2025-11-25", legacy_request, result), ("2026-07-28", modern_request, modern_result)):
    check(version + " finite request", valid(version, "CallToolRequest", request), True)
    check(version + " structured result", valid(version, "CallToolResult", output), True)
    check(version + " batch is not a request", valid(version, "CallToolRequest", [request]), False)

check("modern request rejects absent version metadata", valid("2026-07-28", "CallToolRequest", legacy_request), False)
check("modern result rejects absent resultType", valid("2026-07-28", "CallToolResult", result), False)
check("modern tool catalog rejects absent caching fields", valid("2026-07-28", "ListToolsResult", {"resultType": "complete", "tools": []}), False)
check("modern uncached private tool catalog", valid("2026-07-28", "ListToolsResult", {"resultType": "complete", "tools": [], "ttlMs": 0, "cacheScope": "private"}), True)

# MCP deliberately does not validate an individual tool's application arguments.
sql_request = copy.deepcopy(modern_request)
sql_request["params"]["arguments"] = {"sql": "SELECT * FROM events"}
check("MCP envelope alone permits arbitrary tool arguments", valid("2026-07-28", "CallToolRequest", sql_request), True)

def unique_pairs(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON key")
        result[key] = value
    return result

duplicate = '{"resource":"events","resource":"event_history"}'
try:
    json.loads(duplicate, object_pairs_hook=unique_pairs)
    duplicate_rejected = False
except ValueError:
    duplicate_rejected = True
check("duplicate keys need a parser guard before schema validation", duplicate_rejected, True)

# Counts and BigBrotr metadata carry large integers as strings; event numeric fields
# retain their Nostr wire rules. This is representation evidence, not event validation.
roundtrip = json.loads(json.dumps(modern_result))
check("large fact integer remains a string", roundtrip["structuredContent"]["source"]["policy_epoch"], "9007199254740993")
output = {"scope": "isolated protocol/schema research; no product integration",
          "spec_revision": REV, "schema_sha256": digests, "checks": checks,
          "passed": sum(c["pass"] for c in checks), "total": len(checks)}
print(json.dumps(output, indent=2))
raise SystemExit(0 if all(c["pass"] for c in checks) else 1)
