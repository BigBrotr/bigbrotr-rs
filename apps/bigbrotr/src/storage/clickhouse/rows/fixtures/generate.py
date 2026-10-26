#!/usr/bin/env python3
"""Independent, standard-library initial projection oracle; no product imports.

Run this file to reproduce golden.json. Inputs are structurally typed synthetic
facts, not cryptographic signature verification evidence. Row schemas and values
below are literal independent expectations, not extracted Rust declarations.
"""
import hashlib
import json
from pathlib import Path


def head(kind, value):
    if value < 24:
        return bytes([kind * 32 + value])
    for marker, width in ((24, 1), (25, 2), (26, 4), (27, 8)):
        if value < 1 << (width * 8):
            return bytes([kind * 32 + marker]) + value.to_bytes(width, "big")
    raise ValueError("integer outside profile")


def cbor(value):
    if value is None:
        return b"\xf6"
    if isinstance(value, int):
        return head(0, value) if value >= 0 else head(1, -1 - value)
    if isinstance(value, bytes):
        return head(2, len(value)) + value
    if isinstance(value, str):
        raw = value.encode("utf-8")
        return head(3, len(raw)) + raw
    if isinstance(value, list):
        return head(4, len(value)) + b"".join(map(cbor, value))
    if isinstance(value, dict):
        pairs = sorted((cbor(key), cbor(item)) for key, item in value.items())
        return head(5, len(pairs)) + b"".join(key + item for key, item in pairs)
    raise TypeError(type(value))


def digest(domain, value):
    return hashlib.sha256(b"bigbrotr/" + domain.encode() + b"/1\0" + cbor(value)).digest()


def leb(value):
    result = bytearray()
    while value >= 128:
        result.append((value & 127) | 128)
        value >>= 7
    result.append(value)
    return bytes(result)


def wire(kind, value):
    if isinstance(kind, tuple):
        constructor, child = kind
        if constructor == "optional":
            return b"\1" if value is None else b"\0" + wire(child, value)
        if constructor == "optional_tuple":
            return b"\0" if value is None else b"\1" + wire(child, value)
        if constructor == "array":
            return leb(len(value)) + b"".join(wire(child, item) for item in value)
    if isinstance(kind, list):
        assert len(kind) == len(value)
        return b"".join(wire(field, item) for field, item in zip(kind, value))
    if kind == "bytes":
        return value
    if kind == "text":
        raw = value.encode()
        return leb(len(raw)) + raw
    widths = {"u16": 2, "u32": 4, "u64": 8, "i64": 8}
    return value.to_bytes(widths[kind], "little", signed=kind == "i64")


dataset = bytes([0x11]) * 16
domain = bytes([0x22]) * 16
generation = bytes([0x33]) * 16
event_id = bytes([0xaa]) * 32
pubkey = bytes([0xbb]) * 32
signature = bytes([0xcc]) * 64
source = lambda seq: [dataset, 7, 3, seq]
reference = lambda kind, ordinal: [dataset, kind, domain, ordinal]
scope = reference(4, 9)
cut = [dataset, 17, [[7, 2, 100], [7, 3, 5]]]
source_type = ["bytes", "u32", "u64", "u64"]
reference_type = ["bytes", "u16", "bytes", "u64"]
common_types = ["bytes", source_type, "u16", reference_type, "i64", "bytes", "u64"]
optional_text = ("optional", "text")
optional_bytes = ("optional", "bytes")
optional_ref = ("optional_tuple", reference_type)
optional_source = ("optional_tuple", source_type)

tags = [["t", "z", "ignored"], ["e", "α"], ["t", "a"], ["t", "z"],
        ["T", ""], ["ab", "skip"], ["1", "skip"], ["é", "skip"], ["t"], [],
        ["p", "first", "second"], ["p", "first"], ["x", "\0\n"]]
body = [event_id, pubkey, 2**64 - 1, 65535, tags, '{"text": "still a string"}\n\0é']
body_digest = digest("body", body)
descriptor = {"clock": "external-utc", "vantage": "lab-a", "software": "BigBrotr",
              "method_id": reference(5, 10), "public_key": pubkey,
              "route_class": "tor", "software_version": "v1"}
dictionary = [reference(2, 5), 1, "observer", 1, descriptor]
payloads = [
    ("dictionary", dictionary),
    ("event-payload", body + [reference(8, 42)]),
    ("event-eligibility", [event_id, body_digest, signature, -44, "import", reference(9, 11)]),
    ("event-receive", [event_id, -100, reference(2, 5), [reference(2, 6), 1, None, 5], [dataset, 7, 2, 99]]),
    ("provenance", [event_id, reference(1, 7), reference(2, 5), reference(3, 8), -200,
                    [reference(2, 6), 1, 2, 7], reference(5, 10), [dataset, 7, 2, 100]]),
]
facts = [digest("fact", [source(seq), kind, 1, scope, -(2**63) + seq, payload])
         for seq, (kind, payload) in enumerate(payloads, 1)]


def common(seq, discriminator=0):
    return [generation, source(seq), 1, scope, -(2**63) + seq, facts[seq - 1], discriminator]


targets = {}


def target(name, seq, values, types):
    targets[name] = [(source(seq), 0, common(seq) + values, common_types + types)]


target("dictionaries", 1, [reference(2, 5), 1, "observer", 1, digest("dictionary", dictionary),
    None, None, None, "external-utc", "lab-a", "BigBrotr", reference(5, 10), pubkey,
    "tor", "v1", None, None, None, None, None, None, None, None, None, None, None, None, None],
    [reference_type, "u16", "text", "u16", "bytes", optional_bytes,
     optional_text, optional_text, optional_text, optional_text, optional_text, optional_ref,
     optional_bytes, optional_text, optional_text, optional_text, optional_ref,
     optional_text, optional_text, optional_text, optional_text, optional_text,
     ("optional", "u16"), optional_text, optional_text, ("optional", "u16"), optional_bytes, optional_bytes])
target("events", 2, body + [reference(8, 42), body_digest],
       ["bytes", "bytes", "u64", "u16", ("array", ("array", "text")), "text", reference_type, "bytes"])
target("event_eligibility_facts", 3, payloads[2][1],
       ["bytes", "bytes", "bytes", "i64", "text", optional_ref])
target("event_receive_facts", 4,
       [event_id, -100, reference(2, 5), reference(2, 6), 1, None, 5, [dataset, 7, 2, 99]],
       ["bytes", "i64", reference_type, reference_type, "u64", ("optional", "u64"), "u32", optional_source])
for name in ("event_id_index", "event_author_index", "event_time_index"):
    target(name, 2, [event_id, pubkey, 2**64 - 1, 65535, body_digest], ["bytes", "bytes", "u64", "u16", "bytes"])
# Literal expected postings: no call to the production normalizer or a matching
# filter/sort reducer. Extra signed tag fields are deliberately absent here.
postings = [("T", ""), ("e", "α"), ("p", "first"), ("t", "a"), ("t", "z"), ("x", "\0\n")]
targets["event_tag_index"] = [(source(2), i, common(2, i) + [event_id, key, value],
                              common_types + ["bytes", "text", "text"])
                             for i, (key, value) in enumerate(postings)]
for name in ("provenance_by_event", "provenance_by_relay"):
    target(name, 5, [event_id, reference(1, 7), reference(2, 5), reference(3, 8), -200,
                    reference(2, 6), 1, 2, 7, reference(5, 10), [dataset, 7, 2, 100]],
           ["bytes", reference_type, reference_type, reference_type, "i64", reference_type,
            "u64", ("optional", "u64"), "u32", reference_type, optional_source])

results = {}
for name, rows in targets.items():
    values = [[identity, [name, ordinal], value] for identity, ordinal, value, _ in rows]
    results[name] = {
        "row_count": len(rows),
        "rows_digest": digest("projection-chunk", [generation, name, 1, cut, 0, values]).hex(),
        "row_binary_bytes": sum(len(wire(types, value)) for _, _, value, types in rows),
        "empty_digest": digest("projection-chunk", [generation, name, 1, cut, 0, []]).hex(),
    }
output = {"fact_digests": [item.hex() for item in facts], "body_digest": body_digest.hex(), "targets": results}
Path(__file__).with_name("golden.json").write_text(json.dumps(output, indent=2) + "\n")
