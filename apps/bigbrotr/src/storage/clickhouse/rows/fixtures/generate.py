#!/usr/bin/env python3
"""Independent, standard-library initial projection oracle; no product imports.

Run this file to reproduce golden.json. Inputs are structurally typed synthetic
facts, not cryptographic signature verification evidence. Row schemas and values
below are literal independent expectations, not extracted Rust declarations.
"""
import hashlib
import importlib.util
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
    if isinstance(value, bool):
        return b"\xf5" if value else b"\xf4"
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
    if kind == "bool":
        assert isinstance(value, bool)
        return bytes([int(value)])
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

# Existing five source facts still have an explicit empty Measurement target.
targets["measurement_facts"] = []

# Independent literal compound record. UTC rollback, a known null/null REQ
# moment, EOSE delivery before verification completion, and nullable quality are
# deliberate. This adds no scientific packet/authority claim to the fixture.
m_profile = [1,300000000,15000000,10000000,1,1,1,1,1,1,0,2097152,262144,64,32,3,4194304,65536,2000000,9,4096,64,0,4000000]
m_clock = [reference(2,6),1,0,5]
m_open = [[-5,0],[None,1],[0,2]]
m_read = [[None,None],[event_id,[-10,3],[-20,5]],[0,4],True,True,False]
m_counters = [6,3,1,700,680,140]
m_observed = [[0,[0],-1],m_open,m_read,m_counters]
m_ended = [-100,6]
m_payload = [bytes([0x44])*16,[[0,reference(1,7)],reference(2,5),reference(6,1),reference(3,8),reference(5,10)],m_profile,m_clock,[2,[m_observed,m_ended,[0],1]]]
m_digest = digest("fact",[source(6),"measurement",1,scope,-(2**63)+6,m_payload])
m_values = [bytes([0x44])*16,0,reference(1,7),reference(2,5),reference(6,1),reference(3,8),reference(5,10),m_profile,m_clock,2,None,
            [0,0,None,-1],m_open,m_read,m_counters,m_ended,[0,None,None,None],1]
nullable_u16 = ("optional","u16")
moment_type = [("optional","i64"),("optional","u64")]
optional_moment = ("optional_tuple",moment_type)
open_type = [optional_moment]*3
read_type = [optional_moment,("optional_tuple",["bytes",moment_type,moment_type]),optional_moment,"bool","bool","bool"]
m_types = ["bytes","u16",reference_type,reference_type,reference_type,reference_type,reference_type,["u64"]*24,
           [reference_type,"u64",("optional","u64"),"u32"],"u16",("optional_tuple",[source_type,"bytes"]),
           ("optional_tuple",["u16","u16",nullable_u16,("optional","i64")]),("optional_tuple",open_type),
           ("optional_tuple",read_type),("optional_tuple",["u64"]*6),optional_moment,
           ("optional_tuple",["u16",nullable_u16,nullable_u16,nullable_u16]),nullable_u16]
m_common = [generation,source(6),1,scope,-(2**63)+6,m_digest,0]
m_cut = [dataset,17,[[7,2,100],[7,3,6]]]
m_row = m_common + m_values
m_schema = common_types + m_types
m_chunk = [generation,"measurement_facts",2,m_cut,0,[[source(6),["measurement_facts",0],m_row]]]
measurement = {"fact_digest":m_digest.hex(),"rows_digest":digest("projection-chunk",m_chunk).hex(),
               "row_binary_bytes":len(wire(m_schema,m_row)),"row_binary_hex":wire(m_schema,m_row).hex()}

# The separately maintained literal SQL schema oracle supplies only independent
# column names/types. No product declaration/serializer derives these bytes.
spec = importlib.util.spec_from_file_location("projection_schema_literal", Path(__file__).resolve().parents[7] / "experiments/clickhouse-frontier/initial_schema.py")
independent_schema = importlib.util.module_from_spec(spec)
spec.loader.exec_module(independent_schema)
m_columns = independent_schema.COMMON + independent_schema.MEASUREMENT
assert len(m_columns) == 25
m_header = leb(len(m_columns)) + b"".join(wire("text", name) for name, _ in m_columns) + b"".join(wire("text", ty) for _, ty in m_columns)
measurement["row_binary_with_names_and_types_hex"] = (m_header + wire(m_schema, m_row)).hex()

results = {}
for name, rows in targets.items():
    values = [[identity, [name, ordinal], value] for identity, ordinal, value, _ in rows]
    results[name] = {
        "row_count": len(rows),
        "rows_digest": digest("projection-chunk", [generation, name, 2, cut, 0, values]).hex(),
        "row_binary_bytes": sum(len(wire(types, value)) for _, _, value, types in rows),
        "empty_digest": digest("projection-chunk", [generation, name, 2, cut, 0, []]).hex(),
    }
output = {"fact_digests": [item.hex() for item in facts], "body_digest": body_digest.hex(), "targets": results, "measurement": measurement}
Path(__file__).with_name("golden.json").write_text(json.dumps(output, indent=2) + "\n")
