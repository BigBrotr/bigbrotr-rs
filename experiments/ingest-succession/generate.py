#!/usr/bin/env python3
"""Deterministic portable bytes from the selected contract, using only stdlib.

These are codec/hash inputs, not native outcomes, authority or positive proofs.
No product module, Rust executable, database or native store is consulted.
"""
import argparse
import hashlib
import json
from pathlib import Path
import struct


OUTPUT = Path(__file__).with_name("portable-handoff-v2.json")
CONTRACT_REVISION = "12c3534c6c970839a4371fc00b4ac3eac6730cb0"
DATASET = bytes.fromhex("102132435465768798a9bacbdcedfe0f")
RESOURCE = bytes.fromhex("2031425364758697a8b9cadbecfd0e1f")
SLOT = bytes.fromhex("30415263748596a7b8c9daebfc0d1e2f")
RECOVERY = bytes.fromhex("405162738495a6b7c8d9eafb0c1d2e3f")
TOPOLOGY = 0x0102030405060708
POLICY = 0x8000000000000011
U64_MAX = (1 << 64) - 1


def digest(value):
    return hashlib.sha256(value).hexdigest()


def u64(value):
    return struct.pack(">Q", value)


def manifest(epoch, *, kinds=tuple(range(1, 10)), identity_number=None):
    """Literal manifest table, including deliberately unsigned high coordinates."""
    number = epoch if identity_number is None else identity_number
    schema = 1 if epoch == 1 else 2 if epoch == 2 else 3
    incarnation = struct.pack(">QQ", 0x5162738495A6B700 + number, 0x89ABCDEF01234567)
    header = struct.pack(">BBHHHH", 1, 1, schema, 1, 1, 1)
    fixed = (header + DATASET + u64(TOPOLOGY + number - 1) + RESOURCE
             + struct.pack(">I", 0x8899AABB) + u64(epoch) + incarnation
             + RECOVERY + u64(POLICY + number - 1) + bytes((1, len(kinds))))
    namespaces = b"".join(
        struct.pack(">HQQ", kind, 0x61728394A5B6C700 + number,
                    0x91A2B3C4D5E6F700 + kind)
        for kind in kinds
    )
    assert len(fixed) == 104
    return fixed + namespaces


def key(epoch):
    return b"\x01\x0a" + DATASET + RESOURCE + u64(epoch)


def legacy_parent(previous, target):
    # This fixed digest is an uninterpreted scalar, not a claimed epoch1 proof.
    closure = bytes(range(0x80, 0xA0))
    return (b"\x01\x0a" + SLOT + struct.pack(">QQQ", 1, 4, 12) + closure
            + struct.pack(">H", len(previous)) + previous
            + struct.pack(">H", len(target)) + target)


def value(kind, epoch, issued, cut, closure, parent, previous, target):
    return (b"\x02\x0a" + bytes((kind,)) + SLOT
            + struct.pack(">QQQ", epoch, issued, cut) + bytes.fromhex(closure)
            + hashlib.sha256(parent).digest()
            + struct.pack(">H", len(previous)) + previous
            + struct.pack(">H", len(target)) + target)


def vector(identifier, kind, epoch, issued, cut, closure, parent, previous, target):
    encoded = value(kind, epoch, issued, cut, closure, parent, previous, target)
    return dict(
        id=identifier, key_hex=key(epoch).hex(), value_hex=encoded.hex(),
        value_sha256=digest(encoded),
        fields=dict(terminal_kind=kind, producer_slot_hex=SLOT.hex(),
                    predecessor_epoch=str(epoch), issued_through=str(issued),
                    last_complete_sequence=str(cut), closure_digest_hex=closure,
                    incoming_handoff_sha256=digest(parent),
                    previous_manifest_hex=previous.hex(), next_manifest_hex=target.hex()),
        profile_epoch_admitted=(epoch + 1 <= 255),
    )


def closure_vector(identifier, kind, parent, current, epoch, records, cut):
    domain = (b"bigbrotr/ingest-successor-source-closure/1\0" if kind == 1
              else b"bigbrotr/ingest-successor-empty-closure/1\0")
    prefix = (domain + hashlib.sha256(parent).digest()
              + hashlib.sha256(current).digest() + SLOT + u64(epoch) + u64(len(records)))
    body = b"".join(u64(i) + bytes.fromhex(row["input_sha256"])
                    + hashlib.sha256(bytes.fromhex(row["outcome_hex"])).digest()
                    for i, row in enumerate(records, 1))
    preimage = prefix + body + u64(cut)
    return dict(id=identifier, terminal_kind=kind, incoming_handoff_hex=parent.hex(),
                manifest_hex=current.hex(), producer_slot_hex=SLOT.hex(), epoch=str(epoch),
                issued_through=str(len(records)), last_complete_sequence=str(cut),
                records=records, preimage_hex=preimage.hex(), sha256=digest(preimage),
                scope="Hash recipe only; synthetic request/outcome bytes are not validated effects")


def build():
    manifests = {epoch: manifest(epoch) for epoch in range(1, 5)}
    parent = legacy_parent(manifests[1], manifests[2])
    literal_nonzero = bytes(range(0xA0, 0xC0)).hex()
    literal_zero = bytes(range(0xC0, 0xE0)).hex()
    first = vector("full-2-to-3-activated", 1, 2, 2, 5, literal_nonzero,
                   parent, manifests[2], manifests[3])
    first_bytes = bytes.fromhex(first["value_hex"])
    records = [dict(sequence=str(i), input_hex=bytes((0x10 + i, 0x80, i)).hex(),
                    input_sha256=digest(bytes((0x10 + i, 0x80, i))),
                    outcome_hex=bytes((0x20 + i, 0xFF, i, 0)).hex())
               for i in (1, 2)]
    closures = [
        closure_vector("epoch3-activated-two-requests", 1, first_bytes, manifests[3], 3, records, 5),
        closure_vector("epoch3-activated-zero", 1, first_bytes, manifests[3], 3, [], 0),
        closure_vector("epoch3-empty-zero", 2, first_bytes, manifests[3], 3, [], 0),
    ]
    valid = [first,
             vector("full-2-to-3-activated-zero", 1, 2, 0, 0, literal_zero,
                    parent, manifests[2], manifests[3]),
             vector("full-2-to-3-empty-zero", 2, 2, 0, 0, literal_zero,
                    parent, manifests[2], manifests[3])]
    for closure in closures:
        valid.append(vector("full-3-to-4-" + closure["id"].removeprefix("epoch3-"),
                            closure["terminal_kind"], 3, len(closure["records"]),
                            int(closure["last_complete_sequence"]), closure["sha256"],
                            first_bytes, manifests[3], manifests[4]))
    for identifier, kinds in (("minimum-empty-namespaces", ()),
                              ("sparse-namespaces", (1, 3, 9))):
        old, previous, target = (manifest(i, kinds=kinds) for i in (1, 2, 3))
        valid.append(vector(identifier, 2, 2, 0, 0, literal_zero,
                            legacy_parent(old, previous), previous, target))
    # Portable validity is independent of the installed lineage profile cap.
    for identifier, epoch, count in (("profile-254-to-255", 254, 64),
                                     ("profile-255-to-256", 255, 64),
                                     ("unsigned-upper-neighbor", U64_MAX - 1, 64)):
        previous = manifest(epoch, identity_number=5)
        target = manifest(epoch + 1, identity_number=6)
        valid.append(vector(identifier, 1, epoch, count, U64_MAX, literal_nonzero,
                            first_bytes, previous, target))

    invalid = []

    def bad(identifier, offset, replacement, reason, *, base=first):
        original = bytes.fromhex(base["value_hex"])
        altered = original[:offset] + replacement + original[offset + len(replacement):]
        invalid.append(dict(id=identifier, base=base["id"], value_hex=altered.hex(), reason=reason))

    previous_offset = 109
    next_length_offset = previous_offset + len(manifests[2])
    next_offset = next_length_offset + 2
    for identifier, offset, replacement, reason in (
        ("wrong-value-version", 0, b"\x01", "Value version must be2"),
        ("wrong-value-tag", 1, b"\x0b", "Value type must be0a"),
        ("unknown-terminal-kind", 2, b"\x03", "Terminal kind is1 or2"),
        ("zero-terminal-kind", 2, b"\0", "Terminal kind is1 or2"),
        ("zero-slot", 3, bytes(16), "Producer slot is nonzero"),
        ("zero-predecessor", 19, u64(0), "Predecessor epoch is at least2"),
        ("legacy-predecessor", 19, u64(1), "Version2 cannot represent predecessor1"),
        ("issued65", 27, u64(65), "Issued exceeds64"),
        ("empty-with-issued", 2, b"\x02", "Empty kind requires issued0/source0"),
        ("activated-zero-positive-cut", 27, u64(0), "Issued0 requires source0"),
        ("previous-too-short", 107, struct.pack(">H", 103), "Manifest length below104"),
        ("previous-too-long", 107, struct.pack(">H", 267), "Manifest length above266"),
        ("target-too-short", next_length_offset, struct.pack(">H", 103), "Manifest length below104"),
        ("target-too-long", next_length_offset, struct.pack(">H", 267), "Manifest length above266"),
        ("previous-schema1", previous_offset + 2, struct.pack(">H", 1), "Epoch2 predecessor needs schema2"),
        ("target-schema2", next_offset + 2, struct.pack(">H", 2), "Target needs schema3"),
        ("target-wrong-epoch", next_offset + 54, u64(4), "Target epoch must be predecessor+1"),
        ("previous-wrong-epoch", previous_offset + 54, u64(3), "Manifest and producer epoch differ"),
        ("target-topology-gap", next_offset + 26, u64(TOPOLOGY + 3), "Topology advances exactly one"),
        ("foreign-dataset", next_offset + 10, bytes.fromhex("f1" * 16), "Dataset must remain unchanged"),
        ("foreign-resource", next_offset + 34, bytes.fromhex("f2" * 16), "Owner resource must remain unchanged"),
        ("foreign-shard", next_offset + 50, struct.pack(">I", 7), "Shard must remain unchanged"),
        ("foreign-recovery", next_offset + 78, bytes.fromhex("f3" * 16), "Recovery identity must remain unchanged"),
        ("reused-incarnation", next_offset + 62, manifests[2][62:78], "Target incarnation must differ"),
        ("reused-predecessor-domain", next_offset + 106, manifests[2][106:122], "Domain must be fresh against predecessor"),
        ("duplicate-target-domain", next_offset + 124, manifests[3][106:122], "Target domains must be distinct"),
        ("duplicate-kind", next_offset + 122, struct.pack(">H", 1), "Namespace kinds strictly ordered"),
        ("unknown-kind", next_offset + 104, struct.pack(">H", 10), "Only assigned kinds1..9"),
        ("zero-domain", next_offset + 106, bytes(16), "Allocated domain must be nonzero"),
        ("unknown-protection", next_offset + 102, b"\x02", "Only LocalSynced protection1"),
        ("zero-dataset", previous_offset + 10, bytes(16), "Dataset must be nonzero"),
        ("unknown-manifest-profile", next_offset + 8, struct.pack(">H", 2), "Native profile must remain1"),
    ):
        bad(identifier, offset, replacement, reason)
    empty = valid[2]
    bad("empty-positive-cut", 35, u64(1), "Empty kind requires source0", base=empty)
    later = valid[3]
    bad("later-previous-schema2", previous_offset + 2, struct.pack(">H", 2),
        "Predecessor>=3 needs schema3", base=later)
    top_overflow = bytearray(bytes.fromhex(first["value_hex"]))
    top_overflow[previous_offset + 26:previous_offset + 34] = u64(U64_MAX)
    top_overflow[next_offset + 26:next_offset + 34] = u64(0)
    invalid.append(dict(id="topology-overflow", base=first["id"], value_hex=top_overflow.hex(), reason="Topology addition must not wrap"))
    epoch_overflow = bytearray(bytes.fromhex(valid[-1]["value_hex"]))
    epoch_overflow[19:27] = u64(U64_MAX)
    epoch_overflow[previous_offset + 54:previous_offset + 62] = u64(U64_MAX)
    epoch_overflow[next_offset + 54:next_offset + 62] = u64(1)
    invalid.append(dict(id="epoch-overflow", base=valid[-1]["id"], value_hex=epoch_overflow.hex(), reason="Epoch addition must not wrap"))
    invalid.append(dict(id="trailing-byte", base=first["id"], value_hex=first["value_hex"] + "00", reason="No trailing extension"))
    sparse = valid[7]
    bad("different-kind-set", 109 + 158 + 2 + 104 + 18, struct.pack(">H", 4),
        "Previous and target kind sets must match", base=sparse)

    chain_checks = [dict(id="linked-legacy-to-v2", parent_value_hex=parent.hex(),
                         child_value_hex=first["value_hex"], expected_linked=True,
                         reason="Exact legacy parent bytes and adjacent manifest"),
                    dict(id="linked-v2-to-v2", parent_value_hex=first["value_hex"],
                         child_value_hex=later["value_hex"], expected_linked=True,
                         reason="Exact version2 parent bytes and adjacent manifest")]
    for identifier, offset, replacement, reason in (
        ("wrong-parent-hash", 75, bytes.fromhex("ee" * 32), "Parent hash differs"),
        ("wrong-parent-slot", 3, bytes.fromhex("ef" * 16), "Stable slot differs"),
        ("adjacent-policy-mismatch", previous_offset + 94, u64(POLICY + 9), "Parent next and child previous differ"),
    ):
        original = bytes.fromhex(later["value_hex"])
        changed = original[:offset] + replacement + original[offset + len(replacement):]
        chain_checks.append(dict(id=identifier, parent_value_hex=first["value_hex"],
                                 child_value_hex=changed.hex(), expected_linked=False,
                                 reason=reason, locally_valid=True))

    profile = dict(membership_limit=32768, metadata_ceiling_bytes=262144,
                   scratch_bytes=131072, compact_entry_max_bytes=512,
                   retained_input_max_files=64, retained_input_max_bytes=33554432,
                   cases=[dict(total_epochs=n, topology_memberships=n * (n + 1) // 2,
                               metadata_formula_bytes=131072 + 512 * n,
                               admitted=n <= 255) for n in (254, 255, 256)])
    return dict(format="bigbrotr-repeatable-handoff-fixtures/1", contract_revision=CONTRACT_REVISION,
                scope="Portable codec, chain-link and hash arithmetic only; no positive effects, authority, native, SQL or installed qualification",
                scalar_note="Legacy closure scalars are fixed synthetic bytes; subset/high-epoch vectors are portable-only, not connected lineage proofs",
                valid=valid, invalid=invalid, chain_checks=chain_checks,
                truncation_sweeps=[v["id"] for v in valid], closure_vectors=closures, profile=profile)


def selfcheck(fixture):
    """Inspect explicit offsets and relationships independently of field packing."""
    by_id = {v["id"]: v for v in fixture["valid"]}
    assert len(by_id) == len(fixture["valid"])
    lengths = set()
    for item in by_id.values():
        raw = bytes.fromhex(item["value_hex"])
        fields = item["fields"]
        previous = bytes.fromhex(fields["previous_manifest_hex"])
        target = bytes.fromhex(fields["next_manifest_hex"])
        lengths.add(len(raw))
        assert raw[:2] == bytes((2, 10)) and raw[2] == fields["terminal_kind"]
        assert raw[3:19].hex() == fields["producer_slot_hex"]
        assert tuple(struct.unpack(">QQQ", raw[19:43])) == tuple(int(fields[k]) for k in ("predecessor_epoch", "issued_through", "last_complete_sequence"))
        assert raw[43:75].hex() == fields["closure_digest_hex"]
        assert raw[75:107].hex() == fields["incoming_handoff_sha256"]
        assert struct.unpack(">H", raw[107:109])[0] == len(previous)
        assert raw[109:109 + len(previous)] == previous
        boundary = 109 + len(previous)
        assert struct.unpack(">H", raw[boundary:boundary + 2])[0] == len(target)
        assert raw[boundary + 2:] == target
        assert len(raw) == 111 + len(previous) + len(target)
        assert digest(raw) == item["value_sha256"]
        assert bytes.fromhex(item["key_hex"]) == bytes((1, 10)) + previous[10:26] + previous[34:50] + raw[19:27]
        for m in (previous, target):
            assert len(m) == 104 + 18 * m[103]
        assert int.from_bytes(target[54:62], "big") == int(fields["predecessor_epoch"]) + 1
    assert lengths == {319, 427, 643}
    assert by_id["full-2-to-3-activated-zero"]["value_hex"] != by_id["full-2-to-3-empty-zero"]["value_hex"]
    assert len({v["id"] for v in fixture["invalid"]}) == len(fixture["invalid"])
    for item in fixture["invalid"]:
        assert item["value_hex"] != by_id[item["base"]]["value_hex"]
        assert item["value_hex"] not in {v["value_hex"] for v in by_id.values()}
    for chain in fixture["chain_checks"]:
        parent = bytes.fromhex(chain["parent_value_hex"])
        child = bytes.fromhex(chain["child_value_hex"])
        header = 74 if parent[0] == 1 else 107
        previous_size = int.from_bytes(parent[header:header + 2], "big")
        next_start = header + 2 + previous_size
        parent_next = parent[next_start + 2:]
        child_previous = child[109:109 + int.from_bytes(child[107:109], "big")]
        parent_slot = parent[2:18] if parent[0] == 1 else parent[3:19]
        linked = (hashlib.sha256(parent).digest() == child[75:107]
                  and parent_next == child_previous and parent_slot == child[3:19])
        assert linked == chain["expected_linked"], chain["id"]
    for closure in fixture["closure_vectors"]:
        assert digest(bytes.fromhex(closure["preimage_hex"])) == closure["sha256"]
        assert all(digest(bytes.fromhex(row["input_hex"])) == row["input_sha256"] for row in closure["records"])
    assert fixture["closure_vectors"][1]["sha256"] != fixture["closure_vectors"][2]["sha256"]
    profile = fixture["profile"]
    a, b = profile["cases"][1:]
    assert a == dict(total_epochs=255, topology_memberships=32640, metadata_formula_bytes=261632, admitted=True)
    assert b == dict(total_epochs=256, topology_memberships=32896, metadata_formula_bytes=262144, admitted=False)
    assert a["metadata_formula_bytes"] <= b["metadata_formula_bytes"] == profile["metadata_ceiling_bytes"]
    assert a["topology_memberships"] <= profile["membership_limit"] < b["topology_memberships"]
    return dict(valid_vectors=len(by_id), malformed_vectors=len(fixture["invalid"]),
                chain_cases=len(fixture["chain_checks"]), closure_hashes=len(fixture["closure_vectors"]),
                proper_prefix_cases=sum(len(bytes.fromhex(v["value_hex"])) for v in by_id.values()))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--write", action="store_true", help="Write the deterministic golden JSON")
    args = parser.parse_args()
    fixture = build()
    checked = selfcheck(fixture)
    encoded = (json.dumps(fixture, indent=2, sort_keys=True) + "\n").encode()
    if args.write:
        OUTPUT.write_bytes(encoded)
    else:
        assert OUTPUT.read_bytes() == encoded, "Golden fixture differs from deterministic generation"
    print(json.dumps(dict(status="PASS_PORTABLE_FIXTURE_SELFCHECK", **checked,
                          golden_bytes=len(encoded), golden_sha256=digest(encoded),
                          runtime_qualification="NOT_RUN"), sort_keys=True))


if __name__ == "__main__":
    main()
