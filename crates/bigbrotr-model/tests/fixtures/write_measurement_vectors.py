"""Independent A05 literals; Python/OpenSSL plus the existing Node reference encoder."""
import copy
import hashlib
import json
from pathlib import Path
import subprocess
import tempfile


def head(major, value):
    if value < 24:
        return bytes([(major << 5) | value])
    for width, tag in [(1, 24), (2, 25), (4, 26), (8, 27)]:
        if value < 1 << (width * 8):
            return bytes([(major << 5) | tag]) + value.to_bytes(width, "big")
    raise ValueError("out of domain")


def cbor(value):
    if value is None:
        return b"\xf6"
    if isinstance(value, bool):
        return b"\xf5" if value else b"\xf4"
    if isinstance(value, int):
        return head(0, value) if value >= 0 else head(1, -1-value)
    if isinstance(value, bytes):
        return head(2, len(value)) + value
    if isinstance(value, str):
        raw = value.encode()
        return head(3, len(raw)) + raw
    if isinstance(value, list):
        return head(4, len(value)) + b"".join(map(cbor, value))
    raise ValueError("unsupported fixture input")


def ref(kind, ordinal=1):
    return [bytes([1])*16, kind, bytes([2])*16, ordinal]


def source(sequence, epoch=1):
    return [bytes([1])*16, 0, epoch, sequence]


# Independent literal constants, not extracted from the implementation.
profile = [1, 300000000, 15000000, 10000000, 1, 1, 1, 1, 1, 1, 0,
           2097152, 262144, 64, 32, 3, 4194304, 65536, 2000000, 9, 4096, 64, 0,
           4000000]
context = [[0, ref(1)], ref(2), ref(6), ref(3), ref(5)]
clock = [ref(2, 2), 1, None, 0]


def fact(sequence, state):
    return [source(sequence), "measurement", 1, ref(4), -1,
            [bytes([3])*16, copy.deepcopy(context), profile[:], copy.deepcopy(clock), state]]


def encoded(value):
    return b"bigbrotr/fact/1\0" + cbor(value)


def fact_ref(value):
    return [value[0], hashlib.sha256(encoded(value)).digest()]


def opened():
    return [[10, 1000], [9, 2000], [8, 3000]]  # valid UTC rollback


def observed(event=False, eose=False, prefix=False):
    event_value = [bytes([0xaa])*32, [6, 5000], [4, 8000]] if event else None
    return [[0, [0], -1], opened(),
            [[7, 4000], event_value, [5, 7000] if eose else None, prefix, True, False],
            [6, 3, 1 if event else 0, 700, 680, 140]]


unknown = fact(1, [0])
completion = fact(2, [1, observed(True, True)])
empty_terminal = [observed(False, True, True), [3, 10000], [0], 1]
empty = fact(3, [2, empty_terminal])
event_timeout = fact(4, [2, [observed(True), [3, 10000], [2, 6], 3]])
resolution = fact(5, [3, fact_ref(unknown), copy.deepcopy(empty_terminal)])
resolution[5][3] = [ref(2, 2), 1, 8, 1]
completed_terminal = [observed(True, True, True), [3, 10000], [0], 1]
completion_resolution = fact(6, [3, fact_ref(completion), completed_terminal])
immediate = fact(7, [2, [
    [[1, [1, 0], None], [None, None, None], [None, None, None, False, False, False], [0]*6],
    [None, 0], [5, 2, 0], 0]])
cap = fact(8, [2, [
    [[1, [0], 0], [[0, 0], None, [0, 0]], [[0, 0], None, None, False, False, False],
     [65, 0, 0, 130, 0, 100]], [0, 1], [6, 2], 2]])
cross_stream = copy.deepcopy(resolution)
cross_stream[0] = source(1, 2)
closed_observed = observed()
closed_observed[2][5] = True
closed = fact(9, [2, [closed_observed, [3, 10000], [1], 1]])
authority = fact(10, [2, [observed(True), [3, 10000], [3, 6], 3]])
silent_observed = [[1, [0], 0], opened(),
                   [[None, None], None, None, False, False, False],
                   [0, 0, 0, 0, 0, 140]]
cancelled = fact(11, [2, [silent_observed, [None, 5000], [4, 6], 3]])
tcp_prefix = fact(12, [1, [
    [1, [0], None], [[None, None], None, None],
    [None, None, None, False, False, False], [0]*6]])
upgrade_prefix = fact(13, [1, [
    [1, [0], None], [[None, None], None, [None, None]],
    [None, None, None, False, False, False], [0]*6]])
partial_input = fact(14, [1, [
    [1, [0], None], [[None, None], None, [None, None]],
    [None, None, None, False, False, False], [1, 0, 0, 5, 3, 0]]])

positive = [
    ("execution-unknown", unknown), ("completion-unknown-prefix-pending", completion),
    ("empty-eose-auth-utc-rollback", empty), ("event-before-timeout", event_timeout),
    ("resolve-execution-unknown", resolution), ("resolve-completion-unknown", completion_resolution),
    ("immediate-local-permission", immediate), ("physical-frame-cap", cap),
    ("resolve-across-owner-stream", cross_stream),
    ("correlated-closed", closed), ("event-before-authority-interruption", authority),
    ("cancelled-after-request-before-response", cancelled),
    ("completion-unknown-tcp-zero-counters", tcp_prefix),
    ("completion-unknown-upgrade-zero-counters", upgrade_prefix),
    ("completion-unknown-partial-input", partial_input),
]


def altered(value, path, replacement):
    value = copy.deepcopy(value)
    target = value
    for index in path[:-1]:
        target = target[index]
    target[path[-1]] = replacement
    return value


negative = [
    ("zero-attempt", altered(unknown, [5, 0], bytes(16))),
    ("unknown-subject", altered(unknown, [5, 1, 0, 0], 1)),
    ("wrong-route-kind", altered(unknown, [5, 1, 2, 1], 1)),
    ("foreign-clock", altered(unknown, [5, 3, 0, 0], bytes([9])*16)),
    ("altered-profile", altered(unknown, [5, 2, 13], 65)),
    ("zero-authority", altered(unknown, [5, 2, 23], 0)),
    ("expanded-authority", altered(unknown, [5, 2, 23], 5000001)),
    ("unknown-state", altered(unknown, [5, 4, 0], 4)),
    ("unknown-connect-result", altered(immediate, [5, 4, 1, 0, 0, 1, 1], 7)),
    ("missing-connect-witness", altered(immediate, [5, 4, 1, 0, 0], None)),
    ("eose-without-prefix-barrier", altered(empty, [5, 4, 1, 0, 2, 3], False)),
    ("request-without-upgrade", altered(empty, [5, 4, 1, 0, 1, 2], None)),
    ("monotonic-inversion", altered(empty, [5, 4, 1, 0, 1, 2, 1], 999)),
    ("terminal-before-observation", altered(empty, [5, 4, 1, 1, 1], 2)),
    ("unknown-termination", altered(empty, [5, 4, 1, 2], [7])),
    ("unknown-teardown", altered(empty, [5, 4, 1, 3], 4)),
    ("future-resolution-target", altered(resolution, [5, 4, 1, 0, 3], 6)),
    ("foreign-resolution-target", altered(resolution, [5, 4, 1, 0, 0], bytes([9])*16)),
    ("known-empty-eose-auth-zero-counters", altered(empty, [5, 4, 1, 0, 3], [0]*6)),
    ("known-completion-prefix-zero-counters", altered(completion, [5, 4, 1, 3], [0]*6)),
    ("known-request-null-time-zero-tx", altered(cancelled, [5, 4, 1, 0, 3, 5], 0)),
    ("known-distinct-messages-undercounted", altered(empty, [5, 4, 1, 0, 3, 1], 1)),
    ("known-distinct-frames-undercounted", altered(empty, [5, 4, 1, 0, 3, 0], 1)),
    ("known-distinct-payloads-undercounted", altered(empty, [5, 4, 1, 0, 3, 4], 1)),
    ("verified-event-without-candidate", altered(event_timeout, [5, 4, 1, 0, 3, 2], 0)),
    ("payload-exceeds-consumed-framed-bytes", altered(empty, [5, 4, 1, 0, 3, 4], 701)),
    ("partial-payload-exceeds-consumed-framed-bytes", altered(partial_input, [5, 4, 1, 3, 4], 6)),
    ("unknown-deadline-phase", altered(event_timeout, [5, 4, 1, 2], [2, 9])),
    ("unknown-authority-phase", altered(authority, [5, 4, 1, 2], [3, 9])),
    ("unknown-cancelled-phase", altered(cancelled, [5, 4, 1, 2], [4, 9])),
    ("unknown-failure-phase", altered(immediate, [5, 4, 1, 2], [5, 9, 0])),
    ("unknown-failure-class", altered(immediate, [5, 4, 1, 2], [5, 2, 11])),
    ("unknown-limit-code", altered(cap, [5, 4, 1, 2], [6, 8])),
]


def row(name, value):
    raw = encoded(value)
    digest = hashlib.sha256(raw).hexdigest()
    external = subprocess.check_output(["openssl", "dgst", "-sha256"], input=raw).decode().split()[-1]
    assert external == digest
    def portable(item):
        if isinstance(item, bytes):
            return {"$bytes": item.hex()}
        if isinstance(item, list):
            return list(map(portable, item))
        return item
    return {"name": name, "domain": "fact", "value": portable(value),
            "preimage_hex": raw.hex(), "sha256": digest}


output = {
    "format": "independent-measurement-vectors/1",
    "valid": [row(*value) for value in positive],
    "invalid": [row(*value) for value in negative],
}
with tempfile.NamedTemporaryFile(mode="w", suffix=".json") as reference_input:
    vectors = output["valid"] + output["invalid"]
    json.dump({"vectors": vectors}, reference_input)
    reference_input.flush()
    reference = Path(__file__).resolve().parents[4] / "experiments/identity-contract/reference.mjs"
    checked = json.loads(subprocess.check_output(["node", str(reference), reference_input.name]))
    assert len(checked) == len(vectors)
    for expected, actual in zip(vectors, checked):
        assert all(expected[key] == actual[key] for key in ("name", "preimage_hex", "sha256"))
for vector in output["valid"] + output["invalid"]:
    del vector["domain"]
    del vector["value"]
Path(__file__).with_name("measurement-v1.json").write_text(json.dumps(output, indent=2) + "\n")
