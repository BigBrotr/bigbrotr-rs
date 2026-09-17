#!/usr/bin/env python3
"""In-memory protocol model; not an engine, fsync or process exclusion test."""

from __future__ import annotations

import argparse
import copy
import hashlib
import itertools
import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path


class Refused(Exception):
    pass


@dataclass
class Admission:
    epoch: int = 1
    outstanding_limit: int = 2
    closed: bool = False
    facts: set[str] = field(default_factory=set)
    receipts: dict[int, tuple[str, str]] = field(default_factory=dict)
    checkpoint: dict[int, str] = field(default_factory=dict)
    ack: int = 0
    gc: int = 0

    def admit(self, epoch: int, sequence: int, payload: str) -> str:
        if epoch != self.epoch or self.closed:
            raise Refused("epoch-closed")
        if sequence <= self.gc:
            return "RetiredRequest"
        if sequence in self.receipts:
            old, result = self.receipts[sequence]
            if old != payload:
                raise Refused("request-conflict")
            return result
        if len(self.receipts) >= self.outstanding_limit:
            raise Refused("outstanding-window-full")
        result = "AlreadyDurable" if payload in self.facts else "NewFacts"
        self.facts.add(payload)
        self.receipts[sequence] = (payload, result)
        return result

    def save_checkpoint(self, sequence: int) -> None:
        if sequence not in self.receipts:
            raise Refused("no-resolved-outcome")
        self.checkpoint[sequence] = self.receipts[sequence][1]

    def acknowledge(self, sequence: int) -> None:
        if any(n not in self.checkpoint for n in range(1, sequence + 1)):
            raise Refused("checkpoint-hole")
        self.ack = max(self.ack, sequence)

    def collect(self) -> None:
        self.receipts = {n: v for n, v in self.receipts.items() if n > self.ack}
        self.gc = self.ack


def admission_oracle(state: Admission) -> None:
    assert state.facts <= {"E", "F", "G"}
    assert state.gc <= state.ack
    assert set(range(1, state.gc + 1)) <= state.checkpoint.keys(), "retired outcome lost"
    assert all(n > state.gc for n in state.receipts), "GC/receipt range disagreement"


@dataclass
class Archive:
    prepared: bool = False
    transaction_resolved: bool = True
    catalog_up: bool = True
    committed: bool = False
    protected: bool = False
    attempts: int = 0
    pending_present: bool = True

    def prepare(self) -> None:
        if not self.transaction_resolved or not self.catalog_up:
            raise Refused("unknown-prior-commit")
        if self.committed:
            return
        self.prepared = True
        self.attempts += 1

    def commit_response_lost(self) -> None:
        assert self.prepared
        self.committed = True
        self.transaction_resolved = False

    def resolve(self) -> str:
        if not self.catalog_up:
            return "unknown"
        if not self.committed and not self.transaction_resolved:
            return "unknown"
        self.transaction_resolved = True
        return "committed" if self.committed else "not-committed"

    def reclaim(self) -> None:
        if self.resolve() != "committed" or not self.protected:
            raise Refused("archive-protection-unproven")
        self.pending_present = False


def archive_oracle(state: Archive) -> None:
    assert state.attempts <= 1, "unknown commit submitted again"
    assert state.pending_present or (state.committed and state.protected), "premature reclaim"


@dataclass
class Serving:
    incarnation: int = 1
    required: frozenset[str] = frozenset({"body", "tag", "scope", "empty-evidence"})
    chunks: set[tuple[int, str]] = field(default_factory=set)
    active: tuple[int, int] | None = None

    def insert(self, family: str) -> None:
        self.chunks.add((self.incarnation, family))

    def publish(self, cut: int) -> None:
        if any((self.incarnation, family) not in self.chunks for family in self.required):
            raise Refused("family-incomplete")
        self.active = (self.incarnation, cut)

    def readable(self) -> bool:
        return self.active is not None and self.active[0] == self.incarnation


def serving_oracle(state: Serving) -> None:
    if state.readable():
        assert {f for i, f in state.chunks if i == state.incarnation} == state.required, "phantom cut"


@dataclass
class Derived:
    checkpoint: int = 0
    source_cut: int = 7
    state_value: int = 1
    pending: dict[int, int] = field(default_factory=dict)
    ch: dict[int, int] = field(default_factory=dict)
    active: int = 0
    pins: set[int] = field(default_factory=lambda: {0})

    def dirty_expiry_commit(self) -> None:
        self.checkpoint += 1
        self.state_value = 0
        self.pending[self.checkpoint] = 0

    def materialize(self) -> None:
        self.ch[self.checkpoint] = self.pending[self.checkpoint]

    def activate(self) -> None:
        if self.checkpoint not in self.ch:
            raise Refused("unmaterialized-output")
        self.active = self.checkpoint


def derived_oracle(state: Derived) -> None:
    assert state.source_cut == 7, "dirty-only work invented source ingestion"
    assert state.checkpoint == 0 or state.checkpoint in state.pending or state.checkpoint in state.ch, "output skipped"
    assert state.active == 0 or state.ch.get(state.active) == 0, "phantom derived activation"
    assert 0 in state.pins, "old reader pin lost"


@dataclass
class JobOutbox:
    accepted: bool = False
    pin: bool = False
    cancelled: bool = False
    payload: bytes | None = None
    targets: dict[str, str] = field(default_factory=dict)
    authority: bool = True
    temporal_valid: bool = True
    sends: list[tuple[str, bytes, bool, bool]] = field(default_factory=list)

    def accept(self) -> None:
        self.accepted, self.pin = True, True

    def sign_durable(self) -> None:
        assert self.accepted
        self.payload = b"exact-signed-event-variant-A"
        self.targets = {"relay-a": "pending", "relay-b": "pending"}

    def send(self, target: str, reply_lost: bool = False) -> None:
        if not self.authority or not self.temporal_valid or self.cancelled:
            raise Refused("send-no-longer-authorized")
        assert self.payload is not None
        self.sends.append((target, self.payload, self.authority, self.temporal_valid))
        self.targets[target] = "unknown" if reply_lost else "accepted"


def outbox_oracle(state: JobOutbox) -> None:
    assert not state.accepted or state.pin or state.payload is not None, "job obligation lost"
    assert all(b == b"exact-signed-event-variant-A" for _, b, _, _ in state.sends), "retry resigned"
    assert all(a and t for _, _, a, t in state.sends), "unauthorized retry sent"


def refused(call) -> None:
    try:
        call()
    except Refused:
        return
    raise AssertionError("required refusal did not occur")


def negative(oracle, state) -> bool:
    try:
        oracle(state)
    except AssertionError:
        return True
    raise AssertionError("negative control was not detected")


def run() -> dict:
    cases: list[dict] = []

    def record(name: str, detail: str) -> None:
        cases.append({"name": name, "status": "PASS", "observation": detail})

    # All permutations are attempted. Illegal prerequisites are refused, and
    # durable handoff recovery finishes each schedule without changing identity.
    for order in itertools.permutations(("commit", "reply", "checkpoint", "ack", "gc")):
        s = Admission()
        seen_reply = False
        for action in order:
            try:
                if action == "commit":
                    s.admit(1, 1, "E")
                elif action == "reply":
                    seen_reply = 1 in s.receipts
                elif action == "checkpoint":
                    if not seen_reply:
                        raise Refused("reply-not-resolved")
                    s.save_checkpoint(1)
                elif action == "ack":
                    s.acknowledge(1)
                else:
                    s.collect()
            except Refused:
                pass
            s = copy.deepcopy(s)  # forget caller; model durable state survives.
            admission_oracle(s)
        if s.gc == 0:
            assert s.admit(1, 1, "E") == "NewFacts"
            s.save_checkpoint(1)
            s.acknowledge(1)
            s.collect()
        assert s.facts == {"E"} and s.checkpoint == {1: "NewFacts"} and s.gc == 1
        assert s.admit(1, 1, "changed-after-retirement") == "RetiredRequest"
        admission_oracle(s)
    record("admission-response-checkpoint-ack-gc-orderings", "120 modeled orderings with refused illegal transitions and same-request recovery")

    s = Admission()
    s.admit(1, 1, "E")
    s.admit(1, 2, "F")
    refused(lambda: s.admit(1, 3, "G"))
    s.save_checkpoint(2)
    refused(lambda: s.acknowledge(2))
    s.save_checkpoint(1)
    s.acknowledge(2)
    s.collect()
    s.admit(1, 3, "G")
    assert s.facts == {"E", "F", "G"}
    admission_oracle(s)
    record("window-two-longer-page", "ACK hole refused; incremental handoff allows third request without claiming page completion")

    s = Admission()
    s.admit(1, 1, "E")
    refused(lambda: s.admit(1, 1, "F"))
    # Request 2 is verified outside commit while owner installs closing barrier.
    s.closed = True
    refused(lambda: s.admit(1, 2, "F"))
    assert s.facts == {"E"}
    s.epoch = 2
    s.closed = False
    refused(lambda: s.admit(1, 3, "G"))
    record("inflight-close-and-paused-old-owner", "commit-boundary epoch/closure refusal; verified input and expired lease do not authorize mutation")

    s = Admission(facts={"E"}, checkpoint={1: "NewFacts"}, ack=1, gc=1)
    admission_oracle(s)
    bad = copy.deepcopy(s)
    bad.checkpoint = {}
    assert negative(admission_oracle, bad)
    record("mixed-backup-cut-negative", "post-GC owner plus pre-checkpoint producer rejected by causal closure oracle")

    a = Archive()
    a.prepare()
    a.commit_response_lost()
    a.catalog_up = False
    assert a.resolve() == "unknown"
    refused(a.prepare)
    refused(a.reclaim)
    a.catalog_up = True
    assert a.resolve() == "committed"
    refused(a.reclaim)
    a.protected = True
    a.reclaim()
    archive_oracle(a)
    assert not a.pending_present and a.attempts == 1
    record("archive-lost-response-reversed-recovery", "objects present/catalog unavailable stays unknown; resolution and protection precede reclaim")

    a = Archive(prepared=True, transaction_resolved=False, attempts=1)
    assert a.resolve() == "unknown"  # healthy absence, but original transaction still active.
    refused(a.prepare)
    a.transaction_resolved = True  # separate modeled confirmation of rollback/exclusion.
    assert a.resolve() == "not-committed"
    record("archive-absent-receipt-live-transaction", "healthy receipt absence cannot authorize a second attempt until late commit is excluded")

    c = Serving()
    for family in ("body", "tag", "scope"):
        c.insert(family)
    refused(lambda: c.publish(7))
    c.insert("empty-evidence")
    c.publish(7)
    # Replaying identical immutable identities is represented as set insertion.
    c.insert("body")
    serving_oracle(c)
    assert c.readable()
    c.incarnation = 2
    c.chunks.clear()
    assert not c.readable()
    refused(lambda: c.publish(7))
    record("ch-partial-empty-family-stale-incarnation", "all families required including empty completion; old PG frontier does not certify new CH")

    d = Derived()
    d.dirty_expiry_commit()
    for stage in ("rocks-reply-lost", "ch-reply-lost", "pg-reply-lost"):
        if stage == "ch-reply-lost":
            d.materialize()
        elif stage == "pg-reply-lost":
            d.activate()
        d = copy.deepcopy(d)
        derived_oracle(d)
    assert d.active == 1 and d.ch == {1: 0} and d.source_cut == 7
    record("derived-dirty-only-each-commit-response-lost", "output survives Rocks checkpoint; materialization precedes activation; old pin retained")

    j = JobOutbox()
    j.accept()
    j = copy.deepcopy(j)  # accept response lost; reconnect resolves same job.
    outbox_oracle(j)
    j.sign_durable()
    j.send("relay-a", reply_lost=True)
    j = copy.deepcopy(j)
    j.send("relay-a")
    assert j.targets == {"relay-a": "accepted", "relay-b": "pending"}
    j.authority = False
    refused(lambda: j.send("relay-b"))
    j.authority = True
    j.temporal_valid = False
    refused(lambda: j.send("relay-b"))
    j.cancelled = True
    j.pin = False  # exact durable bytes now own output, outstanding target retained.
    outbox_oracle(j)
    assert len(j.sends) == 2 and j.targets["relay-b"] == "pending"
    record("job-cancel-and-outbox-ambiguity-policy-expiry", "same bytes retried; separate target retained; policy/temporal invalidation prevents dispatch")

    # Human-written forbidden-state fixtures intentionally bypass transition guards.
    bad = Admission(facts={"E", "F"}, checkpoint={2: "NewFacts"}, ack=2, gc=2)
    assert negative(admission_oracle, bad)
    bad_archive = Archive(committed=True, attempts=2)
    assert negative(archive_oracle, bad_archive)
    bad_ch = Serving(active=(1, 7), chunks={(1, "body")})
    assert negative(serving_oracle, bad_ch)
    bad_derived = Derived(checkpoint=1, state_value=0)
    assert negative(derived_oracle, bad_derived)
    bad_send = JobOutbox(sends=[("relay-b", b"exact-signed-event-variant-A", False, True)])
    assert negative(outbox_oracle, bad_send)
    bad_signature = JobOutbox(sends=[("relay-b", b"new-signature", True, True)])
    assert negative(outbox_oracle, bad_signature)
    record("six-deliberately-wrong-controls", "ACK over hole, archive retry while unknown, partial frontier, dropped derived output, unauthorized retry, resigned retry all detected")

    # Independent arithmetic and graph fixtures for the selected example recipe.
    shares_gib = [2, 1.5, 4, 2, 1, 1, 2, 2.5]
    assert sum(shares_gib) == 16
    reserve, outstanding, margin = 64, 8, 16
    assert (72 - outstanding) == reserve
    assert (88 - outstanding) == reserve + margin
    assert 0 < 0.75 < 0.90 < 1
    assert 16 * 1024 * 1024 >= 14 * 1024 * 1024
    assert 20 + 5 < 60 - 20
    # No archive dependency on CH; no completion job holds an intake grant.
    waits = {"decode": {"ingest"}, "ingest": {"pending"}, "pending": set(),
             "archive": {"catalog", "objects"}, "index": {"CH"},
             "control": {"PG"}, "catalog": {"PG"}, "PG": set(),
             "objects": set(), "CH": set()}

    def acyclic(node: str, path: frozenset[str]) -> None:
        assert node not in path
        for dependency in waits[node]:
            acyclic(dependency, path | {node})

    for node in waits:
        acyclic(node, frozenset())
    record("recipe-arithmetic-and-abstract-wait-graph", "16 GiB sum, 72/88 GiB watermarks, timing and peak-unit bounds; selected abstract graph acyclic")

    return {
        "kind": "isolated-reference-state-model",
        "contract": "docs/specification/operations-contract.md",
        "executed_at_utc": datetime.now(timezone.utc).isoformat(),
        "model_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "status": "PASS",
        "cases": cases,
        "enumerated_admission_orderings": 120,
        "negative_controls_detected": 7,
        "not_executed": ["Rust product", "RocksDB/PG/DuckLake/CH integration",
                         "fsync/power/host loss", "real positive fencing", "socket/process scheduling",
                         "native memory peaks", "backup/restore/upgrade of engines", "capacity benchmark"],
        "limits": "Atomic in-memory transitions and explicit oracle fixtures; ordering checks are not a proof of engine durability or whole-system correctness.",
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    result = run()
    encoded = json.dumps(result, indent=2) + "\n"
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(encoded)
    print(encoded, end="")
