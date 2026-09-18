# Native accepted-configuration inspection

This scoped acceptance harness runs the actual `config inspect` executable
against an isolated PostgreSQL 16.15 lab. It is not an installer or bootstrap
implementation. Use the [owned lab lifecycle](../control-migrations/README.md)
with a fresh root, exact Git revision, clean build receipt and matching executable.
The runner refuses dirty/mismatched source and existing output.

```sh
python3 experiments/configuration-inspection/run.py --root /absolute/owned/lab
```

The 20 cases cover absent dataset/configuration, accepted managed values, comment
and policy drift, mode mismatch, declarative staging, JSONB u64 and exact finite-fraction preservation,
private endpoint redaction, missing/root/invalid/future policy fields, root budget
revalidation, digest corruption, wrong dataset/instance, denied SELECT and recovery.
Each compares the complete Control SQL dump before/after the CLI; a nonzero exit
alone is insufficient. The final observation requires no remaining application
backend. The lab's separate setup/migration and fixture SQL writes are explicit.
Stop only the owned lab afterward; retain evidence and protected credentials in
its inventory. No host port or real relay is needed.

`policy.json` freezes the archive-only normalized settings at source 39c34c5.
It was transcribed from the static candidate diagnostics and registered units,
with the three redacted empty endpoint lists restored from the recipe. This is
not an independent oracle for static defaults. The complete fixture was read;
its compact bytes/digest were independently reconstructed using Python's JSON
encoder and hashlib. The runner freezes that digest and applies explicit
mutations independently of the Rust persisted-policy decoder. Actual output
expectations and durable nonmutation checks are separate from that decoder.

The harness does not qualify bootstrap, update CAS, security grants/activation,
native writer ownership, concurrent revision publication, cancellation during a
blocked SQL read, unavailable-engine recovery, maximum-size memory, capacity,
arm64 or release installation. A source candidate and planned cases are not
executed evidence; a completed report must identify its exact artifact and run.

Executed native Linux amd64 evidence at source `5dc6a7f`:
[20 actual PG cases](../../docs/implementation/evidence/p01-config-inspection-native-20260918.json).
The linked report retains exact source/artifact identities and exclusions.

## JSONB document-size acceptance

`python3 experiments/configuration-inspection/size_limits.py --root /absolute/owned/lab`
uses a fresh lab to exercise MON-005 through static validation, initialization,
inspection and initial activation. The historical reproduction at `2c0dfc1` remains
in `experiments/control-runtime/monitor_regressions.py`; do not relabel that failure.
The new harness checks frozen independent numeric/structural vectors against PG16,
then constructs policies at 10 MiB minus one byte, exactly 10 MiB and one byte over
in JSONB text. Each has 200,000 fixed endpoints plus nested records, escaped Unicode,
u64 max and two tiny finite fractions. The original 49-byte endpoint counterexample
is retained as a separate refusal. Python Decimal and literal punctuation provide
independent sizes; actual CLI digests must match independently encoded compact JSON.
Accepted cases execute retry/read/activation/read/retry and compare both complete
stored documents; rejected cases require unchanged entire Control dump digests.
No peak RSS, mixed-load capacity, full task completion or release is established.
`jsonb-cases.json` also supplies the Rust counter's exact-boundary tests.

[Executed native size evidence](../../docs/implementation/evidence/p01-jsonb-limit-native-20260918.json)
qualifies `c7e0c09` on Linux amd64/PG16.15, including both complete stored documents
at the exact cap. The first `de49f18` harness incorrectly selected an unimplemented
source-setting field and failed before large admission. Its 22 passed rendering
vectors and failed case remain recorded; the corrected fixture uses `seed.file`.
