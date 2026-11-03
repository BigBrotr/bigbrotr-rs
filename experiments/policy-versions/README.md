# Current persisted policy and incompatible-format refusals

## Bounded physical-growth diagnostic

The maintained runner also has an opt-in **diagnostic**, requiring a fresh
current parent/probe build and an unused private IPv4 /28:

```sh
python3 -m venv /absolute/physical-python
/absolute/physical-python/bin/pip install secp256k1==0.14.0
/absolute/physical-python/bin/python -B experiments/policy-versions/run.py \
  /absolute/fresh-owned-pg-root /absolute/exact-build \
  --physical-envelope --subnet ROOT_SELECTED_UNUSED_PRIVATE_28
```

Use the existing exact-revision build procedure; the new compiled probe mode
requires its own build. The optional Python environment is a reproducible input,
not a private cache prerequisite. The runner verifies both executable hashes and
the complete source diff before engine setup. This mode permits only the existing
noncompiled policy harness paths plus `physical_envelope.py` and its focused
portable control. Changed compiled code, schema, policy fixture or unknown paths
still refuse artifact reuse. No source/build receipt is rewritten to fit a run.
The existing setup owns the two exact image identities, isolated internal bridge,
no published ports, broad non-owner lab role, complete current migration catalog,
retained PostgreSQL data and final container stop path.

Two predeclared cases create separate fresh current databases/native roots.
Case A has compressible `a` content. Case B uses independent deterministic
SHAKE256 bytes encoded with JSON-safe base64, rather than a repeated random
block. Each corpus contains **63 distinct valid signed complete events of exactly
262144 bytes**, each followed by one JSONL newline. The maintained
`identity-contract/signed_fixture.py` signer uses the public test secret1;
its separate BIP340 group-equation checker and NIP01 canonical SHA256 verify every
event. Source digests bind the genuine DatasetLineage descriptor. An independent
CBOR/native oracle extends the existing signed-import fixture vocabulary to check
all canonical requests, receipts, fact/batch digests, exact pending P, original
names/bytes/hashes, complete native row digest and all64 PG reservations/outcomes.
It separately compares every Control row/xmin and all original/native state across
ordinary close/reopen. The accepted pending high/max-unit/event/outstanding limits
and existing32MiB/64-file input guards are checked before load; neither policy
nor the request65 guard is widened. PG's per-original65536-byte outcome allowance
is included in the aggregate preflight.

One genuine bootstrap advances ACK1. The63 direct imports commit native science
and copy PG outcomes while retaining ACK1/outstanding63. They have no
`acquisition_job_id`; no synthetic SQL job or source authority is inserted.
**Event ACK64, retirement, receipt/input reclamation and OPEN65 remain NOT_RUN.**
The fixture's current installed native profile uses32MiB cache,16MiB write-buffer
manager,2MiB per-CF buffers,16MiB soft WAL target,64 open files, two background jobs
and128MiB native allowance. It uses ordinary synced admission and automatic
background work. There is no manual flush/compaction or native mutable escape.

The readiness protocol is `physical-ready` → `load`; after31 admissions,
`physical-checkpoint-ready` → launch real PG `CHECKPOINT` and send
`checkpoint-started`; after63, `physical-load-complete` → keep the writer live for
the predeclared30 seconds → freeze its LOG capture → `close`;
`physical-closed` → `reopen`; `physical-reopened` → `finish`. Actual exit0 and joined
driver cleanup remain necessary after `physical-complete`. Input validation occurs
before the probe reads credentials or opens either engine. Gates have45-second
individual/120-second total deadlines; the runner has25-second readiness,
30-second load,150-second case and bounded output/sample/file/log caps recorded
in `physical-plan.json`. Missing work evidence never extends these limits.

The observer runs only inside the exactly owned containers under their configured
lab UID. Open directory descriptors bind device/inode identity and `fstatvfs`;
replacement, symlinks and cross-device traversal refuse. Regular-file allocation
uses `st_blocks*512`, deduplicates device/inode, and separates native WAL, SST,
MANIFEST, LOG, retained inputs, fixed source/preflight snapshot and PG
data/WAL/temp/other classes. Native/input and
PG directories are sampled every100ms, PG open-unlinked descriptors every500ms.
Container-scoped descriptor censuses record positive deleted-but-open regular
files and deduplicate their allocation. Permission failures and disappearance
races are explicit partial/unknown coverage, never an assumed zero. Full final
file identities accompany phase counts and non-atomic sampled peaks. Directory
metadata and unrelated filesystem users remain outside the file totals;
descriptor `f_bavail` captures their shared filesystem consequence without
inventing a reservation for them.

The real CHECKPOINT must advance PG requested-checkpoint statistics and actual
checkpoint/WAL LSNs. Engine checkpoint start/completion logs and per-admission API
wallclock intervals independently determine overlap; a readiness message or
Docker launch time does not prove it. Missing overlap is `NOT_WITNESSED`.
Incremental inode-bound LOG capture is frozen before close, excluding later
diagnostic read-only opens and ordinary reopen. Positive automatic flush requires
the pinned Rocks11.8.1 file-creation and post-MANIFEST commit evidence. Nontrivial
automatic compaction requires newly built positive output tables, multiple input
files and the complete same-thread install-OK summary. JSON start/finish alone
cannot establish successful installation. The conservative parser requires one
fresh-open retained LOG generation/session; rotations, missing/truncated records,
manual/shutdown work and unmatched jobs cannot manufacture a positive witness.

Results are `CAPTURED_PHYSICAL_DIAGNOSTIC` or explicitly `...WITH_GAPS`, with
per-case failed/unknown captures retained. These source samples provide diagnostic
peaks and positive work evidence. They supply **no numeric foreground d or
completion R bound, runtime filesystem funding, capacity or OPEN65 guarantee**.
Actual engine runs, physical failures, source-bound ACK/reclamation, concurrent
writers and the remaining reserve domains retain their existing gates.

Portable controls (no engines) are:

```sh
/absolute/physical-python/bin/python -B -m unittest discover \
  -s tests/acceptance -p test_physical_envelope.py -v
```

The bounded connected quota wave reuses this runner and its actual non-owner
lab-role verification:

```sh
python3 -B experiments/policy-versions/run.py FRESH_PG_ROOT EXACT_BUILD --connected-quota
```

Its exact probe seeds one genuine OPEN initial bootstrap, retains the same
Control and native custody across raw reopen, then allows a clone-only privileged
PG injection before the explicit whole-original reconciliation. Separate cases
remove the PG outcome, advance reserved beyond native issued, or change outcome
content with an independently recomputed hash. Two separate foreign-tail cases
insert request2 or outcome2 for the same slot/epoch with one dataset byte flipped,
keeping native issued1 and PG reserved1. The census must inspect the entire
slot/epoch prefix and check dataset identity on each row; filtering by dataset
would hide these corrupt extras. Refusals must preserve all current
Control rows/xmin, complete native bytes/sequence/P and the sole original input;
the next positive request must produce no input. A healthy proof must remain
pressure-latched while its exact committed retry succeeds immediately; the next
new dictionary request must return the ordinary pre-effect `Pressure` refusal.
The wave waits for no shortened policy and changes no process/operation limits.

The original socket continuation separately waits for the configured stable-low
interval before new intake. This wave does not qualify that wait, filesystem
funding, OPEN65, rollback/lease-loss races or production deployment grants. It
uses the existing broad lab role. The original four real PG/native cases passed
at source310b44eb in their bounded scope. Both new foreign-tail cases remain
NOT_RUN until exact-source qualification; portable verifier controls do not
qualify them or the complete logical capability.

Run the bounded native recipe through
`experiments/network-transport/lab.py REVISION TREE policy-versions`.
It builds the current checkout with declared dependencies and creates a fresh
isolated PostgreSQL lab. No old executable or private historical build path is a
prerequisite. The existing lab runner owns container/network/cache lifecycle.

The PostgreSQL runner can reuse qualified native artifacts after a strictly
bounded noncompiled harness correction. It records the built product revision
as `source`, the clean current harness as `harness_revision`, and their exact
`harness_only_changes`. Git's full name diff with rename detection disabled
must contain only this runner, this README and its portable controls. Both
executable hashes must still match `build-result.json` before engine setup.
Product, build, schema, compiled probe, fixture JSON and unknown paths refuse.
This carries the qualified current runner's reuse boundary; no old executable
can stand in for changed compiled inputs.

The case applies the complete current catalog through migration43. Its independent
inventory requires59 applied migration entries and60 hashed inputs, reads the
declared SQL in manifest order, and requires the exact83 Control table names
before comparing rows. A
same-count missing/unexpected table substitution refuses. The previous target19
was a historical fixture cutoff: the unbound dictionary probe itself needs the
migration14 bridge (Control ownership7 and registration12), while initialization
and activation require their declared management migrations. Captured retention
comes from accepted policy JSON; it does not need a PG42 registration column.
Using the complete current catalog exercises the current SQL trigger closure;
minimum-schema CLI qualification belongs to the separate migration/CLI cases.

The isolated laboratory application role retains SELECT/INSERT/UPDATE on all
current Control tables and EXECUTE on current Control functions. Before each
fresh database case, the runner records and checks its login flags, lack of role
memberships, migrator database/schema/table/function custody, exact per-table
grants, and lack of schema CREATE, DELETE, TRUNCATE, REFERENCES or TRIGGER rights.
This broad lab role is not qualification of the separate deployment-grant proposal.

The current single policy schema is `bigbrotr-product-policy/3`. The complete
explicit independent fixture is [policy-v3.json](../../tests/fixtures/configuration/policy-v3.json);
Python compact JSON and SHA256 supply its expected bytes and digest. It includes
explicit bans and receipt-retention64. Managed/declarative initialization,
inspection, identical retry, first activation and activation retry preserve the
accepted document. Staging retention62 does not replace accepted64; declarative
activation refuses drift. Current initial Control/Ingest writes, native reopen
and durable reads retain the existing independent dictionary/scientific oracle.

Separate cloned fixtures exercise superseded formats1/2, future4, missing bans,
missing retention and non-normalized bans, including recomputed digests. The
current CLI/Control owner must refuse them while preserving the full Control
relations. Healthy originals, all failed outcomes and resource receipts remain
retained. A before/after relation comparison is not a proof of no physical WAL or
row-lock writes: Control acquires transaction/row locks before policy validation.

The runner verifies the current probe SHA256 against its exact-source build
receipt before engine setup. Portable controls execute this admission boundary
with a matching fixture and with a probe changed after the receipt. The native
scenario additionally exercises actual `config initialize` retries against missing
retention and changed bodies with stale/recomputed digests; initialization must
restore the complete retained current document before returning its receipt.

Historical format1/2 source, inputs and executed evidence remain bound to their
original revisions. This source recipe no longer claims compatibility with those
formats. It does not perform a populated migration or repair incomplete documents.
Actual native execution remains required for this new schema; static tests are
not engine qualification. OPEN-prefix retirement, Live policy application,
physical-growth grants, pressure/fault capacity and release remain separate gates.

## Current historical-original integrity batch

The same exact-source build of the parent and `ingest_bootstrap_probe` supplies
one opt-in PostgreSQL batch:

```sh
python3 -B experiments/policy-versions/run.py /absolute/new-owned-run /absolute/exact-build --historical-integrity --subnet ROOT_SELECTED_UNUSED_PRIVATE_28
```

Use a new owned run directory, the existing build receipt, and an independently
checked unused private IPv4 /28. The runner checks both artifact bindings before
engine setup, retains the existing role/catalog checks, and owns the same cleanup
path. It does not build or reuse historical private executables. Current history
probe names explicitly use the lab's `postgres` DNS endpoint; the old shared-
network loopback modes retain their historical route. The declared dataset and
instance are written before actual CLI initialization.
Initial and clone database labels share explicit hyphen-to-underscore
normalization and lowercase ASCII SQL-identifier/63-byte validation before any
DDL interpolation. The ordinary policy codec labels retain the same names.

For this historical option only, the noncompiled reuse allowance additionally
contains exactly `experiments/ingest-bootstrap/historical.py` and
`tests/acceptance/test_historical_integrity.py`. The ordinary policy codec mode
keeps its original three-path allowance. The approved five paths let a corrected
fixture reuse an unchanged exact parent/probe pair; a change to any compiled
input still requires its own build. Use a fresh owned root and retain the original
failed run and its built/harness identities.

The maintained `historical.py` batch declares38 integrity checks, including12
private activation/recovered-ACK cases. Two genuine dictionary originals run
through current format2 initialization, typed progress/ACK, positive source
resolution and producer retirement, actual Control reclamation confirmation,
and native closed reclamation. Neither raw native edits nor fabricated PG42
confirmations create the seed. The independent oracle retains all three facts,
two complete batch markers and their exact logical pending charge. A target ACK
case adds one genuine successor dictionary fact and its batch marker.

The seed registers all nine current dictionary kinds from creation, using unique
domains `[60 + kind; 16]`; VisibilityScope remains `[64; 16]`. Unused kinds add
registry/manifest metadata and no scientific facts. The candidate allocates fresh
domains for the same nine kinds, satisfying the existing exact18-domain recovery
binding within the266B manifest/610B handoff bounds. The oracle binds every kind
and domain to the actual manifest and SQL rows; scientific frames and P remain
independent of that metadata.

Source close precedes release of its original registering Control. The probe
awaits actual release and driver join, observes that exact backend pid/start
absent, and acquires a new Control identity through complete accepted-document
restoration. That owner retires, confirms and reclaims the predecessor. Historical
`spec.control` stays unchanged. Preparation and activation intentionally keep the
same owner; only after genuine successor append/drop/reopen does ACK recovery
release/join the activator and acquire a further current Control. Independent
snapshots require unchanged native rows/sequence, science and input files across
both rotations, exact original fencing and only the declared owner/role effects.
No SQL guard, lease duration, resource budget or scientific input is altered.

Healthy later matching Config2/Policy5 bodies intentionally have different
revision numbers and different complete digests. A second seed starts under that
matching changed pair; later Config3/Policy8 remains separate. Privileged fixture
insertion supplies these declared accepted-head preconditions, and does not
qualify the future managed-apply command. Complete immutable Policy2..4 and6..7
intermediates retain their preceding healthy body/configuration; Policy5 and8
adopt the respective changed configuration. Every policy/configuration row keeps
the ordinary adjacent-revision CHECK. Independently rehashed Config1/Policy2
with inconsistent intake refuses. Public lookup covers complete-body corruption,
missing required fields with stale/recomputed digests, old/future schema,
retention range, ambiguous history and the document-cap boundaries.

Private cases obtain the actual prepared successor or opaque compacted replay
first. An atomic readiness/go exchange then permits an original-only mutation
immediately before activation or recovered compacted ACK under healthy current
authority. Exact validator traces distinguish the current pair, each original's
own digest, and activation's fresh delivery check. Refusal preserves every row
and xmin in all83 declared Control relations, native rows/sequence, and original
input bytes, including the corrupted original itself. Positive activation adds
only its exact receipt copied from the retained intent. The positive recovered
ACK is nonzero and idempotent at issued1/ACK1; it does not qualify lost-ACK
watermark advancement. Driver shutdown and normal owner release are separately
observed after the action snapshots.

The exact `4449c1dd` parent/probe build passed. Its first PostgreSQL attempt failed
at fixture database creation: an unquoted hyphenated label, before any of the38
integrity checks. That original failure remains retained. The built source and corrected harness remain separately bound; that failed
attempt is not reclassified by later success. The subsequent
`39e1e4aa` build also passed, and its actual PostgreSQL run passed all four
catalog/role/initialization/activation prerequisites. It then refused seed
retirement under the unfenced original Control. That fixture failure and its
native/input/SQL state remain preserved. Later source/runtime qualification is recorded separately below. At exact `8afb7457`, a subsequent real run recorded42 PASS:30 of the38
scoped checks and12 setup checks. All five first-set private activation cases
passed; the first ACK case then refused before its target gate because the
fixture granted only128KiB to a compacted lineage reader. Its existing checked
profile requires128KiB scratch plus512B for each of two epochs:132,096 bytes.
The corrected fixture reserves that exact profile before the recovery Control
claim, inside the unchanged2GiB payload budget, and moves it once into inspection.
Nine domains fit within each existing entry allowance; they do not multiply the
entry count. The original failure and partial proof remain separately bound to
their source. That attempt qualifies no completed ACK or lost-ACK advancement. At exact
`53f9abc4`, the maintained coherent parent/probe build and all38 required checks
plus12 setup checks subsequently passed, including all12 private cases. Public
original-body/pair refusals, caps, unequal valid revision numbers, private
activation and nonzero idempotent recovered ACK are therefore qualified for
that source. The separate `bb54ea43` native application-library run passed601
cases with exactly2 external drivers excluded and the genuine signed-pending
worker included. These results remain bound to their original sources; no later
main reconciliation or native ABI candidate inherits their artifact identity.

Post-test PostgreSQL fast shutdown exceeded the existing ten-second container
stop interval and ended137; the client ended143. Assertions completed before
cleanup. Retained data and drivers have separate custody evidence; this is not
a clean-shutdown or restart-after-that-kill qualification. Lost-ACK watermark
advancement and the other remaining gates below are still NOT_RUN.

The existing native boundary regression
`compacted_activated_stamps_refuse_stale_sequence_sidecars_cancellation_and_small_grants`
refuses a metadata grant of `required_metadata_bytes()-1` while preserving state;
the fixture uses the same owning formula as installed activated recovery. A
string/constant-only portable assertion would not exercise that guard. Portable
false-PASS controls are available with:

```sh
python3 -B -m unittest discover -s tests/acceptance -p test_historical_integrity.py -v
```

The linked current-format native fixtures require the literal five-put handoff
and exact rebound counter while preserving every original fact, receipt, index
and input. Original and compacted historical reconstruction both undo those five
puts before validating a completed observation; existing drop/reopen/replay and
zero-write retry tests are the discriminators. These native regressions remain
separate from connected SQL and use explicitly synthetic native-only floors where
the fixture declares them. No old counterless format is admitted by this change.

Historical counterless seeds and runners remain bound to their original source.
This batch does not qualify generic10MiB bodies through the standalone16KiB
reader, blocked-query cancellation, signed-event acquisition retirement,
lost-ACK advancement, OPEN request65, physical growth, Live application, load,
arm64 or release. Logical funding/grant wiring remains subsequent work.

The accounting/historical integration scope keeps ordinary prepared-slot refusal
and existing unknown-result/session-unavailable behavior. Its deferred OPEN-only
reporting reader/classifier, residual wrapper and three pure classifier cases
remain preserved in frozen `bb54ea43`; they are not a live recovery capability.
P02-T02 Step4 still requires a fresh locked original/prepared/native-floor reader,
old-floor replay and new-floor residual proof before OPEN effects or intake.
The later candidate's compiled inventory must independently reflect that exact
minus-three model-test boundary; previous601 evidence is not relabeled598PASS.
