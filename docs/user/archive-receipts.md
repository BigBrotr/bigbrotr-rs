# Inspect retained archive receipts and measurements

The implementation candidate provides an explicit observation command. It needs
the existing [local and Control coordinator bindings](archive-initialization.md),
local Control/Archive roles, administration disabled in accepted policy, the
original Control attempt and its complete retained `.inputs` file. Stop the
running coordinator before invoking it. It does not need to reopen Ingest Rocks.

Provision a separate restricted PostgreSQL catalog reader and protected password
file. Configure both deployment roots beside the existing manager credential:

```toml
[postgres.catalog]
read_user = "archive_reader"
read_password_ref = "file:/run/secrets/archive-reader"
```

Merge these keys into the existing section. Offline validation never reads the
file or contacts PostgreSQL. Selected Archive requires both or neither; this
command requires both. The reader must differ from the manager. Root changes do
not alter persisted policy bytes. Existing initializer/configuration commands do
not require a reader.

The reader needs LOGIN, CONNECT on the catalog database, USAGE on `public` and
`bigbrotr_archive`, SELECT on all native `public` tables and SELECT on
`bigbrotr_archive.installation` and `bigbrotr_archive.scientific_schema`. Do not
assign table/column writes, ownership, grant options, database CREATE/TEMP, role
administration or membership in other roles. PUBLIC's default TEMP grant must be
removed from this dedicated database. An explicit administrator provisions those
rights; the command verifies them and does not repair them. The manager still
performs complete installation/guard inspection before native observation, using
the same pinned catalog endpoint and configured TLS identity.

```sh
bigbrotr archive receipt-inspect --file /etc/bigbrotr/instance.toml \
  --attempt-id 0123456789abcdef0123456789abcdef --max-receipts 128
```

Use the actual original attempt ID. The worker bundle defaults to `archive-worker`
beside the executable; `--worker-root` selects another matching installed bundle.
The finite scan reads every catalog receipt through successful EOF. The cap is
whole-catalog size, including unrelated receipts. The default128 fits the initial
512MiB worker allowance; that profile permits at most489 rows under its allocation
floor. A larger catalog requires a supported larger accepted allowance and explicit
work cap; changing a managed seed file does not activate that allowance.

Exit0 returns `bigbrotr-archive-receipt/1`, `state:present` or `state:absent`, original
attempt/batch/ordinal, accepted policy revision and committed Control release.
The revision identifies the policy checked at observation completion. This local
report is emitted after cleanup; it is not a claim of current delivery authority.
Both observations grant **no replay or source protection**. Present verifies the
receipt, not all scientific files or their failure durability. Absent does not
exclude an old writer/transaction. Missing/corrupt inputs, conflicting identities,
unreadable receipt files and exhausted scans are unresolved, never Absent.

Control remains supervised during native work. A policy change cancels this
invocation; retry with current policy after termination. The overall work deadline
is120 seconds, with SQL phases additionally limited to30 seconds. Signals,
expired/unavailable authority and failures cancel and join the owned supervisor.
A process stuck in filesystem/kernel I/O is not promised a hard wall-clock bound.
`TerminationUnknown` preserves the worker/scratch obligations of the archive
contract. Private `receipt-<nonce>` scratch directories remain for explicit cleanup
after verifying process exclusion. The command creates no publication intent,
credential lifetime, protection receipt or reclamation record.

Exit codes:0 observed/released,1 unavailable/output failure,2 usage,3 invalid input
or preflight,4 cancelled/unresolved. A blocked stdout may cause exit1 after a
completed observation/release. Repeating inspection creates a fresh observation.
No readiness or full recovery guarantee follows from a successful command.

## Check the complete batch membership

The candidate also provides a stronger inspection using the same prerequisites:

```sh
bigbrotr archive batch-inspect --file /etc/bigbrotr/instance.toml \
  --attempt-id 0123456789abcdef0123456789abcdef \
  --max-receipts 128 --max-facts 20000
```

This requires the receipt, every retained source ID/digest and its original family
file in one native snapshot. Missing, duplicate, extra or misplaced records refuse.
Directory symlinks, missing objects, unreadable inspected columns and layouts
that change the original paths refuse too. A payload page outside those columns
or a rewrite preserving the inspected values is not checked by this census.
The default inspects the current transaction snapshot. Add
`--snapshot-id N` to require exactly that retained native snapshot; missing or
expired snapshots never fall back to current data.

Exit0 emits `bigbrotr-archive-batch-membership/1`, the actual `snapshot_id`, six
`family_counts` (dictionaries, payloads, eligibility, receive, provenance, measurement) and
`files` (populated family files plus the receipt). It also reports the original
identity, policy revision and committed Control release. There is no successful
Absent state. `max-facts` is a complete-batch cap; it does not truncate the batch.
Its additional allocation allowance means the receipt-only maximum489 above does
not describe this command's capacity. Insufficient accepted resources refuse
before worker launch.

This compares stored digest columns. It does not recompute every payload, certify
dictionary/reference closure, reserve a snapshot, authorize a read or advance a
protected frontier. Preserve the original archive/state and scratch on failure.
The same deadline, cancellation and exit-code rules apply. The
[Linux amd64 qualification at3369091](../implementation/evidence/p02-archive-membership-native-20260920.json)
passes23 CLI cases and two actual blocked-reader cases over signed normal/history
fixtures, including identical restored retries and two PG crashes/restarts.
The120-second deadline returns after120102ms in that lab. This finite10/12-fact
corpus does not qualify aged archives or capacity at the default limits.

## Inspect one original compound measurement after restart

The `archive measurement-inspect` candidate reads one exact retained A05 fact.
Stop the running owner first, retain the same initialized Archive/Control/state
roots and complete publication inputs, and configure the separate SELECT-only
reader above. Explicitly select the deployment root `archive.reader.enabled=true`.
The command restores the current accepted configuration and supervises its policy
revision throughout work; it starts no listener or network measurement.

```sh
bigbrotr archive measurement-inspect --file /etc/bigbrotr/instance.toml \
  --shard-id 0 --owner-epoch 1 --sequence 9 \
  --payload-digest 0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef
```

Use the actual immutable measurement source and original digest from admission.
The dataset comes from the deployment root. These operands select a fact, not an
attempt reduction or the latest replacement for an unknown observation. An
original unknown and its later resolution remain independently inspectable by
their own identities. This is trusted local operator access to the configured
dataset, including private facts; protect stdout and redirected files accordingly.
It does not implement a public observatory resource or management-session grant.

The command selects the actual current complete published frontier from Control,
binds its complete retained inputs, captures that exact native snapshot under the
catalog barrier and obtains a real Archive reader pin. It never manufactures a
pin from a snapshot number or falls back to the catalog head. The native reader
verifies full cut membership and reconstructs the typed scientific measurement,
recomputes its original canonical digest, and verifies any strong prior-unknown
reference within that same cut, including its access/visibility context. This
does not perform a new dictionary-payload scrub or availability/count reduction.

One newline-delimited `bigbrotr-archive-measurement/1` document carries the complete
typed fact, its source/digest, and verified snapshot/frontier/topology/policy and
cut-digest/count metadata. It preserves null versus zero, signed UTC samples and
the original compound attempt. Inline observations are not separate votes. The
scientific document is at most20KiB. Its output worker, retained inputs and real
ReaderCustody stay inside current Control and the actual pin/barrier lifetime.
After output, pin release, barrier rollback and the catalog driver join, the
command releases Control and emits a separate
`bigbrotr-archive-measurement-completion/1` diagnostic. Exit0 requires both the
scientific document and successful cleanup. Partial output without that completion
and exit0 is incomplete; an already emitted observation never proves a later
cleanup succeeded.

The inherited profile permits at most128 whole-catalog receipts and a30-second
operation deadline, with the original pin/action authority window remaining
binding and nonrenewed. Accepted application/native budgets still apply; no scan
is truncated to fit. Missing inputs/facts, wrong digests, conflicting source IDs,
out-of-cut references, changed publication or policy, unavailable authority and
expired pins refuse with exit4. There is no successful Absent result. Invalid
operands/preflight use2/3, and local/diagnostic I/O can use1; scientific output
failure or unjoined work is unresolved4 and retains the pin obligation. Signals
cancel and join owned work before cleanup; unknown joins/COMMITs preserve their
obligations. A filesystem/kernel call is not promised a hard cancellation bound.
Private read-only scratch is removed only after a positively joined native child;
unknown native termination preserves it. Rust/native/installed restart and slow
output qualification for this new command remain required; source controls alone
do not establish those results.

The [native CLI qualification](../implementation/evidence/p02-receipt-cli-native-20260919.json)
at `d78bcc5` covers 22 retained attempts, refusal boundaries, real blocked-reader
supervision and cancellation, and Present/Absent through readonly data/state bind
mounts with writable disjoint scratch. The latter exercises actual custody locks
and file/directory synchronization; it does not make other host mounts immutable.
The initial 512MiB worker scan limit remains finite. No Linux arm64, signed native
mTLS, cross-boot restore, complete Archive service or M1 qualification is claimed.
