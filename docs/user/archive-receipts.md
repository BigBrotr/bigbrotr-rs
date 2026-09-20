# Inspect a retained archive receipt

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

Exit0 emits `bigbrotr-archive-batch-membership/1`, the actual `snapshot_id`, five
`family_counts` (dictionaries, payloads, eligibility, receive, provenance) and
`files` (populated family files plus the receipt). It also reports the original
identity, policy revision and committed Control release. There is no successful
Absent state. `max-facts` is a complete-batch cap; it does not truncate the batch.
Its additional allocation allowance means the receipt-only maximum489 above does
not describe this command's capacity. Insufficient accepted resources refuse
before worker launch.

This compares stored digest columns. It does not recompute every payload, certify
dictionary/reference closure, reserve a snapshot, authorize a read or advance a
protected frontier. Preserve the original archive/state and scratch on failure.
The same deadline, cancellation and exit-code rules apply. Native Linux CLI
qualification for this new command is pending; the older receipt report below
does not qualify it.

The [native CLI qualification](../implementation/evidence/p02-receipt-cli-native-20260919.json)
at `d78bcc5` covers 22 retained attempts, refusal boundaries, real blocked-reader
supervision and cancellation, and Present/Absent through readonly data/state bind
mounts with writable disjoint scratch. The latter exercises actual custody locks
and file/directory synchronization; it does not make other host mounts immutable.
The initial 512MiB worker scan limit remains finite. No Linux arm64, signed native
mTLS, cross-boot restore, complete Archive service or M1 qualification is claimed.
