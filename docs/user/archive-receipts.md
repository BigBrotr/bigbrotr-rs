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

The integrated CLI's native qualification is pending. Existing native reader
fixtures cover READ_ONLY engine/SELECT-only access separately; this command's
custody/fsync path on a read-only data mount remains to be exercised. No Linux
arm64, cross-boot restore, complete Archive service or M1 qualification is claimed.
