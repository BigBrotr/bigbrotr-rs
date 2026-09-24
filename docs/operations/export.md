# Private export destinations

P08-T03/T04 implementation is in progress. The current finite commands prepare
and inspect the filesystem destination. Job acceptance has
[scoped native evidence](../implementation/evidence/p08-export-acceptance-native-20260924.json),
and the private job route has
[native HTTP acceptance/retry evidence](../implementation/evidence/p08-export-job-http-native-20260924.json).
The typed adapter now has [scoped native completion evidence](../implementation/evidence/p08-export-adapter-native-20260924.json)
for signed and empty JSONL files, with durable inventory and fresh-process
resolution. The ordinary runtime does not execute jobs yet. Automatic execution
and its recovery, other output formats, object storage and artifact publication
remain required work.

Configure an explicit private destination in the deployment file:

```toml
[export.destination]
id = "acacacacacacacacacacacacacacacac" # Example; allocate a fresh ID for an installation.
root = "/var/lib/bigbrotr-exports"
```

Both fields are deployment roots, excluded from accepted product-policy bytes.
Offline validation checks their syntax, pairing and lexical separation from
selected writable stores. It does not open the destination. The ID is a nonzero
128-bit lowercase hexadecimal value. Changing it or moving the root requires a
separate destination; an existing job must retain its original binding.

Create the directory as the account that runs BigBrotr, with mode `0700`. It must
be empty at first initialization, outside the archive, state, ingest, index and
scratch roots selected by this deployment. Then run:

```sh
bigbrotr config validate --file instance.toml
bigbrotr export destination-initialize --file instance.toml
bigbrotr export destination-inspect --file instance.toml
```

Initialization writes `destination.json` with format
`bigbrotr-export-destination/1`, dataset, destination ID, configured absolute root,
filesystem device and inode. The exact JSON bytes form the binding SHA-256.
It synchronizes the marker, root directory and root's parent before reporting
success. Higher ancestors must already be durably provisioned by the operator.
An identical complete retry verifies the old marker, writes the same
bytes through a fresh exclusive staging file, synchronizes it and atomically
replaces the marker before synchronizing directories. Its digest stays identical;
its inode may change. This reissues the complete write rather than relying on a
repeated fsync to repair an earlier writeback failure.
Inspection opens existing state only and does not create or repair files.

The implementation walks path components through retained descriptors with
`NOFOLLOW`, requires the current effective user's ownership and private POSIX
modes, and refuses linked or nonregular markers. An exclusive nonblocking root
lock serializes initialization; retained consumers use a shared root lock.
Conflicting users receive an explicit refusal rather than waiting indefinitely.
Runtime placement checks use the existing native Linux/ext4 mount mapping when
other selected stores exist. The [scoped native evidence](../implementation/evidence/p08-export-destination-native-20260923.json)
exercises actual bind aliases, locks, interrupted writes and syscall errors on
Linux amd64. This does not qualify host power loss or a packaged installation.

A copied directory, changed dataset/ID/path, substituted root inode, missing or
partial marker fails inspection. A failed write may leave a uniquely named `.destination-*.partial` staging file;
preserve that directory for diagnosis. A missing final marker and any retained
contents are an uninitialized, nonempty root, so first initialization refuses. Retry only the identical configuration.
If it still refuses, prepare a separate fresh empty destination and ID. There is
no implicit deletion, overwrite, permission repair of existing files or adoption
of old jobs. Deployment mount changes require quiescence and renewed placement
checks. POSIX checks do not attest ACLs or protect against a privileged operator
changing the host while work runs.

Exit codes are `0` for a bound destination, `1` for output failure, `2` for command
usage, `3` for invalid configuration, and `4` for unavailable/conflicting storage.
A lost output is resolved by the identical initialization retry and inspection.
The observation grants no job, dataset, scope or publication permission; the
[explicit scoped permissions](../user/administration/scope-grants.md) remain
separate, and job acceptance must bind source pins and the approved destination
atomically before dispatch.

With an initialized destination selected, `serve --roles control` requires
migration0813 and job/pin privileges, including the source-stream row-lock
privilege, before binding the private listener. It retains the destination
descriptor through Control connection shutdown. Per-request filesystem checks
run on supervised blocking work, outside Control transactions.

`POST /management/v1/jobs` accepts the closed `ExportJobCommand` in the
[management contract](../../contracts/management/v1/openapi.yaml). Supply the
session cookie, explicit `X-CSRF-Token`, exact Origin and original
`Idempotency-Key`; the body is limited to8192bytes. This initial profile selects
one exact event ID through the shared snapshot QueryRequest, a certified native
allocation and destination ID/binding digest. It accepts only
`signed-events-jsonl`. Protocol defaults are snapshot mode and page size100;
they do not change with deployment read defaults. The installed dataset and
actor come from Control. Browser paths, shell text and arbitrary queries are
refused. Unknown and duplicate command keys are refused before their values are
decoded; the parser does not construct a generic tree for unsupported fields.
New acceptance requires dataset `data.read` and all three exact-scope actions
`history.read`, `export` and `private-export`.

A new committed job returns202 with `bigbrotr-job-acceptance/1`, original command
key, job ID, decimal-string acceptance time, `created=true`,
`execution=unobserved` and `authority=acceptance-only`. An identical retry by a
live session of the same actor returns200/`created=false` with the original
identity/time. It does not repeat execution, change grants or refresh pins, and
needs no current destination or native reader. A changed command under the same
key conflicts. After a lost reply retain the exact key/body for resolution. A
receipt does not prove file availability or current job state.

The runtime probes the checksum-validated schema and job privileges at startup.
Without protocol813 or its privileges the route refuses503 while an otherwise
valid session/history runtime remains available when no destination is selected.
With the protocol available, historical receipts remain resolvable even after
removing the destination or disabling the native reader. New work needs both.
Known destination/native refusals leave Control usable. Unknown Control/COMMIT
outcomes fail closed and preserve retained obligations. The scoped native HTTP
evidence covers missing privileges, changed marker,
missing native assets, exact retry, Control restart without destination/reader,
and revocation with retained job/pin. The [blocked-checker native scenario](../implementation/evidence/p08-export-checker-timing-native-20260924.json)
observes Control renewal and bounded fail-stop on SIGTERM with no new job or
active pin. It suspends syscall entry; arbitrary kernel I/O is not qualified.
The [COMMIT-loss/restart evidence](../implementation/evidence/p08-export-job-commits-native-20260924.json)
covers connection loss before forwarding acceptance COMMIT and after server
completion. Both fail-stop and retain prior reader pins; a fresh Control resolves
the original key against actual SQL state. A committed job retains its export
pin; restart without destination/reader still returns that receipt. Old unresolved
pins are not recovered by this retry. The [held-driver destination scenario](../implementation/evidence/p08-export-destination-custody-native-20260924.json)
samples root exclusion while the original Control socket remains pending after
SIGTERM; an isolated source mutation removing original destination custody fails
the same oracle. This qualifies that observed drain interval, not continuous
observation of every possible schedule.
Job discovery, scheduling/execution and
terminal recovery remain required P08-T03/T04 work.
