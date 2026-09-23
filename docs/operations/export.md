# Private export destinations

P08-T03/T04 implementation is in progress. The current finite commands prepare
and inspect the filesystem destination. Job acceptance has
[scoped native evidence](../implementation/evidence/p08-export-acceptance-native-20260924.json),
but no user-facing job route or completed file export yet. Execution, restart,
output formats, object storage and artifact publication remain required work.

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
