# Protected local credentials

The local credential reader accepts `file:/absolute/path/to/file` references,
bounded to 8192 UTF-8 bytes including the `file:` prefix. A reference must name a
file: a root or trailing slash, relative path, control character, `.` or `..`
component is invalid. Repeated separators do not change the selected file.
Offline configuration validation checks only this syntax and does not open a
credential. A valid reference is not authorization to use its contents.

At load time every directory component is opened relative to a retained parent
descriptor, refusing symlinks. The final descriptor must be a regular file owned
by the effective process UID with exactly mode `0400` or `0600`. Group/world
permissions and executable or special mode bits are refused. Metadata and bytes
come from that same descriptor. A final FIFO does not wait for a writer, and
opening a substituted terminal cannot acquire a controlling terminal.

Provision the mount and runtime user together. Root-owned files with mode `0444`,
including common container secret defaults, do not satisfy this boundary for an
unprivileged process. The chosen Compose mount/user combination still needs
packaged installation evidence; this reader alone does not qualify it. Paths
through symlinked directories are refused too, even when the final file has the
right owner and mode. Use an actual supported mount path.

The reader preserves exact bytes, including NULs, whitespace and final newlines;
it does not trim a password or interpret a key. The consuming credential adapter
must validate its own format and purpose. Its positive byte limit is checked
against the descriptor metadata and enforced again while reading, so growth
after metadata inspection cannot bypass the limit. An empty file reaches the
adapter as empty bytes. Errors and debug formatting omit reference names and
contents.

This boundary checks POSIX ownership and mode, not extended ACLs or external
provider permissions. Operators must provision a filesystem/mount without wider
ACL access until that behavior is qualified. The owned read buffer is zeroized
on drop; this does not promise erasure of all process memory, kernel buffers or
downstream copies. The logical read
buffer has `limit + 1` bytes, and `allocated_bytes()` exposes its retained vector
capacity. Allocator overhead and temporary or driver copies need a conservative
process allowance. Resource owners must retain their charges through the actual
allocation lifetime, including unresolved operations.

Current evidence covers the reader and offline syntax. PostgreSQL/TLS consumers,
their resource ownership, extended ACL enforcement, actual container mounts and
credential rotation are separate implementation and acceptance gates. Filesystem
I/O is synchronous; `O_NONBLOCK` does not bound regular-file I/O on a stalled
filesystem. Runtime loading must use its owning bounded blocking boundary.

Cursor keys use this reader plus a dedicated mode0700 parent, single-link files
and an explicit preparation operation. Set `read.cursor.keyring_ref` to a
protected file in a separately provisioned directory, then run
`bigbrotr access cursor-keyring prepare --file instance.toml`.
`bigbrotr access cursor-keyring inspect --file instance.toml` only observes local
bytes. Both omit key material and reference paths from output; neither contacts
Control or activates cursor issuance. Keep the same key bytes for restart and
other workers, using a separately qualified protected distribution procedure.

An identical complete preparation retry preserves every key and replaces the
file through a new synced mode0600 inode. Preparation requires excluding external
provisioning writers; its advisory lock only coordinates cooperating consumers.
Partial staging is retained and preparation is refused. Preserve it for explicit
recovery; do not delete the directory and regenerate keys whose registration or
use is unknown. Inspection can read the last complete final file beside staging,
but cannot certify its previous durable publication. See the
[cursor ownership contract](../architecture/derived-read-publication.md#cursor-key-ownership-and-confidentiality)
for the format and remaining Control/rotation boundaries.

The [native local-keyfile qualification](../implementation/evidence/p06-cursor-keyring-native-20260925.json)
records actual sync/error/process-death/retry effects and a full blocking stdout
pipe. It preserves the original failed observer run separately. This finite
command evidence does not qualify key distribution, Control or rotation.

After explicit Control migration0601, register the existing keys with
`bigbrotr access cursor-keyring register --file instance.toml`. This command
re-synchronizes the same complete local bytes and then contacts Control through
its protected application credential. Missing keys and partial staging refuse;
registration never generates replacement keys. The deployment connection needs
schema usage, SELECT on the Control ledger/dataset/principals/audit and new key
tables, INSERT on `cursor_keyrings`, `cursor_keys`, `management_audit`,
UPDATE on at least one dataset column for the row lock, and EXECUTE on
`guard_cursor_registration` and `require_cursor_registration`. Use the existing
narrow application role, never the migration/schema owner.

Registration stores an immutable initial fingerprint and ordered key IDs/material
fingerprints with the installation actor/audit. It does not activate issuance.
A matching retry returns the original receipt without database writes; changed
keys are a conflict. Exit4 with `COMMIT outcome unknown` requires retaining the
keys and repeating the same registration on a fresh connection. Do not infer
rollback or prepare a replacement identity. Old binaries that do not recognize
0601 cannot open the upgraded Control ledger; stop them before explicit migration.
Native SQL/recovery qualification of this new command remains pending.
