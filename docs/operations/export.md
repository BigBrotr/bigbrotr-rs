# Private export destinations

P08-T03/T04 implementation is in progress. The current finite commands prepare
and inspect the filesystem destination. Job acceptance has
[scoped native evidence](../implementation/evidence/p08-export-acceptance-native-20260924.json),
and the private job route has
[native HTTP acceptance/retry evidence](../implementation/evidence/p08-export-job-http-native-20260924.json).
The typed adapter now has [scoped native completion evidence](../implementation/evidence/p08-export-adapter-native-20260924.json)
for signed and empty JSONL files, with durable inventory and fresh-process
resolution. The ordinary Control runtime now contains a single-job automatic
executor under qualification. [One installed headless empty-export scenario](../implementation/evidence/p08-export-runtime-headless-native-20260924.json)
passes with actual native lookup, files and SQL completion. [One installed signed
HTTP/logout and interrupted-completion restart](../implementation/evidence/p08-export-runtime-recovery-native-20260924.json)
also passes with the same artifact and exact signed bytes; its fault deliberately
freezes the parent and one PostgreSQL backend before killing the parent. [Installed completion-COMMIT interruptions](../implementation/evidence/p08-export-runtime-commits-native-20260924.json)
also pass before COMMIT forwarding and after server completion: restart preserves
the completed unit and rebuilds the uncommitted unit with the same identity and
bytes. Old unresolved reader pins remain retained. Cancellation scenarios, other
output formats, object storage and artifact publication remain required work. A [native catalog-loss test](../implementation/evidence/p08-export-catalog-loss-native-20260924.json)
also checks rollback without partial inventory and reuse of the same Control
session; it does not qualify automatic recovery of the retained job or files.

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

If additive migration0815 is installed, the runtime also needs SELECT and INSERT
on `control.job_admissions`: every new job records its immutable admission order
in the acceptance transaction. The startup probe checks both privileges; missing
either disables the optional job API, or refuses startup when a destination is
selected. Migration does not grant privileges automatically. Stop older binaries
before applying815 because they reject its unknown ledger entry. This foundation
has [SQL and optional-runtime evidence](../implementation/evidence/p08-job-admission-native-20260927.json)
and a separate [installed acceptance/retry/status journey](../implementation/evidence/p08-job-admission-installed-native-20260927.json)
on815. The latter preserves original rows through upgrade and verifies the new
request's atomic job/receipt/pin/admission closure. It deliberately defers that
new job under insufficient execution memory; it does not extend earlier signed
execution or COMMIT-loss qualifications to815, or enable paginated job discovery.

`GET /management/v1/jobs` provides bounded actor-owned protocol1 export metadata.
It requires migration0816, current `status.read`, a live session/explicit CSRF and
an active loaded cursor issuer. Apply migrations explicitly while stopped; grant
the Control role the selected schema privileges, including UPDATE on
`control.cursor_key_lifecycle`. Follow the protected keyring lifecycle in
[cursor key custody](../security/credentials.md). Missing issuance prerequisites yield503
even for empty/final pages. The [scoped native report](../implementation/evidence/p08-job-pages-native-20260927.json)
records actual continuation, retention, restart and key rotation/revocation;
remaining commit/delivery faults, expiry and load acceptance stay open.

The closed query accepts `kind=export`, one `state`, inclusive
`created_from_us`, exclusive `created_before_us`, `page_size` and `cursor`.
Times are canonical signed64 decimal microseconds; page size defaults to50 and
clamps to200. Repeat the same filters and effective page size with `next_cursor`.
An empty page can still advance; only a null continuation means the listing has
ended. A fresh listing includes new admissions. Refused/changed/expired cursors
return400/409/410 respectively and require explicit correction or a new listing.
The typed client and **Your exports** panel have
[scoped installed evidence](../implementation/evidence/p09-job-pages-consumer-native-20260928.json)
on Chromium/Firefox/WebKit at both mounts: 42 stages and120 independently reconstructed
pages, real restart, blocked-read logout, other actor and account revocation.
They do not automatically fetch every page or turn metadata into download authority.
The original insufficient-headroom setup remains recorded; the lab now checks the
unchanged 64 GiB completion reserve before actual execution. Full commit/delivery
fault, expiry, load and accessibility acceptance remains open.

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

`POST /management/v1/jobs/{job_id}/resume` is now wired to the typed resume
adapter with [scoped installed HTTP evidence](../implementation/evidence/p08-export-resume-http-native-20260924.json). Supply the live session,
explicit CSRF, same-origin Origin and a new resume `Idempotency-Key`, distinct
from the original export acceptance. The closed body contains only canonical
positive decimal strings `expected_job_revision`, `expected_config_revision` and
`expected_policy_revision`. A new acceptance returns202 with
`bigbrotr-export-resume/1`, the command/job IDs, original accepted `job_revision`
and `accepted_at_us`, `created=true`, `execution=unobserved` and
`authority=acceptance-only`. An identical historical retry returns200 and
`created=false`, even after a later pause. Preserve the original key/body on a
lost reply; changing them submits a different command. The typed client makes
no automatic mutation retry and checks identity, revisions and response status.

Resume rechecks the original actor's `data.read`, exact-scope `history.read`,
`export`, `private-export`, expected revisions and the initialized original
destination. No `roles.control`, `status.read` or reauthentication proof is needed.
The same unit/caps/cut and retained export pin survive; scheduling TTL expiry does
not release retention. Filesystem checks run under signal/Control supervision,
outside SQL. A new reader or output write is not part of resume acceptance;
execution independently acquires both before writing. Schema0814 and explicit
SQL privileges are required. Missing optional protocol privileges return503;
historical retry needs no current input grant, destination or native reader.
The installed HTTP evidence covers reduced SQL privileges, same-unit acceptance,
expired scheduling TTL with retained input, known refusals and historical retry
after restart/revocation. The reader is disabled in that fixture. A separate
[installed resume COMMIT-loss scenario](../implementation/evidence/p08-export-resume-commits-native-20260924.json)
checks both unforwarded COMMIT and server-completed reply loss, bounded fail-stop
and original-key resolution after restart without destination/input grants.
[Sampled resume destination custody](../implementation/evidence/p08-export-resume-custody-native-20260924.json)
also retains root exclusion during an actual pending Control reply and SIGTERM;
the observed parent exits in about two seconds. This is a sampled interval, not
a continuous proof for every schedule. The [actual TypeScript client](../implementation/evidence/p08-export-resume-client-native-20260924.json)
also passes acceptance, explicit retry, revision/body conflicts, same-session
Control restart and account deactivation against installed HTTP/PostgreSQL on
Node26.4.0. Its in-memory cookie jar is a fixture, not browser qualification.
The [current session context](../implementation/evidence/p08-session-command-context-native-20260924.json)
supplies both checked configuration and policy revisions for new commands. Its
native client test also refuses a changed body on an old key with otherwise-current
revisions, then accepts that body under a new key. Always retain the original
key and body while resolving an uncertain result.
The [blocked resume checker](../implementation/evidence/p08-export-resume-checker-native-20260924.json)
now observes a real held marker open, continuing Control renewal and SIGTERM
fail-stop in about 4.04 seconds, with the original paused unit/pin and all twelve
post-login ledgers unchanged. Two deliberately incompatible-marker priming
requests return 503; this does not qualify accepted resume or arbitrary kernel I/O.
The [session lock-wait scenarios](../implementation/evidence/p08-export-resume-locks-native-20260924.json)
now check actual account/source waits: expired sessions return401 without a resume
INSERT; timely source release accepts exactly one queued revision. An expired
token cannot resolve history; a live session resolves the original receipt without
idle refresh. Short lifetimes are valid SQL fixtures, not a configurable login
feature. The [resume revocation scenario](../implementation/evidence/p08-export-resume-revocation-native-20260924.json)
also checks an authenticated input-permission revocation during the actual
filesystem wait. After the marker check succeeds, fresh authorization refuses
resume with403 and preserves the paused unit/pin. The grant retry resolves its
original receipt; the job observation remains available. This single schedule
does not qualify every grant/revocation interleaving; the CLI remains unfinished.
The [COMMIT expiry scenarios](../implementation/evidence/p08-export-resume-expiry-native-20260925.json)
also hold the actual COMMIT until a short session expires. Both idle and
maximum-clamped cases return401 and roll back the resume while preserving a
usable Control connection. Timely release commits one receipt; an expired
session cannot retrieve it, while a live session of the actor can. These are
audited short-session fixtures, not configurable login lifetimes.
A separate [installed
signed-resume scenario](../implementation/evidence/p08-export-resume-output-native-20260924.json)
restores a revoked input grant through authenticated HTTP, resumes the original
unit and verifies exact signed bytes plus complete inventory. Identical retry
after completion and process restart preserves its original acceptance receipt;
the new reader is released and the original export retention remains.

The private panel now provides **Inspect your job** after sign-in. Paste the job
ID from the acceptance receipt and select **Check job**. The initial view shows
only your own export's stored state,
revision/checkpoint, times and a redacted reason. Check again to refresh it;
there is no background polling or idle-session extension. Signing out, session
expiry or a session change clears the view and cancels a pending lookup.
The same observation is `GET /management/v1/jobs/{job_id}` with a live session
cookie and explicit `X-CSRF-Token`; supply neither body nor query parameters.
Current dataset `status.read` is required. Missing IDs and jobs owned by other
accounts share404. Input permission loss does not hide your operational receipt.
[Scoped native evidence](../implementation/evidence/p08-export-status-native-20260924.json)
checks these visibility/grant/session boundaries and restart against PostgreSQL.
[Scoped browser evidence](../implementation/evidence/p09-export-status-browser-native-20260924.json)
adds Chromium, Firefox and WebKit at both root and `/control/`: process restart
with the same session, cross-tab logout, a blocked lookup discarded on logout,
other-account isolation and actual session revocation. The full-page screenshots
show an unexplained skip-link overlay; viewport/focus verification remains open,
along with natural-expiry, BFCache and full accessibility acceptance.
This observation supplies no download or execution authority. Full listing
qualification, cancel controls and artifact discovery remain required work.

The initial **Resume this export** control now saves the original request before
sending and restores it after reload without automatic submission. Follow
[inspecting and resuming your export](../user/administration/jobs-recovery.md) for
the actual controls and recovery steps. [Scoped installed browser evidence](../implementation/evidence/p09-resume-browser-native-20260924.json)
covers Chromium at the root mount with actual Rust/PostgreSQL, a withheld real 202,
identical retry after reload/restart, account separation/revocation and resolved
local forget. The [remaining five engine/mount pairs](../implementation/evidence/p09-resume-browser-matrix-native-20260924.json)
now pass the same seven steps: Firefox and WebKit at `/` and `/control/`, plus
Chromium at `/control/`. Each uses a fresh isolated database, the same qualified
Rust artifacts and current production bundles. Full-ledger checks preserve old
rows while adding one atomic resume and seven expected session audits per pair.
These journeys observe acceptance and queued state with execution disabled;
accessibility, natural expiry, blocked resume authorization and complete release
installation remain separate gates.

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
Job discovery and terminal recovery remain required P08-T03/T04 work.

Selecting both an export destination and `archive.reader.enabled` selects the
initial automatic exact-ID executor, including when accepted administration is
disabled. This requires migration0501 plus execution privileges before startup;
explicitly apply migrations with the protected migrator. The runtime scans one
accepted/queued/running job at a time, advances past known refusals and wraps at
exhaustion, with a250ms interval between attempts. It commits an immutable artifact
reservation separately, then rechecks current actor/input/scope grants, captures
the original retained snapshot and obtains a fresh reader pin. Browser logout
and disconnect do not affect that committed job. File publication uses the
nonrenewable action window; inventory completion rechecks Control and catalog.
Known joined cleanup releases only the fresh reader pin. The durable export pin
is retained until its separate terminal retention lifecycle is implemented.

Each attempt has a five-second deadline for native work and a nonrenewable
action window. Control collection, positive joins and cleanup have their own
finite allowances and can extend total elapsed time. It shares the runtime's
application/native ledger; `export.memory.max_bytes` must fit the complete worker,
parent decode and credential/output overlap. The export-only profile now reserves
64MiB for the engine and96MiB for native overhead, retaining the full1MiB signed-row
envelope. Admission also charges catalog transport/credentials, reader input,
parent decoding, output and native completion control before secret I/O. Its
logical minimum is254,541,832 bytes with plaintext catalog transport,
262,930,448 with server TLS, and290,193,440 with mutual TLS. The default256MiB
therefore admits the first two profiles and defers mutual TLS; insufficient
admission does not create a durable pause or a new refusal reason. Shared Control
service costs remain charged separately. These are reservation arithmetic,
not measured RSS ceilings: reduced-profile execution and the five-export memory
gate remain under qualification. Publication and other native consumers retain
their existing profiles. Disk preflight checks complete
additional logical copies,64MiB native scratch and1MiB metadata headroom against
available space while preserving `runtime.disk_completion_reserve_bytes` on both
output and scratch filesystems. Existing bytes already reduce observed free
space. This preflight is not a physical quota or a shared multi-process durable
reservation ledger; those integration/capacity guarantees remain open. File I/O
or scratch preparation/cleanup failure halts the current runtime for recovery.
A known refusal after fresh reader admission releases only that joined reader.
After verified capture cleanup, a successfully started job with its exact output
lock still held is durably paused under current Control/policy and its expected
job revision. `resource_limit` identifies capacity refusal; other known refusals
use `execution_refused`. [One installed parking/restart scenario](../implementation/evidence/p08-export-runtime-parking-native-20260924.json)
passes. A [sufficient-cap signed-output and recovery counterpart](../implementation/evidence/p08-export-parking-completion-regression-native-20260924.json)
also passes on the same product artifacts; broader refusal and authority scenarios
remain under qualification. [Pause-COMMIT interruption and restart](../implementation/evidence/p08-export-pause-commits-native-20260924.json)
also passes: a committed pause stays paused after reply loss, and an uncommitted
pause is retried under a new owner using the same unit.
[Authenticated grant revocation during native execution](../implementation/evidence/p08-export-revocation-native-20260924.json)
also passes in one installed scenario: after `history.read` is revoked while a
native query waits, completion refuses the current grants and parks the job.
Already admitted finite work can leave private files; those files have no completed
inventory and confer no artifact visibility. The original pin remains retained.
Paused jobs retain the original unit, immutable caps and export pin, and are
excluded from automatic restart scans. Changing configuration does not enlarge
that unit. The explicit resume API and initial browser consumer have the scoped
evidence above; full browser qualification, CLI and cancel controls remain
required work. A Busy or
pre-output refusal cannot establish cessation of an old writer and remains fatal
after known reader cleanup. The [foreign-lock counterpart](../implementation/evidence/p08-export-runtime-busy-native-20260924.json)
observes an actual kernel `flock` refusal and autonomous fail-stop without a false
pause; it does not qualify reclamation of an old writer. A refusal before reader admission advances the scan
without creating another reader claim. Unknown SQL or worker termination
retains pins and output custody through actual transport joins; it never reports
successful cancellation. Pause/cancel commands, aged-queue fairness, mixed-workload
resource qualification and the complete installed recovery journey are still
required before this executor is advertised as qualified.
