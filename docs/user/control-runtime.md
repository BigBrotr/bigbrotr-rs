# Initial Control runtime

The initial implementation runs the persistent PostgreSQL Control owner:

```sh
bigbrotr serve --file /etc/bigbrotr/root.toml --roles control
```

Use an existing validated root with Control assigned locally, protected application
credentials, the current explicit Control migrations, accepted initial
configuration and an activated security revision. Accepted
`administration.enabled=true` additionally requires migration0809, the browser
session SQL privileges, SELECT/INSERT/DELETE on control.reauthentication_proofs
and SELECT/INSERT on control.management_audit, then binds the
configured loopback private listener.
Its initial routes provide login, self inspection, logout and command-bound
reauthentication; they grant no data
read or management-command authority. `administration.enabled=false` starts no
listener. Editing an already accepted managed
seed does not disable administration. Follow the explicit migration/initialization
commands in [development build](../development/build.md) and the separate
[owner enrollment guide](administration/enrollment.md). `serve` does not initialize,
migrate or enroll. The process emits an `idle` JSON lifecycle record after it has
obtained backend ownership and validated both stored documents. This record is
local status, not a reusable authorization credential.

When the deployment assigns Archive locally, Control first inventories every
issued publication intent through a fixed startup cutoff, in pages of 64. The
`archive_intents` field reports that cutoff, observed prefix and page count.
`resolution=not-performed` and `publication_authority=not-granted` are explicit:
a nonempty inventory contains recovery obligations; an empty inventory grants no
Archive readiness. Historical catalog mismatches or holes refuse before `idle`.
Policy/owner supervision continues between pages, and a signal or failed query
closes and joins the driver. Control opens no catalog connection for this inventory. It first opens the
explicitly registered local coordinator state and holds its permanent custody
through driver join. Run the separate [state initialization command](archive-initialization.md)
after catalog installation/recovery; missing or incompatible bindings refuse
before connecting to Control. The binding does not resolve pre-protocol attempts. A deployment without a local Archive assignment reports
`state=not-selected`. This addition does not enable `--roles archive`.

After the actual Control claim, the current startup also requires the
exact SQL coordinator binding established by the separate `archive
coordinator-initialize --file PATH` command. Missing migration/binding or a mismatch
refuses before inventory/idle without creating a binding. The earlier schema9
inventory could observe pre-protocol rows; this stricter startup preserves them
and refuses their implicit adoption. No complete recovery route for that historical
case is advertised. The [eleven native command/startup cases](../implementation/evidence/p02-coordinator-cli-native-20260919.json)
pass at `5050999`. Older inventory evidence below retains its original revision and
scope; neither result enables Archive dispatch.

Omitting `--roles` uses the root's explicit assignment. Without
`--initialize-ingest`, this slice refuses any selected role other than Control
before opening credentials. Selecting a
subset never starts the omitted roles. It opens no native store and makes no
acquisition, publication or data-read readiness claim. The complete
product and the ordinary Compose installation remain in development.

The explicit `--initialize-ingest --roles control,ingest` path additionally
creates and retains a fresh native owner; see the
[initial owning lifetime](../reference/ingest-store.md#explicit-initial-owning-lifetime).
Its optional private listener supports login, self inspection, logout, source
registration/enrollment, acquisition scope registration and finite job acceptance/status.
It uses the same `access login`/`access logout` clients and refuses selected archive
read/export/assets and reports `ready=false`, `acquisition=not-started`. This is
still a fresh-only path: it cannot adopt or restart an existing native directory.
Explicit `--roles control,ingest,sync` additionally selects the bounded initial
socket consumer under that same owner and reports `acquisition=finite-initial`.
Follow the initial owning lifetime's transport, policy and resource requirements.
Accepted jobs still report queued: this consumer does not claim durable traversal
progress, job completion or complete acquisition readiness.

Explicit `--roles control,ingest,sync,archive` also selects the initial local
publisher. Its [bounded signed native journey](../implementation/evidence/p02-initial-archive-native-20261002.json)
passes, including ordinary Control restart/private history and one signed JSONL
export. Complete recovery and capacity remain under qualification. Initialize the catalog, local state and
coordinator separately first. This fresh-only profile requires migration0016,
Archive mutation privileges, an empty initial catalog and no prior Archive
attempt/consumer history. Configure the dedicated SELECT-only catalog reader's
`read_user` and `read_password_ref` even though HTTP archive reads remain disabled.
The packaged worker, private scratch root and separate native allowance are required.
Startup authenticates the actual retained native reader against the empty catalog
before fresh Ingest effects; the managed file seed does not activate policy.
Both accepted documents must select the fixed native file profile (64,000-row
groups, zstd-3, 256MiB target and at least512MiB per worker), agree on its worker
allowance/publish delay, and keep the initial producer window at most128 requests.

The accepted formation timer starts one complete pending batch at a time; available
batches then drain with250ms gaps. Every publication keeps its original retained
input/attempt, joins the actual native worker, seals the writer and captures the
complete source prefix before committing a frontier. Control/Ingest renewal and
private commands continue during joined worker waits. Application and child
native grants remain separate. The full cut's wire/allocation bounds can refuse
further publication; this is a bounded initial profile, not a capacity guarantee.
If the first catalog connection for a newly issued attempt returns an explicit
connection-refused I/O error, Archive parks for the rest of that initial lifetime.
This boundary precedes catalog SQL, writer provisioning and native launch. The
joined task returns its original attempt custody; its immutable inputs and
Control intent remain retained. Authenticated role status reports Archive
`unavailable/dependency_unavailable`, retaining the previous published cut and
its original observation age. Control, Sync and Index may continue under their
own authority and limits. Restoring the endpoint does not retry that attempt or
resume Archive automatically. Stop the lifetime before explicit recovery.
This direct refusal result does not identify a particular TCP/startup/TLS phase.
Startup failures and other connection errors, including database authentication
errors, timeouts and route failures, remain fatal, as do cancellation, lost
Control authority and every failure after connection. No uncertain catalog/native
outcome is cleared. General outage recovery remains a separate requirement.

Before a finite Sync round or Archive publication, the combined owner reserves
the complete application allowance; Archive also reserves its native worker and
completion allowance through cut verification. Temporary resource pressure defers
the unissued work at250ms intervals, preserving published cuts and observation
ages. Its first refusal starts a separate30s capacity-wait bound, which polling
and renewal cannot reset. This wait begins after read-only nomination and before claim or attempt
creation; it does not extend any admitted action, job, authority or operation
deadline. Permanent insufficiency refuses explicitly.

Deferred roles receive precedence over new Index dispatch. Actual Index work
continues to join, and Control renewal/private status remain available. Empty Sync
scans and Archive polls with no pending batch or due observation create no work
debt. The selected Sync job remains unchanged through a capacity wait; actual
claim rechecks current authority and the original job expiry. Temporary pressure
on the64KiB nomination read leaves its scan unchanged, without claiming no work.
After an Index operation joins, deferred work retries using the returned physical
credit while retaining the partial readiness verifier. Only a still-failed
application reservation may yield idle metadata; its historical observation,
oldest-read deadline and cooldown are never refreshed. Once admitted,
the complete grant funds the original progress/ACK and publication paths. Issued
effects, uncertain outcomes and cancellation retain their existing conservative
failure behavior. These logical bounds require separate installed overlap
qualification; they are not an RSS, disk-pressure or general capacity guarantee.

Supervised lifecycle output reports capacity transitions as
`bigbrotr-initial-capacity/1`, bound to the actual Control identity and a monotonic
wait identity. A denial records the whole request and a labeled post-refusal
budget observation, not the atomic failing snapshot. Admission retains the same
wait and records whether the actual retained Index operation was locally joined
and consumed. Neither record proves remote query termination or readiness.
The mailbox retains two original transitions through coalesced watch updates and
stops after128 records, with an explicit incomplete marker on overflow. The
existing4096-byte line/two-line output queue can still lose output; missing or
incomplete observations cannot establish an overlap or successful admission.

No producer ACK, pending reclaim, source closure, index readiness or native restart
adoption is added. Stop this initial lifetime before using the ordinary Control
read/export consumer with its explicit deployment roots and permissions. The tiny
installed journey enables `archive.reader.enabled`, initializes a separate private
export destination, then grants `history.read`, `export` and `private-export` through
reauthenticated commands. Dataset `data.read` remains separate. Follow the
[history](api/history.md) and [export](../operations/export.md) memory requirements;
that native journey uses 1 GiB read/export operation caps. Its retained signed JSONL
is portable; the export retention pin still needs its future lifecycle consumer.

To serve the initial private panel from that same listener, build the
[static bundle](../development/build.md#private-browser-consumer) and set deployment
roots before starting Control:

```toml
[administration.assets]
directory = "/opt/bigbrotr/private-panel"
base_path = "/"
```

The directory must be absolute with no symlink in any path component. The default
null directory leaves the session API routes available without a panel.
A bundle built for `/control` instead requires `base_path="/control/"` and is
opened at `/control/` (including its trailing slash). Static and API requests use
the configured literal loopback IP and port; `localhost` aliases are refused.
This HTTP profile assumes trusted services on the same host: browser cookies are
host-scoped, not isolated by port. Remote TLS remains a separate implementation gate.

An explicitly configured bundle is verified before connecting to Control, even
if accepted policy subsequently disables the listener. Missing, mismatched or
oversized assets fail startup. All bytes stay in a shared 20MiB process-budget
reservation; per-file limits are2MiB, the bundle is at most16MiB/128files and its
manifest at most64KiB. The ordinary login must still fit the remaining envelope.
Restart explicitly to install another bundle; changing files beneath a running
process does not change its verified bytes. The private `idle` record reports
base path, counts and manifest digest, never the host directory. It is an
integrity observation, not release authentication.

The panel supports one observed session, explicit refresh and logout. It does not
poll to keep idle sessions alive. A timeout during login/logout displays an
unconfirmed outcome; inspect the current session or explicitly retry the intended
action. Local expiry and changes in another tab clear prior observations. A
static loading/reload notice remains available if the startup script fails.
The full administration workflows remain under implementation.

New session issuance and explicit logout append an attributable audit receipt
in the same PostgreSQL transaction. An audit write failure also rolls back the
session mutation. A committed issuance may still have an undelivered cookie;
its audit does not claim the browser received it. Retrying an already committed
logout keeps its original audit, while revocations predating this feature or
caused by account reset receive no fabricated logout receipt. Session cleanup
retains those audit records. Rejected-request audit, audit browsing and retention
maintenance remain under implementation.

The reauthentication route accepts the current password and one typed scope-grant
command, with the original cookie, explicit CSRF token and command Idempotency-Key.
It shares login's attempt counters and single bounded hash worker. It returns a
same-session proof ID for at most five minutes, shortened by session expiry. A
verified retry of the same key/content preserves the original ID, expiry and audit;
different content conflicts. Credential, active-state, grant or policy changes
during hashing invalidate issuance. Up to32 proofs per account are retained;
expired-proof cleanup retains their audit. This route and migration are under
native qualification. Grant acceptance/consumption and actual private history
remain under implementation; a proof is never a data-access credential.

The accepted PostgreSQL configuration/security documents remain authoritative;
changes to a managed seed or staged declarative file do not activate themselves.
Every poll checks both complete documents against the immutable startup roots.
Policy authority lasts at most `runtime.authorization_ttl` (five seconds by
default), measured from request start with the suspend-aware clock. Polling runs
at half the remaining validity or the next ownership heartbeat, whichever is
sooner. Time spent waiting for a response consumes validity; it never extends
the request-start expiry. Only the
separate `runtime.heartbeat_interval` renews the scheduling row. A paused process
checks expiry before attempting another operation.

Initial Sync retains one finite round of at most three captured frames. Completed
nomination, claim, scope/source reads, native visibility/attribution operations,
relay collection, certification and admission are separate scheduling boundaries.
Before the guarded REQ, after prior SQL and attribution finish and before a socket
exists, the same Control/Ingest owner renews and revalidates its accepted policy.
Other boundaries renew when the existing 100ms dispatch margin is unavailable.
Captured receive times, request identity, prepared input and their resource grants
stay with that round; renewal does not replace them or renew a job's own authority.
The existing source/job and native checks still apply to every operation.

Each operation receives its own fixed deadline bounded by the actual remaining
authority, scheduled refresh and renewal timeout. Collection keeps its original
500ms soft cap, including handshake and close. Once an operation is first polled,
its deadline cannot be reset by renewal. A failed renewal, changed policy/owner,
cancelled dispatch or ambiguous admission fails closed; an already started
admission is never retried with a new identity. Original input staging alone is
not a receipt, checkpoint or ACK. Resolve retained native and PostgreSQL outcomes
before continuing an interrupted producer. An already selected management command
keeps its grant and runs after renewal before another Sync round.

SIGINT or SIGTERM while idle performs a bounded fenced release, closes and joins
the driver, then emits `stopped` and exits 0. Cancellation during an operation
closes the connection and exits incomplete; a lost release/acquisition COMMIT
response remains unknown. Database/policy failure exits without continuing work.
This candidate requires an explicit restart (or an external supervisor) after
recovery; it does not yet implement automatic reconnect/backoff. A fresh process
must obtain the actual backend lock before replacing prior owner metadata.
Never infer exclusion from a scheduling TTL or from the lifecycle record alone.

Exit codes: 1 unavailable/I/O, 2 usage, 3 invalid role/configuration/policy or
concurrent owner, 4 cancelled/unavailable ownership or unknown durable outcome.
Messages omit credential paths and database error details. Runtime records are
private operational output; do not publish them as a public health API.
Lifecycle and error output cannot hold up supervision: a bounded diagnostic
queue writes independently. Shutdown waits at most 200 ms for output after
backend cleanup, then terminates the process and its diagnostic writer. A
blocked or failed destination may lose records and changes an otherwise
successful exit to 1, even when the owner release committed. Inspect durable
owner state to resolve that distinction; missing output is not a rollback.

The [native acceptance evidence](../implementation/evidence/p01-control-runtime-native-20260918.json)
records 16 actual executable/PostgreSQL scenarios at `6be8515`, with independent
inspection of 240 relations and all seven process outputs. The
[reproducible matrix](../../experiments/control-runtime/README.md) defines that
scope. Public grants/revocation, native writer handoff, maximum-document/resource
behavior and the complete installed journey remain separate requirements.

The [monitor regression evidence](../implementation/evidence/p01-monitor-corrections-native-20260918.json)
adds delayed successful acquisition/polls, actual full output pipes and bounded
process termination at `2c0dfc1`, plus all sixteen prior process cases. It also
confirmed the then-open compact-JSON/JSONB size mismatch. The later
[JSONB limit qualification](../implementation/evidence/p01-jsonb-limit-native-20260918.json)
records its correction and exact boundary cases; the earlier report is historical
evidence, not an outstanding MON-005 failure.

[Startup inventory qualification](../implementation/evidence/p02-control-archive-inventory-native-20260919.json)
at `a010c15` (harness `d32d303`) exercises eight actual executable cases, including
130 retained intents, restart, historical catalog mismatch and cancelled startup.
A blocked PostgreSQL read can outlive local driver termination; its session fence
continues to refuse a successor with Busy until the backend actually exits. The
case observes that fence, releases the owned test blocker, then verifies a fresh
owner. It does not treat local driver join as remote backend termination.

The [coordinator-state native evidence](../implementation/evidence/p02-coordinator-state-native-20260919.json)
at `952fd9a` with harness `f822978` records 16 actual CLI/Control cases, 98 passing
Rust tests plus one explicitly ignored holder fixture, and the bounded 134-intent
adapter. Actual bind aliases, live contention, SIGKILL/restart, partial bindings and
blocked inventory cancellation are exercised. The local fixture creates an
initialization reservation only; it does not install a native scientific catalog.
No pre-protocol history adoption, Archive dispatch or protection is qualified.

## Finite initial Index recovery

The `index catch-up` candidate finishes a retained initial Index on the same
Linux/ext4 host boot. Stop the original Control/Ingest/Index process, retain its
native and Index directories and ClickHouse database, then explicitly close the
original source before starting catch-up:

```sh
bigbrotr ingest recover --file /etc/bigbrotr/root.toml --close-source
bigbrotr index catch-up --file /etc/bigbrotr/root.toml
```

The preceding Ingest recovery command can complete its retained native
acknowledgement watermark after checking the PostgreSQL checkpoint. That ACK
does not reclaim pending facts or original inputs. Index catch-up observes the
resulting retained state and performs no additional acknowledgement.

The root must assign Control, Ingest and Index locally, use migration0041 or
later, retain either the original schema1/topology0/Ingest epoch1 source or the
complete uncompacted schema2/topology1 initial successor lineage, and accept the
same Index provenance profile. Use the original ClickHouse writer account with
its fixed one-query concurrency constraint. The command reads its protected
writer credential and the Control credential; the initializer credential is not
needed. Retain every original request/outcome and required pending source batch.
The finite bound is 64 original requests across the whole lineage, 64 facts and
1 MiB per whole batch;
accepted target row/wire and process-resource limits also apply.

Catch-up verifies all published plans against the actual tables, replays any
exact prepared plan, and transfers the same generation to a new Index owner
epoch. It publishes the prepared plan only after that transfer, then plans and
publishes remaining complete source batches. Original plan/chunk origins and
digests survive. Missing or corrupt published rows, missing original input,
changed table/native identities, held custody, cancellation or unresolved
query/COMMIT outcomes stop the command while preserving retained state. It does
not automatically repair a published generation. Retry the same command after
resolving the reported prerequisite; a retry revalidates actual state and obtains
another Index epoch, even if no new batch remains.

For initial epoch2 continuation, first finish publishing the original epoch1
Index prefix. Use the existing explicit Ingest retirement, initial-successor
activation and terminal successor recovery/retirement procedures. After the
successor is fully closed and retired, run the same `index catch-up` command.
It verifies the complete native/SQL lineage, retains the closed epoch1 cut and
publishes one explicit Index handoff revision adding epoch2 at zero, then catches
up its complete batches. The bridge has no synthetic batch or CH receipt. An
incomplete epoch1 prefix, old pending plan, missing/stale handoff or nonterminal
successor refuses. Exact retry replays the bridge without adding another one.
This does not start an Index writer inside the successor acquisition process.

After successful Control release and actual driver join, exit 0 accompanies one
JSON record with `format="bigbrotr-initial-index-catch-up/2"`, `state="complete"`,
`dataset_id`, `generation_id`, `storage_incarnation`, `table_identity_digest`,
`previous_owner_epoch`, `owner_epoch`, `published_revision_before`,
`published_revision_after`, `source_epoch`, `source_through`, `published_through`,
`recovered_pending` and `new_batches`. The owner epochs are Index epochs;
`source_epoch` identifies Ingest 1 or 2. `source_cut` and `published_cut` retain the
complete topology generation and ordered `{shard_id, owner_epoch,
last_complete_sequence}` bounds. `index_handoff_revision` is null for epoch1
and the persistent bridge revision for epoch2; `published_handoff` is true only
when this invocation commits that bridge. The pending/new counters count batches,
not the bridge. For a captured revision 2 with pending plan 3 and one
unplanned batch 4, success reports revisions 2→4, `recovered_pending=true` and
`new_batches=1`; an exact complete retry reports revisions 4→4, false and 0.

The record always includes `ready=false`, `acknowledgement="not-performed"` and
`ingest_retirement="not-performed"`. `topology_transition` is `not-performed`
for epoch1 and `existing-ingest-handoff-verified` for epoch2.
It grants no continuing read readiness and starts no listener or acquisition.
The command performs no ACK, retirement, reclamation or new Ingest handoff.
Later successor epochs, compacted lineage and missing-pending Archive fallback
remain unsupported. The ordinary Control read/export procedure and
its own current grants remain separate. Exit codes are 1 output I/O, 2 usage,
3 configuration/policy/resources and 4 refused/cancelled/unknown outcome.
This bounded profile has installed restart, initial-topology continuation and
publication-COMMIT recovery evidence in the
[verification guide](../development/verification.md#installed-index-continuation-across-the-initial-topology-handoff).
General topology recovery, continuous successor ownership and capacity remain
under qualification; these results do not establish complete product readiness.
