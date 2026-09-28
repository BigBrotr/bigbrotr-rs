# Operational relay identity acceptance

P02-T01 Step 6's first actual Control consumer registers immutable private endpoint
identities. This harness is not an enrollment command, installed acquisition or
an archive dictionary allocator. All scenarios below are pending until an exact
revision's report records execution.

The shared native runner accepts a `relay-registry` profile after its exact Git
revision/tree arguments. It reuses the inventoried 1154757 compiler cache under the
same exclusive host lock, builds the current CLI and probe, then invokes `run.py`.
The existing control-migrations laboratory supplies isolated PostgreSQL 16.15,
SCRAM test credentials, an internal network, no host ports and explicit ownership.
Partial results, database state and artifacts remain; owned containers stop.

```sh
python3 experiments/network-transport/lab.py REVISION TREE relay-registry
```

The runner requires a fresh `/home/tux/bigbrotr-implementation-20260917/runs/relay-registry-REVISION_PREFIX`
containing a clean exact-revision Git `source` checkout. Transfer source through
Git using the [delivery workflow](../../docs/engineering/development-and-delivery.md).
It checks current disk/mount/process/container inventory before using caches.
No archive engine or dataset copy is required. Native compilation is not package
or release qualification; the component binary's build receipt states that scope.

Independent cases exercise the actual migration CLI, old ledger bytes from
published eb4ba5a, original-row preservation, idempotent apply, canonical equality,
path/query/scheme distinctions, an 8192-byte poorly compressible URL, 64 artificial
hash collisions with existing/new requests at capacity, a successor owner, actual
expired-owner COMMIT rejection, eight SQL refusals and transaction rollback.
The forced digest helper changes only an empty disposable fixture database; its
populated state remains with that same helper. It is never production migration
or a claim to have found a SHA256 collision.

The Rust probe validates the full independently retained fixture policy and its
canonical digest. It does not replace the product's deployment-root validator.
External SQL compares complete endpoint bytes/digests/IDs and hashes all tables
before/after; successful exit codes alone do not qualify the result. The SQL
fixture temporarily grants mutation privileges only to reach the retained-row
triggers; ordinary registration needs SELECT/INSERT only.

Held/lost COMMIT, current-policy changes across a lock wait, fresh-policy successor,
concurrent callers and corrupt overfull buckets remain separate cases. Raw SQL
forcing deferred constraints early cannot establish commit-time expiry. No
load/capacity, enrollment, route action permission, socket or M1 claim.

The original03b0197 run retained25 passing cases and a final SQL fixture setup
failure: `SET ROLE` from a postgres login hides that session's `backend_start`.
The fixture now connects directly as Control, matching the product. To verify
only this correction, transfer a clean exact Git checkout and execute its
`recheck_guards.py` with the original run root as its sole argument. It restarts
only the ownership-verified PostgreSQL container, contrasts both session shapes,
runs the eight refusals and rollback, verifies complete retained table contents
in all three databases and stops PostgreSQL. Its separately named result never
overwrites the original failure. No compiler cache or new engine copy is needed.

The `relay-registry-faults` native profile builds the actual fault probe and CLI
using the same locked compiler cache. It skips the unchanged catalog suites and
does not start an engine. After that finite build completes, run the exact
checkout's `faults.py RUN_ROOT`. It verifies unchanged product/migration inputs
against03b0197, restarts only that run's verified PostgreSQL and creates new cloned
fixture databases plus one finite, labeled client. Original database contents
and results remain unchanged; all new databases and results remain retained.

The existing wire proxy targets COMMIT2: COMMIT1 acquires Control. Cases cover
normal insertion, both lost-reply outcomes, held cancellation with close/release,
expiry while held, competing-owner refusal, a real dataset-lock wait across policy
revision advancement, fresh-policy old/new attribution and corrupt65-row buckets
for both existing and absent URLs. One existing-identity lookup after scheduling
expiry distinguishes read identity from network permission. Independent SQL
checks complete receipt fields, all60 table digests and actual backend locks.
The retained basic database already has two released owner epochs, so the first
fault owner is3 and its successor4 even when registration did not commit.

The client shares only the owned PostgreSQL network namespace; its loopback
authentication boundary is a fixture, not new SCRAM/TLS qualification. The
earlier bridge-connected adapter evidence remains separate. These cases do not
establish installed runtime resource reservations or network action authorization.
Each proposed fault case remains NOT_RUN until an exact-source report records it.
