# Current execution checkpoint

Observed 2026-09-18. Navigation only; follow the
[resumption procedure](agent-handoff.md#resume-from-one-verified-checkpoint).
Verify Git, dirty work, evidence and resource claims before continuing.

## Mandate and active boundary

Owner-authorized complete implementation remains active. Mac is the editing/Git
workspace; use trusted exact-source Tux execution despite unavailable hosted
Actions. No public release/rename, Linux arm64 qualification or changes to the
read-only Go neighbor. All selected product work remains required.

Active P02-T03/T04: signed durable pending → actual PG DuckLake publication,
positive fencing, receipts/protection, then M1 read/export/restart. No P02 task or
M1 gate is complete. Catalog guard, credential provisioning/sealing and finite
private supervision prerequisites now have scoped native evidence. No scientific
product schema or publisher exists yet. Pending must not be acknowledged/reclaimed.

## Git and integrating ownership

Mac main; root is the sole writer/integrator. Current product candidate HEAD is
`d2b260925b2b1d5d46275c53f4f04d4b05631b25`, tree
`df6436e455aea3dd708ed8ae0f3d307fe41d05d7`. It was committed from locally checked
work and transferred to a fresh Tux checkout through Git. Last observed origin/main
is `d54a5f928f301f4628d51a950257e59e655707c0`; candidate/evidence push remains next.
Current root-owned dirty work is evidence/contract/task/README/inventory/checkpoint
updates for that candidate; no product source edits followed its native execution.
Independent agents were read-only. No concurrent writer or active experiment.

## Latest qualified slice

[Credential report](evidence/p02-catalog-credentials-native-20260918.json), candidate
`d2b2609`: 8 actual-PG sequences and 16 native Rust cases PASS. Mac 3 material tests,
2 bootstrap-wire tests, scoped lib/both-example Clippy, format and Python syntax
PASS. Bootstrap validation failed an actual assertion before implementation;
earlier missing-dependency compiler REDs are separately retained. Resolved Cargo
packages/versions are unchanged; only direct base64/ring/serde edges were added.

Implemented boundaries:

- New random 256-bit password, local PG16 SCRAM verifier, atomic role creation +
  retained Control-intent import + DML grants + owner activation. Only a known
  new commit returns a consuming private credential handoff. Existing roles or
  attempts never reset credentials or produce another handoff. Unknown completion
  returns no credential; resolve from a fresh connection before deciding recovery.
- PG16 implicit creator ADMIN-only membership is permitted only for the guard
  schema owner, with INHERIT=false/SET=false. Provisioning clears the local
  createrole_self_grant setting. The actual non-superuser manager passes; four
  membership modifications are refused and restored. Explicit initial ownership
  transfer used temporary SUPERUSER, removed before ordinary provisioning.
- Seal verifies the exact retained lifetime/current role OID under the owner lock,
  removes LOGIN/password and disables only the matching current owner. Repeated/
  historical seal preserves a successor. Real wrong password fails 28P01; after
  seal, a new login fails 28P01 and an existing backend mutation fails 55000.
- Finite bootstrap now uses the existing verified-artifact private supervisor.
  Closed JSON is counted before spawn (32KiB total,16KiB DSN/4KiB root), then
  streamed to stdin without a retained plaintext JSON buffer. Actual bootstrap
  and a fresh writer's native attach succeed only after exit/EOF and positive reap.
  This is not scientific publication, identity adoption or persistent uniqueness.

Four wire-fault sequences distinguish pre-forward rollback from completed COMMIT
with lost reply for provisioning/sealing. Complete independent state is checked
before retry, preventing retry from hiding an incomplete seal. Two same-intent
races observe actual backend incarnations/pg_blocking_pids: commit permits only one
fresh credential; rollback permits exactly one replacement. Existing-role collision
refuses without adoption/reset. The only dropped role was the exact new, never
used collision fixture. All 8 final lifetimes retain history but have no password
or LOGIN. Catalog owner 8 is disabled; Control has 9 intents, 33 epochs, 0 active owners.
Native scientific tables/data files remain 0. Signed Rocks-to-worker preparation
was separately re-executed without ACK after the shared parent refactor.

Exact PG16.15 upstream source hashes/inspection ranges and pinned ring derivation
are in the dependency record/inventory. AI review caught PG OID JSON typing,
missing pre-retry sealing oracle and too many checks inside the timed lock window;
all corrected before native execution. Root verified source/artifact/state audits.
No full-product review, full CI, RSS/throughput or installed recovery claim.

## Earlier prerequisites and constraints

- [Rows/Parquet](evidence/p02-five-family-rows-native-20260918.json): nullable
  compound references require matching parent/child masks for actual native
  Parquet fidelity. Signature-hash startup threads are separate from query
  threads; PID cap8 aborts the pinned worker. Budget native startup independently.
- [Private supervision](evidence/p02-private-supervision-native-20260918.json) and
  [parent binding](evidence/p02-parent-binding-native-20260918.json): bounded
  nonblocking pipes, verified artifacts, positive reap and Linux parent-death
  behavior. Unknown termination retains grants/root cleanup obligations. A signal
  or dropped handle is not remote catalog exclusion.
- [Input identity](evidence/p02-archive-identity-native-20260918.json), `8d1289d`:
  borrowed archive membership, explicit topology, maximal source ranges and
  independent literal/signed digests. Rocks creation topology is not current
  publication authority. [Control issuance](evidence/p02-archive-attempts-native-20260918.json),
  `b245644`, retains immutable CAS intents; its synthetic empty topology does not
  qualify source completeness or signed publication.
- [Native catalog](evidence/p02-catalog-fence-native-20260918.json), `a531ea3`:
  unfenced duplicates, live old writer after NOLOGIN, positive exclusion and lost
  COMMIT response. Pinned extension requires explicit TEMPORARY memory postgres
  secret via META_SECRET and META_SECRET_STORAGE_TABLE=''. Keep persistent
  secrets/autoload/community extensions disabled. Native ForceAcquire can exceed
  nominal pool size; PG connection limits are approximate.
- [PG startup](evidence/p02-postgres-startup-native-20260918.json) and
  [native startup](evidence/p02-native-startup-exclusion-20260918.json): a live
  invisible old connection can commit after NOLOGIN/empty activity. Fresh PG stats
  snapshots are necessary. Positive client/process exclusion or the enforced
  storage fence, resolution of old transactions and fresh receipt inspection are
  separate requirements. Unknown process state never grants cleanup authority.
- [Guard experiment](evidence/p02-catalog-guard-native-20260918.json) covers six
  native schedules and 44 privilege cases. The selected [product guard](evidence/p02-catalog-product-native-20260918.json),
  `944f3e6`, inspects actual28-table layout/keys/collation/ownership/trigger/function
  hashes, imports immutable intents and rejects live stale native commits. Prior
  String→REGROLE, numeric Control epoch ordering, collation and NOLOGIN defects
  retain failed candidates and corrected evidence. Applied guard SQL is unchanged.
- [Activation recovery](evidence/p02-catalog-recovery-native-20260918.json),
  `55dba36`, covers 14 actual install/activation faults/races/cancellations and
  full independent privileges/identity. Cancelled operations can remain server
  blockers until explicit resolution. Matching observation never authorizes a
  second native client under the same lifetime.

The guard owner row is locked FOR SHARE through real native COMMIT/ROLLBACK;
manager activation/sealing locks it FOR UPDATE. session_user plus role OID binds
writer identity; native values/columns are never manually rewritten. Runtime
metadata DDL is refused and inlining must remain zero at every scope. Full ordinary
mutation/maintenance coverage still gates production enablement. Manager role/DDL
changes and restore are privileged operator boundaries requiring quiescence and
reinspection. Catalog fencing does not prove old process death, release resource
grants or authorize orphan cleanup. A consuming callback can still copy secrets;
persistent one-worker lifecycle enforcement remains required.

## Independent monitor findings

Scoped corrections remain closed within their actual evidence:
[MON-001/002/004/006](evidence/p01-monitor-corrections-native-20260918.json),
[MON-005](evidence/p01-jsonb-limit-native-20260918.json),
[MON-009](evidence/p02-job-bounds-native-20260918.json),
[MON-010](evidence/p01-tls-presence-20260918.json),
[MON-007](evidence/p01-archive-cache-retry-20260918.json).
MON-009 uses additive0008, authoritative caps, sorted SHARE locks, inverse checks,
READ COMMITTED and incompatible-history refusal; 24 actual PG cases passed.
MON-010 preserves offline selected/unselected TLS validation; MON-007 uses private
unique attempts and exclusive verified cache publication with actual HTTPS retry/
concurrency on Mac/Linux. Do not mechanically rerun unrelated suites.

## Verified resources and continuity

Inventory `implementation_archive_worker_20260918.catalog_credentials` in
[the laboratory inventory](../research/laboratory-inventory.json).
Raw root `.research/implementation/archive-worker/catalog-credentials/d2b2609`;
native root `/home/tux/bigbrotr-implementation-20260917/runs/catalog-credentials-d2b2609`.
All 648 source files and the exact Git archive independently match Mac/Tux.
All four actual ELF identities are in the report; worker SHA
`41361bd4ca3354947dbfc49166afb34297705e8c40506f04fbb66864d22078ee`.
Writable Cargo/target caches were separate copies of retained catalog-recovery
build outputs; retained assets were mounted read-only. No independently edited
source copy exists on Tux.

Owner `283b16f0bb55f05232390af3d3172359`; runner
`bigbrotr-implementation-catalog-credentials-d2b2609`, ID
`0736621ec76a2a0e92f5ccc4f01f86bf9edcc392712de4697a6ae985ff456fe6`;
PG suffix `-pg`, ID
`9bdbbd3acba29711811ec999f2283f5636b098cc5d8d8995ce3914629a993b53`.
Runner deliberately stopped143 after successful cases; PG stopped0; no OOM.
Owned network `ec3feec8d475c46683240861717bfa17ec04e9e5743c11ee995ba9541bb7ba61`
retired, no new volume. 20 unrelated identities/processes/mounts/networks unchanged,
8 running; zero BigBrotr running. Sessions89477(transfer),91027(cases),36361(audit/
cleanup),60917(copy) are terminal. No unfinished cleanup or active native run.
Previous catalog/recovery/startup/MON resources stay stopped as inventoried; never
restart retired-network containers. Native amd64 does not qualify arm64.

## Next bounded action

Finish the evidence/contract/checkpoint review and normal private push, then wire
the first real scientific catalog schema and atomic semantic receipt under
P02-T03/T04. Read complete owning initialization/migration, archive format,
publication/protection and receipt contracts plus actual callers. Resolve the
initialization order explicitly: today's finite bootstrap/guard fixture contains
no scientific tables, and ordinary publishers must not silently migrate/adopt an
unfamiliar catalog. Reuse the fresh credential and supervised worker paths; retain
one consumed credential per new lifetime and recovery obligations after ambiguity.

Connect all five families and receipt/protection to signed M1 read/export/restart,
with independent full content/reference readback. No pending ACK/reclaim or task/
M1 completion before actual guarantees exist. Full persistent worker recovery,
S3,aged mixed load/RSS,Linux arm64,acquisition/analytics/read surfaces/admin/website,
and full Compose/CI/release journeys remain selected required work.
